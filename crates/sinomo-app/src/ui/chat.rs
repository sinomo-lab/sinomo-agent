//! "Chat" tab — the AI chat experience.
//!
//! Like the Settings and Files tabs, this is a fully *composed* `sui-widgets`
//! tree: [`view`] returns a responsive host ([`ChatView`]) over pre-built pods
//! (wide split with the agents/chats sidebar, narrow transcript-only), each
//! assembled from stock `ListView` / `ScrollView` / `Select` / `TextArea` /
//! `Button` / `Separator` / `Label` widgets, plus a small set of
//! bespoke painters for the parts whose *shape* changes live.
//!
//! The data layer — [`ChatState`] + its methods, the [`ChatHandle`] worker
//! thread, the provider/model fetch, and send/stream — is UNCHANGED from the
//! hand-painted version. Only the view is rebuilt.
//!
//! Two regions need care:
//!
//! * **Streaming transcript.** The message list grows when a turn is sent, and
//!   the last (agent) message's text grows token-by-token while streaming —
//!   frequently. Rebuilding the whole transcript per token would be wasteful, so
//!   the transcript is a single live-reading painter ([`TranscriptView`]) that
//!   re-reads `ChatState.messages` in `paint` each frame and draws the bubbles
//!   (the same wrapped-text bubble math the hand-painted UI used). It owns its
//!   own scroll (auto-stick-to-bottom while streaming) exactly as before, and
//!   self-invalidates by diffing a content summary on each `RedrawRequested`.
//!
//! * **Streaming pump.** A worker thread streams tokens back over a channel. An
//!   invisible [`ChatPump`] widget keeps the *existing* request-animation-frame
//!   pump alive: while a reply streams (or there was recent activity) it drains
//!   the worker each frame, applies events to `ChatState`, and re-requests an
//!   animation frame — the same wiring the old `ChatView::pump` had.
//!
//! The agents/chats sidebar and the composer are stock widgets. The composer
//! follows the Mesh design language: one shared [`FramedField`] holding
//! queued-steering chips, attachment chips, the borderless message
//! `TextArea` and the action bar (attach · voice · model/context/activity ·
//! Send-becomes-Stop). Each region is wrapped in a [`RebuildOnChange`] keyed on
//! a small signature of the state it renders; the provider/model/effort
//! `Select`s live in the model popup above the field.

use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::path::{Path as FsPath, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sui::{
    ActionTilePaint, Alignment, ArrangeCtx, CalloutPaint, CodePanelPaint, CodeTextLine,
    CodeTextPaint, CodeTextSpan, Color, Constraints, ContextMenu, ControlSize, CustomEvent,
    DefaultTheme, Dock, EmptyState, Event, EventCtx, FieldAppearance, Flex, FlexItem, FlexWrap,
    FontWeight, FramedField, Icon, IconButton, IconGlyph, ImageHandle, ImageSource, Insets,
    KeyState, Label, ListItem, ListView, MeasureCtx, MeasuredBottomDock, MenuItem, PaintCtx, Path,
    Point, PointerButton, PointerEventKind, RebuildOnChange, Rect, RegisteredImage, RichText,
    RichTextSourceMap, ScrollBar, ScrollDelta, ScrollState, ScrollView, SectionLabel,
    SegmentedControl, SegmentedControlItem, SelectionScope, SemanticRegion, SemanticTone,
    SemanticsAction, SemanticsCtx, SemanticsNode, SemanticsRole, SemanticsValue, Separator,
    SideSheet, SideSheetPlacement, SignalMeter, Size, SizedBox, SplitView, Stack, StackHostOptions,
    StatusBadge, Surface, SurfaceAppearance, SurfaceBorder, SurfaceRole, SwitchView, TextArea,
    TextBlockPaint, TextCellPaint, TextCommand, TextDocument, TextInput, TextLayout,
    TextLayoutRequest, TextParagraph, TextParagraphStyle, TextSpan, TextStyle, TextWrap, Tooltip,
    TooltipAlignment, TooltipPlacement, TrailingSlotRow, Widget, WidgetPod, WidgetPodMutVisitor,
    WidgetPodVisitor, containers::Padding as WidgetPadding, draw_glyph, paint_action_tile,
    paint_callout, paint_code_lines, paint_code_panel, paint_rounded_panel, paint_rounded_rect,
    paint_section_label_detail, paint_text_block, paint_text_cell, wrap_text_lines,
};

#[cfg(test)]
use sinomo_agent::{AGENT_CHANNEL_APP_USER_ID, AgentChannelSource};
use sinomo_agent::{
    AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT, AgentChannel, AgentChannelAuthor, AgentChannelKind,
    AgentChannelMessageKind, AgentChannelStore,
};
use sinomo_cli::app_api::{
    AgentChannelMessagePage, AgentChannelMessageReconciliation, AgentChannelTimelineOverview,
    AgentChannelTimelineSegment, AgentChatSifsStorage, AgentSummary, ChatAttachmentInput,
    ChatAttachmentSource, ChatContextSnapshot, ChatHistoryMessage, ChatLocalAttachmentProvenance,
    ChatProcessSnapshot, ChatQueuedTurnInput, ChatQueuedTurnInputKind,
    ChatSifsAttachmentProvenance, ChatTaskSnapshot, ChatToolCallSnapshot, ChatToolRuntimeSelection,
    ChatUsageSnapshot, ProviderSummary, SifsAgentChannelStore, agent_summaries,
    agent_summaries_with_error, built_in_agent_summaries, reasoning_efforts,
};
use sinomo_runtime::{
    api::{
        DAEMON_EVENT_SUBSCRIBE_ROUTE, DAEMON_RPC_CATEGORY, DaemonApiError, DaemonEventStreamItem,
        DaemonEventSubscribeRequest, send_daemon_rpc_stream_from_data_dir_async,
    },
    config::ConfigClient,
    events::SessionId,
    local::SinomoDataDir,
    network::{DaemonRpcTimeouts, ObjectRpcRequest},
};

use crate::backend::Backend;
use crate::backend::chat_engine::{
    ChatCommand, ChatEvent, ChatEventWake, ChatHandle, ChatRole, RuntimeInfo,
};
use crate::backend::sifs::SifsDaemonAccess;
use crate::native_picker::{
    NativePickerKind, NativePickerLane, NativePickerOutcome, NativePickerResult,
};
use crate::state::{Shared, Tab};
use crate::theme::{AppTextScale, Palette};
use crate::ui::chat_render::{
    self, ChatCodeBlock, ChatMarkdownRender, ChatMarkdownStyles, ChatRichRenderBlock,
};
#[cfg(not(test))]
use crate::ui::media_setup::camera_frame_failure_is_terminal;
use crate::ui::media_setup::{
    MediaAccessKind, MediaAccessState, MediaDeviceSelection, MediaPumpChange, MediaSetupShutdown,
    MediaSetupState, SpeakerTestState,
};
use crate::ui::settings::SettingsSection;
use crate::ui::widgets;

/// Below this content width the sidebar collapses away (transcript + composer only).
const WIDE_THRESHOLD: f32 = 620.0;
const SIDEBAR_INITIAL_RATIO: f32 = 0.20;
const SIDEBAR_MIN_W: f32 = 220.0;
const CHAT_PANE_MIN_W: f32 = 320.0;
const COMPACT_HEADER_H: f32 = 54.0;
// Small media instruments preserve the useful 16:9 preview without consuming transcript space.
const MEDIA_CALL_H: f32 = 352.0;
const MEDIA_CALL_COMPACT_H: f32 = 46.0;
const CHAT_MEDIA_DEVICE_OPEN_TIMEOUT: Duration = Duration::from_secs(65);
#[cfg(not(test))]
static CHAT_MEDIA_CAMERA_OPEN_IN_FLIGHT: AtomicBool = AtomicBool::new(false);
const PAD: f32 = 16.0;
// Agent and conversation rows carry two short lines, so they sit one step above a one-line Small
// data row while remaining denser than a standard content row.
const SIDEBAR_ROW_H: f32 = 36.0;
const SIDEBAR_ROW_EDIT_W: f32 = 26.0;
const SIDEBAR_ROW_EDIT_H: f32 = 24.0;
const SIDEBAR_ROW_EDIT_GAP: f32 = 8.0;
const SELECTOR_H: f32 = 30.0;
const COMPOSER_POPUP_H: f32 = 132.0;
const ATTACHMENT_ENTRY_H: f32 = 30.0;
const ATTACHMENT_CHIPS_H: f32 = 22.0;
const COMPOSER_PAD_Y: f32 = 12.0;
const COMPOSER_SECTION_GAP: f32 = 10.0;
/// Chip rows inside the field ride `padding: 8px 10px 0` (`.sm-ctx-row`, `.sm-composer-queue`).
const COMPOSER_CHIP_ROW_PAD_X: f32 = 10.0;
const COMPOSER_CHIP_ROW_PAD_TOP: f32 = 8.0;
/// The action bar's `padding: 6px 8px` and control gap (`.sm-composer-bar`).
const COMPOSER_BAR_PAD_X: f32 = 8.0;
const COMPOSER_BAR_PAD_Y: f32 = 6.0;
const COMPOSER_BAR_GAP: f32 = 4.0;
/// Quiet bar controls use the Small size; Send remains a Medium primary action.
const COMPOSER_GHOST_BUTTON_SIZE: f32 = 28.0;
const SEND_BUTTON_SIZE: f32 = 32.0;
const COMPOSER_INPUT_MIN_LINES: usize = 2;
/// Keep empty-state actions below the title, guidance, and optional detail line.
const EMPTY_ACTION_OFFSET_Y: f32 = 112.0;
/// The docked composer input grows to 160px, then scrolls (Mesh layout spec).
const COMPOSER_INPUT_MAX_H: f32 = 160.0;
const COMPOSER_INPUT_PAD_Y: f32 = 14.0;
const SESSION_LIST_UI_TIMEOUT: Duration = Duration::from_secs(8);
const TRANSCRIPT_LOAD_UI_TIMEOUT: Duration = Duration::from_secs(8);
/// Budget for the background SIFS-backed agent-catalog load; past this the app
/// keeps the built-in agents and reports the catalog as degraded.
const AGENT_CATALOG_LOAD_TIMEOUT: Duration = Duration::from_secs(10);
const TOOL_CHAIN_COLLAPSE_AFTER: usize = 1;
const TOOL_DETAIL_COLLAPSE_CHARS: usize = 420;
const TOOL_DETAIL_PREVIEW_CHARS: usize = 220;
const VISIBLE_TRANSCRIPT_RECENT_LIMIT: usize = AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT;
const TRANSCRIPT_PAGE_SIZE: usize = AGENT_CHANNEL_RECENT_TRANSCRIPT_LIMIT;
const TRANSCRIPT_MAX_LOADED_MESSAGES: usize = TRANSCRIPT_PAGE_SIZE * 3;
const TRANSCRIPT_OLDER_LOAD_THRESHOLD_PX: f32 = 72.0;
const TRANSCRIPT_PREPEND_ESTIMATED_MESSAGE_H: f32 = 76.0;

/// A shared closure that resolves the current widget theme — threaded into every control so the
/// Appearance toggle restyles the whole tab live, with no rebuild. (Same pattern as the other tabs.)
type ThemeReader = Rc<dyn Fn() -> DefaultTheme>;

// ---------------------------------------------------------------------------
// State (data layer) — the worker, send/stream, provider/model fetch, and the
// per-event reducer are UNCHANGED from the hand-painted version. The composed
// view drives state exclusively through the small action methods at the bottom
// of this block (so callbacks stay a clean clone-and-mutate).
// ---------------------------------------------------------------------------

/// One message in the visible transcript.
#[derive(Clone, Debug)]
pub struct ChatMessage {
    pub role: ChatRole,
    pub text: String,
    pub source: ChatMessageSource,
    pub kind: ChatMessageKind,
    /// Stable identity of a durable SIFS channel message. Local streaming and
    /// placeholder rows have no id until they are reloaded from the store.
    pub message_id: Option<String>,
    sequence: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChatMessageSource {
    User,
    Model,
    System,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChatMessageKind {
    Message,
    Info,
    Error,
}

impl ChatMessage {
    pub fn user(text: impl Into<String>) -> Self {
        Self {
            role: ChatRole::User,
            text: text.into(),
            source: ChatMessageSource::User,
            kind: ChatMessageKind::Message,
            message_id: None,
            sequence: None,
        }
    }

    pub fn model(text: impl Into<String>) -> Self {
        Self {
            role: ChatRole::Agent,
            text: text.into(),
            source: ChatMessageSource::Model,
            kind: ChatMessageKind::Message,
            message_id: None,
            sequence: None,
        }
    }

    pub fn system_info(text: impl Into<String>) -> Self {
        Self {
            role: ChatRole::Agent,
            text: text.into(),
            source: ChatMessageSource::System,
            kind: ChatMessageKind::Info,
            message_id: None,
            sequence: None,
        }
    }

    pub fn system_error(text: impl Into<String>) -> Self {
        Self {
            role: ChatRole::Agent,
            text: text.into(),
            source: ChatMessageSource::System,
            kind: ChatMessageKind::Error,
            message_id: None,
            sequence: None,
        }
    }

    fn with_durable_identity(mut self, message_id: impl Into<String>, sequence: u64) -> Self {
        self.message_id = Some(message_id.into());
        self.sequence = Some(sequence);
        self
    }

    #[cfg(test)]
    fn with_sequence(mut self, sequence: u64) -> Self {
        self.sequence = Some(sequence);
        self
    }

    fn history_role(&self) -> &'static str {
        match self.source {
            ChatMessageSource::User => "user",
            ChatMessageSource::Model => "agent",
            ChatMessageSource::System => "system",
        }
    }

    fn is_model_placeholder(&self) -> bool {
        self.role == ChatRole::Agent
            && self.source == ChatMessageSource::Model
            && self.text.trim().is_empty()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ActivityTone {
    Neutral,
    Running,
    Good,
    Warn,
    Bad,
}

#[derive(Clone, Debug)]
pub struct ChatActivityEntry {
    pub title: String,
    pub detail: String,
    pub tone: ActivityTone,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum RuntimeRecoveryTarget {
    Providers,
    Connection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecoveryCardAction {
    RetryChatList,
    CheckSifs,
    OpenConnectionSettings,
    OpenProviderSetup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChatQuickAction {
    ManageChannel,
    NewChannel,
}

impl RecoveryCardAction {
    fn label(self) -> &'static str {
        match self {
            RecoveryCardAction::RetryChatList => "Retry now",
            RecoveryCardAction::CheckSifs => "Check SIFS",
            RecoveryCardAction::OpenConnectionSettings => "Connection settings",
            RecoveryCardAction::OpenProviderSetup => "Provider setup",
        }
    }

    fn icon(self) -> IconGlyph {
        match self {
            RecoveryCardAction::RetryChatList => IconGlyph::Redo,
            RecoveryCardAction::CheckSifs => IconGlyph::Storage,
            RecoveryCardAction::OpenConnectionSettings => IconGlyph::Alert,
            RecoveryCardAction::OpenProviderSetup => IconGlyph::Add,
        }
    }

    fn description(self) -> &'static str {
        match self {
            RecoveryCardAction::RetryChatList => "Retry the chat list",
            RecoveryCardAction::CheckSifs => "Open SIFS browser",
            RecoveryCardAction::OpenConnectionSettings => "Connection settings",
            RecoveryCardAction::OpenProviderSetup => "Provider setup",
        }
    }
}

impl ChatQuickAction {
    const CHAT: [Self; 0] = [];
    const CHANNEL: [Self; 1] = [Self::ManageChannel];
    const CHANNEL_ONBOARDING: [Self; 1] = [Self::NewChannel];

    fn label(self) -> &'static str {
        match self {
            Self::ManageChannel => "Manage channel",
            Self::NewChannel => "New channel",
        }
    }

    fn icon(self) -> IconGlyph {
        match self {
            Self::ManageChannel => IconGlyph::Chat,
            Self::NewChannel => IconGlyph::Add,
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::ManageChannel => "Open channel agents and settings",
            Self::NewChannel => "Create a shared channel",
        }
    }
}

fn quick_actions_for_chat(chat: &ChatState) -> &'static [ChatQuickAction] {
    if chat.show_channel_onboarding() {
        return &ChatQuickAction::CHANNEL_ONBOARDING;
    }
    if chat.current_is_channel() {
        &ChatQuickAction::CHANNEL
    } else {
        &ChatQuickAction::CHAT
    }
}

fn activate_transcript_quick_action(shared: &Shared, action: ChatQuickAction) {
    if action == ChatQuickAction::NewChannel {
        let waker = { shared.borrow().waker.clone() };
        shared.chat_mut().new_channel(waker);
    } else {
        shared.chat_mut().apply_quick_action(action);
    }
}

fn activate_transcript_recovery_action(shared: &Shared, action: RecoveryCardAction) {
    match action {
        RecoveryCardAction::RetryChatList => {
            let waker = { shared.borrow().waker.clone() };
            shared.chat_mut().retry_chat_list_with_waker(waker);
        }
        RecoveryCardAction::CheckSifs => open_files_tab(shared),
        RecoveryCardAction::OpenConnectionSettings => {
            open_settings_section(shared, SettingsSection::Connection)
        }
        RecoveryCardAction::OpenProviderSetup => open_provider_setup(shared),
    }
}

fn activate_transcript_runtime_action(shared: &Shared, action: &RuntimeTranscriptAction) {
    match action {
        RuntimeTranscriptAction::ToggleToolChain => {
            shared.chat_mut().toggle_tool_chain_expanded();
        }
        RuntimeTranscriptAction::ToggleToolDetail(key) => {
            shared.chat_mut().toggle_tool_detail(key.clone());
        }
    }
}

impl RuntimeRecoveryTarget {
    fn key(self) -> &'static str {
        match self {
            RuntimeRecoveryTarget::Providers => "providers",
            RuntimeRecoveryTarget::Connection => "connection",
        }
    }

    fn footer_label(self) -> &'static str {
        match self {
            RuntimeRecoveryTarget::Providers => "Provider setup",
            RuntimeRecoveryTarget::Connection => "Connection",
        }
    }

    fn footer_min_width(self) -> f32 {
        match self {
            RuntimeRecoveryTarget::Providers => 142.0,
            RuntimeRecoveryTarget::Connection => 116.0,
        }
    }

    fn empty_title(self) -> &'static str {
        match self {
            RuntimeRecoveryTarget::Providers => "Provider setup required",
            RuntimeRecoveryTarget::Connection => "Chat needs connection attention",
        }
    }

    fn empty_hint(self) -> &'static str {
        match self {
            RuntimeRecoveryTarget::Providers => {
                "Add credentials or choose an available model from provider setup."
            }
            RuntimeRecoveryTarget::Connection => {
                "Retry now. If the problem continues, inspect SIFS from Files or open Connection settings."
            }
        }
    }

    fn settings_section(self) -> SettingsSection {
        match self {
            RuntimeRecoveryTarget::Providers => SettingsSection::Providers,
            RuntimeRecoveryTarget::Connection => SettingsSection::Connection,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ChatControlOverview {
    pub active_turn: bool,
    pub task_count: usize,
    pub terminal_result_count: usize,
    pub steering_count: usize,
    pub follow_up_count: usize,
    pub runtime_attached: bool,
    pub tasks: Vec<ChatTaskSnapshot>,
}

#[derive(Clone, Debug)]
pub struct ChatMediaCallState {
    pub active: bool,
    pub mic_enabled: bool,
    pub camera_enabled: bool,
    pub provider: String,
    pub model: String,
    pub started_at: Option<Instant>,
    pub input_audio_chunks: u64,
    pub input_video_frames: u64,
    pub output_audio_chunks: u64,
    pub microphone_level: f32,
    pub notice: Option<String>,
}

#[cfg_attr(test, allow(dead_code))]
struct ChatNativeMediaRuntime {
    control_tx: mpsc::Sender<ChatMediaCallControl>,
    event_rx: Receiver<ChatMediaCallWorkerEvent>,
    snapshots: Arc<Mutex<ChatMediaSnapshots>>,
    cancelled: Arc<AtomicBool>,
    microphone_enabled: Arc<AtomicBool>,
    camera_enabled: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

#[cfg_attr(test, allow(dead_code))]
impl ChatNativeMediaRuntime {
    fn send_control(&self, control: ChatMediaCallControl) {
        match &control {
            ChatMediaCallControl::SetMicEnabled(enabled) => {
                self.microphone_enabled.store(*enabled, Ordering::Release);
            }
            ChatMediaCallControl::SetCameraEnabled(enabled) => {
                self.camera_enabled.store(*enabled, Ordering::Release);
            }
            ChatMediaCallControl::Stop => {
                self.cancelled.store(true, Ordering::Release);
            }
        }
        let _ = self.control_tx.send(control);
    }
}

impl Drop for ChatNativeMediaRuntime {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
        let _ = self.control_tx.send(ChatMediaCallControl::Stop);
        if let Some(worker) = self.worker.take() {
            if worker.is_finished() {
                let _ = worker.join();
            } else {
                let _ = thread::Builder::new()
                    .name("sinomo-app-media-call-reaper".to_string())
                    .spawn(move || {
                        let _ = worker.join();
                    });
            }
        }
    }
}

#[cfg_attr(test, allow(dead_code))]
#[derive(Clone, Debug)]
enum ChatMediaCallControl {
    Stop,
    SetMicEnabled(bool),
    SetCameraEnabled(bool),
}

#[cfg_attr(test, allow(dead_code))]
#[derive(Debug)]
enum ChatMediaCallWorkerEvent {
    Ready(String),
    DeviceAccess(MediaAccessKind, MediaAccessState),
    Error(String),
    Done(String),
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ChatMediaControlDrain {
    stop: bool,
    microphone_enable_requested: bool,
    microphone_disable_requested: bool,
    camera_enable_requested: bool,
    camera_disable_requested: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ChatMediaDeviceTransition {
    release: bool,
    mark_idle: bool,
    open: bool,
}

const CHAT_MEDIA_AUDIO_SEND_QUEUE_CAPACITY: usize = 4;
#[cfg(not(test))]
const CHAT_MEDIA_SESSION_EVENT_QUEUE_CAPACITY: usize = 64;

/// Bounded-by-construction handoff for remote video. Local preview publication
/// never waits for the websocket; a slow sender consumes the newest frame and
/// any superseded frame is dropped before it can add latency.
struct ChatMediaLatestVideoSlot<T> {
    frame: Mutex<Option<T>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChatMediaAudioQueueOutcome {
    Queued,
    DroppedBackpressure,
}

fn chat_media_audio_queue_outcome<T>(
    result: Result<(), tokio::sync::mpsc::error::TrySendError<T>>,
) -> Result<ChatMediaAudioQueueOutcome, String> {
    match result {
        Ok(()) => Ok(ChatMediaAudioQueueOutcome::Queued),
        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
            Ok(ChatMediaAudioQueueOutcome::DroppedBackpressure)
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
            Err("realtime session sender stopped".to_string())
        }
    }
}

impl<T> Default for ChatMediaLatestVideoSlot<T> {
    fn default() -> Self {
        Self {
            frame: Mutex::new(None),
        }
    }
}

impl<T> ChatMediaLatestVideoSlot<T> {
    fn replace(&self, frame: T) {
        if let Ok(mut latest) = self.frame.lock() {
            *latest = Some(frame);
        }
    }

    fn take(&self) -> Option<T> {
        self.frame.lock().ok()?.take()
    }
}

struct ChatMediaRemoteVideoCadence {
    frame_interval: Duration,
    next_send_at: Option<Instant>,
}

impl ChatMediaRemoteVideoCadence {
    fn new(frame_interval: Duration) -> Self {
        Self {
            frame_interval,
            next_send_at: None,
        }
    }

    fn is_due(&self, now: Instant) -> bool {
        self.next_send_at.is_none_or(|next| now >= next)
    }

    fn mark_sent(&mut self, now: Instant) {
        self.next_send_at = Some(now + self.frame_interval);
    }
}

impl Default for ChatMediaRemoteVideoCadence {
    fn default() -> Self {
        let interval = sinomo_lm::RealtimeVideoInputConfig::default()
            .model_frame_interval()
            .expect("default realtime video frame rate is bounded");
        Self::new(interval)
    }
}

#[cfg(not(test))]
enum ChatRealtimeSessionEvent {
    Realtime(sinomo_lm::RealtimeAudioEvent),
    AudioSent,
    VideoSent,
    Error(String),
}

#[cfg(not(test))]
struct ChatRealtimeSessionRuntime {
    audio_tx: tokio::sync::mpsc::Sender<sinomo_lm::RealtimeAudioInputChunk>,
    event_rx: tokio::sync::mpsc::Receiver<ChatRealtimeSessionEvent>,
    latest_video: Arc<ChatMediaLatestVideoSlot<Arc<sinomo_video::NativeVideoSurfaceFrame>>>,
    stop: Arc<AtomicBool>,
    task: Option<tokio::task::JoinHandle<()>>,
}

#[cfg(not(test))]
impl ChatRealtimeSessionRuntime {
    fn spawn(session: sinomo_lm::RealtimeAudioSession) -> Self {
        let (audio_tx, audio_rx) = tokio::sync::mpsc::channel(CHAT_MEDIA_AUDIO_SEND_QUEUE_CAPACITY);
        let (event_tx, event_rx) =
            tokio::sync::mpsc::channel(CHAT_MEDIA_SESSION_EVENT_QUEUE_CAPACITY);
        let latest_video = Arc::new(ChatMediaLatestVideoSlot::default());
        let worker_video = latest_video.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let video_frame_interval = session
            .config()
            .input_video
            .as_ref()
            .and_then(sinomo_lm::RealtimeVideoInputConfig::model_frame_interval)
            .unwrap_or_else(|| Duration::from_millis(500));
        let task = tokio::spawn(run_chat_realtime_session_pump(
            session,
            audio_rx,
            worker_video,
            event_tx,
            worker_stop,
            video_frame_interval,
        ));
        Self {
            audio_tx,
            event_rx,
            latest_video,
            stop,
            task: Some(task),
        }
    }

    fn try_send_audio(
        &self,
        input: sinomo_lm::RealtimeAudioInputChunk,
    ) -> Result<ChatMediaAudioQueueOutcome, String> {
        chat_media_audio_queue_outcome(self.audio_tx.try_send(input))
    }

    fn replace_video(&self, frame: Arc<sinomo_video::NativeVideoSurfaceFrame>) {
        self.latest_video.replace(frame);
    }

    fn try_receive_event(&mut self) -> Result<Option<ChatRealtimeSessionEvent>, String> {
        match self.event_rx.try_recv() {
            Ok(event) => Ok(Some(event)),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                Err("realtime session pump stopped".to_string())
            }
        }
    }

    async fn shutdown(&mut self) {
        self.stop.store(true, Ordering::Release);
        let Some(mut task) = self.task.take() else {
            return;
        };
        if tokio::time::timeout(Duration::from_millis(250), &mut task)
            .await
            .is_err()
        {
            task.abort();
            let _ = task.await;
        }
    }
}

#[cfg(not(test))]
impl Drop for ChatRealtimeSessionRuntime {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn chat_media_requested_input_available(
    microphone_requested: bool,
    microphone_open: bool,
    camera_requested: bool,
    camera_open: bool,
) -> bool {
    (microphone_requested && microphone_open)
        || (camera_requested && camera_open)
        || (!microphone_requested && !camera_requested)
}

fn chat_media_device_transition(
    enabled: bool,
    is_open: bool,
    enable_requested: bool,
    disable_requested: bool,
) -> ChatMediaDeviceTransition {
    let release = is_open && (!enabled || disable_requested);
    ChatMediaDeviceTransition {
        release,
        mark_idle: disable_requested || release,
        open: enabled && enable_requested && (!is_open || release),
    }
}

#[derive(Default)]
struct ChatMediaSnapshots {
    input_audio_chunks: u64,
    input_video_frames: u64,
    output_audio_chunks: u64,
    microphone_level: Option<f32>,
    camera_surface_revision: Option<u64>,
    notice: Option<String>,
}

impl ChatMediaSnapshots {
    fn take(&mut self) -> Self {
        Self {
            input_audio_chunks: std::mem::take(&mut self.input_audio_chunks),
            input_video_frames: std::mem::take(&mut self.input_video_frames),
            output_audio_chunks: std::mem::take(&mut self.output_audio_chunks),
            microphone_level: self.microphone_level.take(),
            camera_surface_revision: self.camera_surface_revision.take(),
            notice: self.notice.take(),
        }
    }

    fn is_empty(&self) -> bool {
        self.input_audio_chunks == 0
            && self.input_video_frames == 0
            && self.output_audio_chunks == 0
            && self.microphone_level.is_none()
            && self.camera_surface_revision.is_none()
            && self.notice.is_none()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ChatMediaCallStartConfig {
    devices: MediaDeviceSelection,
    mic_enabled: bool,
    camera_enabled: bool,
}

impl Default for ChatMediaCallState {
    fn default() -> Self {
        Self {
            active: false,
            mic_enabled: true,
            camera_enabled: true,
            provider: String::new(),
            model: String::new(),
            started_at: None,
            input_audio_chunks: 0,
            input_video_frames: 0,
            output_audio_chunks: 0,
            microphone_level: 0.0,
            notice: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ChatToolActivity {
    pub step_index: usize,
    pub call_index: Option<usize>,
    pub call_id: Option<String>,
    pub tool: String,
    pub input: String,
    pub input_preview: String,
    pub status: String,
    pub process_command: Option<String>,
    pub process_status: Option<String>,
    pub exit_code: Option<i32>,
    pub elapsed_ms: Option<u64>,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum ToolDetailSection {
    Input,
    Output,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RuntimeHealth {
    label: &'static str,
    detail: String,
    tone: ActivityTone,
}

/// A previously-created chat or channel reference (sidebar list).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionRef {
    pub id: String,
    pub title: String,
    pub kind: ConversationKind,
    pub updated_unix: i64,
    pub participant_count: usize,
    pub participant_names: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConversationKind {
    Chat,
    Channel,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum SessionKindFilter {
    All,
    Chats,
    Channels,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChannelFilterEmptyState {
    Loading,
    Empty,
}

impl SessionKindFilter {
    const ALL: [Self; 3] = [Self::All, Self::Chats, Self::Channels];

    fn label(self) -> &'static str {
        match self {
            Self::All => "All",
            Self::Chats => "Chats",
            Self::Channels => "Channels",
        }
    }

    fn heading(self) -> &'static str {
        match self {
            Self::All => "CONVERSATIONS",
            Self::Chats => "CHATS",
            Self::Channels => "CHANNELS",
        }
    }

    fn semantic_name(self) -> &'static str {
        match self {
            Self::All => "Show all conversations",
            Self::Chats => "Show chats only",
            Self::Channels => "Show channels only",
        }
    }

    fn matches(self, kind: ConversationKind) -> bool {
        match self {
            Self::All => true,
            Self::Chats => kind == ConversationKind::Chat,
            Self::Channels => kind == ConversationKind::Channel,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ChannelAgentRef {
    participant_id: String,
    agent_name: String,
    agent_key: String,
    pending_store: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ChannelAgentsStoreStatus {
    Saving,
    Failed(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingChatAttachment {
    source: ChatAttachmentSource,
    file_name: String,
    size_bytes: u64,
}

impl PendingChatAttachment {
    fn input(&self) -> ChatAttachmentInput {
        ChatAttachmentInput {
            file_name: self.file_name.clone(),
            source: self.source.clone(),
        }
    }

    fn has_source(&self, source: &ChatAttachmentSource) -> bool {
        match (&self.source, source) {
            (ChatAttachmentSource::LocalPath(left), ChatAttachmentSource::LocalPath(right)) => {
                left == right
            }
            (
                ChatAttachmentSource::LocalSnapshot {
                    provenance: left, ..
                },
                ChatAttachmentSource::LocalSnapshot {
                    provenance: right, ..
                },
            ) => left.source_path() == right.source_path(),
            (
                ChatAttachmentSource::SifsSnapshot {
                    provenance: left, ..
                },
                ChatAttachmentSource::SifsSnapshot {
                    provenance: right, ..
                },
            ) => left.path == right.path,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingAttachmentPicker {
    request_id: u64,
    session_id: String,
    draft_generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocalAttachmentIngress {
    NativePicker,
    ManualPath,
    OsDrop,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingLocalAttachmentValidation {
    request_id: u64,
    session_id: String,
    draft_generation: u64,
    ingress: LocalAttachmentIngress,
    paths: Vec<PathBuf>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LocalAttachmentValidationOutcome {
    Ready {
        path: PathBuf,
        file_name: String,
        size_bytes: u64,
    },
    Failed {
        path: PathBuf,
        detail: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LocalAttachmentValidationResult {
    request_id: u64,
    session_id: String,
    draft_generation: u64,
    outcomes: Vec<LocalAttachmentValidationOutcome>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum ComposerPopup {
    Model,
    Context,
    Attachments,
    Activity,
}

/// How a mid-run composer submit rides into the active turn (Mesh: "steer, don't just stop").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QueuedTurnKind {
    /// Consumed by the next model step of the running turn.
    Steer,
    /// Runs as a fresh user turn once the active one completes.
    FollowUp,
}

impl QueuedTurnKind {
    fn label(self) -> &'static str {
        match self {
            QueuedTurnKind::Steer => "steer",
            QueuedTurnKind::FollowUp => "follow-up",
        }
    }
}

impl From<ChatQueuedTurnInputKind> for QueuedTurnKind {
    fn from(kind: ChatQueuedTurnInputKind) -> Self {
        match kind {
            ChatQueuedTurnInputKind::Steering => QueuedTurnKind::Steer,
            ChatQueuedTurnInputKind::FollowUp => QueuedTurnKind::FollowUp,
        }
    }
}

/// A message queued into the active turn and not yet consumed by the runtime.
/// Rendered as pending chips above the input; the transcript echo is the
/// durable record, the chip is the "I already told it" status.
#[derive(Clone, Debug, PartialEq, Eq)]
struct QueuedTurnInput {
    kind: QueuedTurnKind,
    text: String,
    /// The runtime queue id, once a control status confirms it. Removal keys
    /// on the id, so the chip's remove affordance appears when this lands.
    input_id: Option<String>,
}

impl From<&ChatQueuedTurnInput> for QueuedTurnInput {
    fn from(queued: &ChatQueuedTurnInput) -> Self {
        QueuedTurnInput {
            kind: queued.kind.into(),
            text: queued.message.clone(),
            input_id: Some(queued.input_id.clone()),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct ConversationTimelineState {
    total_messages: u64,
    chunk_size: u64,
    loaded_start_sequence: Option<u64>,
    loaded_end_sequence: Option<u64>,
    has_older: bool,
    has_newer: bool,
    overview: Vec<ConversationTimelineSegment>,
    discovered_message_ids: BTreeSet<String>,
}

impl ConversationTimelineState {
    fn apply_page(&mut self, page: &AgentChannelMessagePage) {
        self.total_messages = page.total_messages;
        self.chunk_size = page.chunk_size;
        self.loaded_start_sequence = page.start_sequence;
        self.loaded_end_sequence = page.end_sequence;
        self.has_older = page.has_older;
        self.has_newer = page.has_newer;
    }

    fn apply_older_page(&mut self, page: &AgentChannelMessagePage) {
        self.total_messages = page.total_messages;
        self.chunk_size = page.chunk_size;
        if let Some(start) = page.start_sequence {
            self.loaded_start_sequence = Some(start);
        }
        if self.loaded_end_sequence.is_none() {
            self.loaded_end_sequence = page.end_sequence;
        }
        self.has_older = page.has_older;
        self.has_newer = self
            .loaded_end_sequence
            .is_some_and(|sequence| sequence < page.total_messages);
    }

    fn apply_newer_page(&mut self, page: &AgentChannelMessagePage) {
        self.total_messages = page.total_messages;
        self.chunk_size = page.chunk_size;
        if self.loaded_start_sequence.is_none() {
            self.loaded_start_sequence = page.start_sequence;
        }
        if let Some(end) = page.end_sequence {
            self.loaded_end_sequence = Some(end);
        }
        self.has_older = self
            .loaded_start_sequence
            .is_some_and(|sequence| sequence > 1);
        self.has_newer = page.has_newer;
    }

    fn refresh_loaded_range_from_messages(&mut self, messages: &[ChatMessage]) {
        self.loaded_start_sequence = messages.iter().find_map(|message| message.sequence);
        self.loaded_end_sequence = messages.iter().rev().find_map(|message| message.sequence);
        self.has_older = self
            .loaded_start_sequence
            .is_some_and(|sequence| sequence > 1);
        self.has_newer = self
            .loaded_end_sequence
            .is_some_and(|sequence| sequence < self.total_messages);
    }

    fn apply_overview(&mut self, overview: AgentChannelTimelineOverview) {
        self.total_messages = overview.total_messages;
        self.chunk_size = overview.chunk_size;
        self.overview = overview
            .segments
            .into_iter()
            .map(ConversationTimelineSegment::from)
            .collect();
    }

    fn visible_summary(&self, visible_count: usize) -> Option<String> {
        if self.total_messages == 0 || visible_count == 0 {
            return None;
        }
        let start = self.loaded_start_sequence?;
        let end = self.loaded_end_sequence?;
        let mut summary = format!("Messages {start}-{end} of {}", self.total_messages);
        if let (Some(first), Some(last)) = (self.overview.first(), self.overview.last()) {
            let indexed_messages = self
                .overview
                .iter()
                .map(|segment| segment.message_count)
                .sum::<u64>();
            summary.push_str(&format!(
                " · {} section{}",
                self.overview.len(),
                if self.overview.len() == 1 { "" } else { "s" }
            ));
            if indexed_messages != self.total_messages {
                summary.push_str(&format!(" · indexed {indexed_messages}"));
            }
            if first.start_sequence != start || last.end_sequence != end {
                summary.push_str(&format!(
                    " · span {}-{}",
                    first.start_sequence, last.end_sequence
                ));
            }
            if let (Some(first_time), Some(last_time)) =
                (first.first_created_unix, last.last_created_unix)
                && last_time > first_time
            {
                summary.push_str(" · long range");
            }
            if let Some(preview) = self
                .overview
                .iter()
                .find_map(|segment| segment.preview.as_ref())
            {
                summary.push_str(" · ");
                summary.push_str(&compact_line(preview, 44));
            }
        }
        if self.has_older && self.has_newer {
            summary.push_str(" · older/newer available");
        } else if self.has_older {
            summary.push_str(" · older available");
        } else if self.has_newer {
            summary.push_str(" · newer available");
        }
        Some(summary)
    }
}

#[derive(Clone, Debug)]
struct ConversationTimelineSegment {
    start_sequence: u64,
    end_sequence: u64,
    message_count: u64,
    first_created_unix: Option<i64>,
    last_created_unix: Option<i64>,
    preview: Option<String>,
}

impl From<AgentChannelTimelineSegment> for ConversationTimelineSegment {
    fn from(segment: AgentChannelTimelineSegment) -> Self {
        Self {
            start_sequence: segment.start_sequence,
            end_sequence: segment.end_sequence,
            message_count: segment.message_count,
            first_created_unix: segment.first_created_unix,
            last_created_unix: segment.last_created_unix,
            preview: segment.preview,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TranscriptLoadMode {
    Initial,
    Older {
        before_sequence: u64,
    },
    Newer {
        after_sequence: u64,
    },
    /// Re-read the bounded durable tail and reconcile it by stable message id.
    /// Numeric cursors alone can miss a delayed record that is resequenced into
    /// an already-loaded range.
    ReconcileLatest,
}

struct ConversationTimelineLoad {
    messages: Vec<ChatMessage>,
    page: AgentChannelMessagePage,
    overview: Option<AgentChannelTimelineOverview>,
    discovered_message_ids: Vec<String>,
}

/// State backing the chat tab.
pub struct ChatState {
    pub data_dir: Arc<SinomoDataDir>,
    daemon_address: Option<String>,
    sifs_daemon: Option<SifsDaemonAccess>,
    daemon_event_rx: Receiver<DaemonEventWorkerUpdate>,
    daemon_event_abort: Option<tokio::task::AbortHandle>,
    ui_waker: Option<sui::UiHandle>,
    pub handle: ChatHandle,
    pub session_id: String,
    pub info: RuntimeInfo,
    pub tool_runtime: ChatToolRuntimeSelection,
    workspace_path_input: String,
    workspace_error: Option<String>,
    pub messages: Vec<ChatMessage>,
    pub input: String,
    pub cursor: usize,
    draft_revision: u64,
    attachment_draft_generation: u64,
    attachment_path_input: String,
    attachment_manual_path_open: bool,
    attachment_controls_open: bool,
    attachment_picker: NativePickerLane,
    pending_attachment_picker: Option<PendingAttachmentPicker>,
    next_attachment_validation_request_id: u64,
    pending_attachment_validations: Vec<PendingLocalAttachmentValidation>,
    attachment_validation_tx: Sender<LocalAttachmentValidationResult>,
    attachment_validation_rx: Receiver<LocalAttachmentValidationResult>,
    attachment_picker_error: Option<String>,
    attachment_drop_hovered: bool,
    attachments: Vec<PendingChatAttachment>,
    /// Steering/follow-up messages queued into the active turn from this
    /// composer and not yet consumed by the runtime (pending chips).
    queued_turn_inputs: Vec<QueuedTurnInput>,
    pub streaming: bool,
    pub notice: Option<String>,
    pub context: Option<ChatContextSnapshot>,
    pub usage: Option<ChatUsageSnapshot>,
    pub activity: Vec<ChatActivityEntry>,
    pub tool_calls: Vec<ChatToolActivity>,
    timeline: ConversationTimelineState,
    tool_chain_expanded: bool,
    expanded_tool_details: BTreeSet<String>,
    pub control: ChatControlOverview,
    pub media_call: ChatMediaCallState,
    pub media_setup: MediaSetupState,
    media_preview_registration: Arc<MediaPreviewRegistration>,
    media_call_runtime: Option<ChatNativeMediaRuntime>,
    media_call_details_open: bool,
    activity_panel_open: bool,
    composer_popup: Option<ComposerPopup>,
    pub scroll: f32,
    pub stick_bottom: bool,
    pub agents: Vec<AgentSummary>,
    pub providers: Vec<ProviderSummary>,
    providers_loading: bool,
    pub efforts: Vec<String>,
    pub provider_idx: usize,
    pub model_idx: usize,
    pub effort_idx: usize,
    /// True while a provider's model list is being fetched from sinomo-lm.
    pub models_loading: bool,
    pub sessions: Vec<SessionRef>,
    session_filter: String,
    session_kind_filter: SessionKindFilter,
    /// Current first-pane ratio for the desktop conversation sidebar. The SUI
    /// split view owns the active drag; retaining the ratio here preserves the
    /// user's width when the responsive tree is rebuilt for theme scaling.
    sidebar_ratio: f32,
    pending_session_reset: Option<String>,
    session_edit_id: Option<String>,
    session_edit_details_open: bool,
    session_title_draft: String,
    session_delete_confirm_id: Option<String>,
    next_session_rename_request_id: u64,
    pending_session_rename: Option<PendingSessionRename>,
    session_rename_rx: Option<Receiver<SessionRenameWorkerResult>>,
    session_rename_error: Option<SessionRenameError>,
    next_channel_config_request_id: u64,
    pending_channel_config: Option<PendingChannelConfig>,
    channel_config_rx: Option<Receiver<ChannelConfigWorkerResult>>,
    channel_config_error: Option<ChannelConfigError>,
    pending_delete_id: Option<String>,
    delete_rx: Option<Receiver<DeleteWorkerResult>>,
    channel_create_rx: Option<Receiver<ChannelCreateWorkerResult>>,
    transcript_load_rx: Option<Receiver<TranscriptLoadWorkerResult>>,
    transcript_load_mode: Option<TranscriptLoadMode>,
    transcript_load_started_at: Option<Instant>,
    transcript_load_slow_notice_shown: bool,
    provider_list_rx: Option<Receiver<ProviderListWorkerResult>>,
    agent_list_rx: Option<Receiver<AgentListWorkerResult>>,
    agents_loading: bool,
    session_list_rx: Option<Receiver<SessionListWorkerResult>>,
    session_list_loading: bool,
    session_list_started_at: Option<Instant>,
    session_list_slow_notice_shown: bool,
    session_list_details_open: bool,
    session_list_refresh_queued: bool,
    channel_agent_idx: usize,
    /// Narrow-layout access to the agents/conversations surface. This is kept
    /// separately from session-edit state so opening a row editor can replace
    /// the drawer without losing the edit request.
    compact_drawer_open: bool,
}

struct DeleteWorkerResult {
    session: SessionRef,
    kind: ConversationKind,
    result: Result<(), String>,
}

struct ChannelCreateWorkerResult {
    session: SessionRef,
    result: Result<SessionListWorkerResult, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingSessionRename {
    request_id: u64,
    session_id: String,
    kind: ConversationKind,
    title: String,
}

struct SessionRenameWorkerResult {
    request_id: u64,
    session_id: String,
    kind: ConversationKind,
    title: String,
    result: Result<(), String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SessionRenameError {
    session_id: String,
    detail: String,
}

enum ChannelConfigOperation {
    AddAgent {
        agent_key: String,
        agent_name: String,
        tool_runtime: ChatToolRuntimeSelection,
    },
    RemoveAgent {
        participant_id: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingChannelConfig {
    request_id: u64,
    channel_id: String,
    action: String,
}

struct ChannelConfigWorkerResult {
    request_id: u64,
    channel_id: String,
    action: String,
    success_notice: String,
    result: Result<(), String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ChannelConfigError {
    channel_id: String,
    detail: String,
}

struct TranscriptLoadWorkerResult {
    session: SessionRef,
    mode: TranscriptLoadMode,
    result: Result<ConversationTimelineLoad, String>,
}

struct SessionListWorkerResult {
    sessions: Vec<SessionRef>,
    error: Option<String>,
}

enum DaemonEventWorkerUpdate {
    Item(DaemonEventStreamItem),
    Disconnected(String),
}

struct ProviderListWorkerResult {
    providers: Vec<ProviderSummary>,
}

struct AgentListWorkerResult {
    /// `None` when the load timed out (built-ins stay in place).
    agents: Option<Vec<AgentSummary>>,
    error: Option<String>,
}

impl ChatState {
    pub fn new(backend: &Backend) -> Self {
        Self::new_with_waker(backend, None)
    }

    pub fn new_with_waker(backend: &Backend, waker: Option<sui::UiHandle>) -> Self {
        let daemon_connected = backend.connection.is_daemon();
        let daemon_address = backend.connection.daemon_address().map(str::to_string);
        let sifs_storage = daemon_address
            .as_deref()
            .map(|address| AgentChatSifsStorage::new(address, backend.app_runtime.handle()));
        let event_wake: Option<ChatEventWake> = waker.as_ref().map(|waker| {
            let waker = waker.clone();
            Arc::new(move || waker.wake()) as ChatEventWake
        });
        let handle = ChatHandle::spawn(
            (*backend.data_dir).clone(),
            daemon_connected,
            backend.agent_runtime_control.clone(),
            sifs_storage,
            event_wake,
        );

        // The interactive app must not read the SIFS-backed agent catalog here: this
        // constructor runs on the connect thread while the window shows the launch
        // status, and SIFS grinds through multi-second distributed timeouts whenever
        // the cluster DFS is unhealthy. Boot with the built-ins (no I/O) and let the
        // shell load the stored catalog in the background once the app is ready.
        let defer_agent_load = waker.is_some();
        let agents = if defer_agent_load {
            built_in_agent_summaries()
        } else {
            agent_summaries(&backend.data_dir)
        };
        let defer_provider_load = waker.is_some() && backend.connection.daemon_address().is_some();
        let providers = if defer_provider_load {
            Vec::new()
        } else {
            crate::backend::config::cluster_provider_summaries(backend.config.as_ref())
        };
        let efforts = reasoning_efforts();
        let effort_idx = efforts.iter().position(|e| e == "medium").unwrap_or(0);
        let sifs_daemon = daemon_address
            .as_deref()
            .map(|address| SifsDaemonAccess::new(address, backend.app_runtime.handle()));
        let media_setup = MediaSetupState::new(
            backend.data_dir.clone(),
            backend.node.node_id.clone(),
            waker.clone(),
        );
        let (sessions, session_list_error) = if sifs_daemon.is_some() {
            (Vec::new(), None)
        } else {
            list_conversations_with_error(&backend.data_dir, None)
        };
        let (attachment_validation_tx, attachment_validation_rx) = mpsc::channel();
        let (daemon_event_tx, daemon_event_rx) = mpsc::channel();
        let daemon_event_abort = daemon_address.as_ref().map(|address| {
            spawn_daemon_event_subscription(
                backend.app_runtime.handle(),
                (*backend.data_dir).clone(),
                address.clone(),
                daemon_event_tx,
                waker.clone(),
            )
        });

        let mut state = Self {
            data_dir: backend.data_dir.clone(),
            daemon_address,
            sifs_daemon,
            daemon_event_rx,
            daemon_event_abort,
            ui_waker: waker,
            handle,
            session_id: String::new(),
            info: RuntimeInfo::default(),
            tool_runtime: ChatToolRuntimeSelection::default(),
            workspace_path_input: String::new(),
            workspace_error: None,
            messages: Vec::new(),
            input: String::new(),
            cursor: 0,
            draft_revision: 0,
            attachment_draft_generation: 0,
            attachment_path_input: String::new(),
            attachment_manual_path_open: false,
            attachment_controls_open: false,
            attachment_picker: NativePickerLane::new(),
            pending_attachment_picker: None,
            next_attachment_validation_request_id: 0,
            pending_attachment_validations: Vec::new(),
            attachment_validation_tx,
            attachment_validation_rx,
            attachment_picker_error: None,
            attachment_drop_hovered: false,
            attachments: Vec::new(),
            queued_turn_inputs: Vec::new(),
            streaming: false,
            notice: None,
            context: None,
            usage: None,
            activity: Vec::new(),
            tool_calls: Vec::new(),
            timeline: ConversationTimelineState::default(),
            tool_chain_expanded: false,
            expanded_tool_details: BTreeSet::new(),
            control: ChatControlOverview::default(),
            media_call: ChatMediaCallState::default(),
            media_setup,
            media_preview_registration: Arc::new(MediaPreviewRegistration::default()),
            media_call_runtime: None,
            media_call_details_open: false,
            activity_panel_open: false,
            composer_popup: None,
            scroll: 0.0,
            stick_bottom: true,
            agents,
            providers,
            providers_loading: defer_provider_load,
            efforts,
            provider_idx: 0,
            model_idx: 0,
            effort_idx,
            models_loading: false,
            sessions,
            session_filter: String::new(),
            session_kind_filter: SessionKindFilter::All,
            sidebar_ratio: SIDEBAR_INITIAL_RATIO,
            pending_session_reset: None,
            session_edit_id: None,
            session_edit_details_open: false,
            session_title_draft: String::new(),
            session_delete_confirm_id: None,
            next_session_rename_request_id: 0,
            pending_session_rename: None,
            session_rename_rx: None,
            session_rename_error: None,
            next_channel_config_request_id: 0,
            pending_channel_config: None,
            channel_config_rx: None,
            channel_config_error: None,
            pending_delete_id: None,
            delete_rx: None,
            channel_create_rx: None,
            transcript_load_rx: None,
            transcript_load_mode: None,
            transcript_load_started_at: None,
            transcript_load_slow_notice_shown: false,
            provider_list_rx: None,
            agent_list_rx: None,
            agents_loading: defer_agent_load,
            session_list_rx: None,
            session_list_loading: false,
            session_list_started_at: None,
            session_list_slow_notice_shown: false,
            session_list_details_open: false,
            session_list_refresh_queued: false,
            channel_agent_idx: 0,
            compact_drawer_open: false,
        };
        if let Some(detail) = session_list_error {
            state.notice = Some(format!("chat list failed: {detail}"));
            state.replace_activity("chat list", detail, ActivityTone::Warn);
        }
        if let Some(detail) = backend.connection_issue_detail() {
            state.notice = Some(detail.clone());
            state.replace_activity("connection", detail, ActivityTone::Warn);
        }
        // Eagerly fetch the default/initial provider's models so the model picker is populated
        // without requiring a manual provider toggle. (Results arrive async via `ChatEvent::Models`;
        // a no-op if there is no provider or the list is already populated.)
        if !state.providers_loading {
            state.maybe_fetch_models();
        }
        state
    }

    /// Rebind every daemon-backed chat surface after the live connection moves to a new
    /// endpoint. The visible transcript/drafts stay in place; only endpoint-bound workers and
    /// SIFS access are replaced. A same-address health probe is a no-op.
    pub(crate) fn rebind_backend(&mut self, backend: &Backend) -> bool {
        let daemon_address = backend.connection.daemon_address().map(str::to_string);
        if self.daemon_address == daemon_address {
            return false;
        }

        let history = self.current_history();
        let session_id = self.session_id.clone();
        let agent = self.info.agent.clone();
        let provider = self.info.provider.clone();
        let model = self.info.model.clone();
        let account = self
            .info
            .account
            .clone()
            .unwrap_or_else(|| "default".to_string());
        let tool_runtime = self.tool_runtime.clone();
        let sifs_storage = daemon_address
            .as_deref()
            .map(|address| AgentChatSifsStorage::new(address, backend.app_runtime.handle()));
        let event_wake: Option<ChatEventWake> = self.ui_waker.as_ref().map(|waker| {
            let waker = waker.clone();
            Arc::new(move || waker.wake()) as ChatEventWake
        });
        let handle = ChatHandle::spawn(
            (*backend.data_dir).clone(),
            backend.connection.is_daemon(),
            backend.agent_runtime_control.clone(),
            sifs_storage,
            event_wake,
        );

        if let Some(abort) = self.daemon_event_abort.take() {
            abort.abort();
        }
        let (daemon_event_tx, daemon_event_rx) = mpsc::channel();
        let daemon_event_abort = daemon_address.as_ref().map(|address| {
            spawn_daemon_event_subscription(
                backend.app_runtime.handle(),
                (*backend.data_dir).clone(),
                address.clone(),
                daemon_event_tx,
                self.ui_waker.clone(),
            )
        });

        self.handle = handle;
        self.daemon_address = daemon_address.clone();
        self.sifs_daemon = daemon_address
            .map(|address| SifsDaemonAccess::new(address, backend.app_runtime.handle()));
        self.daemon_event_rx = daemon_event_rx;
        self.daemon_event_abort = daemon_event_abort;
        self.streaming = false;
        self.control = ChatControlOverview::default();
        self.models_loading = false;
        self.delete_rx = None;
        self.channel_create_rx = None;
        self.transcript_load_rx = None;
        self.transcript_load_mode = None;
        self.transcript_load_started_at = None;
        self.provider_list_rx = None;
        self.providers_loading = false;
        self.session_list_rx = None;
        self.session_list_loading = false;
        self.session_list_started_at = None;
        self.session_list_refresh_queued = false;
        if let Some(pending) = self.pending_session_rename.take() {
            self.session_rename_error = Some(SessionRenameError {
                session_id: pending.session_id,
                detail: "Connection changed before the rename completed; retry it.".to_string(),
            });
        }
        self.session_rename_rx = None;
        if let Some(pending) = self.pending_channel_config.take() {
            let detail = format!(
                "Connection changed before {} completed; retry the channel update.",
                pending.action
            );
            self.channel_config_error = Some(ChannelConfigError {
                channel_id: pending.channel_id,
                detail: detail.clone(),
            });
            self.notice = Some(format!("channel config failed: {detail}"));
        }
        self.channel_config_rx = None;
        if self
            .notice
            .as_deref()
            .is_some_and(text_mentions_connection_problem)
        {
            self.notice = None;
        }
        self.activity.retain(|entry| {
            !matches!(entry.tone, ActivityTone::Warn | ActivityTone::Bad)
                || !text_mentions_connection_problem(&entry.detail)
        });

        if !session_id.trim().is_empty() {
            if agent.trim().is_empty() {
                self.handle.send_command(ChatCommand::Resume {
                    session_id: session_id.clone(),
                    history: history.clone(),
                });
            } else {
                self.handle.send_command(ChatCommand::SetAgent {
                    session_id: session_id.clone(),
                    key: agent,
                    history: history.clone(),
                });
            }
        }
        if !provider.trim().is_empty() && !model.trim().is_empty() {
            self.handle.send_command(ChatCommand::SetModel {
                provider,
                model,
                account,
            });
        }
        self.handle.send_command(ChatCommand::SetToolRuntimeNode {
            sandbox_node_id: tool_runtime.sandbox_node_id,
        });
        self.replace_activity(
            "connection",
            "Daemon connection restored; chat runtime rebound to the live endpoint",
            ActivityTone::Good,
        );

        let waker = self.ui_waker.clone();
        self.start_provider_list_load(backend.config.clone(), waker.clone());
        self.start_session_list_load(waker);
        true
    }

    fn title_from_messages(messages: &[ChatMessage]) -> String {
        messages
            .iter()
            .find(|m| m.role == ChatRole::User)
            .map(|m| {
                let line = m.text.lines().next().unwrap_or("").trim();
                let title: String = line.chars().take(42).collect();
                if title.is_empty() {
                    "New chat".to_string()
                } else {
                    title
                }
            })
            .unwrap_or_else(|| "New chat".to_string())
    }

    /// Update the local sidebar row after backend-owned chat state changes.
    fn persist(&mut self) {
        if self.initial_transcript_load_pending() {
            return;
        }
        let session_id = self.session_id.clone();
        let messages = self.messages.clone();
        let title = Self::title_from_messages(&messages);
        self.upsert_session_ref_from_messages(&session_id, &title, &messages);
    }

    fn upsert_session_ref_from_messages(
        &mut self,
        session_id: &str,
        fallback_title: &str,
        messages: &[ChatMessage],
    ) {
        if session_id.is_empty() || messages.is_empty() {
            return;
        }
        if let Some(session) =
            self.session_ref_after_backend_update(session_id, fallback_title, messages)
        {
            self.upsert_session_ref(session);
        }
    }

    fn session_ref_after_backend_update(
        &self,
        session_id: &str,
        fallback_title: &str,
        messages: &[ChatMessage],
    ) -> Option<SessionRef> {
        let existing = self
            .sessions
            .iter()
            .find(|session| session.id == session_id);
        if existing.is_some_and(|session| session.kind == ConversationKind::Channel) {
            return None;
        }
        if existing.is_none() && session_id != self.session_id {
            return None;
        }
        Some(SessionRef {
            id: session_id.to_string(),
            title: existing
                .map(|session| session.title.clone())
                .unwrap_or_else(|| fallback_title.to_string()),
            kind: ConversationKind::Chat,
            updated_unix: current_unix_timestamp(),
            participant_count: 1,
            participant_names: Vec::new(),
        })
        .filter(|_| {
            messages
                .iter()
                .any(|message| message.role == ChatRole::User)
        })
    }

    /// Refresh the SIFS-backed conversation catalog on a worker for every connection mode.
    /// Local/fallback SIFS can block on the same storage recovery paths as daemon-backed access,
    /// so it must never be special-cased back onto the UI thread.
    pub fn start_session_list_load(&mut self, waker: Option<sui::UiHandle>) {
        if self.session_list_loading {
            // Coalesce state changes that arrive while the catalog is being read. The current
            // snapshot may have been taken before a just-finished background chat was persisted.
            self.session_list_refresh_queued = true;
            return;
        }
        self.session_list_slow_notice_shown = false;
        self.session_list_details_open = false;
        let (tx, rx) = mpsc::channel();
        let data_dir = self.data_dir.clone();
        let sifs_daemon = self.sifs_daemon.clone();
        let worker_waker = waker.clone();
        let spawn_result = thread::Builder::new()
            .name("sinomo-app-chat-list".to_string())
            .spawn(move || {
                let (sessions, error) =
                    list_conversations_with_error(&data_dir, sifs_daemon.as_ref());
                let _ = tx.send(SessionListWorkerResult { sessions, error });
                if let Some(waker) = worker_waker {
                    waker.wake();
                }
            });

        if let Err(error) = spawn_result {
            let detail = error.to_string();
            self.notice = Some(format!("chat list failed: {detail}"));
            self.replace_activity("chat list", detail, ActivityTone::Warn);
            self.session_list_rx = None;
            self.session_list_loading = false;
            self.session_list_started_at = None;
            self.session_list_slow_notice_shown = false;
            self.session_list_details_open = false;
            self.session_list_refresh_queued = false;
            return;
        }

        self.session_list_rx = Some(rx);
        self.session_list_loading = true;
        self.session_list_started_at = Some(Instant::now());
    }

    #[cfg(test)]
    fn reconcile_after_daemon_event(&mut self) {
        self.start_session_list_load(self.ui_waker.clone());
        if !self.streaming
            && !self.transcript_load_pending()
            && let Some(session) = self.current_session_ref().cloned()
        {
            self.start_transcript_load(session, TranscriptLoadMode::ReconcileLatest);
        }
    }

    fn toggle_session_list_details(&mut self) {
        if self.session_list_loading && self.session_list_slow_notice_shown {
            self.session_list_details_open = !self.session_list_details_open;
        }
    }

    pub fn pump_session_list(&mut self) -> bool {
        let mut changed = false;
        loop {
            let result = match self.session_list_rx.as_ref().map(|rx| rx.try_recv()) {
                Some(Ok(result)) => result,
                Some(Err(TryRecvError::Empty)) => {
                    if changed {
                        return true;
                    }
                    if self.session_list_loading
                        && !self.session_list_slow_notice_shown
                        && self
                            .session_list_started_at
                            .is_some_and(|started| started.elapsed() >= SESSION_LIST_UI_TIMEOUT)
                    {
                        let detail = format!(
                            "SIFS conversation history is still loading after {} seconds",
                            SESSION_LIST_UI_TIMEOUT.as_secs()
                        );
                        self.session_list_slow_notice_shown = true;
                        self.replace_activity("chat list", detail, ActivityTone::Running);
                        return true;
                    }
                    return false;
                }
                None => return changed,
                Some(Err(TryRecvError::Disconnected)) => {
                    self.session_list_rx = None;
                    self.session_list_loading = false;
                    self.session_list_started_at = None;
                    self.session_list_slow_notice_shown = false;
                    self.session_list_details_open = false;
                    self.notice = Some("chat list failed: worker stopped".to_string());
                    self.replace_activity(
                        "chat list",
                        "worker stopped before loading chats from SIFS".to_string(),
                        ActivityTone::Warn,
                    );
                    if std::mem::take(&mut self.session_list_refresh_queued) {
                        self.start_session_list_load(self.ui_waker.clone());
                    }
                    return true;
                }
            };
            changed |= self.apply_session_list_result(result);
            self.session_list_rx = None;
            self.session_list_loading = false;
            self.session_list_started_at = None;
            self.session_list_slow_notice_shown = false;
            self.session_list_details_open = false;
            if std::mem::take(&mut self.session_list_refresh_queued) {
                self.start_session_list_load(self.ui_waker.clone());
            }
            return changed;
        }
    }

    pub fn pump_worker_events(&mut self) -> bool {
        let daemon_events_changed = self.pump_daemon_event_subscription();
        let attachment_picker_changed = self.pump_attachment_lanes();
        let rename_changed = self.pump_session_rename();
        let channel_config_changed = self.pump_channel_config();
        let events = self.handle.drain();
        let changed = !events.is_empty();
        for event in events {
            self.apply_event(event);
        }
        changed
            || daemon_events_changed
            || rename_changed
            || channel_config_changed
            || attachment_picker_changed
    }

    fn pump_daemon_event_subscription(&mut self) -> bool {
        let mut changed = false;
        let mut refresh = false;
        loop {
            match self.daemon_event_rx.try_recv() {
                Ok(DaemonEventWorkerUpdate::Item(DaemonEventStreamItem::Ready { .. })) => {
                    changed |= self
                        .activity
                        .iter()
                        .any(|entry| entry.title == "chat updates");
                    self.activity.retain(|entry| entry.title != "chat updates");
                    refresh = true;
                }
                Ok(DaemonEventWorkerUpdate::Item(DaemonEventStreamItem::Event { event })) => {
                    if event
                        .payload
                        .get("operation")
                        .and_then(serde_json::Value::as_str)
                        != Some("create-dir-all")
                    {
                        refresh = true;
                    }
                }
                Ok(DaemonEventWorkerUpdate::Item(DaemonEventStreamItem::Lagged { .. })) => {
                    refresh = true;
                }
                Ok(DaemonEventWorkerUpdate::Disconnected(detail)) => {
                    let already_reported = self
                        .activity
                        .iter()
                        .any(|entry| entry.title == "chat updates" && entry.detail == detail);
                    if !already_reported {
                        self.replace_activity("chat updates", detail, ActivityTone::Warn);
                        changed = true;
                    }
                }
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }
        if refresh {
            self.start_session_list_load(self.ui_waker.clone());
            changed = true;
        }
        changed
    }

    fn apply_session_list_result(&mut self, result: SessionListWorkerResult) -> bool {
        let SessionListWorkerResult { sessions, error } = result;
        let mut changed = self.sessions != sessions;
        if changed {
            self.sessions = sessions;
        }
        match error {
            Some(detail) => {
                let notice = format!("chat list failed: {detail}");
                changed |= self.notice.as_deref() != Some(notice.as_str())
                    || !self
                        .activity
                        .iter()
                        .any(|entry| entry.title == "chat list" && entry.detail == detail);
                self.notice = Some(notice);
                self.replace_activity("chat list", detail, ActivityTone::Warn);
            }
            None => {
                let cleared_notice = self
                    .notice
                    .as_deref()
                    .is_some_and(|notice| notice.starts_with("chat list failed:"));
                let cleared_activity = self.activity.iter().any(|entry| entry.title == "chat list");
                if cleared_notice {
                    self.notice = None;
                }
                self.activity.retain(|entry| entry.title != "chat list");
                changed |= cleared_notice || cleared_activity;
            }
        }
        if !self.streaming
            && !self.transcript_load_pending()
            && let Some(session) = self.current_session_ref().cloned()
        {
            self.start_transcript_load(session, TranscriptLoadMode::ReconcileLatest);
        }
        changed
    }

    fn upsert_session_ref(&mut self, session: SessionRef) {
        self.sessions.retain(|existing| existing.id != session.id);
        self.sessions.push(session);
        sort_conversations(&mut self.sessions);
    }

    fn reset_session_list_load(&mut self) {
        self.session_list_rx = None;
        self.session_list_loading = false;
        self.session_list_started_at = None;
        self.session_list_slow_notice_shown = false;
        self.session_list_details_open = false;
        self.session_list_refresh_queued = false;
    }

    fn retry_chat_list_with_waker(&mut self, waker: Option<sui::UiHandle>) {
        self.reset_session_list_load();
        if self
            .notice
            .as_deref()
            .is_some_and(|notice| notice.starts_with("chat list failed:"))
        {
            self.notice = None;
        }
        self.activity.retain(|entry| entry.title != "chat list");
        self.start_session_list_load(waker);
    }

    fn agent_channel_store(&self) -> std::io::Result<SifsAgentChannelStore> {
        open_agent_channel_store(&self.data_dir, self.sifs_daemon.as_ref())
    }

    fn channel_agent_refs(&self, id: &str) -> Vec<ChannelAgentRef> {
        let Ok(channel_id) = id.parse::<SessionId>() else {
            return Vec::new();
        };
        let Ok(store) = self.agent_channel_store() else {
            return Vec::new();
        };
        let Ok(Some(channel)) = store.load_channel(&channel_id) else {
            return Vec::new();
        };
        let mut participants = channel.participants.clone();
        participants.sort_by_key(|participant| participant.order);
        participants
            .into_iter()
            .filter(|participant| participant.enabled)
            .map(|participant| ChannelAgentRef {
                participant_id: participant.participant_id.to_string(),
                agent_name: participant.agent_name,
                agent_key: participant.agent_key,
                pending_store: false,
            })
            .collect()
    }

    fn fallback_channel_agent_refs(
        session: &SessionRef,
        agents: &[AgentSummary],
    ) -> Vec<ChannelAgentRef> {
        let mut seen = BTreeSet::new();
        session
            .participant_names
            .iter()
            .filter_map(|name| {
                let name = name.trim();
                if name.is_empty() || !seen.insert(name.to_string()) {
                    return None;
                }
                let agent_key = agents
                    .iter()
                    .find(|agent| agent.name == name || agent.key == name)
                    .map(|agent| agent.key.clone())
                    .unwrap_or_else(|| name.to_string());
                Some(ChannelAgentRef {
                    participant_id: String::new(),
                    agent_name: name.to_string(),
                    agent_key,
                    pending_store: true,
                })
            })
            .collect()
    }

    fn push_activity(
        &mut self,
        title: impl Into<String>,
        detail: impl Into<String>,
        tone: ActivityTone,
    ) {
        self.activity.push(ChatActivityEntry {
            title: title.into(),
            detail: detail.into(),
            tone,
        });
        let excess = self.activity.len().saturating_sub(18);
        if excess > 0 {
            self.activity.drain(0..excess);
        }
    }

    fn replace_activity(
        &mut self,
        title: impl Into<String>,
        detail: impl Into<String>,
        tone: ActivityTone,
    ) {
        let title = title.into();
        self.activity.retain(|entry| entry.title != title);
        self.push_activity(title, detail, tone);
    }

    fn record_tool_call(
        &mut self,
        prefix: &str,
        call: ChatToolCallSnapshot,
        status: &str,
        tone: ActivityTone,
    ) {
        let mut detail = format!("step {}", call.step_index);
        if let Some(call_index) = call.call_index {
            detail.push_str(&format!(" · call {}", call_index + 1));
        }
        if let Some(input) = call.input_preview.clone().filter(|input| !input.is_empty()) {
            detail.push_str(&format!(" · {input}"));
        }
        let tool_title = call.tool.clone();
        self.upsert_tool_activity_from_snapshot(call, status);
        self.push_activity(format!("{prefix} {tool_title}"), detail, tone);
    }

    fn upsert_tool_activity_from_snapshot(&mut self, call: ChatToolCallSnapshot, status: &str) {
        let input = call
            .input
            .clone()
            .or_else(|| call.input_preview.clone())
            .unwrap_or_default();
        let input_preview = call
            .input_preview
            .clone()
            .filter(|preview| !preview.trim().is_empty())
            .unwrap_or_else(|| compact_line(&input, TOOL_DETAIL_PREVIEW_CHARS));
        if let Some(existing) = self.tool_calls.iter_mut().find(|existing| {
            tool_activity_matches(
                existing,
                call.step_index,
                call.call_index,
                call.call_id.as_deref(),
                Some(call.tool.as_str()),
            )
        }) {
            existing.tool = call.tool;
            if !input_preview.trim().is_empty() {
                existing.input_preview = compact_line(&input_preview, 120);
            }
            if !input.trim().is_empty() {
                existing.input = input;
            }
            existing.status = status.to_string();
            return;
        }

        self.tool_calls.push(ChatToolActivity {
            step_index: call.step_index,
            call_index: call.call_index,
            call_id: call.call_id,
            tool: call.tool,
            input,
            input_preview: compact_line(&input_preview, 120),
            status: status.to_string(),
            process_command: None,
            process_status: None,
            exit_code: None,
            elapsed_ms: None,
            stdout: String::new(),
            stderr: String::new(),
        });
        let excess = self.tool_calls.len().saturating_sub(8);
        if excess > 0 {
            self.tool_calls.drain(0..excess);
            self.prune_tool_detail_expanders();
        }
    }

    fn record_tool_delta(
        &mut self,
        step_index: usize,
        call_id: Option<String>,
        tool: Option<String>,
        arguments_delta: &str,
    ) {
        let Some(existing) = self.tool_calls.iter_mut().find(|existing| {
            tool_activity_matches(
                existing,
                step_index,
                None,
                call_id.as_deref(),
                tool.as_deref(),
            )
        }) else {
            return;
        };
        if let Some(tool) = tool.filter(|tool| !tool.trim().is_empty()) {
            existing.tool = tool;
        }
        if !arguments_delta.trim().is_empty() {
            existing.input.push_str(arguments_delta);
            existing.input_preview = compact_line(&existing.input, 120);
        }
        existing.status = "streaming".to_string();
    }

    fn record_process(&mut self, process: ChatProcessSnapshot) {
        let tone = match process.status.as_str() {
            "completed" => ActivityTone::Good,
            "failed" | "cancelled" => ActivityTone::Bad,
            "running" => ActivityTone::Running,
            _ => ActivityTone::Neutral,
        };
        let mut detail = format!("{} ms", process.elapsed_ms);
        if let Some(code) = process.exit_code {
            detail.push_str(&format!(" · exit {code}"));
        }
        if !process.stderr_tail.trim().is_empty() {
            detail.push_str(" · stderr ");
            detail.push_str(process.stderr_tail.trim());
        } else if !process.stdout_tail.trim().is_empty() {
            detail.push_str(" · stdout ");
            detail.push_str(process.stdout_tail.trim());
        }
        self.attach_process_to_tool(&process);
        self.push_activity(process.command, detail, tone);
    }

    fn attach_process_to_tool(&mut self, process: &ChatProcessSnapshot) {
        let index = self
            .tool_calls
            .iter()
            .rposition(|call| tool_process_matches(call, process))
            .or_else(|| self.tool_calls.len().checked_sub(1));
        let Some(index) = index else {
            return;
        };
        let call = &mut self.tool_calls[index];
        call.process_command = Some(process.command.clone());
        call.process_status = Some(process.status.clone());
        call.exit_code = process.exit_code;
        call.elapsed_ms = Some(process.elapsed_ms);
        call.stdout = if process.stdout.is_empty() {
            process.stdout_tail.clone()
        } else {
            process.stdout.clone()
        };
        call.stderr = if process.stderr.is_empty() {
            process.stderr_tail.clone()
        } else {
            process.stderr.clone()
        };
    }

    fn tool_chain_collapsible(&self) -> bool {
        self.tool_calls.len() > TOOL_CHAIN_COLLAPSE_AFTER
    }

    fn tool_chain_expanded(&self) -> bool {
        !self.tool_chain_collapsible() || self.tool_chain_expanded
    }

    fn toggle_tool_chain_expanded(&mut self) {
        if self.tool_chain_collapsible() {
            self.tool_chain_expanded = !self.tool_chain_expanded;
        }
    }

    fn tool_detail_expanded(&self, key: &str) -> bool {
        self.expanded_tool_details.contains(key)
    }

    fn toggle_tool_detail(&mut self, key: String) {
        if !self.expanded_tool_details.remove(&key) {
            self.expanded_tool_details.insert(key);
        }
    }

    fn prune_tool_detail_expanders(&mut self) {
        let live: BTreeSet<String> = self
            .tool_calls
            .iter()
            .flat_map(|call| {
                [
                    tool_detail_key(call, ToolDetailSection::Input),
                    tool_detail_key(call, ToolDetailSection::Output),
                ]
            })
            .collect();
        self.expanded_tool_details
            .retain(|key| live.contains(key.as_str()));
    }

    fn runtime_health(&self) -> RuntimeHealth {
        if let Some(task) = self
            .control
            .tasks
            .iter()
            .find(|task| task_tone(&task.status) == ActivityTone::Bad)
        {
            return RuntimeHealth {
                label: "ATTENTION",
                detail: format!("{}: {}", task.status, compact_line(&task.summary, 72)),
                tone: ActivityTone::Bad,
            };
        }

        if let Some(entry) = self
            .activity
            .iter()
            .rev()
            .find(|entry| entry.tone == ActivityTone::Bad)
        {
            return RuntimeHealth {
                label: "ATTENTION",
                detail: format!(
                    "{}: {}",
                    compact_line(&entry.title, 32),
                    compact_line(&entry.detail, 72)
                ),
                tone: ActivityTone::Bad,
            };
        }

        if let Some(detail) = self.runtime_degraded_detail() {
            return RuntimeHealth {
                label: "DEGRADED",
                detail,
                tone: ActivityTone::Warn,
            };
        }

        if self.streaming || self.control.active_turn {
            return RuntimeHealth {
                label: "RUNNING",
                detail: format!(
                    "{} task(s), {} queued",
                    self.control.task_count,
                    self.control.steering_count + self.control.follow_up_count
                ),
                tone: ActivityTone::Running,
            };
        }

        if let Some(task) = self
            .control
            .tasks
            .iter()
            .find(|task| matches!(task.status.as_str(), "running" | "pending"))
        {
            return RuntimeHealth {
                label: "RUNNING",
                detail: format!("{}: {}", task.status, compact_line(&task.summary, 72)),
                tone: ActivityTone::Running,
            };
        }

        if !self.info.provider.is_empty() && !self.info.model.is_empty() {
            return RuntimeHealth {
                label: "READY",
                detail: format!("{} / {}", self.info.provider, self.info.model),
                tone: ActivityTone::Good,
            };
        }

        if let Some(provider) = self.current_provider() {
            let model = self.current_model();
            if !model.is_empty() {
                return RuntimeHealth {
                    label: "READY",
                    detail: format!("{} / {}", provider.id, model),
                    tone: ActivityTone::Good,
                };
            }
        }

        RuntimeHealth {
            label: "SETUP",
            detail: "choose a provider and model".to_string(),
            tone: ActivityTone::Neutral,
        }
    }

    fn activity_panel_visible(&self) -> bool {
        self.composer_popup == Some(ComposerPopup::Activity)
    }

    fn toggle_activity_panel(&mut self) {
        self.toggle_composer_popup(ComposerPopup::Activity);
    }

    fn close_activity_panel(&mut self) {
        if self.activity_panel_visible() {
            self.set_composer_popup(None);
        }
    }

    fn composer_popup(&self) -> Option<ComposerPopup> {
        self.composer_popup
    }

    fn toggle_composer_popup(&mut self, popup: ComposerPopup) {
        if self.composer_popup == Some(popup) {
            self.set_composer_popup(None);
        } else {
            self.set_composer_popup(Some(popup));
        }
    }

    fn set_composer_popup(&mut self, popup: Option<ComposerPopup>) {
        if self.composer_popup == Some(ComposerPopup::Attachments)
            && popup != Some(ComposerPopup::Attachments)
        {
            let had_path = !self.attachment_path_input.trim().is_empty();
            self.attachment_path_input.clear();
            self.attachment_manual_path_open = false;
            self.attachment_controls_open = false;
            self.attachment_picker_error = None;
            if had_path {
                self.draft_revision = self.draft_revision.wrapping_add(1);
            }
        }
        if popup == Some(ComposerPopup::Attachments) {
            self.attachment_controls_open = true;
        }
        self.activity_panel_open = popup == Some(ComposerPopup::Activity);
        self.composer_popup = popup;
    }

    fn runtime_degraded_detail(&self) -> Option<String> {
        if let Some(notice) = self.notice.as_deref()
            && text_mentions_runtime_problem(notice)
        {
            return Some(compact_line(notice, 96));
        }

        if !self.control.runtime_attached
            && (self.control.active_turn
                || self.control.task_count > 0
                || self.control.terminal_result_count > 0
                || self.control.steering_count > 0
                || self.control.follow_up_count > 0)
        {
            return Some("runtime control metadata is detached".to_string());
        }

        self.activity
            .iter()
            .rev()
            .filter(|entry| entry.tone == ActivityTone::Warn)
            .find_map(|entry| {
                let combined = format!("{} {}", entry.title, entry.detail);
                text_mentions_runtime_problem(&combined).then(|| compact_line(&combined, 96))
            })
    }

    fn runtime_recovery_target(&self) -> Option<RuntimeRecoveryTarget> {
        if self.connection_attention_detail().is_some() {
            return Some(RuntimeRecoveryTarget::Connection);
        }

        if self.providers_loading && self.providers.is_empty() {
            return None;
        }

        if self.providers.is_empty() {
            return Some(RuntimeRecoveryTarget::Providers);
        }

        let health = self.runtime_health();
        if !matches!(health.tone, ActivityTone::Warn | ActivityTone::Bad) {
            return None;
        }

        let detail = health.detail.to_ascii_lowercase();
        if ["chat list", "chat save", "sifs", "dfs", "daemon", "cluster"]
            .iter()
            .any(|needle| detail.contains(needle))
        {
            return Some(RuntimeRecoveryTarget::Connection);
        }

        if [
            "auth",
            "credential",
            "provider",
            "model",
            "api key",
            "sign-in",
            "sign in",
            "login",
        ]
        .iter()
        .any(|needle| detail.contains(needle))
        {
            Some(RuntimeRecoveryTarget::Providers)
        } else {
            Some(RuntimeRecoveryTarget::Connection)
        }
    }

    fn shows_recovery_empty_card(&self) -> bool {
        self.messages.is_empty() && self.runtime_recovery_target().is_some()
    }

    pub fn connection_attention_detail(&self) -> Option<String> {
        self.connection_attention_detail_full()
            .map(|detail| compact_line(&detail, 112))
    }

    pub fn connection_attention_detail_full(&self) -> Option<String> {
        let mut candidates = Vec::new();
        if let Some(notice) = self.notice.as_deref().filter(|text| !text.is_empty()) {
            candidates.push(notice.to_string());
        }
        for task in self
            .control
            .tasks
            .iter()
            .filter(|task| task_tone(&task.status) == ActivityTone::Bad)
        {
            candidates.push(format!("{}: {}", task.status, task.summary));
        }
        for entry in self
            .activity
            .iter()
            .rev()
            .filter(|entry| matches!(entry.tone, ActivityTone::Warn | ActivityTone::Bad))
        {
            candidates.push(format!("{}: {}", entry.title, entry.detail));
        }

        if let Some(detail) = candidates
            .into_iter()
            .find(|detail| text_mentions_connection_problem(detail))
        {
            return Some(detail);
        }

        let health = self.runtime_health();
        if matches!(health.tone, ActivityTone::Warn | ActivityTone::Bad)
            && text_mentions_connection_problem(&health.detail)
        {
            Some(health.detail)
        } else {
            None
        }
    }

    fn should_offer_chat_list_retry(&self) -> bool {
        self.connection_attention_detail_full()
            .is_some_and(|detail| detail.to_ascii_lowercase().contains("chat list"))
    }

    fn should_offer_sifs_check(&self) -> bool {
        self.connection_attention_detail_full()
            .is_some_and(|detail| detail_should_offer_sifs_check(&detail))
    }

    fn recovery_empty_detail(&self, target: RuntimeRecoveryTarget) -> String {
        match target {
            RuntimeRecoveryTarget::Providers if self.providers_loading => {
                "Language model providers are loading.".to_string()
            }
            RuntimeRecoveryTarget::Providers if self.providers.is_empty() => {
                "No language model provider is configured yet.".to_string()
            }
            RuntimeRecoveryTarget::Providers => compact_line(&self.runtime_health().detail, 112),
            RuntimeRecoveryTarget::Connection => self
                .connection_attention_detail_full()
                .as_deref()
                .and_then(connection_recovery_display_detail)
                .unwrap_or_else(|| {
                    let detail = self
                        .runtime_degraded_detail()
                        .unwrap_or_else(|| self.runtime_health().detail);
                    connection_recovery_display_detail(&detail)
                        .unwrap_or_else(|| compact_line(&detail, 112))
                }),
        }
    }

    fn recovery_empty_title(&self, target: RuntimeRecoveryTarget) -> &'static str {
        match target {
            RuntimeRecoveryTarget::Providers => target.empty_title(),
            RuntimeRecoveryTarget::Connection => {
                let detail = self
                    .connection_attention_detail_full()
                    .unwrap_or_else(|| self.runtime_health().detail);
                let lower = detail.to_ascii_lowercase();
                if lower.contains("channel create") || lower.contains("channel store") {
                    "Channel needs connection attention"
                } else if lower.contains("chat list") {
                    "Chat list needs connection attention"
                } else if lower.contains("chat save") {
                    "Chat save needs connection attention"
                } else {
                    target.empty_title()
                }
            }
        }
    }

    fn recovery_empty_hint(&self, target: RuntimeRecoveryTarget) -> &'static str {
        match target {
            RuntimeRecoveryTarget::Providers => target.empty_hint(),
            RuntimeRecoveryTarget::Connection => {
                let detail = self
                    .connection_attention_detail_full()
                    .unwrap_or_else(|| self.runtime_health().detail);
                let lower = detail.to_ascii_lowercase();
                if lower.contains("channel create") {
                    "Open Connection settings if channel creation keeps failing; inspect SIFS from Files for storage diagnostics."
                } else if lower.contains("chat save") {
                    "Open Connection settings if saving keeps failing; retry the chat list after access is restored."
                } else {
                    target.empty_hint()
                }
            }
        }
    }

    fn event_belongs_to_visible_session(&self, session_id: &str) -> bool {
        session_id.is_empty() || self.session_id.is_empty() || self.session_id == session_id
    }

    fn route_background_session_event(&mut self, session_id: &str, event: ChatEvent) -> bool {
        if self.event_belongs_to_visible_session(session_id) {
            return false;
        }

        if matches!(event, ChatEvent::TurnDone { .. } | ChatEvent::Error { .. }) {
            self.start_session_list_load(self.ui_waker.clone());
        }
        true
    }

    fn append_model_token(messages: &mut Vec<ChatMessage>, token: &str) {
        if let Some(last) = messages.last_mut()
            && last.role == ChatRole::Agent
            && last.source == ChatMessageSource::Model
        {
            last.text.push_str(token);
            return;
        }
        messages.push(ChatMessage::model(token));
    }

    fn append_model_token_from_speaker(
        messages: &mut Vec<ChatMessage>,
        speaker: &str,
        token: &str,
    ) {
        let speaker = if speaker.trim().is_empty() {
            "Agent"
        } else {
            speaker.trim()
        };
        let prefix = format!("{speaker}: ");
        if let Some(last) = messages.last_mut()
            && last.role == ChatRole::Agent
            && last.source == ChatMessageSource::Model
            && (last.text.is_empty() || last.text.starts_with(&prefix))
        {
            if last.text.is_empty() {
                last.text.push_str(&prefix);
            }
            last.text.push_str(token);
            return;
        }
        messages.push(ChatMessage::model(format!("{prefix}{token}")));
    }

    fn finish_turn_messages(messages: &mut [ChatMessage]) -> bool {
        if matches!(messages.last(), Some(m) if m.is_model_placeholder())
            && let Some(last) = messages.last_mut()
        {
            last.text = "(no response)".to_string();
            last.source = ChatMessageSource::System;
            last.kind = ChatMessageKind::Info;
            return true;
        }
        false
    }

    fn trim_messages_to_recent_window(messages: &mut Vec<ChatMessage>) -> bool {
        let excess = messages
            .len()
            .saturating_sub(VISIBLE_TRANSCRIPT_RECENT_LIMIT);
        if excess == 0 {
            return false;
        }
        messages.drain(0..excess);
        true
    }

    fn apply_system_message(messages: &mut Vec<ChatMessage>, kind: &str, text: &str) {
        let message_kind = if kind == "error" {
            ChatMessageKind::Error
        } else {
            ChatMessageKind::Info
        };
        if let Some(last) = messages.last_mut()
            && last.is_model_placeholder()
        {
            last.text = text.to_string();
            last.source = ChatMessageSource::System;
            last.kind = message_kind;
        } else if message_kind == ChatMessageKind::Error {
            messages.push(ChatMessage::system_error(text));
        } else {
            messages.push(ChatMessage::system_info(text));
        }
    }

    fn apply_error_message(messages: &mut Vec<ChatMessage>, error: &str) {
        let text = format!("Error: {error}");
        if let Some(last) = messages.last_mut()
            && last.is_model_placeholder()
        {
            last.text = text;
            last.source = ChatMessageSource::System;
            last.kind = ChatMessageKind::Error;
        } else {
            messages.push(ChatMessage::system_error(text));
        }
    }

    fn ensure_visible_session_id(&mut self) -> String {
        if self.session_id.is_empty() {
            self.session_id = SessionId::generate().to_string();
        }
        self.session_id.clone()
    }

    fn reset_session_media(&mut self) {
        self.media_setup.stop();
        self.media_setup.set_microphone_enabled(true);
        self.media_setup.set_camera_enabled(true);
        self.media_call_runtime = None;
        self.media_call = ChatMediaCallState::default();
        self.media_call_details_open = false;
    }

    fn reset_visible_session(&mut self, session_id: String) {
        let preserved_notice = self
            .notice
            .as_ref()
            .filter(|notice| text_mentions_connection_problem(notice))
            .cloned();
        let preserved_activity: Vec<ChatActivityEntry> = self
            .activity
            .iter()
            .filter(|entry| {
                matches!(entry.tone, ActivityTone::Warn | ActivityTone::Bad)
                    && text_mentions_connection_problem(&format!(
                        "{} {}",
                        entry.title, entry.detail
                    ))
            })
            .cloned()
            .collect();

        self.session_id = session_id;
        self.messages.clear();
        self.input.clear();
        self.cursor = 0;
        self.advance_attachment_draft_generation();
        self.attachment_path_input.clear();
        self.attachment_manual_path_open = false;
        self.attachment_controls_open = false;
        self.attachments.clear();
        self.streaming = false;
        self.queued_turn_inputs.clear();
        self.notice = preserved_notice;
        self.context = None;
        self.usage = None;
        self.activity = preserved_activity;
        self.tool_calls.clear();
        self.timeline = ConversationTimelineState::default();
        self.tool_chain_expanded = false;
        self.expanded_tool_details.clear();
        self.control = ChatControlOverview::default();
        self.reset_session_media();
        self.activity_panel_open = false;
        self.composer_popup = None;
        self.scroll = 0.0;
        self.stick_bottom = true;
        self.session_edit_id = None;
        self.session_edit_details_open = false;
        self.session_title_draft.clear();
        self.session_delete_confirm_id = None;
        self.transcript_load_rx = None;
        self.transcript_load_mode = None;
        self.transcript_load_started_at = None;
        self.transcript_load_slow_notice_shown = false;
    }

    fn transcript_load_pending(&self) -> bool {
        self.transcript_load_rx.is_some()
    }

    fn initial_transcript_load_pending(&self) -> bool {
        matches!(self.transcript_load_mode, Some(TranscriptLoadMode::Initial))
    }

    fn maybe_start_older_transcript_load(&mut self) -> bool {
        if self.transcript_load_pending() || !self.timeline.has_older {
            return false;
        }
        let Some(before_sequence) = self.timeline.loaded_start_sequence else {
            return false;
        };
        if before_sequence <= 1 {
            self.timeline.has_older = false;
            return false;
        }
        let Some(session) = self.current_session_ref().cloned() else {
            return false;
        };
        self.replace_activity(
            "chat load",
            format!("Loading older messages before {before_sequence} from SIFS"),
            ActivityTone::Running,
        );
        self.start_transcript_load(session, TranscriptLoadMode::Older { before_sequence });
        true
    }

    fn maybe_start_newer_transcript_load(&mut self) -> bool {
        if self.transcript_load_pending() || !self.timeline.has_newer {
            return false;
        }
        let Some(after_sequence) = self.timeline.loaded_end_sequence else {
            return false;
        };
        if after_sequence >= self.timeline.total_messages {
            self.timeline.has_newer = false;
            return false;
        }
        let Some(session) = self.current_session_ref().cloned() else {
            return false;
        };
        self.replace_activity(
            "chat load",
            format!("Loading newer messages after {after_sequence} from SIFS"),
            ActivityTone::Running,
        );
        self.start_transcript_load(session, TranscriptLoadMode::Newer { after_sequence });
        true
    }

    fn visible_session_has_started(&self) -> bool {
        self.messages
            .iter()
            .any(|message| message.role == ChatRole::User)
    }

    fn current_history(&self) -> Vec<ChatHistoryMessage> {
        self.messages
            .iter()
            .map(|message| ChatHistoryMessage {
                role: message.history_role().to_string(),
                text: message.text.clone(),
            })
            .collect()
    }

    fn current_session_ref(&self) -> Option<&SessionRef> {
        self.sessions
            .iter()
            .find(|session| session.id == self.session_id)
    }

    fn current_is_channel(&self) -> bool {
        self.current_session_ref()
            .is_some_and(|session| session.kind == ConversationKind::Channel)
    }

    fn channel_filter_empty_state(&self) -> Option<ChannelFilterEmptyState> {
        if self.session_kind_filter == SessionKindFilter::Channels
            && !self.current_is_channel()
            && !self
                .sessions
                .iter()
                .any(|session| session.kind == ConversationKind::Channel)
        {
            Some(if self.session_list_loading && self.sessions.is_empty() {
                ChannelFilterEmptyState::Loading
            } else {
                ChannelFilterEmptyState::Empty
            })
        } else {
            None
        }
    }

    fn show_channel_onboarding(&self) -> bool {
        self.channel_filter_empty_state().is_some()
    }

    fn session_is_channel(&self, session_id: &str) -> bool {
        self.sessions
            .iter()
            .find(|session| session.id == session_id)
            .is_some_and(|session| session.kind == ConversationKind::Channel)
    }

    fn empty_conversation_title(&self) -> String {
        if let Some(state) = self.channel_filter_empty_state() {
            return match state {
                ChannelFilterEmptyState::Loading => "Loading channels...".to_string(),
                ChannelFilterEmptyState::Empty => "No channels yet".to_string(),
            };
        }
        if let Some(session) = self.current_session_ref() {
            if session.kind == ConversationKind::Channel {
                format!("Channel: {}", session.title)
            } else {
                session.title.clone()
            }
        } else {
            let agent = if self.info.agent.is_empty() {
                "Sinomo"
            } else {
                self.info.agent.as_str()
            };
            format!("Chat with {agent}")
        }
    }

    fn empty_conversation_hint(&self) -> String {
        if let Some(state) = self.channel_filter_empty_state() {
            return match state {
                ChannelFilterEmptyState::Loading => "Saved channels are still loading.".to_string(),
                ChannelFilterEmptyState::Empty => {
                    "Create a channel to coordinate multiple agents around one shared timeline."
                        .to_string()
                }
            };
        }
        if let Some(session) = self
            .current_session_ref()
            .filter(|session| session.kind == ConversationKind::Channel)
        {
            format!(
                "{} · send a message to the channel.",
                active_agents_label(session.participant_count)
            )
        } else {
            "Type a message below to begin.".to_string()
        }
    }

    fn empty_conversation_context(&self) -> Option<String> {
        if let Some(state) = self.channel_filter_empty_state() {
            return Some(match state {
                ChannelFilterEmptyState::Loading => {
                    "You can keep chatting or use New channel to start one now.".to_string()
                }
                ChannelFilterEmptyState::Empty => {
                    "Use New channel to choose agents and edit the title.".to_string()
                }
            });
        }
        let session = self
            .current_session_ref()
            .filter(|session| session.kind == ConversationKind::Channel)?;
        if session.participant_names.is_empty() {
            return None;
        }

        let mut names = session
            .participant_names
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        names.dedup();
        let shown = names.iter().take(3).copied().collect::<Vec<_>>().join(", ");
        let extra = names.len().saturating_sub(3);
        Some(if extra > 0 {
            format!("Agents: {shown}, +{extra} more")
        } else {
            format!("Agents: {shown}")
        })
    }

    fn begin_edit_session(&mut self, session_id: &str) {
        let Some(session) = self
            .sessions
            .iter()
            .find(|session| session.id == session_id)
            .cloned()
        else {
            return;
        };
        self.session_edit_id = Some(session.id);
        self.compact_drawer_open = false;
        self.session_edit_details_open = false;
        self.session_title_draft = session.title;
        self.session_delete_confirm_id = None;
        self.session_rename_error = None;
    }

    fn begin_edit_current_session(&mut self) {
        let Some(session_id) = self.current_session_ref().map(|session| session.id.clone()) else {
            return;
        };
        self.begin_edit_session(&session_id);
    }

    fn close_session_edit(&mut self) {
        self.session_edit_id = None;
        self.session_edit_details_open = false;
        self.session_title_draft.clear();
        self.session_delete_confirm_id = None;
    }

    fn toggle_session_edit_details(&mut self) {
        self.session_edit_details_open = !self.session_edit_details_open;
    }

    fn set_session_title_draft(&mut self, title: String) {
        self.session_title_draft = title;
        if self
            .session_rename_error
            .as_ref()
            .is_some_and(|error| self.session_edit_id.as_deref() == Some(error.session_id.as_str()))
        {
            self.session_rename_error = None;
        }
    }

    fn set_session_filter(&mut self, filter: String) {
        self.session_filter = filter;
    }

    fn set_session_kind_filter(&mut self, filter: SessionKindFilter) {
        self.session_kind_filter = filter;
    }

    fn set_sidebar_ratio(&mut self, ratio: f32) {
        self.sidebar_ratio = ratio.clamp(0.0, 1.0);
    }

    fn set_compact_drawer_open(&mut self, open: bool) {
        self.compact_drawer_open = open;
        if open {
            // A modal row editor and the compact navigation drawer should
            // never compete for focus.
            self.close_session_edit();
        }
    }

    fn toggle_compact_drawer(&mut self) {
        let open = !self.compact_drawer_open;
        self.set_compact_drawer_open(open);
    }

    fn can_save_session_title(&self) -> bool {
        let Some(edit_id) = self.session_edit_id.as_deref() else {
            return false;
        };
        if self.pending_session_rename.is_some() {
            return false;
        }
        let title = self.session_title_draft.trim();
        !title.is_empty()
            && self
                .sessions
                .iter()
                .find(|session| session.id == edit_id)
                .is_some_and(|session| session.title != title)
    }

    fn save_session_title(&mut self) {
        if self.pending_session_rename.is_some() {
            return;
        }
        let Some(edit_id) = self.session_edit_id.clone() else {
            return;
        };
        let kind = self
            .sessions
            .iter()
            .find(|session| session.id == edit_id)
            .map(|session| session.kind)
            .unwrap_or(ConversationKind::Chat);
        let title = self.session_title_draft.trim().to_string();
        if title.is_empty() {
            self.notice = Some("chat title cannot be empty".to_string());
            return;
        }

        self.next_session_rename_request_id =
            self.next_session_rename_request_id.wrapping_add(1).max(1);
        let request_id = self.next_session_rename_request_id;
        let pending = PendingSessionRename {
            request_id,
            session_id: edit_id.clone(),
            kind,
            title: title.clone(),
        };
        let (tx, rx) = mpsc::channel();
        let data_dir = self.data_dir.clone();
        let sifs_daemon = self.sifs_daemon.clone();
        let worker_session_id = edit_id.clone();
        let worker_title = title.clone();
        let worker_waker = self.ui_waker.clone();
        let label = conversation_kind_label(kind);

        self.pending_session_rename = Some(pending);
        self.session_rename_error = None;
        self.notice = Some(format!("renaming {label}…"));
        self.replace_activity(
            format!("{label} rename"),
            "Saving title to SIFS".to_string(),
            ActivityTone::Running,
        );

        let spawn = thread::Builder::new()
            .name(format!("sinomo-app-{label}-rename-{request_id}"))
            .spawn(move || {
                let result = rename_session_in_store(
                    data_dir.as_ref(),
                    sifs_daemon.as_ref(),
                    kind,
                    &worker_session_id,
                    &worker_title,
                )
                .map_err(|error| error.to_string());
                let _ = tx.send(SessionRenameWorkerResult {
                    request_id,
                    session_id: worker_session_id,
                    kind,
                    title: worker_title,
                    result,
                });
                if let Some(waker) = worker_waker {
                    waker.wake();
                }
            });
        match spawn {
            Ok(_) => self.session_rename_rx = Some(rx),
            Err(error) => {
                self.pending_session_rename = None;
                self.session_rename_rx = None;
                let detail = format!("could not start rename worker: {error}");
                self.session_rename_error = Some(SessionRenameError {
                    session_id: edit_id,
                    detail: detail.clone(),
                });
                self.notice = Some(format!("{label} rename failed: {detail}"));
                self.replace_activity(format!("{label} rename"), detail, ActivityTone::Warn);
            }
        }
    }

    fn pump_session_rename(&mut self) -> bool {
        let result = match self.session_rename_rx.as_ref().map(|rx| rx.try_recv()) {
            Some(Ok(result)) => result,
            Some(Err(TryRecvError::Empty)) | None => return false,
            Some(Err(TryRecvError::Disconnected)) => {
                self.session_rename_rx = None;
                let Some(pending) = self.pending_session_rename.take() else {
                    return false;
                };
                let label = conversation_kind_label(pending.kind);
                let detail =
                    "rename worker stopped before returning a result; retry the rename".to_string();
                self.session_rename_error = Some(SessionRenameError {
                    session_id: pending.session_id,
                    detail: detail.clone(),
                });
                self.notice = Some(format!("{label} rename failed: {detail}"));
                self.replace_activity(format!("{label} rename"), detail, ActivityTone::Warn);
                return true;
            }
        };

        if self
            .pending_session_rename
            .as_ref()
            .is_none_or(|pending| pending.request_id != result.request_id)
        {
            // A rebind or newer request owns the UI now. Keep its receiver/pending state intact.
            return false;
        }
        self.session_rename_rx = None;
        self.pending_session_rename = None;
        let label = conversation_kind_label(result.kind);
        match result.result {
            Ok(()) => {
                if let Some(session) = self
                    .sessions
                    .iter_mut()
                    .find(|session| session.id == result.session_id)
                {
                    session.title = result.title.clone();
                }
                self.session_rename_error = None;
                self.notice = Some(format!("renamed {label} to {}", result.title));
                self.activity
                    .retain(|entry| entry.title != format!("{label} rename"));
                self.start_session_list_load(self.ui_waker.clone());
            }
            Err(detail) => {
                self.session_rename_error = Some(SessionRenameError {
                    session_id: result.session_id,
                    detail: detail.clone(),
                });
                self.notice = Some(format!("{label} rename failed: {detail}"));
                self.replace_activity(format!("{label} rename"), detail, ActivityTone::Warn);
            }
        }
        true
    }

    fn request_delete_session(&mut self, session_id: &str) {
        let Some(session) = self
            .sessions
            .iter()
            .find(|session| session.id == session_id)
            .cloned()
        else {
            return;
        };
        if self.pending_delete_id.is_some() {
            self.notice = Some("wait for the current delete to finish".to_string());
            return;
        }
        if self.streaming || self.control.active_turn {
            let label = if session.kind == ConversationKind::Channel {
                "channel"
            } else {
                "chat"
            };
            self.notice = Some(format!(
                "wait for the current turn before deleting this {label}"
            ));
            return;
        }
        self.session_edit_id = Some(session.id.clone());
        self.session_edit_details_open = false;
        self.session_title_draft = session.title;
        self.session_delete_confirm_id = Some(session.id);
    }

    fn cancel_delete_session(&mut self) {
        self.session_delete_confirm_id = None;
    }

    fn confirm_delete_session(&mut self, waker: Option<sui::UiHandle>) {
        let Some(session_id) = self.session_delete_confirm_id.clone() else {
            return;
        };
        let Some(session) = self
            .sessions
            .iter()
            .find(|session| session.id == session_id)
            .cloned()
        else {
            self.session_delete_confirm_id = None;
            self.session_edit_id = None;
            self.session_edit_details_open = false;
            self.session_title_draft.clear();
            return;
        };
        let kind = session.kind;
        if self.streaming || self.control.active_turn {
            let label = if kind == ConversationKind::Channel {
                "channel"
            } else {
                "chat"
            };
            self.notice = Some(format!(
                "wait for the current turn before deleting this {label}"
            ));
            return;
        }
        if self.pending_delete_id.is_some() {
            self.notice = Some("wait for the current delete to finish".to_string());
            return;
        }

        let (tx, rx) = mpsc::channel();
        let data_dir = self.data_dir.clone();
        let sifs_daemon = self.sifs_daemon.clone();
        let worker_session_id = session_id.clone();
        let worker_session = session.clone();
        let worker_waker = waker.clone();
        let spawn_result = thread::Builder::new()
            .name("sinomo-app-chat-delete".to_string())
            .spawn(move || {
                let result = delete_conversation_records(
                    &data_dir,
                    sifs_daemon.as_ref(),
                    &worker_session_id,
                    kind,
                )
                .map_err(|error| error.to_string());
                let _ = tx.send(DeleteWorkerResult {
                    session: worker_session,
                    kind,
                    result,
                });
                if let Some(waker) = worker_waker {
                    waker.wake();
                }
            });

        if let Err(error) = spawn_result {
            let label = if kind == ConversationKind::Channel {
                "channel"
            } else {
                "chat"
            };
            self.notice = Some(format!("{label} delete failed: {error}"));
            self.push_activity(
                format!("{label} delete"),
                error.to_string(),
                ActivityTone::Warn,
            );
            return;
        }

        self.delete_rx = Some(rx);
        self.pending_delete_id = Some(session_id.clone());
        self.session_delete_confirm_id = None;
        self.session_edit_id = None;
        self.session_edit_details_open = false;
        self.session_title_draft.clear();
        self.sessions.retain(|session| session.id != session_id);
        if self.session_id == session_id {
            let new_session_id = SessionId::generate().to_string();
            self.reset_visible_session(new_session_id.clone());
            self.pending_session_reset = Some(new_session_id.clone());
            self.handle.send_command(ChatCommand::NewSession {
                session_id: new_session_id,
            });
        }
        let label = if kind == ConversationKind::Channel {
            "channel"
        } else {
            "chat"
        };
        self.notice = Some(format!("deleted {label}"));
    }

    pub fn pump_delete(&mut self) -> bool {
        let mut changed = false;
        loop {
            let result = match self.delete_rx.as_ref().map(|rx| rx.try_recv()) {
                Some(Ok(result)) => result,
                Some(Err(TryRecvError::Empty)) | None => break,
                Some(Err(TryRecvError::Disconnected)) => {
                    if let Some(session_id) = self.pending_delete_id.take() {
                        self.notice = Some("chat delete failed: worker stopped".to_string());
                        self.push_activity(
                            "chat delete",
                            format!("worker stopped before deleting {session_id}"),
                            ActivityTone::Warn,
                        );
                        changed = true;
                    }
                    self.delete_rx = None;
                    break;
                }
            };
            if self.apply_delete_result(result) {
                changed = true;
            }
        }
        changed
    }

    pub fn pump_channel_create(&mut self) -> bool {
        let result = match self.channel_create_rx.as_ref().map(|rx| rx.try_recv()) {
            Some(Ok(result)) => result,
            Some(Err(TryRecvError::Empty)) | None => return false,
            Some(Err(TryRecvError::Disconnected)) => {
                self.channel_create_rx = None;
                self.notice = Some("channel create failed: worker stopped".to_string());
                self.push_activity(
                    "channel create",
                    "worker stopped before saving the channel to SIFS".to_string(),
                    ActivityTone::Warn,
                );
                return true;
            }
        };

        self.channel_create_rx = None;
        match result.result {
            Ok(list_result) => {
                let session = result.session;
                self.apply_session_list_result(list_result);
                if !self
                    .sessions
                    .iter()
                    .any(|existing| existing.id == session.id)
                {
                    self.upsert_session_ref(session);
                }
                if self
                    .notice
                    .as_deref()
                    .is_some_and(|notice| notice == "creating channel...")
                {
                    self.notice = None;
                }
                self.replace_activity(
                    "channel create",
                    "Saved channel to SIFS".to_string(),
                    ActivityTone::Good,
                );
            }
            Err(detail) => {
                self.notice = Some(format!("channel create failed: {detail}"));
                self.push_activity("channel create", detail, ActivityTone::Warn);
            }
        }
        true
    }

    fn apply_delete_result(&mut self, result: DeleteWorkerResult) -> bool {
        if self.pending_delete_id.as_deref() != Some(result.session.id.as_str()) {
            return false;
        }
        self.pending_delete_id = None;
        self.delete_rx = None;
        let label = if result.kind == ConversationKind::Channel {
            "channel"
        } else {
            "chat"
        };
        match result.result {
            Ok(()) => {
                self.push_activity(format!("{label} delete"), "completed", ActivityTone::Good);
            }
            Err(detail) => {
                if !self
                    .sessions
                    .iter()
                    .any(|session| session.id == result.session.id)
                {
                    self.sessions.push(result.session);
                    sort_conversations(&mut self.sessions);
                }
                self.notice = Some(format!("{label} delete failed: {detail}"));
                self.push_activity(format!("{label} delete"), detail, ActivityTone::Warn);
            }
        }
        true
    }

    /// Open a stored chat/channel without blocking the UI thread. The transcript/timeline is read
    /// from SIFS on a worker thread, then applied by `pump_transcript_load`.
    fn open_session(&mut self, index: usize) {
        let Some(session) = self.sessions.get(index).cloned() else {
            return;
        };
        self.compact_drawer_open = false;
        // A native picker belongs to the visible chat/draft that opened it. The text and any
        // already-queued attachments intentionally remain available while navigating stored
        // chats, but a late OS-dialog result must not leak into the newly visible chat.
        self.advance_attachment_draft_generation();
        self.session_id = session.id.clone();
        self.messages = vec![transcript_loading_message(&session)];
        self.timeline = ConversationTimelineState::default();
        self.streaming = false;
        self.queued_turn_inputs.clear();
        self.notice = None;
        self.context = None;
        self.usage = None;
        self.activity.clear();
        self.tool_calls.clear();
        self.tool_chain_expanded = false;
        self.expanded_tool_details.clear();
        self.control = ChatControlOverview::default();
        self.reset_session_media();
        self.scroll = f32::MAX;
        self.stick_bottom = true;
        self.session_edit_id = None;
        self.session_edit_details_open = false;
        self.session_title_draft.clear();
        self.session_delete_confirm_id = None;
        self.transcript_load_rx = None;
        self.transcript_load_mode = None;
        self.replace_activity(
            "chat load",
            transcript_loading_detail(session.kind),
            ActivityTone::Running,
        );
        self.start_transcript_load(session, TranscriptLoadMode::Initial);
    }

    fn start_transcript_load(&mut self, session: SessionRef, mode: TranscriptLoadMode) {
        let (tx, rx) = mpsc::channel();
        let data_dir = self.data_dir.clone();
        let sifs_daemon = self.sifs_daemon.clone();
        let known_message_ids = self.timeline.discovered_message_ids.clone();
        let worker_session = session.clone();
        let worker_waker = self.ui_waker.clone();
        let spawn_result = thread::Builder::new()
            .name("sinomo-app-chat-load".to_string())
            .spawn(move || {
                let result = load_conversation_timeline(
                    &data_dir,
                    sifs_daemon.as_ref(),
                    &worker_session.id,
                    worker_session.kind,
                    mode,
                    &known_message_ids,
                );
                let _ = tx.send(TranscriptLoadWorkerResult {
                    session: worker_session,
                    mode,
                    result,
                });
                if let Some(waker) = worker_waker {
                    waker.wake();
                }
            });

        match spawn_result {
            Ok(_) => {
                self.transcript_load_rx = Some(rx);
                self.transcript_load_mode = Some(mode);
                self.transcript_load_started_at = Some(Instant::now());
                self.transcript_load_slow_notice_shown = false;
            }
            Err(error) => {
                let detail = error.to_string();
                self.transcript_load_rx = None;
                self.transcript_load_mode = None;
                self.transcript_load_started_at = None;
                self.transcript_load_slow_notice_shown = false;
                if mode == TranscriptLoadMode::Initial {
                    self.messages = vec![transcript_load_error_message(session.kind)];
                }
                self.notice = Some(transcript_load_failed_notice(mode, &detail));
                self.replace_activity("chat load", detail, ActivityTone::Warn);
            }
        }
    }

    pub fn pump_transcript_load(&mut self) -> bool {
        let Some(result) = self.transcript_load_rx.as_ref().map(|rx| rx.try_recv()) else {
            return false;
        };
        match result {
            Ok(result) => {
                self.transcript_load_rx = None;
                self.transcript_load_mode = None;
                self.transcript_load_started_at = None;
                self.transcript_load_slow_notice_shown = false;
                self.apply_transcript_load_result(result);
                true
            }
            Err(TryRecvError::Empty) => self.maybe_show_slow_transcript_load(),
            Err(TryRecvError::Disconnected) => {
                let mode = self
                    .transcript_load_mode
                    .unwrap_or(TranscriptLoadMode::Initial);
                self.transcript_load_rx = None;
                self.transcript_load_mode = None;
                self.transcript_load_started_at = None;
                self.transcript_load_slow_notice_shown = false;
                if mode == TranscriptLoadMode::Initial {
                    self.messages = vec![ChatMessage::system_error(
                        "Chat history stopped loading before SIFS returned a transcript.",
                    )];
                }
                self.notice = Some(transcript_load_failed_notice(mode, "worker stopped"));
                self.replace_activity(
                    "chat load",
                    "worker stopped before loading chat history from SIFS".to_string(),
                    ActivityTone::Warn,
                );
                true
            }
        }
    }

    fn maybe_show_slow_transcript_load(&mut self) -> bool {
        if self.transcript_load_slow_notice_shown
            || self.transcript_load_mode != Some(TranscriptLoadMode::Initial)
            || !self
                .transcript_load_started_at
                .is_some_and(|started| started.elapsed() >= TRANSCRIPT_LOAD_UI_TIMEOUT)
        {
            return false;
        }
        self.transcript_load_slow_notice_shown = true;
        let kind = self
            .sessions
            .iter()
            .find(|session| session.id == self.session_id)
            .map(|session| session.kind)
            .unwrap_or(ConversationKind::Chat);
        self.messages = vec![transcript_slow_loading_message(kind)];
        self.replace_activity(
            "chat load",
            format!(
                "SIFS transcript is still loading after {} seconds",
                TRANSCRIPT_LOAD_UI_TIMEOUT.as_secs()
            ),
            ActivityTone::Running,
        );
        true
    }

    fn apply_transcript_load_result(&mut self, result: TranscriptLoadWorkerResult) {
        if result.session.id != self.session_id {
            return;
        }
        match result.result {
            Ok(mut load) => {
                self.timeline
                    .discovered_message_ids
                    .extend(std::mem::take(&mut load.discovered_message_ids));
                match result.mode {
                    TranscriptLoadMode::Initial => {
                        let history: Vec<ChatHistoryMessage> = load
                            .messages
                            .iter()
                            .map(|m| ChatHistoryMessage {
                                role: m.history_role().to_string(),
                                text: m.text.clone(),
                            })
                            .collect();
                        self.messages = load.messages;
                        self.timeline.apply_page(&load.page);
                        if let Some(overview) = load.overview {
                            self.timeline.apply_overview(overview);
                        }
                        self.activity.retain(|entry| entry.title != "chat load");
                        if self.notice.as_deref().is_some_and(|notice| {
                            notice.starts_with("chat load failed:")
                                || notice == "wait for this chat history to finish loading"
                        }) {
                            self.notice = None;
                        }
                        self.scroll = f32::MAX;
                        self.stick_bottom = true;
                        self.handle.send_command(ChatCommand::Resume {
                            session_id: result.session.id,
                            history,
                        });
                    }
                    TranscriptLoadMode::Older { before_sequence } => {
                        let added = prepend_timeline_messages(&mut self.messages, load.messages);
                        self.timeline.apply_older_page(&load.page);
                        let pruned =
                            trim_timeline_window(&mut self.messages, TimelineTrimEdge::End);
                        self.timeline
                            .refresh_loaded_range_from_messages(&self.messages);
                        if pruned > 0 {
                            self.timeline.has_newer = true;
                        }
                        if added > 0 {
                            self.scroll += added as f32 * TRANSCRIPT_PREPEND_ESTIMATED_MESSAGE_H;
                            self.stick_bottom = false;
                        }
                        self.activity.retain(|entry| entry.title != "chat load");
                        self.replace_activity(
                            "chat history",
                            if added == 0 {
                                format!("No older messages before {before_sequence}")
                            } else {
                                format!(
                                    "Loaded {added} older message{}",
                                    if added == 1 { "" } else { "s" }
                                )
                            },
                            ActivityTone::Good,
                        );
                    }
                    TranscriptLoadMode::Newer { .. } => {
                        let added = append_timeline_messages(&mut self.messages, load.messages);
                        self.timeline.apply_newer_page(&load.page);
                        let pruned =
                            trim_timeline_window(&mut self.messages, TimelineTrimEdge::Start);
                        self.timeline
                            .refresh_loaded_range_from_messages(&self.messages);
                        if pruned > 0 {
                            self.timeline.has_older = true;
                            self.scroll = (self.scroll
                                - pruned as f32 * TRANSCRIPT_PREPEND_ESTIMATED_MESSAGE_H)
                                .max(0.0);
                        }
                        if !self.timeline.has_newer {
                            self.scroll = f32::MAX;
                            self.stick_bottom = true;
                        }
                        self.activity.retain(|entry| entry.title != "chat load");
                        if added > 0 {
                            self.replace_activity(
                                "chat history",
                                format!(
                                    "Loaded {added} newer message{}",
                                    if added == 1 { "" } else { "s" }
                                ),
                                ActivityTone::Good,
                            );
                        }
                    }
                    TranscriptLoadMode::ReconcileLatest => {
                        let added =
                            reconcile_latest_timeline_messages(&mut self.messages, load.messages);
                        self.timeline.apply_newer_page(&load.page);
                        let pruned =
                            trim_timeline_window(&mut self.messages, TimelineTrimEdge::Start);
                        self.timeline
                            .refresh_loaded_range_from_messages(&self.messages);
                        if pruned > 0 {
                            self.timeline.has_older = true;
                            self.scroll = (self.scroll
                                - pruned as f32 * TRANSCRIPT_PREPEND_ESTIMATED_MESSAGE_H)
                                .max(0.0);
                        }
                        if !self.timeline.has_newer && self.stick_bottom {
                            self.scroll = f32::MAX;
                        }
                        self.activity.retain(|entry| entry.title != "chat load");
                        if added > 0 {
                            self.replace_activity(
                                "chat history",
                                format!(
                                    "Synced {added} message{} from another device",
                                    if added == 1 { "" } else { "s" }
                                ),
                                ActivityTone::Good,
                            );
                        }
                    }
                }
            }
            Err(detail) => {
                if result.mode == TranscriptLoadMode::Initial {
                    self.messages = vec![transcript_load_error_message(result.session.kind)];
                }
                self.notice = Some(transcript_load_failed_notice(result.mode, &detail));
                self.replace_activity("chat load", detail, ActivityTone::Warn);
            }
        }
    }

    fn current_provider(&self) -> Option<&ProviderSummary> {
        self.providers.get(self.provider_idx)
    }

    fn current_model(&self) -> String {
        let Some(provider) = self.current_provider() else {
            return self.info.model.clone();
        };
        if let Some(model) = provider.models.get(self.model_idx) {
            return model.clone();
        }
        let runtime_matches_provider = self.info.provider == provider.id
            && self
                .info
                .account
                .as_deref()
                .is_none_or(|account| account == provider.account);
        if runtime_matches_provider {
            self.info.model.clone()
        } else {
            String::new()
        }
    }

    fn current_model_selection(&self) -> Option<(String, String, String)> {
        let provider = self.current_provider()?;
        let provider_id = provider.id.trim();
        let model = self.current_model();
        if provider_id.is_empty() || model.trim().is_empty() {
            return None;
        }
        Some((provider_id.to_string(), model, provider.account.clone()))
    }

    fn runtime_matches_selection(&self, provider: &str, model: &str, account: &str) -> bool {
        self.info.provider == provider
            && self.info.model == model
            && sinomo_lm::normalize_provider_account_name(self.info.account.as_deref()) == account
    }

    fn current_model_selection_command(&self) -> Option<ChatCommand> {
        self.current_model_selection()
            .map(|(provider, model, account)| ChatCommand::SetModel {
                provider,
                model,
                account,
            })
    }

    fn current_media_provider_model(&self) -> (String, String) {
        let provider = self
            .current_provider()
            .map(|provider| {
                if provider.name.trim().is_empty() {
                    provider.id.clone()
                } else {
                    provider.name.clone()
                }
            })
            .filter(|provider| !provider.trim().is_empty())
            .or_else(|| (!self.info.provider.trim().is_empty()).then(|| self.info.provider.clone()))
            .unwrap_or_else(|| "provider pending".to_string());
        let model = self.current_model();
        let model = if model.trim().is_empty() {
            self.info.model.clone()
        } else {
            model
        };
        let model = if model.trim().is_empty() {
            "model pending".to_string()
        } else {
            model
        };
        (provider, model)
    }

    pub fn toggle_media_call(&mut self) {
        if self.media_call.active {
            let provider = self.media_call.provider.clone();
            let model = self.media_call.model.clone();
            self.media_call.active = false;
            self.media_call.started_at = None;
            self.media_call_runtime = None;
            self.media_setup.stop();
            self.media_call.microphone_level = 0.0;
            self.media_setup.camera_surface.clear();
            self.media_setup.microphone_access = MediaAccessState::Idle;
            self.media_setup.speaker_access = MediaAccessState::Idle;
            self.media_setup.camera_access = MediaAccessState::Idle;
            self.media_call_details_open = false;
            self.media_call.notice = Some("call ended".to_string());
            self.notice = Some("media call ended".to_string());
            self.push_activity(
                "media call",
                format!("ended {provider} / {model}"),
                ActivityTone::Neutral,
            );
            return;
        }

        let (provider, model) = self.current_media_provider_model();
        if provider == "provider pending" || model == "model pending" {
            let detail =
                "choose a realtime-capable provider and model before starting a media call";
            self.media_call_details_open = true;
            self.media_call.notice = Some(detail.to_string());
            self.notice = Some(detail.to_string());
            self.push_activity("media call", detail, ActivityTone::Warn);
            return;
        }

        let setup_shutdown = self.media_setup.begin_shutdown();
        let (started, native_notice) =
            self.start_native_media_call_runtime(&provider, &model, setup_shutdown);
        if !started {
            self.media_call.active = false;
            self.media_call.started_at = None;
            self.media_call_details_open = true;
            self.media_call.notice = Some(native_notice.clone());
            self.notice = Some(format!("media call failed: {native_notice}"));
            self.push_activity("media call", native_notice, ActivityTone::Warn);
            self.media_setup.start();
            return;
        }

        self.media_call.active = true;
        self.media_call_details_open = true;
        self.media_call.provider = provider.clone();
        self.media_call.model = model.clone();
        self.media_call.started_at = Some(Instant::now());
        self.media_call.input_audio_chunks = 0;
        self.media_call.input_video_frames = 0;
        self.media_call.output_audio_chunks = 0;
        self.media_call.microphone_level = 0.0;
        self.media_setup.microphone_access = if self.media_call.mic_enabled {
            MediaAccessState::Checking
        } else {
            MediaAccessState::Idle
        };
        self.media_setup.speaker_access = MediaAccessState::Checking;
        self.media_setup.camera_access = if self.media_call.camera_enabled {
            MediaAccessState::Checking
        } else {
            MediaAccessState::Idle
        };
        self.media_call.notice = Some(native_notice.clone());
        self.notice = Some("media call started".to_string());
        self.push_activity(
            "media call",
            format!("started {provider} / {model} · {native_notice}"),
            ActivityTone::Running,
        );
    }

    fn start_native_media_call_runtime(
        &mut self,
        provider: &str,
        model: &str,
        setup_shutdown: Option<MediaSetupShutdown>,
    ) -> (bool, String) {
        let start_config = self.media_call_start_config();
        #[cfg(test)]
        {
            let _ = (provider, model, start_config, setup_shutdown);
            self.media_call_runtime = None;
            return (true, "native media disabled in tests".to_string());
        }
        #[cfg(not(test))]
        {
            let provider_id = self
                .current_provider()
                .map(|provider| provider.id.clone())
                .filter(|provider| !provider.trim().is_empty())
                .unwrap_or_else(|| provider.to_string());
            let model = if model.trim().is_empty() || model == "model pending" {
                self.info.model.clone()
            } else {
                model.to_string()
            };
            let (event_tx, event_rx) = mpsc::channel();
            let (control_tx, control_rx) = mpsc::channel();
            let snapshots = Arc::new(Mutex::new(ChatMediaSnapshots::default()));
            let worker_snapshots = snapshots.clone();
            let cancelled = Arc::new(AtomicBool::new(false));
            let worker_cancelled = cancelled.clone();
            let microphone_enabled = Arc::new(AtomicBool::new(start_config.mic_enabled));
            let worker_microphone_enabled = microphone_enabled.clone();
            let camera_enabled = Arc::new(AtomicBool::new(start_config.camera_enabled));
            let worker_camera_enabled = camera_enabled.clone();
            let camera_surface = self.media_setup.camera_surface.clone();
            let data_dir = self.data_dir.clone();
            let worker_waker = self.ui_waker.clone();
            match thread::Builder::new()
                .name("sinomo-app-media-call".to_string())
                .spawn(move || {
                    run_chat_media_call_worker(
                        data_dir,
                        provider_id,
                        model,
                        start_config,
                        setup_shutdown,
                        event_tx,
                        control_rx,
                        worker_snapshots,
                        worker_cancelled,
                        worker_microphone_enabled,
                        worker_camera_enabled,
                        camera_surface,
                        worker_waker,
                    );
                }) {
                Ok(worker) => {
                    self.media_call_runtime = Some(ChatNativeMediaRuntime {
                        control_tx,
                        event_rx,
                        snapshots,
                        cancelled,
                        microphone_enabled,
                        camera_enabled,
                        worker: Some(worker),
                    });
                    (true, "native media connecting".to_string())
                }
                Err(error) => {
                    self.media_call_runtime = None;
                    (
                        false,
                        format!("native media worker failed to start: {error}"),
                    )
                }
            }
        }
    }

    fn media_call_start_config(&self) -> ChatMediaCallStartConfig {
        ChatMediaCallStartConfig {
            devices: self.media_setup.selection(),
            mic_enabled: self.media_call.mic_enabled,
            camera_enabled: self.media_call.camera_enabled,
        }
    }

    pub(crate) fn pump_media_call_runtime(&mut self) -> MediaPumpChange {
        let mut change = self.media_setup.pump();
        if !self.media_call.active {
            return change;
        }
        let Some(runtime) = self.media_call_runtime.as_mut() else {
            return change;
        };
        let snapshots = runtime
            .snapshots
            .lock()
            .map(|mut snapshots| snapshots.take())
            .unwrap_or_default();
        let mut changed = false;
        let mut ended = false;
        if !snapshots.is_empty() {
            let notice_changed = snapshots.notice.is_some();
            self.media_call.input_audio_chunks = self
                .media_call
                .input_audio_chunks
                .saturating_add(snapshots.input_audio_chunks);
            self.media_call.input_video_frames = self
                .media_call
                .input_video_frames
                .saturating_add(snapshots.input_video_frames);
            self.media_call.output_audio_chunks = self
                .media_call
                .output_audio_chunks
                .saturating_add(snapshots.output_audio_chunks);
            if let Some(level) = snapshots.microphone_level {
                if self.media_call.mic_enabled
                    && matches!(self.media_setup.microphone_access, MediaAccessState::Ready)
                {
                    self.media_call.microphone_level = level.clamp(0.0, 1.0);
                }
            }
            if let Some(notice) = snapshots.notice {
                self.media_call.notice = Some(notice);
            }
            changed = true;
            if notice_changed {
                change.mark_semantics();
            } else {
                change.mark_paint();
            }
        }
        loop {
            let event = match runtime.event_rx.try_recv() {
                Ok(event) => event,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    ended = true;
                    ChatMediaCallWorkerEvent::Done("media call worker stopped".to_string())
                }
            };
            changed = true;
            change.mark_layout();
            match event {
                ChatMediaCallWorkerEvent::Ready(detail) => {
                    self.media_call.notice = Some(detail);
                }
                ChatMediaCallWorkerEvent::DeviceAccess(kind, state) => {
                    let ready = matches!(state, MediaAccessState::Ready);
                    let failed = matches!(
                        &state,
                        MediaAccessState::PermissionDenied(_)
                            | MediaAccessState::Busy(_)
                            | MediaAccessState::Unavailable(_)
                            | MediaAccessState::Unsupported(_)
                    );
                    match kind {
                        MediaAccessKind::Microphone => {
                            if failed {
                                self.media_call.mic_enabled = false;
                                self.media_setup.set_microphone_enabled(false);
                            }
                            self.media_setup.microphone_access = state;
                            if !ready {
                                self.media_call.microphone_level = 0.0;
                            }
                        }
                        MediaAccessKind::Speaker => {
                            self.media_setup.speaker_access = state;
                        }
                        MediaAccessKind::Camera => {
                            if failed {
                                self.media_call.camera_enabled = false;
                                self.media_setup.set_camera_enabled(false);
                            }
                            self.media_setup.camera_access = state;
                            if !ready {
                                self.media_setup.camera_surface.clear();
                            }
                        }
                    }
                }
                ChatMediaCallWorkerEvent::Error(detail) => {
                    self.media_call.active = false;
                    self.media_call.started_at = None;
                    self.media_call.microphone_level = 0.0;
                    self.media_setup.camera_surface.clear();
                    self.media_call_details_open = true;
                    self.media_call.notice = Some(format!("media call failed: {detail}"));
                    self.notice = Some(format!("media call failed: {detail}"));
                    self.push_activity("media call", detail, ActivityTone::Warn);
                    self.media_setup.start();
                    ended = true;
                    break;
                }
                ChatMediaCallWorkerEvent::Done(detail) => {
                    self.media_call.active = false;
                    self.media_call.started_at = None;
                    self.media_call.microphone_level = 0.0;
                    self.media_setup.camera_surface.clear();
                    self.media_setup.microphone_access = MediaAccessState::Idle;
                    self.media_setup.speaker_access = MediaAccessState::Idle;
                    self.media_setup.camera_access = MediaAccessState::Idle;
                    self.media_call_details_open = false;
                    self.media_call.notice = Some(detail);
                    ended = true;
                    break;
                }
            }
        }
        if ended {
            self.media_call_runtime = None;
        }
        if changed && !change.changed() {
            change.mark_paint();
        }
        change
    }

    pub fn toggle_media_call_mic(&mut self) {
        self.media_call.mic_enabled = !self.media_call.mic_enabled;
        self.media_setup
            .set_microphone_enabled(self.media_call.mic_enabled);
        if let Some(runtime) = &self.media_call_runtime {
            runtime.send_control(ChatMediaCallControl::SetMicEnabled(
                self.media_call.mic_enabled,
            ));
        }
        if !self.media_call.mic_enabled {
            self.media_call.microphone_level = 0.0;
        }
        if self.media_call.active {
            self.media_setup.microphone_access = if self.media_call.mic_enabled {
                MediaAccessState::Checking
            } else {
                MediaAccessState::Idle
            };
        }
        let detail = if self.media_call.mic_enabled {
            "microphone enabled"
        } else {
            "microphone muted"
        };
        self.media_call.notice = Some(detail.to_string());
        self.push_activity("media call", detail, ActivityTone::Neutral);
    }

    pub fn toggle_media_call_camera(&mut self) {
        self.media_call.camera_enabled = !self.media_call.camera_enabled;
        self.media_setup
            .set_camera_enabled(self.media_call.camera_enabled);
        if let Some(runtime) = &self.media_call_runtime {
            runtime.send_control(ChatMediaCallControl::SetCameraEnabled(
                self.media_call.camera_enabled,
            ));
        }
        if !self.media_call.camera_enabled {
            self.media_setup.camera_surface.clear();
        }
        if self.media_call.active {
            self.media_setup.camera_access = if self.media_call.camera_enabled {
                MediaAccessState::Checking
            } else {
                MediaAccessState::Idle
            };
        }
        let detail = if self.media_call.camera_enabled {
            "camera enabled"
        } else {
            "camera paused"
        };
        self.media_call.notice = Some(detail.to_string());
        self.push_activity("media call", detail, ActivityTone::Neutral);
    }

    pub fn toggle_media_call_details(&mut self) {
        self.media_call_details_open = !self.media_call_details_open;
        if !self.media_call.active {
            if self.media_call_details_open {
                self.media_setup.start();
            } else {
                self.media_setup.stop();
            }
        }
    }

    pub(crate) fn sync_media_setup_visibility(&mut self, chat_visible: bool) -> bool {
        if self.media_call.active {
            return false;
        }
        let should_run = chat_visible && self.media_call_details_open;
        if should_run == self.media_setup.is_running() {
            return false;
        }
        if should_run {
            self.media_setup.start();
        } else {
            self.media_setup.stop();
        }
        true
    }

    /// The composer voice affordance opens a preflight setup before the first
    /// call, and remains a one-click hang-up while a call is live.
    pub fn activate_media_call_control(&mut self) {
        if self.media_call.active {
            self.toggle_media_call();
        } else {
            self.media_call_details_open = true;
            self.media_call.notice = Some("choose devices, check levels, then start".to_string());
            self.media_setup.start();
        }
    }

    fn media_call_details_visible(&self) -> bool {
        self.media_call_details_open
    }

    fn media_call_panel_visible(&self) -> bool {
        self.media_call.active || self.media_call_details_open
    }

    fn media_call_elapsed_label(&self) -> String {
        let Some(started_at) = self.media_call.started_at else {
            return "00:00".to_string();
        };
        let elapsed = started_at.elapsed().as_secs();
        format!("{:02}:{:02}", elapsed / 60, elapsed % 60)
    }

    fn media_call_status_summary(&self) -> String {
        if self.media_call.active {
            return format!(
                "{} · mic {} · camera {}",
                self.media_call_elapsed_label(),
                if self.media_call.mic_enabled {
                    "on"
                } else {
                    "muted"
                },
                if self.media_call.camera_enabled {
                    "on"
                } else {
                    "paused"
                }
            );
        }
        self.media_call
            .notice
            .clone()
            .unwrap_or_else(|| "ready".to_string())
    }

    /// Whether the reasoning-effort selector should be shown: only for models that take one.
    fn effort_visible(&self) -> bool {
        let provider = self
            .current_provider()
            .map(|p| p.id.clone())
            .unwrap_or_default();
        let model = self.current_model();
        !provider.is_empty() && sinomo_lm::model_supports_reasoning_effort(&provider, &model)
    }

    /// The current model list for the active provider (for the composer's model `Select`).
    fn current_models(&self) -> Vec<String> {
        self.current_provider()
            .map(|p| p.models.clone())
            .unwrap_or_default()
    }

    pub fn set_tool_runtime_node(&mut self, sandbox_node_id: Option<String>) {
        self.handle
            .send_command(ChatCommand::SetToolRuntimeNode { sandbox_node_id });
    }

    fn apply_event(&mut self, event: ChatEvent) -> bool {
        if let Some(session_id) = event.session_id().map(str::to_string) {
            if self.session_id.is_empty() {
                self.session_id = session_id.clone();
            } else if self.route_background_session_event(&session_id, event.clone()) {
                return false;
            }
        }
        match event {
            ChatEvent::Ready { session_id, info } => {
                self.info = info;
                self.sync_selection_from_info();
                if self.session_id.is_empty() || self.session_id == session_id {
                    self.session_id = session_id;
                }
            }
            ChatEvent::Token { text, .. } => {
                self.notice = None;
                Self::append_model_token(&mut self.messages, &text);
                if self.stick_bottom {
                    self.scroll = f32::MAX;
                }
            }
            ChatEvent::AgentToken {
                session_id,
                speaker,
                text,
            } => {
                self.notice = None;
                if self.session_is_channel(&session_id)
                    || (session_id.is_empty() && self.current_is_channel())
                {
                    Self::append_model_token_from_speaker(
                        &mut self.messages,
                        &speaker.agent_name,
                        &text,
                    );
                } else {
                    Self::append_model_token(&mut self.messages, &text);
                }
                if self.stick_bottom {
                    self.scroll = f32::MAX;
                }
            }
            ChatEvent::Notice { notice, .. } => self.notice = Some(notice),
            ChatEvent::SystemMessage { kind, text, .. } => {
                Self::apply_system_message(&mut self.messages, &kind, &text);
                self.notice = None;
                if self.stick_bottom {
                    self.scroll = f32::MAX;
                }
            }
            ChatEvent::Context { context, .. } => {
                self.notice = Some(format!(
                    "context assembled: {}",
                    format_bytes(context.total_bytes)
                ));
                self.context = Some(context);
            }
            ChatEvent::ToolCallStarted { call, .. } => {
                self.record_tool_call("tool", call, "running", ActivityTone::Running);
                self.notice = Some("tool call requested".to_string());
            }
            ChatEvent::ToolCallDelta {
                step_index,
                call_id,
                tool,
                arguments_delta,
                ..
            } => {
                self.record_tool_delta(step_index, call_id.clone(), tool.clone(), &arguments_delta);
                if !arguments_delta.trim().is_empty() {
                    self.push_activity(
                        format!(
                            "tool input {}",
                            tool.unwrap_or_else(|| "stream".to_string())
                        ),
                        format!(
                            "step {step_index} · {}",
                            compact_line(&arguments_delta, 120)
                        ),
                        ActivityTone::Neutral,
                    );
                }
            }
            ChatEvent::ToolCallCompleted { call, .. } => {
                // This provider event only means the model finished assembling
                // the request. Execution outcome arrives separately (for
                // process tools via `ChatEvent::Process`).
                self.record_tool_call("tool requested", call, "requested", ActivityTone::Neutral);
            }
            ChatEvent::Process { process, .. } => {
                self.record_process(process);
            }
            ChatEvent::Usage { usage, .. } => {
                self.usage = Some(usage.clone());
                let (detail, tone) = usage_activity_detail(&usage, self.streaming);
                self.replace_activity("model usage", detail, tone);
            }
            ChatEvent::TurnDone { .. } => {
                self.streaming = false;
                self.control.active_turn = false;
                self.queued_turn_inputs.clear();
                self.notice = None;
                Self::finish_turn_messages(&mut self.messages);
                self.persist();
                if Self::trim_messages_to_recent_window(&mut self.messages) {
                    self.timeline
                        .refresh_loaded_range_from_messages(&self.messages);
                    self.timeline.has_older = self
                        .timeline
                        .loaded_start_sequence
                        .is_some_and(|sequence| sequence > 1);
                }
            }
            ChatEvent::ControlStatus(status) => {
                self.control.active_turn = status.active_turn;
                self.control.task_count = status.task_count;
                self.control.terminal_result_count = status.terminal_result_count;
                self.control.tasks = status.tasks;
                self.control.runtime_attached = status.runtime_attached;
                if let Some(queued) = &status.queued_input {
                    self.attach_queued_input_id(queued);
                }
                if let Some(removed_id) = &status.removed_input_id {
                    // Confirmation of a removal (usually already applied optimistically).
                    self.queued_turn_inputs
                        .retain(|item| item.input_id.as_deref() != Some(removed_id.as_str()));
                }
                self.notice = Some(format!(
                    "agent control {} at sequence {}",
                    if status.accepted {
                        "accepted"
                    } else {
                        "rejected"
                    },
                    status.sequence
                ));
                self.push_activity(
                    "runtime control",
                    format!(
                        "{} task(s), {} terminal result(s)",
                        status.task_count, status.terminal_result_count
                    ),
                    if status.accepted {
                        ActivityTone::Good
                    } else {
                        ActivityTone::Warn
                    },
                );
            }
            ChatEvent::ControlState(state) => {
                self.control.active_turn = state.active_turn;
                self.control.task_count = state.task_count;
                self.control.tasks = state.tasks;
                self.control.steering_count = state.steering_count;
                self.control.follow_up_count = state.follow_up_count;
                self.control.runtime_attached = state.runtime_attached;
                // The session snapshot is authoritative for the pending chips:
                // it carries ids + texts for everything still queued (including
                // inputs queued from other surfaces) and drops the consumed.
                self.queued_turn_inputs = state
                    .queued_inputs
                    .iter()
                    .map(QueuedTurnInput::from)
                    .collect();
                self.notice = Some(format!(
                    "agent control: {} steering, {} follow-ups",
                    state.steering_count, state.follow_up_count
                ));
            }
            ChatEvent::RuntimeInfo(info) => {
                self.info = info;
                self.sync_selection_from_info();
            }
            ChatEvent::ToolRuntime(selection) => {
                self.workspace_path_input = selection
                    .workspace_root
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_default();
                self.tool_runtime = selection;
                self.workspace_error = None;
            }
            ChatEvent::SessionReset { session_id } => {
                let acknowledged_pending =
                    self.pending_session_reset.as_deref() == Some(session_id.as_str());
                if acknowledged_pending {
                    self.pending_session_reset = None;
                    if !self.messages.is_empty() || self.streaming {
                        self.session_id = session_id;
                        return true;
                    }
                }
                self.session_id = session_id;
                self.messages.clear();
                self.timeline = ConversationTimelineState::default();
                self.streaming = false;
                self.queued_turn_inputs.clear();
                self.notice = None;
                self.context = None;
                self.usage = None;
                self.activity.clear();
                self.tool_calls.clear();
                self.tool_chain_expanded = false;
                self.expanded_tool_details.clear();
                self.control = ChatControlOverview::default();
                self.scroll = 0.0;
            }
            ChatEvent::Models { provider, models } => {
                let mut updated_current = false;
                for (idx, summary) in self.providers.iter_mut().enumerate() {
                    if summary.id == provider {
                        let merged = merge_model_lists(&summary.models, &models);
                        if !merged.is_empty() {
                            summary.models = merged;
                        }
                        updated_current |= idx == self.provider_idx;
                    }
                }
                // Re-align the model index to the current model if it's in the list.
                if updated_current {
                    self.model_idx = self.providers[self.provider_idx]
                        .models
                        .iter()
                        .position(|m| *m == self.info.model)
                        .unwrap_or(0);
                }
                self.models_loading = false;
            }
            ChatEvent::Error { session_id, error } => {
                tracing::warn!(
                    session_id = session_id.as_deref().unwrap_or_default(),
                    %error,
                    "chat runtime command failed"
                );
                self.streaming = false;
                self.control.active_turn = false;
                self.queued_turn_inputs.clear();
                if error.to_ascii_lowercase().contains("workspace") {
                    self.workspace_error = Some(error.clone());
                    self.notice = Some("Workspace directory needs attention.".to_string());
                } else if let Some(detail) = connection_problem_summary(&error) {
                    self.push_activity("chat connection", detail.clone(), ActivityTone::Warn);
                    if !self
                        .notice
                        .as_deref()
                        .is_some_and(text_mentions_connection_problem)
                    {
                        self.notice = Some(format!("chat connection failed: {detail}"));
                    }
                } else {
                    self.push_activity("agent error", error.clone(), ActivityTone::Bad);
                    self.notice = None;
                }
                Self::apply_error_message(&mut self.messages, &error);
                if self.stick_bottom {
                    self.scroll = f32::MAX;
                }
                self.persist();
                if Self::trim_messages_to_recent_window(&mut self.messages) {
                    self.timeline
                        .refresh_loaded_range_from_messages(&self.messages);
                    self.timeline.has_older = self
                        .timeline
                        .loaded_start_sequence
                        .is_some_and(|sequence| sequence > 1);
                }
            }
        }
        true
    }

    fn sync_selection_from_info(&mut self) {
        let matched_provider = if let Some(idx) = self.providers.iter().position(|p| {
            p.id == self.info.provider
                && self
                    .info
                    .account
                    .as_deref()
                    .is_none_or(|account| p.account == account)
        }) {
            self.provider_idx = idx;
            if let Some(midx) = self.providers[idx]
                .models
                .iter()
                .position(|m| *m == self.info.model)
            {
                self.model_idx = midx;
            }
            true
        } else {
            false
        };
        if let Some(effort) = &self.info.effort
            && let Some(idx) = self.efforts.iter().position(|e| e == effort)
        {
            self.effort_idx = idx;
        }
        // The runtime just told us which provider is active; if its model list has not been fetched
        // yet, kick the async fetch now so the model picker populates for the default provider
        // (without waiting for the user to toggle providers).
        self.maybe_fetch_models();
        if !matched_provider && !self.providers.is_empty() && !self.current_model().is_empty() {
            self.push_model_selection();
        }
    }

    fn submit(&mut self) {
        let text = self.input.trim().to_string();
        let attachments = self.attachment_inputs();
        if self.attachment_operation_pending() {
            self.notice = Some("wait for selected files to finish attaching".to_string());
            return;
        }
        if text.is_empty() && attachments.is_empty() {
            return;
        }
        if self.providers.is_empty() {
            self.notice = Some("set up a provider before sending".to_string());
            return;
        }
        if self.initial_transcript_load_pending() {
            self.notice = Some("wait for this chat history to finish loading".to_string());
            return;
        }
        let Some((provider, model, account)) = self.current_model_selection() else {
            self.notice = Some("choose a model before sending".to_string());
            return;
        };
        let session_id = self.ensure_visible_session_id();
        if self.streaming {
            if !attachments.is_empty() {
                self.notice = Some("send attachments after the current response".to_string());
                return;
            }
            // Mesh: mid-run typing steers the active turn instead of killing it.
            // The message is consumed by the next model step; Esc interrupts outright.
            self.handle.send_command(ChatCommand::Steer {
                session_id: session_id.clone(),
                message: text.clone(),
            });
            self.queued_turn_inputs.push(QueuedTurnInput {
                kind: QueuedTurnKind::Steer,
                text: text.clone(),
                input_id: None,
            });
            self.messages.push(ChatMessage::user(text));
            self.input.clear();
            self.cursor = 0;
            self.advance_attachment_draft_generation();
            self.attachment_path_input.clear();
            self.attachment_manual_path_open = false;
            self.attachment_controls_open = false;
            if self.composer_popup == Some(ComposerPopup::Attachments) {
                self.composer_popup = None;
            }
            // The input host keys on the revision; bump it so the focused
            // `TextArea` rebuilds empty rather than keeping the queued text.
            self.draft_revision = self.draft_revision.wrapping_add(1);
            self.notice = Some("queued steering".to_string());
            self.stick_bottom = true;
            self.scroll = f32::MAX;
            self.persist();
            return;
        }
        if !self.runtime_matches_selection(&provider, &model, &account) {
            self.handle.send_command(ChatCommand::SetModel {
                provider,
                model,
                account,
            });
        }
        let visible_text = visible_user_message_text(&text, &self.attachments);
        self.messages.push(ChatMessage::user(visible_text));
        self.messages.push(ChatMessage::model(String::new()));
        self.context = None;
        self.usage = None;
        self.activity.clear();
        self.tool_calls.clear();
        self.tool_chain_expanded = false;
        self.expanded_tool_details.clear();
        self.control.active_turn = true;
        self.input.clear();
        self.cursor = 0;
        self.advance_attachment_draft_generation();
        self.attachment_path_input.clear();
        self.attachment_manual_path_open = false;
        self.attachment_controls_open = false;
        if self.composer_popup == Some(ComposerPopup::Attachments) {
            self.composer_popup = None;
        }
        self.attachments.clear();
        self.streaming = true;
        self.stick_bottom = true;
        self.scroll = f32::MAX;
        self.notice = Some("thinking…".to_string());
        self.persist();
        self.handle.send_command(ChatCommand::Send {
            session_id,
            text,
            attachments,
        });
    }

    fn interrupt_turn(&mut self) {
        if !self.streaming {
            return;
        }
        self.handle.send_command(ChatCommand::Interrupt {
            session_id: self.session_id.clone(),
            reason: "user interrupted from app chat".to_string(),
        });
        self.notice = Some("interrupting…".to_string());
    }

    /// Attach the runtime queue id to the optimistic chip it confirms (newest
    /// same-kind, same-text chip still missing an id), or surface the queued
    /// input as a fresh chip when no optimistic echo exists.
    fn attach_queued_input_id(&mut self, queued: &ChatQueuedTurnInput) {
        let kind = QueuedTurnKind::from(queued.kind);
        if self
            .queued_turn_inputs
            .iter()
            .any(|item| item.input_id.as_deref() == Some(queued.input_id.as_str()))
        {
            return;
        }
        if let Some(item) = self.queued_turn_inputs.iter_mut().rev().find(|item| {
            item.input_id.is_none() && item.kind == kind && item.text == queued.message
        }) {
            item.input_id = Some(queued.input_id.clone());
            return;
        }
        self.queued_turn_inputs.push(QueuedTurnInput::from(queued));
    }

    /// Remove a pending queued message: optimistically drop the chip and ask
    /// the runtime to delete it from the session queue. If the runtime already
    /// consumed it, the removal is moot and the error path reports the race.
    fn remove_queued_turn_input(&mut self, index: usize) {
        let Some(item) = self.queued_turn_inputs.get(index) else {
            return;
        };
        let Some(input_id) = item.input_id.clone() else {
            self.notice = Some("queued message is still registering".to_string());
            return;
        };
        self.queued_turn_inputs.remove(index);
        self.handle.send_command(ChatCommand::RemoveQueuedInput {
            session_id: self.session_id.clone(),
            input_id,
        });
        self.notice = Some("removing queued message…".to_string());
    }

    /// Mesh: Esc is interruption-first-class. One press closes an open composer
    /// popup; the next (or the first, with nothing open) halts the active turn.
    /// Returns whether the key was consumed. The session-edit dialog owns Esc
    /// while it is open, so callers skip this entirely in that state.
    fn handle_escape(&mut self) -> bool {
        if self.composer_popup.is_some() {
            self.set_composer_popup(None);
            return true;
        }
        if self.streaming {
            self.interrupt_turn();
            return true;
        }
        false
    }

    // -- Composed-view action methods (mutation entry points for the widgets) --

    /// Whether the composer has a non-empty draft that can be submitted.
    fn can_send(&self) -> bool {
        (!self.input.trim().is_empty() || !self.attachments.is_empty())
            && !self.providers.is_empty()
            && !self.initial_transcript_load_pending()
            && !self.attachment_operation_pending()
    }

    fn latest_reusable_answer(&self) -> Option<&str> {
        self.messages.iter().rev().find_map(|message| {
            (message.source == ChatMessageSource::Model
                && message.kind == ChatMessageKind::Message
                && !message.text.trim().is_empty())
            .then_some(message.text.as_str())
        })
    }

    fn can_reuse_latest_answer(&self) -> bool {
        !self.streaming
            && !self.providers.is_empty()
            && !self.initial_transcript_load_pending()
            && self.input.trim().is_empty()
            && self.attachments.is_empty()
            && self.attachment_path_input.trim().is_empty()
            && !self.attachment_operation_pending()
            && self.latest_reusable_answer().is_some()
    }

    fn use_latest_answer_as_draft(&mut self) {
        let Some(answer) = self.latest_reusable_answer().map(str::to_string) else {
            return;
        };
        self.input = answer;
        self.cursor = self.input.len();
        self.advance_attachment_draft_generation();
        self.attachment_path_input.clear();
        self.attachment_manual_path_open = false;
        self.attachment_controls_open = false;
        self.attachments.clear();
        self.draft_revision = self.draft_revision.wrapping_add(1);
    }

    /// Replace the configured provider-account list after Settings changes it.
    pub fn refresh_providers(&mut self, providers: Vec<ProviderSummary>) {
        let previous = self
            .current_provider()
            .map(|provider| (provider.id.clone(), provider.account.clone()));
        self.providers_loading = false;
        self.providers = providers;
        self.models_loading = false;
        if self.providers.is_empty() {
            self.provider_idx = 0;
            self.model_idx = 0;
            return;
        }
        self.provider_idx = previous
            .and_then(|(provider, account)| {
                self.providers
                    .iter()
                    .position(|candidate| candidate.id == provider && candidate.account == account)
            })
            .unwrap_or(0);
        let model_len = self.current_provider().map(|p| p.models.len()).unwrap_or(0);
        if self.model_idx >= model_len {
            self.model_idx = 0;
        }
        self.maybe_fetch_models();
        if !self.current_model().is_empty() {
            self.push_model_selection();
        }
    }

    pub fn start_provider_list_load(
        &mut self,
        config: Arc<dyn ConfigClient>,
        waker: Option<sui::UiHandle>,
    ) -> bool {
        if !self.providers_loading || self.provider_list_rx.is_some() {
            return false;
        }
        let (tx, rx) = mpsc::channel();
        match thread::Builder::new()
            .name("sinomo-app-chat-providers".to_string())
            .spawn(move || {
                let providers = crate::backend::config::cluster_provider_summaries(config.as_ref());
                let _ = tx.send(ProviderListWorkerResult { providers });
                if let Some(waker) = waker {
                    waker.wake();
                }
            }) {
            Ok(_) => {
                self.provider_list_rx = Some(rx);
                true
            }
            Err(error) => {
                self.providers_loading = false;
                self.replace_activity(
                    "providers",
                    format!("provider list failed: {error}"),
                    ActivityTone::Warn,
                );
                true
            }
        }
    }

    pub fn pump_provider_list(&mut self) -> bool {
        let Some(rx) = self.provider_list_rx.as_ref() else {
            return false;
        };
        match rx.try_recv() {
            Ok(result) => {
                self.provider_list_rx = None;
                self.refresh_providers(result.providers);
                true
            }
            Err(TryRecvError::Empty) => false,
            Err(TryRecvError::Disconnected) => {
                self.provider_list_rx = None;
                self.providers_loading = false;
                self.replace_activity(
                    "providers",
                    "provider list failed: worker exited",
                    ActivityTone::Warn,
                );
                true
            }
        }
    }

    /// Load the stored (SIFS-backed) agent catalog in the background. The composer
    /// starts on the built-ins; this upgrades it once — or reports the catalog as
    /// degraded — without ever blocking the boot path on the distributed file system.
    pub fn start_agent_list_load(
        &mut self,
        data_dir: Arc<SinomoDataDir>,
        waker: Option<sui::UiHandle>,
    ) -> bool {
        if !self.agents_loading || self.agent_list_rx.is_some() {
            return false;
        }
        let (tx, rx) = mpsc::channel();
        match thread::Builder::new()
            .name("sinomo-app-chat-agents".to_string())
            .spawn(move || {
                let result = match crate::backend::run_with_timeout(
                    "agent-catalog",
                    AGENT_CATALOG_LOAD_TIMEOUT,
                    move || agent_summaries_with_error(&data_dir),
                ) {
                    Ok((agents, error)) => AgentListWorkerResult {
                        agents: Some(agents),
                        error,
                    },
                    Err(error) => AgentListWorkerResult {
                        agents: None,
                        error: Some(error),
                    },
                };
                let _ = tx.send(result);
                if let Some(waker) = waker {
                    waker.wake();
                }
            }) {
            Ok(_) => {
                self.agent_list_rx = Some(rx);
                true
            }
            Err(error) => {
                self.agents_loading = false;
                self.replace_activity(
                    "agents",
                    format!("agent catalog load failed: {error}"),
                    ActivityTone::Warn,
                );
                true
            }
        }
    }

    pub fn pump_agent_list(&mut self) -> bool {
        let Some(rx) = self.agent_list_rx.as_ref() else {
            return false;
        };
        match rx.try_recv() {
            Ok(result) => {
                self.agent_list_rx = None;
                self.agents_loading = false;
                if let Some(agents) = result.agents
                    && !agents.is_empty()
                {
                    self.refresh_agents(agents);
                }
                if let Some(error) = result.error {
                    tracing::warn!(%error, "chat agent catalog load degraded");
                    self.replace_activity(
                        "agents",
                        format!("stored agent catalog unavailable (using built-ins): {error}"),
                        ActivityTone::Warn,
                    );
                }
                true
            }
            Err(TryRecvError::Empty) => false,
            Err(TryRecvError::Disconnected) => {
                self.agent_list_rx = None;
                self.agents_loading = false;
                self.replace_activity(
                    "agents",
                    "agent catalog load failed: worker exited",
                    ActivityTone::Warn,
                );
                true
            }
        }
    }

    /// Replace the selectable agent catalog after Settings changes custom definitions.
    pub fn refresh_agents(&mut self, agents: Vec<AgentSummary>) {
        let previous = self.info.agent.clone();
        self.agents = agents;
        if self.agents.is_empty() {
            self.channel_agent_idx = 0;
            return;
        }
        if self.channel_agent_idx >= self.agents.len() {
            self.channel_agent_idx = 0;
        }
        if !previous.trim().is_empty()
            && !self
                .agents
                .iter()
                .any(|agent| agent.key == previous || agent.name == previous)
        {
            self.info.agent = self
                .agents
                .iter()
                .find(|agent| agent.key == "main-agent" || agent.name == "main-agent")
                .or_else(|| self.agents.first())
                .map(|agent| agent.key.clone())
                .unwrap_or_default();
        }
    }

    /// Update the composer draft (bound to the composer `TextArea` via its `on_change`).
    fn set_draft(&mut self, text: String) {
        self.cursor = text.len();
        self.input = text;
    }

    /// Reset the draft, attachments and pending attachment entry. The Mesh
    /// composer has no Clear button (Send is its one primary action), so this
    /// is a state-level affordance for quick actions and tests.
    #[cfg_attr(not(test), allow(dead_code))]
    fn clear_draft(&mut self) {
        if self.input.is_empty()
            && self.attachments.is_empty()
            && self.attachment_path_input.is_empty()
            && !self.attachment_operation_pending()
        {
            return;
        }
        self.input.clear();
        self.cursor = 0;
        self.advance_attachment_draft_generation();
        self.attachment_path_input.clear();
        self.attachment_manual_path_open = false;
        self.attachment_controls_open = false;
        if self.composer_popup == Some(ComposerPopup::Attachments) {
            self.composer_popup = None;
        }
        self.attachments.clear();
        self.draft_revision = self.draft_revision.wrapping_add(1);
    }

    fn attachment_controls_visible(&self) -> bool {
        self.attachment_controls_open
    }

    fn attachment_picker_pending(&self) -> bool {
        self.pending_attachment_picker.is_some()
    }

    fn attachment_picker_lane_pending(&self) -> bool {
        self.attachment_picker.is_pending()
    }

    fn attachment_operation_pending(&self) -> bool {
        self.attachment_picker_pending() || !self.pending_attachment_validations.is_empty()
    }

    fn advance_attachment_draft_generation(&mut self) {
        self.attachment_draft_generation = self.attachment_draft_generation.wrapping_add(1);
        self.pending_attachment_picker = None;
        self.pending_attachment_validations.clear();
        self.attachment_picker_error = None;
        self.attachment_drop_hovered = false;
    }

    fn request_attachment_picker(&mut self) {
        self.attachment_picker_error = None;
        let session_id = self.session_id.clone();
        let draft_generation = self.attachment_draft_generation;
        match self.attachment_picker.request(
            NativePickerKind::MultipleFiles,
            "Choose files to attach",
            draft_generation,
            self.ui_waker.clone(),
        ) {
            Ok(request_id) => {
                self.pending_attachment_picker = Some(PendingAttachmentPicker {
                    request_id,
                    session_id,
                    draft_generation,
                });
            }
            Err(error) => {
                self.attachment_picker_error = Some(error);
            }
        }
        self.draft_revision = self.draft_revision.wrapping_add(1);
    }

    /// Drain native-picker and path-validation completions without blocking the UI thread.
    /// Shell may call this after an external wake; `pump_worker_events` also calls it so Chat owns
    /// the lane when it is visible.
    pub(crate) fn pump_attachment_lanes(&mut self) -> bool {
        let mut changed = false;
        if let Some(result) = self.attachment_picker.poll() {
            self.apply_attachment_picker_result(result);
            changed = true;
        }
        loop {
            match self.attachment_validation_rx.try_recv() {
                Ok(result) => {
                    self.apply_attachment_validation_result(result);
                    changed = true;
                }
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }
        changed
    }

    fn apply_attachment_picker_result(&mut self, result: NativePickerResult) {
        let Some(pending) = self.pending_attachment_picker.take() else {
            // The visible session or draft was reset while the OS dialog was open.
            return;
        };
        // A fresh chat adopts its first runtime session id without replacing the visible draft.
        // Once an id exists, any different id means the picker result is stale.
        if pending.request_id != result.request_id
            || (!pending.session_id.is_empty() && pending.session_id != self.session_id)
            || pending.draft_generation != self.attachment_draft_generation
            || result.context_generation != self.attachment_draft_generation
        {
            return;
        }

        match result.outcome {
            NativePickerOutcome::Selected(paths) => {
                if let Err(error) = self.request_local_attachment_validation(
                    paths,
                    LocalAttachmentIngress::NativePicker,
                ) {
                    self.attachment_picker_error = Some(error);
                }
            }
            NativePickerOutcome::Cancelled => {
                self.attachment_picker_error = None;
            }
            NativePickerOutcome::Failed(error) => {
                self.attachment_picker_error = Some(error);
            }
        }
        self.draft_revision = self.draft_revision.wrapping_add(1);
    }

    fn apply_attachment_validation_result(&mut self, result: LocalAttachmentValidationResult) {
        let Some(index) = self
            .pending_attachment_validations
            .iter()
            .position(|pending| pending.request_id == result.request_id)
        else {
            // The session/draft was reset while the validation worker was in flight.
            return;
        };
        let pending = self.pending_attachment_validations.remove(index);
        // The runtime may assign the first session id while validation is in flight. Draft
        // generation still rejects actual navigation or composer resets.
        if (!pending.session_id.is_empty() && pending.session_id != self.session_id)
            || pending.session_id != result.session_id
            || pending.draft_generation != self.attachment_draft_generation
            || pending.draft_generation != result.draft_generation
        {
            return;
        }

        let mut errors = Vec::new();
        for outcome in result.outcomes {
            match outcome {
                LocalAttachmentValidationOutcome::Ready {
                    path,
                    file_name,
                    size_bytes,
                } => {
                    let source = ChatAttachmentSource::LocalPath(path);
                    if !self
                        .attachments
                        .iter()
                        .any(|attachment| attachment.has_source(&source))
                    {
                        self.attachments.push(PendingChatAttachment {
                            source,
                            file_name,
                            size_bytes,
                        });
                    }
                }
                LocalAttachmentValidationOutcome::Failed { path, detail } => {
                    errors.push(format!("{}: {detail}", path.display()));
                }
            }
        }
        self.attachment_picker_error = if errors.is_empty() {
            None
        } else {
            Some(errors.join(" "))
        };
        self.draft_revision = self.draft_revision.wrapping_add(1);
    }

    fn request_local_attachment_validation(
        &mut self,
        paths: Vec<PathBuf>,
        ingress: LocalAttachmentIngress,
    ) -> Result<u64, String> {
        let mut paths: Vec<PathBuf> = paths
            .into_iter()
            .filter(|path| !path.as_os_str().is_empty())
            .collect();
        paths.dedup();
        if paths.is_empty() {
            let detail = "choose at least one file to attach".to_string();
            self.attachment_picker_error = Some(detail.clone());
            return Err(detail);
        }

        self.next_attachment_validation_request_id = self
            .next_attachment_validation_request_id
            .wrapping_add(1)
            .max(1);
        let request_id = self.next_attachment_validation_request_id;
        let session_id = self.session_id.clone();
        let draft_generation = self.attachment_draft_generation;
        let worker_paths = paths.clone();
        let worker_session_id = session_id.clone();
        let sender = self.attachment_validation_tx.clone();
        let waker = self.ui_waker.clone();
        let spawn = thread::Builder::new()
            .name(format!("sinomo-chat-attachment-{request_id}"))
            .spawn(move || {
                let outcomes = worker_paths
                    .into_iter()
                    .map(validate_local_attachment_path)
                    .collect();
                let _ = sender.send(LocalAttachmentValidationResult {
                    request_id,
                    session_id: worker_session_id,
                    draft_generation,
                    outcomes,
                });
                if let Some(waker) = waker {
                    waker.wake();
                }
            });
        if let Err(error) = spawn {
            let detail = format!("could not inspect the selected files: {error}");
            self.attachment_picker_error = Some(detail.clone());
            return Err(detail);
        }

        self.pending_attachment_validations
            .push(PendingLocalAttachmentValidation {
                request_id,
                session_id,
                draft_generation,
                ingress,
                paths,
            });
        self.attachment_picker_error = None;
        self.draft_revision = self.draft_revision.wrapping_add(1);
        Ok(request_id)
    }

    fn toggle_manual_attachment_path(&mut self) {
        self.attachment_manual_path_open = !self.attachment_manual_path_open;
        if !self.attachment_manual_path_open {
            self.attachment_path_input.clear();
        }
        self.attachment_picker_error = None;
        self.draft_revision = self.draft_revision.wrapping_add(1);
    }

    fn set_attachment_path_input(&mut self, value: String) {
        self.attachment_path_input = value;
        self.attachment_picker_error = None;
    }

    fn add_attachment_from_path_input(&mut self) {
        let value = self.attachment_path_input.clone();
        match self.queue_attachment_path(&value) {
            Ok(()) => {
                self.attachment_path_input.clear();
            }
            Err(error) => {
                self.attachment_picker_error = Some(error);
            }
        }
    }

    fn queue_attachment_path(&mut self, value: &str) -> Result<(), String> {
        let normalized = normalize_attachment_path(value)
            .ok_or_else(|| "enter a file path before attaching".to_string())?;
        self.queue_local_attachment_path(PathBuf::from(normalized))
    }

    /// Queue a host file for background validation. This does no filesystem I/O on the caller's
    /// thread, so native-picker, pasted-path, and OS-drop entry points share the same safe lane.
    ///
    /// The path remains the source until the chat worker accepts the turn, preserving the existing
    /// local-file behavior while giving shell integration one typed entry point.
    pub(crate) fn queue_local_attachment_path(
        &mut self,
        path: impl Into<PathBuf>,
    ) -> Result<(), String> {
        self.request_local_attachment_validation(
            vec![path.into()],
            LocalAttachmentIngress::ManualPath,
        )
        .map(|_| ())
    }

    /// Queue files received from SUI's root drag/drop event and open the attachment surface.
    pub(crate) fn queue_dropped_local_attachment_paths(
        &mut self,
        paths: Vec<PathBuf>,
    ) -> Result<(), String> {
        self.attachment_drop_hovered = false;
        self.open_attachment_surface();
        self.request_local_attachment_validation(paths, LocalAttachmentIngress::OsDrop)
            .map(|_| ())
    }

    pub(crate) fn queue_dropped_local_attachment_path(
        &mut self,
        path: PathBuf,
    ) -> Result<(), String> {
        self.queue_dropped_local_attachment_paths(vec![path])
    }

    /// Reveal the attachment panel while a host-file drag is over the window.
    pub(crate) fn begin_external_attachment_hover(&mut self) {
        if self.attachment_drop_hovered {
            return;
        }
        self.attachment_drop_hovered = true;
        self.open_attachment_surface();
        self.draft_revision = self.draft_revision.wrapping_add(1);
    }

    /// Clear the transient drag target when the pointer leaves or the host cancels the drag.
    pub(crate) fn cancel_external_attachment_hover(&mut self) {
        if self.attachment_drop_hovered {
            self.attachment_drop_hovered = false;
            self.draft_revision = self.draft_revision.wrapping_add(1);
        }
    }

    pub(crate) fn open_attachment_surface(&mut self) {
        self.set_composer_popup(Some(ComposerPopup::Attachments));
    }

    /// Queue the exact local bytes already displayed by Files. Unlike picker/drop paths, this
    /// snapshot is never reopened after the user chooses "Attach to current chat".
    #[cfg(test)]
    pub(crate) fn queue_local_attachment_snapshot(
        &mut self,
        file_name: impl Into<String>,
        source_path: impl Into<String>,
        bytes: impl Into<Arc<[u8]>>,
    ) -> Result<(), String> {
        self.queue_local_attachment_snapshot_with_provenance(
            file_name,
            ChatLocalAttachmentProvenance::new(source_path),
            bytes,
        )
    }

    pub(crate) fn queue_local_attachment_snapshot_with_provenance(
        &mut self,
        file_name: impl Into<String>,
        provenance: ChatLocalAttachmentProvenance,
        bytes: impl Into<Arc<[u8]>>,
    ) -> Result<(), String> {
        if provenance.source_path().trim().is_empty() {
            let detail = "local attachment source is missing".to_string();
            self.attachment_picker_error = Some(detail.clone());
            return Err(detail);
        }
        let bytes = bytes.into();
        let file_name = file_name.into();
        let file_name = if file_name.trim().is_empty() {
            FsPath::new(provenance.source_path())
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|name| !name.trim().is_empty())
                .unwrap_or("attachment")
                .to_string()
        } else {
            file_name.trim().to_string()
        };
        let source = ChatAttachmentSource::LocalSnapshot {
            provenance,
            bytes: bytes.clone(),
        };
        let pending = PendingChatAttachment {
            source: source.clone(),
            file_name,
            size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        };
        if let Some(existing) = self
            .attachments
            .iter_mut()
            .find(|attachment| attachment.has_source(&source))
        {
            *existing = pending;
        } else {
            self.attachments.push(pending);
        }
        self.open_attachment_surface();
        self.attachment_picker_error = None;
        self.draft_revision = self.draft_revision.wrapping_add(1);
        Ok(())
    }

    /// Queue an immutable SIFS viewer snapshot without staging it through a local temporary file.
    /// The SIFS path is retained as provenance when the chat worker stores the attachment.
    #[cfg(test)]
    pub(crate) fn queue_sifs_attachment_snapshot(
        &mut self,
        file_name: impl Into<String>,
        sifs_path: impl Into<String>,
        bytes: impl Into<Arc<[u8]>>,
    ) -> Result<(), String> {
        self.queue_sifs_attachment_snapshot_with_provenance(
            file_name,
            ChatSifsAttachmentProvenance::new(sifs_path),
            bytes,
        )
    }

    pub(crate) fn queue_sifs_attachment_snapshot_with_provenance(
        &mut self,
        file_name: impl Into<String>,
        provenance: ChatSifsAttachmentProvenance,
        bytes: impl Into<Arc<[u8]>>,
    ) -> Result<(), String> {
        if provenance.path.trim().is_empty() {
            let detail = "SIFS attachment path is missing".to_string();
            self.attachment_picker_error = Some(detail.clone());
            return Err(detail);
        }
        let bytes = bytes.into();
        let file_name = file_name.into();
        let file_name = if file_name.trim().is_empty() {
            FsPath::new(&provenance.path)
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|name| !name.trim().is_empty())
                .unwrap_or("attachment")
                .to_string()
        } else {
            file_name.trim().to_string()
        };
        let source = ChatAttachmentSource::SifsSnapshot {
            provenance,
            bytes: bytes.clone(),
        };
        let pending = PendingChatAttachment {
            source: source.clone(),
            file_name,
            size_bytes: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        };
        if let Some(existing) = self
            .attachments
            .iter_mut()
            .find(|attachment| attachment.has_source(&source))
        {
            *existing = pending;
        } else {
            self.attachments.push(pending);
        }
        self.open_attachment_surface();
        self.attachment_picker_error = None;
        self.draft_revision = self.draft_revision.wrapping_add(1);
        Ok(())
    }

    fn remove_attachment(&mut self, index: usize) {
        if index >= self.attachments.len() {
            return;
        }
        self.attachments.remove(index);
        self.draft_revision = self.draft_revision.wrapping_add(1);
    }

    fn attachment_inputs(&self) -> Vec<ChatAttachmentInput> {
        self.attachments
            .iter()
            .map(PendingChatAttachment::input)
            .collect()
    }

    fn apply_quick_action(&mut self, action: ChatQuickAction) {
        match action {
            ChatQuickAction::NewChannel => self.new_channel(None),
            ChatQuickAction::ManageChannel => self.begin_edit_current_session(),
        }
    }

    /// Select an agent by sidebar index and tell the worker to switch.
    fn select_agent(&mut self, index: usize) {
        let Some(agent) = self.agents.get(index) else {
            return;
        };
        self.compact_drawer_open = false;
        if self.current_is_channel() {
            self.channel_agent_idx = index;
            self.begin_edit_current_session();
            self.notice = Some("use channel config to add agents".to_string());
            return;
        }
        if self.initial_transcript_load_pending() {
            self.notice = Some("wait for this chat history to finish loading".to_string());
            return;
        }
        if self.visible_session_has_started() {
            self.notice = Some("start a new chat before switching agents".to_string());
            return;
        }
        let key = agent.key.clone();
        let session_id = self.ensure_visible_session_id();
        let history = self.current_history();
        self.info.agent = key.clone();
        self.handle.send_command(ChatCommand::SetAgent {
            session_id,
            key,
            history,
        });
    }

    /// Whether `index` is the currently-active agent (matched by key or display name).
    fn is_active_agent(&self, index: usize) -> bool {
        self.active_agent_index() == Some(index)
    }

    fn active_agent_index(&self) -> Option<usize> {
        if let Some(index) = self
            .agents
            .iter()
            .position(|agent| agent.name == self.info.agent || agent.key == self.info.agent)
        {
            return Some(index);
        }
        if self.info.agent.trim().is_empty() {
            return self
                .agents
                .iter()
                .position(|agent| agent.key == "main-agent" || agent.name == "main-agent")
                .or_else(|| (!self.agents.is_empty()).then_some(0));
        }
        None
    }

    /// Start a fresh session from the sidebar "New chat" action.
    fn new_session(&mut self) {
        self.compact_drawer_open = false;
        self.persist();
        let interrupted_session_id = self.session_id.clone();
        let session_id = SessionId::generate().to_string();
        if self.streaming || self.control.active_turn {
            self.handle.send_command(ChatCommand::Interrupt {
                session_id: interrupted_session_id,
                reason: "user started a new chat from app chat".to_string(),
            });
        }
        self.reset_visible_session(session_id.clone());
        self.pending_session_reset = Some(session_id.clone());
        self.handle
            .send_command(ChatCommand::NewSession { session_id });
    }

    /// Start a fresh explicit channel from the sidebar "New channel" action.
    fn new_channel(&mut self, waker: Option<sui::UiHandle>) {
        if self.channel_create_rx.is_some() {
            self.notice = Some("wait for the current channel to finish creating".to_string());
            return;
        }
        self.compact_drawer_open = false;
        self.persist();
        let interrupted_session_id = self.session_id.clone();
        let session_id = SessionId::generate().to_string();
        if self.streaming || self.control.active_turn {
            self.handle.send_command(ChatCommand::Interrupt {
                session_id: interrupted_session_id,
                reason: "user started a new channel from app chat".to_string(),
            });
        }
        let mut channel = AgentChannel::new_channel(
            session_id
                .parse::<SessionId>()
                .unwrap_or_else(|_| SessionId::generate()),
            "New channel",
        );
        channel.ensure_user(sinomo_agent::AgentChannelUser::app_local());
        let (agent_key, agent_name) = self.default_channel_agent();
        let participant_id = channel.ensure_participant(agent_key.clone(), agent_name);
        let tool_runtime = self.agent_tool_runtime(&agent_key);
        set_channel_participant_tool_runtime(&mut channel, &participant_id, &tool_runtime);
        let session_ref = session_ref_for_channel(&channel);

        self.reset_visible_session(session_id.clone());
        self.pending_session_reset = Some(session_id.clone());
        self.upsert_session_ref(session_ref.clone());
        self.session_edit_id = Some(session_id.clone());
        self.session_edit_details_open = false;
        self.session_title_draft = session_ref.title.clone();
        self.session_delete_confirm_id = None;
        self.channel_agent_idx = 0;
        self.notice = Some("creating channel...".to_string());
        self.replace_activity(
            "channel create",
            "Saving channel to SIFS".to_string(),
            ActivityTone::Running,
        );
        self.handle
            .send_command(ChatCommand::NewSession { session_id });

        let (tx, rx) = mpsc::channel();
        let data_dir = self.data_dir.clone();
        let sifs_daemon = self.sifs_daemon.clone();
        let worker_channel = channel;
        let worker_session = session_ref;
        let worker_waker = waker.clone();
        let spawn_result = thread::Builder::new()
            .name("sinomo-app-channel-create".to_string())
            .spawn(move || {
                let result = open_agent_channel_store(&data_dir, sifs_daemon.as_ref())
                    .and_then(|store| {
                        store
                            .save_channel(&worker_channel)
                            .map_err(channel_store_io_error)?;
                        let (sessions, error) =
                            list_conversations_with_error(&data_dir, sifs_daemon.as_ref());
                        Ok(SessionListWorkerResult { sessions, error })
                    })
                    .map_err(|error| error.to_string());
                let _ = tx.send(ChannelCreateWorkerResult {
                    session: worker_session,
                    result,
                });
                if let Some(waker) = worker_waker {
                    waker.wake();
                }
            });

        match spawn_result {
            Ok(_) => {
                self.channel_create_rx = Some(rx);
            }
            Err(error) => {
                self.channel_create_rx = None;
                let detail = error.to_string();
                self.notice = Some(format!("channel create failed: {detail}"));
                self.push_activity("channel create", detail, ActivityTone::Warn);
            }
        }
    }

    fn default_channel_agent(&self) -> (String, String) {
        let default_index = self.active_agent_index().or_else(|| {
            self.agents
                .iter()
                .position(|agent| agent.key == "main-agent" || agent.name == "main-agent")
                .or_else(|| (!self.agents.is_empty()).then_some(0))
        });
        if let Some(agent) = default_index.and_then(|index| self.agents.get(index)) {
            return (agent.key.clone(), agent.name.clone());
        }
        let key = if self.info.agent.trim().is_empty() {
            "main-agent".to_string()
        } else {
            self.info.agent.clone()
        };
        (key.clone(), key)
    }

    fn agent_tool_runtime(&self, agent_key: &str) -> ChatToolRuntimeSelection {
        self.agents
            .iter()
            .find(|agent| agent.key == agent_key || agent.name == agent_key)
            .map(|agent| agent.tool_runtime.clone())
            .unwrap_or_default()
    }

    fn set_channel_agent_idx(&mut self, index: usize) {
        self.channel_agent_idx = index.min(self.agents.len().saturating_sub(1));
    }

    fn add_selected_agent_to_channel(&mut self) {
        if self.pending_channel_config.is_some() {
            return;
        }
        let Some(channel_id) = self.session_edit_id.clone() else {
            return;
        };
        if !self
            .sessions
            .iter()
            .any(|session| session.id == channel_id && session.kind == ConversationKind::Channel)
        {
            return;
        }
        let Some(agent) = self.agents.get(self.channel_agent_idx) else {
            return;
        };
        let agent_key = agent.key.clone();
        let agent_name = agent.name.clone();
        let tool_runtime = agent.tool_runtime.clone();
        self.start_channel_config(
            channel_id,
            format!("adding {agent_name}"),
            format!("added {agent_name} to channel"),
            ChannelConfigOperation::AddAgent {
                agent_key,
                agent_name,
                tool_runtime,
            },
        );
    }

    fn remove_channel_agent(&mut self, channel_id: &str, participant_id: &str) {
        if self.pending_channel_config.is_some() {
            return;
        }
        self.start_channel_config(
            channel_id.to_string(),
            "removing agent".to_string(),
            "removed agent from channel".to_string(),
            ChannelConfigOperation::RemoveAgent {
                participant_id: participant_id.to_string(),
            },
        );
    }

    fn start_channel_config(
        &mut self,
        channel_id: String,
        action: String,
        success_notice: String,
        operation: ChannelConfigOperation,
    ) {
        if self.pending_channel_config.is_some() {
            return;
        }
        self.next_channel_config_request_id =
            self.next_channel_config_request_id.wrapping_add(1).max(1);
        let request_id = self.next_channel_config_request_id;
        let pending = PendingChannelConfig {
            request_id,
            channel_id: channel_id.clone(),
            action: action.clone(),
        };
        let (tx, rx) = mpsc::channel();
        let data_dir = self.data_dir.clone();
        let sifs_daemon = self.sifs_daemon.clone();
        let worker_channel_id = channel_id.clone();
        let worker_action = action.clone();
        let worker_waker = self.ui_waker.clone();

        self.pending_channel_config = Some(pending);
        self.channel_config_error = None;
        self.notice = Some(format!("{action} in channel…"));
        self.replace_activity(
            "channel config",
            format!("{action} in SIFS"),
            ActivityTone::Running,
        );

        let spawn = thread::Builder::new()
            .name(format!("sinomo-app-channel-config-{request_id}"))
            .spawn(move || {
                let result = update_channel_membership_in_store(
                    data_dir.as_ref(),
                    sifs_daemon.as_ref(),
                    &worker_channel_id,
                    operation,
                )
                .map_err(|error| error.to_string());
                let _ = tx.send(ChannelConfigWorkerResult {
                    request_id,
                    channel_id: worker_channel_id,
                    action: worker_action,
                    success_notice,
                    result,
                });
                if let Some(waker) = worker_waker {
                    waker.wake();
                }
            });

        match spawn {
            Ok(_) => self.channel_config_rx = Some(rx),
            Err(error) => {
                self.pending_channel_config = None;
                self.channel_config_rx = None;
                let detail = format!("could not start channel config worker: {error}");
                self.channel_config_error = Some(ChannelConfigError {
                    channel_id,
                    detail: detail.clone(),
                });
                self.notice = Some(format!("channel config failed: {detail}"));
                self.replace_activity("channel config", detail, ActivityTone::Warn);
            }
        }
    }

    fn pump_channel_config(&mut self) -> bool {
        let result = match self.channel_config_rx.as_ref().map(|rx| rx.try_recv()) {
            Some(Ok(result)) => result,
            Some(Err(TryRecvError::Empty)) | None => return false,
            Some(Err(TryRecvError::Disconnected)) => {
                self.channel_config_rx = None;
                let Some(pending) = self.pending_channel_config.take() else {
                    return false;
                };
                let detail = format!(
                    "worker stopped before {} completed; retry the channel update",
                    pending.action
                );
                self.channel_config_error = Some(ChannelConfigError {
                    channel_id: pending.channel_id,
                    detail: detail.clone(),
                });
                self.notice = Some(format!("channel config failed: {detail}"));
                self.replace_activity("channel config", detail, ActivityTone::Warn);
                return true;
            }
        };

        if self
            .pending_channel_config
            .as_ref()
            .is_none_or(|pending| pending.request_id != result.request_id)
        {
            // A reconnect or newer channel edit owns the UI now.
            return false;
        }
        self.channel_config_rx = None;
        self.pending_channel_config = None;
        match result.result {
            Ok(()) => {
                self.channel_config_error = None;
                self.notice = Some(result.success_notice);
                self.activity
                    .retain(|entry| entry.title != "channel config");
                self.start_session_list_load(self.ui_waker.clone());
            }
            Err(detail) => {
                self.channel_config_error = Some(ChannelConfigError {
                    channel_id: result.channel_id,
                    detail: detail.clone(),
                });
                self.notice = Some(format!("channel config failed: {detail}"));
                self.replace_activity(
                    "channel config",
                    format!("{} failed: {detail}", result.action),
                    ActivityTone::Warn,
                );
            }
        }
        true
    }

    /// Push the current provider/model selection to the worker.
    fn push_model_selection(&mut self) {
        if let Some(command) = self.current_model_selection_command() {
            self.handle.send_command(command);
        }
    }

    /// Select a provider by index: reset the model to the first, fetch its models if needed, and
    /// push the new selection to the worker.
    fn set_provider(&mut self, index: usize) {
        if index >= self.providers.len() {
            return;
        }
        self.provider_idx = index;
        self.model_idx = 0;
        self.maybe_fetch_models();
        self.push_model_selection();
    }

    /// Select a model by index within the current provider and push it to the worker.
    fn set_model(&mut self, index: usize) {
        let len = self.current_provider().map(|p| p.models.len()).unwrap_or(0);
        if index < len {
            self.model_idx = index;
            self.push_model_selection();
        }
    }

    /// Select a reasoning-effort option by index.
    fn set_effort(&mut self, index: usize) {
        if index < self.efforts.len() {
            self.effort_idx = index;
        }
    }

    /// Ask the worker to fetch the current provider's models from sinomo-lm, unless they are already
    /// loaded or a fetch is in flight. Results arrive via [`ChatEvent::Models`].
    fn maybe_fetch_models(&mut self) {
        if self.models_loading {
            return;
        }
        let needs_fetch = self
            .current_provider()
            .map(|p| p.models.len() <= 1)
            .unwrap_or(false);
        if !needs_fetch {
            return;
        }
        if let Some(provider) = self.current_provider().map(|p| p.id.clone()) {
            self.models_loading = true;
            self.handle
                .send_command(ChatCommand::FetchModels { provider });
        }
    }
}

impl Drop for ChatState {
    fn drop(&mut self) {
        if let Some(abort) = self.daemon_event_abort.take() {
            abort.abort();
        }
    }
}

fn spawn_daemon_event_subscription(
    runtime: &tokio::runtime::Handle,
    data_dir: SinomoDataDir,
    address: String,
    tx: Sender<DaemonEventWorkerUpdate>,
    waker: Option<sui::UiHandle>,
) -> tokio::task::AbortHandle {
    let task = runtime.spawn(async move {
        let request = match ObjectRpcRequest::encode_body(
            DAEMON_EVENT_SUBSCRIBE_ROUTE,
            DAEMON_RPC_CATEGORY,
            &DaemonEventSubscribeRequest::for_path_prefix(
                sinomo_cli::app_api::SIFS_AGENT_CHANNELS_ROOT,
            ),
        ) {
            Ok(request) => request,
            Err(error) => {
                let _ = tx.send(DaemonEventWorkerUpdate::Disconnected(format!(
                    "chat update subscription could not be created: {error}"
                )));
                if let Some(waker) = waker {
                    waker.wake();
                }
                return;
            }
        };
        let mut retry_delay = Duration::from_millis(250);
        loop {
            let mut received_ready = false;
            let mut consumer_closed = false;
            let result = send_daemon_rpc_stream_from_data_dir_async(
                data_dir.clone(),
                &address,
                &request,
                DaemonRpcTimeouts::default().with_connect_timeout(Some(Duration::from_secs(3))),
                |response| {
                    response.error_detail()?;
                    let item = response
                        .decode_body::<DaemonEventStreamItem>()
                        .map_err(DaemonApiError::from)?;
                    received_ready |= matches!(&item, DaemonEventStreamItem::Ready { .. });
                    if tx.send(DaemonEventWorkerUpdate::Item(item)).is_err() {
                        consumer_closed = true;
                        return Ok(false);
                    }
                    if let Some(waker) = waker.as_ref() {
                        waker.wake();
                    }
                    Ok(true)
                },
            )
            .await;
            if consumer_closed {
                return;
            }
            if received_ready {
                retry_delay = Duration::from_millis(250);
            }
            let detail = match result {
                Ok(()) => "daemon closed the event stream".to_string(),
                Err(error) => error.to_string(),
            };
            if tx
                .send(DaemonEventWorkerUpdate::Disconnected(format!(
                    "Live chat updates disconnected; retrying: {detail}"
                )))
                .is_err()
            {
                return;
            }
            if let Some(waker) = waker.as_ref() {
                waker.wake();
            }
            tokio::time::sleep(retry_delay).await;
            retry_delay = (retry_delay * 2).min(Duration::from_secs(5));
        }
    });
    task.abort_handle()
}

fn list_conversations_with_error(
    data_dir: &SinomoDataDir,
    sifs_daemon: Option<&SifsDaemonAccess>,
) -> (Vec<SessionRef>, Option<String>) {
    let result = open_agent_channel_store(data_dir, sifs_daemon).and_then(|store| {
        store
            .list_channel_summaries()
            .map_err(channel_store_io_error)
    });
    let mut conversations = Vec::new();
    let error = match result {
        Ok(channels) => {
            let mut seen = BTreeSet::new();
            for channel in channels {
                let id = channel.channel_id.to_string();
                if !seen.insert(id.clone()) {
                    continue;
                }
                let (kind, fallback_title, participant_count) = match channel.channel_kind {
                    AgentChannelKind::Chat => (
                        ConversationKind::Chat,
                        "New chat",
                        channel.enabled_participant_count.max(1),
                    ),
                    AgentChannelKind::Channel => (
                        ConversationKind::Channel,
                        "New channel",
                        channel.enabled_participant_count,
                    ),
                };
                conversations.push(SessionRef {
                    id,
                    title: if channel.title.trim().is_empty() {
                        fallback_title.to_string()
                    } else {
                        channel.title
                    },
                    kind,
                    updated_unix: channel.updated_at.unix_timestamp(),
                    participant_count,
                    participant_names: channel.participant_names,
                });
            }
            None
        }
        Err(error) => Some(error.to_string()),
    };
    sort_conversations(&mut conversations);
    (conversations, error)
}

fn session_ref_for_channel(channel: &AgentChannel) -> SessionRef {
    SessionRef {
        id: channel.channel_id.to_string(),
        title: if channel.title.trim().is_empty() {
            "New channel".to_string()
        } else {
            channel.title.clone()
        },
        kind: ConversationKind::Channel,
        updated_unix: channel.updated_at.unix_timestamp(),
        participant_count: channel.enabled_participants_in_order().len(),
        participant_names: channel
            .enabled_participants_in_order()
            .into_iter()
            .map(|participant| {
                if participant.agent_name.trim().is_empty() {
                    participant.agent_key
                } else {
                    participant.agent_name
                }
            })
            .collect(),
    }
}

fn sort_conversations(conversations: &mut [SessionRef]) {
    conversations.sort_by(|left, right| {
        right
            .updated_unix
            .cmp(&left.updated_unix)
            .then_with(|| left.title.cmp(&right.title))
    });
}

fn delete_conversation_records(
    data_dir: &SinomoDataDir,
    sifs_daemon: Option<&SifsDaemonAccess>,
    id: &str,
    kind: ConversationKind,
) -> std::io::Result<()> {
    let tombstoned = tombstone_channel_record_in(data_dir, sifs_daemon, id)?;
    if tombstoned || kind == ConversationKind::Chat {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "channel not found",
        ))
    }
}

fn tombstone_channel_record_in(
    data_dir: &SinomoDataDir,
    sifs_daemon: Option<&SifsDaemonAccess>,
    id: &str,
) -> std::io::Result<bool> {
    let channel_id = parse_channel_id(id)?;
    let store = open_agent_channel_store(data_dir, sifs_daemon)?;
    let mut channel = match store
        .load_channel(&channel_id)
        .map_err(channel_store_io_error)?
    {
        Some(channel) => channel,
        None => return Ok(false),
    };
    channel.tombstone();
    store
        .save_channel(&channel)
        .map_err(channel_store_io_error)?;
    Ok(true)
}

fn rename_chat_channel(
    data_dir: &SinomoDataDir,
    sifs_daemon: Option<&SifsDaemonAccess>,
    id: &str,
    title: &str,
) -> std::io::Result<()> {
    let channel_id = parse_channel_id(id)?;
    let store = open_agent_channel_store(data_dir, sifs_daemon)?;
    let mut channel = store
        .load_channel(&channel_id)
        .map_err(channel_store_io_error)?
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::NotFound, "chat not found"))?;
    if channel.channel_kind != AgentChannelKind::Chat {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "chat not found",
        ));
    }
    channel.rename(title);
    store.save_channel(&channel).map_err(channel_store_io_error)
}

#[cfg(test)]
fn channel_messages(channel: &AgentChannel) -> Vec<ChatMessage> {
    channel_messages_from_agent(&channel.transcript, channel.channel_kind)
}

fn channel_messages_from_agent(
    messages: &[sinomo_agent::AgentChannelMessage],
    channel_kind: AgentChannelKind,
) -> Vec<ChatMessage> {
    messages
        .iter()
        .filter_map(|message| {
            if message.content.trim().is_empty() && message.attachments.is_empty() {
                return None;
            }
            let content = visible_channel_message_text(message);
            let chat_message = match &message.author {
                AgentChannelAuthor::User { .. } => ChatMessage::user(content),
                AgentChannelAuthor::Agent { .. } if channel_kind == AgentChannelKind::Chat => {
                    ChatMessage::model(content)
                }
                AgentChannelAuthor::Agent { agent_name, .. } => {
                    let label = if agent_name.trim().is_empty() {
                        "Agent"
                    } else {
                        agent_name
                    };
                    ChatMessage::model(format!("{label}: {content}"))
                }
                AgentChannelAuthor::System => {
                    if message.message_kind == AgentChannelMessageKind::Error {
                        ChatMessage::system_error(content)
                    } else {
                        ChatMessage::system_info(content)
                    }
                }
            };
            Some(
                chat_message
                    .with_durable_identity(message.message_id.to_string(), message.sequence),
            )
        })
        .collect()
}

#[cfg(test)]
fn load_conversation_messages(
    data_dir: &SinomoDataDir,
    sifs_daemon: Option<&SifsDaemonAccess>,
    id: &str,
    kind: ConversationKind,
) -> Result<Vec<ChatMessage>, String> {
    Ok(load_conversation_timeline(
        data_dir,
        sifs_daemon,
        id,
        kind,
        TranscriptLoadMode::Initial,
        &BTreeSet::new(),
    )?
    .messages)
}

fn load_conversation_timeline(
    data_dir: &SinomoDataDir,
    sifs_daemon: Option<&SifsDaemonAccess>,
    id: &str,
    kind: ConversationKind,
    mode: TranscriptLoadMode,
    known_message_ids: &BTreeSet<String>,
) -> Result<ConversationTimelineLoad, String> {
    let timeline =
        load_channel_timeline_from_sifs(data_dir, sifs_daemon, id, mode, known_message_ids)?;
    match (timeline, kind) {
        (Some(timeline), _) => Ok(timeline),
        (None, ConversationKind::Chat) => {
            let channel_id = parse_channel_id(id).map_err(|error| error.to_string())?;
            Ok(empty_conversation_timeline_load(channel_id))
        }
        (None, ConversationKind::Channel) => Err("channel not found".to_string()),
    }
}

fn load_channel_timeline_from_sifs(
    data_dir: &SinomoDataDir,
    sifs_daemon: Option<&SifsDaemonAccess>,
    id: &str,
    mode: TranscriptLoadMode,
    known_message_ids: &BTreeSet<String>,
) -> Result<Option<ConversationTimelineLoad>, String> {
    let channel_id = parse_channel_id(id).map_err(|error| error.to_string())?;
    let store =
        open_agent_channel_store(data_dir, sifs_daemon).map_err(|error| error.to_string())?;
    let summary = store
        .load_channel_summary(&channel_id)
        .map_err(channel_store_io_error)
        .map_err(|error| error.to_string())?;
    let Some(summary) = summary else {
        return Ok(None);
    };
    if summary.deleted_at.is_some() {
        return Ok(None);
    }
    let reconciliation = match mode {
        TranscriptLoadMode::Initial => store.load_message_reconciliation_page(
            &channel_id,
            &BTreeSet::new(),
            TRANSCRIPT_PAGE_SIZE,
        ),
        TranscriptLoadMode::Older { before_sequence } => store
            .load_message_page_before(&channel_id, before_sequence, TRANSCRIPT_PAGE_SIZE)
            .map(reconciliation_from_page),
        TranscriptLoadMode::Newer { after_sequence } => store
            .load_message_page_from(
                &channel_id,
                after_sequence.saturating_add(1),
                TRANSCRIPT_PAGE_SIZE,
            )
            .map(reconciliation_from_page),
        TranscriptLoadMode::ReconcileLatest => store.load_message_reconciliation_page(
            &channel_id,
            known_message_ids,
            TRANSCRIPT_PAGE_SIZE,
        ),
    }
    .map_err(channel_store_io_error)
    .map_err(|error| error.to_string())?;
    let AgentChannelMessageReconciliation {
        page,
        discovered_message_ids,
    } = reconciliation;
    let overview = if mode == TranscriptLoadMode::Initial {
        Some(
            store
                .load_timeline_overview(&channel_id)
                .map_err(channel_store_io_error)
                .map_err(|error| error.to_string())?,
        )
    } else {
        None
    };
    Ok(Some(ConversationTimelineLoad {
        messages: channel_messages_from_agent(&page.messages, summary.channel_kind),
        page,
        overview,
        discovered_message_ids,
    }))
}

fn reconciliation_from_page(page: AgentChannelMessagePage) -> AgentChannelMessageReconciliation {
    let discovered_message_ids = page
        .messages
        .iter()
        .map(|message| message.message_id.to_string())
        .collect();
    AgentChannelMessageReconciliation {
        page,
        discovered_message_ids,
    }
}

fn empty_conversation_timeline_load(channel_id: SessionId) -> ConversationTimelineLoad {
    let overview_channel_id = channel_id.clone();
    ConversationTimelineLoad {
        messages: Vec::new(),
        page: AgentChannelMessagePage {
            channel_id,
            messages: Vec::new(),
            start_sequence: None,
            end_sequence: None,
            has_older: false,
            has_newer: false,
            total_messages: 0,
            chunk_size: 0,
        },
        overview: Some(AgentChannelTimelineOverview {
            channel_id: overview_channel_id,
            total_messages: 0,
            oldest_sequence: None,
            newest_sequence: None,
            chunk_size: 0,
            segments: Vec::new(),
        }),
        discovered_message_ids: Vec::new(),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TimelineTrimEdge {
    Start,
    End,
}

fn prepend_timeline_messages(messages: &mut Vec<ChatMessage>, older: Vec<ChatMessage>) -> usize {
    let mut existing_ids = messages
        .iter()
        .filter_map(|message| message.message_id.clone())
        .collect::<BTreeSet<_>>();
    let mut existing_sequences = messages
        .iter()
        .filter_map(|message| message.sequence)
        .collect::<BTreeSet<_>>();
    let mut unique = older
        .into_iter()
        .filter(|message| match &message.message_id {
            Some(message_id) => {
                let unique = existing_ids.insert(message_id.clone());
                if unique && let Some(sequence) = message.sequence {
                    existing_sequences.insert(sequence);
                }
                unique
            }
            None => match message.sequence {
                Some(sequence) => existing_sequences.insert(sequence),
                None => true,
            },
        })
        .collect::<Vec<_>>();
    let added = unique.len();
    unique.append(messages);
    *messages = unique;
    added
}

fn append_timeline_messages(messages: &mut Vec<ChatMessage>, newer: Vec<ChatMessage>) -> usize {
    let mut existing_ids = messages
        .iter()
        .filter_map(|message| message.message_id.clone())
        .collect::<BTreeSet<_>>();
    let mut existing_sequences = messages
        .iter()
        .filter_map(|message| message.sequence)
        .collect::<BTreeSet<_>>();
    let mut added = 0;
    for message in newer {
        if let Some(message_id) = &message.message_id {
            if !existing_ids.insert(message_id.clone()) {
                continue;
            }
            if let Some(sequence) = message.sequence {
                existing_sequences.insert(sequence);
            }
        } else if let Some(sequence) = message.sequence
            && !existing_sequences.insert(sequence)
        {
            continue;
        }
        messages.push(message);
        added += 1;
    }
    added
}

fn reconcile_latest_timeline_messages(
    messages: &mut Vec<ChatMessage>,
    latest: Vec<ChatMessage>,
) -> usize {
    let mut deduplicated = Vec::with_capacity(messages.len().saturating_add(latest.len()));
    for message in messages.drain(..) {
        if timeline_message_position(&deduplicated, &message).is_none() {
            deduplicated.push(message);
        }
    }

    // A just-sent user row (and its streamed response) is visible before the
    // durable SIFS reload gives it an id. Keep the original contiguous
    // ephemeral tail bounded here so backdated or remote records appended
    // during reconciliation cannot consume those rows. Only records beyond
    // the sequence range that was already durable may promote the tail.
    let durable_sequence_floor = deduplicated
        .iter()
        .filter_map(|message| message.sequence)
        .max()
        .unwrap_or(0);
    let ephemeral_tail_end = deduplicated.len();
    let mut ephemeral_tail_cursor = deduplicated
        .iter()
        .rposition(|message| message.message_id.is_some() || message.sequence.is_some())
        .map(|index| index + 1)
        .unwrap_or(0);

    let mut added = 0;
    for message in latest {
        if let Some(index) = timeline_message_position(&deduplicated, &message) {
            deduplicated[index] = message;
        } else if let Some(index) = matching_ephemeral_tail_position(
            &deduplicated,
            &message,
            durable_sequence_floor,
            ephemeral_tail_cursor,
            ephemeral_tail_end,
        ) {
            deduplicated[index] = message;
            ephemeral_tail_cursor = index + 1;
        } else {
            deduplicated.push(message);
            added += 1;
        }
    }
    deduplicated.sort_by(|left, right| match (left.sequence, right.sequence) {
        (Some(left_sequence), Some(right_sequence)) => left_sequence
            .cmp(&right_sequence)
            .then_with(|| left.message_id.cmp(&right.message_id)),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    });
    *messages = deduplicated;
    added
}

fn matching_ephemeral_tail_position(
    messages: &[ChatMessage],
    candidate: &ChatMessage,
    durable_sequence_floor: u64,
    tail_start: usize,
    tail_end: usize,
) -> Option<usize> {
    let sequence = candidate.sequence?;
    if candidate.message_id.is_none() || sequence <= durable_sequence_floor {
        return None;
    }
    messages
        .iter()
        .enumerate()
        .take(tail_end)
        .skip(tail_start)
        .find_map(|(index, message)| {
            (message.message_id.is_none()
                && message.sequence.is_none()
                && message.role == candidate.role
                && message.source == candidate.source
                && message.kind == candidate.kind
                && message.text == candidate.text)
                .then_some(index)
        })
}

fn timeline_message_position(messages: &[ChatMessage], candidate: &ChatMessage) -> Option<usize> {
    if let Some(message_id) = candidate.message_id.as_deref() {
        return messages
            .iter()
            .position(|message| message.message_id.as_deref() == Some(message_id));
    }
    candidate.sequence.and_then(|sequence| {
        messages
            .iter()
            .position(|message| message.sequence == Some(sequence))
    })
}

fn trim_timeline_window(messages: &mut Vec<ChatMessage>, edge: TimelineTrimEdge) -> usize {
    let excess = messages
        .len()
        .saturating_sub(TRANSCRIPT_MAX_LOADED_MESSAGES);
    if excess == 0 {
        return 0;
    }
    match edge {
        TimelineTrimEdge::Start => {
            let removable = messages
                .iter()
                .take_while(|message| message.sequence.is_some())
                .count()
                .min(excess);
            messages.drain(0..removable);
            removable
        }
        TimelineTrimEdge::End => {
            let mut removed = 0;
            let mut index = messages.len();
            while removed < excess && index > 0 {
                index -= 1;
                if messages[index].sequence.is_some() {
                    messages.remove(index);
                    removed += 1;
                }
            }
            removed
        }
    }
}

fn transcript_loading_message(session: &SessionRef) -> ChatMessage {
    ChatMessage::system_info(match session.kind {
        ConversationKind::Chat => "Loading chat history from SIFS...",
        ConversationKind::Channel => "Loading channel timeline from SIFS...",
    })
}

fn transcript_loading_detail(kind: ConversationKind) -> String {
    match kind {
        ConversationKind::Chat => "Loading chat history from SIFS".to_string(),
        ConversationKind::Channel => "Loading channel timeline from SIFS".to_string(),
    }
}

fn transcript_slow_loading_message(kind: ConversationKind) -> ChatMessage {
    ChatMessage::system_info(match kind {
        ConversationKind::Chat => {
            "Chat history is still loading from SIFS. You can keep using Sinomo or select another chat."
        }
        ConversationKind::Channel => {
            "Channel timeline is still loading from SIFS. You can keep using Sinomo or select another chat."
        }
    })
}

fn transcript_load_error_message(kind: ConversationKind) -> ChatMessage {
    ChatMessage::system_error(match kind {
        ConversationKind::Chat => {
            "Sinomo could not load this chat history. Open Activity for SIFS recovery details."
        }
        ConversationKind::Channel => {
            "Sinomo could not load this channel timeline. Open Activity for SIFS recovery details."
        }
    })
}

fn transcript_load_failed_notice(mode: TranscriptLoadMode, detail: &str) -> String {
    match mode {
        TranscriptLoadMode::Initial => format!("chat load failed: {detail}"),
        TranscriptLoadMode::Older { .. } => format!("older history load failed: {detail}"),
        TranscriptLoadMode::Newer { .. } => format!("newer history load failed: {detail}"),
        TranscriptLoadMode::ReconcileLatest => format!("chat sync failed: {detail}"),
    }
}

fn visible_channel_message_text(message: &sinomo_agent::AgentChannelMessage) -> String {
    let mut text = if matches!(&message.author, AgentChannelAuthor::Agent { .. }) {
        visible_model_message_text(&message.content)
    } else {
        message.content.clone()
    };
    if message.attachments.is_empty() {
        return text;
    }
    append_visible_attachment_summary(
        &mut text,
        message.attachments.iter().map(|attachment| {
            (
                attachment.file_name.as_str(),
                attachment.size_bytes,
                Some(attachment.stored_path.as_str()),
            )
        }),
    );
    text
}

fn visible_model_message_text(text: &str) -> String {
    agent_decision_envelope_display_text(text).unwrap_or_else(|| text.to_string())
}

fn agent_decision_envelope_display_text(raw: &str) -> Option<String> {
    let trimmed = raw.trim_start();
    let (speaker, candidate) = split_agent_speaker_prefix(trimmed);
    let mut stream = serde_json::Deserializer::from_str(candidate).into_iter::<serde_json::Value>();
    let value = stream.next()?.ok()?;
    let offset = stream.byte_offset();
    let object = value.as_object()?;
    if !is_agent_decision_envelope(object) {
        return None;
    }

    let text = object
        .get("text")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .trim();
    let trailing = candidate.get(offset..).unwrap_or_default().trim();
    let visible = if text.is_empty() {
        if trailing.is_empty() {
            "No user-facing message was returned.".to_string()
        } else {
            trailing.to_string()
        }
    } else if trailing.is_empty() || trailing == text {
        text.to_string()
    } else {
        format!("{text}\n\n{trailing}")
    };

    Some(match speaker {
        Some(speaker) if !visible.trim().is_empty() => format!("{speaker}: {visible}"),
        _ => visible,
    })
}

fn split_agent_speaker_prefix(text: &str) -> (Option<&str>, &str) {
    let Some((speaker, rest)) = text.split_once(':') else {
        return (None, text);
    };
    let speaker = speaker.trim();
    let rest = rest.trim_start();
    if rest.starts_with('{') && is_agent_speaker_label(speaker) {
        (Some(speaker), rest)
    } else {
        (None, text)
    }
}

fn is_agent_speaker_label(label: &str) -> bool {
    if label.is_empty() || label.len() > 64 || label.contains('\n') || label.contains('\r') {
        return false;
    }
    if !label
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '/' | ' '))
    {
        return false;
    }
    let lower = label.to_ascii_lowercase();
    lower == "agent" || lower.ends_with("-agent") || lower.ends_with(" agent")
}

fn is_agent_decision_envelope(object: &serde_json::Map<String, serde_json::Value>) -> bool {
    object.contains_key("text")
        && (object.contains_key("response_mode")
            || object.contains_key("tool_calls")
            || object.contains_key("background_handoff")
            || object.contains_key("next_state"))
}

fn visible_user_message_text(text: &str, attachments: &[PendingChatAttachment]) -> String {
    let mut visible = text.to_string();
    append_visible_attachment_summary(
        &mut visible,
        attachments
            .iter()
            .map(|attachment| (attachment.file_name.as_str(), attachment.size_bytes, None)),
    );
    visible
}

fn append_visible_attachment_summary<'a>(
    text: &mut String,
    attachments: impl IntoIterator<Item = (&'a str, u64, Option<&'a str>)>,
) {
    let attachments = attachments.into_iter().collect::<Vec<_>>();
    if attachments.is_empty() {
        return;
    }
    if !text.trim().is_empty() {
        text.push_str("\n\n");
    }
    text.push_str("Attachments:\n");
    for (name, size_bytes, stored_path) in attachments {
        text.push_str("- ");
        text.push_str(name);
        text.push_str(" (");
        text.push_str(&format_bytes(size_bytes.min(usize::MAX as u64) as usize));
        text.push(')');
        if let Some(stored_path) = stored_path {
            text.push_str(" at ");
            text.push_str(stored_path);
        }
        text.push('\n');
    }
    while text.ends_with('\n') {
        text.pop();
    }
}

fn normalize_attachment_path(value: &str) -> Option<String> {
    let mut path = value
        .trim()
        .trim_matches('"')
        .trim_matches('\'')
        .to_string();
    if path.is_empty() {
        return None;
    }
    if let Some(rest) = path.strip_prefix("file://") {
        path = rest.to_string();
        #[cfg(windows)]
        if path.len() >= 3 && path.as_bytes().first() == Some(&b'/') && path.as_bytes()[2] == b':' {
            path.remove(0);
        }
    }
    if path.trim().is_empty() {
        None
    } else {
        Some(path)
    }
}

fn validate_local_attachment_path(path: PathBuf) -> LocalAttachmentValidationOutcome {
    let metadata = match std::fs::metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) => {
            return LocalAttachmentValidationOutcome::Failed {
                path,
                detail: format!("attachment unavailable: {error}"),
            };
        }
    };
    if !metadata.is_file() {
        return LocalAttachmentValidationOutcome::Failed {
            path,
            detail: "attachment must be a file".to_string(),
        };
    }
    let path = std::fs::canonicalize(&path).unwrap_or(path);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(ToString::to_string)
        .unwrap_or_else(|| "attachment".to_string());
    LocalAttachmentValidationOutcome::Ready {
        path,
        file_name,
        size_bytes: metadata.len(),
    }
}

fn open_agent_channel_store(
    data_dir: &SinomoDataDir,
    sifs_daemon: Option<&SifsDaemonAccess>,
) -> std::io::Result<SifsAgentChannelStore> {
    match sifs_daemon {
        Some(daemon) => daemon
            .open_browser()
            .map(SifsAgentChannelStore::from_browser)
            .map_err(|detail| std::io::Error::new(std::io::ErrorKind::Other, detail)),
        None => open_local_agent_channel_store(data_dir),
    }
}

fn conversation_kind_label(kind: ConversationKind) -> &'static str {
    match kind {
        ConversationKind::Chat => "chat",
        ConversationKind::Channel => "channel",
    }
}

fn rename_session_in_store(
    data_dir: &SinomoDataDir,
    sifs_daemon: Option<&SifsDaemonAccess>,
    kind: ConversationKind,
    session_id: &str,
    title: &str,
) -> std::io::Result<()> {
    match kind {
        ConversationKind::Chat => rename_chat_channel(data_dir, sifs_daemon, session_id, title),
        ConversationKind::Channel => {
            let channel_id = parse_channel_id(session_id)?;
            let store = open_agent_channel_store(data_dir, sifs_daemon)?;
            let mut channel = store
                .load_channel(&channel_id)
                .map_err(channel_store_io_error)?
                .ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::NotFound, "channel not found")
                })?;
            channel.channel_kind = AgentChannelKind::Channel;
            channel.rename(title);
            store.save_channel(&channel).map_err(channel_store_io_error)
        }
    }
}

fn update_channel_membership_in_store(
    data_dir: &SinomoDataDir,
    sifs_daemon: Option<&SifsDaemonAccess>,
    channel_id: &str,
    operation: ChannelConfigOperation,
) -> std::io::Result<()> {
    let parsed_channel_id = parse_channel_id(channel_id)?;
    let store = open_agent_channel_store(data_dir, sifs_daemon)?;
    let mut channel = match &operation {
        ChannelConfigOperation::AddAgent { .. } => store
            .load_channel(&parsed_channel_id)
            .map_err(channel_store_io_error)?
            .unwrap_or_else(|| AgentChannel::new_channel(parsed_channel_id, "New channel")),
        ChannelConfigOperation::RemoveAgent { .. } => store
            .load_channel(&parsed_channel_id)
            .map_err(channel_store_io_error)?
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::NotFound, "channel not found")
            })?,
    };
    channel.channel_kind = AgentChannelKind::Channel;
    match operation {
        ChannelConfigOperation::AddAgent {
            agent_key,
            agent_name,
            tool_runtime,
        } => {
            let participant_id = channel.ensure_participant(agent_key, agent_name);
            set_channel_participant_tool_runtime(&mut channel, &participant_id, &tool_runtime);
        }
        ChannelConfigOperation::RemoveAgent { participant_id } => {
            let parsed_participant_id =
                participant_id
                    .parse::<sinomo_agent::TaskId>()
                    .map_err(|error| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            format!("invalid participant id: {error}"),
                        )
                    })?;
            if !channel.disable_participant(&parsed_participant_id) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "agent not found",
                ));
            }
        }
    }
    store.save_channel(&channel).map_err(channel_store_io_error)
}

fn open_local_agent_channel_store(
    data_dir: &SinomoDataDir,
) -> std::io::Result<SifsAgentChannelStore> {
    SifsAgentChannelStore::open(data_dir)
        .map_err(|detail| std::io::Error::new(std::io::ErrorKind::Other, detail))
}

fn parse_channel_id(id: &str) -> std::io::Result<SessionId> {
    id.parse::<SessionId>().map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid channel id: {error}"),
        )
    })
}

fn channel_store_io_error(error: sinomo_agent::AgentChannelStoreError) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, error.to_string())
}

fn set_channel_participant_tool_runtime(
    channel: &mut AgentChannel,
    participant_id: &sinomo_agent::TaskId,
    tool_runtime: &ChatToolRuntimeSelection,
) {
    if let Some(participant) = channel.participant_mut(participant_id) {
        participant.tool_runtime.sandbox_node_id = tool_runtime.sandbox_node_id.clone();
    }
}

fn merge_model_lists(existing: &[String], fetched: &[String]) -> Vec<String> {
    let mut models = Vec::with_capacity(existing.len() + fetched.len());
    for model in existing.iter().chain(fetched.iter()) {
        if model.trim().is_empty() || models.iter().any(|candidate| candidate == model) {
            continue;
        }
        models.push(model.clone());
    }
    models
}

// ---------------------------------------------------------------------------
// Theme reader + small helpers (mirrors the Settings / Files tabs).
// ---------------------------------------------------------------------------

/// Clone the shared theme reader into a fresh `Fn() -> DefaultTheme` for one control.
fn clone_reader(reader: &ThemeReader) -> impl Fn() -> DefaultTheme + 'static {
    let reader = Rc::clone(reader);
    move || reader()
}

fn small_control_reader(reader: &ThemeReader) -> impl Fn() -> DefaultTheme + 'static {
    let reader = Rc::clone(reader);
    move || reader().with_size(ControlSize::Small)
}

fn composer_control_reader(reader: &ThemeReader) -> impl Fn() -> DefaultTheme + 'static {
    let reader = Rc::clone(reader);
    move || reader().with_size(ControlSize::Small)
}

#[cfg(not(test))]
fn run_chat_media_call_worker(
    data_dir: Arc<SinomoDataDir>,
    provider_id: String,
    model: String,
    mut start_config: ChatMediaCallStartConfig,
    setup_shutdown: Option<MediaSetupShutdown>,
    event_tx: mpsc::Sender<ChatMediaCallWorkerEvent>,
    control_rx: Receiver<ChatMediaCallControl>,
    snapshots: Arc<Mutex<ChatMediaSnapshots>>,
    cancelled: Arc<AtomicBool>,
    microphone_requested: Arc<AtomicBool>,
    camera_requested: Arc<AtomicBool>,
    camera_surface: Arc<sinomo_video::NativeVideoSurface>,
    waker: Option<sui::UiHandle>,
) {
    if let Some(shutdown) = setup_shutdown {
        shutdown.wait();
    }
    if cancelled.load(Ordering::Acquire) {
        return;
    }
    if drain_chat_media_controls(
        &control_rx,
        &mut start_config.mic_enabled,
        &mut start_config.camera_enabled,
    )
    .stop
    {
        let _ = send_chat_media_call_event(
            &event_tx,
            &waker,
            ChatMediaCallWorkerEvent::Done("call ended".to_string()),
        );
        return;
    }
    let result_tx = event_tx.clone();
    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("tokio runtime failed: {error}"))
        .and_then(|runtime| {
            runtime.block_on(run_chat_media_call_async(
                data_dir,
                provider_id,
                model,
                start_config,
                event_tx,
                control_rx,
                snapshots,
                cancelled,
                microphone_requested,
                camera_requested,
                camera_surface,
                waker.clone(),
            ))
        });
    match result {
        Ok(()) => {
            let _ = send_chat_media_call_event(
                &result_tx,
                &waker,
                ChatMediaCallWorkerEvent::Done("call ended".to_string()),
            );
        }
        Err(error) => {
            let _ = send_chat_media_call_event(
                &result_tx,
                &waker,
                ChatMediaCallWorkerEvent::Error(error),
            );
        }
    }
}

#[cfg(not(test))]
async fn await_chat_media_operation<F, T, W>(
    future: F,
    cancelled: &AtomicBool,
    mut on_wait: W,
) -> Option<T>
where
    F: std::future::Future<Output = T>,
    W: FnMut(),
{
    tokio::pin!(future);
    loop {
        on_wait();
        if cancelled.load(Ordering::Acquire) {
            return None;
        }
        tokio::select! {
            result = &mut future => return Some(result),
            _ = tokio::time::sleep(Duration::from_millis(5)) => {}
        }
    }
}

/// Run a potentially blocking OS device/permission open away from the call
/// worker. Cancellation drops the result receiver immediately; a late device
/// handle is then dropped on the opener thread instead of reviving a closed
/// call. The underlying native camera permission wait is itself bounded.
enum ChatMediaDeviceOpen<T> {
    Completed(Result<T, String>),
    Disabled,
    Cancelled,
}

async fn await_chat_media_device_open<T, F, W>(
    label: &'static str,
    cancelled: &AtomicBool,
    device_requested: Option<&AtomicBool>,
    open: F,
    mut on_wait: W,
) -> ChatMediaDeviceOpen<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
    W: FnMut(),
{
    on_wait();
    if cancelled.load(Ordering::Acquire) {
        return ChatMediaDeviceOpen::Cancelled;
    }
    if device_requested.is_some_and(|requested| !requested.load(Ordering::Acquire)) {
        return ChatMediaDeviceOpen::Disabled;
    }
    let (result_tx, result_rx) = mpsc::sync_channel(1);
    if let Err(error) = thread::Builder::new()
        .name(format!("sinomo-app-{label}-open"))
        .spawn(move || {
            let _ = result_tx.send(open());
        })
    {
        if cancelled.load(Ordering::Acquire) {
            return ChatMediaDeviceOpen::Cancelled;
        }
        if device_requested.is_some_and(|requested| !requested.load(Ordering::Acquire)) {
            return ChatMediaDeviceOpen::Disabled;
        }
        return ChatMediaDeviceOpen::Completed(Err(format!(
            "{label} open worker failed to start: {error}"
        )));
    }

    let started = Instant::now();
    loop {
        on_wait();
        if cancelled.load(Ordering::Acquire) {
            return ChatMediaDeviceOpen::Cancelled;
        }
        if device_requested.is_some_and(|requested| !requested.load(Ordering::Acquire)) {
            return ChatMediaDeviceOpen::Disabled;
        }
        match result_rx.try_recv() {
            Ok(result) => {
                on_wait();
                if cancelled.load(Ordering::Acquire) {
                    drop(result);
                    return ChatMediaDeviceOpen::Cancelled;
                }
                if device_requested.is_some_and(|requested| !requested.load(Ordering::Acquire)) {
                    drop(result);
                    return ChatMediaDeviceOpen::Disabled;
                }
                return ChatMediaDeviceOpen::Completed(result);
            }
            Err(TryRecvError::Disconnected) => {
                return ChatMediaDeviceOpen::Completed(Err(format!("{label} open worker stopped")));
            }
            Err(TryRecvError::Empty) => {}
        }
        if started.elapsed() >= CHAT_MEDIA_DEVICE_OPEN_TIMEOUT {
            return ChatMediaDeviceOpen::Completed(Err(format!("{label} open timed out")));
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn open_chat_media_device<T, F, W>(
    kind: MediaAccessKind,
    label: &'static str,
    cancelled: &AtomicBool,
    device_requested: Option<&AtomicBool>,
    event_tx: &mpsc::Sender<ChatMediaCallWorkerEvent>,
    waker: &Option<sui::UiHandle>,
    open: F,
    on_wait: W,
) -> ChatMediaDeviceOpen<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
    W: FnMut(),
{
    let _ = send_chat_media_call_event(
        event_tx,
        waker,
        ChatMediaCallWorkerEvent::DeviceAccess(kind, MediaAccessState::Checking),
    );
    let outcome =
        await_chat_media_device_open(label, cancelled, device_requested, open, on_wait).await;
    let state = match &outcome {
        ChatMediaDeviceOpen::Completed(Ok(_)) => Some(MediaAccessState::Ready),
        ChatMediaDeviceOpen::Completed(Err(detail)) => {
            Some(chat_media_access_failure(detail.clone()))
        }
        ChatMediaDeviceOpen::Disabled => Some(MediaAccessState::Idle),
        ChatMediaDeviceOpen::Cancelled => None,
    };
    if let Some(state) = state {
        let _ = send_chat_media_call_event(
            event_tx,
            waker,
            ChatMediaCallWorkerEvent::DeviceAccess(kind, state),
        );
    }
    outcome
}

#[cfg(not(test))]
async fn run_chat_realtime_session_pump(
    mut session: sinomo_lm::RealtimeAudioSession,
    mut audio_rx: tokio::sync::mpsc::Receiver<sinomo_lm::RealtimeAudioInputChunk>,
    latest_video: Arc<ChatMediaLatestVideoSlot<Arc<sinomo_video::NativeVideoSurfaceFrame>>>,
    event_tx: tokio::sync::mpsc::Sender<ChatRealtimeSessionEvent>,
    stop: Arc<AtomicBool>,
    video_frame_interval: Duration,
) {
    let mut video_cadence = ChatMediaRemoteVideoCadence::new(video_frame_interval);
    while !stop.load(Ordering::Acquire) {
        for _ in 0..4 {
            let input = match audio_rx.try_recv() {
                Ok(input) => input,
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
            };
            if let Err(error) = session.send_audio(&input).await {
                let _ = event_tx
                    .send(ChatRealtimeSessionEvent::Error(format!(
                        "realtime audio send failed: {error}"
                    )))
                    .await;
                return;
            }
            if event_tx
                .send(ChatRealtimeSessionEvent::AudioSent)
                .await
                .is_err()
            {
                return;
            }
        }

        match session
            .receive_event_with_timeout(Duration::from_millis(1))
            .await
        {
            Ok(event) => {
                let closed = matches!(event, sinomo_lm::RealtimeAudioEvent::Closed { .. });
                if event_tx
                    .send(ChatRealtimeSessionEvent::Realtime(event))
                    .await
                    .is_err()
                {
                    return;
                }
                if closed {
                    return;
                }
            }
            Err(sinomo_lm::RealtimeAudioError::Timeout { .. }) => {}
            Err(error) => {
                let _ = event_tx
                    .send(ChatRealtimeSessionEvent::Error(format!(
                        "realtime receive failed: {error}"
                    )))
                    .await;
                return;
            }
        }

        let now = Instant::now();
        if video_cadence.is_due(now)
            && let Some(frame) = latest_video.take()
        {
            video_cadence.mark_sent(now);
            let mut input = sinomo_lm::RealtimeVideoFrameInput::new(
                frame.rgba.clone(),
                sinomo_lm::RealtimeVideoPixelFormat::Rgba8,
                frame.width,
                frame.height,
                frame.width as usize * 4,
                frame.sequence,
            );
            input.detail = sinomo_lm::RealtimeVideoFrameDetail::Auto;
            input.timestamp_ms = Some(frame.timestamp_ns / 1_000_000);
            if let Err(error) = session.send_video_frame(&input).await {
                let _ = event_tx
                    .send(ChatRealtimeSessionEvent::Error(format!(
                        "realtime video send failed: {error}"
                    )))
                    .await;
                return;
            }
            if event_tx
                .send(ChatRealtimeSessionEvent::VideoSent)
                .await
                .is_err()
            {
                return;
            }
        }
    }
    let _ = session.close().await;
}

#[cfg(not(test))]
async fn run_chat_media_call_async(
    data_dir: Arc<SinomoDataDir>,
    provider_id: String,
    model: String,
    start_config: ChatMediaCallStartConfig,
    event_tx: mpsc::Sender<ChatMediaCallWorkerEvent>,
    control_rx: Receiver<ChatMediaCallControl>,
    snapshots: Arc<Mutex<ChatMediaSnapshots>>,
    cancelled: Arc<AtomicBool>,
    microphone_requested: Arc<AtomicBool>,
    camera_requested: Arc<AtomicBool>,
    camera_surface: Arc<sinomo_video::NativeVideoSurface>,
    waker: Option<sui::UiHandle>,
) -> Result<(), String> {
    let ChatMediaCallStartConfig {
        devices: device_selection,
        mut mic_enabled,
        mut camera_enabled,
    } = start_config;
    let provider = sinomo_lm::RealtimeAudioProvider::from_id(&provider_id)
        .ok_or_else(|| format!("unsupported realtime provider `{provider_id}`"))?;
    let model = if model.trim().is_empty() {
        provider.default_model().to_string()
    } else {
        model
    };
    let mut config = sinomo_lm::RealtimeAudioSessionConfig::new(provider, model);
    config.turn_detection = sinomo_lm::RealtimeAudioTurnDetection::SemanticVad;
    config.api_key = chat_media_realtime_api_key(&data_dir, provider)?;

    if cancelled.load(Ordering::Acquire) {
        return Ok(());
    }
    if drain_chat_media_controls(&control_rx, &mut mic_enabled, &mut camera_enabled).stop {
        return Ok(());
    }

    let mut microphone_config = sinomo_audio::NativeAudioCaptureConfig::new(
        config.input_sample_rate_hz,
        app_native_audio_format(config.input_format),
    );
    microphone_config.device = device_selection.microphone.clone();
    let camera_config = sinomo_video::NativeVideoCaptureConfig {
        device: device_selection.camera.clone(),
        frame_interval_ms: sinomo_video::NATIVE_VIDEO_PREVIEW_FRAME_INTERVAL_MS,
        ..Default::default()
    };
    let mut speaker_config = sinomo_audio::NativeSpeakerPlaybackConfig::new(
        config.output_sample_rate_hz,
        app_native_audio_format(config.output_format),
    );
    speaker_config.device = device_selection.speaker;

    let mut issues = Vec::new();
    if cancelled.load(Ordering::Acquire) {
        return Ok(());
    }
    let mut microphone = if mic_enabled {
        let open_config = microphone_config.clone();
        match open_chat_media_device(
            MediaAccessKind::Microphone,
            "microphone",
            &cancelled,
            Some(&microphone_requested),
            &event_tx,
            &waker,
            move || open_chat_media_microphone(&open_config),
            || {},
        )
        .await
        {
            ChatMediaDeviceOpen::Cancelled => return Ok(()),
            ChatMediaDeviceOpen::Disabled => {
                mic_enabled = false;
                None
            }
            ChatMediaDeviceOpen::Completed(Ok(capture)) => Some(capture),
            ChatMediaDeviceOpen::Completed(Err(detail)) => {
                issues.push(detail);
                None
            }
        }
    } else {
        let _ = send_chat_media_call_event(
            &event_tx,
            &waker,
            ChatMediaCallWorkerEvent::DeviceAccess(
                MediaAccessKind::Microphone,
                MediaAccessState::Idle,
            ),
        );
        None
    };
    if cancelled.load(Ordering::Acquire) {
        return Ok(());
    }
    let mut camera = if camera_enabled {
        let open_config = camera_config.clone();
        match open_chat_media_device(
            MediaAccessKind::Camera,
            "camera",
            &cancelled,
            Some(&camera_requested),
            &event_tx,
            &waker,
            move || open_chat_media_camera(&open_config),
            || {
                release_chat_media_microphone_if_unrequested(
                    &microphone_requested,
                    &mut microphone,
                    &event_tx,
                    &snapshots,
                    &waker,
                );
            },
        )
        .await
        {
            ChatMediaDeviceOpen::Cancelled => return Ok(()),
            ChatMediaDeviceOpen::Disabled => {
                camera_enabled = false;
                None
            }
            ChatMediaDeviceOpen::Completed(Ok(capture)) => Some(capture),
            ChatMediaDeviceOpen::Completed(Err(detail)) => {
                issues.push(detail);
                None
            }
        }
    } else {
        let _ = send_chat_media_call_event(
            &event_tx,
            &waker,
            ChatMediaCallWorkerEvent::DeviceAccess(MediaAccessKind::Camera, MediaAccessState::Idle),
        );
        None
    };
    if cancelled.load(Ordering::Acquire) {
        return Ok(());
    }
    let speaker = match open_chat_media_device(
        MediaAccessKind::Speaker,
        "speaker",
        &cancelled,
        None,
        &event_tx,
        &waker,
        move || open_chat_media_speaker(&speaker_config),
        || {
            release_chat_media_microphone_if_unrequested(
                &microphone_requested,
                &mut microphone,
                &event_tx,
                &snapshots,
                &waker,
            );
            release_chat_media_camera_if_unrequested(
                &camera_requested,
                &mut camera,
                &event_tx,
                &snapshots,
                &camera_surface,
                &waker,
            );
        },
    )
    .await
    {
        ChatMediaDeviceOpen::Cancelled => return Ok(()),
        ChatMediaDeviceOpen::Disabled => None,
        ChatMediaDeviceOpen::Completed(Ok(playback)) => Some(playback),
        ChatMediaDeviceOpen::Completed(Err(detail)) => {
            issues.push(detail);
            None
        }
    };
    let controls = drain_chat_media_controls(&control_rx, &mut mic_enabled, &mut camera_enabled);
    if controls.stop {
        return Ok(());
    }
    if !reconcile_chat_media_devices(
        controls,
        mic_enabled,
        camera_enabled,
        &microphone_config,
        &camera_config,
        &mut microphone,
        &mut camera,
        &event_tx,
        &snapshots,
        &cancelled,
        &microphone_requested,
        &camera_requested,
        &camera_surface,
        &waker,
    )
    .await
    {
        return Ok(());
    }
    let microphone_requested_now = microphone_requested.load(Ordering::Acquire);
    let camera_requested_now = camera_requested.load(Ordering::Acquire);
    let requested_input_available = chat_media_requested_input_available(
        microphone_requested_now,
        microphone.is_some(),
        camera_requested_now,
        camera.is_some(),
    );
    if !requested_input_available {
        return Err(format!(
            "native mic/camera unavailable: {}",
            issues.join("; ")
        ));
    }
    config.input_video = Some(sinomo_lm::RealtimeVideoInputConfig {
        frame_format: sinomo_lm::RealtimeVideoFrameFormat::Jpeg,
        detail: sinomo_lm::RealtimeVideoFrameDetail::Auto,
        max_frame_rate_hz: Some(2),
        ..Default::default()
    });

    let _ = send_chat_media_call_event(
        &event_tx,
        &waker,
        ChatMediaCallWorkerEvent::Ready(chat_media_ready_notice(
            microphone.is_some(),
            camera.is_some(),
            speaker.is_some(),
            &issues,
        )),
    );
    let Some(connect_result) = await_chat_media_operation(
        sinomo_lm::RealtimeAudioSession::connect(config.clone()),
        &cancelled,
        || {
            release_chat_media_microphone_if_unrequested(
                &microphone_requested,
                &mut microphone,
                &event_tx,
                &snapshots,
                &waker,
            );
            release_chat_media_camera_if_unrequested(
                &camera_requested,
                &mut camera,
                &event_tx,
                &snapshots,
                &camera_surface,
                &waker,
            );
        },
    )
    .await
    else {
        return Ok(());
    };
    let session = connect_result.map_err(|error| format!("realtime connect failed: {error}"))?;
    let controls = drain_chat_media_controls(&control_rx, &mut mic_enabled, &mut camera_enabled);
    if controls.stop {
        return Ok(());
    }
    if !reconcile_chat_media_devices(
        controls,
        mic_enabled,
        camera_enabled,
        &microphone_config,
        &camera_config,
        &mut microphone,
        &mut camera,
        &event_tx,
        &snapshots,
        &cancelled,
        &microphone_requested,
        &camera_requested,
        &camera_surface,
        &waker,
    )
    .await
    {
        return Ok(());
    }
    publish_chat_media_notice(&snapshots, &waker, "realtime session connected".to_string());
    let mut realtime_session = ChatRealtimeSessionRuntime::spawn(session);
    let mut audio_backpressure_notice_shown = false;
    let mut camera_frame_failures = 0_u8;

    loop {
        let controls =
            drain_chat_media_controls(&control_rx, &mut mic_enabled, &mut camera_enabled);
        if controls.stop {
            break;
        }
        if controls.camera_enable_requested || controls.camera_disable_requested {
            camera_frame_failures = 0;
        }
        if !reconcile_chat_media_devices(
            controls,
            mic_enabled,
            camera_enabled,
            &microphone_config,
            &camera_config,
            &mut microphone,
            &mut camera,
            &event_tx,
            &snapshots,
            &cancelled,
            &microphone_requested,
            &camera_requested,
            &camera_surface,
            &waker,
        )
        .await
        {
            return Ok(());
        }
        let mut progressed = false;
        let mut microphone_stopped = false;
        if microphone.is_some() {
            for _ in 0..4 {
                let capture_result = match microphone.as_ref() {
                    Some(capture) => capture.recv_timeout(Duration::ZERO),
                    None => break,
                };
                match capture_result {
                    Ok(Some(chunk)) => {
                        if mic_enabled && microphone_requested.load(Ordering::Acquire) {
                            let level = sinomo_audio::normalized_audio_meter_level(
                                &chunk.audio,
                                chunk.sample_format,
                            );
                            publish_chat_media_snapshot(&snapshots, &waker, |snapshots| {
                                snapshots.microphone_level = Some(level);
                            });
                            let input = sinomo_lm::RealtimeAudioInputChunk::new(chunk.audio);
                            if matches!(
                                realtime_session.try_send_audio(input)?,
                                ChatMediaAudioQueueOutcome::DroppedBackpressure
                            ) && !audio_backpressure_notice_shown
                            {
                                publish_chat_media_notice(
                                    &snapshots,
                                    &waker,
                                    "realtime audio sender is catching up; dropped one input chunk"
                                        .to_string(),
                                );
                                audio_backpressure_notice_shown = true;
                            }
                        }
                        progressed = true;
                    }
                    Ok(None) => break,
                    Err(error) => {
                        let detail = format!("microphone stopped: {error}");
                        publish_chat_media_snapshot(&snapshots, &waker, |snapshots| {
                            snapshots.microphone_level = None;
                            snapshots.notice = Some(detail.clone());
                        });
                        let _ = send_chat_media_call_event(
                            &event_tx,
                            &waker,
                            ChatMediaCallWorkerEvent::DeviceAccess(
                                MediaAccessKind::Microphone,
                                chat_media_access_failure(detail),
                            ),
                        );
                        microphone_stopped = true;
                        progressed = true;
                        break;
                    }
                }
            }
        }
        if microphone_stopped {
            microphone = None;
        }

        let mut camera_stopped = false;
        if camera.is_some() {
            for _ in 0..2 {
                let capture_result = match camera.as_ref() {
                    Some(capture) => capture.recv_sample_timeout(Duration::ZERO),
                    None => break,
                };
                match capture_result {
                    Ok(Some(sample)) => {
                        if camera_enabled && camera_requested.load(Ordering::Acquire) {
                            match camera_surface.publish_sample(&sample) {
                                Ok(frame) => {
                                    camera_frame_failures = 0;
                                    realtime_session.replace_video(frame.clone());
                                    publish_chat_media_snapshot(&snapshots, &waker, |snapshots| {
                                        snapshots.camera_surface_revision = Some(frame.revision);
                                    });
                                }
                                Err(error) => {
                                    if camera_frame_failure_is_terminal(&mut camera_frame_failures)
                                    {
                                        let detail = format!(
                                            "camera produced {camera_frame_failures} invalid frames: {error}"
                                        );
                                        let revision = camera_surface.clear();
                                        publish_chat_media_snapshot(
                                            &snapshots,
                                            &waker,
                                            |snapshots| {
                                                snapshots.camera_surface_revision = Some(revision);
                                                snapshots.notice = Some(detail.clone());
                                            },
                                        );
                                        let _ = send_chat_media_call_event(
                                            &event_tx,
                                            &waker,
                                            ChatMediaCallWorkerEvent::DeviceAccess(
                                                MediaAccessKind::Camera,
                                                MediaAccessState::Unavailable(detail),
                                            ),
                                        );
                                        camera_stopped = true;
                                    }
                                    progressed = true;
                                    if camera_stopped {
                                        break;
                                    }
                                }
                            }
                        }
                        progressed = true;
                    }
                    Ok(None) => break,
                    Err(error) => {
                        let detail = format!("camera stopped: {error}");
                        let revision = camera_surface.clear();
                        publish_chat_media_snapshot(&snapshots, &waker, |snapshots| {
                            snapshots.camera_surface_revision = Some(revision);
                            snapshots.notice = Some(detail.clone());
                        });
                        let _ = send_chat_media_call_event(
                            &event_tx,
                            &waker,
                            ChatMediaCallWorkerEvent::DeviceAccess(
                                MediaAccessKind::Camera,
                                chat_media_access_failure(detail),
                            ),
                        );
                        camera_stopped = true;
                        progressed = true;
                        break;
                    }
                }
            }
        }
        if camera_stopped {
            camera = None;
        }
        let mut realtime_closed = false;
        for _ in 0..16 {
            let Some(event) = realtime_session.try_receive_event()? else {
                break;
            };
            progressed = true;
            match event {
                ChatRealtimeSessionEvent::Realtime(event) => {
                    if handle_chat_media_realtime_event(
                        event,
                        speaker.as_ref(),
                        &event_tx,
                        &snapshots,
                        &waker,
                    )? {
                        realtime_closed = true;
                        break;
                    }
                }
                ChatRealtimeSessionEvent::AudioSent => {
                    publish_chat_media_snapshot(&snapshots, &waker, |snapshots| {
                        snapshots.input_audio_chunks =
                            snapshots.input_audio_chunks.saturating_add(1);
                    });
                }
                ChatRealtimeSessionEvent::VideoSent => {
                    publish_chat_media_snapshot(&snapshots, &waker, |snapshots| {
                        snapshots.input_video_frames =
                            snapshots.input_video_frames.saturating_add(1);
                    });
                }
                ChatRealtimeSessionEvent::Error(detail) => return Err(detail),
            }
        }
        if realtime_closed {
            break;
        }
        if !progressed {
            tokio::time::sleep(Duration::from_millis(5)).await;
        } else {
            tokio::task::yield_now().await;
        }
    }

    drop(microphone);
    drop(camera);
    drop(speaker);
    realtime_session.shutdown().await;
    Ok(())
}

fn drain_chat_media_controls(
    control_rx: &Receiver<ChatMediaCallControl>,
    mic_enabled: &mut bool,
    camera_enabled: &mut bool,
) -> ChatMediaControlDrain {
    let mut drained = ChatMediaControlDrain::default();
    loop {
        match control_rx.try_recv() {
            Ok(ChatMediaCallControl::Stop) | Err(TryRecvError::Disconnected) => {
                drained.stop = true;
                return drained;
            }
            Ok(ChatMediaCallControl::SetMicEnabled(enabled)) => {
                *mic_enabled = enabled;
                if enabled {
                    drained.microphone_enable_requested = true;
                } else {
                    drained.microphone_disable_requested = true;
                }
            }
            Ok(ChatMediaCallControl::SetCameraEnabled(enabled)) => {
                *camera_enabled = enabled;
                if enabled {
                    drained.camera_enable_requested = true;
                } else {
                    drained.camera_disable_requested = true;
                }
            }
            Err(TryRecvError::Empty) => return drained,
        }
    }
}

#[cfg(not(test))]
fn release_chat_media_microphone_if_unrequested(
    requested: &AtomicBool,
    microphone: &mut Option<sinomo_audio::NativeAudioCapture>,
    event_tx: &mpsc::Sender<ChatMediaCallWorkerEvent>,
    snapshots: &Arc<Mutex<ChatMediaSnapshots>>,
    waker: &Option<sui::UiHandle>,
) {
    if requested.load(Ordering::Acquire) || microphone.take().is_none() {
        return;
    }
    publish_chat_media_snapshot(snapshots, waker, |snapshots| {
        snapshots.microphone_level = None;
    });
    let _ = send_chat_media_call_event(
        event_tx,
        waker,
        ChatMediaCallWorkerEvent::DeviceAccess(MediaAccessKind::Microphone, MediaAccessState::Idle),
    );
}

#[cfg(not(test))]
fn release_chat_media_camera_if_unrequested(
    requested: &AtomicBool,
    camera: &mut Option<sinomo_video::NativeVideoCapture>,
    event_tx: &mpsc::Sender<ChatMediaCallWorkerEvent>,
    snapshots: &Arc<Mutex<ChatMediaSnapshots>>,
    camera_surface: &sinomo_video::NativeVideoSurface,
    waker: &Option<sui::UiHandle>,
) {
    if requested.load(Ordering::Acquire) || camera.take().is_none() {
        return;
    }
    let revision = camera_surface.clear();
    publish_chat_media_snapshot(snapshots, waker, |snapshots| {
        snapshots.camera_surface_revision = Some(revision);
    });
    let _ = send_chat_media_call_event(
        event_tx,
        waker,
        ChatMediaCallWorkerEvent::DeviceAccess(MediaAccessKind::Camera, MediaAccessState::Idle),
    );
}

#[cfg(not(test))]
#[allow(clippy::too_many_arguments)]
async fn reconcile_chat_media_devices(
    controls: ChatMediaControlDrain,
    mic_enabled: bool,
    camera_enabled: bool,
    microphone_config: &sinomo_audio::NativeAudioCaptureConfig,
    camera_config: &sinomo_video::NativeVideoCaptureConfig,
    microphone: &mut Option<sinomo_audio::NativeAudioCapture>,
    camera: &mut Option<sinomo_video::NativeVideoCapture>,
    event_tx: &mpsc::Sender<ChatMediaCallWorkerEvent>,
    snapshots: &Arc<Mutex<ChatMediaSnapshots>>,
    cancelled: &AtomicBool,
    microphone_requested: &AtomicBool,
    camera_requested: &AtomicBool,
    camera_surface: &sinomo_video::NativeVideoSurface,
    waker: &Option<sui::UiHandle>,
) -> bool {
    let microphone_transition = chat_media_device_transition(
        mic_enabled,
        microphone.is_some(),
        controls.microphone_enable_requested,
        controls.microphone_disable_requested,
    );
    if microphone_transition.release {
        drop(microphone.take());
    }
    if microphone_transition.mark_idle {
        publish_chat_media_snapshot(snapshots, waker, |snapshots| {
            snapshots.microphone_level = None;
        });
        let _ = send_chat_media_call_event(
            event_tx,
            waker,
            ChatMediaCallWorkerEvent::DeviceAccess(
                MediaAccessKind::Microphone,
                MediaAccessState::Idle,
            ),
        );
    }
    if microphone_transition.open {
        let open_config = microphone_config.clone();
        match open_chat_media_device(
            MediaAccessKind::Microphone,
            "microphone",
            cancelled,
            Some(microphone_requested),
            event_tx,
            waker,
            move || open_chat_media_microphone(&open_config),
            || {
                release_chat_media_camera_if_unrequested(
                    camera_requested,
                    camera,
                    event_tx,
                    snapshots,
                    camera_surface,
                    waker,
                );
            },
        )
        .await
        {
            ChatMediaDeviceOpen::Cancelled => return false,
            ChatMediaDeviceOpen::Disabled => {}
            ChatMediaDeviceOpen::Completed(Ok(capture)) => *microphone = Some(capture),
            ChatMediaDeviceOpen::Completed(Err(detail)) => {
                publish_chat_media_notice(snapshots, waker, detail);
            }
        }
    }

    let camera_transition = chat_media_device_transition(
        camera_enabled,
        camera.is_some(),
        controls.camera_enable_requested,
        controls.camera_disable_requested,
    );
    if camera_transition.release {
        drop(camera.take());
    }
    if camera_transition.mark_idle {
        let revision = camera_surface.clear();
        publish_chat_media_snapshot(snapshots, waker, |snapshots| {
            snapshots.camera_surface_revision = Some(revision);
        });
        let _ = send_chat_media_call_event(
            event_tx,
            waker,
            ChatMediaCallWorkerEvent::DeviceAccess(MediaAccessKind::Camera, MediaAccessState::Idle),
        );
    }
    if camera_transition.open {
        let open_config = camera_config.clone();
        match open_chat_media_device(
            MediaAccessKind::Camera,
            "camera",
            cancelled,
            Some(camera_requested),
            event_tx,
            waker,
            move || open_chat_media_camera(&open_config),
            || {
                release_chat_media_microphone_if_unrequested(
                    microphone_requested,
                    microphone,
                    event_tx,
                    snapshots,
                    waker,
                );
            },
        )
        .await
        {
            ChatMediaDeviceOpen::Cancelled => return false,
            ChatMediaDeviceOpen::Disabled => {}
            ChatMediaDeviceOpen::Completed(Ok(capture)) => *camera = Some(capture),
            ChatMediaDeviceOpen::Completed(Err(detail)) => {
                publish_chat_media_notice(snapshots, waker, detail);
            }
        }
    }
    true
}

#[cfg(not(test))]
fn open_chat_media_microphone(
    config: &sinomo_audio::NativeAudioCaptureConfig,
) -> Result<sinomo_audio::NativeAudioCapture, String> {
    sinomo_audio::start_native_microphone_capture(config.clone())
        .map_err(|error| format!("microphone unavailable: {error}"))
}

#[cfg(not(test))]
struct ChatMediaCameraOpenGuard;

#[cfg(not(test))]
impl Drop for ChatMediaCameraOpenGuard {
    fn drop(&mut self) {
        CHAT_MEDIA_CAMERA_OPEN_IN_FLIGHT.store(false, Ordering::Release);
    }
}

#[cfg(not(test))]
fn open_chat_media_camera(
    config: &sinomo_video::NativeVideoCaptureConfig,
) -> Result<sinomo_video::NativeVideoCapture, String> {
    if CHAT_MEDIA_CAMERA_OPEN_IN_FLIGHT
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err("camera access is already being checked; retry shortly".to_string());
    }
    let _open_guard = ChatMediaCameraOpenGuard;
    sinomo_video::start_native_camera_capture(config.clone())
        .map_err(|error| format!("camera unavailable: {error}"))
}

#[cfg(not(test))]
fn open_chat_media_speaker(
    config: &sinomo_audio::NativeSpeakerPlaybackConfig,
) -> Result<sinomo_audio::NativeSpeakerPlayback, String> {
    sinomo_audio::start_native_speaker_playback(config.clone())
        .map_err(|error| format!("speaker unavailable: {error}"))
}

fn chat_media_access_failure(detail: String) -> MediaAccessState {
    match sinomo_runtime::device::NativeDeviceFailureKind::classify_detail(&detail) {
        sinomo_runtime::device::NativeDeviceFailureKind::PermissionDenied => {
            MediaAccessState::PermissionDenied(detail)
        }
        sinomo_runtime::device::NativeDeviceFailureKind::Busy => MediaAccessState::Busy(detail),
        sinomo_runtime::device::NativeDeviceFailureKind::Unsupported => {
            MediaAccessState::Unsupported(detail)
        }
        sinomo_runtime::device::NativeDeviceFailureKind::NotFound
        | sinomo_runtime::device::NativeDeviceFailureKind::Other => {
            MediaAccessState::Unavailable(detail)
        }
    }
}

#[cfg(not(test))]
fn handle_chat_media_realtime_event(
    event: sinomo_lm::RealtimeAudioEvent,
    speaker: Option<&sinomo_audio::NativeSpeakerPlayback>,
    event_tx: &mpsc::Sender<ChatMediaCallWorkerEvent>,
    snapshots: &Arc<Mutex<ChatMediaSnapshots>>,
    waker: &Option<sui::UiHandle>,
) -> Result<bool, String> {
    match event {
        sinomo_lm::RealtimeAudioEvent::SessionQueued {
            position,
            estimated_wait_s,
        } => {
            let detail = match (position, estimated_wait_s) {
                (Some(position), Some(wait)) => {
                    format!("realtime queued: position {position}, about {wait:.0}s")
                }
                (Some(position), None) => format!("realtime queued: position {position}"),
                _ => "realtime queued".to_string(),
            };
            publish_chat_media_notice(snapshots, waker, detail);
        }
        sinomo_lm::RealtimeAudioEvent::SessionCreated { .. } => {
            publish_chat_media_notice(snapshots, waker, "realtime session ready".to_string());
        }
        sinomo_lm::RealtimeAudioEvent::SessionUpdated => {}
        sinomo_lm::RealtimeAudioEvent::Listening { .. } => {
            publish_chat_media_notice(snapshots, waker, "model listening".to_string());
        }
        sinomo_lm::RealtimeAudioEvent::InputSpeechStarted => {
            publish_chat_media_notice(snapshots, waker, "speech detected".to_string());
        }
        sinomo_lm::RealtimeAudioEvent::InputSpeechStopped => {
            publish_chat_media_notice(snapshots, waker, "speech ended".to_string());
        }
        sinomo_lm::RealtimeAudioEvent::OutputAudioDelta { audio, .. } => {
            if !audio.is_empty() {
                if let Some(speaker) = speaker {
                    if let Err(error) = speaker.write(&audio) {
                        let detail = format!("speaker playback failed: {error}");
                        let _ = send_chat_media_call_event(
                            event_tx,
                            waker,
                            ChatMediaCallWorkerEvent::DeviceAccess(
                                MediaAccessKind::Speaker,
                                chat_media_access_failure(detail.clone()),
                            ),
                        );
                        publish_chat_media_notice(snapshots, waker, detail.clone());
                        return Err(detail);
                    }
                }
                publish_chat_media_snapshot(snapshots, waker, |snapshots| {
                    snapshots.output_audio_chunks = snapshots.output_audio_chunks.saturating_add(1);
                });
            }
        }
        sinomo_lm::RealtimeAudioEvent::OutputTranscriptDelta { delta } => {
            if !delta.trim().is_empty() {
                publish_chat_media_notice(snapshots, waker, delta);
            }
        }
        sinomo_lm::RealtimeAudioEvent::ResponseDone => {
            publish_chat_media_notice(snapshots, waker, "response complete".to_string());
        }
        sinomo_lm::RealtimeAudioEvent::Closed { reason } => {
            publish_chat_media_notice(snapshots, waker, format!("realtime closed: {reason}"));
            return Ok(true);
        }
        sinomo_lm::RealtimeAudioEvent::Error { code, message } => {
            return Err(format!("realtime error {code}: {message}"));
        }
        sinomo_lm::RealtimeAudioEvent::Raw { .. } => {}
    }
    Ok(false)
}

fn send_chat_media_call_event(
    event_tx: &mpsc::Sender<ChatMediaCallWorkerEvent>,
    waker: &Option<sui::UiHandle>,
    event: ChatMediaCallWorkerEvent,
) -> bool {
    let sent = event_tx.send(event).is_ok();
    if sent && let Some(waker) = waker {
        waker.wake();
    }
    sent
}

#[cfg(not(test))]
fn publish_chat_media_snapshot(
    snapshots: &Arc<Mutex<ChatMediaSnapshots>>,
    waker: &Option<sui::UiHandle>,
    update: impl FnOnce(&mut ChatMediaSnapshots),
) {
    if let Ok(mut snapshots) = snapshots.lock() {
        update(&mut snapshots);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

#[cfg(not(test))]
fn publish_chat_media_notice(
    snapshots: &Arc<Mutex<ChatMediaSnapshots>>,
    waker: &Option<sui::UiHandle>,
    notice: String,
) {
    publish_chat_media_snapshot(snapshots, waker, |snapshots| {
        snapshots.notice = Some(notice);
    });
}

#[cfg(not(test))]
fn chat_media_ready_notice(
    microphone: bool,
    camera: bool,
    speaker: bool,
    issues: &[String],
) -> String {
    let mut ready = Vec::new();
    if microphone {
        ready.push("mic");
    }
    if camera {
        ready.push("camera");
    }
    if speaker {
        ready.push("speaker");
    }
    if issues.is_empty() {
        format!("native {} ready", ready.join(", "))
    } else {
        format!("native {} ready; {}", ready.join(", "), issues.join("; "))
    }
}

#[cfg(not(test))]
fn chat_media_realtime_api_key(
    data_dir: &SinomoDataDir,
    provider: sinomo_lm::RealtimeAudioProvider,
) -> Result<Option<String>, String> {
    match provider {
        sinomo_lm::RealtimeAudioProvider::OpenAi => sinomo_lm::OpenAiProvider::default()
            .api_key()
            .map(Some)
            .map_err(|error| error.to_string()),
        sinomo_lm::RealtimeAudioProvider::MiniCpmO => {
            if let Some((api_key, _source)) =
                sinomo_lm::provider_api_key_from_env(provider.provider_id())
            {
                return Ok(Some(api_key));
            }
            sinomo_lm::stored_provider_api_key_in_data_dir(data_dir, provider.provider_id())
                .map_err(|error| error.to_string())
        }
    }
}

#[cfg_attr(test, allow(dead_code))]
fn app_native_audio_format(
    format: sinomo_lm::RealtimeAudioSampleFormat,
) -> sinomo_audio::NativeAudioSampleFormat {
    match format {
        sinomo_lm::RealtimeAudioSampleFormat::Pcm16Le => {
            sinomo_audio::NativeAudioSampleFormat::Pcm16Le
        }
        sinomo_lm::RealtimeAudioSampleFormat::F32Le => sinomo_audio::NativeAudioSampleFormat::F32Le,
    }
}

/// Resolve the app chrome palette for the current theme (used by the `Label` color readers and the
/// bespoke painters so muted/accent text matches the chrome).
fn palette(shared: &Shared) -> Palette {
    Palette::from_theme(&shared.theme(), widgets::theme_is_dark(shared))
}

fn format_bytes(bytes: usize) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.1} MB", bytes as f32 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f32 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

fn context_capacity_bytes(context: &ChatContextSnapshot) -> usize {
    context
        .max_context_bytes
        .or(context.compaction_threshold_bytes)
        .unwrap_or(context.total_bytes.max(1))
        .max(1)
}

fn context_count_label(count: usize, singular: &str, plural: &str) -> String {
    if count == 1 {
        format!("1 {singular}")
    } else {
        format!("{count} {plural}")
    }
}

fn context_overview_summary(chat: &ChatState) -> String {
    if let Some(context) = &chat.context {
        return format!(
            "Context {} of {} · {} · {}",
            format_bytes(context.total_bytes),
            format_bytes(context_capacity_bytes(context)),
            context_count_label(context.included_count, "source", "sources"),
            context_count_label(context.omitted_count, "omitted", "omitted")
        );
    }

    if chat.streaming {
        return "Assembling context for this response".to_string();
    }

    if let Some(target) = chat.runtime_recovery_target() {
        return match target {
            RuntimeRecoveryTarget::Providers => "Provider setup needed".to_string(),
            RuntimeRecoveryTarget::Connection => "Cluster connection needed".to_string(),
        };
    }

    if chat
        .messages
        .iter()
        .any(|message| message.source == ChatMessageSource::User)
    {
        "Context will refresh on the next send".to_string()
    } else {
        "Context will assemble when you send a message".to_string()
    }
}

#[cfg(test)]
fn runtime_overview_title(chat: &ChatState) -> String {
    if let Some(provider) = chat.current_provider() {
        let model = chat.current_model();
        if !model.is_empty() {
            return format!("{} / {}", provider.name, model);
        }
        return format!("{} / Choose a model", provider.name);
    }

    if !chat.info.provider.is_empty() && !chat.info.model.is_empty() {
        return format!("{} / {}", chat.info.provider, chat.info.model);
    }

    "Choose a provider".to_string()
}

fn compact_line(text: &str, max_chars: usize) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() <= max_chars {
        return collapsed;
    }
    let head: String = collapsed
        .chars()
        .take(max_chars.saturating_sub(3))
        .collect();
    format!("{head}...")
}

fn preview_multiline(text: &str, max_chars: usize, max_lines: usize) -> String {
    let mut out = String::new();
    let mut chars = 0;
    for (index, line) in text.lines().enumerate() {
        if index >= max_lines || chars >= max_chars {
            break;
        }
        if !out.is_empty() {
            out.push('\n');
        }
        let remaining = max_chars.saturating_sub(chars);
        let line_chars = line.chars().count();
        if line_chars > remaining {
            out.extend(line.chars().take(remaining.saturating_sub(3)));
            out.push_str("...");
            break;
        }
        out.push_str(line);
        chars += line_chars;
    }
    if out.is_empty() {
        compact_line(text, max_chars)
    } else if text.lines().count() > max_lines || text.chars().count() > max_chars {
        format!("{out}\n...")
    } else {
        out
    }
}

fn tool_call_summary(call: &ChatToolActivity) -> String {
    let mut parts = vec![format!("{} · step {}", call.status, call.step_index)];
    if let Some(call_index) = call.call_index {
        parts.push(format!("call {}", call_index + 1));
    }
    if let Some(command) = &call.process_command {
        parts.push(compact_line(command, 46));
    } else if !call.input_preview.trim().is_empty() {
        parts.push(compact_line(&call.input_preview, 46));
    }
    if let Some(code) = call.exit_code {
        parts.push(format!("exit {code}"));
    }
    parts.join(" · ")
}

fn tool_display_name(tool: &str) -> String {
    tool.rsplit('.').next().unwrap_or(tool).replace('_', " ")
}

fn color_from_rgb(rgb: [u8; 3]) -> Color {
    Color::rgba(
        rgb[0] as f32 / 255.0,
        rgb[1] as f32 / 255.0,
        rgb[2] as f32 / 255.0,
        1.0,
    )
}

fn diff_line_color(line: &str, pal: &Palette) -> Color {
    if line.starts_with('+') && !line.starts_with("+++") {
        pal.ok_text
    } else if line.starts_with('-') && !line.starts_with("---") {
        pal.danger_text
    } else if line.starts_with("@@") {
        pal.accent_text
    } else {
        pal.text
    }
}

fn diff_line_background(line: &str, pal: &Palette) -> Option<Color> {
    if line.starts_with('+') && !line.starts_with("+++") {
        Some(pal.ok_soft)
    } else if line.starts_with('-') && !line.starts_with("---") {
        Some(pal.danger_soft)
    } else if line.starts_with("@@") {
        Some(pal.accent_soft)
    } else {
        None
    }
}

fn task_tone(status: &str) -> ActivityTone {
    match status {
        "running" | "pending" => ActivityTone::Running,
        "completed" => ActivityTone::Good,
        "failed" | "cancelled" | "timed-out" => ActivityTone::Bad,
        _ => ActivityTone::Neutral,
    }
}

fn tool_status_tone(status: &str) -> ActivityTone {
    match status {
        "running" | "streaming" => ActivityTone::Running,
        "completed" => ActivityTone::Good,
        "failed" | "cancelled" | "timed-out" => ActivityTone::Bad,
        _ => ActivityTone::Neutral,
    }
}

fn tool_activity_matches(
    existing: &ChatToolActivity,
    step_index: usize,
    call_index: Option<usize>,
    call_id: Option<&str>,
    tool: Option<&str>,
) -> bool {
    if existing.step_index != step_index {
        return false;
    }
    if let (Some(existing_id), Some(call_id)) = (existing.call_id.as_deref(), call_id) {
        return existing_id == call_id;
    }
    if let (Some(existing_index), Some(call_index)) = (existing.call_index, call_index) {
        return existing_index == call_index;
    }
    if let Some(tool) = tool {
        return existing.tool == tool;
    }
    false
}

fn tool_process_matches(call: &ChatToolActivity, process: &ChatProcessSnapshot) -> bool {
    let command = process.command.trim();
    if command.is_empty() {
        return false;
    }
    call.input.contains(command)
        || call.input_preview.contains(command)
        || call
            .process_command
            .as_deref()
            .is_some_and(|existing| existing == command)
}

fn tool_call_key(call: &ChatToolActivity) -> String {
    if let Some(call_id) = &call.call_id {
        return format!("id:{call_id}");
    }
    if let Some(call_index) = call.call_index {
        return format!("step:{}:call:{call_index}", call.step_index);
    }
    format!("step:{}:tool:{}", call.step_index, call.tool)
}

fn tool_detail_key(call: &ChatToolActivity, section: ToolDetailSection) -> String {
    let suffix = match section {
        ToolDetailSection::Input => "input",
        ToolDetailSection::Output => "output",
    };
    format!("{}:{suffix}", tool_call_key(call))
}

fn tool_detail_collapsible(raw: &str) -> bool {
    raw.chars().count() > TOOL_DETAIL_COLLAPSE_CHARS || raw.lines().count() > 12
}

fn runtime_action_label(action: &RuntimeTranscriptAction) -> &'static str {
    match action {
        RuntimeTranscriptAction::ToggleToolChain => "Toggle tool activity",
        RuntimeTranscriptAction::ToggleToolDetail(_) => "Toggle tool detail",
    }
}

fn tool_output_text(call: &ChatToolActivity) -> String {
    let stdout = call.stdout.trim();
    let stderr = call.stderr.trim();
    match (stdout.is_empty(), stderr.is_empty()) {
        (true, true) => String::new(),
        (false, true) => stdout.to_string(),
        (true, false) => stderr.to_string(),
        (false, false) => format!("stdout:\n{stdout}\n\nstderr:\n{stderr}"),
    }
}

fn text_mentions_runtime_problem(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    [
        "sifs mirror failed",
        "auth",
        "credential",
        "dfs",
        "sandbox",
        "policy",
        "unavailable",
        "denied",
        "rejected",
        "blocked",
        "timed out",
        "timeout",
        "failed",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn text_mentions_connection_problem(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    [
        "chat connection",
        "chat list",
        "chat save",
        "chat unavailable",
        "sifs",
        "dfs",
        "daemon",
        "cluster",
        "device authorization",
        "session store",
        "channel store",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn text_mentions_chat_sync_problem(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.contains("agent session checkpoint")
        || lower.contains("roaming agent session")
        || lower.contains("reconciled chat channel")
}

fn connection_problem_summary(text: &str) -> Option<String> {
    if !text_mentions_connection_problem(text) {
        return None;
    }

    let lower = text.to_ascii_lowercase();
    let denied = lower.contains("device authorization") || lower.contains("denied");
    let summary = if text_mentions_chat_sync_problem(text) {
        "Chat session state could not be synchronized through SIFS."
    } else if lower.contains("agent channel") || lower.contains("channel store") {
        if denied {
            "Agent channel storage could not open through SIFS; device authorization was denied."
        } else {
            "Agent channel storage could not open through SIFS."
        }
    } else if lower.contains("chat list") {
        if denied {
            "Chat list storage could not be read; device authorization was denied."
        } else {
            "Chat list storage could not be read through SIFS."
        }
    } else if lower.contains("chat save") {
        if denied {
            "Chat history could not be saved; device authorization was denied."
        } else {
            "Chat history could not be saved through SIFS."
        }
    } else if lower.contains("device authorization") {
        "Device authorization blocked chat storage."
    } else if lower.contains("daemon") || lower.contains("cluster") {
        "Chat storage could not reach the cluster service."
    } else {
        "Chat storage needs SIFS access."
    };
    Some(summary.to_string())
}

fn connection_recovery_display_detail(text: &str) -> Option<String> {
    if !text_mentions_connection_problem(text) {
        return None;
    }

    let lower = text.to_ascii_lowercase();
    let summary = if text_mentions_chat_sync_problem(text) {
        "Sinomo could not synchronize this chat."
    } else if lower.contains("agent channel")
        || lower.contains("channel create")
        || lower.contains("channel store")
    {
        "Sinomo could not open this chat channel."
    } else if lower.contains("chat list") {
        "Sinomo could not load saved chats."
    } else if lower.contains("chat save") {
        "Sinomo could not save chat history."
    } else if lower.contains("daemon") || lower.contains("cluster") {
        "The local cluster service is not reachable."
    } else {
        "Chat needs connection attention."
    };
    Some(summary.to_string())
}

fn activity_visible_detail(text: &str) -> String {
    connection_recovery_display_detail(text).unwrap_or_else(|| compact_line(text, 96))
}

fn detail_should_offer_sifs_check(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    [
        "sifs",
        "dfs",
        "chat list",
        "chat save",
        "chat history",
        "session store",
        "channel store",
        "device authorization",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn activity_status_summary(text: &str) -> String {
    let lower = text.to_ascii_lowercase();
    let summary = if text_mentions_chat_sync_problem(text) {
        Some("Chat synchronization needs attention")
    } else if lower.contains("agent channel")
        || lower.contains("channel create")
        || lower.contains("channel store")
    {
        Some("Channel storage needs recovery")
    } else if lower.contains("chat list") {
        Some("Chat history needs recovery")
    } else if lower.contains("chat save") {
        Some("Chat history cannot be saved")
    } else if lower.contains("daemon") || lower.contains("cluster") {
        Some("Cluster service needs attention")
    } else if text_mentions_connection_problem(text) {
        Some("Chat storage needs recovery")
    } else if lower.contains("runtime control metadata") && lower.contains("detached") {
        Some("Runtime controls need refresh")
    } else {
        None
    };

    summary
        .map(str::to_string)
        .unwrap_or_else(|| activity_visible_detail(text))
}

fn count_label(count: usize, singular: &str, plural: &str) -> String {
    if count == 1 {
        format!("1 {singular}")
    } else {
        format!("{count} {plural}")
    }
}

fn activity_work_summary(chat: &ChatState) -> String {
    let active = chat.streaming || chat.control.active_turn;
    let tasks = chat.control.task_count.max(chat.control.tasks.len());
    let tools = chat.tool_calls.len();
    let queued = chat.control.steering_count + chat.control.follow_up_count;

    if !active && tasks == 0 && tools == 0 && queued == 0 {
        return "Idle · no queued work".to_string();
    }

    let mut parts = vec![if active { "Active" } else { "Idle" }.to_string()];
    if tasks > 0 {
        parts.push(count_label(tasks, "task", "tasks"));
    }
    if queued > 0 {
        parts.push(count_label(queued, "queued turn", "queued turns"));
    }
    if tools > 0 {
        parts.push(count_label(tools, "tool update", "tool updates"));
    }
    parts.join(" · ")
}

fn activity_button_label(chat: &ChatState) -> &'static str {
    match chat.runtime_health().tone {
        ActivityTone::Bad | ActivityTone::Warn => "Review",
        ActivityTone::Running => "Monitor",
        ActivityTone::Good | ActivityTone::Neutral => "Activity",
    }
}

fn activity_button_description(chat: &ChatState) -> String {
    let health = chat.runtime_health();
    let status = activity_status_summary(&health.detail);
    format!(
        "Show or hide runtime activity and details. {}: {}. {}",
        health.label,
        compact_line(&status, 72),
        activity_work_summary(chat)
    )
}

#[cfg(test)]
fn usage_summary(chat: &ChatState) -> Option<String> {
    chat.usage.as_ref().map(|usage| {
        let output = usage
            .output_tokens
            .map(format_token_count)
            .unwrap_or_else(|| "?".to_string());
        let input = usage
            .input_tokens
            .map(format_token_count)
            .unwrap_or_else(|| "?".to_string());
        format!("{input} in · {output} out")
    })
}

fn usage_activity_detail(usage: &ChatUsageSnapshot, streaming: bool) -> (String, ActivityTone) {
    let input = usage
        .input_tokens
        .map(|tokens| tokens.to_string())
        .unwrap_or_else(|| "?".to_string());
    match usage.output_tokens {
        Some(0) if streaming => (
            format!("{input} input · waiting for output"),
            ActivityTone::Running,
        ),
        Some(output) => (
            format!("{input} input · {output} output"),
            ActivityTone::Neutral,
        ),
        None if streaming => (
            format!("{input} input · waiting for output"),
            ActivityTone::Running,
        ),
        None => (format!("{input} input · ? output"), ActivityTone::Neutral),
    }
}

fn transcript_message_display_text(message: &ChatMessage) -> String {
    if message.kind != ChatMessageKind::Error {
        let text = if message.source == ChatMessageSource::Model {
            visible_model_message_text(&message.text)
        } else {
            message.text.clone()
        };
        return if text.is_empty() {
            "…".to_string()
        } else {
            text
        };
    }

    let detail = message
        .text
        .strip_prefix("Error:")
        .unwrap_or(message.text.as_str())
        .trim();
    if detail.is_empty() {
        return "Error\nNo detail was recorded.".to_string();
    }

    if text_mentions_connection_problem(detail) {
        let lower = detail.to_ascii_lowercase();
        let denied = lower.contains("device authorization") || lower.contains("denied");
        let (title, summary) = if text_mentions_chat_sync_problem(detail) {
            (
                "Chat sync issue",
                "This chat's roaming session state could not be synchronized.",
            )
        } else if lower.contains("agent channel") || lower.contains("channel store") {
            if denied {
                ("Connection issue", "This chat needs storage authorization.")
            } else {
                (
                    "Connection issue",
                    "Sinomo could not open this chat channel.",
                )
            }
        } else if lower.contains("chat list") {
            ("Connection issue", "Saved chats could not be loaded.")
        } else if lower.contains("chat save") {
            ("Connection issue", "This chat could not be saved.")
        } else if lower.contains("device authorization") {
            (
                "Connection issue",
                "This chat action needs storage authorization.",
            )
        } else if lower.contains("daemon") || lower.contains("cluster") {
            (
                "Connection issue",
                "The local cluster service is not reachable.",
            )
        } else {
            (
                "Connection issue",
                "The chat runtime could not reach a required cluster service.",
            )
        };
        let next = if detail_should_offer_sifs_check(detail) {
            "Check SIFS first. Connection settings has low-level detail."
        } else {
            "Open Connection settings for low-level detail."
        };
        format!("{title}\n{summary}\n{next}")
    } else {
        format!("Error\n{detail}")
    }
}

fn transcript_message_recovery_action(message: &ChatMessage) -> Option<RecoveryCardAction> {
    if message.kind != ChatMessageKind::Error || !text_mentions_connection_problem(&message.text) {
        return None;
    }

    if detail_should_offer_sifs_check(&message.text) {
        Some(RecoveryCardAction::CheckSifs)
    } else {
        Some(RecoveryCardAction::OpenConnectionSettings)
    }
}

/// A small uppercase section heading `Label` (AGENTS / CHATS), color tracking the theme.
fn heading(shared: &Shared, text: &str) -> Label {
    let s = shared.clone();
    Label::new(text)
        .style({
            let theme = shared.theme();
            sui::TextStyle {
                weight: FontWeight::SEMIBOLD,
                ..small_theme_text_style(&theme)
            }
        })
        .color_when(move || palette(&s).text_faint)
}

fn chats_heading(shared: &Shared) -> impl Widget + use<> {
    let s = shared.clone();
    RebuildOnChange::new(
        {
            let s = s.clone();
            move || {
                let chat = s.chat();
                chats_heading_text(&chat)
            }
        },
        move |text| WidgetPod::new(heading(&s, text)),
    )
}

fn muted_label(shared: &Shared, text: impl Into<String>) -> Label {
    let s = shared.clone();
    small_label(shared, text).color_when(move || palette(&s).text_muted)
}

fn small_text_style(shared: &Shared) -> sui::TextStyle {
    let theme = shared.theme();
    small_theme_text_style(&theme)
}

fn small_theme_text_style(theme: &DefaultTheme) -> sui::TextStyle {
    sui::TextStyle {
        font_size: theme.text.sm.size,
        line_height: theme.text.sm.line_height,
        ..theme.body_text_style()
    }
}

fn small_label(shared: &Shared, text: impl Into<String>) -> Label {
    Label::new(text).style(small_text_style(shared))
}

fn dialog_section_label(shared: &Shared, text: impl Into<String>) -> Label {
    let s = shared.clone();
    let theme = shared.theme();
    Label::new(text)
        .style(sui::TextStyle {
            weight: FontWeight::SEMIBOLD,
            ..small_theme_text_style(&theme)
        })
        .color_when(move || palette(&s).text_muted)
}

fn session_edit_details(
    shared: &Shared,
    session: &SessionRef,
    participant_count: usize,
    participant_names: &[String],
) -> impl Widget + use<> {
    let storage = match session.kind {
        ConversationKind::Chat => "SIFS-backed chat list",
        ConversationKind::Channel => "SIFS channel record and timeline",
    };
    let mut details = Stack::vertical()
        .spacing(6.0)
        .alignment(Alignment::Stretch)
        .with_child(dialog_section_label(shared, "Details"))
        .with_child(muted_label(
            shared,
            format!("Type: {}", session_kind_icon_description(session.kind)),
        ))
        .with_child(muted_label(
            shared,
            format!("Session ID: {}", compact_line(&session.id, 48)),
        ))
        .with_child(muted_label(
            shared,
            format!("Updated: {}", session_updated_label(session.updated_unix)),
        ))
        .with_child(muted_label(shared, format!("Storage: {storage}")));

    if session.kind == ConversationKind::Channel {
        details = details.with_child(muted_label(
            shared,
            format!("Agents: {}", active_agents_label(participant_count)),
        ));
        if is_untitled_channel(session) {
            details = details.with_child(muted_label(
                shared,
                "Title: Untitled channel; rename it to make it easy to find.",
            ));
        }
        if !participant_names.is_empty() {
            details = details.with_child(muted_label(
                shared,
                format!(
                    "Agent names: {}",
                    compact_line(&participant_names.join(", "), 56)
                ),
            ));
        }
    }

    details
}

fn session_meta_label(session: &SessionRef) -> String {
    let updated = session_updated_label(session.updated_unix);
    match session.kind {
        ConversationKind::Chat => updated,
        ConversationKind::Channel => {
            if is_untitled_channel(session) {
                format!(
                    "untitled · {} · {updated}",
                    plural_agents(session.participant_count)
                )
            } else {
                format!("{} · {updated}", plural_agents(session.participant_count))
            }
        }
    }
}

fn is_untitled_channel(session: &SessionRef) -> bool {
    session.kind == ConversationKind::Channel
        && session.title.trim().eq_ignore_ascii_case("new channel")
}

fn session_kind_icon_description(kind: ConversationKind) -> &'static str {
    match kind {
        ConversationKind::Chat => "Direct chat",
        ConversationKind::Channel => "Shared channel",
    }
}

fn normalized_session_filter(filter: &str) -> String {
    filter.trim().to_ascii_lowercase()
}

fn session_kind_filter_label(kind: ConversationKind) -> &'static str {
    match kind {
        ConversationKind::Chat => "chat",
        ConversationKind::Channel => "channel",
    }
}

fn session_matches_filter(session: &SessionRef, filter: &str) -> bool {
    if filter.is_empty() {
        return true;
    }
    session.title.to_ascii_lowercase().contains(filter)
        || session_meta_label(session)
            .to_ascii_lowercase()
            .contains(filter)
        || session_kind_filter_label(session.kind).contains(filter)
}

fn session_matches_kind_filter(session: &SessionRef, kind_filter: SessionKindFilter) -> bool {
    kind_filter.matches(session.kind)
}

fn session_kind_filter_count(sessions: &[SessionRef], kind_filter: SessionKindFilter) -> usize {
    sessions
        .iter()
        .filter(|session| session_matches_kind_filter(session, kind_filter))
        .count()
}

#[cfg(test)]
fn session_kind_filter_count_label(
    sessions: &[SessionRef],
    kind_filter: SessionKindFilter,
    loading: bool,
) -> String {
    if loading && sessions.is_empty() {
        "...".to_string()
    } else {
        session_kind_filter_count(sessions, kind_filter).to_string()
    }
}

fn filtered_session_indices(
    sessions: &[SessionRef],
    filter: &str,
    kind_filter: SessionKindFilter,
) -> Vec<usize> {
    sessions
        .iter()
        .enumerate()
        .filter_map(|(index, session)| {
            (session_matches_kind_filter(session, kind_filter)
                && session_matches_filter(session, filter))
            .then_some(index)
        })
        .collect()
}

fn chats_heading_text(chat: &ChatState) -> String {
    let kind_filter = chat.session_kind_filter;
    let filter = normalized_session_filter(&chat.session_filter);
    if chat.session_list_loading && chat.sessions.is_empty() {
        return format!("{} · loading", kind_filter.heading());
    }
    let total = chat
        .sessions
        .iter()
        .filter(|session| session_matches_kind_filter(session, kind_filter))
        .count();
    if filter.is_empty() {
        format!("{} · {total}", kind_filter.heading())
    } else {
        let shown = chat
            .sessions
            .iter()
            .filter(|session| {
                session_matches_kind_filter(session, kind_filter)
                    && session_matches_filter(session, &filter)
            })
            .count();
        format!("{} · {shown}/{total}", kind_filter.heading())
    }
}

fn empty_sessions_copy(
    chat: &ChatState,
    filter: &str,
    kind_filter: SessionKindFilter,
) -> (&'static str, Option<&'static str>) {
    if !filter.is_empty() {
        return ("No matching conversations", None);
    }
    if chat.visible_session_has_started()
        && !chat.current_is_channel()
        && matches!(
            kind_filter,
            SessionKindFilter::All | SessionKindFilter::Chats
        )
    {
        return ("Current chat is open", Some("Saved after history syncs"));
    }
    match kind_filter {
        SessionKindFilter::All => ("No conversations yet", None),
        SessionKindFilter::Chats => ("No chats yet", None),
        SessionKindFilter::Channels => ("No channels yet", None),
    }
}

fn loading_sessions_copy(chat: &ChatState) -> (&'static str, &'static str) {
    if chat.session_list_slow_notice_shown {
        ("History still loading...", "You can keep chatting")
    } else {
        ("Loading history...", "Saved chats and channels")
    }
}

fn plural_agents(count: usize) -> String {
    if count == 1 {
        "1 agent".to_string()
    } else {
        format!("{count} agents")
    }
}

fn active_agents_label(count: usize) -> String {
    if count == 1 {
        "1 active agent".to_string()
    } else {
        format!("{count} active agents")
    }
}

fn current_unix_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

fn session_updated_label(updated_unix: i64) -> String {
    if updated_unix <= 0 {
        return "not saved".to_string();
    }
    let now = current_unix_timestamp().max(updated_unix);
    let age = now.saturating_sub(updated_unix);
    if age < 60 {
        "just now".to_string()
    } else if age < 60 * 60 {
        format!("{}m ago", age / 60)
    } else if age < 24 * 60 * 60 {
        format!("{}h ago", age / (60 * 60))
    } else {
        format!("{}d ago", age / (24 * 60 * 60))
    }
}

fn agent_list_detail(agent: &AgentSummary) -> String {
    match agent.key.as_str() {
        "main-agent" => return "Interactive assistant".to_string(),
        "coding-agent" => return "Code and verification".to_string(),
        "evolution-agent" => return "Self-evolution workflows".to_string(),
        "background-agent" => return "Maintenance + cluster support".to_string(),
        _ => {}
    }
    let description = agent.description.trim();
    if !description.is_empty() {
        return compact_line(description, 36);
    }
    "Agent participant".to_string()
}

// ---------------------------------------------------------------------------
// Sidebar — agents + sessions ListViews (rebuilt when the lists change).
// ---------------------------------------------------------------------------

/// Build the AGENTS ListView: one row per agent, the active one selected, selecting one switches.
fn agents_list(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let items: Vec<ListItem> = {
        let chat = shared.chat();
        chat.agents
            .iter()
            .map(|agent| {
                let mut item = ListItem::new(agent.name.clone())
                    .detail(agent_list_detail(agent))
                    .leading_icon(IconGlyph::Sparkles);
                if chat.info.agent == agent.key {
                    item = item.trailing("Active");
                }
                item
            })
            .collect()
    };
    let s = shared.clone();
    ListView::new("chat-agents")
        .items(items)
        .row_height(SIDEBAR_ROW_H)
        .theme_when(small_control_reader(reader))
        .selected_when({
            let s = s.clone();
            move || {
                let chat = s.chat();
                (0..chat.agents.len()).find(|i| chat.is_active_agent(*i))
            }
        })
        .on_change(move |index, _label| {
            s.chat_mut().select_agent(index);
        })
}

/// Build the CHATS ListView: stored chats/channels only. Creation actions live
/// above the scroll area so the list itself behaves like persistent history.
fn sessions_list(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let (sessions, filter, kind_filter, loading, empty_copy) = {
        let chat = shared.chat();
        let filter = normalized_session_filter(&chat.session_filter);
        let kind_filter = chat.session_kind_filter;
        let empty_copy = empty_sessions_copy(&chat, &filter, kind_filter);
        (
            chat.sessions.clone(),
            filter,
            kind_filter,
            chat.session_list_loading,
            empty_copy,
        )
    };
    let visible_indices = filtered_session_indices(&sessions, &filter, kind_filter);
    let mut items = visible_indices
        .iter()
        .map(|index| {
            let session = sessions[*index].clone();
            ListItem::new(session.title.clone())
                .description(session_row_description(&session))
                .activate_with_child()
                .with_child(session_row_content(shared, reader, session))
        })
        .collect::<Vec<_>>();
    let slow_loading = loading && sessions.is_empty() && {
        let chat = shared.chat();
        chat.session_list_slow_notice_shown
    };
    if loading && sessions.is_empty() {
        let (label, detail, details_open) = {
            let chat = shared.chat();
            let (label, detail) = loading_sessions_copy(&chat);
            (label, detail, chat.session_list_details_open)
        };
        let mut item = ListItem::new(label).detail(detail).leading_text("...");
        if slow_loading {
            item = item.trailing(if details_open { "Hide" } else { "Details" });
        } else {
            item = item.disabled();
        }
        items.push(item);
    } else if visible_indices.is_empty() {
        let (label, detail) = empty_copy;
        let mut item = ListItem::new(label).leading_text(" ").disabled();
        if let Some(detail) = detail {
            item = item.detail(detail);
        }
        items.push(item);
    }
    let s = shared.clone();
    ListView::new("chat-sessions")
        .items(items)
        .selected_when({
            let s = s.clone();
            move || {
                let chat = s.chat();
                let filter = normalized_session_filter(&chat.session_filter);
                filtered_session_indices(&chat.sessions, &filter, chat.session_kind_filter)
                    .into_iter()
                    .position(|session_index| chat.sessions[session_index].id == chat.session_id)
                    .map(|index| index)
            }
        })
        .row_height(SIDEBAR_ROW_H)
        .theme_when(small_control_reader(reader))
        .on_change(move |index, _label| {
            let mut chat = s.chat_mut();
            if chat.session_list_loading && chat.sessions.is_empty() {
                if index == 0 && chat.session_list_slow_notice_shown {
                    chat.toggle_session_list_details();
                }
                return;
            }
            let filter = normalized_session_filter(&chat.session_filter);
            let visible_indices =
                filtered_session_indices(&chat.sessions, &filter, chat.session_kind_filter);
            if let Some(session_index) = visible_indices.get(index) {
                chat.open_session(*session_index);
            }
        })
}

fn session_create_actions(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let small_reader: ThemeReader = {
        let reader = Rc::clone(reader);
        Rc::new(move || reader().with_size(ControlSize::Small))
    };
    let new_chat = {
        let s = shared.clone();
        sui::Button::new("New chat")
            .icon(IconGlyph::Chat)
            .min_height(28.0)
            .theme_when(clone_reader(&small_reader))
            .on_press(move || {
                s.chat_mut().new_session();
            })
    };
    let new_channel = {
        let s = shared.clone();
        sui::Button::new("New channel")
            .icon(IconGlyph::Add)
            .min_height(28.0)
            .theme_when(clone_reader(&small_reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .on_press_with_ctx(move |ctx| {
                let waker = { s.borrow().waker.clone() };
                s.chat_mut().new_channel(waker);
                request_chat_window_relayout(ctx);
            })
    };

    Flex::horizontal()
        .main_gap(6.0)
        .align_items(Alignment::Center)
        .with_item(new_chat, FlexItem::flex(1.0).min_width(0.0))
        .with_item(new_channel, FlexItem::flex(1.0).min_width(0.0))
}

fn request_chat_window_relayout(ctx: &mut EventCtx) {
    use sui::{InvalidationKind, InvalidationRequest, InvalidationTarget};

    let window = InvalidationTarget::Window(ctx.window_id());
    ctx.request(InvalidationRequest::new(window, InvalidationKind::Measure));
    ctx.request(InvalidationRequest::new(window, InvalidationKind::Arrange));
    ctx.request_paint();
    ctx.request_semantics();
}

fn session_filter_input(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let s = shared.clone();
    let value = shared.chat().session_filter.clone();
    TextInput::new("chat-session-filter")
        .value(value)
        .placeholder("Search conversations")
        .leading_icon(IconGlyph::Search)
        .theme_when(small_control_reader(reader))
        .on_change(move |value| {
            s.chat_mut().set_session_filter(value);
        })
}

fn sessions_scroll_signature(
    shared: &Shared,
) -> (
    String,
    String,
    SessionKindFilter,
    String,
    String,
    String,
    String,
    usize,
    bool,
    bool,
) {
    let chat = shared.chat();
    let sessions = chat
        .sessions
        .iter()
        .map(|s| format!("{}:{:?}:{}:{}", s.id, s.kind, s.title, s.participant_count))
        .collect::<Vec<_>>()
        .join("\u{1f}");
    (
        sessions,
        chat.session_filter.clone(),
        chat.session_kind_filter,
        chat.session_id.clone(),
        chat.session_edit_id.clone().unwrap_or_default(),
        chat.session_delete_confirm_id.clone().unwrap_or_default(),
        chat.pending_delete_id.clone().unwrap_or_default(),
        chat.channel_agent_idx,
        chat.session_list_loading,
        chat.session_list_slow_notice_shown,
    )
}

fn sessions_scroll(
    shared: &Shared,
    reader: &ThemeReader,
    scroll_state: &ScrollState,
) -> impl Widget + use<> {
    ScrollView::vertical(sessions_list(shared, reader))
        .name("chat-sessions-scroll")
        .state(scroll_state.clone())
        .theme(shared.theme())
}

fn sessions_scroll_rebuild(
    shared: &Shared,
    reader: &ThemeReader,
    scroll_state: &ScrollState,
) -> impl Widget + use<> {
    let s = shared.clone();
    let r = Rc::clone(reader);
    let sessions_scroll_state = scroll_state.clone();
    RebuildOnChange::new(
        {
            let s = s.clone();
            move || sessions_scroll_signature(&s)
        },
        move |_sig| WidgetPod::new(sessions_scroll(&s, &r, &sessions_scroll_state)),
    )
}

fn request_session_filter_refresh(ctx: &mut EventCtx) {
    use sui::{InvalidationKind, InvalidationRequest, InvalidationTarget};

    let window = InvalidationTarget::Window(ctx.window_id());
    ctx.request(InvalidationRequest::new(window, InvalidationKind::Measure));
    ctx.request(InvalidationRequest::new(window, InvalidationKind::Arrange));
    ctx.request_paint();
    ctx.request_semantics();
}

fn session_kind_filter_control(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let (items, selected) = {
        let chat = shared.chat();
        let selected = SessionKindFilter::ALL
            .iter()
            .position(|filter| *filter == chat.session_kind_filter)
            .unwrap_or(0);
        let items = SessionKindFilter::ALL
            .iter()
            .copied()
            .map(|filter| {
                // Counts remain in the semantic description and the active section heading. Keep
                // the three visible segments short enough to survive large text in the sidebar.
                let label = filter.label().to_string();
                let description = if chat.session_list_loading && chat.sessions.is_empty() {
                    "Loading conversations".to_string()
                } else {
                    format!(
                        "{} visible conversation(s)",
                        session_kind_filter_count(&chat.sessions, filter)
                    )
                };
                SegmentedControlItem::new(label)
                    .semantic_name(filter.semantic_name())
                    .description(description)
            })
            .collect::<Vec<_>>();
        (items, selected)
    };
    let s = shared.clone();

    SegmentedControl::new("Conversation kind")
        .items(items)
        .selected(selected)
        .theme_when(clone_reader(reader))
        .on_change_with_ctx(move |index, _label, ctx| {
            if let Some(filter) = SessionKindFilter::ALL.get(index).copied() {
                s.chat_mut().set_session_kind_filter(filter);
                request_session_filter_refresh(ctx);
            }
        })
}

fn session_row_description(session: &SessionRef) -> String {
    match session.kind {
        ConversationKind::Chat => session_kind_icon_description(session.kind).to_string(),
        ConversationKind::Channel => {
            let prefix = if is_untitled_channel(session) {
                "Untitled shared channel"
            } else {
                session_kind_icon_description(session.kind)
            };
            format!("{prefix}, {} agents", session.participant_count)
        }
    }
}

fn session_row_content(
    shared: &Shared,
    reader: &ThemeReader,
    session: SessionRef,
) -> impl Widget + use<> {
    let theme = reader();
    let pal = Palette::from_theme(&theme, widgets::theme_is_dark(shared));
    let active = shared.chat().session_id == session.id;
    let editing = shared.chat().session_edit_id.as_deref() == Some(session.id.as_str());
    let label_color = if active { pal.text } else { pal.text_muted };
    let title_style = pal.text_style(&theme, 12.0, FontWeight::MEDIUM, label_color);
    let meta_style = pal.text_style(&theme, 11.0, FontWeight::NORMAL, pal.text_faint);
    let icon_color = match session.kind {
        ConversationKind::Chat if active => pal.accent,
        ConversationKind::Chat => pal.text_faint,
        ConversationKind::Channel => pal.accent,
    };

    let labels = Flex::vertical()
        .align_items(Alignment::Start)
        .with_child(Label::new(session.title.clone()).style(title_style))
        .with_child(Label::new(session_meta_label(&session)).style(meta_style));
    let body = Flex::horizontal()
        .main_gap(7.0)
        .align_items(Alignment::Center)
        .with_child(
            SizedBox::new()
                .width(20.0)
                .height(20.0)
                .with_child(Icon::new(IconGlyph::Chat).size(16.0).color(icon_color)),
        )
        .with_item(labels, FlexItem::flex(1.0).min_width(0.0));

    let s = shared.clone();
    let session_id = session.id.clone();
    let edit = IconButton::new(IconGlyph::MoreHorizontal, format!("Edit {}", session.title))
        .size(SIDEBAR_ROW_EDIT_H)
        .icon_size(14.0)
        .selected(editing)
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .on_press_with_ctx(move |ctx| {
            s.chat_mut().begin_edit_session(&session_id);
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_paint();
            ctx.request_semantics();
        });

    TrailingSlotRow::new(body, edit)
        .trailing_width(SIDEBAR_ROW_EDIT_W.max(SIDEBAR_ROW_EDIT_H))
        .trailing_height(SIDEBAR_ROW_EDIT_H)
        .gap(SIDEBAR_ROW_EDIT_GAP)
}

/// The sidebar column: AGENTS heading + list, then conversations. Wrapped in a
/// [`RebuildOnChange`] keyed on the agent/chat signature so it rebuilds when those lists change
/// (e.g. a new chat is persisted) but not on every token.
fn sidebar(
    shared: &Shared,
    reader: &ThemeReader,
    scroll_state: &ScrollState,
) -> impl Widget + use<> {
    let s = shared.clone();
    let r = Rc::clone(reader);
    let sessions_scroll_state = scroll_state.clone();
    RebuildOnChange::new(
        {
            let s = s.clone();
            move || sidebar_signature(&s)
        },
        move |_sig| {
            let mut column = Flex::vertical()
                .main_gap(4.0)
                .align_items(Alignment::Stretch)
                .with_child(SizedBox::new().height(18.0))
                .with_child(
                    WidgetPadding::horizontal(18.0, heading(&s, "AGENTS")).fill_child_height(),
                )
                .with_child(SizedBox::new().height(2.0))
                .with_child(WidgetPadding::horizontal(8.0, agents_list(&s, &r)).fill_child_height())
                .with_child(SizedBox::new().height(16.0))
                .with_child(WidgetPadding::horizontal(18.0, chats_heading(&s)).fill_child_height())
                .with_child(SizedBox::new().height(6.0))
                .with_child(
                    WidgetPadding::horizontal(
                        8.0,
                        SizedBox::new()
                            .height(28.0)
                            .with_child(session_kind_filter_control(&s, &r)),
                    )
                    .fill_child_height(),
                )
                .with_child(SizedBox::new().height(6.0))
                .with_child(
                    WidgetPadding::horizontal(8.0, session_filter_input(&s, &r)).fill_child_width(),
                )
                .with_child(SizedBox::new().height(6.0))
                .with_child(
                    WidgetPadding::horizontal(8.0, session_create_actions(&s, &r)).fill_child(),
                )
                .with_child(SizedBox::new().height(8.0))
                .with_item(
                    WidgetPadding::horizontal(
                        8.0,
                        sessions_scroll_rebuild(&s, &r, &sessions_scroll_state),
                    )
                    .fill_child_height(),
                    FlexItem::flex(1.0).min_height(0.0),
                );
            if let Some(actions) = slow_session_load_actions(&s, &r) {
                column = column
                    .with_child(SizedBox::new().height(8.0))
                    .with_child(WidgetPadding::horizontal(8.0, actions).fill_child_height())
                    .with_child(SizedBox::new().height(8.0));
            }
            WidgetPod::new(Surface::sidebar(column).fill().theme_when(clone_reader(&r)))
        },
    )
}

/// A signature of the sidebar's list contents (agents are static; sessions + the active agent
/// change). Used to drive the sidebar [`RebuildOnChange`].
fn sidebar_signature(
    shared: &Shared,
) -> (
    String,
    String,
    SessionKindFilter,
    String,
    String,
    String,
    usize,
    bool,
    bool,
    bool,
) {
    let chat = shared.chat();
    let active = chat
        .agents
        .iter()
        .enumerate()
        .find(|(i, _)| chat.is_active_agent(*i))
        .map(|(_, a)| a.key.clone())
        .unwrap_or_default();
    let sessions = chat
        .sessions
        .iter()
        .map(|s| format!("{}:{:?}:{}:{}", s.id, s.kind, s.title, s.participant_count))
        .collect::<Vec<_>>()
        .join("\u{1f}");
    (
        active,
        sessions,
        chat.session_kind_filter,
        chat.session_id.clone(),
        chat.session_edit_id.clone().unwrap_or_default(),
        chat.session_delete_confirm_id.clone().unwrap_or_default(),
        chat.channel_agent_idx,
        chat.session_list_loading,
        chat.session_list_slow_notice_shown,
        chat.session_list_details_open,
    )
}

fn slow_session_load_actions(shared: &Shared, reader: &ThemeReader) -> Option<impl Widget + use<>> {
    let details_open = {
        let chat = shared.chat();
        if !(chat.session_list_loading && chat.session_list_slow_notice_shown) {
            return None;
        }
        if !chat.session_list_details_open {
            return None;
        }
        chat.session_list_details_open
    };

    let details = {
        let s = shared.clone();
        sui::Button::new(if details_open { "Hide" } else { "Details" })
            .icon(if details_open {
                IconGlyph::ChevronUp
            } else {
                IconGlyph::ChevronDown
            })
            .min_height(26.0)
            .min_width(78.0)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .on_press(move || {
                s.chat_mut().toggle_session_list_details();
            })
    };

    let retry = {
        let s = shared.clone();
        sui::Button::new("Retry")
            .icon(IconGlyph::Redo)
            .min_height(28.0)
            .min_width(78.0)
            .theme_when(composer_control_reader(reader))
            .on_press(move || {
                let waker = { s.borrow().waker.clone() };
                s.chat_mut().retry_chat_list_with_waker(waker);
            })
    };
    let check_sifs = {
        let s = shared.clone();
        sui::Button::new("Check SIFS")
            .icon(IconGlyph::Storage)
            .min_height(28.0)
            .min_width(112.0)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .on_press(move || open_files_tab(&s))
    };
    let connection = {
        let s = shared.clone();
        sui::Button::new("Connection")
            .icon(IconGlyph::Alert)
            .min_height(28.0)
            .min_width(128.0)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .on_press(move || open_settings_section(&s, SettingsSection::Connection))
    };

    let actions = Flex::horizontal()
        .main_gap(6.0)
        .align_items(Alignment::Stretch)
        .with_item(
            SizedBox::new().height(28.0).with_child(retry),
            FlexItem::flex(1.0).min_width(78.0),
        )
        .with_item(
            SizedBox::new().height(28.0).with_child(check_sifs),
            FlexItem::flex(1.35).min_width(112.0),
        );
    let summary = TrailingSlotRow::new(
        dialog_section_label(shared, "History diagnostics"),
        Flex::horizontal()
            .align_items(Alignment::Center)
            .spacer()
            .with_child(details),
    )
    .trailing_width(86.0)
    .trailing_height(26.0)
    .gap(8.0);
    let mut body = Flex::vertical()
        .main_gap(6.0)
        .align_items(Alignment::Stretch)
        .with_child(SizedBox::new().height(26.0).with_child(summary));

    if details_open {
        body = body
            .with_child(muted_label(
                shared,
                "Chat and channel history stays in SIFS. You can keep chatting while it syncs.",
            ))
            .with_child(SizedBox::new().height(28.0).with_child(actions))
            .with_child(SizedBox::new().height(28.0).with_child(connection));
    }

    Some(
        Surface::panel(body)
            .padding(Insets {
                left: 10.0,
                top: 10.0,
                right: 10.0,
                bottom: 10.0,
            })
            .fill_width()
            .theme_when(clone_reader(reader)),
    )
}

fn session_edit_dialog(shared: &Shared, reader: &ThemeReader) -> Option<WidgetPod> {
    let (
        session,
        draft,
        confirming_delete,
        details_open,
        participants,
        agent_status,
        agent_keys,
        agent_options,
        rename_pending,
        rename_error,
    ) = {
        let chat = shared.chat();
        let edit_id = chat.session_edit_id.as_deref()?;
        let session = chat
            .sessions
            .iter()
            .find(|session| session.id == edit_id)
            .cloned()?;
        let creating_channel = chat.channel_create_rx.is_some() && session.id == chat.session_id;
        let mut participants = if session.kind == ConversationKind::Channel && !creating_channel {
            chat.channel_agent_refs(&session.id)
        } else {
            Vec::new()
        };
        if session.kind == ConversationKind::Channel && participants.is_empty() {
            participants = ChatState::fallback_channel_agent_refs(&session, &chat.agents);
        }
        let agent_status = if chat
            .pending_channel_config
            .as_ref()
            .is_some_and(|pending| pending.channel_id == edit_id)
        {
            Some(ChannelAgentsStoreStatus::Saving)
        } else if let Some(error) = chat
            .channel_config_error
            .as_ref()
            .filter(|error| error.channel_id == edit_id)
        {
            Some(ChannelAgentsStoreStatus::Failed(error.detail.clone()))
        } else {
            channel_agents_store_status(&participants, creating_channel, chat.notice.as_deref())
        };
        let agent_keys = chat
            .agents
            .iter()
            .map(|agent| agent.key.clone())
            .collect::<Vec<_>>();
        let agent_options = chat
            .agents
            .iter()
            .map(|agent| agent.name.clone())
            .collect::<Vec<_>>();
        let rename_pending = chat
            .pending_session_rename
            .as_ref()
            .is_some_and(|pending| pending.session_id == edit_id);
        let rename_error = chat
            .session_rename_error
            .as_ref()
            .filter(|error| error.session_id == edit_id)
            .map(|error| error.detail.clone());
        (
            session,
            chat.session_title_draft.clone(),
            chat.session_delete_confirm_id.as_deref() == Some(edit_id),
            chat.session_edit_details_open,
            participants,
            agent_status,
            agent_keys,
            agent_options,
            rename_pending,
            rename_error,
        )
    };
    let session_id = session.id.clone();
    let is_channel = session.kind == ConversationKind::Channel;
    let participant_count = if is_channel {
        participants
            .len()
            .max(session.participant_count)
            .max(session.participant_names.len())
    } else {
        session.participant_count
    };
    let participant_names = if is_channel {
        let names = participants
            .iter()
            .map(|participant| participant.agent_name.clone())
            .collect::<Vec<_>>();
        if names.is_empty() {
            session.participant_names.clone()
        } else {
            names
        }
    } else {
        Vec::new()
    };
    let title = if is_channel {
        "Edit channel"
    } else {
        "Edit chat"
    };
    let placeholder = if is_channel {
        "Channel title"
    } else {
        "Chat title"
    };

    let input = {
        let s = shared.clone();
        TextInput::new(format!("chat-session-title-{session_id}"))
            .value(draft)
            .placeholder(placeholder)
            .theme_when(clone_reader(reader))
            .on_change(move |value| {
                s.chat_mut().set_session_title_draft(value);
            })
    };
    let save = {
        let s = shared.clone();
        sui::Button::new(if rename_pending { "Saving…" } else { "Save" })
            .icon(IconGlyph::Check)
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || s.chat().can_save_session_title()
            })
            .on_press_with_ctx(move |ctx| {
                s.chat_mut().save_session_title();
                ctx.request_measure();
                ctx.request_paint();
            })
    };
    let done = {
        let s = shared.clone();
        sui::Button::new("Done")
            .icon(IconGlyph::Close)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .on_press_with_ctx(move |ctx| {
                s.chat_mut().close_session_edit();
                ctx.request_measure();
                ctx.request_paint();
            })
    };
    let close = {
        let s = shared.clone();
        IconButton::new(IconGlyph::Close, "Close edit dialog")
            .size(28.0)
            .icon_size(14.0)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Ghost)
            .tone(SemanticTone::Neutral)
            .on_press_with_ctx(move |ctx| {
                s.chat_mut().close_session_edit();
                ctx.request_measure();
                ctx.request_paint();
                ctx.request_semantics();
            })
    };
    let details_toggle = {
        let s = shared.clone();
        sui::Button::new(if details_open {
            "Hide details"
        } else {
            "Show details"
        })
        .icon(if details_open {
            IconGlyph::ChevronUp
        } else {
            IconGlyph::ChevronDown
        })
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .on_press_with_ctx(move |ctx| {
            s.chat_mut().toggle_session_edit_details();
            ctx.request_measure();
            ctx.request_paint();
            ctx.request_semantics();
        })
    };

    let mut body = Stack::vertical()
        .spacing(10.0)
        .alignment(Alignment::Stretch)
        .with_child(
            TrailingSlotRow::new(dialog_section_label(shared, "Name"), close)
                .trailing_width(28.0)
                .trailing_height(28.0)
                .gap(8.0),
        )
        .with_child(SizedBox::new().height(32.0).with_child(input));
    if is_untitled_channel(&session) {
        body = body.with_child(muted_label(
            shared,
            "Untitled channel. Rename it so it is easy to recognize in the sidebar.",
        ));
    }
    if let Some(error) = rename_error {
        body = body.with_child(muted_label(
            shared,
            format!("Rename failed: {error}. Edit the title or retry Save."),
        ));
    }
    body = body
        .with_child(
            Flex::horizontal()
                .gap(6.0)
                .align_items(Alignment::Center)
                .spacer()
                .with_child(done)
                .with_child(save),
        )
        .with_child(
            Flex::horizontal()
                .gap(6.0)
                .align_items(Alignment::Center)
                .spacer()
                .with_child(details_toggle),
        );

    if is_channel {
        body = body
            .with_child(Separator::horizontal().theme_when(clone_reader(reader)))
            .with_child(channel_agents_editor(
                shared,
                reader,
                session_id.clone(),
                participants,
                agent_status,
                agent_keys,
                agent_options,
            ));
    }

    if details_open {
        body = body.with_child(session_edit_details(
            shared,
            &session,
            participant_count,
            &participant_names,
        ));
    }

    let delete_label = if is_channel {
        "Delete channel"
    } else {
        "Delete chat"
    };
    if confirming_delete {
        body = body.with_child(Separator::horizontal().theme_when(clone_reader(reader)));
        let confirm_title = compact_line(&session.title, 34);
        let confirm = {
            let s = shared.clone();
            sui::Button::new(delete_label)
                .icon(IconGlyph::Trash)
                .theme_when(clone_reader(reader))
                .appearance(sui::ButtonAppearance::Filled)
                .tone(SemanticTone::Danger)
                .on_press_with_ctx(move |ctx| {
                    let waker = s.borrow().waker.clone();
                    s.chat_mut().confirm_delete_session(waker);
                    ctx.request_measure();
                    ctx.request_paint();
                })
        };
        let cancel = {
            let s = shared.clone();
            sui::Button::new("Cancel")
                .icon(IconGlyph::Close)
                .theme_when(clone_reader(reader))
                .appearance(sui::ButtonAppearance::Tonal)
                .tone(SemanticTone::Neutral)
                .on_press_with_ctx(move |ctx| {
                    s.chat_mut().cancel_delete_session();
                    ctx.request_measure();
                    ctx.request_paint();
                })
        };
        body = body
            .with_child(dialog_section_label(shared, "Danger zone"))
            .with_child(muted_label(
                shared,
                format!("Delete \"{confirm_title}\" from SIFS?"),
            ))
            .with_child(
                Flex::horizontal()
                    .gap(6.0)
                    .align_items(Alignment::Center)
                    .with_child(cancel)
                    .spacer()
                    .with_child(confirm),
            );
    } else if details_open {
        body = body.with_child(Separator::horizontal().theme_when(clone_reader(reader)));
        let delete_session_id = session.id.clone();
        let delete = {
            let s = shared.clone();
            sui::Button::new(delete_label)
                .icon(IconGlyph::Trash)
                .theme_when(clone_reader(reader))
                .appearance(sui::ButtonAppearance::Filled)
                .tone(SemanticTone::Danger)
                .enabled_when({
                    let s = s.clone();
                    move || {
                        let chat = s.chat();
                        !chat.streaming
                            && !chat.control.active_turn
                            && chat.pending_delete_id.is_none()
                    }
                })
                .on_press_with_ctx(move |ctx| {
                    s.chat_mut().request_delete_session(&delete_session_id);
                    ctx.request_measure();
                    ctx.request_paint();
                })
        };
        let delete_hint = if is_channel {
            "Removes this channel timeline and its channel membership record."
        } else {
            "Removes this chat timeline from the SIFS-backed chat list."
        };
        body = body
            .with_child(dialog_section_label(shared, "Danger zone"))
            .with_child(muted_label(shared, delete_hint))
            .with_child(
                Flex::horizontal()
                    .gap(6.0)
                    .align_items(Alignment::Center)
                    .spacer()
                    .with_child(delete),
            );
    }

    let s = shared.clone();
    let body = SizedBox::new().height(520.0).with_child(
        ScrollView::vertical(body)
            .name("chat-session-editor-scroll")
            .state(ScrollState::new())
            .theme(reader()),
    );
    Some(WidgetPod::new(
        sui::Dialog::new(title, body)
            .theme(shared.theme())
            .max_width(760.0)
            .dismiss_on_scrim(true)
            .on_dismiss(move || {
                s.chat_mut().close_session_edit();
            }),
    ))
}

fn channel_agents_store_status(
    participants: &[ChannelAgentRef],
    creating_channel: bool,
    notice: Option<&str>,
) -> Option<ChannelAgentsStoreStatus> {
    if !participants
        .iter()
        .any(|participant| participant.pending_store)
    {
        return None;
    }
    let notice = notice.unwrap_or_default();
    let lower = notice.to_ascii_lowercase();
    if lower.contains("channel create failed") || lower.contains("channel config failed") {
        return Some(ChannelAgentsStoreStatus::Failed(notice.to_string()));
    }
    if creating_channel {
        Some(ChannelAgentsStoreStatus::Saving)
    } else {
        Some(ChannelAgentsStoreStatus::Failed(
            "channel membership has not reached SIFS yet".to_string(),
        ))
    }
}

fn channel_agents_editor(
    shared: &Shared,
    reader: &ThemeReader,
    channel_id: String,
    participants: Vec<ChannelAgentRef>,
    agent_status: Option<ChannelAgentsStoreStatus>,
    agent_keys: Vec<String>,
    agent_options: Vec<String>,
) -> impl Widget + use<> {
    let mutation_pending = shared.chat().pending_channel_config.is_some();
    let participant_count = participants.len();
    let assigned_agent_keys = participants
        .iter()
        .map(|participant| participant.agent_key.clone())
        .collect::<Vec<_>>();
    let mut list = Stack::vertical()
        .spacing(4.0)
        .alignment(Alignment::Stretch)
        .with_child(
            Flex::horizontal()
                .gap(8.0)
                .align_items(Alignment::Center)
                .with_child(dialog_section_label(shared, "Agents"))
                .spacer()
                .with_child(muted_label(shared, format!("{} active", participant_count))),
        );

    if participants.is_empty() {
        list = list.with_child(muted_label(shared, "No agents assigned yet."));
    } else {
        for participant in participants {
            let row_channel_id = channel_id.clone();
            let participant_id = participant.participant_id.clone();
            let base_label = if participant.agent_key == participant.agent_name {
                participant.agent_name
            } else {
                format!("{} ({})", participant.agent_name, participant.agent_key)
            };
            let label = if participant.pending_store {
                let suffix = match agent_status.as_ref() {
                    Some(ChannelAgentsStoreStatus::Failed(_)) => "not saved",
                    _ => "saving",
                };
                format!("{base_label} ({suffix})")
            } else {
                base_label
            };
            let remove_label = label.clone();
            let action = if participant.pending_store {
                let action_label = match agent_status.as_ref() {
                    Some(ChannelAgentsStoreStatus::Failed(_)) => {
                        format!("Connection needed for {remove_label}")
                    }
                    _ => format!("Waiting for {remove_label}"),
                };
                IconButton::new(IconGlyph::Alert, action_label)
                    .size(26.0)
                    .icon_size(14.0)
                    .enabled(false)
                    .theme_when(clone_reader(reader))
            } else {
                let s = shared.clone();
                let enabled_shared = shared.clone();
                IconButton::new(IconGlyph::Trash, format!("Remove {remove_label}"))
                    .size(26.0)
                    .icon_size(14.0)
                    .theme_when(clone_reader(reader))
                    .enabled_when(move || enabled_shared.chat().pending_channel_config.is_none())
                    .on_press_with_ctx(move |ctx| {
                        s.chat_mut()
                            .remove_channel_agent(&row_channel_id, &participant_id);
                        ctx.request_measure();
                        ctx.request_paint();
                    })
            };
            list = list.with_child(channel_agent_row(shared, reader, label, action));
        }
    }

    if let Some(agent_status) = agent_status {
        match agent_status {
            ChannelAgentsStoreStatus::Saving => {
                list = list.with_child(muted_label(shared, "Saving channel membership to SIFS..."));
            }
            ChannelAgentsStoreStatus::Failed(detail) => {
                let settings = {
                    let s = shared.clone();
                    sui::Button::new("Open connection settings")
                        .icon(IconGlyph::Alert)
                        .theme_when(clone_reader(reader))
                        .appearance(sui::ButtonAppearance::Tonal)
                        .tone(SemanticTone::Neutral)
                        .on_press(move || open_settings_section(&s, SettingsSection::Connection))
                };
                list = list
                    .with_child(muted_label(
                        shared,
                        "Channel membership is not saved. Connection settings has recovery detail.",
                    ))
                    .with_child(
                        Flex::horizontal()
                            .gap(6.0)
                            .align_items(Alignment::Center)
                            .with_child(settings)
                            .spacer()
                            .with_child(muted_label(
                                shared,
                                connection_recovery_display_detail(&detail)
                                    .unwrap_or_else(|| "SIFS needs attention.".to_string()),
                            )),
                    );
            }
        }
    }

    let picker = {
        let s = shared.clone();
        sui::Select::new(format!("channel-agent-picker-{channel_id}"))
            .options(agent_options)
            .placeholder("Agent")
            .theme_when(clone_reader(reader))
            .selected_when({
                let s = s.clone();
                move || {
                    let chat = s.chat();
                    (!chat.agents.is_empty()).then_some(
                        chat.channel_agent_idx
                            .min(chat.agents.len().saturating_sub(1)),
                    )
                }
            })
            .on_change(move |index, _label| {
                s.chat_mut().set_channel_agent_idx(index);
            })
    };
    let add = {
        let s = shared.clone();
        sui::Button::new(if mutation_pending {
            "Updating…"
        } else {
            "Add agent"
        })
        .icon(IconGlyph::Add)
        .theme_when(clone_reader(reader))
        .enabled_when({
            let s = s.clone();
            let agent_keys = agent_keys.clone();
            let assigned_agent_keys = assigned_agent_keys.clone();
            move || {
                let chat = s.chat();
                if chat.pending_channel_config.is_some() {
                    return false;
                }
                let Some(agent_key) = agent_keys.get(chat.channel_agent_idx) else {
                    return false;
                };
                !assigned_agent_keys
                    .iter()
                    .any(|assigned_key| assigned_key == agent_key)
            }
        })
        .on_press_with_ctx(move |ctx| {
            s.chat_mut().add_selected_agent_to_channel();
            ctx.request_measure();
            ctx.request_paint();
        })
    };

    Stack::vertical()
        .spacing(8.0)
        .alignment(Alignment::Stretch)
        .with_child(list)
        .with_child(SizedBox::new().height(SELECTOR_H).with_child(picker))
        .with_child(
            Flex::horizontal()
                .gap(6.0)
                .align_items(Alignment::Center)
                .spacer()
                .with_child(add),
        )
}

const CHANNEL_AGENT_ROW_H: f32 = 32.0;
const CHANNEL_AGENT_ROW_ACTION: f32 = 26.0;
const CHANNEL_AGENT_ROW_GAP: f32 = 8.0;
const CHANNEL_AGENT_ROW_ICON: f32 = 15.0;

fn channel_agent_row(
    shared: &Shared,
    reader: &ThemeReader,
    label: String,
    action: IconButton,
) -> impl Widget + use<> {
    let icon_color = {
        let s = shared.clone();
        move || palette(&s).accent
    };
    let text_color = {
        let s = shared.clone();
        move || palette(&s).text
    };
    let icon = SizedBox::new()
        .width(CHANNEL_AGENT_ROW_ICON + 2.0)
        .height(CHANNEL_AGENT_ROW_H)
        .with_child(
            Icon::new(IconGlyph::Sparkles)
                .size(CHANNEL_AGENT_ROW_ICON)
                .color_when(icon_color)
                .label("Channel agent"),
        );
    let body = Flex::horizontal()
        .main_gap(7.0)
        .align_items(Alignment::Center)
        .with_item(icon, FlexItem::new().no_shrink())
        .with_item(
            small_label(shared, label).color_when(text_color),
            FlexItem::new().grow(1.0).min_width(0.0),
        );
    let row = TrailingSlotRow::new(body, action)
        .trailing_width(CHANNEL_AGENT_ROW_ACTION)
        .trailing_height(CHANNEL_AGENT_ROW_ACTION)
        .gap(CHANNEL_AGENT_ROW_GAP);

    SizedBox::new().height(CHANNEL_AGENT_ROW_H).with_child(
        Surface::field(row)
            .padding(Insets {
                left: 9.0,
                top: 0.0,
                right: 3.0,
                bottom: 0.0,
            })
            .fill()
            .theme_when(clone_reader(reader)),
    )
}

fn session_edit_dialog_signature(shared: &Shared) -> String {
    let chat = shared.chat();
    let Some(edit_id) = chat.session_edit_id.as_deref() else {
        return String::new();
    };
    let sessions = chat
        .sessions
        .iter()
        .map(|session| {
            format!(
                "{}:{:?}:{}:{}",
                session.id, session.kind, session.title, session.participant_count
            )
        })
        .collect::<Vec<_>>()
        .join("\u{1f}");
    let pending_rename = chat
        .pending_session_rename
        .as_ref()
        .filter(|pending| pending.session_id == edit_id)
        .map(|pending| format!("{}:{}", pending.request_id, pending.title))
        .unwrap_or_default();
    let rename_error = chat
        .session_rename_error
        .as_ref()
        .filter(|error| error.session_id == edit_id)
        .map(|error| error.detail.as_str())
        .unwrap_or_default();
    let pending_channel_config = chat
        .pending_channel_config
        .as_ref()
        .filter(|pending| pending.channel_id == edit_id)
        .map(|pending| format!("{}:{}", pending.request_id, pending.action))
        .unwrap_or_default();
    let channel_config_error = chat
        .channel_config_error
        .as_ref()
        .filter(|error| error.channel_id == edit_id)
        .map(|error| error.detail.as_str())
        .unwrap_or_default();
    format!(
        "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
        edit_id,
        chat.session_delete_confirm_id
            .as_deref()
            .unwrap_or_default(),
        chat.session_edit_details_open,
        chat.channel_agent_idx,
        chat.agents.len(),
        chat.channel_create_rx.is_some(),
        chat.notice.as_deref().unwrap_or_default(),
        pending_rename,
        rename_error,
        pending_channel_config,
        channel_config_error,
        sessions
    )
}

fn compact_chat_drawer(shared: &Shared, reader: &ThemeReader) -> Option<WidgetPod> {
    if !shared.chat().compact_drawer_open || shared.chat().session_edit_id.is_some() {
        return None;
    }
    let scroll_state = ScrollState::new();
    let body = sidebar(shared, reader, &scroll_state);
    let dismiss_shared = shared.clone();
    let close_shared = shared.clone();
    Some(WidgetPod::new(
        SideSheet::new("Conversations and agents", body)
            .placement(SideSheetPlacement::Left)
            .width(336.0)
            .header_action(
                IconButton::new(IconGlyph::Close, "Close conversation drawer")
                    .description("Return to the active conversation")
                    .appearance(sui::ButtonAppearance::Ghost)
                    .tone(SemanticTone::Neutral)
                    .theme_when(clone_reader(reader))
                    .on_press(move || {
                        close_shared.chat_mut().set_compact_drawer_open(false);
                    }),
            )
            .theme_when(clone_reader(reader))
            .on_dismiss(move || {
                dismiss_shared.chat_mut().set_compact_drawer_open(false);
            }),
    ))
}

fn chat_overlay_signature(shared: &Shared) -> String {
    let edit = session_edit_dialog_signature(shared);
    if !edit.is_empty() {
        return format!("edit|{edit}");
    }
    if shared.chat().compact_drawer_open {
        return format!("drawer|{:?}", sidebar_signature(shared));
    }
    String::new()
}

fn chat_overlay(shared: &Shared, reader: &ThemeReader) -> Option<WidgetPod> {
    if shared.chat().session_edit_id.is_some() {
        session_edit_dialog(shared, reader)
    } else {
        compact_chat_drawer(shared, reader)
    }
}

struct ChatDialogHost {
    shared: Shared,
    reader: ThemeReader,
    body: WidgetPod,
    dialog: Option<WidgetPod>,
    dialog_signature: String,
}

impl ChatDialogHost {
    fn new<W>(body: W, shared: Shared, reader: ThemeReader) -> Self
    where
        W: Widget + 'static,
    {
        let mut host = Self {
            shared,
            reader,
            body: WidgetPod::new(body),
            dialog: None,
            dialog_signature: String::new(),
        };
        host.sync_dialog();
        host
    }

    fn sync_dialog(&mut self) {
        let signature = chat_overlay_signature(&self.shared);
        if signature == self.dialog_signature {
            return;
        }
        self.dialog_signature = signature;
        self.dialog = chat_overlay(&self.shared, &self.reader);
    }
}

impl Widget for ChatDialogHost {
    fn event(&mut self, ctx: &mut EventCtx, _event: &Event) {
        let signature = chat_overlay_signature(&self.shared);
        if signature != self.dialog_signature {
            ctx.request_measure();
            ctx.request_paint();
            ctx.request_semantics();
        }
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        self.sync_dialog();
        let size = constraints.clamp(Size::new(
            if constraints.max.width.is_finite() {
                constraints.max.width
            } else {
                920.0
            },
            if constraints.max.height.is_finite() {
                constraints.max.height
            } else {
                640.0
            },
        ));
        let tight = Constraints::tight(size);
        self.body.measure(ctx, tight);
        if let Some(dialog) = &mut self.dialog {
            dialog.measure(ctx, tight);
        }
        size
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        self.body.arrange(ctx, bounds);
        if let Some(dialog) = &mut self.dialog {
            dialog.arrange(ctx, bounds);
        }
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.body.paint(ctx);
        if let Some(dialog) = &self.dialog {
            dialog.paint(ctx);
        }
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.body.semantics(ctx);
        if let Some(dialog) = &self.dialog {
            dialog.semantics(ctx);
        }
    }

    fn stack_host_options(&self) -> Option<StackHostOptions> {
        Some(StackHostOptions::default())
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.body);
        if let Some(dialog) = &self.dialog {
            visitor.visit(dialog);
        }
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.body);
        if let Some(dialog) = &mut self.dialog {
            visitor.visit(dialog);
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MediaCallPanelSignature {
    details_open: bool,
    active: bool,
    mic_enabled: bool,
    camera_enabled: bool,
    provider: String,
    model: String,
    microphones: Vec<String>,
    speakers: Vec<String>,
    cameras: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MediaSetupAccessSignature {
    microphone: MediaAccessState,
    speaker: MediaAccessState,
    camera: MediaAccessState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MediaSpeakerTestSignature {
    active: bool,
    access: MediaAccessState,
    state: SpeakerTestState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MediaCallTile {
    Remote,
    Local,
}

fn media_call_panel_signature(shared: &Shared) -> MediaCallPanelSignature {
    let chat = shared.chat();
    let (selected_provider, selected_model) = chat.current_media_provider_model();
    let call = &chat.media_call;
    let provider = if call.active && !call.provider.trim().is_empty() {
        call.provider.clone()
    } else {
        selected_provider
    };
    let model = if call.active && !call.model.trim().is_empty() {
        call.model.clone()
    } else {
        selected_model
    };
    MediaCallPanelSignature {
        details_open: chat.media_call_details_visible(),
        active: call.active,
        mic_enabled: call.mic_enabled,
        camera_enabled: call.camera_enabled,
        provider,
        model,
        microphones: chat.media_setup.microphone_labels(),
        speakers: chat.media_setup.speaker_labels(),
        cameras: chat.media_setup.camera_labels(),
    }
}

fn media_call_panel(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let key_shared = shared.clone();
    let build_shared = shared.clone();
    let build_reader = Rc::clone(reader);
    RebuildOnChange::new(
        move || media_call_panel_signature(&key_shared),
        move |signature| media_call_panel_body(&build_shared, &build_reader, signature),
    )
}

fn media_call_panel_body(
    shared: &Shared,
    reader: &ThemeReader,
    signature: &MediaCallPanelSignature,
) -> WidgetPod {
    if signature.details_open {
        media_call_panel_frame(
            shared,
            reader,
            media_call_expanded_body(shared, reader, signature),
        )
    } else {
        media_call_panel_frame(
            shared,
            reader,
            media_call_compact_body(shared, reader, signature),
        )
    }
}

fn media_call_panel_frame<W>(shared: &Shared, reader: &ThemeReader, body: W) -> WidgetPod
where
    W: Widget + 'static,
{
    let s = shared.clone();
    WidgetPod::new(
        SemanticRegion::new(
            "Media call",
            Surface::new(SurfaceRole::Panel, body)
                .border(SurfaceBorder::Bottom)
                .radius(0.0)
                .fill()
                .theme_when(clone_reader(reader)),
        )
        .description_when(move || media_call_accessibility_summary(&s)),
    )
}

fn media_call_compact_body(
    shared: &Shared,
    reader: &ThemeReader,
    signature: &MediaCallPanelSignature,
) -> impl Widget + use<> {
    let details = media_call_details_button(shared, reader, signature.details_open);
    // Keep the compact panel to one reflow-safe title row; provider/model detail remains in the
    // panel's accessibility summary.
    let row = TrailingSlotRow::new(media_call_title_line(shared, reader), details)
        .trailing_width(100.0)
        .trailing_height(30.0)
        .gap(10.0);
    WidgetPadding::symmetric(PAD, 5.0, row).fill_child()
}

fn media_call_expanded_body(
    shared: &Shared,
    reader: &ThemeReader,
    signature: &MediaCallPanelSignature,
) -> impl Widget + use<> {
    let header = TrailingSlotRow::new(
        media_call_title_line(shared, reader),
        media_call_details_button(shared, reader, signature.details_open),
    )
    .trailing_width(78.0)
    .trailing_height(30.0)
    .gap(10.0);

    let tiles = Flex::horizontal()
        .gap(8.0)
        .align_items(Alignment::Stretch)
        .with_item(
            media_call_video_tile(shared, reader, signature, MediaCallTile::Remote),
            FlexItem::flex(1.0).min_width(84.0).min_height(145.0),
        )
        .with_item(
            media_call_video_tile(shared, reader, signature, MediaCallTile::Local),
            FlexItem::fixed(168.0).min_height(145.0),
        );

    let body = Stack::vertical()
        .spacing(3.0)
        .alignment(Alignment::Stretch)
        .with_child(SizedBox::new().height(30.0).with_child(header))
        .with_child(SizedBox::new().height(145.0).with_child(tiles))
        .with_child(
            SizedBox::new()
                .height(30.0)
                .with_child(media_setup_access_row(shared, reader)),
        )
        .with_child(
            SizedBox::new()
                .height(34.0)
                .with_child(media_setup_device_selectors(shared, reader, signature)),
        )
        .with_child(
            SizedBox::new()
                .height(30.0)
                .with_child(media_setup_test_row(shared, reader)),
        )
        .with_child(
            SizedBox::new()
                .height(24.0)
                .with_child(media_setup_recovery_row(shared, reader)),
        );
    let body = Dock::new(body).bottom(34.0, media_call_controls_row(shared, reader, signature));

    WidgetPadding::symmetric(PAD, 7.0, body).fill_child()
}

fn media_call_title_line(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let icon_shared = shared.clone();
    Flex::horizontal()
        .gap(7.0)
        .align_items(Alignment::Center)
        .with_child(
            Icon::new(IconGlyph::AudioLines)
                .size(15.0)
                .color_when(move || {
                    if icon_shared.chat().media_call.active {
                        palette(&icon_shared).accent
                    } else {
                        palette(&icon_shared).text_muted
                    }
                }),
        )
        .with_child(small_label(shared, "Media call").color_when({
            let s = shared.clone();
            move || palette(&s).text
        }))
        .with_child(media_call_status_badge(shared, reader))
}

fn media_call_status_badge(shared: &Shared, reader: &ThemeReader) -> StatusBadge {
    let label_shared = shared.clone();
    let tone_shared = shared.clone();
    StatusBadge::dynamic(media_call_status_label(&shared.chat()), move || {
        media_call_status_label(&label_shared.chat()).to_string()
    })
    .icon(IconGlyph::AudioLines)
    .tone_when(move || media_call_status_tone(&tone_shared.chat()))
    .min_width(54.0)
    .theme_when(clone_reader(reader))
}

fn media_call_status_label(chat: &ChatState) -> &'static str {
    if chat.media_call.active {
        "LIVE"
    } else {
        "READY"
    }
}

fn media_call_status_tone(chat: &ChatState) -> SemanticTone {
    if chat.media_call.active {
        SemanticTone::Success
    } else if chat.media_call.notice.is_some() {
        SemanticTone::Warning
    } else {
        SemanticTone::Neutral
    }
}

fn media_call_video_tile(
    shared: &Shared,
    reader: &ThemeReader,
    signature: &MediaCallPanelSignature,
    tile: MediaCallTile,
) -> impl Widget + use<> {
    let (icon, title, _subtitle, status, tone, signal_active, signal_name, signal_description) =
        match tile {
            MediaCallTile::Remote => (
                if signature.active {
                    IconGlyph::Video
                } else {
                    IconGlyph::VideoOff
                },
                compact_line(&signature.model, 36),
                compact_line(&signature.provider, 32),
                if signature.active {
                    "realtime response audio"
                } else {
                    "standby"
                },
                if signature.active {
                    SemanticTone::Success
                } else {
                    SemanticTone::Neutral
                },
                signature.active,
                "Remote media signal",
                if signature.active {
                    "Remote realtime response audio is active"
                } else {
                    "Remote realtime media is standing by"
                },
            ),
            MediaCallTile::Local => (
                if signature.camera_enabled {
                    IconGlyph::Camera
                } else {
                    IconGlyph::CameraOff
                },
                if signature.camera_enabled {
                    "Local".to_string()
                } else {
                    "Camera".to_string()
                },
                if signature.camera_enabled {
                    "preview".to_string()
                } else {
                    "paused".to_string()
                },
                if signature.mic_enabled {
                    "mic on"
                } else {
                    "mic muted"
                },
                if signature.camera_enabled && signature.mic_enabled {
                    SemanticTone::Success
                } else {
                    SemanticTone::Warning
                },
                signature.camera_enabled || signature.mic_enabled,
                "Local capture signal",
                if signature.camera_enabled && signature.mic_enabled {
                    "Local microphone and camera are enabled"
                } else if signature.mic_enabled {
                    "Local microphone is enabled and camera is paused"
                } else if signature.camera_enabled {
                    "Local camera is enabled and microphone is muted"
                } else {
                    "Local microphone and camera are paused"
                },
            ),
        };
    let signal = SwitchView::new()
        .selected(match tile {
            MediaCallTile::Remote => 0,
            MediaCallTile::Local => 1,
        })
        .with_child(
            SignalMeter::new(signal_name)
                .description(signal_description)
                .active(signal_active)
                .tone(tone)
                .bars(12)
                .size(Size::new(1024.0, 20.0))
                .theme_when(clone_reader(reader)),
        )
        .with_child(
            SizedBox::new()
                .height(82.0)
                .with_child(MediaCameraPreviewWidget::new(shared.clone())),
        );
    let icon_shared = shared.clone();
    let title_shared = shared.clone();
    let status = StatusBadge::new(status)
        .tone(tone)
        .min_width(70.0)
        .theme_when(clone_reader(reader));
    let content = Flex::vertical()
        .gap(2.0)
        .with_child(
            Flex::horizontal()
                .gap(6.0)
                .align_items(Alignment::Center)
                .with_child(Icon::new(icon).size(15.0).color_when(move || {
                    if tone == SemanticTone::Success {
                        palette(&icon_shared).accent
                    } else {
                        palette(&icon_shared).text_faint
                    }
                }))
                .with_child(
                    small_label(shared, title).color_when(move || palette(&title_shared).text),
                ),
        )
        .with_child(signal)
        .spacer()
        .with_child(status);

    Surface::field(WidgetPadding::symmetric(8.0, 6.0, content).fill_child())
        .fill()
        .theme_when(clone_reader(reader))
}

#[derive(Default)]
struct MediaPreviewRegistration {
    handle: AtomicU64,
    revision: AtomicU64,
}

impl MediaPreviewRegistration {
    fn handle(&self) -> Option<ImageHandle> {
        let handle = self.handle.load(Ordering::Acquire);
        (handle != 0).then(|| ImageHandle::new(handle))
    }

    fn revision(&self) -> Option<u64> {
        let revision = self.revision.load(Ordering::Acquire);
        (revision != 0).then_some(revision)
    }

    fn image_handle(&self, ctx: &PaintCtx) -> ImageHandle {
        if let Some(handle) = self.handle() {
            return handle;
        }
        let candidate = ctx.widget_image_handle(0);
        match self
            .handle
            .compare_exchange(0, candidate.get(), Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => candidate,
            Err(handle) => ImageHandle::new(handle),
        }
    }
}

struct MediaCameraPreviewWidget {
    shared: Shared,
    registration: Arc<MediaPreviewRegistration>,
}

impl MediaCameraPreviewWidget {
    fn new(shared: Shared) -> Self {
        let registration = shared.chat().media_preview_registration.clone();
        Self {
            shared,
            registration,
        }
    }

    fn preview(&self) -> Option<Arc<sinomo_video::NativeVideoSurfaceFrame>> {
        let chat = self.shared.chat();
        if !chat.media_call.camera_enabled {
            return None;
        }
        let preview = chat.media_setup.camera_surface.latest()?;
        let expected_len = (preview.width as usize)
            .checked_mul(preview.height as usize)?
            .checked_mul(4)?;
        (preview.width > 0 && preview.height > 0 && preview.rgba.len() == expected_len)
            .then_some(preview)
    }
}

impl Widget for MediaCameraPreviewWidget {
    fn measure(&mut self, _ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        constraints.clamp(Size::new(132.0, 28.0))
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let bounds = ctx.bounds();
        let pal = palette(&self.shared);
        paint_rounded_rect(ctx, bounds, pal.field, 5.0);
        let Some(preview) = self.preview() else {
            draw_glyph(
                ctx,
                IconGlyph::CameraOff,
                centered_rect(bounds, Size::new(16.0, 16.0)),
                pal.text_faint,
            );
            return;
        };
        let handle = self.registration.image_handle(ctx);
        if self.registration.revision() != Some(preview.revision) || !ctx.image_registered(handle) {
            if let Ok(image) = RegisteredImage::from_rgba8(
                preview.width,
                preview.height,
                preview.rgba.as_ref().to_vec(),
            ) {
                ctx.register_image(handle, image);
                self.registration
                    .revision
                    .store(preview.revision, Ordering::Release);
            } else {
                draw_glyph(
                    ctx,
                    IconGlyph::CameraOff,
                    centered_rect(bounds, Size::new(16.0, 16.0)),
                    pal.text_faint,
                );
                return;
            }
        }
        if !ctx.image_registered(handle) {
            draw_glyph(
                ctx,
                IconGlyph::CameraOff,
                centered_rect(bounds, Size::new(16.0, 16.0)),
                pal.text_faint,
            );
            return;
        }
        let source = Size::new(preview.width as f32, preview.height as f32);
        let image_rect = media_preview_cover_rect(bounds, source);
        ctx.push_clip(Path::rounded_rect(bounds, 5.0));
        ctx.draw_image_source(image_rect, ImageSource::new(handle));
        ctx.pop_clip();
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let mut node = SemanticsNode::new(ctx.widget_id(), SemanticsRole::Image, ctx.bounds());
        node.name = Some("Camera preview".to_string());
        node.description = Some(if self.preview().is_some() {
            "Live preview from the selected local camera".to_string()
        } else {
            "Camera preview is unavailable or paused".to_string()
        });
        ctx.push(node);
    }
}

fn media_preview_cover_rect(bounds: Rect, source: Size) -> Rect {
    if source.width <= 0.0 || source.height <= 0.0 {
        return bounds;
    }
    let scale = (bounds.width() / source.width)
        .max(bounds.height() / source.height)
        .max(0.0);
    let size = Size::new(source.width * scale, source.height * scale);
    centered_rect(bounds, size)
}

fn centered_rect(bounds: Rect, size: Size) -> Rect {
    Rect::from_origin_size(
        Point::new(
            bounds.x() + (bounds.width() - size.width) * 0.5,
            bounds.y() + (bounds.height() - size.height) * 0.5,
        ),
        size,
    )
}

fn media_setup_access_row(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let key_shared = shared.clone();
    let build_shared = shared.clone();
    let build_reader = Rc::clone(reader);
    RebuildOnChange::new(
        move || {
            let chat = key_shared.chat();
            MediaSetupAccessSignature {
                microphone: chat.media_setup.microphone_access.clone(),
                speaker: chat.media_setup.speaker_access.clone(),
                camera: chat.media_setup.camera_access.clone(),
            }
        },
        move |signature| {
            WidgetPod::new(media_setup_access_row_body(
                &build_shared,
                &build_reader,
                signature,
            ))
        },
    )
}

fn media_setup_access_row_body(
    shared: &Shared,
    reader: &ThemeReader,
    signature: &MediaSetupAccessSignature,
) -> impl Widget + use<> {
    let retry_shared = shared.clone();
    let retry = IconButton::new(IconGlyph::History, "Retry media device access")
        .description("Check devices again after allowing microphone or camera access")
        .size(26.0)
        .icon_size(14.0)
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .enabled_when({
            let enabled_shared = shared.clone();
            move || {
                let chat = enabled_shared.chat();
                !chat.media_call.active && !chat.media_setup.inventory_loading
            }
        })
        .on_press(move || retry_shared.chat_mut().media_setup.retry_access());
    Flex::horizontal()
        .gap(4.0)
        .align_items(Alignment::Center)
        .with_item(
            media_setup_access_badge("Mic", &signature.microphone, reader),
            FlexItem::fixed(68.0).min_height(24.0),
        )
        .with_item(
            media_setup_access_badge("Out", &signature.speaker, reader),
            FlexItem::fixed(68.0).min_height(24.0),
        )
        .with_item(
            media_setup_access_badge("Cam", &signature.camera, reader),
            FlexItem::fixed(68.0).min_height(24.0),
        )
        .spacer()
        .with_item(retry, FlexItem::fixed(26.0).min_height(26.0))
}

fn media_setup_access_badge(
    prefix: &str,
    state: &MediaAccessState,
    reader: &ThemeReader,
) -> StatusBadge {
    StatusBadge::new(format!(
        "{prefix} {}",
        media_setup_access_compact_label(state)
    ))
    .tone(media_setup_access_tone(state))
    .min_width(68.0)
    .theme_when(clone_reader(reader))
}

fn media_setup_access_compact_label(state: &MediaAccessState) -> &'static str {
    match state {
        MediaAccessState::Idle => "Idle",
        MediaAccessState::Checking => "Check",
        MediaAccessState::Ready => "Ready",
        MediaAccessState::PermissionDenied(_) => "Allow",
        MediaAccessState::Busy(_) => "Busy",
        MediaAccessState::Unavailable(_) => "Error",
        MediaAccessState::Unsupported(_) => "N/A",
    }
}

fn media_setup_access_tone(state: &MediaAccessState) -> SemanticTone {
    match state {
        MediaAccessState::Ready => SemanticTone::Success,
        MediaAccessState::Checking => SemanticTone::Accent,
        MediaAccessState::PermissionDenied(_)
        | MediaAccessState::Busy(_)
        | MediaAccessState::Unavailable(_) => SemanticTone::Warning,
        MediaAccessState::Idle | MediaAccessState::Unsupported(_) => SemanticTone::Neutral,
    }
}

fn media_setup_device_selectors(
    shared: &Shared,
    reader: &ThemeReader,
    signature: &MediaCallPanelSignature,
) -> impl Widget + use<> {
    let microphone = {
        let selected_shared = shared.clone();
        let change_shared = shared.clone();
        media_setup_device_selector(
            "Microphone",
            "Microphone device",
            signature.microphones.clone(),
            move || Some(selected_shared.chat().media_setup.microphone_index),
            move |index, _| {
                change_shared
                    .chat_mut()
                    .media_setup
                    .select_microphone(index)
            },
            reader,
        )
    };
    let speaker = {
        let selected_shared = shared.clone();
        let change_shared = shared.clone();
        media_setup_device_selector(
            "Speaker",
            "Speaker device",
            signature.speakers.clone(),
            move || Some(selected_shared.chat().media_setup.speaker_index),
            move |index, _| change_shared.chat_mut().media_setup.select_speaker(index),
            reader,
        )
    };
    let camera = {
        let selected_shared = shared.clone();
        let change_shared = shared.clone();
        media_setup_device_selector(
            "Camera",
            "Camera device",
            signature.cameras.clone(),
            move || Some(selected_shared.chat().media_setup.camera_index),
            move |index, _| change_shared.chat_mut().media_setup.select_camera(index),
            reader,
        )
    };
    Flex::horizontal()
        .gap(8.0)
        .align_items(Alignment::Stretch)
        .with_item(microphone, FlexItem::flex(1.0).min_width(84.0))
        .with_item(speaker, FlexItem::flex(1.0).min_width(84.0))
        .with_item(camera, FlexItem::flex(1.0).min_width(84.0))
}

fn media_setup_device_selector<S, C>(
    label: &'static str,
    name: &'static str,
    options: Vec<String>,
    selected_when: S,
    on_change: C,
    reader: &ThemeReader,
) -> impl Widget + use<S, C>
where
    S: Fn() -> Option<usize> + 'static,
    C: FnMut(usize, String) + 'static,
{
    let label_reader = Rc::clone(reader);
    let label_style = small_theme_text_style(&reader());
    Stack::vertical()
        .spacing(2.0)
        .alignment(Alignment::Stretch)
        .with_child(
            Label::new(label)
                .style(label_style)
                .color_when(move || label_reader().palette.text_muted),
        )
        .with_child(
            SizedBox::new().height(28.0).with_child(
                sui::Select::new(name)
                    .options(options)
                    .selected_when(selected_when)
                    .theme_when(clone_reader(reader))
                    .on_change(on_change),
            ),
        )
}

fn media_setup_test_row(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let meter_shared = shared.clone();
    let active_shared = shared.clone();
    Flex::horizontal()
        .gap(8.0)
        .align_items(Alignment::Center)
        .with_item(
            SignalMeter::new("Microphone input level")
                .description("Live microphone input level")
                .active_when(move || {
                    let chat = active_shared.chat();
                    if chat.media_call.active {
                        chat.media_call.mic_enabled
                    } else {
                        matches!(chat.media_setup.microphone_access, MediaAccessState::Ready)
                    }
                })
                .level_when(move || {
                    let chat = meter_shared.chat();
                    if chat.media_call.active {
                        chat.media_call.microphone_level
                    } else {
                        chat.media_setup.microphone_level
                    }
                })
                .bars(18)
                .size(Size::new(1024.0, 14.0))
                .theme_when(clone_reader(reader)),
            FlexItem::flex(1.0).min_width(120.0).min_height(20.0),
        )
        .with_item(
            media_setup_speaker_test_button(shared, reader),
            FlexItem::fixed(118.0).min_height(26.0),
        )
}

fn media_setup_speaker_test_button(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let key_shared = shared.clone();
    let build_shared = shared.clone();
    let build_reader = Rc::clone(reader);
    RebuildOnChange::new(
        move || {
            let chat = key_shared.chat();
            MediaSpeakerTestSignature {
                active: chat.media_call.active,
                access: chat.media_setup.speaker_access.clone(),
                state: chat.media_setup.speaker_test.clone(),
            }
        },
        move |signature| {
            let press_shared = build_shared.clone();
            let detail = match &signature.state {
                SpeakerTestState::Failed(detail) => {
                    format!("Speaker test failed: {detail}. Activate to try again.")
                }
                SpeakerTestState::Passed => {
                    "The selected speaker played the test tone.".to_string()
                }
                SpeakerTestState::Testing => {
                    "The test tone is playing through the selected speaker.".to_string()
                }
                SpeakerTestState::Idle => {
                    "Play a short tone through the selected speaker.".to_string()
                }
            };
            WidgetPod::new(
                sui::Button::new(signature.state.label())
                    .semantic_name("Test selected speaker")
                    .description(detail)
                    .icon(IconGlyph::AudioLines)
                    .min_height(26.0)
                    .theme_when(clone_reader(&build_reader))
                    .appearance(sui::ButtonAppearance::Tonal)
                    .tone(SemanticTone::Neutral)
                    .enabled(
                        !signature.active
                            && matches!(signature.access, MediaAccessState::Ready)
                            && !matches!(signature.state, SpeakerTestState::Testing),
                    )
                    .on_press(move || press_shared.chat_mut().media_setup.test_speaker()),
            )
        },
    )
}

fn media_setup_recovery_row(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let text_shared = shared.clone();
    let color_shared = shared.clone();
    let label = Label::dynamic("Saved on this node.", move || {
        media_setup_recovery_label(&text_shared.chat()).to_string()
    })
    .style(small_text_style(shared))
    .color_when(move || palette(&color_shared).text_faint);
    let settings_shared = shared.clone();
    let enabled_shared = shared.clone();
    let settings = sui::Button::new("Privacy")
        .semantic_name("Open media privacy settings")
        .description("Open operating system microphone and camera privacy settings")
        .icon(IconGlyph::Lock)
        .min_height(22.0)
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .enabled_when(move || {
            let chat = enabled_shared.chat();
            !chat.media_call.active && chat.media_setup.permission_settings_available()
        })
        .on_press(move || {
            settings_shared
                .chat_mut()
                .media_setup
                .open_permission_settings()
        });
    TrailingSlotRow::new(label, settings)
        .trailing_width(88.0)
        .trailing_height(22.0)
        .gap(8.0)
}

fn media_setup_recovery_label(chat: &ChatState) -> &'static str {
    if chat.media_call.active {
        return "Changes apply next call.";
    }
    let states = [
        &chat.media_setup.microphone_access,
        &chat.media_setup.speaker_access,
        &chat.media_setup.camera_access,
    ];
    if states
        .iter()
        .any(|state| matches!(state, MediaAccessState::PermissionDenied(_)))
    {
        "Allow access, then retry."
    } else if states
        .iter()
        .any(|state| matches!(state, MediaAccessState::Busy(_)))
    {
        "Device busy; close other apps."
    } else if states
        .iter()
        .any(|state| matches!(state, MediaAccessState::Unavailable(_)))
    {
        "Choose or reconnect a device."
    } else if states
        .iter()
        .any(|state| matches!(state, MediaAccessState::Unsupported(_)))
    {
        "Unsupported on this node."
    } else if chat.media_setup.recovery_detail().is_some() {
        "Review device status, then retry."
    } else {
        "Saved on this node."
    }
}

fn media_call_controls_row(
    shared: &Shared,
    reader: &ThemeReader,
    signature: &MediaCallPanelSignature,
) -> impl Widget + use<> {
    Flex::horizontal()
        .gap(8.0)
        .align_items(Alignment::Center)
        .with_item(
            media_call_mic_button(shared, reader, signature.mic_enabled),
            FlexItem::flex(1.0).min_width(0.0).min_height(28.0),
        )
        .with_item(
            media_call_toggle_button(shared, reader, signature.active),
            FlexItem::flex(1.0).min_width(0.0).min_height(28.0),
        )
        .with_item(
            media_call_camera_button(shared, reader, signature.camera_enabled),
            FlexItem::flex(1.0).min_width(0.0).min_height(28.0),
        )
}

fn media_call_details_button(
    shared: &Shared,
    reader: &ThemeReader,
    details_open: bool,
) -> sui::Button {
    let s = shared.clone();
    let (label, semantic_name, icon) = if details_open {
        ("Hide", "Hide media controls", IconGlyph::ChevronUp)
    } else {
        ("Controls", "Show media controls", IconGlyph::Video)
    };
    sui::Button::new(label)
        .semantic_name(semantic_name)
        .description("Toggle realtime media controls")
        .icon(icon)
        .icon_size(13.0)
        .min_width(0.0)
        .min_height(26.0)
        .padding(media_call_button_padding())
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .on_press_with_ctx(move |ctx| {
            s.chat_mut().toggle_media_call_details();
            request_chat_window_relayout(ctx);
        })
}

fn media_call_mic_button(shared: &Shared, reader: &ThemeReader, enabled: bool) -> sui::Button {
    let s = shared.clone();
    let (label, semantic_name, icon) = if enabled {
        ("Mic", "Mute microphone", IconGlyph::Mic)
    } else {
        ("Muted", "Enable microphone", IconGlyph::MicOff)
    };
    sui::Button::new(label)
        .semantic_name(semantic_name)
        .description("Toggle microphone input for the media call")
        .icon(icon)
        .icon_size(13.0)
        .min_width(0.0)
        .min_height(28.0)
        .padding(media_call_button_padding())
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .on_press_with_ctx(move |ctx| {
            s.chat_mut().toggle_media_call_mic();
            request_chat_window_relayout(ctx);
        })
}

fn media_call_toggle_button(shared: &Shared, reader: &ThemeReader, active: bool) -> sui::Button {
    let s = shared.clone();
    let enabled_shared = shared.clone();
    let (label, semantic_name, icon) = if active {
        ("End", "End media call", IconGlyph::PhoneOff)
    } else {
        ("Start", "Start media call", IconGlyph::Phone)
    };
    let button = sui::Button::new(label)
        .semantic_name(semantic_name)
        .description("Start or end the realtime media call")
        .icon(icon)
        .icon_size(13.0)
        .min_width(0.0)
        .min_height(28.0)
        .padding(media_call_button_padding());
    let button = if active {
        button
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Filled)
            .tone(SemanticTone::Danger)
    } else {
        button.theme_when(clone_reader(reader))
    };
    button
        .enabled_when(move || active || enabled_shared.chat().media_setup.access_check_complete())
        .on_press_with_ctx(move |ctx| {
            s.chat_mut().toggle_media_call();
            request_chat_window_relayout(ctx);
        })
}

fn media_call_camera_button(shared: &Shared, reader: &ThemeReader, enabled: bool) -> sui::Button {
    let s = shared.clone();
    let (label, semantic_name, icon) = if enabled {
        ("Cam", "Pause camera", IconGlyph::Camera)
    } else {
        ("Paused", "Enable camera", IconGlyph::CameraOff)
    };
    sui::Button::new(label)
        .semantic_name(semantic_name)
        .description("Toggle camera input for the media call")
        .icon(icon)
        .icon_size(13.0)
        .min_width(0.0)
        .min_height(28.0)
        .padding(media_call_button_padding())
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .on_press_with_ctx(move |ctx| {
            s.chat_mut().toggle_media_call_camera();
            request_chat_window_relayout(ctx);
        })
}

fn media_call_button_padding() -> Insets {
    Insets {
        left: 7.0,
        right: 7.0,
        top: 2.0,
        bottom: 2.0,
    }
}

fn media_call_accessibility_summary(shared: &Shared) -> String {
    let chat = shared.chat();
    let call = &chat.media_call;
    let (provider, model) = if call.active {
        (call.provider.clone(), call.model.clone())
    } else {
        chat.current_media_provider_model()
    };
    let mut parts = vec![format!(
        "{} media call using {provider} / {model}",
        if call.active { "Live" } else { "Ready" }
    )];
    parts.push(chat.media_call_status_summary());
    parts.push(format!(
        "input {} audio chunks and {} video frames, output {} audio chunks",
        call.input_audio_chunks, call.input_video_frames, call.output_audio_chunks
    ));
    if let Some(notice) = call.notice.as_deref() {
        parts.push(notice.to_string());
    }
    if let Some(recovery) = chat.media_setup.recovery_detail() {
        parts.push(recovery);
    }
    parts.join("; ")
}

#[cfg(test)]
fn format_token_count(tokens: u64) -> String {
    if tokens >= 1_000_000 {
        format!("{:.1}m", tokens as f32 / 1_000_000.0)
    } else if tokens >= 1_000 {
        format!("{:.1}k", tokens as f32 / 1_000.0)
    } else {
        tokens.to_string()
    }
}

fn context_lane_summary(label: &str, bytes: usize, total: usize) -> String {
    let share = if total == 0 {
        0.0
    } else {
        bytes as f32 * 100.0 / total as f32
    };
    format!(
        "{} · {} · {:.0}%",
        compact_line(label, 28),
        format_bytes(bytes),
        share
    )
}

fn activity_tone_semantic(tone: ActivityTone) -> SemanticTone {
    match tone {
        ActivityTone::Neutral => SemanticTone::Neutral,
        ActivityTone::Running => SemanticTone::Accent,
        ActivityTone::Good => SemanticTone::Success,
        ActivityTone::Warn => SemanticTone::Warning,
        ActivityTone::Bad => SemanticTone::Danger,
    }
}

fn activity_tone_label(tone: ActivityTone) -> &'static str {
    match tone {
        ActivityTone::Neutral => "Idle",
        ActivityTone::Running => "Running",
        ActivityTone::Good => "Ready",
        ActivityTone::Warn => "Review",
        ActivityTone::Bad => "Failed",
    }
}

fn activity_detail_entry(chat: &ChatState, health: &RuntimeHealth) -> Option<ChatActivityEntry> {
    chat.activity
        .iter()
        .rev()
        .find(|entry| matches!(entry.tone, ActivityTone::Warn | ActivityTone::Bad))
        .cloned()
        .or_else(|| {
            matches!(health.tone, ActivityTone::Warn | ActivityTone::Bad).then(|| {
                ChatActivityEntry {
                    title: health.label.to_string(),
                    detail: health.detail.clone(),
                    tone: health.tone,
                }
            })
        })
}

fn activity_popup_signature(shared: &Shared, detail_expanded: bool) -> String {
    let chat = shared.chat();
    let health = chat.runtime_health();
    let mut signature = format!(
        "expanded={detail_expanded}|health={}:{}:{:?}|work={}",
        health.label,
        health.detail,
        health.tone,
        activity_work_summary(&chat)
    );
    for task in chat.control.tasks.iter().take(8) {
        signature.push_str(&format!(
            "|task={}:{}:{}:{}",
            task.summary,
            task.status,
            task.progress_summary.as_deref().unwrap_or_default(),
            task.result_ref.as_deref().unwrap_or_default()
        ));
    }
    for call in chat.tool_calls.iter().rev().take(8) {
        signature.push_str(&format!(
            "|tool={}:{}:{}:{}",
            call.tool, call.status, call.step_index, call.input_preview
        ));
    }
    if let Some(context) = &chat.context {
        signature.push_str(&format!(
            "|context={}:{}:{}",
            context.total_bytes, context.included_count, context.omitted_count
        ));
        for lane in context.lanes.iter().filter(|lane| lane.bytes > 0).take(6) {
            signature.push_str(&format!(":{}:{}:{}", lane.kind, lane.label, lane.bytes));
        }
    }
    for entry in chat.activity.iter().rev().take(16) {
        signature.push_str(&format!(
            "|event={}:{}:{:?}",
            entry.title, entry.detail, entry.tone
        ));
    }
    signature
}

fn activity_close_button(shared: &Shared, reader: &ThemeReader) -> IconButton {
    let s = shared.clone();
    IconButton::new(IconGlyph::Close, "Close activity")
        .description("Hide the runtime activity panel")
        .size(26.0)
        .icon_size(13.0)
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .on_press_with_ctx(move |ctx| {
            s.chat_mut().close_activity_panel();
            request_chat_window_relayout(ctx);
        })
}

fn activity_popup_header(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    Flex::horizontal()
        .align_items(Alignment::Center)
        .with_child(dialog_section_label(shared, "Activity"))
        .spacer()
        .with_child(activity_close_button(shared, reader))
}

fn activity_detail_toggle_button(
    reader: &ThemeReader,
    detail_expanded: Rc<Cell<bool>>,
) -> impl Widget + use<> {
    let expanded = detail_expanded.get();
    let label = if expanded {
        "Hide detail"
    } else {
        "Show detail"
    };
    let expanded_state = Rc::clone(&detail_expanded);
    sui::Button::new(label)
        .icon(if expanded {
            IconGlyph::ChevronUp
        } else {
            IconGlyph::ChevronDown
        })
        .icon_size(13.0)
        .semantic_name(label)
        .description("Activity detail")
        .min_width(108.0)
        .min_height(24.0)
        .padding(Insets {
            left: 8.0,
            right: 8.0,
            top: 2.0,
            bottom: 2.0,
        })
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .on_press_with_ctx(move |ctx| {
            expanded_state.set(!expanded_state.get());
            request_chat_window_relayout(ctx);
        })
}

fn activity_status_card(
    shared: &Shared,
    reader: &ThemeReader,
    health: &RuntimeHealth,
    work_summary: String,
    has_detail: bool,
    detail_expanded: Rc<Cell<bool>>,
) -> impl Widget + use<> {
    let s = shared.clone();
    let status = activity_status_summary(&health.detail);
    let title = StatusBadge::new(health.label)
        .tone(activity_tone_semantic(health.tone))
        .icon(IconGlyph::Chat)
        .min_width(104.0)
        .theme_when(clone_reader(reader));
    let detail_slot = SwitchView::new()
        .selected(if has_detail { 1 } else { 0 })
        .with_child(SizedBox::new())
        .with_child(activity_detail_toggle_button(reader, detail_expanded));
    let header = TrailingSlotRow::new(title, detail_slot)
        .trailing_width(if has_detail { 108.0 } else { 0.0 })
        .trailing_height(24.0)
        .gap(8.0);
    Surface::panel(
        Stack::vertical()
            .spacing(6.0)
            .alignment(Alignment::Stretch)
            .with_child(header)
            .with_child(small_label(shared, compact_line(&status, 84)).color_when({
                let s = s.clone();
                move || palette(&s).text
            }))
            .with_child(muted_label(shared, compact_line(&work_summary, 84))),
    )
    .padding(Insets {
        left: 12.0,
        right: 12.0,
        top: 10.0,
        bottom: 10.0,
    })
    .fill_width()
    .theme_when(clone_reader(reader))
}

fn activity_detail_card(
    shared: &Shared,
    reader: &ThemeReader,
    entry: ChatActivityEntry,
) -> impl Widget + use<> {
    let s = shared.clone();
    Surface::field(
        Stack::vertical()
            .spacing(4.0)
            .alignment(Alignment::Stretch)
            .with_child(
                StatusBadge::new(compact_line(&entry.title, 40))
                    .tone(activity_tone_semantic(entry.tone))
                    .min_width(96.0)
                    .theme_when(clone_reader(reader)),
            )
            .with_child(
                small_label(shared, compact_line(&entry.detail, 120))
                    .color_when(move || palette(&s).text_muted),
            ),
    )
    .padding(Insets {
        left: 10.0,
        right: 10.0,
        top: 8.0,
        bottom: 8.0,
    })
    .fill_width()
    .theme_when(clone_reader(reader))
    .appearance(SurfaceAppearance::Raised)
    .tone(SemanticTone::Neutral)
}

fn activity_section_label(reader: &ThemeReader, label: &'static str) -> impl Widget + use<> {
    SectionLabel::new(label).theme_when(clone_reader(reader))
}

fn activity_item_card(
    shared: &Shared,
    reader: &ThemeReader,
    title: String,
    detail: String,
    tone: ActivityTone,
) -> impl Widget + use<> {
    let s = shared.clone();
    let title_label = small_label(shared, compact_line(&title, 46)).color_when({
        let s = s.clone();
        move || palette(&s).text
    });
    let detail_label =
        small_label(shared, compact_line(&detail, 64)).color_when(move || palette(&s).text_muted);
    let text = Stack::vertical()
        .spacing(1.0)
        .alignment(Alignment::Stretch)
        .with_child(title_label)
        .with_child(detail_label);
    Surface::field(
        TrailingSlotRow::new(
            text,
            StatusBadge::new(activity_tone_label(tone))
                .tone(activity_tone_semantic(tone))
                .min_width(74.0)
                .theme_when(clone_reader(reader)),
        )
        .trailing_width(78.0)
        .trailing_height(24.0)
        .gap(8.0),
    )
    .padding(Insets {
        left: 10.0,
        right: 8.0,
        top: 7.0,
        bottom: 7.0,
    })
    .fill_width()
    .theme_when(clone_reader(reader))
    .appearance(SurfaceAppearance::Raised)
    .tone(SemanticTone::Neutral)
}

fn activity_popup_content(
    shared: &Shared,
    reader: &ThemeReader,
    detail_expanded: Rc<Cell<bool>>,
) -> impl Widget + use<> {
    let chat = shared.chat();
    let health = chat.runtime_health();
    let detail_entry = activity_detail_entry(&chat, &health);
    let mut body = Stack::vertical()
        .spacing(8.0)
        .alignment(Alignment::Stretch)
        .with_child(activity_status_card(
            shared,
            reader,
            &health,
            activity_work_summary(&chat),
            detail_entry.is_some(),
            Rc::clone(&detail_expanded),
        ));

    if detail_expanded.get()
        && let Some(entry) = detail_entry
    {
        body = body.with_child(activity_detail_card(shared, reader, entry));
    }

    if chat.control.tasks.is_empty()
        && chat.tool_calls.is_empty()
        && chat.activity.is_empty()
        && chat.context.is_none()
    {
        return ScrollView::vertical(
            body.with_child(muted_label(shared, "No runtime activity yet")),
        )
        .viewport_width_hint(true);
    }

    if !chat.control.tasks.is_empty() {
        body = body.with_child(activity_section_label(reader, "Tasks"));
        for task in chat.control.tasks.iter().take(4) {
            let detail = task
                .progress_summary
                .as_deref()
                .or(task.result_ref.as_deref())
                .unwrap_or(task.kind.as_str());
            body = body.with_child(activity_item_card(
                shared,
                reader,
                task.summary.clone(),
                format!("{} · {}", task.status, compact_line(detail, 48)),
                task_tone(&task.status),
            ));
        }
    }

    if !chat.tool_calls.is_empty() {
        body = body.with_child(activity_section_label(reader, "Tools"));
        for call in chat.tool_calls.iter().rev().take(3) {
            let mut detail = format!("{} · step {}", call.status, call.step_index);
            if let Some(input) = (!call.input_preview.trim().is_empty())
                .then(|| compact_line(&call.input_preview, 30))
            {
                detail.push_str(" · ");
                detail.push_str(&input);
            }
            body = body.with_child(activity_item_card(
                shared,
                reader,
                call.tool.clone(),
                detail,
                tool_status_tone(&call.status),
            ));
        }
    }

    if let Some(context) = &chat.context {
        body = body.with_child(activity_section_label(reader, "Context"));
        let lane_total: usize = context.lanes.iter().map(|lane| lane.bytes).sum();
        let visible_lane_count = context.lanes.iter().filter(|lane| lane.bytes > 0).count();
        let mut drawn = 0usize;
        for lane in context.lanes.iter().filter(|lane| lane.bytes > 0).take(4) {
            let label = if lane.label.trim().is_empty() {
                lane.kind.as_str()
            } else {
                lane.label.as_str()
            };
            body = body.with_child(activity_item_card(
                shared,
                reader,
                context_lane_summary(label, lane.bytes, lane_total),
                "context lane".to_string(),
                ActivityTone::Neutral,
            ));
            drawn += 1;
        }
        if visible_lane_count > drawn {
            body = body.with_child(muted_label(
                shared,
                format!("{} more lane(s)", visible_lane_count - drawn),
            ));
        }
    }

    if !chat.activity.is_empty() {
        body = body.with_child(activity_section_label(reader, "Events"));
        for entry in chat.activity.iter().rev().take(10) {
            body = body.with_child(activity_item_card(
                shared,
                reader,
                entry.title.clone(),
                activity_visible_detail(&entry.detail),
                entry.tone,
            ));
        }
        if chat.activity.len() > 10 {
            body = body.with_child(muted_label(
                shared,
                format!("{} more event(s)", chat.activity.len() - 10),
            ));
        }
    }

    ScrollView::vertical(body).viewport_width_hint(true)
}

// ---------------------------------------------------------------------------
// Transcript — a live-reading painter that owns its own scroll (auto-stick to
// bottom while streaming). Re-reads `ChatState.messages` each `paint`, so a
// streamed token never rebuilds the widget tree. The repaint each streaming
// frame is driven by `ChatView`'s pump (which `request_paint`s as it drains
// tokens); a parent repaint repaints this whole subtree, so the transcript
// stays in sync without its own self-invalidation. The bubble layout/measure
// math is the same the hand-painted UI used.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TranscriptTextStyleKind {
    Body,
    Heading(u8),
    Muted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TranscriptLayoutMode {
    Row,
    Bubble,
}

impl TranscriptLayoutMode {
    fn for_width(width: f32) -> Self {
        if width < 720.0 {
            Self::Bubble
        } else {
            Self::Row
        }
    }

    fn message_gap(self) -> f32 {
        match self {
            Self::Row => 6.0,
            Self::Bubble => 12.0,
        }
    }
}

#[derive(Clone, Debug)]
enum TranscriptLaidBlock {
    Text {
        layout: TextLayout,
        display_range: std::ops::Range<usize>,
        height: f32,
    },
    Code {
        code: ChatCodeBlock,
        layout: TextLayout,
        display_range: std::ops::Range<usize>,
        height: f32,
    },
}

#[derive(Clone, Debug)]
struct TranscriptLaidMessage {
    is_user: bool,
    kind: ChatMessageKind,
    blocks: Vec<TranscriptLaidBlock>,
    bubble_w: f32,
    bubble_h: f32,
    action: Option<RecoveryCardAction>,
    source_map: RichTextSourceMap,
}

#[derive(Clone, Debug)]
enum TranscriptLaidTool {
    ChainCollapsed {
        call_count: usize,
        latest: String,
        bubble_w: f32,
        bubble_h: f32,
    },
    ChainHeader {
        call_count: usize,
        bubble_w: f32,
        bubble_h: f32,
    },
    Call {
        call: ChatToolActivity,
        input_block: Option<TranscriptLaidToolDetail>,
        output_block: Option<TranscriptLaidToolDetail>,
        bubble_w: f32,
        bubble_h: f32,
    },
}

#[derive(Clone, Debug)]
struct TranscriptLaidToolDetail {
    section: ToolDetailSection,
    title: String,
    collapsible: bool,
    code: Option<ChatCodeBlock>,
    text: Option<String>,
    height: f32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RuntimeTranscriptAction {
    ToggleToolChain,
    ToggleToolDetail(String),
}

/// Custom event posted by the transcript context menu to copy the full raw
/// text of the message under the right-click.
const CHAT_COPY_MESSAGE_EVENT: &str = "sinomo.chat.copy-message";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TranscriptTextPoint {
    message_key: u64,
    offset: usize,
}

/// One rich-text layout in the painted transcript. The retained SUI layout is
/// used for character hit-testing and selection geometry.
#[derive(Clone, Debug)]
struct TranscriptTextHit {
    message_key: u64,
    display_range: std::ops::Range<usize>,
    rect: Rect,
    origin: Point,
    layout: TextLayout,
}

#[derive(Clone, Debug)]
struct TranscriptMessageSource {
    message_key: u64,
    source_map: RichTextSourceMap,
}

/// The accessibility projection is intentionally limited to messages that
/// intersect the viewport. Long channel windows can retain hundreds of
/// messages for smooth scrolling, while UI Automation still receives useful
/// row text without building an unbounded semantic tree.
#[derive(Clone, Debug)]
struct TranscriptMessageSemantic {
    message_key: u64,
    bounds: Rect,
    speaker: String,
    text: String,
    position: usize,
    total: usize,
    busy: bool,
}

/// Selection key for a message: the backend sequence when present, otherwise a
/// high-bit key derived from the transcript position (local/streaming tail
/// messages), so refs stay distinct and survive appends.
fn transcript_message_selection_key(index: usize, message: &ChatMessage) -> u64 {
    message
        .sequence
        .unwrap_or((1 << 63) | (index as u64 & !(1 << 63)))
}

fn vertical_distance(rect: Rect, y: f32) -> f32 {
    if y < rect.y() {
        rect.y() - y
    } else if y >= rect.max_y() {
        y - rect.max_y()
    } else {
        0.0
    }
}

/// The transcript lays out the same rich text during measure (for positioning
/// real child controls) and paint (for its specialized canvas). Keep that
/// shaping path shared so stock buttons cannot drift from the bubbles and tool
/// details they belong to.
trait TranscriptLayoutContext {
    fn transcript_layout_document(&self, request: TextLayoutRequest) -> sui::Result<TextLayout>;

    fn transcript_measure_text_width(&self, text: &str, style: TextStyle) -> f32;
}

impl TranscriptLayoutContext for MeasureCtx {
    fn transcript_layout_document(&self, request: TextLayoutRequest) -> sui::Result<TextLayout> {
        self.layout().layout_document(request)
    }

    fn transcript_measure_text_width(&self, text: &str, style: TextStyle) -> f32 {
        self.layout()
            .measure_text(text.to_string(), style)
            .map(|measurement| measurement.width)
            .unwrap_or(0.0)
    }
}

impl TranscriptLayoutContext for PaintCtx {
    fn transcript_layout_document(&self, request: TextLayoutRequest) -> sui::Result<TextLayout> {
        self.layout_text_document(request)
    }

    fn transcript_measure_text_width(&self, text: &str, style: TextStyle) -> f32 {
        self.measure_text(text.to_string(), style)
            .map(|measurement| measurement.width)
            .unwrap_or(0.0)
    }
}

struct TranscriptView {
    shared: Shared,
    selection_scope: SelectionScope,
    controls: WidgetPod,
    content_height: Cell<f32>,
    viewport_height: Cell<f32>,
    bounds: Cell<Rect>,
    transcript_text_hits: RefCell<Vec<TranscriptTextHit>>,
    transcript_message_sources: RefCell<Vec<TranscriptMessageSource>>,
    transcript_message_semantics: RefCell<Vec<TranscriptMessageSemantic>>,
    selection: Cell<Option<(TranscriptTextPoint, TranscriptTextPoint)>>,
    dragging_selection: Cell<bool>,
    menu_target_key: Cell<Option<u64>>,
}

impl TranscriptView {
    fn new(shared: Shared, selection_scope: SelectionScope) -> Self {
        let controls = WidgetPod::new(TranscriptControls::new(shared.clone()));
        Self {
            shared,
            selection_scope,
            controls,
            content_height: Cell::new(0.0),
            viewport_height: Cell::new(0.0),
            bounds: Cell::new(Rect::ZERO),
            transcript_text_hits: RefCell::new(Vec::new()),
            transcript_message_sources: RefCell::new(Vec::new()),
            transcript_message_semantics: RefCell::new(Vec::new()),
            selection: Cell::new(None),
            dragging_selection: Cell::new(false),
            menu_target_key: Cell::new(None),
        }
    }

    /// Line under the pointer, requiring a hit inside (a slightly padded copy
    /// of) the painted line rect — used to start a selection drag.
    fn point_in_text_hit(hit: &TranscriptTextHit, position: Point) -> TranscriptTextPoint {
        let cursor = hit.layout.hit_test_point(Point::new(
            position.x - hit.origin.x,
            position.y - hit.origin.y,
        ));
        TranscriptTextPoint {
            message_key: hit.message_key,
            offset: hit.display_range.start + cursor.utf8_offset.min(hit.display_range.len()),
        }
    }

    fn text_point_at(&self, position: Point) -> Option<TranscriptTextPoint> {
        const SLACK: f32 = 4.0;
        self.transcript_text_hits
            .borrow()
            .iter()
            .find(|hit| {
                position.y >= hit.rect.y() - SLACK
                    && position.y < hit.rect.max_y() + SLACK
                    && position.x >= hit.rect.x() - SLACK
                    && position.x < hit.rect.max_x() + SLACK
            })
            .map(|hit| Self::point_in_text_hit(hit, position))
    }

    /// Line nearest the pointer's vertical position — used while extending a
    /// selection drag, so dragging past a bubble edge keeps selecting.
    fn nearest_text_point(&self, position: Point) -> Option<TranscriptTextPoint> {
        self.transcript_text_hits
            .borrow()
            .iter()
            .min_by(|a, b| {
                vertical_distance(a.rect, position.y)
                    .total_cmp(&vertical_distance(b.rect, position.y))
            })
            .map(|hit| Self::point_in_text_hit(hit, position))
    }

    /// Message selection key for the bubble line band under the pointer.
    fn message_key_at(&self, position: Point) -> Option<u64> {
        const SLACK: f32 = 12.0;
        self.transcript_text_hits
            .borrow()
            .iter()
            .find(|hit| position.y >= hit.rect.y() - SLACK && position.y < hit.rect.max_y() + SLACK)
            .map(|hit| hit.message_key)
    }

    fn ordered_selection(&self) -> Option<(TranscriptTextPoint, TranscriptTextPoint)> {
        let (anchor, focus) = self.selection.get()?;
        let sources = self.transcript_message_sources.borrow();
        let anchor_index = sources
            .iter()
            .position(|source| source.message_key == anchor.message_key)?;
        let focus_index = sources
            .iter()
            .position(|source| source.message_key == focus.message_key)?;
        if (anchor_index, anchor.offset) <= (focus_index, focus.offset) {
            Some((anchor, focus))
        } else {
            Some((focus, anchor))
        }
    }

    /// Selected transcript text: selected lines joined with newlines, with a
    /// blank line between messages.
    fn selection_text(&self) -> Option<String> {
        let (start, end) = self.ordered_selection()?;
        let sources = self.transcript_message_sources.borrow();
        let start_index = sources
            .iter()
            .position(|source| source.message_key == start.message_key)?;
        let end_index = sources
            .iter()
            .position(|source| source.message_key == end.message_key)?;
        let mut selected = Vec::new();
        for (index, source) in sources[start_index..=end_index].iter().enumerate() {
            let absolute_index = start_index + index;
            let rendered_len = source.source_map.rendered().len();
            let range_start = if absolute_index == start_index {
                start.offset.min(rendered_len)
            } else {
                0
            };
            let range_end = if absolute_index == end_index {
                end.offset.min(rendered_len)
            } else {
                rendered_len
            };
            let text = source.source_map.copy_range(range_start..range_end);
            if !text.is_empty() {
                selected.push(text);
            }
        }
        let text = selected.join("\n\n");
        (!text.trim().is_empty()).then_some(text)
    }

    fn set_selection(
        &self,
        ctx: &mut EventCtx,
        selection: Option<(TranscriptTextPoint, TranscriptTextPoint)>,
    ) {
        if self.selection.get() == selection {
            return;
        }
        self.selection.set(selection);
        self.publish_selection(ctx);
        ctx.request_paint();
    }

    /// Mirror the current selection into the shared [`SelectionScope`], so the
    /// composer and transcript replace each other's selection and the context
    /// menus can observe it for enabled state.
    fn publish_selection(&self, ctx: &mut EventCtx) {
        let owner = ctx.widget_id();
        match self.selection_text() {
            Some(text) => {
                let len = text.len();
                self.selection_scope
                    .replace_text(owner, owner, 0..len, len, text);
            }
            None => {
                self.selection_scope.clear_owner(owner);
            }
        }
    }

    fn clear_selection(&self, ctx: &mut EventCtx) {
        if self.selection.get().is_some() {
            self.set_selection(ctx, None);
        }
    }

    fn select_all_text(&self, ctx: &mut EventCtx) {
        let sources = self.transcript_message_sources.borrow();
        let selection = match (sources.first(), sources.last()) {
            (Some(first), Some(last)) => Some((
                TranscriptTextPoint {
                    message_key: first.message_key,
                    offset: 0,
                },
                TranscriptTextPoint {
                    message_key: last.message_key,
                    offset: last.source_map.rendered().len(),
                },
            )),
            _ => None,
        };
        drop(sources);
        self.set_selection(ctx, selection);
    }

    fn copy_selection_to_clipboard(&self, ctx: &mut EventCtx) -> bool {
        let Some(text) = self.selection_text() else {
            return false;
        };
        ctx.set_clipboard_text(text);
        true
    }

    fn copy_message_to_clipboard(&self, ctx: &mut EventCtx) -> bool {
        let Some(key) = self.menu_target_key.get() else {
            return false;
        };
        let text = {
            let chat = self.shared.chat();
            chat.messages
                .iter()
                .enumerate()
                .find(|(index, message)| transcript_message_selection_key(*index, message) == key)
                .map(|(_, message)| transcript_message_display_text(message))
        };
        let Some(text) = text else {
            return false;
        };
        if text.trim().is_empty() {
            return false;
        }
        ctx.set_clipboard_text(text);
        true
    }

    fn empty_state_center(area: Rect) -> (f32, f32) {
        let lift = if area.height() > 260.0 { 28.0 } else { 0.0 };
        (
            area.x() + area.width() / 2.0,
            area.y() + area.height() / 2.0 - lift,
        )
    }

    fn quick_action_width(action: ChatQuickAction) -> f32 {
        match action {
            ChatQuickAction::ManageChannel => 176.0,
            ChatQuickAction::NewChannel => 176.0,
        }
    }

    fn quick_action_height(area: Rect) -> f32 {
        if area.width() <= 480.0 || area.height() < 320.0 {
            44.0
        } else {
            58.0
        }
    }

    fn quick_actions(chat: &ChatState) -> &'static [ChatQuickAction] {
        quick_actions_for_chat(chat)
    }

    fn quick_action_rects(area: Rect, actions: &[ChatQuickAction]) -> Vec<(ChatQuickAction, Rect)> {
        if area.height() < 180.0 || area.width() < 260.0 {
            return Vec::new();
        }
        let (_, cy) = Self::empty_state_center(area);
        let gap = 10.0;
        let button_h = Self::quick_action_height(area);
        let widths = actions
            .iter()
            .map(|action| Self::quick_action_width(*action))
            .collect::<Vec<_>>();
        let row_w = widths.iter().sum::<f32>() + gap * actions.len().saturating_sub(1) as f32;
        let max_w = (area.width() - PAD * 4.0).max(0.0).min(560.0);

        if row_w <= max_w {
            let mut x = area.x() + (area.width() - row_w) * 0.5;
            let lower = area.y() + PAD;
            let upper = (area.max_y() - PAD - button_h).max(lower);
            let y = (cy + EMPTY_ACTION_OFFSET_Y).clamp(lower, upper);
            return actions
                .into_iter()
                .zip(widths)
                .map(|(action, width)| {
                    let rect = Rect::new(x, y, width, button_h);
                    x += width + gap;
                    (*action, rect)
                })
                .collect();
        }

        if max_w >= 420.0 {
            let cols = if actions.len() >= 5 && max_w >= 540.0 {
                3_usize
            } else {
                2_usize
            };
            let width =
                ((max_w - gap * cols.saturating_sub(1) as f32) / cols as f32).clamp(148.0, 260.0);
            let rows = actions.len().div_ceil(cols);
            let grid_w = cols as f32 * width + gap * cols.saturating_sub(1) as f32;
            let grid_h = rows as f32 * button_h + gap * rows.saturating_sub(1) as f32;
            let x0 = area.x() + (area.width() - grid_w) * 0.5;
            let lower = area.y() + PAD;
            let upper = (area.max_y() - PAD - grid_h).max(lower);
            let y0 = (cy + EMPTY_ACTION_OFFSET_Y).clamp(lower, upper);
            return actions
                .iter()
                .enumerate()
                .map(|(index, action)| {
                    let row = index / cols;
                    let col = index % cols;
                    (
                        *action,
                        Rect::new(
                            x0 + col as f32 * (width + gap),
                            y0 + row as f32 * (button_h + gap),
                            width,
                            button_h,
                        ),
                    )
                })
                .collect();
        }

        let width = max_w.clamp(206.0, 280.0);
        let stack_h =
            button_h * actions.len() as f32 + gap * actions.len().saturating_sub(1) as f32;
        let x = area.x() + (area.width() - width) * 0.5;
        let lower = area.y() + PAD;
        let upper = (area.max_y() - PAD - stack_h).max(lower);
        let y0 = (cy + EMPTY_ACTION_OFFSET_Y).clamp(lower, upper);
        actions
            .iter()
            .enumerate()
            .map(|(index, action)| {
                (
                    *action,
                    Rect::new(x, y0 + index as f32 * (button_h + gap), width, button_h),
                )
            })
            .collect()
    }

    fn timeline_header_height(chat: &ChatState) -> f32 {
        if chat
            .timeline
            .visible_summary(chat.messages.len())
            .is_some_and(|summary| !summary.trim().is_empty())
        {
            34.0
        } else {
            0.0
        }
    }

    fn paint_timeline_header(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        area: Rect,
        y: f32,
        summary: &str,
    ) {
        let rect = Rect::new(area.x() + PAD, y, area.width() - PAD * 2.0, 26.0);
        paint_callout(
            ctx,
            theme,
            rect,
            None,
            None,
            &compact_line(summary, 120),
            CalloutPaint::new(SemanticTone::Neutral)
                .fill(pal.field)
                .border(pal.border)
                .rail_width(0.0)
                .radius(7.0)
                .padding(Insets {
                    left: 10.0,
                    top: 4.0,
                    right: 10.0,
                    bottom: 4.0,
                })
                .body_color(pal.text_faint),
        );
    }

    fn recovery_card_rect(area: Rect) -> Rect {
        let cx = area.x() + area.width() / 2.0;
        let cy = area.y() + area.height() / 2.0;
        let card_w = (area.width() - 56.0).max(220.0).min(560.0);
        let card_h = 188.0_f32.min((area.height() - 48.0).max(156.0));
        Rect::new(cx - card_w / 2.0, cy - card_h / 2.0, card_w, card_h)
    }

    fn recovery_card_text_rect(card: Rect) -> Rect {
        Rect::new(
            card.x() + 60.0,
            card.y() + 17.0,
            (card.width() - 82.0).max(120.0),
            (card.height() - 34.0).max(0.0),
        )
    }

    fn recovery_card_actions(
        chat: &ChatState,
        target: RuntimeRecoveryTarget,
    ) -> Vec<RecoveryCardAction> {
        match target {
            RuntimeRecoveryTarget::Providers => vec![RecoveryCardAction::OpenProviderSetup],
            RuntimeRecoveryTarget::Connection => {
                let mut actions = Vec::new();
                if chat.should_offer_chat_list_retry() {
                    actions.push(RecoveryCardAction::RetryChatList);
                }
                if chat.should_offer_sifs_check() {
                    actions.push(RecoveryCardAction::CheckSifs);
                }
                actions.push(RecoveryCardAction::OpenConnectionSettings);
                actions
            }
        }
    }

    fn recovery_action_is_primary(
        chat: &ChatState,
        target: RuntimeRecoveryTarget,
        action: RecoveryCardAction,
    ) -> bool {
        let actions = Self::recovery_card_actions(chat, target);
        match action {
            RecoveryCardAction::RetryChatList | RecoveryCardAction::OpenProviderSetup => true,
            RecoveryCardAction::CheckSifs => !actions.contains(&RecoveryCardAction::RetryChatList),
            RecoveryCardAction::OpenConnectionSettings => {
                !actions.contains(&RecoveryCardAction::RetryChatList)
                    && !actions.contains(&RecoveryCardAction::CheckSifs)
            }
        }
    }

    fn recovery_card_action_width(action: RecoveryCardAction) -> f32 {
        match action {
            RecoveryCardAction::RetryChatList => 112.0,
            RecoveryCardAction::CheckSifs => 116.0,
            RecoveryCardAction::OpenConnectionSettings => 164.0,
            RecoveryCardAction::OpenProviderSetup => 142.0,
        }
    }

    fn recovery_action_rects(
        area: Rect,
        target: RuntimeRecoveryTarget,
        chat: &ChatState,
    ) -> Vec<(RecoveryCardAction, Rect)> {
        let card = Self::recovery_card_rect(area);
        let text = Self::recovery_card_text_rect(card);
        let actions = Self::recovery_card_actions(chat, target);
        let button_h = 28.0;
        let gap = 8.0;

        if actions.len() > 1 {
            let widths: Vec<f32> = actions
                .iter()
                .map(|action| Self::recovery_card_action_width(*action))
                .collect();
            let row_w = widths.iter().sum::<f32>() + actions.len().saturating_sub(1) as f32 * gap;
            if row_w <= text.width() {
                let y = card.y() + card.height() - button_h - 14.0;
                let mut x = text.x();
                return actions
                    .iter()
                    .zip(widths)
                    .map(|(action, width)| {
                        let rect = Rect::new(x, y, width, button_h);
                        x += width + gap;
                        (*action, rect)
                    })
                    .collect();
            }
        }

        let stack_h =
            actions.len() as f32 * button_h + actions.len().saturating_sub(1) as f32 * gap;
        let y0 = card.y() + card.height() - stack_h - 12.0;
        let button_w = actions
            .iter()
            .map(|action| Self::recovery_card_action_width(*action))
            .fold(0.0_f32, f32::max)
            .min(text.width())
            .max(text.width().min(112.0));
        actions
            .iter()
            .enumerate()
            .map(|(index, action)| {
                (
                    *action,
                    Rect::new(
                        text.x(),
                        y0 + index as f32 * (button_h + gap),
                        button_w,
                        button_h,
                    ),
                )
            })
            .collect()
    }

    fn timeline_header_widget_id(parent: sui::WidgetId) -> sui::WidgetId {
        sui::WidgetId::new(parent.get().wrapping_mul(53).wrapping_add(10_301))
    }

    fn empty_text_widget_id(parent: sui::WidgetId, slot: usize) -> sui::WidgetId {
        sui::WidgetId::new(
            parent
                .get()
                .wrapping_mul(43)
                .wrapping_add(9_501 + slot as u64),
        )
    }

    fn message_widget_id(parent: sui::WidgetId, message_key: u64) -> sui::WidgetId {
        // Sequence-backed keys are stable across appends and scrolling. Mix the
        // high/low halves so generated local keys remain distinct too.
        let mixed = message_key ^ message_key.rotate_left(29);
        sui::WidgetId::new(
            parent
                .get()
                .wrapping_mul(59)
                .wrapping_add(11_003)
                .wrapping_add(mixed),
        )
    }

    fn progress_widget_id(parent: sui::WidgetId) -> sui::WidgetId {
        sui::WidgetId::new(parent.get().wrapping_mul(61).wrapping_add(11_401))
    }

    fn transcript_text_style(
        pal: &Palette,
        theme: &sui::DefaultTheme,
        kind: TranscriptTextStyleKind,
        color: Color,
    ) -> sui::TextStyle {
        let (token, weight) = Self::transcript_text_token(theme, kind);
        let mut style = pal.text_style(theme, token.size, weight, color);
        style.line_height = token.line_height;
        style
    }

    fn transcript_text_token(
        theme: &sui::DefaultTheme,
        kind: TranscriptTextStyleKind,
    ) -> (sui::ThemeTextToken, FontWeight) {
        match kind {
            TranscriptTextStyleKind::Body => (theme.text.base, FontWeight::NORMAL),
            TranscriptTextStyleKind::Heading(level) => {
                let token = match level {
                    1 => theme.text.xl,
                    2 => theme.text.lg,
                    _ => theme.text.base,
                };
                (token, FontWeight::SEMIBOLD)
            }
            TranscriptTextStyleKind::Muted => (theme.text.sm, FontWeight::NORMAL),
        }
    }

    fn markdown_styles(
        pal: &Palette,
        theme: &sui::DefaultTheme,
        text_color: Color,
    ) -> ChatMarkdownStyles {
        let body =
            Self::transcript_text_style(pal, theme, TranscriptTextStyleKind::Body, text_color);
        let headings = std::array::from_fn(|index| {
            Self::transcript_text_style(
                pal,
                theme,
                TranscriptTextStyleKind::Heading((index + 1) as u8),
                text_color,
            )
        });
        let mut inline_code = theme.mono_text_style(body.color);
        inline_code.font_size = theme.text.sm.size;
        inline_code.line_height = theme.text.base.line_height;
        inline_code.weight = FontWeight::MEDIUM;
        let mut link = body.clone();
        link.color = pal.accent;
        ChatMarkdownStyles {
            body,
            headings,
            inline_code,
            link,
        }
    }

    fn plain_rich_render(text: String, style: TextStyle) -> ChatMarkdownRender {
        let len = text.len();
        ChatMarkdownRender {
            blocks: vec![ChatRichRenderBlock::Text {
                document: TextDocument::from_plain_text(text.clone(), style),
                display_range: 0..len,
            }],
            source_map: RichTextSourceMap::identity(text),
        }
    }

    fn message_rich_render(
        message: &ChatMessage,
        styles: ChatMarkdownStyles,
    ) -> ChatMarkdownRender {
        if message.kind == ChatMessageKind::Error {
            return Self::plain_rich_render(transcript_message_display_text(message), styles.body);
        }

        if message.source == ChatMessageSource::Model {
            let text = visible_model_message_text(&message.text);
            if text.trim().is_empty() {
                return Self::plain_rich_render("…".to_string(), styles.body);
            }
            return chat_render::markdown_rich_render(&text, styles);
        }

        let text = if message.text.is_empty() {
            "…".to_string()
        } else {
            message.text.clone()
        };
        Self::plain_rich_render(text, styles.body)
    }

    fn code_text_document(
        code: &ChatCodeBlock,
        dark: bool,
        pal: &Palette,
        theme: &sui::DefaultTheme,
    ) -> TextDocument {
        let highlighted = chat_render::highlight_code(code.language.as_deref(), &code.text, dark);
        let source_lines = code.text.split('\n').collect::<Vec<_>>();
        let mut paragraphs = highlighted
            .iter()
            .enumerate()
            .map(|(index, spans)| {
                let source = source_lines.get(index).copied().unwrap_or_default();
                let fallback = if code.is_diff {
                    diff_line_color(source, pal)
                } else {
                    pal.text
                };
                let spans = spans
                    .iter()
                    .map(|span| {
                        let color = span.rgb.map(color_from_rgb).unwrap_or(fallback);
                        let mut style = theme.mono_text_style(color);
                        style.font_size = theme.text.sm.size;
                        style.line_height = theme.text.sm.line_height;
                        TextSpan::new(&span.text, style)
                    })
                    .collect::<Vec<_>>();
                TextParagraph {
                    style: TextParagraphStyle {
                        wrap: TextWrap::NoWrap,
                        ..TextParagraphStyle::default()
                    },
                    spans,
                }
            })
            .collect::<Vec<_>>();
        if paragraphs.is_empty() {
            let mut style = theme.mono_text_style(pal.text);
            style.font_size = theme.text.sm.size;
            style.line_height = theme.text.sm.line_height;
            paragraphs.push(TextParagraph::new("", style));
        }
        TextDocument { paragraphs }
    }

    fn layout_message<C: TranscriptLayoutContext>(
        ctx: &C,
        pal: &Palette,
        theme: &sui::DefaultTheme,
        message: &ChatMessage,
        inner_w: f32,
    ) -> TranscriptLaidMessage {
        let is_user = message.role == ChatRole::User;
        let text_color = pal.text;
        let render =
            Self::message_rich_render(message, Self::markdown_styles(pal, theme, text_color));
        let mut blocks = Vec::new();
        let mut widest = 0.0_f32;
        let mut height = 0.0_f32;

        for block in render.blocks {
            match block {
                ChatRichRenderBlock::Text {
                    document,
                    display_range,
                } => {
                    let layout = ctx
                        .transcript_layout_document(
                            TextLayoutRequest::new(document)
                                .with_box_size(Size::new(inner_w.max(1.0), 1.0)),
                        )
                        .expect("chat rich-text layout should succeed");
                    let measurement = layout.measurement();
                    let block_h = measurement.height.max(theme.text.base.line_height) + 6.0;
                    widest = widest.max(measurement.width.min(inner_w));
                    height += block_h;
                    blocks.push(TranscriptLaidBlock::Text {
                        layout,
                        display_range,
                        height: block_h,
                    });
                }
                ChatRichRenderBlock::Code {
                    code,
                    display_range,
                } => {
                    let document = Self::code_text_document(&code, pal.dark, pal, theme);
                    let layout = ctx
                        .transcript_layout_document(
                            TextLayoutRequest::new(document)
                                .with_box_size(Size::new((inner_w - 20.0).max(1.0), 1.0)),
                        )
                        .expect("chat code layout should succeed");
                    let code_h = 36.0 + layout.measurement().height.max(17.0) + 8.0;
                    widest = widest.max(inner_w);
                    height += code_h + 6.0;
                    blocks.push(TranscriptLaidBlock::Code {
                        code,
                        layout,
                        display_range,
                        height: code_h,
                    });
                }
            }
        }

        let action = transcript_message_recovery_action(message);
        let action_w = action
            .map(Self::recovery_card_action_width)
            .unwrap_or_default();
        let action_h = if action.is_some() { 38.0 } else { 0.0 };
        let bubble_w = widest.min(inner_w).max(action_w).max(64.0) + 24.0;
        let bubble_h = height.max(18.0) + 20.0 + action_h;
        TranscriptLaidMessage {
            is_user,
            kind: message.kind,
            blocks,
            bubble_w,
            bubble_h,
            action,
            source_map: render.source_map,
        }
    }

    fn code_block_height(code: &ChatCodeBlock) -> f32 {
        let line_count = code.text.lines().count().max(1) as f32;
        30.0 + line_count * 17.0 + 12.0
    }

    fn tool_trace_layout<C: TranscriptLayoutContext>(
        ctx: &C,
        pal: &Palette,
        theme: &sui::DefaultTheme,
        chat: &ChatState,
        inner_w: f32,
        max_bubble: f32,
    ) -> Vec<TranscriptLaidTool> {
        if chat.tool_calls.is_empty() {
            return Vec::new();
        }

        let bubble_w = max_bubble.max(260.0);
        if !chat.tool_chain_expanded() {
            let latest = chat
                .tool_calls
                .last()
                .map(tool_call_summary)
                .unwrap_or_default();
            return vec![TranscriptLaidTool::ChainCollapsed {
                call_count: chat.tool_calls.len(),
                latest,
                bubble_w,
                bubble_h: 64.0,
            }];
        }

        let mut items = vec![TranscriptLaidTool::ChainHeader {
            call_count: chat.tool_calls.len(),
            bubble_w,
            bubble_h: 42.0,
        }];
        for call in &chat.tool_calls {
            items.push(Self::layout_tool_call(
                ctx, pal, theme, chat, call, inner_w, bubble_w,
            ));
        }
        items
    }

    fn layout_tool_call<C: TranscriptLayoutContext>(
        ctx: &C,
        pal: &Palette,
        theme: &sui::DefaultTheme,
        chat: &ChatState,
        call: &ChatToolActivity,
        inner_w: f32,
        bubble_w: f32,
    ) -> TranscriptLaidTool {
        let input_block = Self::layout_tool_input(ctx, pal, theme, chat, call, inner_w);
        let output_block = Self::layout_tool_output(ctx, pal, theme, chat, call, inner_w);
        let detail_h = input_block
            .as_ref()
            .map(|block| block.height)
            .unwrap_or(0.0)
            + output_block
                .as_ref()
                .map(|block| block.height)
                .unwrap_or(0.0);
        TranscriptLaidTool::Call {
            call: call.clone(),
            input_block,
            output_block,
            bubble_w,
            bubble_h: 50.0 + detail_h,
        }
    }

    fn layout_tool_input<C: TranscriptLayoutContext>(
        ctx: &C,
        pal: &Palette,
        theme: &sui::DefaultTheme,
        chat: &ChatState,
        call: &ChatToolActivity,
        inner_w: f32,
    ) -> Option<TranscriptLaidToolDetail> {
        if call.input.trim().is_empty() {
            return None;
        }
        let key = tool_detail_key(call, ToolDetailSection::Input);
        let expanded = chat.tool_detail_expanded(&key);
        let edit = chat_render::file_edit_render(&call.tool, &call.input);
        let (title, code, raw) = if let Some(edit) = edit {
            (edit.title, Some(edit.code.clone()), edit.code.text)
        } else {
            (
                "Input".to_string(),
                Some(ChatCodeBlock {
                    language: Some("json".to_string()),
                    text: call.input.clone(),
                    is_diff: false,
                }),
                call.input.clone(),
            )
        };
        Some(Self::layout_tool_detail(
            ctx,
            pal,
            theme,
            ToolDetailSection::Input,
            title,
            raw,
            code,
            expanded,
            inner_w,
        ))
    }

    fn layout_tool_output<C: TranscriptLayoutContext>(
        ctx: &C,
        pal: &Palette,
        theme: &sui::DefaultTheme,
        chat: &ChatState,
        call: &ChatToolActivity,
        inner_w: f32,
    ) -> Option<TranscriptLaidToolDetail> {
        let output = tool_output_text(call);
        if output.trim().is_empty() {
            return None;
        }
        let key = tool_detail_key(call, ToolDetailSection::Output);
        let expanded = chat.tool_detail_expanded(&key);
        let is_diff = output
            .lines()
            .take(24)
            .filter(|line| line.starts_with('+') || line.starts_with('-') || line.starts_with("@@"))
            .count()
            >= 2;
        let code = Some(ChatCodeBlock {
            language: if is_diff {
                Some("diff".to_string())
            } else {
                None
            },
            text: output.clone(),
            is_diff,
        });
        Some(Self::layout_tool_detail(
            ctx,
            pal,
            theme,
            ToolDetailSection::Output,
            "Output".to_string(),
            output,
            code,
            expanded,
            inner_w,
        ))
    }

    fn layout_tool_detail<C: TranscriptLayoutContext>(
        ctx: &C,
        pal: &Palette,
        theme: &sui::DefaultTheme,
        section: ToolDetailSection,
        title: String,
        raw: String,
        code: Option<ChatCodeBlock>,
        expanded: bool,
        inner_w: f32,
    ) -> TranscriptLaidToolDetail {
        let collapsible = tool_detail_collapsible(&raw);
        let visible_text = if collapsible && !expanded {
            preview_multiline(&raw, TOOL_DETAIL_PREVIEW_CHARS, 8)
        } else {
            raw.clone()
        };
        let visible_code = code.as_ref().map(|code| ChatCodeBlock {
            text: visible_text.clone(),
            ..code.clone()
        });
        let content_h = if let Some(code) = &visible_code {
            Self::code_block_height(code)
        } else {
            let style =
                Self::transcript_text_style(pal, theme, TranscriptTextStyleKind::Muted, pal.text);
            let measure_style = style.clone();
            let lines = wrap_text_lines(&visible_text, inner_w, |s| {
                ctx.transcript_measure_text_width(s, measure_style.clone())
            });
            lines.len().max(1) as f32 * style.line_height + 8.0
        };
        let button_h = if collapsible { 30.0 } else { 0.0 };
        TranscriptLaidToolDetail {
            section,
            title,
            collapsible,
            code: visible_code,
            text: if code.is_none() {
                Some(visible_text)
            } else {
                None
            },
            height: 22.0 + content_h + button_h + 8.0,
        }
    }

    /// Pure layout for one laid message: `(container, content_x, content_w,
    /// row_mode)`. Shared between painting and selection line recording so
    /// hit-test rects can never drift from the painted text.
    fn message_content_geometry(
        area: Rect,
        item: &TranscriptLaidMessage,
        y: f32,
        mode: TranscriptLayoutMode,
    ) -> (Rect, f32, f32, bool) {
        let is_error = item.kind == ChatMessageKind::Error;
        let row_mode = mode == TranscriptLayoutMode::Row && !item.is_user && !is_error;
        if row_mode {
            let rail_w = 24.0;
            let rail_gap = 10.0;
            let row_w =
                (item.bubble_w + rail_w + rail_gap).min((area.width() - PAD * 2.0).max(0.0));
            let row = Rect::new(area.x() + PAD, y, row_w, item.bubble_h);
            (
                row,
                row.x() + rail_w + rail_gap,
                (row.width() - rail_w - rail_gap - 10.0).max(0.0),
                true,
            )
        } else {
            let bx = if item.is_user {
                area.x() + area.width() - PAD - item.bubble_w
            } else {
                area.x() + PAD
            };
            let bubble = Rect::new(bx, y, item.bubble_w, item.bubble_h);
            (bubble, bubble.x() + 12.0, bubble.width() - 24.0, false)
        }
    }

    fn text_layout_origin(x: f32, y: f32, layout: &TextLayout) -> Point {
        let bounds = layout.measurement().bounds;
        Point::new(x - bounds.x(), y - bounds.y())
    }

    fn text_layout_rect(origin: Point, layout: &TextLayout) -> Rect {
        let bounds = layout.measurement().bounds;
        Rect::new(
            origin.x + bounds.x(),
            origin.y + bounds.y(),
            bounds.width().max(1.0),
            bounds.height().max(1.0),
        )
    }

    /// Retain each SUI rich-text layout at its painted origin so pointer events
    /// and selection overlays use the exact same shaped character geometry.
    fn record_message_text_hits(
        hits_out: &mut Vec<TranscriptTextHit>,
        area: Rect,
        item: &TranscriptLaidMessage,
        y: f32,
        mode: TranscriptLayoutMode,
        message_key: u64,
    ) {
        let (container, content_x, content_w, row_mode) =
            Self::message_content_geometry(area, item, y, mode);
        let mut ty = container.y() + 10.0;
        for block in &item.blocks {
            match block {
                TranscriptLaidBlock::Text {
                    layout,
                    display_range,
                    height,
                } => {
                    let origin = Self::text_layout_origin(content_x, ty, layout);
                    hits_out.push(TranscriptTextHit {
                        message_key,
                        display_range: display_range.clone(),
                        rect: Self::text_layout_rect(origin, layout),
                        origin,
                        layout: layout.clone(),
                    });
                    ty += *height;
                }
                TranscriptLaidBlock::Code {
                    layout,
                    display_range,
                    height,
                    ..
                } => {
                    let rect = if row_mode {
                        Rect::new(content_x, ty, content_w, *height)
                    } else {
                        Rect::new(container.x() + 8.0, ty, container.width() - 16.0, *height)
                    };
                    let origin = Self::text_layout_origin(rect.x() + 10.0, rect.y() + 28.0, layout);
                    hits_out.push(TranscriptTextHit {
                        message_key,
                        display_range: display_range.clone(),
                        rect: Self::text_layout_rect(origin, layout),
                        origin,
                        layout: layout.clone(),
                    });
                    ty += *height + 6.0;
                }
            }
        }
    }

    fn selected_range_for_message(
        &self,
        message_key: u64,
        rendered_len: usize,
    ) -> Option<std::ops::Range<usize>> {
        let (start, end) = self.ordered_selection()?;
        let sources = self.transcript_message_sources.borrow();
        let start_index = sources
            .iter()
            .position(|source| source.message_key == start.message_key)?;
        let end_index = sources
            .iter()
            .position(|source| source.message_key == end.message_key)?;
        let index = sources
            .iter()
            .position(|source| source.message_key == message_key)?;
        if index < start_index || index > end_index {
            return None;
        }
        let range_start = if index == start_index {
            start.offset.min(rendered_len)
        } else {
            0
        };
        let range_end = if index == end_index {
            end.offset.min(rendered_len)
        } else {
            rendered_len
        };
        (range_start < range_end).then_some(range_start..range_end)
    }

    fn paint_rich_selection(
        ctx: &mut PaintCtx,
        layout: &TextLayout,
        origin: Point,
        display_range: &std::ops::Range<usize>,
        selection: Option<&std::ops::Range<usize>>,
        fill: Color,
    ) {
        let Some(selection) = selection else {
            return;
        };
        let start = selection.start.max(display_range.start);
        let end = selection.end.min(display_range.end);
        if start >= end {
            return;
        }
        let local =
            start.saturating_sub(display_range.start)..end.saturating_sub(display_range.start);
        for rect in layout.selection_rects(local) {
            paint_rounded_rect(
                ctx,
                Rect::new(
                    origin.x + rect.x(),
                    origin.y + rect.y(),
                    rect.width().max(1.0),
                    rect.height(),
                ),
                fill,
                2.0,
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn paint_laid_message(
        &self,
        ctx: &mut PaintCtx,
        pal: &Palette,
        theme: &sui::DefaultTheme,
        area: Rect,
        item: &TranscriptLaidMessage,
        y: f32,
        mode: TranscriptLayoutMode,
        message_key: u64,
    ) {
        let is_error = item.kind == ChatMessageKind::Error;
        let bubble_radius = theme.radius._2xl.min(14.0);
        let (container, content_x, content_w, row_mode) =
            Self::message_content_geometry(area, item, y, mode);
        let bubble_color = if row_mode {
            // Wide transcripts read as a log row: quiet surface, subtle
            // hairline, the agent's identity hue on the rail glyph.
            let radius = theme.radius.md.min(6.0);
            paint_rounded_panel(ctx, container, pal.panel, pal.border_subtle, radius);
            draw_glyph(
                ctx,
                IconGlyph::Sparkles,
                Rect::new(container.x() + 5.0, container.y() + 10.0, 14.0, 14.0),
                pal.agent[0],
            );
            pal.panel
        } else {
            // Mesh bubbles: user rides the accent wash with an accent border,
            // the agent stays on the quiet surface tier, errors go danger-soft.
            let (bubble_color, _text_color, border) = if item.is_user {
                (pal.accent_soft, pal.text, Some(pal.accent_border))
            } else if is_error {
                (pal.danger_soft, pal.text, Some(pal.bad))
            } else {
                (pal.surface_2, pal.text, Some(pal.border_subtle))
            };
            match border {
                Some(bc) => paint_rounded_panel(ctx, container, bubble_color, bc, bubble_radius),
                None => paint_rounded_rect(ctx, container, bubble_color, bubble_radius),
            }
            bubble_color
        };

        // Selection highlight: flattened accent wash painted between the
        // bubble surface and the text (sui blends linear, so translucent
        // tokens are pre-composited with `Color::over`).
        let selection_fill = pal
            .accent
            .with_alpha(if pal.dark { 0.38 } else { 0.26 })
            .over(bubble_color);

        let selected =
            self.selected_range_for_message(message_key, item.source_map.rendered().len());
        let mut ty = container.y() + 10.0;
        for block in &item.blocks {
            match block {
                TranscriptLaidBlock::Text {
                    layout,
                    display_range,
                    height,
                } => {
                    let origin = Self::text_layout_origin(content_x, ty, layout);
                    Self::paint_rich_selection(
                        ctx,
                        layout,
                        origin,
                        display_range,
                        selected.as_ref(),
                        selection_fill,
                    );
                    ctx.draw_text_layout(origin, layout);
                    ty += *height;
                }
                TranscriptLaidBlock::Code {
                    code,
                    layout,
                    display_range,
                    height,
                } => {
                    let rect = if row_mode {
                        Rect::new(content_x, ty, content_w, *height)
                    } else {
                        Rect::new(container.x() + 8.0, ty, container.width() - 16.0, *height)
                    };
                    let label = code.language.as_deref().unwrap_or(if code.is_diff {
                        "diff"
                    } else {
                        "text"
                    });
                    paint_code_panel(
                        ctx,
                        theme,
                        rect,
                        label,
                        CodePanelPaint::new()
                            .fill(if pal.dark { pal.field } else { pal.bg_subtle })
                            .border(pal.border)
                            .header_fill(pal.surface_2)
                            .label_color(pal.text_faint)
                            .radius(theme.radius.md.min(6.0))
                            .header_height(24.0),
                    );
                    let origin = Self::text_layout_origin(rect.x() + 10.0, rect.y() + 28.0, layout);
                    if code.is_diff {
                        for (line, source) in layout.lines().iter().zip(code.text.split('\n')) {
                            if let Some(background) = diff_line_background(source, pal) {
                                ctx.fill_rect(
                                    Rect::new(
                                        rect.x() + 6.0,
                                        origin.y + line.rect.y(),
                                        (rect.width() - 12.0).max(1.0),
                                        line.rect.height(),
                                    ),
                                    background,
                                );
                            }
                        }
                    }
                    let code_selection_fill = pal
                        .accent
                        .with_alpha(if pal.dark { 0.38 } else { 0.24 })
                        .over(if pal.dark { pal.field } else { pal.bg_subtle });
                    Self::paint_rich_selection(
                        ctx,
                        layout,
                        origin,
                        display_range,
                        selected.as_ref(),
                        code_selection_fill,
                    );
                    ctx.draw_text_layout(origin, layout);
                    ty += *height + 6.0;
                }
            }
        }
    }

    fn paint_code_block(
        ctx: &mut PaintCtx,
        pal: &Palette,
        theme: &sui::DefaultTheme,
        rect: Rect,
        code: &ChatCodeBlock,
        dark: bool,
    ) {
        let label = code
            .language
            .as_deref()
            .unwrap_or(if code.is_diff { "diff" } else { "text" });
        // Mesh code blocks: bg-subtle in Light, the inset field tier in
        // dark/void; a quiet surface-2 header band with the mono meta label.
        let content = paint_code_panel(
            ctx,
            theme,
            rect,
            label,
            CodePanelPaint::new()
                .fill(if pal.dark { pal.field } else { pal.bg_subtle })
                .border(pal.border)
                .header_fill(pal.surface_2)
                .label_color(pal.text_faint)
                .radius(theme.radius.md.min(6.0))
                .header_height(24.0),
        );
        if content.width() <= 0.0 || content.height() <= 0.0 {
            return;
        }
        let highlighted = chat_render::highlight_code(code.language.as_deref(), &code.text, dark);
        let source_lines = code.text.lines().collect::<Vec<_>>();
        let painted_spans = highlighted
            .iter()
            .map(|spans| {
                spans
                    .iter()
                    .map(|span| {
                        let mut painted = CodeTextSpan::new(&span.text);
                        if let Some(rgb) = span.rgb {
                            painted = painted.color(color_from_rgb(rgb));
                        }
                        painted
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let painted_lines = painted_spans
            .iter()
            .enumerate()
            .map(|(index, spans)| {
                let source = source_lines.get(index).copied().unwrap_or_default();
                let fallback = if code.is_diff {
                    diff_line_color(source, pal)
                } else {
                    pal.text
                };
                let line = CodeTextLine::new(spans).fallback_color(fallback);
                if code.is_diff
                    && let Some(background) = diff_line_background(source, pal)
                {
                    return line.background(background);
                }
                line
            })
            .collect::<Vec<_>>();
        paint_code_lines(
            ctx,
            theme,
            content,
            &painted_lines,
            CodeTextPaint::new()
                .color(pal.text)
                .font_size(theme.text.sm.size)
                .line_height(theme.text.sm.line_height)
                .x_padding(2.0),
        );
    }

    fn paint_tool_item(
        &self,
        ctx: &mut PaintCtx,
        pal: &Palette,
        theme: &sui::DefaultTheme,
        area: Rect,
        item: &TranscriptLaidTool,
        y: f32,
    ) {
        match item {
            TranscriptLaidTool::ChainCollapsed {
                call_count,
                latest,
                bubble_w,
                bubble_h,
            } => {
                let bx = area.x() + PAD;
                let bubble = Rect::new(bx, y, *bubble_w, *bubble_h);
                let title = format!("Tool activity · {call_count} calls");
                let subtitle = compact_line(latest, 72);
                paint_action_tile(
                    ctx,
                    theme,
                    bubble,
                    &title,
                    Some(&subtitle),
                    Some(IconGlyph::File),
                    Self::tool_card_paint(pal)
                        .icon_color(pal.accent)
                        .trailing_width(76.0),
                );
            }
            TranscriptLaidTool::ChainHeader {
                call_count,
                bubble_w,
                bubble_h,
            } => {
                let bx = area.x() + PAD;
                let bubble = Rect::new(bx, y, *bubble_w, *bubble_h);
                let title = format!("Tool activity · {call_count} calls");
                paint_action_tile(
                    ctx,
                    theme,
                    bubble,
                    &title,
                    None,
                    None,
                    Self::tool_card_paint(pal).trailing_width(70.0),
                );
            }
            TranscriptLaidTool::Call {
                call,
                input_block,
                output_block,
                bubble_w,
                bubble_h,
            } => {
                let bx = area.x() + PAD;
                let bubble = Rect::new(bx, y, *bubble_w, *bubble_h);
                let title = compact_line(&tool_display_name(&call.tool), 64);
                let subtitle = compact_line(&tool_call_summary(call), 90);
                paint_action_tile(
                    ctx,
                    theme,
                    bubble,
                    &title,
                    Some(&subtitle),
                    None,
                    Self::tool_card_paint(pal)
                        .leading_tone_dot(activity_tone_semantic(tool_status_tone(&call.status)))
                        .leading_width(18.0),
                );
                let mut ty = y + 50.0;
                let dark = widgets::theme_is_dark(&self.shared);
                if let Some(input) = input_block {
                    Self::paint_tool_detail(
                        ctx,
                        pal,
                        theme,
                        Rect::new(bx + 12.0, ty, bubble.width() - 24.0, input.height),
                        input,
                        dark,
                    );
                    ty += input.height;
                }
                if let Some(output) = output_block {
                    Self::paint_tool_detail(
                        ctx,
                        pal,
                        theme,
                        Rect::new(bx + 12.0, ty, bubble.width() - 24.0, output.height),
                        output,
                        dark,
                    );
                }
            }
        }
    }

    fn tool_card_paint(pal: &Palette) -> ActionTilePaint {
        // Mesh tool-call card: quiet surface, hairline border, r-2 corners.
        ActionTilePaint::neutral()
            .background(pal.panel)
            .border(pal.border)
            .title_color(pal.text)
            .subtitle_color(pal.text_faint)
            .radius(6.0)
            .padding_x(12.0)
    }

    fn paint_tool_detail(
        ctx: &mut PaintCtx,
        pal: &Palette,
        theme: &sui::DefaultTheme,
        rect: Rect,
        detail: &TranscriptLaidToolDetail,
        dark: bool,
    ) {
        let section_label = match detail.section {
            ToolDetailSection::Input => "Input",
            ToolDetailSection::Output => "Output",
        };
        paint_section_label_detail(
            ctx,
            theme,
            Rect::new(rect.x(), rect.y() + 2.0, rect.width(), 16.0),
            section_label,
            &detail.title,
            sui::SectionLabelPaint::new().color(pal.text_faint),
        );
        let y = rect.y() + 22.0;
        if let Some(code) = &detail.code {
            let code_rect = Rect::new(rect.x(), y, rect.width(), Self::code_block_height(code));
            Self::paint_code_block(ctx, pal, theme, code_rect, code, dark);
        } else if let Some(text) = detail.text.as_deref() {
            let bottom_reserve = if detail.collapsible { 34.0 } else { 8.0 };
            paint_text_block(
                ctx,
                theme,
                Rect::new(
                    rect.x(),
                    y,
                    rect.width(),
                    (rect.max_y() - y - bottom_reserve).max(0.0),
                ),
                text,
                TextBlockPaint::new()
                    .text_token(theme.text.sm)
                    .color(pal.text),
            );
        }
    }

    fn request_recovery_action_refresh(ctx: &mut EventCtx) {
        use sui::{InvalidationKind, InvalidationRequest, InvalidationTarget};

        let window = InvalidationTarget::Window(ctx.window_id());
        ctx.request(InvalidationRequest::new(window, InvalidationKind::Measure));
        ctx.request(InvalidationRequest::new(window, InvalidationKind::Arrange));
        ctx.request_paint();
        ctx.request_semantics();
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TranscriptEmptyControlState {
    Standard {
        title: String,
        hint: String,
        context: Option<String>,
        actions: Vec<ChatQuickAction>,
    },
    Recovery {
        target: RuntimeRecoveryTarget,
        title: String,
        detail: String,
        hint: String,
        actions: Vec<RecoveryCardAction>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TranscriptRuntimeControlSpec {
    action: RuntimeTranscriptAction,
    expanded: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct TranscriptControlSignature {
    empty: Option<TranscriptEmptyControlState>,
    message_actions: Vec<RecoveryCardAction>,
    runtime_actions: Vec<TranscriptRuntimeControlSpec>,
}

impl TranscriptControlSignature {
    fn current(shared: &Shared) -> Self {
        let chat = shared.chat();
        if chat.messages.is_empty() {
            let empty = if let Some(target) = chat.runtime_recovery_target() {
                Some(TranscriptEmptyControlState::Recovery {
                    target,
                    title: chat.recovery_empty_title(target).to_string(),
                    detail: chat.recovery_empty_detail(target).to_string(),
                    hint: chat.recovery_empty_hint(target).to_string(),
                    actions: TranscriptView::recovery_card_actions(&chat, target),
                })
            } else {
                Some(TranscriptEmptyControlState::Standard {
                    title: chat.empty_conversation_title(),
                    hint: chat.empty_conversation_hint(),
                    context: chat.empty_conversation_context(),
                    actions: TranscriptView::quick_actions(&chat).to_vec(),
                })
            };
            return Self {
                empty,
                ..Self::default()
            };
        }

        let message_actions = chat
            .messages
            .iter()
            .filter_map(transcript_message_recovery_action)
            .collect();
        let mut runtime_actions = Vec::new();
        if !chat.tool_calls.is_empty() {
            let chain_expanded = chat.tool_chain_expanded();
            runtime_actions.push(TranscriptRuntimeControlSpec {
                action: RuntimeTranscriptAction::ToggleToolChain,
                expanded: chain_expanded,
            });
            if chain_expanded {
                for call in &chat.tool_calls {
                    let input = chat_render::file_edit_render(&call.tool, &call.input)
                        .map(|edit| edit.code.text)
                        .unwrap_or_else(|| call.input.clone());
                    if !input.trim().is_empty() && tool_detail_collapsible(&input) {
                        let key = tool_detail_key(call, ToolDetailSection::Input);
                        runtime_actions.push(TranscriptRuntimeControlSpec {
                            expanded: chat.tool_detail_expanded(&key),
                            action: RuntimeTranscriptAction::ToggleToolDetail(key),
                        });
                    }
                    let output = tool_output_text(call);
                    if !output.trim().is_empty() && tool_detail_collapsible(&output) {
                        let key = tool_detail_key(call, ToolDetailSection::Output);
                        runtime_actions.push(TranscriptRuntimeControlSpec {
                            expanded: chat.tool_detail_expanded(&key),
                            action: RuntimeTranscriptAction::ToggleToolDetail(key),
                        });
                    }
                }
            }
        }

        Self {
            empty: None,
            message_actions,
            runtime_actions,
        }
    }
}

struct TranscriptControls {
    shared: Shared,
    signature: TranscriptControlSignature,
    empty_body: Option<WidgetPod>,
    empty_body_rect: Rect,
    empty_actions: Vec<WidgetPod>,
    empty_action_rects: Vec<Option<Rect>>,
    message_actions: Vec<WidgetPod>,
    message_action_rects: Vec<Option<Rect>>,
    runtime_actions: Vec<WidgetPod>,
    runtime_action_rects: Vec<Option<Rect>>,
}

impl TranscriptControls {
    fn new(shared: Shared) -> Self {
        Self {
            shared,
            signature: TranscriptControlSignature::default(),
            empty_body: None,
            empty_body_rect: Rect::ZERO,
            empty_actions: Vec::new(),
            empty_action_rects: Vec::new(),
            message_actions: Vec::new(),
            message_action_rects: Vec::new(),
            runtime_actions: Vec::new(),
            runtime_action_rects: Vec::new(),
        }
    }

    fn button_theme(shared: &Shared) -> impl Fn() -> DefaultTheme + 'static {
        let shared = shared.clone();
        move || shared.theme().with_size(ControlSize::Small)
    }

    fn refresh_after_activation(ctx: &mut EventCtx) {
        TranscriptView::request_recovery_action_refresh(ctx);
    }

    fn build_standard_empty_body(
        shared: &Shared,
        title: &str,
        hint: &str,
        context: Option<&str>,
    ) -> WidgetPod {
        let mut empty = EmptyState::new(title, hint)
            .name("Conversation empty state")
            .icon(IconGlyph::Sparkles)
            .transparent()
            .theme_when({
                let shared = shared.clone();
                move || shared.theme()
            });
        if let Some(context) = context {
            empty = empty.detail(context);
        }
        WidgetPod::new(empty)
    }

    fn build_recovery_empty_body(
        shared: &Shared,
        title: &str,
        detail: &str,
        hint: &str,
    ) -> WidgetPod {
        let theme = shared.theme();
        let mut title_style = theme.body_text_style();
        title_style.font_size = theme.text.lg.size;
        title_style.line_height = theme.text.lg.line_height;
        title_style.weight = FontWeight::SEMIBOLD;
        let mut body_style = theme.body_text_style();
        body_style.font_size = theme.text.sm.size;
        body_style.line_height = theme.text.sm.line_height;
        body_style.color = theme.palette.text_muted;
        let mut hint_style = body_style.clone();
        hint_style.font_size = theme.text.xs.size;
        hint_style.line_height = theme.text.xs.line_height;
        hint_style.color = theme.palette.text_muted;

        let header = Flex::horizontal()
            .main_gap(12.0)
            .align_items(Alignment::Center)
            .with_child(
                Icon::new(IconGlyph::Alert)
                    .size(28.0)
                    .color(theme.palette.warning),
            )
            .with_item(
                Label::new(title).style(title_style),
                FlexItem::flex(1.0).min_width(0.0),
            );
        let body = Flex::vertical()
            .main_gap(8.0)
            .align_items(Alignment::Stretch)
            .with_child(header)
            .with_child(RichText::from_plain_text(detail, body_style))
            .with_child(RichText::from_plain_text(hint, hint_style))
            .with_child(SizedBox::new().height(38.0));
        WidgetPod::new(
            Surface::panel(body)
                .appearance(SurfaceAppearance::Soft)
                .tone(SemanticTone::Warning)
                .padding(Insets {
                    left: 18.0,
                    top: 18.0,
                    right: 18.0,
                    bottom: 14.0,
                })
                .fill_width()
                .theme_when({
                    let shared = shared.clone();
                    move || shared.theme()
                }),
        )
    }

    fn build_quick_action(shared: &Shared, action: ChatQuickAction) -> WidgetPod {
        let activation_shared = shared.clone();
        let primary = matches!(
            action,
            ChatQuickAction::ManageChannel | ChatQuickAction::NewChannel
        );
        WidgetPod::new(
            sui::Button::new(action.label())
                .description(action.description())
                .icon(action.icon())
                .min_width(TranscriptView::quick_action_width(action))
                .min_height(44.0)
                .theme_when(Self::button_theme(shared))
                .appearance(if primary {
                    sui::ButtonAppearance::Tonal
                } else {
                    sui::ButtonAppearance::Outline
                })
                .tone(if primary {
                    SemanticTone::Accent
                } else {
                    SemanticTone::Neutral
                })
                .on_press_with_ctx(move |ctx| {
                    activate_transcript_quick_action(&activation_shared, action);
                    Self::refresh_after_activation(ctx);
                }),
        )
    }

    fn build_recovery_action(
        shared: &Shared,
        action: RecoveryCardAction,
        primary: bool,
    ) -> WidgetPod {
        let activation_shared = shared.clone();
        WidgetPod::new(
            sui::Button::new(action.label())
                .description(action.description())
                .icon(action.icon())
                .min_width(TranscriptView::recovery_card_action_width(action))
                .min_height(28.0)
                .theme_when(Self::button_theme(shared))
                .appearance(if primary {
                    sui::ButtonAppearance::Filled
                } else {
                    sui::ButtonAppearance::Tonal
                })
                .tone(match action {
                    RecoveryCardAction::OpenConnectionSettings if !primary => SemanticTone::Warning,
                    _ if primary => SemanticTone::Accent,
                    _ => SemanticTone::Neutral,
                })
                .on_press_with_ctx(move |ctx| {
                    activate_transcript_recovery_action(&activation_shared, action);
                    Self::refresh_after_activation(ctx);
                }),
        )
    }

    fn build_runtime_action(shared: &Shared, spec: &TranscriptRuntimeControlSpec) -> WidgetPod {
        let activation_shared = shared.clone();
        let action = spec.action.clone();
        let (label, min_width, min_height) = match &action {
            RuntimeTranscriptAction::ToggleToolChain => {
                (if spec.expanded { "Hide" } else { "Show" }, 56.0, 28.0)
            }
            RuntimeTranscriptAction::ToggleToolDetail(_) => (
                if spec.expanded {
                    "Show less"
                } else {
                    "Show more"
                },
                126.0,
                24.0,
            ),
        };
        let description = match &action {
            RuntimeTranscriptAction::ToggleToolChain => "Show or hide the chain of tool calls",
            RuntimeTranscriptAction::ToggleToolDetail(_) => {
                "Show or hide long tool input or output"
            }
        };
        WidgetPod::new(
            sui::Button::new(label)
                .semantic_name(runtime_action_label(&action))
                .description(description)
                .icon(if spec.expanded {
                    IconGlyph::ChevronUp
                } else {
                    IconGlyph::ChevronDown
                })
                .min_width(min_width)
                .min_height(min_height)
                .theme_when(Self::button_theme(shared))
                .appearance(sui::ButtonAppearance::Ghost)
                .tone(SemanticTone::Neutral)
                .on_press_with_ctx(move |ctx| {
                    activate_transcript_runtime_action(&activation_shared, &action);
                    Self::refresh_after_activation(ctx);
                }),
        )
    }

    fn sync_controls(&mut self) {
        let signature = TranscriptControlSignature::current(&self.shared);
        if signature == self.signature {
            return;
        }

        self.empty_body = None;
        self.empty_actions.clear();
        self.message_actions.clear();
        self.runtime_actions.clear();
        if let Some(empty) = &signature.empty {
            match empty {
                TranscriptEmptyControlState::Standard {
                    title,
                    hint,
                    context,
                    actions,
                } => {
                    self.empty_body = Some(Self::build_standard_empty_body(
                        &self.shared,
                        title,
                        hint,
                        context.as_deref(),
                    ));
                    self.empty_actions = actions
                        .iter()
                        .copied()
                        .map(|action| Self::build_quick_action(&self.shared, action))
                        .collect();
                }
                TranscriptEmptyControlState::Recovery {
                    target,
                    title,
                    detail,
                    hint,
                    actions,
                } => {
                    self.empty_body = Some(Self::build_recovery_empty_body(
                        &self.shared,
                        title,
                        detail,
                        hint,
                    ));
                    let chat = self.shared.chat();
                    self.empty_actions = actions
                        .iter()
                        .copied()
                        .map(|action| {
                            Self::build_recovery_action(
                                &self.shared,
                                action,
                                TranscriptView::recovery_action_is_primary(&chat, *target, action),
                            )
                        })
                        .collect();
                }
            }
        }
        self.message_actions = signature
            .message_actions
            .iter()
            .copied()
            .map(|action| Self::build_recovery_action(&self.shared, action, true))
            .collect();
        self.runtime_actions = signature
            .runtime_actions
            .iter()
            .map(|spec| Self::build_runtime_action(&self.shared, spec))
            .collect();
        self.empty_action_rects = vec![None; self.empty_actions.len()];
        self.message_action_rects = vec![None; self.message_actions.len()];
        self.runtime_action_rects = vec![None; self.runtime_actions.len()];
        self.signature = signature;
    }

    fn rect_is_visible(area: Rect, rect: Rect) -> bool {
        rect.max_y() >= area.y()
            && rect.y() <= area.max_y()
            && rect.max_x() >= area.x()
            && rect.x() <= area.max_x()
    }

    fn measure_pod_for_rect(pod: &mut WidgetPod, ctx: &mut MeasureCtx, rect: Rect) {
        pod.measure(ctx, Constraints::tight(rect.size));
    }

    fn compute_nonempty_rects(&mut self, ctx: &MeasureCtx, area: Rect) {
        self.message_action_rects.fill(None);
        self.runtime_action_rects.fill(None);
        let theme = self.shared.theme();
        let pal = palette(&self.shared);
        let chat = self.shared.chat();
        let layout_mode = TranscriptLayoutMode::for_width(area.width());
        let max_bubble = match layout_mode {
            TranscriptLayoutMode::Row => (area.width() - PAD * 2.0 - 34.0).clamp(260.0, 760.0),
            TranscriptLayoutMode::Bubble => (area.width() * 0.85).clamp(200.0, 620.0),
        };
        let inner_w = max_bubble - 24.0;
        let message_gap = layout_mode.message_gap();
        let laid = chat
            .messages
            .iter()
            .map(|message| TranscriptView::layout_message(ctx, &pal, &theme, message, inner_w))
            .collect::<Vec<_>>();
        let tool_layout =
            TranscriptView::tool_trace_layout(ctx, &pal, &theme, &chat, inner_w, max_bubble);
        let timeline_header_h = TranscriptView::timeline_header_height(&chat);
        let mut total = PAD + timeline_header_h;
        for item in &laid {
            total += item.bubble_h + message_gap;
        }
        for item in &tool_layout {
            let height = match item {
                TranscriptLaidTool::ChainCollapsed { bubble_h, .. }
                | TranscriptLaidTool::ChainHeader { bubble_h, .. }
                | TranscriptLaidTool::Call { bubble_h, .. } => *bubble_h,
            };
            total += height + 10.0;
        }
        total += PAD;
        let max_scroll = (total - area.height()).max(0.0);
        let scroll = if chat.stick_bottom {
            max_scroll
        } else {
            chat.scroll.clamp(0.0, max_scroll)
        };

        let mut y = area.y() + PAD - scroll + timeline_header_h;
        let mut message_slot = 0usize;
        for item in &laid {
            if let Some(action) = item.action {
                let (container, content_x, content_w, _) =
                    TranscriptView::message_content_geometry(area, item, y, layout_mode);
                let action_w = TranscriptView::recovery_card_action_width(action).min(content_w);
                let rect = Rect::new(
                    content_x,
                    container.y() + item.bubble_h - 38.0,
                    action_w,
                    28.0,
                );
                if let Some(slot) = self.message_action_rects.get_mut(message_slot)
                    && Self::rect_is_visible(area, rect)
                {
                    *slot = Some(rect);
                }
                message_slot += 1;
            }
            y += item.bubble_h + message_gap;
        }

        let mut runtime_slot = 0usize;
        for item in &tool_layout {
            let height = match item {
                TranscriptLaidTool::ChainCollapsed {
                    bubble_w, bubble_h, ..
                } => {
                    let bubble = Rect::new(area.x() + PAD, y, *bubble_w, *bubble_h);
                    let rect = Rect::new(bubble.max_x() - 76.0, y + 18.0, 60.0, 28.0);
                    if let Some(slot) = self.runtime_action_rects.get_mut(runtime_slot)
                        && Self::rect_is_visible(area, rect)
                    {
                        *slot = Some(rect);
                    }
                    runtime_slot += 1;
                    *bubble_h
                }
                TranscriptLaidTool::ChainHeader {
                    bubble_w, bubble_h, ..
                } => {
                    let bubble = Rect::new(area.x() + PAD, y, *bubble_w, *bubble_h);
                    let rect = Rect::new(bubble.max_x() - 72.0, y + 7.0, 56.0, 28.0);
                    if let Some(slot) = self.runtime_action_rects.get_mut(runtime_slot)
                        && Self::rect_is_visible(area, rect)
                    {
                        *slot = Some(rect);
                    }
                    runtime_slot += 1;
                    *bubble_h
                }
                TranscriptLaidTool::Call {
                    input_block,
                    output_block,
                    bubble_w,
                    bubble_h,
                    ..
                } => {
                    let bx = area.x() + PAD;
                    let mut ty = y + 50.0;
                    for detail in [input_block.as_ref(), output_block.as_ref()]
                        .into_iter()
                        .flatten()
                    {
                        if detail.collapsible {
                            let rect = Rect::new(bx + 12.0, ty + detail.height - 30.0, 126.0, 24.0);
                            if let Some(slot) = self.runtime_action_rects.get_mut(runtime_slot)
                                && Self::rect_is_visible(area, rect)
                            {
                                *slot = Some(rect);
                            }
                            runtime_slot += 1;
                        }
                        ty += detail.height;
                    }
                    let _ = bubble_w;
                    *bubble_h
                }
            };
            y += height + 10.0;
        }
    }
}

impl Widget for TranscriptControls {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        self.sync_controls();
        let size = constraints.max;
        let area = Rect::from_origin_size(Point::ZERO, size);
        self.empty_body_rect = Rect::ZERO;
        self.empty_action_rects.fill(None);

        if let Some(empty) = &self.signature.empty {
            self.empty_body_rect = match empty {
                TranscriptEmptyControlState::Standard { .. } => area,
                TranscriptEmptyControlState::Recovery { .. } => {
                    TranscriptView::recovery_card_rect(area)
                }
            };
            if let Some(body) = &mut self.empty_body {
                Self::measure_pod_for_rect(body, ctx, self.empty_body_rect);
            }
            let rects = match empty {
                TranscriptEmptyControlState::Standard { actions, .. } => {
                    TranscriptView::quick_action_rects(area, actions)
                        .into_iter()
                        .map(|(_, rect)| rect)
                        .collect::<Vec<_>>()
                }
                TranscriptEmptyControlState::Recovery {
                    target, actions: _, ..
                } => {
                    let chat = self.shared.chat();
                    TranscriptView::recovery_action_rects(area, *target, &chat)
                        .into_iter()
                        .map(|(_, rect)| rect)
                        .collect::<Vec<_>>()
                }
            };
            for (slot, rect) in self.empty_action_rects.iter_mut().zip(rects) {
                *slot = Some(rect);
            }
        } else {
            self.compute_nonempty_rects(ctx, area);
        }

        for (pod, rect) in self
            .empty_actions
            .iter_mut()
            .zip(self.empty_action_rects.iter().copied())
            .chain(
                self.message_actions
                    .iter_mut()
                    .zip(self.message_action_rects.iter().copied()),
            )
            .chain(
                self.runtime_actions
                    .iter_mut()
                    .zip(self.runtime_action_rects.iter().copied()),
            )
        {
            if let Some(rect) = rect {
                Self::measure_pod_for_rect(pod, ctx, rect);
            } else {
                pod.measure(ctx, Constraints::tight(Size::ZERO));
            }
        }
        size
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        let translate = bounds.origin.to_vector();
        if let Some(body) = &mut self.empty_body {
            body.arrange(ctx, self.empty_body_rect.translate(translate));
        }
        for (pod, rect) in self
            .empty_actions
            .iter_mut()
            .zip(self.empty_action_rects.iter().copied())
            .chain(
                self.message_actions
                    .iter_mut()
                    .zip(self.message_action_rects.iter().copied()),
            )
            .chain(
                self.runtime_actions
                    .iter_mut()
                    .zip(self.runtime_action_rects.iter().copied()),
            )
        {
            pod.arrange(
                ctx,
                rect.map(|rect| rect.translate(translate))
                    .unwrap_or(Rect::ZERO),
            );
        }
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        if let Some(body) = &self.empty_body {
            body.paint(ctx);
        }
        for (pod, rect) in self
            .empty_actions
            .iter()
            .zip(self.empty_action_rects.iter())
            .chain(self.message_actions.iter().zip(&self.message_action_rects))
            .chain(self.runtime_actions.iter().zip(&self.runtime_action_rects))
        {
            if rect.is_some() {
                pod.paint(ctx);
            }
        }
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        if let Some(body) = &self.empty_body {
            body.semantics(ctx);
        }
        for (pod, rect) in self
            .empty_actions
            .iter()
            .zip(self.empty_action_rects.iter())
            .chain(self.message_actions.iter().zip(&self.message_action_rects))
            .chain(self.runtime_actions.iter().zip(&self.runtime_action_rects))
        {
            if rect.is_some() {
                pod.semantics(ctx);
            }
        }
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        if let Some(body) = &self.empty_body {
            visitor.visit(body);
        }
        for (pod, rect) in self
            .empty_actions
            .iter()
            .zip(self.empty_action_rects.iter())
            .chain(self.message_actions.iter().zip(&self.message_action_rects))
            .chain(self.runtime_actions.iter().zip(&self.runtime_action_rects))
        {
            if rect.is_some() {
                visitor.visit(pod);
            }
        }
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        if let Some(body) = &mut self.empty_body {
            visitor.visit(body);
        }
        for (pod, rect) in self
            .empty_actions
            .iter_mut()
            .zip(self.empty_action_rects.iter())
            .chain(
                self.message_actions
                    .iter_mut()
                    .zip(&self.message_action_rects),
            )
            .chain(
                self.runtime_actions
                    .iter_mut()
                    .zip(&self.runtime_action_rects),
            )
        {
            if rect.is_some() {
                visitor.visit(pod);
            }
        }
    }
}

impl Widget for TranscriptView {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        match event {
            Event::Pointer(pointer) if pointer.kind == PointerEventKind::Move => {
                if self.dragging_selection.get()
                    && pointer.buttons.contains(PointerButton::Primary)
                    && let Some(focus) = self.nearest_text_point(pointer.position)
                {
                    let anchor = self
                        .selection
                        .get()
                        .map(|(anchor, _)| anchor)
                        .unwrap_or(focus);
                    self.set_selection(ctx, Some((anchor, focus)));
                }
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Down
                    && pointer.button == Some(PointerButton::Primary) =>
            {
                if let Some(point) = self.text_point_at(pointer.position) {
                    // Start a character-granular selection drag on rich text.
                    self.dragging_selection.set(true);
                    self.set_selection(ctx, Some((point, point)));
                    ctx.request_pointer_capture(pointer.pointer_id);
                    ctx.request_focus();
                    ctx.set_handled();
                } else {
                    self.clear_selection(ctx);
                }
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Up
                    && pointer.button == Some(PointerButton::Primary) =>
            {
                if self.dragging_selection.take() {
                    ctx.release_pointer_capture(pointer.pointer_id);
                    ctx.set_handled();
                }
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Down
                    && pointer.button == Some(PointerButton::Secondary)
                    && self.bounds.get().contains(pointer.position) =>
            {
                // Remember the bubble under the right-click for the context
                // menu's "Copy Message"; the wrapping menu opens on the same
                // press (in its bubble phase), so stay unhandled here.
                self.menu_target_key
                    .set(self.message_key_at(pointer.position));
                if !ctx.is_focused() {
                    ctx.request_focus();
                }
            }
            Event::Pointer(pointer)
                if matches!(
                    pointer.kind,
                    PointerEventKind::Leave | PointerEventKind::Cancel
                ) =>
            {
                if pointer.kind == PointerEventKind::Cancel && self.dragging_selection.take() {
                    ctx.release_pointer_capture(pointer.pointer_id);
                    ctx.set_handled();
                }
            }
            Event::Pointer(p)
                if p.kind == PointerEventKind::Scroll && self.bounds.get().contains(p.position) =>
            {
                if let Some(delta) = p.scroll_delta {
                    let dy = match delta {
                        ScrollDelta::Lines(v) => v.y * 48.0,
                        ScrollDelta::Pixels(v) => v.y,
                    };
                    let max = (self.content_height.get() - self.viewport_height.get()).max(0.0);
                    let mut chat = self.shared.chat_mut();
                    chat.scroll = (chat.scroll - dy).clamp(0.0, max);
                    chat.stick_bottom = chat.scroll >= max - 2.0;
                    if chat.scroll <= TRANSCRIPT_OLDER_LOAD_THRESHOLD_PX {
                        chat.maybe_start_older_transcript_load();
                    } else if max - chat.scroll <= TRANSCRIPT_OLDER_LOAD_THRESHOLD_PX {
                        chat.maybe_start_newer_transcript_load();
                    }
                    drop(chat);
                    Self::request_recovery_action_refresh(ctx);
                }
            }
            Event::Keyboard(key) if ctx.is_focused() && key.state == KeyState::Pressed => {
                let command_modifier = key.modifiers.control || key.modifiers.meta;
                match key.key.as_str() {
                    "c" | "C" if command_modifier => {
                        if self.copy_selection_to_clipboard(ctx) {
                            ctx.set_handled();
                        }
                    }
                    "a" | "A" if command_modifier => {
                        self.select_all_text(ctx);
                        ctx.set_handled();
                    }
                    "Escape" if self.selection.get().is_some() => {
                        self.clear_selection(ctx);
                        ctx.set_handled();
                    }
                    _ => {}
                }
            }
            Event::Custom(custom) => {
                if let Some(command) = TextCommand::from_custom_event(custom) {
                    match command {
                        TextCommand::Copy => {
                            if self.copy_selection_to_clipboard(ctx) {
                                ctx.set_handled();
                            }
                        }
                        TextCommand::SelectAll => {
                            self.select_all_text(ctx);
                            ctx.set_handled();
                        }
                        // The transcript is read-only.
                        TextCommand::Cut | TextCommand::Paste => {}
                    }
                } else if custom.kind == CHAT_COPY_MESSAGE_EVENT {
                    if self.copy_message_to_clipboard(ctx) {
                        ctx.set_handled();
                    }
                }
            }
            _ => {}
        }
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let size = constraints.max;
        self.controls.measure(ctx, Constraints::tight(size));
        size
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        self.bounds.set(bounds);
        self.controls.arrange(ctx, bounds);
    }

    fn accepts_focus(&self) -> bool {
        // Focusable so Ctrl+C / Ctrl+A act on the transcript selection.
        true
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let theme = self.shared.theme();
        let pal = palette(&self.shared);
        let area = ctx.bounds();
        self.bounds.set(area);
        self.viewport_height.set(area.height());

        let chat = self.shared.chat();
        if chat.messages.is_empty() {
            self.transcript_text_hits.borrow_mut().clear();
            self.transcript_message_sources.borrow_mut().clear();
            self.transcript_message_semantics.borrow_mut().clear();
            self.selection.set(None);
            self.controls.paint(ctx);
            return;
        }

        // Selection lives in the shared scope: when another owner (the
        // composer) replaces it, drop the transcript highlight.
        if self.selection.get().is_some()
            && !self.selection_scope.has_owner_selection(ctx.widget_id())
        {
            self.selection.set(None);
        }

        ctx.push_clip_rect(area);

        let layout_mode = TranscriptLayoutMode::for_width(area.width());
        let max_bubble = match layout_mode {
            TranscriptLayoutMode::Row => (area.width() - PAD * 2.0 - 34.0).clamp(260.0, 760.0),
            TranscriptLayoutMode::Bubble => (area.width() * 0.85).clamp(200.0, 620.0),
        };
        let inner_w = max_bubble - 24.0;
        let message_gap = layout_mode.message_gap();
        let mut laid: Vec<TranscriptLaidMessage> = Vec::with_capacity(chat.messages.len());
        let mut total = PAD;
        let timeline_summary = chat.timeline.visible_summary(chat.messages.len());
        let timeline_header_h = Self::timeline_header_height(&chat);
        total += timeline_header_h;
        for msg in &chat.messages {
            let item = Self::layout_message(ctx, &pal, &theme, msg, inner_w);
            total += item.bubble_h + message_gap;
            laid.push(item);
        }
        let tool_layout = Self::tool_trace_layout(ctx, &pal, &theme, &chat, inner_w, max_bubble);
        for item in &tool_layout {
            let height = match item {
                TranscriptLaidTool::ChainCollapsed { bubble_h, .. }
                | TranscriptLaidTool::ChainHeader { bubble_h, .. }
                | TranscriptLaidTool::Call { bubble_h, .. } => *bubble_h,
            };
            total += height + 10.0;
        }
        total += PAD;
        self.content_height.set(total);

        let max_scroll = (total - area.height()).max(0.0);
        let scroll = if chat.stick_bottom {
            max_scroll
        } else {
            chat.scroll.clamp(0.0, max_scroll)
        };

        let message_sources = chat
            .messages
            .iter()
            .zip(&laid)
            .enumerate()
            .map(|(index, (message, item))| TranscriptMessageSource {
                message_key: transcript_message_selection_key(index, message),
                source_map: item.source_map.clone(),
            })
            .collect();
        *self.transcript_message_sources.borrow_mut() = message_sources;

        // Retain rich-text geometry for every message, including content that
        // is currently scrolled away, so drag extension and Select All keep a
        // stable character range across the full transcript.
        let mut text_hits = Vec::new();
        {
            let mut record_y = area.y() + PAD - scroll + timeline_header_h;
            for (index, (message, item)) in chat.messages.iter().zip(&laid).enumerate() {
                Self::record_message_text_hits(
                    &mut text_hits,
                    area,
                    item,
                    record_y,
                    layout_mode,
                    transcript_message_selection_key(index, message),
                );
                record_y += item.bubble_h + message_gap;
            }
        }
        *self.transcript_text_hits.borrow_mut() = text_hits;

        // Draw pass: only mutable ctx ops here. Keep a bounded semantic
        // projection beside the visible paint rows so native accessibility
        // receives the same message text and geometry users see.
        let mut message_semantics = Vec::new();
        let mut y = area.y() + PAD - scroll;
        if let Some(summary) = timeline_summary.as_deref() {
            if y + timeline_header_h >= area.y() && y <= area.y() + area.height() {
                self.paint_timeline_header(ctx, &theme, &pal, area, y, summary);
            }
            y += timeline_header_h;
        }
        for (index, (message, item)) in chat.messages.iter().zip(&laid).enumerate() {
            if y + item.bubble_h >= area.y() && y <= area.y() + area.height() {
                let (row_bounds, _, _, _) =
                    Self::message_content_geometry(area, item, y, layout_mode);
                let speaker = match message.source {
                    ChatMessageSource::User => "You".to_string(),
                    ChatMessageSource::System => "System".to_string(),
                    ChatMessageSource::Model => chat
                        .agents
                        .iter()
                        .find(|agent| agent.key == chat.info.agent || agent.name == chat.info.agent)
                        .map(|agent| agent.name.clone())
                        .filter(|name| !name.trim().is_empty())
                        .unwrap_or_else(|| {
                            if chat.info.agent.trim().is_empty() {
                                "Sinomo".to_string()
                            } else {
                                chat.info.agent.clone()
                            }
                        }),
                };
                message_semantics.push(TranscriptMessageSemantic {
                    message_key: transcript_message_selection_key(index, message),
                    bounds: row_bounds,
                    speaker,
                    text: transcript_message_display_text(message),
                    position: index + 1,
                    total: chat.messages.len(),
                    busy: chat.streaming
                        && index + 1 == chat.messages.len()
                        && message.source == ChatMessageSource::Model,
                });
                self.paint_laid_message(
                    ctx,
                    &pal,
                    &theme,
                    area,
                    item,
                    y,
                    layout_mode,
                    transcript_message_selection_key(index, message),
                );
            }
            y += item.bubble_h + message_gap;
        }
        for item in &tool_layout {
            let height = match item {
                TranscriptLaidTool::ChainCollapsed { bubble_h, .. }
                | TranscriptLaidTool::ChainHeader { bubble_h, .. }
                | TranscriptLaidTool::Call { bubble_h, .. } => *bubble_h,
            };
            if y + height >= area.y() && y <= area.y() + area.height() {
                self.paint_tool_item(ctx, &pal, &theme, area, item, y);
            }
            y += height + 10.0;
        }
        *self.transcript_message_semantics.borrow_mut() = message_semantics;
        ctx.pop_clip();
        self.controls.paint(ctx);

        // Streaming / typing notice, pinned to the bottom of the transcript.
        if chat.streaming
            && let Some(notice) = &chat.notice
        {
            paint_text_cell(
                ctx,
                &theme,
                Rect::new(
                    area.x() + PAD,
                    area.y() + area.height() - 22.0,
                    area.width() - PAD * 2.0,
                    18.0,
                ),
                notice,
                TextCellPaint::new()
                    .text_token(theme.text.xs)
                    .weight(FontWeight::MEDIUM)
                    .color(pal.text_faint)
                    .padding(0.0, 0.0),
            );
        }
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let (messages_empty, streaming) = {
            let chat = self.shared.chat();
            (chat.messages.is_empty(), chat.streaming)
        };
        let visible_message_count = self.transcript_message_semantics.borrow().len();
        let mut transcript = SemanticsNode::new(ctx.widget_id(), SemanticsRole::List, ctx.bounds());
        transcript.name = Some("Conversation messages".to_string());
        transcript.description = Some(if messages_empty {
            "No visible messages; conversation actions are available below".to_string()
        } else {
            format!(
                "{} visible message{}; use the transcript for selection and copy",
                visible_message_count,
                if visible_message_count == 1 { "" } else { "s" }
            )
        });
        transcript.state.focused = ctx.is_focused();
        transcript.state.busy = streaming;
        transcript.actions = if messages_empty {
            vec![SemanticsAction::Focus]
        } else {
            vec![
                SemanticsAction::Focus,
                SemanticsAction::SetSelection,
                SemanticsAction::Copy,
            ]
        };
        ctx.push(transcript);
        if messages_empty {
            // `EmptyState` exposes its copy as one descriptive container. Keep
            // the individual title, guidance, and context lines discoverable
            // as text as well, without synthesizing any actionable semantics.
            let standard_copy = {
                let chat = self.shared.chat();
                chat.runtime_recovery_target().is_none().then(|| {
                    (
                        chat.empty_conversation_title(),
                        chat.empty_conversation_hint(),
                        chat.empty_conversation_context(),
                    )
                })
            };
            if let Some((title, hint, context)) = standard_copy {
                let (cx, cy) = Self::empty_state_center(ctx.bounds());
                let text_width = (ctx.bounds().width() - PAD * 4.0).max(120.0).min(560.0);
                let mut text_nodes = vec![(title, cy + 2.0, 22.0_f32), (hint, cy + 26.0, 18.0)];
                if let Some(context) = context {
                    text_nodes.push((context, cy + 44.0, 18.0));
                }
                for (slot, (name, y, height)) in text_nodes.into_iter().enumerate() {
                    let mut node = SemanticsNode::new(
                        Self::empty_text_widget_id(ctx.widget_id(), slot),
                        SemanticsRole::Text,
                        Rect::new(cx - text_width / 2.0, y, text_width, height),
                    );
                    node.parent = Some(ctx.widget_id());
                    node.name = Some(name);
                    ctx.push(node);
                }
            }
            self.controls.semantics(ctx);
            return;
        }

        let notice = {
            let chat = self.shared.chat();
            chat.notice.clone()
        };
        let semantics = self.transcript_message_semantics.borrow();
        for message in semantics.iter() {
            let mut node = SemanticsNode::new(
                Self::message_widget_id(ctx.widget_id(), message.message_key),
                SemanticsRole::ListItem,
                message.bounds,
            );
            node.parent = Some(ctx.widget_id());
            node.name = Some(format!("{}: {}", message.speaker, message.text));
            node.description = Some(format!(
                "Message {} of {}{}",
                message.position,
                message.total,
                if message.busy {
                    "; response in progress"
                } else {
                    ""
                }
            ));
            node.value = Some(SemanticsValue::Text(message.text.clone()));
            node.state.busy = message.busy;
            ctx.push(node);
        }
        drop(semantics);
        if streaming {
            let progress_text = notice
                .filter(|notice| !notice.trim().is_empty())
                .unwrap_or_else(|| "Agent response in progress".to_string());
            let mut progress = SemanticsNode::new(
                Self::progress_widget_id(ctx.widget_id()),
                SemanticsRole::BusyIndicator,
                Rect::new(
                    ctx.bounds().x() + PAD,
                    ctx.bounds().max_y() - 24.0,
                    (ctx.bounds().width() - PAD * 2.0).max(1.0),
                    22.0,
                ),
            );
            progress.parent = Some(ctx.widget_id());
            progress.name = Some(progress_text.clone());
            progress.value = Some(SemanticsValue::Text(progress_text));
            progress.state.busy = true;
            ctx.push(progress);
        }
        let timeline_summary = {
            let chat = self.shared.chat();
            chat.timeline.visible_summary(chat.messages.len())
        };
        if let Some(summary) = timeline_summary {
            let rect = Rect::new(
                ctx.bounds().x() + PAD,
                ctx.bounds().y() + PAD,
                (ctx.bounds().width() - PAD * 2.0).max(0.0),
                26.0,
            );
            let mut node = SemanticsNode::new(
                Self::timeline_header_widget_id(ctx.widget_id()),
                SemanticsRole::Text,
                rect,
            );
            node.parent = Some(ctx.widget_id());
            node.name = Some(summary);
            ctx.push(node);
        }
        self.controls.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.controls);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.controls);
    }
}

// ---------------------------------------------------------------------------
// Composer — provider / model / effort `Select`s, the message `TextArea`, and a
// Send `Button`. The model `Select` (and effort visibility) depend on the
// provider + fetched model list, so the selector row is wrapped in a
// `RebuildOnChange` keyed on the selection signature.
// ---------------------------------------------------------------------------

/// The selector row signature: provider index, model index, model count, effort visibility, and a
/// loading flag — anything that changes the *set* of options or which selectors are shown. (The
/// selected-index reactivity is handled by each `Select`'s `selected_when`; this only drives the
/// rebuild when the option lists themselves change.)
fn selector_signature(shared: &Shared) -> (usize, usize, usize, bool, bool, bool, String, String) {
    let chat = shared.chat();
    (
        chat.provider_idx,
        chat.providers.len(),
        chat.current_models().len(),
        chat.effort_visible(),
        chat.models_loading,
        chat.shows_recovery_empty_card(),
        chat.providers
            .iter()
            .map(|provider| format!("{}@{}:{}", provider.id, provider.account, provider.name))
            .collect::<Vec<_>>()
            .join("\u{1f}"),
        chat.runtime_recovery_target()
            .map(RuntimeRecoveryTarget::key)
            .unwrap_or("none")
            .to_string(),
    )
}

/// Build the provider / model / effort selector row for the current selection.
fn selectors(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    if shared.chat().providers.is_empty() {
        if shared.chat().providers_loading {
            let s = shared.clone();
            let row = Stack::horizontal()
                .spacing(8.0)
                .alignment(Alignment::Center)
                .with_child(
                    SizedBox::new().height(SELECTOR_H).with_child(
                        small_label(shared, "Loading providers...")
                            .color_when(move || palette(&s).text_faint),
                    ),
                );
            return WidgetPod::new(row);
        }

        if shared.chat().shows_recovery_empty_card() {
            let recovery_target = shared.chat().runtime_recovery_target();
            let s = shared.clone();
            let mut row = Stack::horizontal()
                .spacing(8.0)
                .alignment(Alignment::Center)
                .with_child(
                    SizedBox::new().height(SELECTOR_H).with_child(
                        small_label(shared, "Provider setup required")
                            .color_when(move || palette(&s).text_faint),
                    ),
                );
            if recovery_target == Some(RuntimeRecoveryTarget::Connection) {
                row = row.with_child(provider_setup_button(shared, reader));
            }
            return WidgetPod::new(row);
        }

        let row = Stack::horizontal()
            .spacing(8.0)
            .alignment(Alignment::Center)
            .with_child(provider_setup_button(shared, reader));
        return WidgetPod::new(row);
    }

    let provider = {
        let s = shared.clone();
        let names: Vec<String> = shared
            .chat()
            .providers
            .iter()
            .map(|p| p.name.clone())
            .collect();
        sui::Select::new("chat-provider")
            .options(names)
            .placeholder("Provider")
            .theme_when(composer_control_reader(reader))
            .selected_when({
                let s = s.clone();
                move || Some(s.chat().provider_idx)
            })
            .on_change(move |index, _label| {
                s.chat_mut().set_provider(index);
            })
    };

    let model = {
        let s = shared.clone();
        let models = shared.chat().current_models();
        let loading = shared.chat().models_loading;
        let placeholder = if loading {
            "Loading models…"
        } else if models.is_empty() {
            "No models"
        } else {
            "Model"
        };
        sui::Select::new("chat-model")
            .options(models)
            .placeholder(placeholder)
            .theme_when(composer_control_reader(reader))
            .selected_when({
                let s = s.clone();
                move || Some(s.chat().model_idx)
            })
            .on_change(move |index, _label| {
                s.chat_mut().set_model(index);
            })
    };

    let mut row = Stack::horizontal()
        .spacing(8.0)
        .alignment(Alignment::Center)
        .with_child(SizedBox::new().height(SELECTOR_H).with_child(provider))
        .with_child(SizedBox::new().height(SELECTOR_H).with_child(model));

    if shared.chat().effort_visible() {
        let s = shared.clone();
        let efforts = shared.chat().efforts.clone();
        let effort = sui::Select::new("chat-effort")
            .options(efforts)
            .placeholder("Effort")
            .theme_when(composer_control_reader(reader))
            .selected_when({
                let s = s.clone();
                move || Some(s.chat().effort_idx)
            })
            .on_change(move |index, _label| {
                s.chat_mut().set_effort(index);
            });
        row = row.with_child(SizedBox::new().height(SELECTOR_H).with_child(effort));
    }

    if !shared.chat().shows_recovery_empty_card() {
        if let Some(action) = retry_chat_list_button(shared, reader) {
            row = row.with_child(action);
        }

        if let Some(action) = check_sifs_button(shared, reader) {
            row = row.with_child(action);
        }

        if let Some(action) = runtime_recovery_button(shared, reader) {
            row = row.with_child(action);
        }
    }

    WidgetPod::new(row)
}

fn provider_setup_button(shared: &Shared, reader: &ThemeReader) -> SizedBox {
    let s = shared.clone();
    SizedBox::new().height(SELECTOR_H).with_child(
        sui::Button::new("Set up provider")
            .icon(IconGlyph::Add)
            .theme_when(composer_control_reader(reader))
            .on_press(move || open_provider_setup(&s)),
    )
}

fn runtime_recovery_button(shared: &Shared, reader: &ThemeReader) -> Option<SizedBox> {
    let target = shared.chat().runtime_recovery_target()?;
    let s = shared.clone();
    Some(
        SizedBox::new().height(SELECTOR_H).with_child(
            sui::Button::new(target.footer_label())
                .icon(IconGlyph::Alert)
                .min_width(target.footer_min_width())
                .theme_when(composer_control_reader(reader))
                .on_press(move || open_settings_section(&s, target.settings_section())),
        ),
    )
}

fn retry_chat_list_button(shared: &Shared, reader: &ThemeReader) -> Option<SizedBox> {
    let chat = shared.chat();
    let target = chat.runtime_recovery_target()?;
    if target != RuntimeRecoveryTarget::Connection {
        return None;
    }
    if !chat.should_offer_chat_list_retry() {
        return None;
    }
    let s = shared.clone();
    Some(
        SizedBox::new().height(SELECTOR_H).with_child(
            sui::Button::new("Retry chat list")
                .icon(IconGlyph::Redo)
                .min_width(132.0)
                .theme_when(composer_control_reader(reader))
                .on_press(move || {
                    let waker = { s.borrow().waker.clone() };
                    s.chat_mut().retry_chat_list_with_waker(waker);
                }),
        ),
    )
}

fn check_sifs_button(shared: &Shared, reader: &ThemeReader) -> Option<SizedBox> {
    let chat = shared.chat();
    let target = chat.runtime_recovery_target()?;
    if target != RuntimeRecoveryTarget::Connection {
        return None;
    }
    if !chat.should_offer_sifs_check() {
        return None;
    }
    let s = shared.clone();
    Some(
        SizedBox::new().height(SELECTOR_H).with_child(
            sui::Button::new("Check SIFS")
                .icon(IconGlyph::Storage)
                .min_width(120.0)
                .theme_when(composer_control_reader(reader))
                .on_press(move || open_files_tab(&s)),
        ),
    )
}

fn open_settings_section(shared: &Shared, section: SettingsSection) {
    let mut app = shared.borrow_mut();
    app.active_tab = Tab::Settings;
    if let Some(ready) = app.ready.as_mut() {
        ready.settings.open(section);
    }
}

fn open_files_tab(shared: &Shared) {
    shared.borrow_mut().active_tab = Tab::Files;
}

fn open_provider_setup(shared: &Shared) {
    let mut app = shared.borrow_mut();
    app.active_tab = Tab::Settings;
    if let Some(ready) = app.ready.as_mut() {
        ready.settings.open_provider_add();
    }
}

fn attachment_signature(
    shared: &Shared,
) -> (u64, usize, bool, bool, bool, bool, bool, Option<String>) {
    let chat = shared.chat();
    (
        chat.draft_revision,
        chat.attachments.len(),
        chat.attachment_controls_visible(),
        chat.attachment_manual_path_open,
        chat.attachment_operation_pending(),
        chat.attachment_picker_lane_pending(),
        chat.attachment_drop_hovered,
        chat.attachment_picker_error.clone(),
    )
}

fn attachment_drawer_chip(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    attachment: &PendingChatAttachment,
) -> impl Widget + use<> {
    let label = format!(
        "{} · {}",
        compact_line(&attachment.file_name, 22),
        format_bytes(attachment.size_bytes.min(usize::MAX as u64) as usize)
    );
    let remove_label = format!("Remove {}", attachment.file_name);
    let remove = {
        let s = shared.clone();
        IconButton::new(IconGlyph::Close, remove_label)
            .size(ATTACHMENT_CHIPS_H)
            .icon_size(12.0)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .on_press(move || {
                s.chat_mut().remove_attachment(index);
            })
    };
    let s = shared.clone();
    Surface::field(
        Flex::horizontal()
            .main_gap(4.0)
            .align_items(Alignment::Center)
            .with_item(
                small_label(shared, label).color_when(move || palette(&s).text_muted),
                FlexItem::flex(1.0).min_width(48.0),
            )
            .with_child(remove),
    )
    .radius(999.0)
    .padding(Insets {
        left: 8.0,
        top: 0.0,
        right: 3.0,
        bottom: 0.0,
    })
    .theme_when(clone_reader(reader))
    .appearance(SurfaceAppearance::Raised)
    .tone(SemanticTone::Neutral)
}

fn attachment_controls(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let mut chips = Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .gap(6.0)
        .align_items(Alignment::Center);
    for (index, attachment) in shared.chat().attachments.iter().enumerate() {
        chips = chips.with_child(attachment_drawer_chip(shared, reader, index, attachment));
    }

    let picker_pending = shared.chat().attachment_picker_pending();
    let picker_lane_pending = shared.chat().attachment_picker_lane_pending();
    let choose = {
        let s = shared.clone();
        sui::Button::new(if picker_pending {
            "Choosing files…"
        } else if picker_lane_pending {
            "Close previous picker…"
        } else {
            "Choose files"
        })
        .icon(IconGlyph::Attach)
        .min_height(ATTACHMENT_ENTRY_H)
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Filled)
        .tone(SemanticTone::Accent)
        .enabled_when({
            let s = s.clone();
            move || !s.chat().attachment_picker_lane_pending()
        })
        .on_press_with_ctx(move |ctx| {
            s.chat_mut().request_attachment_picker();
            request_chat_window_relayout(ctx);
        })
    };
    let manual_path_open = shared.chat().attachment_manual_path_open;
    let manual = {
        let s = shared.clone();
        sui::Button::new(if manual_path_open {
            "Hide path entry"
        } else {
            "Enter path manually"
        })
        .icon(IconGlyph::File)
        .min_height(ATTACHMENT_ENTRY_H)
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .on_press_with_ctx(move |ctx| {
            s.chat_mut().toggle_manual_attachment_path();
            request_chat_window_relayout(ctx);
        })
    };
    let actions = Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .gap(8.0)
        .align_items(Alignment::Center)
        .with_child(choose)
        .with_child(manual);

    let hint = {
        let s = shared.clone();
        small_label(
            shared,
            "Choose one or more files with the system picker. Manual paths are a fallback.",
        )
        .color_when(move || palette(&s).text_muted)
    };
    let mut body = Stack::vertical()
        .spacing(8.0)
        .alignment(Alignment::Stretch)
        .with_child(actions)
        .with_child(hint);

    if shared.chat().attachment_drop_hovered {
        body = body.with_child(
            Stack::horizontal().with_child(
                StatusBadge::new("Drop files to attach")
                    .icon(IconGlyph::Attach)
                    .tone(SemanticTone::Info)
                    .theme_when(clone_reader(reader)),
            ),
        );
    }

    let pending_validations = shared.chat().pending_attachment_validations.clone();
    if !pending_validations.is_empty() {
        let mut pending_chips = Flex::horizontal()
            .wrap(FlexWrap::Wrap)
            .gap(6.0)
            .align_items(Alignment::Center);
        for pending in pending_validations {
            let ingress = match pending.ingress {
                LocalAttachmentIngress::NativePicker => "Checking",
                LocalAttachmentIngress::ManualPath => "Checking path",
                LocalAttachmentIngress::OsDrop => "Checking drop",
            };
            for path in pending.paths {
                let name = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .filter(|name| !name.trim().is_empty())
                    .unwrap_or("file");
                pending_chips = pending_chips.with_child(
                    StatusBadge::new(format!("{ingress} {}…", compact_line(name, 24)))
                        .icon(IconGlyph::File)
                        .tone(SemanticTone::Info)
                        .theme_when(clone_reader(reader)),
                );
            }
        }
        body = body.with_child(pending_chips);
    }

    if manual_path_open {
        let path_input = {
            let s = shared.clone();
            TextInput::new("chat-attachment-path")
                .value(shared.chat().attachment_path_input.clone())
                .placeholder("Enter a file path")
                .leading_icon(IconGlyph::File)
                .theme_when(composer_control_reader(reader))
                .on_change(move |value| {
                    s.chat_mut().set_attachment_path_input(value);
                })
        };
        let add = {
            let s = shared.clone();
            sui::Button::new("Add path")
                .icon(IconGlyph::Add)
                .min_height(ATTACHMENT_ENTRY_H)
                .min_width(96.0)
                .theme_when(clone_reader(reader))
                .appearance(sui::ButtonAppearance::Tonal)
                .tone(SemanticTone::Neutral)
                .enabled_when({
                    let s = s.clone();
                    move || !s.chat().attachment_path_input.trim().is_empty()
                })
                .on_press_with_ctx(move |ctx| {
                    s.chat_mut().add_attachment_from_path_input();
                    request_chat_window_relayout(ctx);
                })
        };
        body = body.with_child(
            Flex::horizontal()
                .wrap(FlexWrap::Wrap)
                .gap(8.0)
                .align_items(Alignment::Center)
                .with_item(path_input, FlexItem::flex(1.0).min_width(180.0))
                .with_child(add),
        );
    }

    if let Some(error) = shared.chat().attachment_picker_error.clone() {
        let s = shared.clone();
        body = body.with_child(
            Flex::horizontal()
                .wrap(FlexWrap::Wrap)
                .gap(8.0)
                .align_items(Alignment::Center)
                .with_child(
                    StatusBadge::new("Couldn’t attach")
                        .icon(IconGlyph::Alert)
                        .tone(SemanticTone::Danger)
                        .theme_when(clone_reader(reader)),
                )
                .with_item(
                    small_label(shared, error).color_when(move || palette(&s).danger_text),
                    FlexItem::flex(1.0).min_width(140.0),
                ),
        );
    }

    if !shared.chat().attachments.is_empty() {
        body = body.with_child(chips);
    }

    WidgetPod::new(body)
}

fn model_button_label(chat: &ChatState) -> String {
    let model = chat.current_model();
    if !model.is_empty() {
        return compact_line(&model, 26);
    }

    if let Some(provider) = chat.current_provider() {
        return format!("{} model", compact_line(&provider.name, 18));
    }

    "Model".to_string()
}

/// The live-voice hint beside the mic toggle (`voice link · MM:SS`), present
/// only while the full-duplex call is running.
fn voice_link_hint(chat: &ChatState) -> Option<String> {
    chat.media_call
        .active
        .then(|| format!("voice link · {}", chat.media_call_elapsed_label()))
}

/// The action bar rebuild key: the Send<->Stop swap, the quiet-control labels,
/// and the live voice state (whose hint ticks once a second).
fn composer_bar_signature(
    shared: &Shared,
) -> (
    bool,
    String,
    String,
    String,
    bool,
    Option<String>,
    bool,
    bool,
) {
    let chat = shared.chat();
    (
        chat.streaming,
        model_button_label(&chat),
        context_overview_summary(&chat),
        activity_button_label(&chat).to_string(),
        chat.media_call.active,
        voice_link_hint(&chat),
        chat.can_reuse_latest_answer(),
        chat.providers.is_empty(),
    )
}

fn context_popup_signature(shared: &Shared) -> (String, usize, usize, usize, bool) {
    let chat = shared.chat();
    (
        context_overview_summary(&chat),
        chat.context
            .as_ref()
            .map(|context| context.total_bytes)
            .unwrap_or_default(),
        chat.context
            .as_ref()
            .map(|context| context.included_count)
            .unwrap_or_default(),
        chat.context
            .as_ref()
            .map(|context| context.omitted_count)
            .unwrap_or_default(),
        chat.streaming,
    )
}

fn close_popup_button(shared: &Shared, reader: &ThemeReader, label: &'static str) -> IconButton {
    let s = shared.clone();
    IconButton::new(IconGlyph::Close, label)
        .size(26.0)
        .icon_size(13.0)
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .on_press_with_ctx(move |ctx| {
            s.chat_mut().set_composer_popup(None);
            request_chat_window_relayout(ctx);
        })
}

fn popup_header(
    shared: &Shared,
    reader: &ThemeReader,
    title: &'static str,
    close_label: &'static str,
) -> impl Widget + use<> {
    Flex::horizontal()
        .align_items(Alignment::Center)
        .with_child(dialog_section_label(shared, title))
        .spacer()
        .with_child(close_popup_button(shared, reader, close_label))
}

fn composer_model_popup(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let selector_host = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        RebuildOnChange::new(
            {
                let s = s.clone();
                move || selector_signature(&s)
            },
            move |_sig| selectors(&s, &r),
        )
    };
    let detail = {
        let chat = shared.chat();
        if chat.models_loading {
            "Refreshing available models from the provider".to_string()
        } else if let Some(provider) = chat.current_provider() {
            format!(
                "{} models available for {}",
                chat.current_models().len(),
                provider.name
            )
        } else {
            "Choose a provider before sending.".to_string()
        }
    };
    let body = Stack::vertical()
        .spacing(8.0)
        .alignment(Alignment::Stretch)
        .with_child(popup_header(
            shared,
            reader,
            "Model selector",
            "Close model selector",
        ))
        .with_child(SizedBox::new().height(SELECTOR_H).with_child(selector_host))
        .with_child(muted_label(shared, detail));
    WidgetPod::new(
        Surface::new(
            SurfaceRole::Titlebar,
            WidgetPadding::horizontal(PAD, body).fill_child_height(),
        )
        .fill()
        .theme_when(clone_reader(reader)),
    )
}

fn composer_context_popup(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let content = {
        let s = shared.clone();
        RebuildOnChange::new(
            {
                let s = s.clone();
                move || context_popup_signature(&s)
            },
            move |_sig| {
                let chat = s.chat();
                let mut body = Stack::vertical()
                    .spacing(5.0)
                    .alignment(Alignment::Stretch)
                    .with_child(muted_label(&s, context_overview_summary(&chat)));
                if let Some(context) = &chat.context {
                    body = body
                        .with_child(muted_label(
                            &s,
                            format!(
                                "Step {} · {} / {}",
                                context.step_index,
                                compact_line(&context.provider, 18),
                                compact_line(&context.model, 28)
                            ),
                        ))
                        .with_child(muted_label(
                            &s,
                            format!(
                                "{} included · {} omitted",
                                context_count_label(context.included_count, "source", "sources"),
                                context_count_label(context.omitted_count, "source", "sources")
                            ),
                        ));
                    let lane_total: usize = context.lanes.iter().map(|lane| lane.bytes).sum();
                    for lane in context.lanes.iter().filter(|lane| lane.bytes > 0).take(3) {
                        let label = if lane.label.trim().is_empty() {
                            lane.kind.as_str()
                        } else {
                            lane.label.as_str()
                        };
                        body = body.with_child(muted_label(
                            &s,
                            context_lane_summary(label, lane.bytes, lane_total),
                        ));
                    }
                }
                WidgetPod::new(body)
            },
        )
    };
    let body = Stack::vertical()
        .spacing(8.0)
        .alignment(Alignment::Stretch)
        .with_child(popup_header(
            shared,
            reader,
            "Context",
            "Close context details",
        ))
        .with_child(content);
    WidgetPod::new(
        Surface::new(
            SurfaceRole::Titlebar,
            WidgetPadding::horizontal(PAD, body).fill_child_height(),
        )
        .fill()
        .theme_when(clone_reader(reader)),
    )
}

fn composer_attachments_popup(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let controls = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        RebuildOnChange::new(
            {
                let s = s.clone();
                move || attachment_signature(&s)
            },
            move |_sig| attachment_controls(&s, &r),
        )
    };
    let body = Stack::vertical()
        .spacing(8.0)
        .alignment(Alignment::Stretch)
        .with_child(popup_header(
            shared,
            reader,
            "Attachments",
            "Close attachments",
        ))
        .with_child(
            SizedBox::new()
                .height(COMPOSER_POPUP_H - 34.0)
                .with_child(ScrollView::vertical(controls).viewport_width_hint(true)),
        );
    WidgetPod::new(
        Surface::new(
            SurfaceRole::Titlebar,
            WidgetPadding::horizontal(PAD, body).fill_child_height(),
        )
        .fill()
        .theme_when(clone_reader(reader)),
    )
}

fn composer_activity_popup(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    if shared.chat().runtime_recovery_target().is_some() {
        let health = shared.chat().runtime_health();
        let status = activity_status_summary(&health.detail);
        let mut actions = Flex::horizontal()
            .wrap(FlexWrap::Wrap)
            .gap(8.0)
            .align_items(Alignment::Center);
        let mut action_count = 0usize;
        if let Some(action) = retry_chat_list_button(shared, reader) {
            actions = actions.with_child(action);
            action_count += 1;
        }
        if let Some(action) = check_sifs_button(shared, reader) {
            actions = actions.with_child(action);
            action_count += 1;
        }
        if let Some(action) = runtime_recovery_button(shared, reader) {
            actions = actions.with_child(action);
            action_count += 1;
        }

        let mut body = Stack::vertical()
            .spacing(8.0)
            .alignment(Alignment::Stretch)
            .with_child(popup_header(shared, reader, "Activity", "Close activity"))
            .with_child(muted_label(
                shared,
                format!("{} · {}", health.label, compact_line(&status, 84)),
            ))
            .with_child(muted_label(shared, activity_work_summary(&shared.chat())));
        if action_count > 0 {
            body = body.with_child(actions);
        }
        return WidgetPod::new(
            Surface::new(
                SurfaceRole::Titlebar,
                WidgetPadding::horizontal(PAD, body).fill_child_height(),
            )
            .fill()
            .theme_when(clone_reader(reader)),
        );
    }

    let detail_expanded = Rc::new(Cell::new(false));
    let activity_content = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        let expanded = Rc::clone(&detail_expanded);
        RebuildOnChange::new(
            {
                let s = s.clone();
                let expanded = Rc::clone(&expanded);
                move || activity_popup_signature(&s, expanded.get())
            },
            move |_sig| WidgetPod::new(activity_popup_content(&s, &r, Rc::clone(&expanded))),
        )
    };
    let body = Stack::vertical()
        .spacing(8.0)
        .alignment(Alignment::Stretch)
        .with_child(activity_popup_header(shared, reader))
        .with_child(
            SizedBox::new()
                .height(COMPOSER_POPUP_H - 34.0)
                .with_child(activity_content),
        );

    WidgetPod::new(
        Surface::new(
            SurfaceRole::Titlebar,
            WidgetPadding::horizontal(PAD, body).fill_child_height(),
        )
        .fill()
        .theme_when(clone_reader(reader)),
    )
}

fn composer_popup_panel(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    match shared.chat().composer_popup() {
        Some(ComposerPopup::Model) => composer_model_popup(shared, reader),
        Some(ComposerPopup::Context) => composer_context_popup(shared, reader),
        Some(ComposerPopup::Attachments) => composer_attachments_popup(shared, reader),
        Some(ComposerPopup::Activity) => composer_activity_popup(shared, reader),
        None => WidgetPod::new(SizedBox::new().height(0.0)),
    }
}

fn composer_popup_host(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let s = shared.clone();
    let r = Rc::clone(reader);
    RebuildOnChange::new(
        {
            let s = s.clone();
            move || s.chat().composer_popup()
        },
        move |_popup| composer_popup_panel(&s, &r),
    )
}

/// A pending attachment as a Mesh context chip riding on top of the input:
/// pill silhouette, leading file glyph, `name · size` label, remove affordance.
fn composer_context_chip(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    attachment: &PendingChatAttachment,
) -> impl Widget + use<> {
    let label = format!(
        "{} · {}",
        compact_line(&attachment.file_name, 22),
        format_bytes(attachment.size_bytes.min(usize::MAX as u64) as usize)
    );
    let remove_label = format!("Remove {}", attachment.file_name);
    let icon = {
        let s = shared.clone();
        Icon::new(IconGlyph::FileText)
            .size(11.0)
            .color_when(move || palette(&s).text_faint)
    };
    let remove = {
        let s = shared.clone();
        IconButton::new(IconGlyph::Close, remove_label)
            .size(16.0)
            .icon_size(10.0)
            .theme_when(composer_control_reader(reader))
            .appearance(sui::ButtonAppearance::Ghost)
            .tone(SemanticTone::Neutral)
            .on_press_with_ctx(move |ctx| {
                s.chat_mut().remove_attachment(index);
                request_chat_window_relayout(ctx);
            })
    };
    let s = shared.clone();
    SizedBox::new().height(ATTACHMENT_CHIPS_H).with_child(
        Surface::field(
            Flex::horizontal()
                .main_gap(5.0)
                .align_items(Alignment::Center)
                .with_child(icon)
                .with_child(small_label(shared, label).color_when(move || palette(&s).text_muted))
                .with_child(remove),
        )
        .radius(999.0)
        .padding(Insets {
            left: 7.0,
            top: 0.0,
            right: 3.0,
            bottom: 0.0,
        })
        .appearance(SurfaceAppearance::Raised)
        .tone(SemanticTone::Neutral)
        .theme_when(clone_reader(reader)),
    )
}

/// One queued steering/follow-up message: an accent-washed chip with the kind
/// spelled out, pending until the runtime consumes it into the turn. Once the
/// runtime confirms the queue id, the chip grows a remove affordance —
/// queued items are visible *and removable*.
fn composer_queue_chip(
    shared: &Shared,
    reader: &ThemeReader,
    index: usize,
    item: &QueuedTurnInput,
) -> impl Widget + use<> {
    let icon = {
        let s = shared.clone();
        Icon::new(IconGlyph::Hourglass)
            .size(10.0)
            .color_when(move || palette(&s).accent_text)
    };
    let kind = {
        let s = shared.clone();
        let theme = shared.theme();
        Label::new(item.kind.label().to_uppercase())
            .style(sui::TextStyle {
                weight: FontWeight::BOLD,
                ..small_theme_text_style(&theme)
            })
            .color_when(move || palette(&s).accent_text)
    };
    let s = shared.clone();
    let text = small_label(shared, compact_line(&item.text, 46))
        .color_when(move || palette(&s).accent_text);
    let mut row = Flex::horizontal()
        .main_gap(5.0)
        .align_items(Alignment::Center)
        .with_child(icon)
        .with_child(kind)
        .with_child(text);
    let mut right_pad = 7.0;
    if item.input_id.is_some() {
        let s = shared.clone();
        row = row.with_child(
            IconButton::new(
                IconGlyph::Close,
                format!("Remove queued {}", item.kind.label()),
            )
            .size(16.0)
            .icon_size(10.0)
            .theme_when(composer_control_reader(reader))
            .appearance(sui::ButtonAppearance::Ghost)
            .tone(SemanticTone::Neutral)
            .on_press_with_ctx(move |ctx| {
                s.chat_mut().remove_queued_turn_input(index);
                request_chat_window_relayout(ctx);
            }),
        );
        right_pad = 2.0;
    }
    SizedBox::new().height(20.0).with_child(
        Surface::field(row)
            .radius(4.0)
            .padding(Insets {
                left: 7.0,
                top: 0.0,
                right: right_pad,
                bottom: 0.0,
            })
            .appearance(SurfaceAppearance::Soft)
            .tone(SemanticTone::Accent)
            .theme_when(clone_reader(reader)),
    )
}

/// Chip rows ride on top of the composer input (`.sm-composer-queue`,
/// `.sm-ctx-row`): wrap-flex with the Mesh 8/10 row padding, collapsing to
/// nothing when empty.
fn composer_chip_row(row: impl Widget + 'static, semantic_label: &'static str) -> WidgetPod {
    WidgetPod::new(SemanticRegion::new(
        semantic_label,
        WidgetPadding::new(
            Insets {
                left: COMPOSER_CHIP_ROW_PAD_X,
                top: COMPOSER_CHIP_ROW_PAD_TOP,
                right: COMPOSER_CHIP_ROW_PAD_X,
                bottom: 0.0,
            },
            row,
        ),
    ))
}

/// Steering / follow-up messages queued into the next turn, visible so
/// "I already told it" is never in doubt.
fn composer_queue_row(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let items = shared.chat().queued_turn_inputs.clone();
    if items.is_empty() {
        return WidgetPod::new(SizedBox::new().height(0.0));
    }
    let mut row = Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .gap(6.0)
        .align_items(Alignment::Center);
    for (index, item) in items.iter().enumerate() {
        row = row.with_child(composer_queue_chip(shared, reader, index, item));
    }
    composer_chip_row(row, "Queued for next turn")
}

/// Attached context files as removable chips.
fn composer_context_row(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let attachments = shared.chat().attachments.clone();
    if attachments.is_empty() {
        return WidgetPod::new(SizedBox::new().height(0.0));
    }
    let mut row = Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .gap(6.0)
        .align_items(Alignment::Center);
    for (index, attachment) in attachments.iter().enumerate() {
        row = row.with_child(composer_context_chip(shared, reader, index, attachment));
    }
    composer_chip_row(row, "Attached context")
}

/// The composer action bar (`.sm-composer-bar`): compact ghost controls on the
/// left and Send on the right. Detailed state and the keyboard shortcut live in
/// hover tooltips instead of consuming the writing surface.
fn composer_bar(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let (streaming, media_active, hint, can_reuse) = {
        let chat = shared.chat();
        (
            chat.streaming,
            chat.media_call.active,
            voice_link_hint(&chat),
            chat.can_reuse_latest_answer(),
        )
    };

    let attach = {
        let s = shared.clone();
        IconButton::new(IconGlyph::Attach, "Attach files")
            .size(COMPOSER_GHOST_BUTTON_SIZE)
            .icon_size(14.0)
            .theme_when(composer_control_reader(reader))
            .appearance(sui::ButtonAppearance::Ghost)
            .tone(SemanticTone::Neutral)
            .on_press_with_ctx(move |ctx| {
                s.chat_mut()
                    .toggle_composer_popup(ComposerPopup::Attachments);
                request_chat_window_relayout(ctx);
            })
    };

    let mic = {
        let s = shared.clone();
        IconButton::new(
            if media_active {
                IconGlyph::AudioLines
            } else {
                IconGlyph::Mic
            },
            if media_active {
                "Voice link on — tap to end"
            } else {
                "Set up voice link"
            },
        )
        .size(COMPOSER_GHOST_BUTTON_SIZE)
        .icon_size(14.0)
        .theme_when(composer_control_reader(reader))
        .appearance(sui::ButtonAppearance::Ghost)
        .tone(SemanticTone::Neutral)
        .selected_when({
            let s = s.clone();
            move || s.chat().media_call.active
        })
        .on_press_with_ctx(move |ctx| {
            s.chat_mut().activate_media_call_control();
            request_chat_window_relayout(ctx);
        })
    };

    // Model/context/activity match the icon-sized attachment and voice affordances.
    // Their current state remains available to pointer users via tooltips and to
    // assistive technology through the IconButton semantic name.
    let model = {
        let s = shared.clone();
        let label = model_button_label(&shared.chat());
        Tooltip::new(
            label.clone(),
            IconButton::new(IconGlyph::Sparkles, label)
                .size(COMPOSER_GHOST_BUTTON_SIZE)
                .icon_size(14.0)
                .theme_when(composer_control_reader(reader))
                .appearance(sui::ButtonAppearance::Ghost)
                .tone(SemanticTone::Neutral)
                .on_press_with_ctx(move |ctx| {
                    s.chat_mut().toggle_composer_popup(ComposerPopup::Model);
                    request_chat_window_relayout(ctx);
                }),
        )
        .placement(TooltipPlacement::Above)
        .theme(reader())
    };

    let context = {
        let s = shared.clone();
        let label = context_overview_summary(&shared.chat());
        Tooltip::new(
            label.clone(),
            IconButton::new(IconGlyph::History, label)
                .size(COMPOSER_GHOST_BUTTON_SIZE)
                .icon_size(14.0)
                .theme_when(composer_control_reader(reader))
                .appearance(sui::ButtonAppearance::Ghost)
                .tone(SemanticTone::Neutral)
                .on_press_with_ctx(move |ctx| {
                    s.chat_mut().toggle_composer_popup(ComposerPopup::Context);
                    request_chat_window_relayout(ctx);
                }),
        )
        .placement(TooltipPlacement::Above)
        .theme(reader())
    };

    let activity = {
        let s = shared.clone();
        let label = activity_button_label(&shared.chat()).to_string();
        Tooltip::new(
            activity_button_description(&shared.chat()),
            IconButton::new(IconGlyph::Hourglass, label)
                .size(COMPOSER_GHOST_BUTTON_SIZE)
                .icon_size(14.0)
                .theme_when(composer_control_reader(reader))
                .appearance(sui::ButtonAppearance::Ghost)
                .tone(SemanticTone::Neutral)
                .on_press_with_ctx(move |ctx| {
                    s.chat_mut().toggle_activity_panel();
                    request_chat_window_relayout(ctx);
                }),
        )
        .placement(TooltipPlacement::Above)
        .theme(reader())
    };

    let mut row = Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .gap(COMPOSER_BAR_GAP)
        .align_items(Alignment::Center)
        .with_child(attach)
        .with_child(mic);

    if let Some(hint_text) = hint {
        let s = shared.clone();
        row = row
            .with_child(small_label(shared, hint_text).color_when(move || palette(&s).accent_text));
    }

    row = row
        .with_child(model)
        .with_child(context)
        .with_child(activity)
        .spacer();

    if can_reuse && !streaming {
        let s = shared.clone();
        row = row.with_child(
            sui::Button::new("Use answer")
                .icon(IconGlyph::Redo)
                .min_height(COMPOSER_GHOST_BUTTON_SIZE)
                .theme_when(clone_reader(reader))
                .appearance(sui::ButtonAppearance::Ghost)
                .tone(SemanticTone::Neutral)
                .on_press(move || {
                    s.chat_mut().use_latest_answer_as_draft();
                }),
        );
    }

    // Interruption is first-class: the send button becomes stop mid-stream.
    let (action, action_tooltip) = if streaming {
        let s = shared.clone();
        (
            IconButton::new(IconGlyph::Stop, "Stop generation")
                .size(SEND_BUTTON_SIZE)
                .icon_size(13.0)
                .theme_when(composer_control_reader(reader))
                .appearance(sui::ButtonAppearance::Ghost)
                .tone(SemanticTone::Accent)
                .on_press(move || {
                    s.chat_mut().interrupt_turn();
                }),
            "Stop the current response",
        )
    } else {
        let s = shared.clone();
        (
            IconButton::new(IconGlyph::ArrowUp, "Send message")
                .size(SEND_BUTTON_SIZE)
                .icon_size(16.0)
                .theme_when(composer_control_reader(reader))
                .appearance(sui::ButtonAppearance::Ghost)
                .tone(SemanticTone::Accent)
                .enabled_when({
                    let s = s.clone();
                    move || s.chat().can_send()
                })
                .on_press(move || {
                    s.chat_mut().submit();
                }),
            "Enter sends · Shift+Enter newline",
        )
    };
    row = row.with_child(
        Tooltip::new(action_tooltip, action)
            .placement(TooltipPlacement::Above)
            .alignment(TooltipAlignment::End)
            .theme(reader()),
    );

    WidgetPod::new(WidgetPadding::new(
        Insets {
            left: COMPOSER_BAR_PAD_X,
            top: COMPOSER_BAR_PAD_Y,
            right: COMPOSER_BAR_PAD_X,
            bottom: COMPOSER_BAR_PAD_Y,
        },
        row,
    ))
}

fn composer_input_min_height() -> f32 {
    DefaultTheme::default().text.base.line_height * COMPOSER_INPUT_MIN_LINES as f32
        + COMPOSER_INPUT_PAD_Y
}

/// The input host rebuild key: provider availability (editable <-> setup-required
/// read-only state) and the draft revision, bumped only by programmatic draft
/// replacement — normal typing must not rebuild the focused `TextArea`.
fn composer_input_signature(shared: &Shared) -> (bool, bool, u64) {
    let chat = shared.chat();
    (
        chat.streaming,
        chat.providers.is_empty(),
        chat.draft_revision,
    )
}

/// The borderless message input. The field frame owns the composer's border and
/// focus treatment, so the `TextArea` itself paints no chrome of its own.
/// Enter submits (steering the turn while one streams); Shift+Enter breaks the line.
/// Right-click opens the standard edit menu (cut/copy/paste/select-all), whose
/// activations post [`TextCommand`]s back to the input widget.
fn composer_input_row(
    shared: &Shared,
    reader: &ThemeReader,
    selection: &SelectionScope,
) -> WidgetPod {
    let s = shared.clone();
    let draft = shared.chat().input.clone();
    let provider_ready = !shared.chat().providers.is_empty();
    let placeholder = if provider_ready {
        "Message sinomo…"
    } else {
        "Set up a provider before sending."
    };
    let mut input = TextArea::new("chat-input")
        .value(draft)
        .placeholder(placeholder)
        .min_height(composer_input_min_height())
        .padding(Insets {
            left: 12.0,
            top: 10.0,
            right: 12.0,
            bottom: 4.0,
        })
        .selection_scope(selection.clone())
        .appearance(FieldAppearance::Bare)
        .theme_when(clone_reader(reader));
    if provider_ready {
        input = input.on_change({
            let s = s.clone();
            move |value| {
                s.chat_mut().set_draft(value);
            }
        });
        // Enter sends, Shift+Enter inserts a newline (the on_submit only fires while the
        // TextArea is focused — Enter only reaches a focused input). While streaming,
        // `submit()` queues the draft as steering for the active runtime turn.
        input = input.on_submit({
            let s = s.clone();
            move |_text| {
                s.chat_mut().submit();
            }
        });
    } else {
        input = input.read_only();
    }

    let menu = ContextMenu::new("chat-input-menu", input).theme_when(clone_reader(reader));
    let input_id = menu.trigger_id();
    let items_scope = selection.clone();
    let items_shared = s.clone();
    let menu = menu
        .items_when(move || {
            let has_selection = items_scope.has_owner_selection(input_id);
            let has_text = !items_shared.chat().input.is_empty();
            vec![
                menu_item_enabled(
                    MenuItem::new("Cut").shortcut("Ctrl+X"),
                    provider_ready && has_selection,
                ),
                menu_item_enabled(MenuItem::new("Copy").shortcut("Ctrl+C"), has_selection),
                menu_item_enabled(MenuItem::new("Paste").shortcut("Ctrl+V"), provider_ready),
                menu_item_enabled(
                    MenuItem::new("Select All")
                        .shortcut("Ctrl+A")
                        .separator_before(),
                    has_text,
                ),
            ]
        })
        .on_activate_with_ctx(move |ctx, _index, item| {
            let command = match item.label() {
                "Cut" => Some(TextCommand::Cut),
                "Copy" => Some(TextCommand::Copy),
                "Paste" => Some(TextCommand::Paste),
                "Select All" => Some(TextCommand::SelectAll),
                _ => None,
            };
            if let Some(command) = command {
                ctx.post_event(input_id, command.into_event());
            }
        });

    let scroll_state = ScrollState::new();
    WidgetPod::new(ComposerInputScroll::new(
        WidgetPod::new(
            ScrollView::vertical(menu)
                .name("chat-input-scroll")
                .state(scroll_state.clone()),
        ),
        WidgetPod::new(
            ScrollBar::vertical(scroll_state)
                .name("chat-input-scrollbar")
                .width(8.0)
                .theme(reader()),
        ),
    ))
}

/// Assemble the Mesh composer field — one surface for text, attachments and
/// voice: queued-steering chips and context chips ride on top, the borderless
/// input grows in the middle, and the action bar closes the surface.
fn composer_field(
    shared: &Shared,
    reader: &ThemeReader,
    selection: &SelectionScope,
) -> impl Widget + use<> {
    let queue_host = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        RebuildOnChange::new(
            {
                let s = s.clone();
                move || {
                    s.chat()
                        .queued_turn_inputs
                        .iter()
                        .map(|item| {
                            (
                                item.kind == QueuedTurnKind::Steer,
                                item.text.clone(),
                                item.input_id.clone(),
                            )
                        })
                        .collect::<Vec<_>>()
                }
            },
            move |_items| composer_queue_row(&s, &r),
        )
    };

    let context_host = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        RebuildOnChange::new(
            {
                let s = s.clone();
                move || attachment_signature(&s)
            },
            move |_sig| composer_context_row(&s, &r),
        )
    };

    let input_host = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        let sel = selection.clone();
        RebuildOnChange::new(
            {
                let s = s.clone();
                move || composer_input_signature(&s)
            },
            move |_sig| composer_input_row(&s, &r, &sel),
        )
    };

    let bar_host = {
        let s = shared.clone();
        let r = Rc::clone(reader);
        RebuildOnChange::new(
            {
                let s = s.clone();
                move || composer_bar_signature(&s)
            },
            move |_sig| composer_bar(&s, &r),
        )
    };

    let rows = Stack::vertical()
        .alignment(Alignment::Stretch)
        .with_child(queue_host)
        .with_child(context_host)
        .with_child(input_host)
        .with_child(bar_host);

    FramedField::new(rows)
        .name("Message composer")
        .description("Message, attachments, model controls, and send actions")
        .fill_width()
        .theme_when(clone_reader(reader))
}

fn composer_body(
    shared: &Shared,
    reader: &ThemeReader,
    selection: &SelectionScope,
) -> impl Widget + use<> {
    let body = ComposerPanel::new(
        shared.clone(),
        WidgetPod::new(composer_popup_host(shared, reader)),
        WidgetPod::new(composer_field(shared, reader, selection)),
    );

    Surface::new(SurfaceRole::Titlebar, body)
        .border(SurfaceBorder::Top)
        .fill_width()
        .theme_when(clone_reader(reader))
}

/// The full composer: a bottom-anchored panel with popups opening above the field.
fn composer(
    shared: &Shared,
    reader: &ThemeReader,
    selection: &SelectionScope,
) -> impl Widget + use<> {
    composer_body(shared, reader, selection)
}

// ---------------------------------------------------------------------------
// Composed pods (wide split / narrow) + responsive host.
// ---------------------------------------------------------------------------

fn menu_item_enabled(item: MenuItem, enabled: bool) -> MenuItem {
    if enabled { item } else { item.disabled() }
}

/// Wrap the transcript in its right-click menu: copy the line selection, copy
/// the whole message under the pointer, or select the entire transcript. The
/// activations post standard text commands back to the transcript widget.
fn transcript_with_context_menu(
    shared: &Shared,
    reader: &ThemeReader,
    selection: &SelectionScope,
    view: TranscriptView,
) -> impl Widget + use<> {
    let menu = ContextMenu::new("chat-transcript-menu", view).theme_when(clone_reader(reader));
    let transcript_id = menu.trigger_id();
    let items_scope = selection.clone();
    let items_shared = shared.clone();
    menu.items_when(move || {
        let has_selection = items_scope.has_owner_selection(transcript_id);
        let has_messages = !items_shared.chat().messages.is_empty();
        vec![
            menu_item_enabled(MenuItem::new("Copy").shortcut("Ctrl+C"), has_selection),
            menu_item_enabled(MenuItem::new("Copy Message"), has_messages),
            menu_item_enabled(
                MenuItem::new("Select All")
                    .shortcut("Ctrl+A")
                    .separator_before(),
                has_messages,
            ),
        ]
    })
    .on_activate_with_ctx(move |ctx, _index, item| {
        let event = match item.label() {
            "Copy" => Some(TextCommand::Copy.into_event()),
            "Copy Message" => Some(Event::Custom(CustomEvent::new(CHAT_COPY_MESSAGE_EVENT))),
            "Select All" => Some(TextCommand::SelectAll.into_event()),
            _ => None,
        };
        if let Some(event) = event {
            ctx.post_event(transcript_id, event);
        }
    })
}

/// The main column: the transcript fills, with the composer docked at the bottom. The streaming
/// request-animation-frame pump lives on [`ChatView`] (a reliable ancestor of every control), so no
/// invisible overlay widget is needed here.
fn main_column(
    shared: &Shared,
    reader: &ThemeReader,
    selection: &SelectionScope,
) -> impl Widget + use<> {
    let transcript = {
        let key_shared = shared.clone();
        let build_shared = shared.clone();
        let build_reader = Rc::clone(reader);
        let build_selection = selection.clone();
        RebuildOnChange::new(
            move || {
                let chat = key_shared.chat();
                (
                    chat.media_call_panel_visible(),
                    chat.media_call_details_visible(),
                )
            },
            move |media_sig| {
                let (panel_visible, details_open) = *media_sig;
                let transcript = transcript_with_context_menu(
                    &build_shared,
                    &build_reader,
                    &build_selection,
                    TranscriptView::new(build_shared.clone(), build_selection.clone()),
                );
                if !panel_visible {
                    return WidgetPod::new(transcript);
                }
                let height = if details_open {
                    MEDIA_CALL_H
                } else {
                    MEDIA_CALL_COMPACT_H
                };
                WidgetPod::new(
                    Dock::new(transcript)
                        .top(height, media_call_panel(&build_shared, &build_reader)),
                )
            },
        )
    };
    MeasuredBottomDock::new(transcript, composer(shared, reader, selection))
}

fn chat_work_area(
    shared: &Shared,
    reader: &ThemeReader,
    selection: &SelectionScope,
) -> impl Widget + use<> {
    main_column(shared, reader, selection)
}

fn compact_chat_header(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let theme = shared.theme();
    let pal = palette(shared);

    let open_drawer = {
        let s = shared.clone();
        IconButton::new(IconGlyph::Chat, "Open conversations and agents")
            .size(34.0)
            .icon_size(16.0)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .selected_when({
                let s = s.clone();
                move || s.chat().compact_drawer_open
            })
            .on_press_with_ctx(move |ctx| {
                s.chat_mut().toggle_compact_drawer();
                request_chat_window_relayout(ctx);
            })
    };
    let title = {
        let s = shared.clone();
        Label::dynamic("Conversation", move || {
            let chat = s.chat();
            chat.current_session_ref()
                .map(|session| session.title.clone())
                .unwrap_or_else(|| chat.empty_conversation_title())
        })
        .semantic_name("Current conversation")
        .style(pal.text_style(&theme, 13.0, FontWeight::SEMIBOLD, pal.text))
    };
    let agent = {
        let s = shared.clone();
        Label::dynamic("Agent", move || {
            let chat = s.chat();
            let display = chat
                .agents
                .iter()
                .find(|agent| agent.key == chat.info.agent || agent.name == chat.info.agent)
                .map(|agent| agent.name.as_str())
                .filter(|name| !name.trim().is_empty())
                .unwrap_or_else(|| {
                    if chat.info.agent.trim().is_empty() {
                        "Sinomo"
                    } else {
                        chat.info.agent.as_str()
                    }
                });
            format!("Agent · {display}")
        })
        .semantic_name("Current agent")
        .style(pal.text_style(&theme, 11.0, FontWeight::NORMAL, pal.text_muted))
    };
    let edit = {
        let s = shared.clone();
        IconButton::new(IconGlyph::MoreHorizontal, "Edit current conversation")
            .size(34.0)
            .icon_size(16.0)
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .enabled_when({
                let s = s.clone();
                move || s.chat().current_session_ref().is_some()
            })
            .on_press_with_ctx(move |ctx| {
                s.chat_mut().begin_edit_current_session();
                request_chat_window_relayout(ctx);
            })
    };

    let labels = Stack::vertical()
        .spacing(0.0)
        .alignment(Alignment::Stretch)
        .with_child(title)
        .with_child(agent);
    let row = Flex::horizontal()
        .main_gap(8.0)
        .align_items(Alignment::Center)
        .with_item(open_drawer, FlexItem::new().no_shrink())
        .with_item(labels, FlexItem::fill().min_width(160.0))
        .with_item(edit, FlexItem::new().no_shrink());
    Surface::new(
        SurfaceRole::Titlebar,
        WidgetPadding::new(
            Insets {
                left: 10.0,
                top: 8.0,
                right: 10.0,
                bottom: 8.0,
            },
            row,
        )
        .fill_child(),
    )
    .border(SurfaceBorder::Bottom)
    .fill_width()
    .theme_when(clone_reader(reader))
}

fn wide_pod(
    shared: &Shared,
    reader: &ThemeReader,
    sessions_scroll_state: &ScrollState,
    selection: &SelectionScope,
) -> WidgetPod {
    let work_area = chat_work_area(shared, reader, selection);
    let ratio = shared.chat().sidebar_ratio;
    let ratio_shared = shared.clone();
    let row = SplitView::horizontal(sidebar(shared, reader, sessions_scroll_state), work_area)
        .name("Conversation sidebar")
        .ratio(ratio)
        .min_first(SIDEBAR_MIN_W)
        .min_second(CHAT_PANE_MIN_W)
        .theme(reader())
        .on_change(move |ratio| ratio_shared.chat_mut().set_sidebar_ratio(ratio));
    let dialog_host = ChatDialogHost::new(row, shared.clone(), Rc::clone(reader));
    WidgetPod::new(
        Surface::window(dialog_host)
            .fill()
            .theme_when(clone_reader(reader)),
    )
}

fn narrow_pod(shared: &Shared, reader: &ThemeReader, selection: &SelectionScope) -> WidgetPod {
    let body = Dock::new(main_column(shared, reader, selection))
        .top(COMPACT_HEADER_H, compact_chat_header(shared, reader));
    let dialog_host = ChatDialogHost::new(body, shared.clone(), Rc::clone(reader));
    WidgetPod::new(
        Surface::window(dialog_host)
            .fill()
            .theme_when(clone_reader(reader)),
    )
}

/// The chat tab: a thin responsive host owning two pre-built pods (wide split with sidebar / narrow
/// compact-header + transcript) and delegating to the one chosen by the available width. The choice is made in
/// `measure` (constraints-driven) and cached so `arrange`/`paint`/event routing stay in sync.
/// Mirrors the Settings / Files hosts.
///
/// `ChatView` additionally owns the streaming **pump**: it is a reliable ancestor of every control
/// (the sidebar/composer `RebuildOnChange` hosts and the transcript), and it is reliably on the
/// event path for the pointer / keyboard gestures that start a turn — so it is the right place to
/// drive the request-animation-frame loop. (A sibling overlay widget could not: in sui an event
/// only reaches widgets on the `path_to(target)`, and `RedrawRequested` / external wakes route to
/// the *root* only, so an off-path overlay would never receive the kick.) While a reply streams (or
/// there was recent activity) the pump drains the worker each frame, applies events to `ChatState`,
/// and — when the parts the `RebuildOnChange` hosts key on change — forces a full window relayout so
/// those hosts rebuild and re-measure (a worker-driven change arrives outside any pointer gesture).
pub struct ChatView {
    shared: Shared,
    theme_reader: ThemeReader,
    sessions_scroll_state: ScrollState,
    selection: SelectionScope,
    wide: WidgetPod,
    narrow: WidgetPod,
    use_wide: Cell<bool>,
    /// Coarse signature of the selection / session / streaming state the inner `RebuildOnChange`
    /// hosts key on; a change forces a full relayout so they rebuild.
    last_sig: Cell<u64>,
    last_scale_key: Cell<AppTextScale>,
    /// Whether an animation frame is currently requested (so we don't stack duplicate requests).
    pump_alive: Cell<bool>,
    /// Frames since the last state change; bounds the idle tail so we stop requesting frames.
    idle_frames: Cell<u32>,
}

/// Idle-frame budget: once this many frames pass with no state change (and nothing streaming) the
/// pump stops requesting animation frames, so an idle tab never busy-loops.
const PUMP_IDLE_BUDGET: u32 = 90;

impl ChatView {
    pub fn new(shared: Shared) -> Self {
        let theme_reader: ThemeReader = {
            let s = shared.clone();
            Rc::new(move || s.theme())
        };
        let sessions_scroll_state = ScrollState::new();
        // One selection registry for the whole chat tab: the composer and the
        // transcript replace each other's selection through it, and the
        // context menus read it for enabled state.
        let selection = SelectionScope::new();
        let wide = wide_pod(&shared, &theme_reader, &sessions_scroll_state, &selection);
        let narrow = narrow_pod(&shared, &theme_reader, &selection);
        let last_sig = Cell::new(view_signature(&shared));
        let last_scale_key = Cell::new(chat_scale_key(&shared));
        Self {
            shared,
            theme_reader,
            sessions_scroll_state,
            selection,
            wide,
            narrow,
            use_wide: Cell::new(true),
            last_sig,
            last_scale_key,
            pump_alive: Cell::new(false),
            idle_frames: Cell::new(0),
        }
    }

    fn active(&self) -> &WidgetPod {
        if self.use_wide.get() {
            &self.wide
        } else {
            &self.narrow
        }
    }

    fn active_mut(&mut self) -> &mut WidgetPod {
        if self.use_wide.get() {
            &mut self.wide
        } else {
            &mut self.narrow
        }
    }

    fn sync_scale(&mut self) -> bool {
        let current = chat_scale_key(&self.shared);
        if current == self.last_scale_key.get() {
            return false;
        }
        self.last_scale_key.set(current);
        self.wide = wide_pod(
            &self.shared,
            &self.theme_reader,
            &self.sessions_scroll_state,
            &self.selection,
        );
        self.narrow = narrow_pod(&self.shared, &self.theme_reader, &self.selection);
        true
    }

    /// Ensure the request-animation-frame pump is running (idempotent).
    fn ensure_pump(&self, ctx: &mut EventCtx) {
        if !self.pump_alive.get() {
            ctx.request_animation_frame();
            self.pump_alive.set(true);
        }
        // Any fresh gesture resets the idle tail so a just-dispatched command is drained promptly.
        self.idle_frames.set(0);
    }

    /// Force a full window relayout (re-measure + re-arrange + repaint) for this frame.
    ///
    /// The streaming worker mutates `ChatState` outside any pointer gesture, and the
    /// sidebar/composer `RebuildOnChange` hosts only `refresh()` when their `measure()` runs. A
    /// plain `ctx.request_measure()` here would only invalidate `ChatView`'s own measure path, which
    /// can short-circuit before reaching those hosts. Targeting the *window* makes sui's
    /// `build_measure_scope` upgrade the pass to `force_all`, so every pod (including those hosts)
    /// re-measures and the newly-built option lists / Send<->Interrupt swap take effect.
    fn force_window_relayout(&self, ctx: &mut EventCtx) {
        use sui::{InvalidationKind, InvalidationRequest, InvalidationTarget};
        let window = InvalidationTarget::Window(ctx.window_id());
        ctx.request(InvalidationRequest::new(window, InvalidationKind::Measure));
        ctx.request(InvalidationRequest::new(window, InvalidationKind::Arrange));
        ctx.request_paint();
        ctx.request_semantics();
    }

    /// One pump tick: drain the worker, apply events, and relayout/repaint as needed; keep the loop
    /// alive while streaming or recently active.
    fn pump(&self, ctx: &mut EventCtx) {
        self.pump_alive.set(false);
        let (structural_changed, media_changed) = {
            let mut chat = self.shared.chat_mut();
            let worker_changed = chat.pump_worker_events();
            let provider_changed = chat.pump_provider_list();
            let list_changed = chat.pump_session_list();
            let transcript_changed = chat.pump_transcript_load();
            let delete_changed = chat.pump_delete();
            let channel_create_changed = chat.pump_channel_create();
            let media_changed = chat.pump_media_call_runtime();
            let structural_changed = worker_changed
                || provider_changed
                || list_changed
                || transcript_changed
                || delete_changed
                || channel_create_changed;
            (structural_changed, media_changed)
        };
        let changed = structural_changed || media_changed.changed();
        let streaming = self.shared.chat().streaming;
        let media_call_active = self.shared.chat().media_call.active;
        let transcript_pending = self.shared.chat().transcript_load_pending();

        // Recompute the coarse signature each tick: a worker event (RuntimeInfo / Models / TurnDone,
        // or a token that resolved the selection) may have changed the selector option lists, the
        // sidebar sessions, or the streaming flag (Send <-> Interrupt) — anything the inner
        // `RebuildOnChange` hosts key on. On a signature change, force a full relayout so those hosts
        // rebuild; on a bare token (same signature) just repaint the live-reading transcript.
        let sig = view_signature(&self.shared);
        let sig_changed = sig != self.last_sig.get();
        if sig_changed || media_changed.layout {
            self.last_sig.set(sig);
            self.force_window_relayout(ctx);
        } else if structural_changed || media_changed.semantics {
            ctx.request_paint();
            // Transcript rows and the busy indicator are live semantic
            // content, not paint-only decoration.
            ctx.request_semantics();
        } else if media_changed.paint {
            ctx.request_paint();
        }

        if changed || sig_changed {
            self.idle_frames.set(0);
        } else {
            self.idle_frames
                .set(self.idle_frames.get().saturating_add(1));
        }
        if streaming
            || media_call_active
            || transcript_pending
            || self.idle_frames.get() < PUMP_IDLE_BUDGET
        {
            ctx.request_animation_frame();
            self.pump_alive.set(true);
        }
    }
}

/// A coarse signature of the parts of `ChatState` whose change requires a host relayout (the
/// session/agent lists, provider/model/effort selection, and the streaming flag — anything the
/// inner `RebuildOnChange` hosts key on). Cheap to compute each redraw.
fn view_signature(shared: &Shared) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let chat = shared.chat();
    let mut h = DefaultHasher::new();
    chat.provider_idx.hash(&mut h);
    chat.model_idx.hash(&mut h);
    chat.effort_idx.hash(&mut h);
    chat.current_models().len().hash(&mut h);
    chat.effort_visible().hash(&mut h);
    chat.streaming.hash(&mut h);
    chat.media_call.active.hash(&mut h);
    chat.media_call_details_visible().hash(&mut h);
    voice_link_hint(&chat).hash(&mut h);
    chat.can_reuse_latest_answer().hash(&mut h);
    chat.draft_revision.hash(&mut h);
    chat.attachments.len().hash(&mut h);
    chat.attachment_path_input.is_empty().hash(&mut h);
    chat.attachment_manual_path_open.hash(&mut h);
    chat.attachment_operation_pending().hash(&mut h);
    chat.attachment_picker_lane_pending().hash(&mut h);
    chat.pending_attachment_validations.len().hash(&mut h);
    chat.attachment_drop_hovered.hash(&mut h);
    chat.attachment_picker_error.hash(&mut h);
    chat.queued_turn_inputs.len().hash(&mut h);
    for queued in &chat.queued_turn_inputs {
        (queued.kind == QueuedTurnKind::Steer).hash(&mut h);
        queued.text.hash(&mut h);
        queued.input_id.hash(&mut h);
    }
    chat.models_loading.hash(&mut h);
    chat.composer_popup().hash(&mut h);
    chat.activity_panel_visible().hash(&mut h);
    context_overview_summary(&chat).hash(&mut h);
    chat.activity.len().hash(&mut h);
    for entry in chat.activity.iter().rev().take(16) {
        entry.title.hash(&mut h);
        entry.detail.hash(&mut h);
        entry.tone.hash(&mut h);
    }
    chat.tool_calls.len().hash(&mut h);
    chat.tool_chain_expanded.hash(&mut h);
    chat.expanded_tool_details.len().hash(&mut h);
    for call in &chat.tool_calls {
        call.status.hash(&mut h);
        call.input_preview.hash(&mut h);
        call.process_status.hash(&mut h);
        call.exit_code.hash(&mut h);
        call.stdout.len().hash(&mut h);
        call.stderr.len().hash(&mut h);
    }
    chat.control.task_count.hash(&mut h);
    chat.control.active_turn.hash(&mut h);
    for task in chat.control.tasks.iter().take(8) {
        task.summary.hash(&mut h);
        task.status.hash(&mut h);
        task.progress_summary.hash(&mut h);
        task.result_ref.hash(&mut h);
    }
    chat.usage
        .as_ref()
        .and_then(|usage| usage.output_tokens)
        .hash(&mut h);
    chat.session_id.hash(&mut h);
    chat.transcript_load_pending().hash(&mut h);
    chat.transcript_load_slow_notice_shown.hash(&mut h);
    chat.session_list_loading.hash(&mut h);
    chat.session_list_slow_notice_shown.hash(&mut h);
    chat.session_kind_filter.hash(&mut h);
    chat.session_edit_id.hash(&mut h);
    chat.session_delete_confirm_id.hash(&mut h);
    chat.session_edit_details_open.hash(&mut h);
    chat.compact_drawer_open.hash(&mut h);
    chat.channel_agent_idx.hash(&mut h);
    chat.channel_create_rx.is_some().hash(&mut h);
    chat.visible_session_has_started().hash(&mut h);
    chat.runtime_recovery_target()
        .map(RuntimeRecoveryTarget::key)
        .unwrap_or("none")
        .hash(&mut h);
    chat.providers.len().hash(&mut h);
    for provider in &chat.providers {
        provider.id.hash(&mut h);
        provider.account.hash(&mut h);
        provider.name.hash(&mut h);
    }
    chat.sessions.len().hash(&mut h);
    for s in &chat.sessions {
        s.id.hash(&mut h);
        s.kind.hash(&mut h);
        s.title.hash(&mut h);
        s.participant_count.hash(&mut h);
        s.participant_names.hash(&mut h);
    }
    chat.info.agent.hash(&mut h);
    h.finish()
}

impl Widget for ChatView {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        // The streaming pump: an animation-frame wake drains the worker and relayouts. (The wake
        // targets `ChatView` because `ChatView` requested it, so it routes here reliably.)
        if let Event::Wake(sui::WakeEvent::AnimationFrame { .. }) = event {
            if self.shared.is_ready() {
                self.pump(ctx);
            }
            return;
        }

        if !self.shared.is_ready() {
            return;
        }

        // Mesh: Esc is the most important key in an agent UI — it halts
        // generation. One press closes an open composer popup; the next (or
        // the first, with nothing open) interrupts the active turn. The
        // session-edit dialog owns Esc while it is open (it dismisses itself
        // deeper on this capture path), so leave the key alone in that state.
        if let Event::Keyboard(key) = event
            && key.state == KeyState::Pressed
            && key.key == "Escape"
            && self.shared.chat().session_edit_id.is_none()
        {
            let handled = self.shared.chat_mut().handle_escape();
            if handled {
                self.ensure_pump(ctx);
                self.force_window_relayout(ctx);
                ctx.set_handled();
                return;
            }
        }

        // Any pointer or key activity over the tab is a moment the worker may have pending events:
        // a turn just started (Send click / Enter submit), a selection changed (sidebar / Select),
        // or the initial Ready/Models fetch landed in the channel. Make sure the pump is running so
        // those are drained. (Pointer Move/Enter are plentiful in the live app, so the initial model
        // fetch is drained promptly without requiring an explicit click.)
        let pointer_or_key = matches!(event, Event::Pointer(_))
            || matches!(event, Event::Keyboard(k) if k.state == KeyState::Pressed);
        if pointer_or_key {
            self.ensure_pump(ctx);
        }

        // A pointer release / key press can also have just rebuilt a `RebuildOnChange` host (a
        // sidebar/Select on-change fired during this gesture); relayout so it re-measures. The
        // `measure` pass refreshes `last_sig` to the post-change signature, so the next pump tick
        // does not redundantly relayout for a change this gesture already handled.
        let commit = matches!(event, Event::Pointer(p) if matches!(p.kind, PointerEventKind::Up))
            || matches!(event, Event::Keyboard(k) if k.state == KeyState::Pressed);
        if commit {
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_paint();
        }
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        self.sync_scale();
        let use_wide = constraints.max.width >= WIDE_THRESHOLD;
        if use_wide && !self.use_wide.get() {
            self.shared.chat_mut().compact_drawer_open = false;
        }
        self.use_wide.set(use_wide);
        self.last_sig.set(view_signature(&self.shared));
        self.active_mut().measure(ctx, constraints)
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        self.active_mut().arrange(ctx, bounds);
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.active().paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.active().semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(self.active());
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        // CRITICAL: forward to the active pod so pointer / keyboard / focus reach the controls.
        if self.use_wide.get() {
            visitor.visit(&mut self.wide);
        } else {
            visitor.visit(&mut self.narrow);
        }
    }
}

fn chat_scale_key(shared: &Shared) -> AppTextScale {
    shared.borrow().text_scale
}

/// The docked composer panel: an optional popup block above the field surface.
/// The popup keeps its fixed height; the field takes its natural height (the
/// input grows to `COMPOSER_INPUT_MAX_H`, then scrolls).
struct ComposerPanel {
    shared: Shared,
    popup: WidgetPod,
    field: WidgetPod,
}

impl ComposerPanel {
    fn new(shared: Shared, popup: WidgetPod, field: WidgetPod) -> Self {
        Self {
            shared,
            popup,
            field,
        }
    }

    fn popup_visible(&self) -> bool {
        self.shared.chat().composer_popup().is_some()
    }

    fn popup_block_h(&self) -> f32 {
        if self.popup_visible() {
            COMPOSER_POPUP_H + COMPOSER_SECTION_GAP
        } else {
            0.0
        }
    }

    fn content_rect(bounds: Rect, y: f32, height: f32) -> Rect {
        Rect::new(
            bounds.x() + PAD,
            y,
            (bounds.width() - PAD * 2.0).max(0.0),
            height.max(0.0),
        )
    }
}

impl Widget for ComposerPanel {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let width = if constraints.max.width.is_finite() {
            constraints.max.width
        } else {
            constraints.min.width.max(420.0)
        };
        let content_w = (width - PAD * 2.0).max(0.0);
        let popup_block_h = self.popup_block_h();

        if self.popup_visible() {
            self.popup.measure(
                ctx,
                Constraints::tight(Size::new(content_w, COMPOSER_POPUP_H)),
            );
        } else {
            self.popup
                .measure(ctx, Constraints::tight(Size::new(content_w, 0.0)));
        }

        let field = self.field.measure(
            ctx,
            Constraints::new(
                Size::new(content_w, 0.0),
                Size::new(content_w, f32::INFINITY),
            ),
        );

        constraints.clamp(Size::new(
            width,
            COMPOSER_PAD_Y * 2.0 + popup_block_h + field.height,
        ))
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        let field_h = self
            .field
            .measured_size()
            .height
            .min((bounds.height() - COMPOSER_PAD_Y * 2.0).max(0.0));
        let field_y = bounds.max_y() - COMPOSER_PAD_Y - field_h;
        let popup_y = field_y - COMPOSER_SECTION_GAP - COMPOSER_POPUP_H;

        if self.popup_visible() {
            self.popup
                .arrange(ctx, Self::content_rect(bounds, popup_y, COMPOSER_POPUP_H));
        } else {
            self.popup
                .arrange(ctx, Self::content_rect(bounds, field_y, 0.0));
        }
        self.field
            .arrange(ctx, Self::content_rect(bounds, field_y, field_h));
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        if self.popup_visible() {
            self.popup.paint(ctx);
        }
        self.field.paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        if self.popup_visible() {
            self.popup.semantics(ctx);
        }
        self.field.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.popup);
        visitor.visit(&self.field);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.popup);
        visitor.visit(&mut self.field);
    }
}

struct ComposerInputScroll {
    scroll: WidgetPod,
    bar: WidgetPod,
}

impl ComposerInputScroll {
    const BAR_W: f32 = 8.0;

    fn new(scroll: WidgetPod, bar: WidgetPod) -> Self {
        Self { scroll, bar }
    }
}

impl Widget for ComposerInputScroll {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let width = if constraints.max.width.is_finite() {
            constraints.max.width
        } else {
            constraints.min.width.max(320.0)
        };
        if !constraints.max.height.is_finite() {
            let natural = self.scroll.measure(
                ctx,
                Constraints::new(Size::new(width, 0.0), Size::new(width, f32::INFINITY)),
            );
            // Mesh: the docked input grows to 160px, then scrolls.
            let height = natural
                .height
                .max(composer_input_min_height())
                .min(COMPOSER_INPUT_MAX_H)
                .max(constraints.min.height);
            self.scroll
                .measure(ctx, Constraints::tight(Size::new(width, height)));
            self.bar.measure(
                ctx,
                Constraints::tight(Size::new(Self::BAR_W.min(width), height)),
            );
            return constraints.clamp(Size::new(width, height));
        }
        let height = if constraints.max.height.is_finite() {
            constraints.max.height
        } else {
            constraints.min.height.max(composer_input_min_height())
        };
        let size = constraints.clamp(Size::new(width, height));
        self.scroll.measure(ctx, Constraints::tight(size));
        self.bar.measure(
            ctx,
            Constraints::tight(Size::new(Self::BAR_W.min(size.width), size.height)),
        );
        size
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        self.scroll.arrange(ctx, bounds);
        let bar_w = Self::BAR_W.min(bounds.width()).max(0.0);
        self.bar.arrange(
            ctx,
            Rect::new(bounds.max_x() - bar_w, bounds.y(), bar_w, bounds.height()),
        );
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.scroll.paint(ctx);
        self.bar.paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.scroll.semantics(ctx);
        self.bar.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.scroll);
        visitor.visit(&self.bar);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.scroll);
        visitor.visit(&mut self.bar);
    }
}

/// Build the chat tab content.
pub fn view(shared: Shared) -> impl Widget {
    ChatView::new(shared)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{Backend, Connection};
    use crate::state::AppState;
    use std::time::{Duration, Instant};

    fn seed_default_provider(state: &mut ChatState) {
        if state.providers.is_empty() {
            state.providers.push(ProviderSummary {
                id: "openai".to_string(),
                account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
                name: "OpenAI".to_string(),
                models: vec!["gpt-5".to_string()],
            });
        }
        state.models_loading = false;
    }

    fn publish_test_camera_frame(
        state: &ChatState,
        sequence: u64,
        width: u32,
        height: u32,
        rgba: impl Into<Arc<[u8]>>,
    ) -> Arc<sinomo_video::NativeVideoSurfaceFrame> {
        state
            .media_setup
            .camera_surface
            .publish(sinomo_video::NativeVideoSurfaceFrame {
                revision: 0,
                sequence,
                width,
                height,
                timestamp_ns: sequence.saturating_mul(33_333_333),
                duration_ns: 33_333_333,
                captured_at_unix_ms: u128::from(sequence.saturating_mul(33)),
                rgba: rgba.into(),
            })
    }

    fn seed_test_agents(state: &mut ChatState) {
        state.agents = vec![
            AgentSummary {
                key: "main-agent".to_string(),
                name: "main-agent".to_string(),
                description: String::new(),
                tool_runtime: ChatToolRuntimeSelection::default(),
            },
            AgentSummary {
                key: "coding-agent".to_string(),
                name: "coding-agent".to_string(),
                description: String::new(),
                tool_runtime: ChatToolRuntimeSelection::default(),
            },
        ];
        state.info.agent = "main-agent".to_string();
    }

    #[test]
    fn transcript_character_selection_copies_original_markdown() {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        let theme = shared.theme();
        let pal = palette(&shared);
        let source = "Use **strong** and `code`.\n\n```rust\nfn main() {}\n```";
        let render = chat_render::markdown_rich_render(
            source,
            TranscriptView::markdown_styles(&pal, &theme, pal.text),
        );
        let rendered = render.source_map.rendered().to_string();
        let strong_start = rendered.find("strong").expect("rendered strong");

        let transcript = TranscriptView::new(shared, SelectionScope::new());
        transcript
            .transcript_message_sources
            .borrow_mut()
            .push(TranscriptMessageSource {
                message_key: 7,
                source_map: render.source_map,
            });
        transcript.selection.set(Some((
            TranscriptTextPoint {
                message_key: 7,
                offset: strong_start,
            },
            TranscriptTextPoint {
                message_key: 7,
                offset: strong_start + "strong".len(),
            },
        )));
        assert_eq!(transcript.selection_text().as_deref(), Some("**strong**"));

        transcript.selection.set(Some((
            TranscriptTextPoint {
                message_key: 7,
                offset: strong_start + 2,
            },
            TranscriptTextPoint {
                message_key: 7,
                offset: strong_start + 5,
            },
        )));
        assert_eq!(transcript.selection_text().as_deref(), Some("ron"));
    }

    #[test]
    fn chat_small_and_code_text_styles_follow_theme_tokens_and_font_stacks() {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        let theme = shared.theme();
        let pal = palette(&shared);

        let small = small_text_style(&shared);
        assert_eq!(small.font_size, theme.text.sm.size);
        assert_eq!(small.line_height, theme.text.sm.line_height);
        assert_eq!(small.font_families, theme.body_text_style().font_families);

        let markdown = TranscriptView::markdown_styles(&pal, &theme, pal.text);
        assert_eq!(
            markdown.inline_code.font_families,
            theme.mono_text_style(pal.text).font_families
        );

        let document = TranscriptView::code_text_document(
            &ChatCodeBlock {
                language: Some("rust".to_string()),
                text: "let answer = 42;".to_string(),
                is_diff: false,
            },
            false,
            &pal,
            &theme,
        );
        let style = &document.paragraphs[0].spans[0].style;
        assert_eq!(style.font_size, theme.text.sm.size);
        assert_eq!(style.line_height, theme.text.sm.line_height);
        assert_eq!(
            style.font_families,
            theme.mono_text_style(style.color).font_families
        );
    }

    #[test]
    fn headless_chat_state_loads_agent_catalog_synchronously() {
        let backend = Backend::local_fallback();
        let state = ChatState::new(&backend);

        assert!(
            !state.agents.is_empty(),
            "headless construction should load agents synchronously"
        );
        assert!(
            !state.agents_loading,
            "headless construction must not schedule a background agent load"
        );
    }

    #[test]
    fn background_agent_list_load_upgrades_built_ins() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        // Mirror the interactive boot: built-ins only, catalog deferred.
        state.agents = built_in_agent_summaries();
        state.agents_loading = true;

        assert!(
            state.start_agent_list_load(backend.data_dir.clone(), None),
            "background agent load should start"
        );
        assert!(
            !state.start_agent_list_load(backend.data_dir.clone(), None),
            "a second load must not start while one is in flight"
        );

        let deadline = Instant::now() + Duration::from_secs(30);
        while !state.pump_agent_list() {
            assert!(
                Instant::now() < deadline,
                "agent list load should resolve well within its budget"
            );
            std::thread::sleep(Duration::from_millis(25));
        }

        assert!(!state.agents_loading);
        assert!(state.agent_list_rx.is_none());
        assert!(
            state.agents.iter().any(|agent| agent.key == "main-agent"),
            "stored/built-in catalog should include the default agent: {:?}",
            state
                .agents
                .iter()
                .map(|agent| agent.key.as_str())
                .collect::<Vec<_>>()
        );
    }

    struct TempAttachmentFile {
        path: PathBuf,
        dir: PathBuf,
    }

    impl Drop for TempAttachmentFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn temp_attachment_file(slug: &str, file_name: &str, contents: &[u8]) -> TempAttachmentFile {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "sinomo-app-chat-attachment-{slug}-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create temp attachment dir");
        let path = dir.join(file_name);
        std::fs::write(&path, contents).expect("write temp attachment");
        TempAttachmentFile { path, dir }
    }

    fn pending_attachment(file_name: &str, size_bytes: u64) -> PendingChatAttachment {
        PendingChatAttachment {
            source: ChatAttachmentSource::LocalPath(PathBuf::from(format!("C:\\tmp\\{file_name}"))),
            file_name: file_name.to_string(),
            size_bytes,
        }
    }

    #[test]
    fn transcript_layout_mode_matches_mesh_desktop_mobile_split() {
        assert_eq!(
            TranscriptLayoutMode::for_width(719.0),
            TranscriptLayoutMode::Bubble
        );
        assert_eq!(
            TranscriptLayoutMode::for_width(720.0),
            TranscriptLayoutMode::Row
        );
    }

    #[test]
    fn session_meta_label_exposes_agents_and_save_state() {
        let channel = SessionRef {
            id: "channel-meta".to_string(),
            title: "Release triage".to_string(),
            kind: ConversationKind::Channel,
            updated_unix: 0,
            participant_count: 2,
            participant_names: Vec::new(),
        };
        let chat = SessionRef {
            id: "chat-meta".to_string(),
            title: "Old question".to_string(),
            kind: ConversationKind::Chat,
            updated_unix: 0,
            participant_count: 1,
            participant_names: Vec::new(),
        };
        let untitled_channel = SessionRef {
            id: "channel-untitled".to_string(),
            title: "New channel".to_string(),
            kind: ConversationKind::Channel,
            updated_unix: 0,
            participant_count: 2,
            participant_names: Vec::new(),
        };

        assert_eq!(session_meta_label(&channel), "2 agents · not saved");
        assert_eq!(
            session_meta_label(&untitled_channel),
            "untitled · 2 agents · not saved"
        );
        assert_eq!(session_meta_label(&chat), "not saved");
        assert_eq!(
            session_kind_icon_description(channel.kind),
            "Shared channel"
        );
        assert_eq!(session_kind_icon_description(chat.kind), "Direct chat");
    }

    #[test]
    fn session_filter_matches_title_kind_and_metadata() {
        let channel = SessionRef {
            id: "channel-filter".to_string(),
            title: "Release triage".to_string(),
            kind: ConversationKind::Channel,
            updated_unix: 0,
            participant_count: 2,
            participant_names: Vec::new(),
        };
        let chat = SessionRef {
            id: "chat-filter".to_string(),
            title: "Old question".to_string(),
            kind: ConversationKind::Chat,
            updated_unix: 0,
            participant_count: 1,
            participant_names: Vec::new(),
        };

        assert!(session_matches_filter(
            &channel,
            &normalized_session_filter("release")
        ));
        assert!(session_matches_filter(
            &channel,
            &normalized_session_filter("channel")
        ));
        assert!(session_matches_filter(
            &channel,
            &normalized_session_filter("2 agents")
        ));
        assert!(session_matches_filter(
            &SessionRef {
                id: "channel-untitled".to_string(),
                title: "New channel".to_string(),
                kind: ConversationKind::Channel,
                updated_unix: 0,
                participant_count: 2,
                participant_names: Vec::new(),
            },
            &normalized_session_filter("untitled")
        ));
        assert!(session_matches_filter(
            &chat,
            &normalized_session_filter("chat")
        ));
        assert!(!session_matches_filter(
            &chat,
            &normalized_session_filter("release")
        ));
    }

    #[test]
    fn session_kind_filter_limits_visible_indices() {
        let sessions = vec![
            SessionRef {
                id: "chat-filter-a".to_string(),
                title: "Current chat".to_string(),
                kind: ConversationKind::Chat,
                updated_unix: 0,
                participant_count: 1,
                participant_names: Vec::new(),
            },
            SessionRef {
                id: "channel-filter".to_string(),
                title: "Release triage".to_string(),
                kind: ConversationKind::Channel,
                updated_unix: 0,
                participant_count: 2,
                participant_names: Vec::new(),
            },
            SessionRef {
                id: "chat-filter-b".to_string(),
                title: "Release note question".to_string(),
                kind: ConversationKind::Chat,
                updated_unix: 0,
                participant_count: 1,
                participant_names: Vec::new(),
            },
        ];

        assert_eq!(
            filtered_session_indices(&sessions, "", SessionKindFilter::All),
            vec![0, 1, 2]
        );
        assert_eq!(
            filtered_session_indices(&sessions, "", SessionKindFilter::Chats),
            vec![0, 2]
        );
        assert_eq!(
            filtered_session_indices(&sessions, "", SessionKindFilter::Channels),
            vec![1]
        );
        assert_eq!(
            filtered_session_indices(
                &sessions,
                &normalized_session_filter("release"),
                SessionKindFilter::Channels,
            ),
            vec![1]
        );
        assert_eq!(
            filtered_session_indices(
                &sessions,
                &normalized_session_filter("release"),
                SessionKindFilter::Chats,
            ),
            vec![2]
        );
    }

    #[test]
    fn chat_session_counts_distinguish_empty_loading_from_empty_loaded() {
        let (_backend, mut state) = isolated_chat_state("chat-list-loading-label");

        assert_eq!(
            loading_sessions_copy(&state),
            ("Loading history...", "Saved chats and channels")
        );
        state.session_list_slow_notice_shown = true;
        assert_eq!(
            loading_sessions_copy(&state),
            ("History still loading...", "You can keep chatting")
        );
        state.session_list_slow_notice_shown = false;
        state.session_list_loading = true;
        assert_eq!(chats_heading_text(&state), "CONVERSATIONS · loading");
        assert_eq!(
            session_kind_filter_count_label(&state.sessions, SessionKindFilter::All, true),
            "..."
        );

        state.session_filter = "release".to_string();
        state.session_kind_filter = SessionKindFilter::Channels;
        assert_eq!(chats_heading_text(&state), "CHANNELS · loading");

        state.session_list_loading = false;
        assert_eq!(chats_heading_text(&state), "CHANNELS · 0/0");
        assert_eq!(
            session_kind_filter_count_label(&state.sessions, SessionKindFilter::Channels, false),
            "0"
        );

        state.session_filter.clear();
        state.session_kind_filter = SessionKindFilter::All;
        state.session_list_loading = true;
        state.sessions.push(SessionRef {
            id: "chat-known-during-refresh".to_string(),
            title: "Known chat".to_string(),
            kind: ConversationKind::Chat,
            updated_unix: 0,
            participant_count: 1,
            participant_names: Vec::new(),
        });
        assert_eq!(chats_heading_text(&state), "CONVERSATIONS · 1");
        assert_eq!(
            session_kind_filter_count_label(&state.sessions, SessionKindFilter::All, true),
            "1"
        );
    }

    #[test]
    fn rendered_empty_loading_sidebar_uses_product_copy() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            chat.sessions.clear();
            chat.session_list_loading = true;
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat sessions loading")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::ListItem)
            .with_name("Loading history...")
            .with_description("Saved chats and channels")
            .expect()
            .to_be_visible()?;

        Ok(())
    }

    #[test]
    fn rendered_slow_loading_sidebar_collapses_sifs_recovery_actions_until_details()
    -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            chat.sessions.clear();
            chat.session_list_loading = true;
            chat.session_list_slow_notice_shown = true;
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat sessions slow loading")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::ListItem)
            .with_name("History still loading...")
            .with_description("You can keep chatting")
            .expect()
            .to_be_visible()?;
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Details")
                .count()?,
            0,
            "the collapsed sidebar should not show a second footer status"
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Hide")
                .count()?,
            0
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Retry")
                .count()?,
            0,
            "slow SIFS recovery controls should start behind Details"
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Check SIFS")
                .count()?,
            0,
            "SIFS diagnostics should not occupy the primary sidebar until expanded"
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Connection")
                .count()?,
            0,
            "connection diagnostics should stay behind Details"
        );

        window
            .get_by_role(SemanticsRole::ListItem)
            .with_name("History still loading...")
            .click()?;
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Hide")
                .count()?,
            1,
            "expanding from the loading row should expose the diagnostics footer"
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Retry")
                .count()?,
            1
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Check SIFS")
                .count()?,
            1
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Connection")
                .count()?,
            1
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Check SIFS")
            .click()?;
        assert_eq!(shared.borrow().active_tab, Tab::Files);

        Ok(())
    }

    #[test]
    fn slow_loading_sidebar_connection_action_opens_settings() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            chat.sessions.clear();
            chat.session_list_loading = true;
            chat.session_list_slow_notice_shown = true;
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat sessions slow loading settings")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::ListItem)
            .with_name("History still loading...")
            .click()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Connection")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        assert_eq!(
            shared.settings().selected,
            Some(SettingsSection::Connection)
        );

        Ok(())
    }

    #[test]
    fn empty_sessions_copy_reflects_active_unlisted_chat() {
        let (_backend, mut state) = isolated_chat_state("active-unlisted-chat-copy");
        assert_eq!(
            empty_sessions_copy(&state, "", SessionKindFilter::All),
            ("No conversations yet", None)
        );

        state
            .messages
            .push(ChatMessage::user("Draft a rollout plan."));
        assert_eq!(
            empty_sessions_copy(&state, "", SessionKindFilter::All),
            ("Current chat is open", Some("Saved after history syncs"))
        );
        assert_eq!(
            empty_sessions_copy(&state, "", SessionKindFilter::Chats),
            ("Current chat is open", Some("Saved after history syncs"))
        );
        assert_eq!(
            empty_sessions_copy(&state, "", SessionKindFilter::Channels),
            ("No channels yet", None)
        );
        assert_eq!(
            empty_sessions_copy(&state, "release", SessionKindFilter::All),
            ("No matching conversations", None)
        );
    }

    #[test]
    fn agent_list_detail_uses_catalog_description_and_built_in_fallbacks() {
        let described = AgentSummary {
            key: "custom-agent".to_string(),
            name: "Custom".to_string(),
            description: "Investigates product workflows and summarizes risky edges.".to_string(),
            tool_runtime: ChatToolRuntimeSelection::default(),
        };
        let coding = AgentSummary {
            key: "coding-agent".to_string(),
            name: "coding-agent".to_string(),
            description: String::new(),
            tool_runtime: ChatToolRuntimeSelection::default(),
        };

        assert_eq!(
            agent_list_detail(&described),
            "Investigates product workflows an..."
        );
        assert_eq!(agent_list_detail(&coding), "Code and verification");
    }

    fn isolated_chat_state(_slug: &str) -> (Backend, ChatState) {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.sessions.clear();
        (backend, state)
    }

    #[test]
    fn compact_chat_header_opens_conversation_and_agent_drawer() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        let reader: ThemeReader = {
            let s = shared.clone();
            Rc::new(move || s.theme())
        };
        let body = SizedBox::new()
            .height(COMPACT_HEADER_H)
            .with_child(compact_chat_header(&shared, &reader));
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Compact chat navigation")
                    .root(ChatDialogHost::new(
                        body,
                        shared.clone(),
                        Rc::clone(&reader),
                    )),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        app.run_until_idle()?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Open conversations and agents")
            .click()?;
        app.run_until_idle()?;

        assert!(shared.chat().compact_drawer_open);
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Dialog)
                .with_name("Conversations and agents")
                .count()?,
            1
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::List)
                .with_name("chat-agents")
                .count()?,
            1,
            "agent access must remain available below the sidebar breakpoint"
        );
        Ok(())
    }

    #[test]
    fn transcript_semantics_include_message_text_and_live_progress() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        {
            let mut chat = shared.chat_mut();
            chat.messages = vec![
                ChatMessage::user("Please inspect the cluster"),
                ChatMessage::model("Inspecting routes"),
            ];
            chat.info.agent = "main-agent".to_string();
            chat.streaming = true;
            chat.notice = Some("Checking cluster routes".to_string());
        }
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat semantics")
                    .root(TranscriptView::new(shared, SelectionScope::new())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        app.run_until_idle()?;
        assert_eq!(
            window
                .get_by_role(SemanticsRole::ListItem)
                .with_name("main-agent: Inspecting routes")
                .count()?,
            1
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::BusyIndicator)
                .with_name("Checking cluster routes")
                .count()?,
            1
        );
        Ok(())
    }

    fn mock_chunked_timeline_shared(
        title: &str,
        total_messages: u64,
        loaded_start_sequence: u64,
        loaded_end_sequence: u64,
        overview_sections: usize,
    ) -> Shared {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let session_id = SessionId::generate().to_string();
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.sessions = vec![SessionRef {
                id: session_id.clone(),
                title: title.to_string(),
                kind: ConversationKind::Channel,
                updated_unix: 1,
                participant_count: 2,
                participant_names: vec!["main-agent".to_string(), "coding-agent".to_string()],
            }];
            chat.session_id = session_id;
            chat.models_loading = false;
            chat.messages = (loaded_start_sequence..=loaded_end_sequence)
                .map(|sequence| {
                    let text = format!("chunked message {sequence}");
                    if sequence % 2 == 0 {
                        ChatMessage::model(format!("main-agent: {text}")).with_sequence(sequence)
                    } else {
                        ChatMessage::user(text).with_sequence(sequence)
                    }
                })
                .collect();
            let section_count = overview_sections.max(1) as u64;
            let section_size = total_messages.div_ceil(section_count).max(1);
            chat.timeline = ConversationTimelineState {
                total_messages,
                chunk_size: section_size,
                loaded_start_sequence: Some(loaded_start_sequence),
                loaded_end_sequence: Some(loaded_end_sequence),
                has_older: loaded_start_sequence > 1,
                has_newer: loaded_end_sequence < total_messages,
                overview: (0..overview_sections)
                    .scan(1_u64, |start, index| {
                        if *start > total_messages {
                            return None;
                        }
                        let segment_start = *start;
                        let segment_end = if index + 1 == overview_sections {
                            total_messages
                        } else {
                            (*start + section_size - 1).min(total_messages)
                        };
                        *start = segment_end.saturating_add(1);
                        Some(ConversationTimelineSegment {
                            start_sequence: segment_start,
                            end_sequence: segment_end,
                            message_count: segment_end.saturating_sub(segment_start) + 1,
                            first_created_unix: Some(index as i64 * 86_400),
                            last_created_unix: Some(index as i64 * 86_400 + 3_600),
                            preview: Some(format!("chunk {} starts at {segment_start}", index + 1)),
                        })
                    })
                    .collect(),
                discovered_message_ids: BTreeSet::new(),
            };
            chat.stick_bottom = false;
            chat.scroll = 0.0;
        }
        shared
    }

    #[test]
    fn rendered_chunked_timeline_window_exposes_overview_and_visible_message_semantics()
    -> sui::Result<()> {
        let shared = mock_chunked_timeline_shared("Chunked timeline", 1_024, 401, 800, 8);
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chunked timeline")
                    .root(view(shared)),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        app.run_until_idle()?;
        let snapshot = window.snapshot()?;
        let summary = snapshot
            .accessibility
            .nodes
            .iter()
            .find_map(|node| {
                (node.role == SemanticsRole::Text)
                    .then(|| node.name.as_deref())
                    .flatten()
                    .filter(|name| name.starts_with("Messages 401-800 of 1024"))
            })
            .expect("chunked transcript timeline summary");

        assert!(summary.contains("8 sections"));
        assert!(summary.contains("span 1-1024"));
        assert!(summary.contains("older/newer available"));
        assert!(
            snapshot.accessibility.nodes.iter().any(|node| {
                node.role == SemanticsRole::ListItem
                    && node
                        .name
                        .as_deref()
                        .is_some_and(|name| name.contains("chunked message 401"))
            }),
            "the visible transcript must expose message text to accessibility"
        );
        assert!(
            !snapshot.accessibility.nodes.iter().any(|node| {
                node.name
                    .as_deref()
                    .is_some_and(|name| name.contains("chunked message 800"))
            }),
            "offscreen rows should stay out of the bounded accessibility projection"
        );

        Ok(())
    }

    #[test]
    fn rendered_chunked_timeline_large_window_keeps_semantics_bounded() -> sui::Result<()> {
        let loaded_count = TRANSCRIPT_MAX_LOADED_MESSAGES as u64;
        let loaded_start = 20_001;
        let loaded_end = loaded_start + loaded_count - 1;
        let shared =
            mock_chunked_timeline_shared("Long channel", 50_000, loaded_start, loaded_end, 20);
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Long channel")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        app.run_until_idle()?;
        let snapshot = window.snapshot()?;
        let expected_summary_prefix = format!("Messages {loaded_start}-{loaded_end} of 50000");
        assert!(
            snapshot.accessibility.nodes.iter().any(|node| {
                node.role == SemanticsRole::Text
                    && node
                        .name
                        .as_deref()
                        .is_some_and(|name| name.starts_with(&expected_summary_prefix))
            }),
            "large rendered transcript should expose the loaded range summary"
        );
        assert_eq!(
            shared.chat().messages.len(),
            TRANSCRIPT_MAX_LOADED_MESSAGES,
            "mock should exercise the same bounded transcript window the app keeps loaded"
        );
        let message_semantics = snapshot
            .accessibility
            .nodes
            .iter()
            .filter(|node| {
                node.name
                    .as_deref()
                    .is_some_and(|name| name.contains("chunked message "))
            })
            .count();
        assert!(
            message_semantics > 0,
            "visible messages should be semantic rows"
        );
        assert!(
            message_semantics < 40,
            "loaded transcript windows should expose only viewport rows, got {message_semantics}"
        );

        Ok(())
    }

    fn seed_backend_chat(
        data_dir: &SinomoDataDir,
        channel_id: &SessionId,
        title: &str,
        messages: &[ChatMessage],
    ) -> AgentChannel {
        let store = SifsAgentChannelStore::open(data_dir).expect("open channel store");
        let mut channel = AgentChannel::new(channel_id.clone(), title);
        let participant_id = channel.ensure_participant("main-agent", "main-agent");
        let participant = channel
            .participant(&participant_id)
            .expect("participant")
            .clone();
        for message in messages {
            let channel_message = match (message.source, message.kind) {
                (ChatMessageSource::User, _) => channel.append_user_message(
                    AGENT_CHANNEL_APP_USER_ID,
                    "User",
                    message.text.clone(),
                    AgentChannelSource::app(),
                ),
                (ChatMessageSource::System, ChatMessageKind::Error) => {
                    channel.append_system_error(message.text.clone(), AgentChannelSource::app())
                }
                (ChatMessageSource::System, _) => {
                    channel.append_system_info(message.text.clone(), AgentChannelSource::app())
                }
                (ChatMessageSource::Model, _) => channel.append_agent_message(
                    &participant,
                    message.text.clone(),
                    AgentChannelSource::app(),
                ),
            };
            store
                .append_message(&channel.channel_id, &channel_message)
                .expect("append backend message");
        }
        store.save_channel(&channel).expect("save backend chat");
        channel
    }

    fn load_backend_chat_messages(
        data_dir: &SinomoDataDir,
        channel_id: &SessionId,
    ) -> Vec<ChatMessage> {
        load_conversation_messages(
            data_dir,
            None,
            &channel_id.to_string(),
            ConversationKind::Chat,
        )
        .expect("load backend chat")
    }

    #[test]
    fn typing_draft_does_not_rebuild_focused_composer() {
        let (_backend, mut state) = isolated_chat_state("typing-draft");
        let revision = state.draft_revision;

        state.set_draft("h".to_string());
        state.set_draft("hello".to_string());

        assert_eq!(state.input, "hello");
        assert_eq!(state.cursor, "hello".len());
        assert_eq!(
            state.draft_revision, revision,
            "typing must not change the programmatic composer rebuild key"
        );
    }

    #[test]
    fn open_session_loads_transcript_asynchronously() {
        let (_backend, mut state) = isolated_chat_state("open-session-async");
        seed_default_provider(&mut state);
        let channel_id = SessionId::generate();
        seed_backend_chat(
            &state.data_dir,
            &channel_id,
            "Saved plan",
            &[
                ChatMessage::user("original prompt"),
                ChatMessage::model("original answer"),
            ],
        );
        let session_id = channel_id.to_string();
        state.sessions = vec![SessionRef {
            id: session_id.clone(),
            title: "Saved plan".to_string(),
            kind: ConversationKind::Chat,
            updated_unix: 1,
            participant_count: 1,
            participant_names: Vec::new(),
        }];
        state.set_draft("follow up".to_string());
        state.media_call_details_open = true;
        state.media_setup.start();
        state.toggle_media_call_mic();
        state.toggle_media_call_camera();
        assert!(state.media_setup.is_running());

        state.open_session(0);

        assert_eq!(state.session_id, session_id);
        assert!(!state.media_setup.is_running());
        assert!(state.media_setup.microphone_enabled());
        assert!(state.media_setup.camera_enabled());
        assert!(state.media_call.mic_enabled);
        assert!(state.media_call.camera_enabled);
        assert!(!state.media_call_details_open);
        assert!(state.transcript_load_pending());
        assert_eq!(state.messages.len(), 1);
        assert_eq!(state.messages[0].source, ChatMessageSource::System);
        assert_eq!(state.messages[0].kind, ChatMessageKind::Info);
        assert!(
            state.messages[0].text.contains("Loading chat history"),
            "selection should show a non-blocking loading row"
        );
        assert!(
            !state.can_send(),
            "composer should wait until saved history is hydrated"
        );

        state.submit();
        assert_eq!(
            state.notice.as_deref(),
            Some("wait for this chat history to finish loading")
        );

        wait_for(&mut state, Duration::from_secs(5), |state| {
            !state.transcript_load_pending() && state.messages.len() == 2
        });

        assert_eq!(state.messages[0].text, "original prompt");
        assert_eq!(state.messages[1].text, "original answer");
        assert_eq!(state.notice, None);
        assert!(
            state.can_send(),
            "composer should become available after the transcript loads"
        );
    }

    #[test]
    fn reconcile_latest_deduplicates_by_id_and_resequences_backdated_records() {
        let mut visible = vec![
            ChatMessage::user("first").with_durable_identity("message-first", 1),
            ChatMessage::user("second").with_durable_identity("message-second", 2),
        ];
        let latest = vec![
            ChatMessage::user("arrived late").with_durable_identity("message-delayed", 1),
            ChatMessage::user("first").with_durable_identity("message-first", 2),
            ChatMessage::user("second").with_durable_identity("message-second", 3),
        ];

        assert_eq!(
            reconcile_latest_timeline_messages(&mut visible, latest.clone()),
            1,
            "the delayed immutable record is the only new message"
        );
        assert_eq!(
            visible
                .iter()
                .map(|message| (message.message_id.as_deref(), message.sequence))
                .collect::<Vec<_>>(),
            vec![
                (Some("message-delayed"), Some(1)),
                (Some("message-first"), Some(2)),
                (Some("message-second"), Some(3)),
            ],
            "stable ids must update the old rows to their canonical resequenced positions"
        );

        assert_eq!(
            reconcile_latest_timeline_messages(&mut visible, latest),
            0,
            "polling the same durable tail must be idempotent"
        );
        assert_eq!(visible.len(), 3);
    }

    #[test]
    fn reconcile_latest_promotes_local_send_tail_without_collapsing_repeated_messages() {
        let mut visible = vec![
            ChatMessage::user("repeat").with_durable_identity("message-existing", 1),
            ChatMessage::user("repeat"),
            ChatMessage::model("response"),
        ];
        let latest = vec![
            // This different durable message has the same contents but was
            // backdated into the loaded range; it must be added, not matched
            // to the pending local row at the tail.
            ChatMessage::user("repeat").with_durable_identity("message-backdated", 1),
            ChatMessage::user("repeat").with_durable_identity("message-existing", 2),
            // These two records are the durable forms of the local send and
            // streamed response already visible above.
            ChatMessage::user("repeat").with_durable_identity("message-local-user", 3),
            ChatMessage::model("response").with_durable_identity("message-local-model", 4),
        ];

        assert_eq!(
            reconcile_latest_timeline_messages(&mut visible, latest),
            1,
            "only the genuinely remote backdated record should add a visible row"
        );
        assert_eq!(visible.len(), 4, "local rows should be promoted in place");
        assert_eq!(
            visible
                .iter()
                .map(|message| message.message_id.as_deref())
                .collect::<Vec<_>>(),
            vec![
                Some("message-backdated"),
                Some("message-existing"),
                Some("message-local-user"),
                Some("message-local-model"),
            ],
            "identical durable messages remain distinct while the ordered ephemeral tail gains ids"
        );
    }

    #[test]
    fn timeline_merge_uses_sequence_only_when_message_id_is_absent() {
        let mut durable =
            vec![ChatMessage::user("first device").with_durable_identity("message-first", 7)];
        assert_eq!(
            append_timeline_messages(
                &mut durable,
                vec![ChatMessage::user("second device").with_durable_identity("message-second", 7),],
            ),
            1,
            "different durable ids must not be discarded merely because a stale sequence collides"
        );
        assert_eq!(
            append_timeline_messages(
                &mut durable,
                vec![ChatMessage::user("legacy view of second device").with_sequence(7)],
            ),
            0,
            "an id-less compatibility row falls back to every sequence already in the merge"
        );

        let mut legacy = vec![ChatMessage::user("legacy").with_sequence(7)];
        assert_eq!(
            append_timeline_messages(
                &mut legacy,
                vec![ChatMessage::user("legacy duplicate").with_sequence(7)],
            ),
            0,
            "sequence remains the compatibility fallback for rows without durable ids"
        );
    }

    #[test]
    fn pushed_namespace_event_reconciles_remote_timeline_without_reopening_chat() {
        let (_backend, mut state) = isolated_chat_state("multi-device-timeline-sync");
        seed_default_provider(&mut state);
        let channel_id = SessionId::generate();
        seed_backend_chat(
            &state.data_dir,
            &channel_id,
            "Shared chat",
            &[ChatMessage::user("first device")],
        );
        state.sessions = vec![SessionRef {
            id: channel_id.to_string(),
            title: "Shared chat".to_string(),
            kind: ConversationKind::Chat,
            updated_unix: 1,
            participant_count: 1,
            participant_names: Vec::new(),
        }];
        state.open_session(0);
        wait_for(&mut state, Duration::from_secs(5), |state| {
            !state.transcript_load_pending()
                && state
                    .messages
                    .iter()
                    .any(|message| message.text == "first device")
        });

        let store = SifsAgentChannelStore::open(&state.data_dir).expect("open second device store");
        let mut remote = store
            .load_channel(&channel_id)
            .expect("load shared chat")
            .expect("shared chat exists");
        let message = remote.append_user_message(
            "user.remote",
            "Remote user",
            "second device",
            AgentChannelSource::app(),
        );
        store
            .append_message(&channel_id, &message)
            .expect("append remote message");
        store.save_channel(&remote).expect("save remote snapshot");

        state.reconcile_after_daemon_event();
        wait_for(&mut state, Duration::from_secs(5), |state| {
            !state.transcript_load_pending()
                && state
                    .messages
                    .iter()
                    .any(|message| message.text == "second device")
        });

        assert_eq!(
            state
                .messages
                .iter()
                .filter(|message| message.text == "second device")
                .count(),
            1,
            "remote refresh should merge each immutable message once"
        );
    }

    #[test]
    fn pushed_namespace_event_reconciles_backdated_message_with_unchanged_timestamp() {
        let (_backend, mut state) = isolated_chat_state("multi-device-backdated-sync");
        seed_default_provider(&mut state);
        let channel_id = SessionId::generate();
        let seeded = seed_backend_chat(
            &state.data_dir,
            &channel_id,
            "Shared backdated chat",
            &[ChatMessage::user("original message")],
        );
        let original_created_at = seeded
            .transcript
            .first()
            .expect("seeded transcript")
            .created_at;
        state.sessions = vec![SessionRef {
            id: channel_id.to_string(),
            title: "Shared backdated chat".to_string(),
            kind: ConversationKind::Chat,
            updated_unix: 1,
            participant_count: 1,
            participant_names: Vec::new(),
        }];
        state.open_session(0);
        wait_for(&mut state, Duration::from_secs(5), |state| {
            !state.transcript_load_pending()
                && state
                    .messages
                    .iter()
                    .any(|message| message.text == "original message")
        });
        let original_message_id = state
            .messages
            .iter()
            .find(|message| message.text == "original message")
            .and_then(|message| message.message_id.clone())
            .expect("durable original message id");

        let store = SifsAgentChannelStore::open(&state.data_dir).expect("open second device store");
        let mut remote = store
            .load_channel(&channel_id)
            .expect("load shared chat")
            .expect("shared chat exists");
        let mut delayed = remote.append_user_message(
            "user.remote",
            "Remote user",
            "arrived before the cursor",
            AgentChannelSource::app(),
        );
        delayed.created_at = original_created_at - time::Duration::seconds(1);
        let delayed_message_id = delayed.message_id.to_string();
        store
            .append_message(&channel_id, &delayed)
            .expect("append backdated remote message");
        store.save_channel(&remote).expect("save remote snapshot");

        let summary = store
            .load_channel_summary(&channel_id)
            .expect("load channel summary")
            .expect("channel summary exists");
        let same_updated_unix = summary.updated_at.unix_timestamp();
        state
            .sessions
            .iter_mut()
            .find(|session| session.id == channel_id.to_string())
            .expect("active session")
            .updated_unix = same_updated_unix;
        assert_eq!(
            state
                .current_session_ref()
                .expect("current session")
                .updated_unix,
            same_updated_unix,
            "event-driven reconciliation must not depend on a newer second-resolution timestamp"
        );

        state.reconcile_after_daemon_event();
        wait_for(&mut state, Duration::from_secs(5), |state| {
            !state.transcript_load_pending()
                && state.messages.iter().any(|message| {
                    message.message_id.as_deref() == Some(delayed_message_id.as_str())
                })
        });

        let delayed = state
            .messages
            .iter()
            .find(|message| message.message_id.as_deref() == Some(delayed_message_id.as_str()))
            .expect("backdated message synced");
        let original = state
            .messages
            .iter()
            .find(|message| message.message_id.as_deref() == Some(original_message_id.as_str()))
            .expect("original message retained");
        assert_eq!(delayed.sequence, Some(2));
        assert_eq!(original.sequence, Some(1));
        assert_eq!(
            state
                .messages
                .iter()
                .filter(|message| {
                    message.message_id.as_deref() == Some(delayed_message_id.as_str())
                })
                .count(),
            1,
            "reconciliation must merge the immutable message exactly once"
        );
    }

    #[test]
    fn transcript_history_loads_older_backend_page_without_blocking_composer() {
        let (_backend, mut state) = isolated_chat_state("open-session-older-page");
        seed_default_provider(&mut state);
        let channel_id = SessionId::generate();
        let session_id = channel_id.to_string();
        let session = SessionRef {
            id: session_id.clone(),
            title: "Long chat".to_string(),
            kind: ConversationKind::Chat,
            updated_unix: 1,
            participant_count: 1,
            participant_names: Vec::new(),
        };
        state.session_id = session_id;
        state.sessions = vec![session.clone()];
        let total_messages = (VISIBLE_TRANSCRIPT_RECENT_LIMIT + 25) as u64;
        state.messages = (26..=total_messages)
            .map(|sequence| {
                ChatMessage::user(format!("message {}", sequence - 1)).with_sequence(sequence)
            })
            .collect();
        state.timeline = ConversationTimelineState {
            total_messages,
            chunk_size: VISIBLE_TRANSCRIPT_RECENT_LIMIT as u64,
            loaded_start_sequence: Some(26),
            loaded_end_sequence: Some(total_messages),
            has_older: true,
            has_newer: false,
            overview: Vec::new(),
            discovered_message_ids: BTreeSet::new(),
        };

        state.set_draft("follow up while history loads".to_string());
        assert!(state.maybe_start_older_transcript_load());
        assert!(
            state.can_send(),
            "older page loads should not block the composer"
        );

        // Replace the real SIFS worker receiver with a deterministic older-page result. Storage
        // integration is covered separately; this regression targets the nonblocking UI merge.
        let mode = TranscriptLoadMode::Older {
            before_sequence: 26,
        };
        let (tx, rx) = mpsc::channel();
        state.transcript_load_rx = Some(rx);
        state.transcript_load_mode = Some(mode);
        tx.send(TranscriptLoadWorkerResult {
            session,
            mode,
            result: Ok(ConversationTimelineLoad {
                messages: (1..=25)
                    .map(|sequence| {
                        ChatMessage::user(format!("message {}", sequence - 1))
                            .with_sequence(sequence)
                    })
                    .collect(),
                page: AgentChannelMessagePage {
                    channel_id,
                    messages: Vec::new(),
                    start_sequence: Some(1),
                    end_sequence: Some(25),
                    has_older: false,
                    has_newer: true,
                    total_messages,
                    chunk_size: VISIBLE_TRANSCRIPT_RECENT_LIMIT as u64,
                },
                overview: None,
                discovered_message_ids: Vec::new(),
            }),
        })
        .expect("queue older transcript page");
        wait_for(&mut state, Duration::from_secs(5), |state| {
            !state.transcript_load_pending()
                && state.messages.len() == VISIBLE_TRANSCRIPT_RECENT_LIMIT + 25
        });

        assert_eq!(state.messages[0].text, "message 0");
        assert_eq!(state.messages[24].text, "message 24");
        assert_eq!(state.messages[25].text, "message 25");
        assert_eq!(state.timeline.loaded_start_sequence, Some(1));
        assert_eq!(
            state.timeline.loaded_end_sequence,
            Some((VISIBLE_TRANSCRIPT_RECENT_LIMIT + 25) as u64)
        );
        assert!(!state.timeline.has_older);
        assert!(!state.timeline.has_newer);
    }

    #[test]
    fn transcript_load_timeout_surfaces_background_sifs_work() {
        let (_backend, mut state) = isolated_chat_state("open-session-slow");
        let (_tx, rx) = mpsc::channel();
        state.session_id = "slow-chat".to_string();
        state.sessions = vec![SessionRef {
            id: "slow-chat".to_string(),
            title: "Slow chat".to_string(),
            kind: ConversationKind::Chat,
            updated_unix: 1,
            participant_count: 1,
            participant_names: Vec::new(),
        }];
        state.messages = vec![ChatMessage::system_info(
            "Loading chat history from SIFS...",
        )];
        state.transcript_load_rx = Some(rx);
        state.transcript_load_mode = Some(TranscriptLoadMode::Initial);
        state.transcript_load_started_at =
            Some(Instant::now() - TRANSCRIPT_LOAD_UI_TIMEOUT - Duration::from_millis(10));

        assert!(state.pump_transcript_load());
        assert!(state.transcript_load_pending());
        assert!(state.transcript_load_slow_notice_shown);
        assert_eq!(state.messages.len(), 1);
        assert!(
            state.messages[0]
                .text
                .contains("Chat history is still loading from SIFS")
        );
        assert!(
            state
                .activity
                .iter()
                .any(|entry| entry.title == "chat load" && entry.tone == ActivityTone::Running)
        );
    }

    fn chat_sessions_list_y(window: &sui_testing::TestWindow) -> sui::Result<f32> {
        let snapshot = window.snapshot()?;
        Ok(snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::List && node.name.as_deref() == Some("chat-sessions")
            })
            .expect("chat sessions list semantics node")
            .bounds
            .y())
    }

    #[test]
    fn composer_control_reader_keeps_footer_text_compact() {
        let reader: ThemeReader = Rc::new(DefaultTheme::default);
        let theme = composer_control_reader(&reader)();
        let body = theme.body_text_style();
        let placeholder = theme.placeholder_text_style();

        assert_eq!(theme.control_size, Some(ControlSize::Small));
        assert_eq!(body.font_size, theme.text.sm.size);
        assert_eq!(body.line_height, theme.text.sm.line_height);
        assert_eq!(placeholder.font_size, theme.text.sm.size);
        assert_eq!(placeholder.line_height, theme.text.sm.line_height);
    }

    #[test]
    fn submit_pushes_user_and_agent_placeholder_and_sets_streaming() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.set_draft("hello there".to_string());
        state.submit();
        assert!(state.streaming, "submitting begins a streaming turn");
        assert_eq!(state.messages.len(), 2, "user message + agent placeholder");
        assert_eq!(state.messages[0].role, ChatRole::User);
        assert_eq!(state.messages[0].source, ChatMessageSource::User);
        assert_eq!(state.messages[0].kind, ChatMessageKind::Message);
        assert_eq!(state.messages[0].text, "hello there");
        assert_eq!(state.messages[1].role, ChatRole::Agent);
        assert_eq!(state.messages[1].source, ChatMessageSource::Model);
        assert_eq!(state.messages[1].kind, ChatMessageKind::Message);
        assert!(
            state.messages[1].text.is_empty(),
            "agent placeholder is blank"
        );
        assert!(state.input.is_empty(), "the draft is cleared on submit");
    }

    #[test]
    fn submit_before_ready_assigns_session_id_and_indexes_session() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.session_id.clear();

        state.set_draft("persist me before ready".to_string());
        state.submit();

        let session_id = state.session_id.clone();
        assert!(
            !session_id.is_empty(),
            "submitting before Ready must still create a visible session id"
        );
        assert!(
            state
                .sessions
                .iter()
                .any(|session| session.id == session_id),
            "new conversation should appear in the sessions list after submit"
        );

        state.apply_event(ChatEvent::Ready {
            session_id: SessionId::generate().to_string(),
            info: RuntimeInfo::default(),
        });
        assert_eq!(
            state.session_id, session_id,
            "a late initial Ready event must not move the visible conversation to another id"
        );
    }

    #[test]
    fn agent_switch_is_blocked_after_start_but_allowed_after_new_chat() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_test_agents(&mut state);
        state.session_id = SessionId::generate().to_string();
        state.messages = vec![ChatMessage::user("already started")];

        state.select_agent(1);

        assert_eq!(
            state.info.agent, "main-agent",
            "started sessions keep their original agent"
        );
        assert_eq!(
            state.notice.as_deref(),
            Some("start a new chat before switching agents")
        );

        state.new_session();
        state.select_agent(1);

        assert!(
            state.messages.is_empty(),
            "new chat creates an empty switchable session"
        );
        assert_eq!(
            state.info.agent, "coding-agent",
            "agent selection is allowed again in a fresh chat"
        );
    }

    #[test]
    fn workspace_runtime_events_update_the_selector_and_surface_errors() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        let workspace = PathBuf::from("/tmp/sinomo-selected-workspace");

        state.apply_event(ChatEvent::ToolRuntime(ChatToolRuntimeSelection {
            workspace_root: Some(workspace.clone()),
            ..ChatToolRuntimeSelection::default()
        }));
        assert_eq!(state.tool_runtime.workspace_root.as_ref(), Some(&workspace));
        assert_eq!(state.workspace_path_input, workspace.display().to_string());
        assert!(state.workspace_error.is_none());

        state.apply_event(ChatEvent::Error {
            session_id: Some(state.session_id.clone()),
            error: "tool runtime update failed: workspace directory is unavailable".to_string(),
        });
        assert!(
            state
                .workspace_error
                .as_deref()
                .is_some_and(|error| error.contains("workspace directory is unavailable"))
        );
        assert_eq!(
            state.notice.as_deref(),
            Some("Workspace directory needs attention.")
        );
    }

    #[test]
    fn empty_conversation_title_follows_selected_saved_chat() {
        let (_backend, mut state) = isolated_chat_state("empty-chat-title");
        let session_id = SessionId::generate().to_string();
        state.sessions = vec![SessionRef {
            id: session_id.clone(),
            title: "Release triage".to_string(),
            kind: ConversationKind::Chat,
            updated_unix: 1,
            participant_count: 1,
            participant_names: Vec::new(),
        }];
        state.session_id = session_id;
        state.info.agent = "main-agent".to_string();

        assert_eq!(state.empty_conversation_title(), "Release triage");
        assert_eq!(
            state.empty_conversation_hint(),
            "Type a message below to begin."
        );
    }

    #[test]
    fn empty_conversation_title_follows_selected_channel() {
        let (_backend, mut state) = isolated_chat_state("empty-channel-title");
        let channel_id = SessionId::generate().to_string();
        state.sessions = vec![SessionRef {
            id: channel_id.clone(),
            title: "Release triage".to_string(),
            kind: ConversationKind::Channel,
            updated_unix: 1,
            participant_count: 2,
            participant_names: vec!["main-agent".to_string(), "coding-agent".to_string()],
        }];
        state.session_id = channel_id;
        state.info.agent = "main-agent".to_string();

        assert_eq!(state.empty_conversation_title(), "Channel: Release triage");
        assert_eq!(
            state.empty_conversation_hint(),
            "2 active agents · send a message to the channel."
        );
        assert_eq!(
            state.empty_conversation_context().as_deref(),
            Some("Agents: main-agent, coding-agent")
        );
    }

    #[test]
    fn empty_conversation_title_guides_empty_channel_filter() {
        let (_backend, mut state) = isolated_chat_state("empty-channel-filter-title");
        let chat_id = SessionId::generate().to_string();
        state.sessions = vec![SessionRef {
            id: chat_id.clone(),
            title: "Current chat".to_string(),
            kind: ConversationKind::Chat,
            updated_unix: 1,
            participant_count: 1,
            participant_names: Vec::new(),
        }];
        state.session_id = chat_id;
        state.session_kind_filter = SessionKindFilter::Channels;
        state.messages.clear();

        assert!(state.show_channel_onboarding());
        assert_eq!(state.empty_conversation_title(), "No channels yet");
        assert_eq!(
            state.empty_conversation_hint(),
            "Create a channel to coordinate multiple agents around one shared timeline."
        );
        assert_eq!(
            state.empty_conversation_context().as_deref(),
            Some("Use New channel to choose agents and edit the title.")
        );
        assert_eq!(
            quick_actions_for_chat(&state),
            &[ChatQuickAction::NewChannel],
            "channel onboarding should present only the channel creation action"
        );

        state.sessions.clear();
        state.session_id.clear();
        state.session_list_loading = true;
        assert_eq!(state.empty_conversation_title(), "Loading channels...");
        assert_eq!(
            state.empty_conversation_hint(),
            "Saved channels are still loading."
        );
        assert_eq!(
            state.empty_conversation_context().as_deref(),
            Some("You can keep chatting or use New channel to start one now.")
        );
        assert_eq!(
            quick_actions_for_chat(&state),
            &[ChatQuickAction::NewChannel],
            "channel loading should also avoid direct-chat starter actions"
        );
    }

    #[test]
    fn session_rename_updates_sidebar_metadata_and_preserves_title() {
        let (_backend, mut state) = isolated_chat_state("rename");
        let channel_id = SessionId::generate();
        let session_id = channel_id.to_string();
        state.session_id = session_id.clone();
        state.messages = vec![
            ChatMessage::user("original prompt"),
            ChatMessage::model("original answer"),
        ];
        seed_backend_chat(
            &state.data_dir,
            &channel_id,
            "original prompt",
            &state.messages,
        );
        state.persist();

        state.begin_edit_current_session();
        assert_eq!(state.session_edit_id.as_deref(), Some(session_id.as_str()));
        state.set_session_title_draft("Renamed chat".to_string());
        assert!(state.can_save_session_title());
        state.save_session_title();
        assert!(state.pending_session_rename.is_some());
        assert_eq!(state.session_title_draft, "Renamed chat");
        wait_for(&mut state, Duration::from_secs(5), |state| {
            state.pending_session_rename.is_none()
        });

        assert_eq!(state.session_edit_id.as_deref(), Some(session_id.as_str()));
        assert_eq!(
            state
                .sessions
                .iter()
                .find(|session| session.id == session_id)
                .map(|session| session.title.as_str()),
            Some("Renamed chat")
        );
        let store = SifsAgentChannelStore::open(&state.data_dir).expect("open channel store");
        let channel = store
            .load_channel(&channel_id)
            .expect("load backend chat")
            .expect("backend chat");
        assert_eq!(channel.title, "Renamed chat");
        let transcript = load_backend_chat_messages(&state.data_dir, &channel_id);
        assert_eq!(transcript[0].text, "original prompt");
        assert_eq!(transcript[1].text, "original answer");

        state.persist();
        assert_eq!(
            state
                .sessions
                .iter()
                .find(|session| session.id == session_id)
                .map(|session| session.title.as_str()),
            Some("Renamed chat")
        );
    }

    #[test]
    fn session_rename_ignores_stale_result_and_preserves_draft_on_failure() {
        let (_backend, mut state) = isolated_chat_state("rename-request-id");
        let session_id = SessionId::generate().to_string();
        state.sessions = vec![SessionRef {
            id: session_id.clone(),
            title: "Original".to_string(),
            kind: ConversationKind::Chat,
            updated_unix: 1,
            participant_count: 1,
            participant_names: Vec::new(),
        }];
        state.session_edit_id = Some(session_id.clone());
        state.session_title_draft = "Keep this draft".to_string();
        state.pending_session_rename = Some(PendingSessionRename {
            request_id: 2,
            session_id: session_id.clone(),
            kind: ConversationKind::Chat,
            title: "Keep this draft".to_string(),
        });
        let (tx, rx) = mpsc::channel();
        state.session_rename_rx = Some(rx);
        tx.send(SessionRenameWorkerResult {
            request_id: 1,
            session_id: session_id.clone(),
            kind: ConversationKind::Chat,
            title: "Stale title".to_string(),
            result: Ok(()),
        })
        .expect("stale result");
        tx.send(SessionRenameWorkerResult {
            request_id: 2,
            session_id: session_id.clone(),
            kind: ConversationKind::Chat,
            title: "Keep this draft".to_string(),
            result: Err("recoverable SIFS failure".to_string()),
        })
        .expect("current result");

        assert!(!state.pump_session_rename());
        assert_eq!(
            state
                .pending_session_rename
                .as_ref()
                .map(|pending| pending.request_id),
            Some(2)
        );
        assert_eq!(state.session_title_draft, "Keep this draft");
        assert!(state.pump_session_rename());
        assert!(state.pending_session_rename.is_none());
        assert_eq!(state.session_title_draft, "Keep this draft");
        assert_eq!(
            state
                .session_rename_error
                .as_ref()
                .map(|error| error.detail.as_str()),
            Some("recoverable SIFS failure")
        );
        assert_eq!(state.sessions[0].title, "Original");
    }

    #[test]
    fn session_delete_hides_session_and_resets_visible_chat() {
        let (_backend, mut state) = isolated_chat_state("delete");
        let channel_id = SessionId::generate();
        let session_id = channel_id.to_string();
        state.session_id = session_id.clone();
        state.messages = vec![
            ChatMessage::user("delete this prompt"),
            ChatMessage::model("delete this answer"),
        ];
        seed_backend_chat(
            &state.data_dir,
            &channel_id,
            "delete this prompt",
            &state.messages,
        );
        state.persist();

        state.request_delete_session(&session_id);
        assert_eq!(
            state.session_delete_confirm_id.as_deref(),
            Some(session_id.as_str())
        );
        state.confirm_delete_session(None);

        assert!(
            state
                .sessions
                .iter()
                .all(|session| session.id != session_id),
            "deleted sessions leave the sidebar list"
        );
        assert!(state.messages.is_empty());
        assert_ne!(state.session_id, session_id);
        assert_eq!(
            state.pending_session_reset.as_deref(),
            Some(state.session_id.as_str())
        );
        assert_eq!(
            state.pending_delete_id.as_deref(),
            Some(session_id.as_str()),
            "SIFS tombstoning continues in the background"
        );

        wait_for(&mut state, Duration::from_secs(20), |state| {
            state.pending_delete_id.is_none()
        });
        let store = SifsAgentChannelStore::open(&state.data_dir).expect("open channel store");
        let channel = store
            .load_channel(&channel_id)
            .expect("load backend chat")
            .expect("backend chat");
        assert!(channel.deleted_at.is_some(), "deleted chats are tombstoned");
    }

    #[test]
    fn new_channel_creates_channel_entry_and_opens_config() {
        let (_backend, mut state) = isolated_chat_state("new-channel");
        seed_test_agents(&mut state);
        state.agents[0].tool_runtime.sandbox_node_id = Some("main-node".to_string());

        state.new_channel(None);
        wait_for(&mut state, Duration::from_secs(5), |state| {
            state.pending_session_reset.is_none() && state.channel_create_rx.is_none()
        });

        let channel_id = state.session_id.clone();
        assert!(
            state.messages.is_empty(),
            "new channel starts with an empty visible transcript"
        );
        assert_eq!(
            state.session_edit_id.as_deref(),
            Some(channel_id.as_str()),
            "new channels open their edit popup"
        );
        let listed = state
            .sessions
            .iter()
            .find(|session| session.id == channel_id)
            .expect("channel listed");
        assert_eq!(listed.kind, ConversationKind::Channel);
        assert_eq!(listed.participant_count, 1);

        let parsed = channel_id.parse::<SessionId>().expect("channel id");
        let channel = state
            .agent_channel_store()
            .expect("channel store")
            .load_channel(&parsed)
            .expect("load channel")
            .expect("channel saved");
        assert_eq!(channel.channel_kind, AgentChannelKind::Channel);
        let participants = channel.enabled_participants_in_order();
        assert_eq!(participants.len(), 1);
        assert_eq!(participants[0].agent_key, "main-agent");
        assert_eq!(
            participants[0].tool_runtime.sandbox_node_id.as_deref(),
            Some("main-node")
        );
        assert_ne!(
            participants[0].agent_session_id, channel.channel_id,
            "explicit channel participants keep their own agent sessions"
        );
    }

    #[test]
    fn new_channel_defaults_to_main_agent_before_runtime_ready() {
        let (_backend, mut state) = isolated_chat_state("new-channel-default-agent");
        state.info.agent.clear();
        state.agents = vec![
            AgentSummary {
                key: "background-agent".to_string(),
                name: "background-agent".to_string(),
                description: String::new(),
                tool_runtime: ChatToolRuntimeSelection::default(),
            },
            AgentSummary {
                key: "main-agent".to_string(),
                name: "main-agent".to_string(),
                description: String::new(),
                tool_runtime: ChatToolRuntimeSelection::default(),
            },
        ];

        assert!(state.is_active_agent(1));
        assert_eq!(
            state.default_channel_agent(),
            ("main-agent".to_string(), "main-agent".to_string())
        );

        state.new_channel(None);
        wait_for(&mut state, Duration::from_secs(5), |state| {
            state.pending_session_reset.is_none() && state.channel_create_rx.is_none()
        });

        let parsed = state.session_id.parse::<SessionId>().expect("channel id");
        let channel = state
            .agent_channel_store()
            .expect("channel store")
            .load_channel(&parsed)
            .expect("load channel")
            .expect("channel saved");
        let participants = channel.enabled_participants_in_order();
        assert_eq!(participants.len(), 1);
        assert_eq!(participants[0].agent_key, "main-agent");
    }

    #[test]
    fn channel_config_adds_selected_agent_and_updates_sidebar_count() {
        let (_backend, mut state) = isolated_chat_state("channel-add-agent");
        seed_test_agents(&mut state);
        state.agents[1].tool_runtime.sandbox_node_id = Some("coding-node".to_string());
        state.new_channel(None);
        wait_for(&mut state, Duration::from_secs(5), |state| {
            state.pending_session_reset.is_none() && state.channel_create_rx.is_none()
        });
        let channel_id = state.session_id.clone();

        state.set_channel_agent_idx(1);
        state.add_selected_agent_to_channel();
        assert!(state.pending_channel_config.is_some());
        wait_for(&mut state, Duration::from_secs(5), |state| {
            state.pending_channel_config.is_none() && !state.session_list_loading
        });

        let parsed = channel_id.parse::<SessionId>().expect("channel id");
        let channel = state
            .agent_channel_store()
            .expect("channel store")
            .load_channel(&parsed)
            .expect("load channel")
            .expect("channel saved");
        let participants = channel.enabled_participants_in_order();
        assert_eq!(participants.len(), 2);
        assert!(
            participants
                .iter()
                .any(|participant| participant.agent_key == "coding-agent")
        );
        let coding = participants
            .iter()
            .find(|participant| participant.agent_key == "coding-agent")
            .expect("coding participant");
        assert_eq!(
            coding.tool_runtime.sandbox_node_id.as_deref(),
            Some("coding-node")
        );
        assert_eq!(
            state
                .sessions
                .iter()
                .find(|session| session.id == channel_id)
                .map(|session| session.participant_count),
            Some(2)
        );
        assert_eq!(
            state
                .channel_agent_refs(&channel_id)
                .into_iter()
                .map(|participant| participant.agent_name)
                .collect::<Vec<_>>(),
            vec!["main-agent".to_string(), "coding-agent".to_string()]
        );
    }

    #[test]
    fn channel_config_removes_agent_and_updates_sidebar_count() {
        let (_backend, mut state) = isolated_chat_state("channel-remove-agent");
        seed_test_agents(&mut state);
        state.new_channel(None);
        wait_for(&mut state, Duration::from_secs(5), |state| {
            state.pending_session_reset.is_none() && state.channel_create_rx.is_none()
        });
        let channel_id = state.session_id.clone();
        state.set_channel_agent_idx(1);
        state.add_selected_agent_to_channel();
        wait_for(&mut state, Duration::from_secs(5), |state| {
            state.pending_channel_config.is_none() && !state.session_list_loading
        });

        let coding_participant_id = state
            .channel_agent_refs(&channel_id)
            .into_iter()
            .find(|participant| participant.agent_key == "coding-agent")
            .map(|participant| participant.participant_id)
            .expect("coding participant listed");

        state.remove_channel_agent(&channel_id, &coding_participant_id);
        assert!(state.pending_channel_config.is_some());
        wait_for(&mut state, Duration::from_secs(5), |state| {
            state.pending_channel_config.is_none() && !state.session_list_loading
        });

        let parsed = channel_id.parse::<SessionId>().expect("channel id");
        let channel = state
            .agent_channel_store()
            .expect("channel store")
            .load_channel(&parsed)
            .expect("load channel")
            .expect("channel saved");
        let participants = channel.enabled_participants_in_order();
        assert_eq!(participants.len(), 1);
        assert!(
            participants
                .iter()
                .all(|participant| participant.agent_key != "coding-agent")
        );
        assert_eq!(
            state
                .sessions
                .iter()
                .find(|session| session.id == channel_id)
                .map(|session| session.participant_count),
            Some(1)
        );
    }

    #[test]
    fn channel_config_ignores_stale_result_and_surfaces_current_error() {
        let (_backend, mut state) = isolated_chat_state("channel-config-request-id");
        let channel_id = SessionId::generate().to_string();
        state.pending_channel_config = Some(PendingChannelConfig {
            request_id: 9,
            channel_id: channel_id.clone(),
            action: "adding coding-agent".to_string(),
        });
        let (tx, rx) = mpsc::channel();
        state.channel_config_rx = Some(rx);
        tx.send(ChannelConfigWorkerResult {
            request_id: 8,
            channel_id: channel_id.clone(),
            action: "removing old-agent".to_string(),
            success_notice: "stale success".to_string(),
            result: Ok(()),
        })
        .expect("stale result");
        tx.send(ChannelConfigWorkerResult {
            request_id: 9,
            channel_id: channel_id.clone(),
            action: "adding coding-agent".to_string(),
            success_notice: "current success".to_string(),
            result: Err("authorization can be retried".to_string()),
        })
        .expect("current result");

        assert!(!state.pump_channel_config());
        assert_eq!(
            state
                .pending_channel_config
                .as_ref()
                .map(|pending| pending.request_id),
            Some(9)
        );
        assert!(state.pump_channel_config());
        assert!(state.pending_channel_config.is_none());
        assert_eq!(
            state
                .channel_config_error
                .as_ref()
                .map(|error| error.detail.as_str()),
            Some("authorization can be retried")
        );
        assert!(
            state
                .notice
                .as_deref()
                .is_some_and(|notice| notice.contains("authorization can be retried"))
        );
    }

    #[test]
    fn session_edit_dialog_done_button_closes_in_rendered_view() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let session_id = SessionId::generate().to_string();
        {
            let mut chat = shared.chat_mut();
            chat.sessions = vec![SessionRef {
                id: session_id.clone(),
                title: "Dialog chat".to_string(),
                kind: ConversationKind::Chat,
                updated_unix: 1,
                participant_count: 1,
                participant_names: Vec::new(),
            }];
            chat.session_id = session_id.clone();
            chat.session_edit_id = Some(session_id.clone());
            chat.session_title_draft = "Dialog chat".to_string();
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat dialog")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Done")
            .click()?;

        assert!(
            shared.chat().session_edit_id.is_none(),
            "Done should close the edit dialog"
        );

        Ok(())
    }

    #[test]
    fn session_edit_dialog_close_button_closes_in_rendered_view() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let session_id = SessionId::generate().to_string();
        {
            let mut chat = shared.chat_mut();
            chat.sessions = vec![SessionRef {
                id: session_id.clone(),
                title: "Closable chat".to_string(),
                kind: ConversationKind::Chat,
                updated_unix: 1,
                participant_count: 1,
                participant_names: Vec::new(),
            }];
            chat.session_id = session_id.clone();
            chat.session_edit_id = Some(session_id.clone());
            chat.session_title_draft = "Closable chat".to_string();
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat dialog close")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Close edit dialog")
            .click()?;

        assert!(
            shared.chat().session_edit_id.is_none(),
            "close button should dismiss the edit dialog"
        );

        Ok(())
    }

    #[test]
    fn rendered_session_edit_button_opens_dialog() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let session_id = SessionId::generate().to_string();
        {
            let mut chat = shared.chat_mut();
            chat.sessions = vec![SessionRef {
                id: session_id.clone(),
                title: "Dialog chat".to_string(),
                kind: ConversationKind::Chat,
                updated_unix: 1,
                participant_count: 1,
                participant_names: Vec::new(),
            }];
            chat.session_id = session_id.clone();
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat edit button")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        let snapshot = window.snapshot()?;
        let edit_button = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::Button
                    && node.name.as_deref() == Some("Edit Dialog chat")
            })
            .expect("chat row edit button");
        assert!(
            edit_button.bounds.width() >= SIDEBAR_ROW_EDIT_W - 0.5
                && edit_button.bounds.height() >= SIDEBAR_ROW_EDIT_H - 0.5,
            "row edit button should be a compact visible affordance, got {:?}",
            edit_button.bounds
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Edit Dialog chat")
            .click()?;

        assert_eq!(
            shared.chat().session_edit_id.as_deref(),
            Some(session_id.as_str()),
            "row edit button should open the edit dialog"
        );
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Show details")
            .click()?;
        app.run_until_idle()?;
        let snapshot = window.snapshot()?;
        for label in [
            "Hide details",
            "Type: Direct chat",
            "Storage: SIFS-backed chat list",
        ] {
            assert!(
                snapshot
                    .accessibility
                    .nodes
                    .iter()
                    .any(|node| node.name.as_deref() == Some(label)),
                "{label} should be visible after expanding chat details"
            );
        }
        assert!(
            snapshot.accessibility.nodes.iter().any(|node| {
                node.name
                    .as_deref()
                    .is_some_and(|name| name.starts_with("Session ID: "))
            }),
            "expanded details should expose the session id"
        );
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Done")
            .click()?;
        assert!(shared.chat().session_edit_id.is_none());

        Ok(())
    }

    #[test]
    fn rendered_channel_edit_dialog_shows_agent_list() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let data_dir = backend.data_dir.clone();
        let shared = Shared::new(AppState::ready(backend));
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id.clone(), "Team channel");
        channel.ensure_participant("main-agent", "main-agent");
        channel.ensure_participant("coding-agent", "coding-agent");
        SifsAgentChannelStore::open(&data_dir)
            .expect("open channel store")
            .save_channel(&channel)
            .expect("save channel");
        let session_id = channel_id.to_string();
        {
            let mut chat = shared.chat_mut();
            chat.sessions = vec![SessionRef {
                id: session_id.clone(),
                title: "Team channel".to_string(),
                kind: ConversationKind::Channel,
                updated_unix: 1,
                participant_count: 2,
                participant_names: vec!["main-agent".to_string(), "coding-agent".to_string()],
            }];
            chat.session_id = session_id.clone();
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Channel editor")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        app.run_until_idle()?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Edit Team channel")
            .click()?;
        app.run_until_idle()?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Remove main-agent")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Delete channel")
            .expect()
            .to_be_hidden()?;
        window
            .get_by_text("Storage: SIFS channel record and timeline")
            .expect()
            .to_be_hidden()?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Show details")
            .click()?;
        app.run_until_idle()?;
        let snapshot = window.snapshot()?;

        for label in [
            "Close edit dialog",
            "Delete channel",
            "Remove main-agent",
            "Remove coding-agent",
        ] {
            assert!(
                snapshot.accessibility.nodes.iter().any(|node| {
                    node.role == SemanticsRole::Button && node.name.as_deref() == Some(label)
                }),
                "{label} control should be visible in the channel edit dialog"
            );
        }
        for label in [
            "Type: Shared channel",
            "Storage: SIFS channel record and timeline",
            "Agents: 2 active agents",
            "Agent names: main-agent, coding-agent",
        ] {
            assert!(
                snapshot
                    .accessibility
                    .nodes
                    .iter()
                    .any(|node| node.name.as_deref() == Some(label)),
                "{label} detail should be visible after expanding channel details"
            );
        }
        assert!(
            snapshot
                .accessibility
                .nodes
                .iter()
                .any(|node| node.name.as_deref() == Some("2 active")),
            "channel edit dialog should show the active-agent count"
        );

        Ok(())
    }

    #[test]
    fn rendered_channel_edit_dialog_marks_default_title_as_untitled() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let channel_id = SessionId::generate().to_string();
        {
            let mut chat = shared.chat_mut();
            seed_test_agents(&mut chat);
            chat.sessions = vec![SessionRef {
                id: channel_id.clone(),
                title: "New channel".to_string(),
                kind: ConversationKind::Channel,
                updated_unix: 1,
                participant_count: 1,
                participant_names: vec!["main-agent".to_string()],
            }];
            chat.session_id = channel_id.clone();
            chat.session_edit_id = Some(channel_id);
            chat.session_title_draft = "New channel".to_string();
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Untitled channel editor")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        app.run_until_idle()?;
        let snapshot = window.snapshot()?;

        assert!(
            snapshot.accessibility.nodes.iter().any(|node| {
                node.name.as_deref()
                    == Some(
                        "Untitled channel. Rename it so it is easy to recognize in the sidebar.",
                    )
            }),
            "default channel title should be called out in the edit dialog"
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Show details")
            .click()?;
        app.run_until_idle()?;
        let snapshot = window.snapshot()?;
        assert!(
            snapshot.accessibility.nodes.iter().any(|node| {
                node.name.as_deref()
                    == Some("Title: Untitled channel; rename it to make it easy to find.")
            }),
            "expanded details should explain the untitled channel state"
        );

        Ok(())
    }

    #[test]
    fn rendered_channel_edit_dialog_uses_pending_session_agents() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let channel_id = SessionId::generate().to_string();
        {
            let mut chat = shared.chat_mut();
            seed_test_agents(&mut chat);
            let (_tx, rx) = mpsc::channel();
            chat.sessions = vec![SessionRef {
                id: channel_id.clone(),
                title: "Pending channel".to_string(),
                kind: ConversationKind::Channel,
                updated_unix: 1,
                participant_count: 1,
                participant_names: vec!["main-agent".to_string()],
            }];
            chat.session_id = channel_id.clone();
            chat.session_edit_id = Some(channel_id);
            chat.session_title_draft = "Pending channel".to_string();
            chat.channel_agent_idx = 0;
            chat.channel_create_rx = Some(rx);
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Pending channel editor")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        app.run_until_idle()?;
        let snapshot = window.snapshot()?;

        assert!(
            snapshot
                .accessibility
                .nodes
                .iter()
                .any(|node| node.name.as_deref() == Some("1 active")),
            "pending session participants should contribute to the active count"
        );
        assert!(
            snapshot.accessibility.nodes.iter().any(|node| {
                node.role == SemanticsRole::Button
                    && node.name.as_deref() == Some("Waiting for main-agent (saving)")
                    && node.state.disabled
            }),
            "pending session participants should render as non-removable saving rows"
        );
        assert!(
            snapshot
                .accessibility
                .nodes
                .iter()
                .all(|node| node.name.as_deref() != Some("No agents assigned yet.")),
            "pending session participants should not look like an empty channel"
        );

        Ok(())
    }

    #[test]
    fn rendered_channel_edit_dialog_surfaces_failed_pending_agents() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let channel_id = SessionId::generate().to_string();
        {
            let mut chat = shared.chat_mut();
            seed_test_agents(&mut chat);
            chat.sessions = vec![SessionRef {
                id: channel_id.clone(),
                title: "Blocked channel".to_string(),
                kind: ConversationKind::Channel,
                updated_unix: 1,
                participant_count: 1,
                participant_names: vec!["main-agent".to_string()],
            }];
            chat.session_id = channel_id.clone();
            chat.session_edit_id = Some(channel_id);
            chat.session_title_draft = "Blocked channel".to_string();
            chat.channel_agent_idx = 0;
            chat.notice =
                Some("channel create failed: device authorization was denied".to_string());
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Failed pending channel editor")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        app.run_until_idle()?;

        let snapshot = window.snapshot()?;
        assert!(
            snapshot.accessibility.nodes.iter().any(|node| {
                node.role == SemanticsRole::Button
                    && node.name.as_deref() == Some("Connection needed for main-agent (not saved)")
                    && node.state.disabled
            }),
            "failed pending agents should render as a blocked storage state"
        );
        assert!(
            snapshot.accessibility.nodes.iter().any(|node| {
                node.name.as_deref()
                    == Some(
                        "Channel membership is not saved. Connection settings has recovery detail.",
                    )
            }),
            "failed pending agents should explain the recovery path"
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Open connection settings")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        assert_eq!(
            shared.settings().selected,
            Some(SettingsSection::Connection)
        );

        Ok(())
    }

    #[test]
    fn rendered_channel_edit_dialog_disables_duplicate_agent_add() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let data_dir = backend.data_dir.clone();
        let shared = Shared::new(AppState::ready(backend));
        let channel_id = SessionId::generate();
        let mut channel = AgentChannel::new_channel(channel_id.clone(), "Ops channel");
        channel.ensure_participant("main-agent", "main-agent");
        SifsAgentChannelStore::open(&data_dir)
            .expect("open channel store")
            .save_channel(&channel)
            .expect("save channel");
        let session_id = channel_id.to_string();
        {
            let mut chat = shared.chat_mut();
            seed_test_agents(&mut chat);
            chat.sessions = vec![SessionRef {
                id: session_id.clone(),
                title: "Ops channel".to_string(),
                kind: ConversationKind::Channel,
                updated_unix: 1,
                participant_count: 1,
                participant_names: vec!["main-agent".to_string()],
            }];
            chat.session_id = session_id.clone();
            chat.session_edit_id = Some(session_id);
            chat.session_title_draft = "Ops channel".to_string();
            chat.channel_agent_idx = 0;
            chat.models_loading = false;
        }

        let runtime = crate::build_runtime(shared.clone())?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        app.run_until_idle()?;
        let snapshot = window.snapshot()?;
        let add_agent = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::Button && node.name.as_deref() == Some("Add agent")
            })
            .expect("Add agent button should be visible");

        assert!(
            add_agent.state.disabled,
            "Add agent should be disabled when the selected agent is already assigned"
        );

        Ok(())
    }

    #[test]
    fn clicking_rendered_chat_row_opens_session_without_refcell_panic() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let first_id = SessionId::generate().to_string();
        let second_id = SessionId::generate().to_string();
        {
            let mut chat = shared.chat_mut();
            chat.sessions = vec![
                SessionRef {
                    id: first_id.clone(),
                    title: "Current chat".to_string(),
                    kind: ConversationKind::Chat,
                    updated_unix: 1,
                    participant_count: 1,
                    participant_names: Vec::new(),
                },
                SessionRef {
                    id: second_id.clone(),
                    title: "Target chat".to_string(),
                    kind: ConversationKind::Chat,
                    updated_unix: 2,
                    participant_count: 1,
                    participant_names: Vec::new(),
                },
            ];
            chat.session_id = first_id;
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat rows")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::ListItem)
            .with_name("Target chat")
            .with_description("Direct chat")
            .click()?;

        assert_eq!(shared.chat().session_id, second_id);

        Ok(())
    }

    #[test]
    fn rendered_chat_session_filter_opens_matching_session() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let first_id = SessionId::generate().to_string();
        let second_id = SessionId::generate().to_string();
        {
            let mut chat = shared.chat_mut();
            chat.sessions = vec![
                SessionRef {
                    id: first_id.clone(),
                    title: "Current chat".to_string(),
                    kind: ConversationKind::Chat,
                    updated_unix: 1,
                    participant_count: 1,
                    participant_names: Vec::new(),
                },
                SessionRef {
                    id: second_id.clone(),
                    title: "Release triage".to_string(),
                    kind: ConversationKind::Channel,
                    updated_unix: 2,
                    participant_count: 2,
                    participant_names: Vec::new(),
                },
            ];
            chat.session_id = first_id;
            chat.session_filter = "release".to_string();
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat filtered rows")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        let snapshot = window.snapshot()?;

        assert!(
            snapshot
                .accessibility
                .nodes
                .iter()
                .any(|node| node.name.as_deref() == Some("CONVERSATIONS · 1/2")),
            "chat heading should show filtered and total counts"
        );
        assert!(
            !snapshot
                .accessibility
                .nodes
                .iter()
                .any(|node| node.name.as_deref() == Some("Current chat")),
            "non-matching chats should not be rendered in the filtered list"
        );

        window
            .get_by_role(SemanticsRole::ListItem)
            .with_name("Release triage")
            .with_description("Shared channel, 2 agents")
            .click()?;

        assert_eq!(shared.chat().session_id, second_id);

        Ok(())
    }

    #[test]
    fn rendered_session_kind_filter_limits_sidebar_rows() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let first_id = SessionId::generate().to_string();
        let second_id = SessionId::generate().to_string();
        {
            let mut chat = shared.chat_mut();
            chat.sessions = vec![
                SessionRef {
                    id: first_id.clone(),
                    title: "Current chat".to_string(),
                    kind: ConversationKind::Chat,
                    updated_unix: 1,
                    participant_count: 1,
                    participant_names: Vec::new(),
                },
                SessionRef {
                    id: second_id.clone(),
                    title: "Release triage".to_string(),
                    kind: ConversationKind::Channel,
                    updated_unix: 2,
                    participant_count: 2,
                    participant_names: Vec::new(),
                },
            ];
            chat.session_id = first_id;
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat kind filtered rows")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::RadioButton)
            .with_name("Show channels only")
            .with_description("1 visible conversation(s)")
            .click()?;
        app.run_until_idle()?;

        assert_eq!(
            shared.chat().session_kind_filter,
            SessionKindFilter::Channels
        );
        let snapshot = window.snapshot()?;
        assert!(
            snapshot
                .accessibility
                .nodes
                .iter()
                .any(|node| node.name.as_deref() == Some("CHANNELS · 1")),
            "channel heading should show the channel-only count"
        );
        assert!(
            !snapshot
                .accessibility
                .nodes
                .iter()
                .any(|node| node.name.as_deref() == Some("Current chat")),
            "chat rows should not be rendered in the channel-only list"
        );

        window
            .get_by_role(SemanticsRole::ListItem)
            .with_name("Release triage")
            .with_description("Shared channel, 2 agents")
            .click()?;

        assert_eq!(shared.chat().session_id, second_id);

        Ok(())
    }

    #[test]
    fn rendered_empty_chat_has_no_predefined_templates() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            seed_test_agents(&mut chat);
            chat.sessions.clear();
            chat.messages.clear();
            chat.input.clear();
            chat.session_id = SessionId::generate().to_string();
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Empty chat")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        for label in [
            "Plan a change",
            "Draft document",
            "Inspect attachment",
            "Role play",
            "Check cluster",
        ] {
            assert_eq!(
                window
                    .get_by_role(SemanticsRole::Button)
                    .with_name(label)
                    .count()?,
                0,
                "empty chats must not render the predefined `{label}` template"
            );
        }
        assert!(shared.chat().input.is_empty());

        Ok(())
    }

    #[test]
    fn empty_state_quick_actions_stay_inside_transcript_area() {
        let actions = [ChatQuickAction::ManageChannel, ChatQuickAction::NewChannel];
        for area in [
            Rect::new(0.0, 0.0, 700.0, 300.0),
            Rect::new(0.0, 0.0, 460.0, 300.0),
            Rect::new(0.0, 0.0, 320.0, 360.0),
        ] {
            let rects = TranscriptView::quick_action_rects(area, &actions);

            assert_eq!(rects.len(), actions.len());
            for (action, rect) in rects {
                assert!(
                    rect.x() >= area.x() + PAD - 0.5,
                    "{action:?} quick action should not render left of the transcript area: rect={rect:?}, area={area:?}"
                );
                assert!(
                    rect.max_x() <= area.max_x() - PAD + 0.5,
                    "{action:?} quick action should not render right of the transcript area: rect={rect:?}, area={area:?}"
                );
                assert!(
                    rect.y() >= area.y() + PAD,
                    "{action:?} quick action should not render above the transcript area"
                );
                assert!(
                    rect.max_y() <= area.max_y() - PAD + 0.5,
                    "{action:?} quick action should not overlap the composer: rect={rect:?}"
                );
            }
        }
    }

    #[test]
    fn rendered_started_chat_uses_single_control_row_popups() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            seed_test_agents(&mut chat);
            chat.sessions.clear();
            chat.session_id = SessionId::generate().to_string();
            chat.messages = vec![
                ChatMessage::user("We already have history."),
                ChatMessage::model("Ready for the next task."),
            ];
            chat.input.clear();
            chat.cursor = 0;
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Started chat control row")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        for label in ["Code", "Doc", "Role", "Cluster"] {
            assert_eq!(
                window
                    .get_by_role(SemanticsRole::Button)
                    .with_name(label)
                    .count()?,
                0,
                "{label} compact scenario action should not render in the composer control row"
            );
        }

        let button_y = |name: &str| -> sui::Result<f32> {
            let snapshot = window.snapshot()?;
            let node = snapshot
                .accessibility
                .nodes
                .iter()
                .find(|node| {
                    node.role == SemanticsRole::Button && node.name.as_deref() == Some(name)
                })
                .expect("button semantics");
            Ok(node.bounds.y())
        };
        let model_button_y = button_y("gpt-5")?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("gpt-5")
            .click()?;
        assert_eq!(
            window.get_by_text("Model selector").count()?,
            1,
            "model button should open the model selector popup"
        );
        let popup_title_bounds = {
            let snapshot = window.snapshot()?;
            snapshot
                .accessibility
                .nodes
                .iter()
                .find(|node| {
                    node.role == SemanticsRole::Text
                        && node.name.as_deref() == Some("Model selector")
                })
                .expect("model popup title semantics")
                .bounds
        };
        assert!(
            popup_title_bounds.y() < model_button_y,
            "model selector popup should open above the control row"
        );
        assert!(
            (button_y("gpt-5")? - model_button_y).abs() < 0.5,
            "opening the model popup should not move the control row"
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Close model selector")
            .click()?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Context will refresh on the next send")
            .click()?;
        assert_eq!(
            window.get_by_text("Context").count()?,
            1,
            "context bar should open context details"
        );

        Ok(())
    }

    #[test]
    fn rendered_composer_input_autogrows_and_caps_with_scrollbar() -> sui::Result<()> {
        let short_shared = Shared::new(AppState::ready(Backend::local_fallback()));
        {
            let mut chat = short_shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.models_loading = false;
        }
        let short_runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Composer short input")
                    .root(view(short_shared.clone())),
            )
            .build()?;
        let short_app = sui_testing::TestApp::from_runtime(short_runtime)?;
        let short_window = short_app.main_window()?;
        let short_snapshot = short_window.snapshot()?;
        let short_input = short_snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::ScrollView
                    && node.name.as_deref() == Some("chat-input-scroll")
            })
            .expect("short input scroll semantics");
        assert!(
            (54.0..=64.0).contains(&short_input.bounds.height()),
            "empty composer should default to about two text lines: {:?}",
            short_input.bounds
        );

        let long_shared = Shared::new(AppState::ready(Backend::local_fallback()));
        {
            let mut chat = long_shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.models_loading = false;
            let long_draft = (0..48)
                .map(|index| format!("line {index:02} with enough text to wrap naturally"))
                .collect::<Vec<_>>()
                .join("\n");
            chat.set_draft(long_draft);
        }
        let long_runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Composer long input")
                    .root(view(long_shared.clone())),
            )
            .build()?;
        let long_app = sui_testing::TestApp::from_runtime(long_runtime)?;
        let long_window = long_app.main_window()?;
        let long_snapshot = long_window.snapshot()?;
        let long_input = long_snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::ScrollView
                    && node.name.as_deref() == Some("chat-input-scroll")
            })
            .expect("long input scroll semantics");
        assert!(
            (COMPOSER_INPUT_MAX_H - 1.0..=COMPOSER_INPUT_MAX_H + 1.0)
                .contains(&long_input.bounds.height()),
            "long composer should cap at {COMPOSER_INPUT_MAX_H}px: {:?}",
            long_input.bounds
        );
        let scrollbar = long_snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::Slider
                    && node.name.as_deref() == Some("chat-input-scrollbar")
            })
            .expect("input scrollbar semantics");
        match &scrollbar.value {
            Some(sui::SemanticsValue::Range { max, .. }) => {
                assert!(
                    *max > 0.0,
                    "capped composer input should expose a scrollable overflow range"
                );
            }
            other => panic!("input scrollbar should expose range semantics, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn rendered_started_channel_hides_compact_manage_action() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let channel_id = SessionId::generate().to_string();
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            seed_test_agents(&mut chat);
            chat.sessions = vec![SessionRef {
                id: channel_id.clone(),
                title: "Release triage".to_string(),
                kind: ConversationKind::Channel,
                updated_unix: 1,
                participant_count: 2,
                participant_names: vec!["main-agent".to_string(), "coding-agent".to_string()],
            }];
            chat.session_id = channel_id.clone();
            chat.messages = vec![
                ChatMessage::user("Continue this channel."),
                ChatMessage::model("main-agent: I am ready."),
            ];
            chat.input.clear();
            chat.cursor = 0;
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Started channel control row")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        for label in ["Manage", "Code", "Doc", "Role"] {
            assert_eq!(
                window
                    .get_by_role(SemanticsRole::Button)
                    .with_name(label)
                    .count()?,
                0,
                "{label} compact channel action should not render in the composer control row"
            );
        }

        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Activity")
                .count()?,
            1,
            "the started channel should still expose the activity popup button"
        );

        Ok(())
    }

    #[test]
    fn rendered_latest_answer_can_be_loaded_into_composer() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let answer = "# Design note\n\nThis response should become an editable draft.";
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.sessions.clear();
            chat.messages = vec![
                ChatMessage::user("draft a note"),
                ChatMessage::model(answer),
            ];
            chat.input.clear();
            chat.cursor = 0;
            chat.session_id = SessionId::generate().to_string();
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat answer reuse")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Use answer")
                .count()?,
            1
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Use answer")
            .click()?;
        app.run_until_idle()?;

        let chat = shared.chat();
        assert_eq!(chat.input, answer);
        assert_eq!(chat.cursor, answer.len());
        drop(chat);
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Use answer")
                .count()?,
            0,
            "a populated draft should hide the reuse action"
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Send message")
                .count()?,
            1,
            "loading an answer should leave the primary Send action ready"
        );

        Ok(())
    }

    #[test]
    fn rendered_empty_channel_surfaces_active_agent_count() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let data_dir = backend.data_dir.clone();
        let shared = Shared::new(AppState::ready(backend));
        let parsed_channel_id = SessionId::generate();
        let channel_id = parsed_channel_id.to_string();
        let mut channel = AgentChannel::new_channel(parsed_channel_id, "Release triage");
        channel.ensure_participant("main-agent", "main-agent");
        channel.ensure_participant("coding-agent", "coding-agent");
        SifsAgentChannelStore::open(&data_dir)
            .expect("open channel store")
            .save_channel(&channel)
            .expect("save channel");
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            seed_test_agents(&mut chat);
            chat.sessions = vec![SessionRef {
                id: channel_id.clone(),
                title: "Release triage".to_string(),
                kind: ConversationKind::Channel,
                updated_unix: 1,
                participant_count: 2,
                participant_names: vec!["main-agent".to_string(), "coding-agent".to_string()],
            }];
            chat.session_id = channel_id.clone();
            chat.messages.clear();
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Channel empty state")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert!(
            window
                .get_by_text("2 active agents · send a message to the channel.")
                .count()?
                >= 1,
            "selected channel empty state should show the active-agent count"
        );
        assert!(
            window
                .get_by_text("Agents: main-agent, coding-agent")
                .count()?
                >= 1,
            "selected channel empty state should show the active-agent names"
        );
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Manage channel")
            .with_description("Open channel agents and settings")
            .click()?;
        assert_eq!(
            shared.chat().session_edit_id.as_deref(),
            Some(channel_id.as_str()),
            "Manage channel should open the selected channel editor"
        );

        Ok(())
    }

    #[test]
    fn rendered_empty_channel_filter_explains_next_step() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            seed_test_agents(&mut chat);
            chat.sessions.clear();
            chat.session_id.clear();
            chat.session_kind_filter = SessionKindFilter::Channels;
            chat.messages.clear();
            chat.input.clear();
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Channel onboarding")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert!(
            window
                .get_by_text(
                    "Create a channel to coordinate multiple agents around one shared timeline."
                )
                .count()?
                >= 1,
            "empty channel filter should explain what channels are for"
        );
        assert!(
            window
                .get_by_text("Use New channel to choose agents and edit the title.")
                .count()?
                >= 1,
            "empty channel filter should point to the visible create action"
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Plan a change")
                .count()?,
            0,
            "channel onboarding should not show direct-chat starter actions"
        );
        let direct_channel_button = window
            .get_by_role(SemanticsRole::Button)
            .with_name("New channel")
            .with_description("Create a shared channel");
        assert_eq!(
            direct_channel_button.count()?,
            1,
            "the main pane should expose a direct channel creation action"
        );
        direct_channel_button.click()?;
        {
            let chat = shared.chat();
            assert_eq!(
                chat.session_edit_id.as_deref(),
                Some(chat.session_id.as_str()),
                "main-pane New channel should open the channel editor"
            );
        }
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Dialog)
                .with_name("Edit channel")
                .count()?,
            1,
            "main-pane New channel should render the channel editor dialog"
        );

        Ok(())
    }

    #[test]
    fn rendered_sidebar_create_actions_are_explicit_buttons() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            seed_test_agents(&mut chat);
            chat.sessions.clear();
            chat.messages.clear();
            chat.input.clear();
            chat.models_loading = false;
            chat.session_edit_id = None;
        }

        let initial_session_id = shared.chat().session_id.clone();
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat sidebar create actions")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("New chat")
            .click()?;
        let new_chat_id = shared.chat().session_id.clone();
        assert_ne!(
            new_chat_id, initial_session_id,
            "New chat button should start a fresh visible session"
        );
        assert!(
            shared.chat().session_edit_id.is_none(),
            "New chat should not open the edit dialog"
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("New channel")
            .click()?;
        {
            let chat = shared.chat();
            assert_ne!(
                chat.session_id, new_chat_id,
                "New channel button should start a separate visible session"
            );
            assert_eq!(
                chat.session_edit_id.as_deref(),
                Some(chat.session_id.as_str()),
                "New channel should open its edit dialog"
            );
        }
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Dialog)
                .with_name("Edit channel")
                .count()?,
            1,
            "sidebar New channel should render the channel editor dialog"
        );

        Ok(())
    }

    #[test]
    fn rendered_chat_sessions_scroll_with_sui_scroll_view() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let sessions = (0..48)
            .map(|index| SessionRef {
                id: SessionId::generate().to_string(),
                title: format!("Scrollable chat {index:02}"),
                kind: ConversationKind::Chat,
                updated_unix: index,
                participant_count: 1,
                participant_names: Vec::new(),
            })
            .collect::<Vec<_>>();
        let active_id = sessions[0].id.clone();
        {
            let mut chat = shared.chat_mut();
            chat.sessions = sessions;
            chat.session_id = active_id;
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat sessions scroll")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        let before_y = chat_sessions_list_y(&window)?;
        window
            .get_by_role(SemanticsRole::ScrollView)
            .with_name("chat-sessions-scroll")
            .scroll_pixels(sui::Vector::new(0.0, -260.0))?;
        let after_y = chat_sessions_list_y(&window)?;

        assert!(
            after_y < before_y - 100.0,
            "sessions list should move when the SUI ScrollView scrolls: before={before_y}, after={after_y}"
        );

        let selected_id = shared.chat().sessions[10].id.clone();
        window
            .get_by_role(SemanticsRole::ListItem)
            .with_name("Scrollable chat 10")
            .with_description("Direct chat")
            .click()?;
        let selected_y = chat_sessions_list_y(&window)?;
        assert_eq!(shared.chat().session_id, selected_id);
        assert!(
            (selected_y - after_y).abs() < 2.0,
            "selecting a chat rebuilds the sidebar but should preserve scroll position: before_select={after_y}, after_select={selected_y}"
        );

        Ok(())
    }

    #[test]
    fn chat_sessions_scroll_state_clamps_after_list_shrinks() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            chat.sessions = (0..48)
                .map(|index| SessionRef {
                    id: SessionId::generate().to_string(),
                    title: format!("Clamp chat {index:02}"),
                    kind: ConversationKind::Chat,
                    updated_unix: index,
                    participant_count: 1,
                    participant_names: Vec::new(),
                })
                .collect();
            chat.session_id = chat.sessions[0].id.clone();
            chat.models_loading = false;
        }

        let theme_reader: ThemeReader = {
            let s = shared.clone();
            Rc::new(move || s.theme())
        };
        let scroll_state = ScrollState::new();
        let shrink = {
            let s = shared.clone();
            sui::Button::new("Shrink")
                .theme_when(clone_reader(&theme_reader))
                .on_press_with_ctx(move |ctx| {
                    s.chat_mut().sessions.truncate(18);
                    use sui::{InvalidationKind, InvalidationRequest, InvalidationTarget};
                    let window = InvalidationTarget::Window(ctx.window_id());
                    ctx.request(InvalidationRequest::new(window, InvalidationKind::Measure));
                    ctx.request(InvalidationRequest::new(window, InvalidationKind::Arrange));
                    ctx.request_paint();
                    ctx.request_semantics();
                })
        };
        let root_widget = Flex::horizontal()
            .align_items(Alignment::Stretch)
            .with_item(
                sidebar(&shared, &theme_reader, &scroll_state),
                FlexItem::fixed(SIDEBAR_MIN_W),
            )
            .with_item(shrink, FlexItem::fixed(120.0));

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat sessions scroll clamp")
                    .root(root_widget),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::ScrollView)
            .with_name("chat-sessions-scroll")
            .scroll_pixels(sui::Vector::new(0.0, -2000.0))?;
        let before_offset = scroll_state.current_offset().y;
        let before_max = scroll_state.max_offset().y;
        assert!(
            before_offset > 100.0,
            "test setup should start scrolled away from the top"
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Shrink")
            .click()?;
        let after_offset = scroll_state.current_offset().y;
        let after_max = scroll_state.max_offset().y;

        assert!(
            after_max < before_max,
            "shrinking the chat list should reduce the maximum offset"
        );
        assert!(
            after_offset <= after_max + 1.0,
            "scroll offset should clamp to the shorter list: offset={after_offset}, max={after_max}"
        );
        assert!(
            after_offset > 0.0,
            "shrinking the chat list should not reset scroll to the top while content still overflows"
        );

        Ok(())
    }

    #[test]
    fn rendered_sidebar_is_resizable_and_search_fills_available_width() -> sui::Result<()> {
        use sui::{PointerButtons, PointerEvent};

        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.models_loading = false;
            chat.session_kind_filter = SessionKindFilter::Channels;
            chat.sessions.clear();
        }
        let viewport = Size::new(1_200.0, 720.0);
        let window = test_window(
            "Resizable conversation sidebar",
            view(shared.clone()),
            viewport,
        )?;

        assert_eq!(window.get_by_text("WORKSPACE").count()?, 0);
        let before = window.snapshot()?;
        let search_before =
            named_node_bounds(&before, SemanticsRole::TextInput, "chat-session-filter");
        let new_chat = named_node_bounds(&before, SemanticsRole::Button, "New chat");
        let new_channel = before
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::Button
                    && node.name.as_deref() == Some("New channel")
                    && node.description.as_deref() != Some("Create a shared channel")
            })
            .expect("sidebar New channel button")
            .bounds;
        assert!(
            (new_chat.y() - new_channel.y()).abs() < 1.0,
            "creation actions should share one compact row: {new_chat:?}, {new_channel:?}"
        );
        assert_non_overlapping("sidebar creation actions", new_chat, new_channel);
        assert_eq!(
            before
                .accessibility
                .nodes
                .iter()
                .filter(|node| {
                    node.role == SemanticsRole::Splitter
                        && node.name.as_deref() == Some("Conversation sidebar")
                })
                .count(),
            1,
            "desktop Chat should expose the native resizable splitter"
        );

        let theme = shared.theme();
        let divider = theme.metrics.split_view_divider_thickness.max(1.0);
        let initial_ratio = shared.chat().sidebar_ratio;
        let divider_x = (viewport.width - divider) * initial_ratio + divider * 0.5;
        let divider_point = Point::new(divider_x, 240.0);
        let drag_point = Point::new(380.0, 240.0);
        let root = window.root();
        root.dispatch_event(Event::Pointer(PointerEvent::new(
            PointerEventKind::Move,
            divider_point,
        )))?;
        let mut down = PointerEvent::new(PointerEventKind::Down, divider_point);
        down.pointer_id = 1;
        down.button = Some(PointerButton::Primary);
        down.buttons = PointerButtons::new(1);
        root.dispatch_event(Event::Pointer(down))?;
        let mut moved = PointerEvent::new(PointerEventKind::Move, drag_point);
        moved.pointer_id = 1;
        moved.buttons = PointerButtons::new(1);
        root.dispatch_event(Event::Pointer(moved))?;
        let mut up = PointerEvent::new(PointerEventKind::Up, drag_point);
        up.pointer_id = 1;
        up.button = Some(PointerButton::Primary);
        root.dispatch_event(Event::Pointer(up))?;
        window.run_until_idle()?;

        assert!(
            shared.chat().sidebar_ratio > initial_ratio + 0.08,
            "dragging the divider should retain the wider sidebar ratio"
        );
        let after = window.capture_artifacts()?;
        write_visual_debug_artifacts("chat-layout-polish", &after)?;
        let search_after = named_node_bounds(
            &after.snapshot,
            SemanticsRole::TextInput,
            "chat-session-filter",
        );
        assert!(
            search_after.width() > search_before.width() + 100.0,
            "search should expand with the resized sidebar: before={search_before:?}, after={search_after:?}"
        );

        let main_new_channel = after
            .snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::Button
                    && node.name.as_deref() == Some("New channel")
                    && node.description.as_deref() == Some("Create a shared channel")
            })
            .expect("main-pane New channel action")
            .bounds;
        let empty_copy_bottom = after
            .snapshot
            .accessibility
            .nodes
            .iter()
            .filter(|node| {
                node.role == SemanticsRole::Text
                    && matches!(
                        node.name.as_deref(),
                        Some("No channels yet")
                            | Some(
                                "Create a channel to coordinate multiple agents around one shared timeline."
                            )
                            | Some("Use New channel to choose agents and edit the title.")
                    )
            })
            .map(|node| node.bounds.max_y())
            .fold(0.0, f32::max);
        assert!(
            main_new_channel.y() >= empty_copy_bottom + 12.0,
            "main New channel action should sit below empty-state copy: copy_bottom={empty_copy_bottom}, action={main_new_channel:?}"
        );

        Ok(())
    }

    #[test]
    fn rendered_composer_uses_compact_controls_and_send_shortcut_tooltip() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.models_loading = false;
            chat.messages = vec![
                ChatMessage::user("Keep the composer compact."),
                ChatMessage::model("Ready."),
            ];
            chat.set_draft("Send this message".to_string());
        }
        let window = test_window(
            "Compact composer controls",
            view(shared.clone()),
            Size::new(1_000.0, 680.0),
        )?;

        assert_eq!(
            window
                .get_by_role(SemanticsRole::Tooltip)
                .with_name("Enter sends · Shift+Enter newline")
                .count()?,
            0,
            "keyboard guidance should not consume persistent composer space"
        );
        let snapshot = window.snapshot()?;
        for name in ["gpt-5", "Context will refresh on the next send", "Activity"] {
            let bounds = named_node_bounds(&snapshot, SemanticsRole::Button, name);
            assert!(
                bounds.width() <= COMPOSER_GHOST_BUTTON_SIZE + 1.0
                    && bounds.height() <= COMPOSER_GHOST_BUTTON_SIZE + 1.0,
                "{name} should use the compact icon-button footprint, got {bounds:?}"
            );
        }

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Send message")
            .hover()?;
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Tooltip)
                .with_name("Enter sends · Shift+Enter newline")
                .count()?,
            1,
            "the send button should reveal the keyboard guidance on hover"
        );
        window.advance_time(1.0)?;
        window.pump_frames(2)?;
        let artifacts = window.capture_artifacts()?;
        write_visual_debug_artifacts("chat-composer-polish", &artifacts)?;

        Ok(())
    }

    #[test]
    fn empty_submit_is_noop_and_streaming_submit_queues_steering() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        // Empty draft → nothing happens.
        state.submit();
        assert!(!state.streaming);
        assert!(state.messages.is_empty());
        // While streaming, a submit queues steering into the active runtime
        // turn (Mesh: "steer, don't just stop") instead of killing the run.
        let revision = state.draft_revision;
        state.set_draft("queued".to_string());
        state.streaming = true;
        state.submit();
        assert_eq!(state.messages.len(), 1, "queued steering is echoed");
        assert_eq!(state.messages[0].role, ChatRole::User);
        assert_eq!(state.messages[0].source, ChatMessageSource::User);
        assert_eq!(state.messages[0].text, "queued");
        assert!(state.input.is_empty(), "the queued draft is cleared");
        assert_ne!(
            state.draft_revision, revision,
            "queueing must rebuild the composer input so the cleared draft shows"
        );
        assert!(
            state.streaming,
            "queueing steering keeps active stream state"
        );
        assert_eq!(state.notice.as_deref(), Some("queued steering"));
        assert_eq!(
            state.queued_turn_inputs,
            vec![QueuedTurnInput {
                kind: QueuedTurnKind::Steer,
                text: "queued".to_string(),
                input_id: None,
            }],
            "the pending chip mirrors the queued message"
        );
    }

    #[test]
    fn queued_steering_chips_clear_when_the_turn_settles() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.set_draft("prefer AV1".to_string());
        state.streaming = true;
        state.submit();
        assert_eq!(state.queued_turn_inputs.len(), 1);

        // A control-state snapshot with drained queues means the runtime
        // consumed the steering mid-turn: the pending chip disappears.
        state.apply_event(ChatEvent::ControlState(
            sinomo_cli::app_api::ChatControlState {
                session_id: state.session_id.clone(),
                accepted: true,
                steering_count: 0,
                follow_up_count: 0,
                ..Default::default()
            },
        ));
        assert!(
            state.queued_turn_inputs.is_empty(),
            "drained runtime queues clear the pending chips"
        );

        // Queue again, then finish the turn: chips never outlive the stream.
        state.set_draft("share to #renders".to_string());
        state.submit();
        assert_eq!(state.queued_turn_inputs.len(), 1);
        state.apply_event(ChatEvent::TurnDone {
            session_id: state.session_id.clone(),
        });
        assert!(!state.streaming);
        assert!(
            state.queued_turn_inputs.is_empty(),
            "turn completion consumes the queue"
        );
    }

    fn control_status_with_queue(
        session_id: &str,
        queued_input: Option<sinomo_cli::app_api::ChatQueuedTurnInput>,
        removed_input_id: Option<String>,
    ) -> sinomo_cli::app_api::ChatControlStatus {
        sinomo_cli::app_api::ChatControlStatus {
            session_id: session_id.to_string(),
            accepted: true,
            runtime_attached: true,
            sequence: 1,
            event_count: 1,
            active_turn: true,
            task_count: 0,
            terminal_result_count: 0,
            tasks: Vec::new(),
            queued_input,
            removed_input_id,
        }
    }

    #[test]
    fn control_status_attaches_queue_ids_and_confirms_removals() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.set_draft("prefer AV1".to_string());
        state.streaming = true;
        state.submit();
        assert_eq!(state.queued_turn_inputs.len(), 1);
        assert_eq!(state.queued_turn_inputs[0].input_id, None);

        // The steer control status confirms the runtime queue id: the
        // optimistic chip picks it up and becomes removable.
        state.apply_event(ChatEvent::ControlStatus(control_status_with_queue(
            &state.session_id.clone(),
            Some(ChatQueuedTurnInput {
                input_id: "task_queued_1".to_string(),
                kind: ChatQueuedTurnInputKind::Steering,
                message: "prefer AV1".to_string(),
            }),
            None,
        )));
        assert_eq!(
            state.queued_turn_inputs.len(),
            1,
            "id attaches, no duplicate chip"
        );
        assert_eq!(
            state.queued_turn_inputs[0].input_id.as_deref(),
            Some("task_queued_1")
        );

        // Re-delivery of the same confirmation stays idempotent.
        state.apply_event(ChatEvent::ControlStatus(control_status_with_queue(
            &state.session_id.clone(),
            Some(ChatQueuedTurnInput {
                input_id: "task_queued_1".to_string(),
                kind: ChatQueuedTurnInputKind::Steering,
                message: "prefer AV1".to_string(),
            }),
            None,
        )));
        assert_eq!(state.queued_turn_inputs.len(), 1);

        // A removal confirmation drops the chip.
        state.apply_event(ChatEvent::ControlStatus(control_status_with_queue(
            &state.session_id.clone(),
            None,
            Some("task_queued_1".to_string()),
        )));
        assert!(state.queued_turn_inputs.is_empty());
    }

    #[test]
    fn control_state_queue_snapshot_is_authoritative_for_chips() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.streaming = true;
        state.queued_turn_inputs.push(QueuedTurnInput {
            kind: QueuedTurnKind::Steer,
            text: "stale optimistic chip".to_string(),
            input_id: None,
        });

        // The resume snapshot replaces local chips wholesale — including a
        // follow-up queued from another surface that this composer never saw.
        state.apply_event(ChatEvent::ControlState(
            sinomo_cli::app_api::ChatControlState {
                session_id: state.session_id.clone(),
                accepted: true,
                steering_count: 1,
                follow_up_count: 1,
                queued_inputs: vec![
                    ChatQueuedTurnInput {
                        input_id: "task_steer_1".to_string(),
                        kind: ChatQueuedTurnInputKind::Steering,
                        message: "prefer AV1".to_string(),
                    },
                    ChatQueuedTurnInput {
                        input_id: "task_follow_1".to_string(),
                        kind: ChatQueuedTurnInputKind::FollowUp,
                        message: "share to #renders".to_string(),
                    },
                ],
                ..Default::default()
            },
        ));
        assert_eq!(
            state.queued_turn_inputs,
            vec![
                QueuedTurnInput {
                    kind: QueuedTurnKind::Steer,
                    text: "prefer AV1".to_string(),
                    input_id: Some("task_steer_1".to_string()),
                },
                QueuedTurnInput {
                    kind: QueuedTurnKind::FollowUp,
                    text: "share to #renders".to_string(),
                    input_id: Some("task_follow_1".to_string()),
                },
            ],
        );
    }

    #[test]
    fn remove_queued_turn_input_drops_chip_and_requires_id() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.queued_turn_inputs.push(QueuedTurnInput {
            kind: QueuedTurnKind::Steer,
            text: "not yet confirmed".to_string(),
            input_id: None,
        });
        state.queued_turn_inputs.push(QueuedTurnInput {
            kind: QueuedTurnKind::Steer,
            text: "confirmed".to_string(),
            input_id: Some("task_queued_2".to_string()),
        });

        // Without a runtime id there is nothing to address yet.
        state.remove_queued_turn_input(0);
        assert_eq!(state.queued_turn_inputs.len(), 2);
        assert_eq!(
            state.notice.as_deref(),
            Some("queued message is still registering")
        );

        // With an id the chip drops optimistically and the removal is issued.
        state.remove_queued_turn_input(1);
        assert_eq!(state.queued_turn_inputs.len(), 1);
        assert_eq!(state.queued_turn_inputs[0].text, "not yet confirmed");
        assert_eq!(state.notice.as_deref(), Some("removing queued message…"));

        // Out-of-range indices are ignored.
        state.remove_queued_turn_input(9);
        assert_eq!(state.queued_turn_inputs.len(), 1);
    }

    #[test]
    fn escape_closes_composer_popup_before_interrupting() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);

        // Nothing open, nothing streaming: Esc is left for others.
        assert!(!state.handle_escape());

        // An open popup wins the first press.
        state.streaming = true;
        state.set_composer_popup(Some(ComposerPopup::Model));
        assert!(state.handle_escape());
        assert_eq!(state.composer_popup(), None);
        assert!(state.streaming, "closing the popup must not interrupt");
        assert_ne!(state.notice.as_deref(), Some("interrupting…"));

        // The next press halts generation — interruption is first-class.
        assert!(state.handle_escape());
        assert_eq!(state.notice.as_deref(), Some("interrupting…"));
    }

    #[test]
    fn new_session_clears_visible_chat_immediately_and_routes_stale_turn_events() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        let old_session_id = SessionId::from_legacy_named("sess_old_chat")
            .expect("session id")
            .to_string();
        let old_channel_id = old_session_id.parse::<SessionId>().expect("old channel id");
        state.session_id = old_session_id.clone();
        state.messages = vec![
            ChatMessage::user("old question"),
            ChatMessage::model("partial answer"),
        ];
        seed_backend_chat(
            &state.data_dir,
            &old_channel_id,
            "old question",
            &state.messages,
        );
        state.input = "draft".to_string();
        state.cursor = state.input.len();
        state.streaming = true;
        state.notice = Some("thinking…".to_string());
        state.control.active_turn = true;
        state.media_call_details_open = true;
        state.media_setup.start();
        state.toggle_media_call_mic();
        state.toggle_media_call_camera();
        assert!(state.media_setup.is_running());

        state.new_session();
        let new_session_id = state.session_id.clone();

        assert!(
            state.messages.is_empty(),
            "New chat clears the transcript immediately"
        );
        assert!(state.input.is_empty(), "New chat clears the composer draft");
        assert!(!state.streaming, "New chat leaves the visible chat idle");
        assert!(!state.media_setup.is_running());
        assert!(state.media_setup.microphone_enabled());
        assert!(state.media_setup.camera_enabled());
        assert!(state.media_call.mic_enabled);
        assert!(state.media_call.camera_enabled);
        assert!(!state.media_call_details_open);
        assert!(
            state.notice.is_none(),
            "New chat clears transient per-turn notices"
        );
        assert_eq!(
            state.pending_session_reset.as_deref(),
            Some(new_session_id.as_str())
        );

        state.apply_event(ChatEvent::Error {
            session_id: Some(old_session_id.clone()),
            error: "old provider failed".to_string(),
        });
        assert!(
            state.messages.is_empty(),
            "stale errors from the abandoned turn must not repopulate the new chat"
        );
        let old_transcript = load_backend_chat_messages(&state.data_dir, &old_channel_id);
        assert!(
            old_transcript
                .iter()
                .all(|message| !message.text.contains("old provider failed")),
            "background events must not be persisted by the app reducer"
        );

        state.set_draft("new question".to_string());
        state.submit();
        assert_eq!(state.messages.len(), 2);
        assert_eq!(state.messages[0].text, "new question");
        assert!(
            state.streaming,
            "sending is allowed while the worker reset ack is pending"
        );

        state.apply_event(ChatEvent::SessionReset {
            session_id: new_session_id,
        });
        assert!(state.pending_session_reset.is_none());
    }

    #[test]
    fn new_session_preserves_connection_attention_until_retry_succeeds() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.notice = Some("chat list failed: device authorization was denied".to_string());
        state.push_activity(
            "chat list",
            "device authorization was denied",
            ActivityTone::Warn,
        );
        state.set_draft("draft that should clear".to_string());

        state.new_session();

        assert!(state.messages.is_empty());
        assert!(state.input.is_empty());
        assert_eq!(
            state.notice.as_deref(),
            Some("chat list failed: device authorization was denied")
        );
        assert!(
            state.activity.iter().any(|entry| {
                entry.title == "chat list" && entry.detail == "device authorization was denied"
            }),
            "New chat must not hide unresolved storage attention"
        );
        assert_eq!(
            state.runtime_recovery_target(),
            Some(RuntimeRecoveryTarget::Connection)
        );
        assert_eq!(
            state.recovery_empty_title(RuntimeRecoveryTarget::Connection),
            "Chat list needs connection attention"
        );
    }

    #[test]
    fn embedded_daemon_failure_initializes_chat_attention() {
        let mut backend = Backend::local_fallback();
        backend.connection = Connection::LocalFallback {
            reason: Some("embedded daemon exited before ready: duplicate config key".to_string()),
        };

        let state = ChatState::new(&backend);

        assert_eq!(
            state.notice.as_deref(),
            Some(
                "embedded daemon failed: embedded daemon exited before ready: duplicate config key; daemon-backed chat and SIFS are unavailable"
            )
        );
        assert_eq!(
            state.connection_attention_detail_full().as_deref(),
            state.notice.as_deref()
        );
        assert_eq!(
            state.runtime_recovery_target(),
            Some(RuntimeRecoveryTarget::Connection)
        );
    }

    #[test]
    fn session_scoped_events_do_not_mutate_visible_or_backend_timeline() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        let visible_session_id = SessionId::from_legacy_named("sess_visible_chat")
            .expect("visible session id")
            .to_string();
        let background_session_id = SessionId::from_legacy_named("sess_background_chat")
            .expect("background session id")
            .to_string();
        let background_channel_id = background_session_id
            .parse::<SessionId>()
            .expect("background channel id");
        state.session_id = visible_session_id.clone();
        state.messages = vec![
            ChatMessage::user("visible question"),
            ChatMessage::model(""),
        ];
        state.streaming = true;
        state.control.active_turn = true;

        seed_backend_chat(
            &state.data_dir,
            &background_channel_id,
            "background question",
            &[ChatMessage::user("background question")],
        );

        state.apply_event(ChatEvent::Token {
            session_id: background_session_id.clone(),
            text: "background answer".to_string(),
        });
        state.apply_event(ChatEvent::TurnDone {
            session_id: background_session_id.clone(),
        });

        assert_eq!(
            state.session_id, visible_session_id,
            "background events keep the visible session selected"
        );
        assert!(
            state.streaming,
            "background TurnDone must not end visible work"
        );
        assert_eq!(
            state.messages[1].text, "",
            "background tokens must not append to the visible placeholder"
        );
        let background = load_backend_chat_messages(&state.data_dir, &background_channel_id);
        assert_eq!(background.len(), 1);
        assert_eq!(background[0].text, "background question");
    }

    #[test]
    fn visible_transcript_trims_to_recent_window_after_turn_done() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.session_id = SessionId::from_legacy_named("sess_long_visible_chat")
            .expect("session id")
            .to_string();
        state.messages = (0..(VISIBLE_TRANSCRIPT_RECENT_LIMIT + 5))
            .map(|index| ChatMessage::user(format!("message {index}")))
            .collect();
        state.streaming = true;

        state.apply_event(ChatEvent::TurnDone {
            session_id: state.session_id.clone(),
        });

        assert_eq!(state.messages.len(), VISIBLE_TRANSCRIPT_RECENT_LIMIT);
        assert_eq!(state.messages[0].text, "message 5");
        assert!(!state.streaming);
    }

    #[test]
    fn backend_update_upserts_visible_row_without_relisting_sifs() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        let other_channel_id =
            SessionId::from_legacy_named("sess_saved_elsewhere").expect("other session id");
        let other_session_id = other_channel_id.to_string();
        seed_backend_chat(
            &state.data_dir,
            &other_channel_id,
            "Saved elsewhere",
            &[ChatMessage::user("other")],
        );
        state.sessions.clear();

        let visible_session_id = SessionId::from_legacy_named("sess_visible_save")
            .expect("visible session id")
            .to_string();
        state.session_id = visible_session_id.clone();
        let messages = vec![
            ChatMessage::user("visible saved prompt"),
            ChatMessage::model("visible saved answer"),
        ];

        state.upsert_session_ref_from_messages(
            &visible_session_id,
            &ChatState::title_from_messages(&messages),
            &messages,
        );

        assert!(
            state
                .sessions
                .iter()
                .any(|session| session.id == visible_session_id
                    && session.kind == ConversationKind::Chat
                    && session.title == "visible saved prompt"),
            "the just-saved visible chat should appear immediately"
        );
        assert!(
            state
                .sessions
                .iter()
                .all(|session| session.id != other_session_id),
            "updating one chat should not synchronously relist unrelated SIFS records"
        );
    }

    #[test]
    fn direct_chat_loads_structured_channel_timeline() {
        let backend = Backend::local_fallback();
        let state = ChatState::new(&backend);
        let channel_id = SessionId::from_legacy_named("sess_structured_chat").expect("channel id");

        let store = SifsAgentChannelStore::open(&state.data_dir).expect("open channel store");
        let mut channel = AgentChannel::new(channel_id.clone(), "structured chat");
        let participant_id = channel.ensure_participant("main-agent", "main-agent");
        let participant = channel
            .participant(&participant_id)
            .expect("participant")
            .clone();
        let user = channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "User",
            "structured question",
            AgentChannelSource::app(),
        );
        store
            .append_message(&channel.channel_id, &user)
            .expect("append user");
        let agent = channel.append_agent_message(
            &participant,
            "structured answer",
            AgentChannelSource::app(),
        );
        store
            .append_message(&channel.channel_id, &agent)
            .expect("append agent");
        store.save_channel(&channel).expect("save channel");

        let messages = load_conversation_messages(
            &state.data_dir,
            None,
            &channel_id.to_string(),
            ConversationKind::Chat,
        )
        .expect("load chat messages");
        assert_eq!(messages[0].text, "structured question");
        assert!(messages[1].text.contains("structured answer"));

        let (sessions, error) = list_conversations_with_error(&state.data_dir, None);
        assert!(error.is_none());
        let session = sessions
            .iter()
            .find(|session| session.id == channel_id.to_string())
            .expect("structured chat listed");
        assert_eq!(session.title, "structured chat");
    }

    #[test]
    fn backend_update_does_not_write_timeline() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        let channel_id =
            SessionId::from_legacy_named("sess_backend_owned_chat").expect("channel id");
        let mut channel = AgentChannel::new(channel_id.clone(), "backend owned");
        let user = channel.append_user_message(
            AGENT_CHANNEL_APP_USER_ID,
            "User",
            "backend question",
            AgentChannelSource::app(),
        );
        let store = SifsAgentChannelStore::open(&state.data_dir).expect("open channel store");
        store
            .append_message(&channel.channel_id, &user)
            .expect("append user");
        store.save_channel(&channel).expect("save channel");

        state.session_id = channel_id.to_string();
        state.upsert_session_ref_from_messages(
            &channel_id.to_string(),
            "visible app copy",
            &[ChatMessage::user("visible app copy")],
        );

        let loaded = load_backend_chat_messages(&state.data_dir, &channel_id);
        assert!(
            loaded
                .iter()
                .all(|message| message.text != "visible app copy"),
            "UI sidebar updates must not write timeline content"
        );
        assert!(
            state
                .sessions
                .iter()
                .any(|session| session.id == channel_id.to_string()
                    && session.kind == ConversationKind::Chat),
            "the sidebar can still update from the save result"
        );
    }

    #[test]
    fn backend_update_preserves_existing_channel_sidebar_row() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        let channel_id = SessionId::from_legacy_named("sess_channel_save")
            .expect("channel session id")
            .to_string();
        state.session_id = channel_id.clone();
        state.sessions = vec![SessionRef {
            id: channel_id.clone(),
            title: "Release triage".to_string(),
            kind: ConversationKind::Channel,
            updated_unix: 7,
            participant_count: 2,
            participant_names: vec!["main-agent".to_string(), "coding-agent".to_string()],
        }];
        let messages = vec![ChatMessage::user("channel prompt")];

        state.upsert_session_ref_from_messages(
            &channel_id,
            &ChatState::title_from_messages(&messages),
            &messages,
        );

        assert_eq!(state.sessions.len(), 1);
        assert_eq!(state.sessions[0].id, channel_id);
        assert_eq!(state.sessions[0].title, "Release triage");
        assert_eq!(state.sessions[0].kind, ConversationKind::Channel);
        assert_eq!(state.sessions[0].participant_count, 2);
    }

    #[test]
    fn interrupt_turn_only_updates_notice_while_streaming() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);

        state.interrupt_turn();
        assert!(state.notice.is_none(), "idle interrupt is a no-op");

        state.streaming = true;
        state.interrupt_turn();
        assert_eq!(state.notice.as_deref(), Some("interrupting…"));
        assert!(state.streaming, "interrupt request waits for runtime event");
    }

    #[test]
    fn streamed_tokens_append_to_the_last_agent_message() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.set_draft("hi".to_string());
        state.submit();
        // Tokens append to the trailing (empty) agent placeholder rather than pushing new bubbles.
        state.apply_event(ChatEvent::Token {
            session_id: state.session_id.clone(),
            text: "Hel".to_string(),
        });
        state.apply_event(ChatEvent::Token {
            session_id: state.session_id.clone(),
            text: "lo".to_string(),
        });
        assert_eq!(state.messages.len(), 2, "tokens append, not push");
        assert_eq!(state.messages[1].text, "Hello");
        // TurnDone clears streaming and the notice.
        state.apply_event(ChatEvent::TurnDone {
            session_id: state.session_id.clone(),
        });
        assert!(!state.streaming, "TurnDone ends the stream");
        assert!(
            !state.control.active_turn,
            "TurnDone clears the active turn marker"
        );
        assert!(state.notice.is_none());
    }

    #[test]
    fn direct_chat_agent_tokens_do_not_prefix_speaker() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        let session_id = SessionId::generate().to_string();
        state.session_id = session_id.clone();
        state.messages = vec![ChatMessage::user("hi"), ChatMessage::model("")];

        state.apply_event(ChatEvent::AgentToken {
            session_id,
            speaker: sinomo_cli::app_api::ChatSpeaker {
                agent_key: "main-agent".to_string(),
                agent_name: "main-agent".to_string(),
                ..Default::default()
            },
            text: "Hello".to_string(),
        });

        assert_eq!(state.messages.len(), 2);
        assert_eq!(state.messages[1].text, "Hello");
    }

    #[test]
    fn durable_agent_messages_match_live_speaker_labels_for_chat_and_channel() {
        let mut conversation = AgentChannel::new(SessionId::generate(), "speaker labels");
        let participant_id = conversation.ensure_participant("main-agent", "main-agent");
        let participant = conversation
            .participant(&participant_id)
            .expect("participant")
            .clone();
        conversation.append_agent_message(&participant, "Hello", AgentChannelSource::app());

        assert_eq!(
            channel_messages(&conversation)[0].text,
            "Hello",
            "a restored direct chat must match its unprefixed live token row"
        );
        conversation.channel_kind = AgentChannelKind::Channel;
        assert_eq!(
            channel_messages(&conversation)[0].text,
            "main-agent: Hello",
            "multi-speaker channels retain the author label"
        );
    }

    #[test]
    fn channel_agent_tokens_keep_speaker_prefix() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        let channel_id = SessionId::generate().to_string();
        state.session_id = channel_id.clone();
        state.sessions = vec![SessionRef {
            id: channel_id.clone(),
            title: "Release triage".to_string(),
            kind: ConversationKind::Channel,
            updated_unix: 1,
            participant_count: 1,
            participant_names: vec!["main-agent".to_string()],
        }];
        state.messages = vec![ChatMessage::user("hi"), ChatMessage::model("")];
        let speaker = sinomo_cli::app_api::ChatSpeaker {
            agent_key: "main-agent".to_string(),
            agent_name: "main-agent".to_string(),
            ..Default::default()
        };

        state.apply_event(ChatEvent::AgentToken {
            session_id: channel_id.clone(),
            speaker: speaker.clone(),
            text: "Hel".to_string(),
        });
        state.apply_event(ChatEvent::AgentToken {
            session_id: channel_id,
            speaker,
            text: "lo".to_string(),
        });

        assert_eq!(state.messages.len(), 2);
        assert_eq!(state.messages[1].text, "main-agent: Hello");
    }

    #[test]
    fn channel_message_metadata_distinguishes_system_errors_from_model_text() {
        let mut channel = AgentChannel::new(SessionId::generate(), "errors");
        channel.append_system_error("Error: provider unavailable", AgentChannelSource::app());

        let restored = channel_messages(&channel);
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].role, ChatRole::Agent);
        assert_eq!(restored[0].source, ChatMessageSource::System);
        assert_eq!(restored[0].kind, ChatMessageKind::Error);
        assert_eq!(restored[0].history_role(), "system");
    }

    #[test]
    fn transcript_model_display_hides_decision_envelope() {
        let raw = format!(
            "main-agent: {}\nI need a sandbox executor to inspect the repository.",
            serde_json::json!({
                "response_mode": "defer",
                "text": "I need a sandbox executor to inspect the repository.",
                "tool_calls": [],
                "background_handoff": null,
                "next_state": "waiting"
            })
        );

        let rendered = transcript_message_display_text(&ChatMessage::model(raw));

        assert_eq!(
            rendered,
            "main-agent: I need a sandbox executor to inspect the repository."
        );
        assert!(!rendered.contains("response_mode"));
        assert!(!rendered.contains("tool_calls"));
    }

    #[test]
    fn transcript_model_display_preserves_non_envelope_json() {
        let raw = r#"main-agent: {"example":true,"text":"visible"}"#;

        assert_eq!(
            transcript_message_display_text(&ChatMessage::model(raw)),
            raw
        );
    }

    #[test]
    fn transcript_error_display_defers_connection_detail_to_settings() {
        let connection = ChatMessage::system_error(
            "Error: chat unavailable: failed to parse command: open agent channel SIFS store failed: device authorization was denied",
        );
        let rendered = transcript_message_display_text(&connection);
        assert!(rendered.starts_with("Connection issue\n"));
        assert!(rendered.contains("This chat needs storage authorization."));
        assert!(rendered.contains("Check SIFS first. Connection settings has low-level detail."));
        assert!(!rendered.contains("failed to parse command"));
        assert!(!rendered.contains("SIFS store failed"));
        assert!(!rendered.contains("device authorization was denied"));

        let sync = ChatMessage::system_error(
            "Error: resume failed: publish reconciled agent session checkpoint failed: network error at /daemon/dfs/browser/write-file: clustered DFS object has no committed metadata-shard assignment",
        );
        let rendered = transcript_message_display_text(&sync);
        assert!(rendered.starts_with("Chat sync issue\n"));
        assert!(rendered.contains("This chat's roaming session state could not be synchronized."));
        assert!(!rendered.contains("The local cluster service is not reachable."));

        let provider = ChatMessage::system_error("Error: provider unavailable");
        assert_eq!(
            transcript_message_display_text(&provider),
            "Error\nprovider unavailable"
        );
    }

    #[test]
    fn rendered_storage_error_message_opens_files() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.models_loading = false;
            chat.notice = None;
            chat.messages.push(ChatMessage::system_error(
                "Error: chat unavailable: failed to parse command: open agent channel SIFS store failed: device authorization was denied",
            ));
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Connection error message action")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Check SIFS")
            .with_description("Open SIFS browser")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Files);

        Ok(())
    }

    #[test]
    fn rendered_daemon_error_message_opens_connection_settings() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.models_loading = false;
            chat.notice = None;
            chat.messages.push(ChatMessage::system_error(
                "Error: chat unavailable: daemon connection failed",
            ));
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Daemon error message action")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Connection settings")
            .with_description("Connection settings")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        assert_eq!(
            shared.settings().selected,
            Some(SettingsSection::Connection)
        );

        Ok(())
    }

    #[test]
    fn activity_panel_is_quiet_until_requested_or_needed() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.info.provider = "openai".to_string();
        state.info.model = "gpt-5".to_string();

        assert!(
            !state.activity_panel_visible(),
            "ready idle chat should leave the activity popup closed"
        );

        state.toggle_activity_panel();
        assert!(state.activity_panel_visible());
        state.toggle_activity_panel();
        assert!(!state.activity_panel_visible());

        state.streaming = true;
        assert!(
            !state.activity_panel_visible(),
            "active streaming should keep the activity popup closed until the user opens Monitor"
        );
        assert_eq!(activity_button_label(&state), "Monitor");
        state.toggle_activity_panel();
        assert!(
            state.activity_panel_visible(),
            "Monitor should still open runtime details on demand"
        );
        state.toggle_activity_panel();
        state.streaming = false;
        assert!(!state.activity_panel_visible());

        state.push_activity(
            "chat list",
            "device authorization was denied",
            ActivityTone::Warn,
        );
        assert!(
            !state.activity_panel_visible(),
            "connection warnings should change the button label without forcing a popup open"
        );
        assert_eq!(activity_button_label(&state), "Review");
    }

    #[test]
    fn session_list_result_applies_chat_and_channel_catalog_atomically() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        let (tx, rx) = mpsc::channel();
        state.session_list_rx = Some(rx);
        state.session_list_loading = true;
        state.session_list_started_at = Some(Instant::now());

        let chat_id = SessionId::generate().to_string();
        let channel_id = SessionId::generate().to_string();
        tx.send(SessionListWorkerResult {
            sessions: vec![
                SessionRef {
                    id: chat_id.clone(),
                    title: "Visible chat".to_string(),
                    kind: ConversationKind::Chat,
                    updated_unix: 10,
                    participant_count: 1,
                    participant_names: Vec::new(),
                },
                SessionRef {
                    id: channel_id.clone(),
                    title: "Planning channel".to_string(),
                    kind: ConversationKind::Channel,
                    updated_unix: 20,
                    participant_count: 2,
                    participant_names: vec!["main-agent".to_string(), "reviewer".to_string()],
                },
            ],
            error: None,
        })
        .expect("send atomic catalog result");

        assert!(
            state.pump_session_list(),
            "the complete catalog should update visible history"
        );
        assert_eq!(state.sessions.len(), 2);
        assert!(state.sessions.iter().any(|session| session.id == chat_id));
        assert!(
            state
                .sessions
                .iter()
                .any(|session| session.id == channel_id)
        );
        assert!(!state.session_list_loading);
        assert!(state.session_list_rx.is_none());
        assert!(state.session_list_started_at.is_none());

        let unchanged = state.sessions.clone();
        assert!(
            !state.apply_session_list_result(SessionListWorkerResult {
                sessions: unchanged,
                error: None,
            }),
            "an identical pushed refresh must not rebuild or flash the conversation list"
        );
    }

    #[test]
    fn pushed_daemon_event_starts_catalog_refresh_without_a_polling_timer() {
        let (_backend, mut state) = isolated_chat_state("pushed-catalog-refresh");
        let (tx, rx) = mpsc::channel();
        state.daemon_event_rx = rx;

        tx.send(DaemonEventWorkerUpdate::Item(
            DaemonEventStreamItem::Ready {
                subscription_id: SessionId::generate().to_string(),
            },
        ))
        .expect("send ready frame");

        assert!(state.pump_daemon_event_subscription());
        assert!(state.session_list_loading);
        assert!(state.session_list_rx.is_some());
    }

    #[test]
    fn timed_out_session_list_load_can_recover_from_late_result() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        let (tx, rx) = mpsc::channel();
        state.session_list_rx = Some(rx);
        state.session_list_loading = true;
        state.session_list_started_at = Some(
            Instant::now()
                .checked_sub(SESSION_LIST_UI_TIMEOUT + Duration::from_millis(1))
                .expect("past timestamp"),
        );
        state.session_list_slow_notice_shown = false;

        assert!(
            state.pump_session_list(),
            "slow load should mutate visible state once"
        );
        assert!(state.session_list_loading);
        assert!(state.session_list_slow_notice_shown);
        assert!(state.notice.is_none());
        assert!(state.connection_attention_detail().is_none());
        let chat_list_statuses = state
            .activity
            .iter()
            .filter(|entry| entry.title == "chat list")
            .collect::<Vec<_>>();
        assert_eq!(chat_list_statuses.len(), 1);
        assert_eq!(chat_list_statuses[0].tone, ActivityTone::Running);
        assert!(
            chat_list_statuses[0].detail.contains("still loading"),
            "slow load should be a running status, not a failure"
        );
        assert!(
            !state.pump_session_list(),
            "slow status should not duplicate while the worker is still pending"
        );
        assert_eq!(
            state
                .activity
                .iter()
                .filter(|entry| entry.title == "chat list")
                .count(),
            1
        );

        let session_id = SessionId::generate().to_string();
        tx.send(SessionListWorkerResult {
            sessions: vec![SessionRef {
                id: session_id.clone(),
                title: "Recovered chat".to_string(),
                kind: ConversationKind::Chat,
                updated_unix: 7,
                participant_count: 1,
                participant_names: Vec::new(),
            }],
            error: None,
        })
        .expect("send late result");

        assert!(
            state.pump_session_list(),
            "late worker result should still apply"
        );
        assert_eq!(state.sessions.len(), 1);
        assert_eq!(state.sessions[0].id, session_id);
        assert!(!state.session_list_loading);
        assert!(!state.session_list_slow_notice_shown);
        assert!(state.notice.is_none());
        assert!(
            state
                .activity
                .iter()
                .all(|entry| entry.title != "chat list"),
            "successful late result should clear stale chat-list warnings"
        );
    }

    #[test]
    fn retry_chat_list_restarts_stuck_load_and_refreshes_history() {
        let root =
            std::env::temp_dir().join(format!("sinomo-app-chat-list-retry-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let data_dir = SinomoDataDir::new(&root);
        let channel_id = "01hxy6qa9k0t9vr4g9n8d3q6b1"
            .parse::<SessionId>()
            .expect("session id");
        seed_backend_chat(
            &data_dir,
            &channel_id,
            "Recovered by retry",
            &[ChatMessage::user("hello")],
        );
        let session_id = channel_id.to_string();
        let backend = Backend::local_fallback_for_data_dir(data_dir);
        let mut state = ChatState::new(&backend);
        state.sessions.clear();
        let (_tx, rx) = mpsc::channel();
        state.session_list_rx = Some(rx);
        state.session_list_loading = true;
        state.session_list_started_at = Some(
            Instant::now()
                .checked_sub(SESSION_LIST_UI_TIMEOUT + Duration::from_secs(1))
                .expect("past timestamp"),
        );
        state.session_list_slow_notice_shown = true;
        state.session_list_details_open = true;

        state.retry_chat_list_with_waker(None);

        assert!(state.session_list_loading);
        assert!(state.session_list_rx.is_some());
        wait_for(&mut state, Duration::from_secs(5), |state| {
            !state.session_list_loading
        });
        assert!(state.session_list_started_at.is_none());
        assert!(!state.session_list_slow_notice_shown);
        assert!(!state.session_list_details_open);
        assert!(
            state
                .sessions
                .iter()
                .any(|session| session.id == session_id && session.title == "Recovered by retry"),
            "retry should discard the stale worker and reload persisted sidebar history"
        );

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn local_chat_list_refresh_uses_worker_and_coalesces_background_completion() {
        let (_backend, mut state) = isolated_chat_state("local-list-worker");
        state.session_id = SessionId::generate().to_string();
        let background_session_id = SessionId::generate().to_string();

        state.start_session_list_load(None);
        assert!(state.session_list_loading);
        assert!(state.session_list_rx.is_some());
        assert!(state.route_background_session_event(
            &background_session_id,
            ChatEvent::TurnDone {
                session_id: background_session_id.clone(),
            },
        ));
        assert!(
            state.session_list_refresh_queued,
            "background completion should coalesce a follow-up worker refresh"
        );

        wait_for(&mut state, Duration::from_secs(5), |state| {
            !state.session_list_loading && !state.session_list_refresh_queued
        });
        assert!(state.session_list_rx.is_none());
    }

    #[test]
    fn rendered_activity_button_toggles_idle_activity_panel() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.info.provider = "openai".to_string();
            chat.info.model = "gpt-5".to_string();
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat activity button")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Activity")
            .click()?;
        assert!(
            shared.chat().activity_panel_visible(),
            "Activity button should open the idle activity popup"
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Activity")
            .click()?;
        assert!(
            !shared.chat().activity_panel_visible(),
            "Activity button should close the idle activity popup"
        );

        Ok(())
    }

    #[test]
    fn rendered_monitor_button_opens_running_activity_panel_on_demand() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.info.provider = "openai".to_string();
            chat.info.model = "gpt-5".to_string();
            chat.models_loading = false;
            chat.streaming = true;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat monitor button")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert!(
            !shared.chat().activity_panel_visible(),
            "running state should not force the activity popup open"
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Close activity")
                .count()?,
            0,
            "the activity popup should not be mounted until Monitor is opened"
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Monitor")
            .click()?;
        assert!(shared.chat().activity_panel_visible());

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Close activity")
            .with_description("Hide the runtime activity panel")
            .click()?;
        assert!(
            !shared.chat().activity_panel_visible(),
            "manually-opened running details should be closable"
        );

        Ok(())
    }

    #[test]
    fn rendered_activity_rail_close_button_hides_idle_panel() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.info.provider = "openai".to_string();
            chat.info.model = "gpt-5".to_string();
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat activity close")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Activity")
            .click()?;
        assert!(shared.chat().activity_panel_visible());

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Close activity")
            .with_description("Hide the runtime activity panel")
            .click()?;
        assert!(
            !shared.chat().activity_panel_visible(),
            "Close activity should hide a manually-opened idle activity popup"
        );

        Ok(())
    }

    #[test]
    fn activity_button_label_tracks_runtime_state() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.info.provider = "openai".to_string();
        state.info.model = "gpt-5".to_string();
        state.models_loading = false;

        assert_eq!(activity_button_label(&state), "Activity");
        assert!(
            activity_button_description(&state).contains("READY"),
            "quiet runtime description should include the readiness state"
        );

        state.streaming = true;
        assert_eq!(activity_button_label(&state), "Monitor");
        state.streaming = false;

        state.push_activity(
            "chat list",
            "device authorization was denied",
            ActivityTone::Warn,
        );
        assert_eq!(activity_button_label(&state), "Review");
        assert!(
            activity_button_description(&state).contains("Chat history needs recovery"),
            "warning description should use the user-facing recovery summary"
        );
    }

    #[test]
    fn runtime_overview_uses_visible_model_selection_before_runtime_info() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.info = RuntimeInfo::default();
        state.models_loading = false;

        assert_eq!(runtime_overview_title(&state), "OpenAI / gpt-5");

        let health = state.runtime_health();
        assert_eq!(health.label, "READY");
        assert_eq!(health.detail, "openai / gpt-5");
        assert_eq!(health.tone, ActivityTone::Good);
    }

    #[test]
    fn media_call_state_tracks_selected_runtime_and_controls() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.models_loading = false;

        assert!(!state.media_call_details_visible());
        state.toggle_media_call();
        assert!(state.media_call.active);
        assert!(state.media_call_details_visible());
        assert_eq!(state.media_call.provider, "OpenAI");
        assert_eq!(state.media_call.model, "gpt-5");
        assert!(state.media_call_status_summary().contains("mic on"));
        assert!(state.media_call_status_summary().contains("camera on"));

        state.media_setup.microphone_access = MediaAccessState::Ready;
        state.media_setup.camera_access = MediaAccessState::Ready;
        state.media_call.microphone_level = 0.75;
        publish_test_camera_frame(&state, 3, 1, 1, [0, 0, 0, 255]);
        state.toggle_media_call_mic();
        state.toggle_media_call_camera();
        assert!(!state.media_call.mic_enabled);
        assert!(!state.media_call.camera_enabled);
        assert_eq!(state.media_call.microphone_level, 0.0);
        assert!(state.media_setup.camera_surface.latest().is_none());
        assert_eq!(state.media_setup.microphone_access, MediaAccessState::Idle);
        assert_eq!(state.media_setup.camera_access, MediaAccessState::Idle);
        assert!(state.media_call_status_summary().contains("mic muted"));
        assert!(state.media_call_status_summary().contains("camera paused"));

        state.toggle_media_call();
        assert!(!state.media_call.active);
        assert!(!state.media_call_details_visible());
        assert_eq!(state.media_call.notice.as_deref(), Some("call ended"));
    }

    #[test]
    fn media_call_control_drain_records_release_and_explicit_reopen() {
        let (control_tx, control_rx) = mpsc::channel();
        control_tx
            .send(ChatMediaCallControl::SetMicEnabled(false))
            .expect("queue mic mute");
        control_tx
            .send(ChatMediaCallControl::SetMicEnabled(true))
            .expect("queue mic reopen");
        control_tx
            .send(ChatMediaCallControl::SetCameraEnabled(false))
            .expect("queue camera pause");
        let mut mic_enabled = true;
        let mut camera_enabled = true;

        let drained = drain_chat_media_controls(&control_rx, &mut mic_enabled, &mut camera_enabled);

        assert!(!drained.stop);
        assert!(mic_enabled);
        assert!(!camera_enabled);
        assert!(drained.microphone_disable_requested);
        assert!(drained.microphone_enable_requested);
        assert!(drained.camera_disable_requested);
        assert!(!drained.camera_enable_requested);
        assert_eq!(
            chat_media_device_transition(
                mic_enabled,
                true,
                drained.microphone_enable_requested,
                drained.microphone_disable_requested,
            ),
            ChatMediaDeviceTransition {
                release: true,
                mark_idle: true,
                open: true,
            }
        );
        assert_eq!(
            chat_media_device_transition(
                camera_enabled,
                true,
                drained.camera_enable_requested,
                drained.camera_disable_requested,
            ),
            ChatMediaDeviceTransition {
                release: true,
                mark_idle: true,
                open: false,
            }
        );
    }

    #[test]
    fn media_call_device_transition_opens_only_after_explicit_enable() {
        assert_eq!(
            chat_media_device_transition(true, false, false, false),
            ChatMediaDeviceTransition::default()
        );
        assert_eq!(
            chat_media_device_transition(true, false, true, false),
            ChatMediaDeviceTransition {
                release: false,
                mark_idle: false,
                open: true,
            }
        );
        assert_eq!(
            chat_media_device_transition(false, false, false, false),
            ChatMediaDeviceTransition::default()
        );
    }

    #[test]
    fn media_call_accepts_current_all_inputs_disabled_intent() {
        assert!(chat_media_requested_input_available(
            false, false, false, false
        ));
        assert!(!chat_media_requested_input_available(
            true, false, false, false
        ));
        assert!(chat_media_requested_input_available(
            false, false, true, true
        ));
    }

    #[test]
    fn media_call_permission_failures_remain_classified_for_recovery() {
        assert!(matches!(
            chat_media_access_failure("microphone permission denied".to_string()),
            MediaAccessState::PermissionDenied(_)
        ));
    }

    #[test]
    fn media_call_local_preview_does_not_wait_for_slow_remote_video_slot() {
        let remote = ChatMediaLatestVideoSlot::default();
        let mut published_previews = Vec::new();

        for sequence in 0..30_u64 {
            published_previews.push(sequence);
            remote.replace(sequence);
        }

        assert_eq!(published_previews.len(), 30);
        assert_eq!(remote.take(), Some(29));
        assert_eq!(remote.take(), None);
    }

    #[test]
    fn media_call_remote_video_cadence_is_independent_from_local_preview_rate() {
        let start = Instant::now();
        let mut cadence = ChatMediaRemoteVideoCadence::default();

        assert!(cadence.is_due(start));
        cadence.mark_sent(start);
        assert!(!cadence.is_due(start + Duration::from_millis(499)));
        assert!(cadence.is_due(start + Duration::from_millis(500)));
    }

    #[test]
    fn media_call_audio_queue_drops_new_chunks_when_its_capture_bound_is_full() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(CHAT_MEDIA_AUDIO_SEND_QUEUE_CAPACITY);

        for chunk in 0..CHAT_MEDIA_AUDIO_SEND_QUEUE_CAPACITY {
            assert_eq!(
                chat_media_audio_queue_outcome(tx.try_send(chunk)),
                Ok(ChatMediaAudioQueueOutcome::Queued)
            );
        }
        assert_eq!(
            chat_media_audio_queue_outcome(tx.try_send(99)),
            Ok(ChatMediaAudioQueueOutcome::DroppedBackpressure)
        );
        for chunk in 0..CHAT_MEDIA_AUDIO_SEND_QUEUE_CAPACITY {
            assert_eq!(rx.try_recv(), Ok(chunk), "the queued audio remains ordered");
        }
        drop(rx);
        assert!(chat_media_audio_queue_outcome(tx.try_send(100)).is_err());
    }

    #[test]
    fn dropping_media_runtime_cancels_network_waits_and_queues_stop() {
        let (control_tx, control_rx) = mpsc::channel();
        let (_event_tx, event_rx) = mpsc::channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        let runtime = ChatNativeMediaRuntime {
            control_tx,
            event_rx,
            snapshots: Arc::new(Mutex::new(ChatMediaSnapshots::default())),
            cancelled: cancelled.clone(),
            microphone_enabled: Arc::new(AtomicBool::new(true)),
            camera_enabled: Arc::new(AtomicBool::new(true)),
            worker: None,
        };

        drop(runtime);

        assert!(cancelled.load(Ordering::Acquire));
        assert!(matches!(
            control_rx.try_recv(),
            Ok(ChatMediaCallControl::Stop)
        ));
    }

    #[test]
    fn cancelled_device_open_returns_promptly_and_drops_late_handle() {
        struct DropProbe(Arc<AtomicBool>);
        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let cancelled = Arc::new(AtomicBool::new(false));
        let cancel_from_thread = cancelled.clone();
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_from_open = dropped.clone();
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let canceller = thread::spawn(move || {
            started_rx.recv().expect("device open started");
            cancel_from_thread.store(true, Ordering::Release);
        });
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("test runtime");
        let started = Instant::now();

        let result = runtime.block_on(await_chat_media_device_open(
            "test-device",
            &cancelled,
            None,
            move || {
                started_tx.send(()).expect("signal device open");
                thread::sleep(Duration::from_millis(75));
                Ok(DropProbe(dropped_from_open))
            },
            || {},
        ));

        canceller.join().expect("cancel thread");
        assert!(matches!(result, ChatMediaDeviceOpen::Cancelled));
        assert!(started.elapsed() < Duration::from_secs(1));
        let deadline = Instant::now() + Duration::from_secs(1);
        while !dropped.load(Ordering::Acquire) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(dropped.load(Ordering::Acquire));
    }

    #[test]
    fn disabling_device_open_emits_idle_and_never_publishes_late_ready() {
        struct DropProbe(Arc<AtomicBool>);
        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let cancelled = AtomicBool::new(false);
        let requested = Arc::new(AtomicBool::new(true));
        let requested_from_thread = requested.clone();
        let dropped = Arc::new(AtomicBool::new(false));
        let dropped_from_open = dropped.clone();
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let disabler = thread::spawn(move || {
            started_rx.recv().expect("device open started");
            requested_from_thread.store(false, Ordering::Release);
        });
        let (event_tx, event_rx) = mpsc::channel();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("test runtime");

        let result = runtime.block_on(open_chat_media_device(
            MediaAccessKind::Camera,
            "test-camera",
            &cancelled,
            Some(&requested),
            &event_tx,
            &None,
            move || {
                started_tx.send(()).expect("signal device open");
                thread::sleep(Duration::from_millis(75));
                Ok(DropProbe(dropped_from_open))
            },
            || {},
        ));

        disabler.join().expect("disable thread");
        assert!(matches!(result, ChatMediaDeviceOpen::Disabled));
        assert!(matches!(
            event_rx.recv().expect("checking event"),
            ChatMediaCallWorkerEvent::DeviceAccess(
                MediaAccessKind::Camera,
                MediaAccessState::Checking
            )
        ));
        assert!(matches!(
            event_rx.recv().expect("idle event"),
            ChatMediaCallWorkerEvent::DeviceAccess(MediaAccessKind::Camera, MediaAccessState::Idle)
        ));
        let deadline = Instant::now() + Duration::from_secs(1);
        while !dropped.load(Ordering::Acquire) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(dropped.load(Ordering::Acquire));
        assert!(matches!(event_rx.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn pending_device_open_polls_other_device_intent() {
        let cancelled = AtomicBool::new(false);
        let microphone_requested = Arc::new(AtomicBool::new(true));
        let microphone_from_thread = microphone_requested.clone();
        let release_observed = Arc::new(AtomicBool::new(false));
        let release_from_wait = release_observed.clone();
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let disabler = thread::spawn(move || {
            started_rx.recv().expect("camera open started");
            microphone_from_thread.store(false, Ordering::Release);
        });
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("test runtime");

        let result = runtime.block_on(await_chat_media_device_open(
            "test-camera",
            &cancelled,
            None,
            move || {
                started_tx.send(()).expect("signal camera open");
                thread::sleep(Duration::from_millis(75));
                Ok(7_u8)
            },
            || {
                if !microphone_requested.load(Ordering::Acquire) {
                    release_from_wait.store(true, Ordering::Release);
                }
            },
        ));

        disabler.join().expect("disable thread");
        assert!(matches!(result, ChatMediaDeviceOpen::Completed(Ok(7))));
        assert!(release_observed.load(Ordering::Acquire));
    }

    #[test]
    fn media_call_snapshots_coalesce_notice_and_live_values() {
        let mut snapshots = ChatMediaSnapshots {
            microphone_level: Some(0.5),
            notice: Some("latest notice".to_string()),
            ..Default::default()
        };

        let taken = snapshots.take();

        assert_eq!(taken.microphone_level, Some(0.5));
        assert_eq!(taken.notice.as_deref(), Some("latest notice"));
        assert!(snapshots.is_empty());
    }

    #[test]
    fn media_call_access_event_clears_snapshot_taken_before_device_stop() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.media_call.active = true;
        state.media_setup.microphone_access = MediaAccessState::Ready;
        state.media_setup.camera_access = MediaAccessState::Ready;
        let frame = publish_test_camera_frame(&state, 7, 1, 1, [0, 0, 0, 255]);
        let (control_tx, _control_rx) = mpsc::channel();
        let (event_tx, event_rx) = mpsc::channel();
        let snapshots = Arc::new(Mutex::new(ChatMediaSnapshots {
            microphone_level: Some(0.9),
            camera_surface_revision: Some(frame.revision),
            ..Default::default()
        }));
        state.media_call_runtime = Some(ChatNativeMediaRuntime {
            control_tx,
            event_rx,
            snapshots,
            cancelled: Arc::new(AtomicBool::new(false)),
            microphone_enabled: Arc::new(AtomicBool::new(true)),
            camera_enabled: Arc::new(AtomicBool::new(true)),
            worker: None,
        });
        event_tx
            .send(ChatMediaCallWorkerEvent::DeviceAccess(
                MediaAccessKind::Microphone,
                MediaAccessState::Idle,
            ))
            .expect("queue microphone stop");
        event_tx
            .send(ChatMediaCallWorkerEvent::DeviceAccess(
                MediaAccessKind::Camera,
                MediaAccessState::Idle,
            ))
            .expect("queue camera stop");

        let change = state.pump_media_call_runtime();
        assert!(change.changed());
        assert!(change.layout);

        assert_eq!(state.media_call.microphone_level, 0.0);
        assert!(state.media_setup.camera_surface.latest().is_none());
        assert_eq!(state.media_setup.microphone_access, MediaAccessState::Idle);
        assert_eq!(state.media_setup.camera_access, MediaAccessState::Idle);

        event_tx
            .send(ChatMediaCallWorkerEvent::DeviceAccess(
                MediaAccessKind::Microphone,
                MediaAccessState::Unavailable("microphone disconnected".to_string()),
            ))
            .expect("queue microphone failure");
        event_tx
            .send(ChatMediaCallWorkerEvent::DeviceAccess(
                MediaAccessKind::Camera,
                MediaAccessState::Busy("camera is in use".to_string()),
            ))
            .expect("queue camera failure");
        let change = state.pump_media_call_runtime();
        assert!(change.changed());
        assert!(change.layout);
        assert!(!state.media_call.mic_enabled);
        assert!(!state.media_call.camera_enabled);
    }

    #[test]
    fn media_call_start_config_snapshots_selected_devices_and_initial_toggles() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state
            .media_setup
            .microphones
            .push(crate::ui::media_setup::MediaDeviceOption {
                native_id: Some("mic-selected".to_string()),
                display_name: "Selected microphone".to_string(),
                is_default: false,
            });
        state
            .media_setup
            .speakers
            .push(crate::ui::media_setup::MediaDeviceOption {
                native_id: Some("speaker-selected".to_string()),
                display_name: "Selected speaker".to_string(),
                is_default: false,
            });
        state
            .media_setup
            .cameras
            .push(crate::ui::media_setup::MediaDeviceOption {
                native_id: Some("camera-selected".to_string()),
                display_name: "Selected camera".to_string(),
                is_default: false,
            });
        state.media_setup.microphone_index = 1;
        state.media_setup.speaker_index = 1;
        state.media_setup.camera_index = 1;
        state.media_call.mic_enabled = false;
        state.media_call.camera_enabled = true;

        let config = state.media_call_start_config();

        assert_eq!(config.devices.microphone.as_deref(), Some("mic-selected"));
        assert_eq!(config.devices.speaker.as_deref(), Some("speaker-selected"));
        assert_eq!(config.devices.camera.as_deref(), Some("camera-selected"));
        assert!(!config.mic_enabled);
        assert!(config.camera_enabled);
    }

    #[test]
    fn inactive_preflight_releases_devices_off_tab_and_resumes_on_return() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.media_call_details_open = true;
        state.media_setup.start();
        assert!(state.media_setup.is_running());

        assert!(state.sync_media_setup_visibility(false));
        assert!(!state.media_setup.is_running());
        assert!(state.sync_media_setup_visibility(true));
        assert!(state.media_setup.is_running());

        state.media_setup.stop();
        state.media_call.active = true;
        assert!(!state.sync_media_setup_visibility(false));
        assert!(!state.media_setup.is_running());
    }

    #[test]
    fn inactive_preflight_toggles_release_and_explicitly_reopen_capture_devices() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.media_call_details_open = true;
        state.media_setup.start();
        state.media_setup.microphone_level = 0.8;
        publish_test_camera_frame(&state, 4, 1, 1, [0, 0, 0, 255]);

        state.toggle_media_call_mic();
        state.toggle_media_call_camera();

        assert!(state.media_setup.is_running());
        assert!(!state.media_setup.microphone_enabled());
        assert!(!state.media_setup.camera_enabled());
        assert_eq!(state.media_setup.microphone_access, MediaAccessState::Idle);
        assert_eq!(state.media_setup.camera_access, MediaAccessState::Idle);
        assert_eq!(state.media_setup.microphone_level, 0.0);
        assert!(state.media_setup.camera_surface.latest().is_none());

        state.toggle_media_call_mic();
        state.toggle_media_call_camera();

        assert!(state.media_setup.microphone_enabled());
        assert!(state.media_setup.camera_enabled());
        assert_eq!(state.media_setup.microphone_access, MediaAccessState::Ready);
        assert_eq!(state.media_setup.camera_access, MediaAccessState::Ready);
    }

    #[test]
    fn media_setup_recovery_copy_stays_compact_and_keeps_detail_accessible() {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let detail = "microphone permission denied by operating system privacy settings";
        shared.chat_mut().media_setup.microphone_access =
            MediaAccessState::PermissionDenied(detail.to_string());

        assert_eq!(
            media_setup_recovery_label(&shared.chat()),
            "Allow access, then retry."
        );
        assert!(media_setup_recovery_label(&shared.chat()).chars().count() <= 30);
        assert!(media_call_accessibility_summary(&shared).contains(detail));

        shared.chat_mut().media_setup.microphone_access = MediaAccessState::Ready;
        shared.chat_mut().media_setup.camera_access =
            MediaAccessState::Busy("camera is in use".to_string());
        assert_eq!(
            media_setup_recovery_label(&shared.chat()),
            "Device busy; close other apps."
        );
    }

    #[test]
    fn device_selection_index_does_not_replace_the_media_panel_tree() {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        shared
            .chat_mut()
            .media_setup
            .microphones
            .push(crate::ui::media_setup::MediaDeviceOption {
                native_id: Some("desk-mic".to_string()),
                display_name: "Desk microphone".to_string(),
                is_default: false,
            });
        let before = media_call_panel_signature(&shared);

        shared.chat_mut().media_setup.microphone_index = 1;

        assert_eq!(media_call_panel_signature(&shared), before);
    }

    #[test]
    fn rendered_voice_link_button_opens_setup_before_starting_media_call() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.info.provider = "openai".to_string();
            chat.info.model = "gpt-5".to_string();
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat media controls")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Set up voice link")
                .count()?,
            1
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Show media controls")
                .count()?,
            0,
            "idle media controls should not consume a transcript row"
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Start media call")
                .count()?,
            0,
            "idle media controls should be hidden until the voice link opens"
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Set up voice link")
            .click()?;
        assert!(!shared.chat().media_call.active);
        assert!(shared.chat().media_call_details_visible());
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Hide media controls")
                .count()?,
            1
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Start media call")
                .count()?,
            1
        );
        for name in ["Microphone device", "Speaker device", "Camera device"] {
            assert_eq!(
                window
                    .get_by_role(SemanticsRole::ComboBox)
                    .with_name(name)
                    .count()?,
                1,
                "setup should expose the {name} selector"
            );
        }
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Retry media device access")
                .count()?,
            1
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Open media privacy settings")
                .count()?,
            1
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Test selected speaker")
                .count()?,
            1
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Start media call")
            .click()?;
        assert!(
            shared.chat().media_call.active,
            "start button should activate the call; notice={:?}",
            shared.chat().media_call.notice
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("End media call")
                .count()?,
            1
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Mute microphone")
                .count()?,
            1
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Pause camera")
                .count()?,
            1
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Hide media controls")
            .click()?;
        assert!(!shared.chat().media_call_details_visible());
        assert!(
            shared.chat().media_call_panel_visible(),
            "hiding active controls should keep the compact media row visible"
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Show media controls")
                .count()?,
            1
        );

        Ok(())
    }

    #[test]
    fn rendered_active_media_call_keeps_compact_status_after_hiding_controls() -> sui::Result<()> {
        use sui::{Event, WindowEvent};

        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.info.provider = "openai".to_string();
            chat.info.model = "gpt-5".to_string();
            chat.models_loading = false;
            chat.media_call.active = true;
            chat.media_call.provider = "OpenAI".to_string();
            chat.media_call.model = "gpt-5".to_string();
            chat.media_call.started_at = Some(Instant::now());
            chat.media_call_details_open = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Active chat media controls")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        window
            .root()
            .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(320.0, 480.0))))?;
        app.run_until_idle()?;

        assert!(shared.chat().media_call_panel_visible());
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Show media controls")
                .count()?,
            1,
            "active calls should keep a compact media status row available"
        );
        let compact_snapshot = window.snapshot()?;
        let media = compact_snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::GenericContainer
                    && node.name.as_deref() == Some("Media call")
            })
            .expect("compact media region");
        assert!(media.bounds.height() <= MEDIA_CALL_COMPACT_H + 0.5);
        let contains = |outer: Rect, inner: Rect| {
            inner.x() >= outer.x() - 0.5
                && inner.y() >= outer.y() - 0.5
                && inner.max_x() <= outer.max_x() + 0.5
                && inner.max_y() <= outer.max_y() + 0.5
        };
        for node in compact_snapshot.accessibility.nodes.iter().filter(|node| {
            matches!(
                node.name.as_deref(),
                Some("Show media controls" | "Media call" | "LIVE")
            )
        }) {
            assert!(
                contains(media.bounds, node.bounds),
                "compact media child must stay inside its row: {:?}",
                node.bounds
            );
        }
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Start media call")
                .count()?,
            0
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Show media controls")
            .click()?;
        assert!(shared.chat().media_call_details_visible());
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("End media call")
                .count()?,
            1
        );

        Ok(())
    }

    #[test]
    fn rendered_media_setup_stays_separated_at_320px_and_bad_preview_falls_back() -> sui::Result<()>
    {
        use sui::{Event, WindowEvent};

        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.models_loading = false;
            chat.media_call_details_open = true;
            chat.media_setup.start();
            publish_test_camera_frame(&chat, 7, 2, 2, Arc::<[u8]>::from(vec![255; 3]));
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Compact media setup")
                    .root(view(shared)),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        window
            .root()
            .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(320.0, 720.0))))?;
        app.run_until_idle()?;

        if let Some(path) = std::env::var_os("SINOMO_MEDIA_LAYOUT_SHOT") {
            let path = std::path::PathBuf::from(path);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).expect("create media layout screenshot directory");
            }
            window
                .capture_screenshot()?
                .write_png(&path)
                .expect("write media layout screenshot");
        }

        let snapshot = window.snapshot()?;
        let media = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::GenericContainer
                    && node.name.as_deref() == Some("Media call")
            })
            .expect("expanded media region");
        assert!(
            (MEDIA_CALL_H - 1.0..=MEDIA_CALL_H + 0.5).contains(&media.bounds.height()),
            "expanded media panel should reserve only its fixed layout height: {:?}",
            media.bounds
        );
        let combos = snapshot
            .accessibility
            .nodes
            .iter()
            .filter(|node| node.role == SemanticsRole::ComboBox)
            .filter(|node| {
                matches!(
                    node.name.as_deref(),
                    Some("Microphone device" | "Speaker device" | "Camera device")
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(combos.len(), 3);
        assert!(combos.iter().all(|node| {
            node.bounds.x() >= 0.0 && node.bounds.max_x() <= 320.0 && node.bounds.width() > 0.0
        }));
        let selector_y = combos
            .iter()
            .map(|node| node.bounds.y())
            .fold(f32::INFINITY, f32::min);
        let retry = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::Button
                    && node.name.as_deref() == Some("Retry media device access")
            })
            .expect("retry access button");
        assert!(
            retry.bounds.max_y() <= selector_y + 0.5,
            "access controls must not overlap selectors: retry={:?}, selector_y={selector_y}",
            retry.bounds
        );
        let preview = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::Image && node.name.as_deref() == Some("Camera preview")
            })
            .expect("camera preview semantics");
        assert!(
            preview.bounds.height() >= 80.0,
            "camera preview should remain useful at narrow widths: {:?}",
            preview.bounds
        );
        assert!((1.6..=2.0).contains(&(preview.bounds.width() / preview.bounds.height())));

        let named = |name: &str| {
            snapshot
                .accessibility
                .nodes
                .iter()
                .find(|node| node.name.as_deref() == Some(name))
                .unwrap_or_else(|| panic!("missing media control {name}"))
        };
        let contains = |outer: Rect, inner: Rect| {
            inner.x() >= outer.x() - 0.5
                && inner.y() >= outer.y() - 0.5
                && inner.max_x() <= outer.max_x() + 0.5
                && inner.max_y() <= outer.max_y() + 0.5
        };
        for name in [
            "Hide media controls",
            "Retry media device access",
            "Test selected speaker",
            "Open media privacy settings",
            "Mute microphone",
            "Start media call",
            "Pause camera",
        ] {
            let child = named(name);
            assert!(
                contains(media.bounds, child.bounds),
                "{name} must stay inside the expanded media panel: {:?}",
                child.bounds
            );
        }
        let meter = named("Microphone input level");
        let speaker_test = named("Test selected speaker");
        let privacy = named("Open media privacy settings");
        let start = named("Start media call");
        let selector_bottom = combos
            .iter()
            .map(|node| node.bounds.max_y())
            .fold(f32::NEG_INFINITY, f32::max);
        assert!(selector_bottom <= meter.bounds.y() + 0.5);
        assert!(selector_bottom <= speaker_test.bounds.y() + 0.5);
        assert!(meter.bounds.max_y() <= privacy.bounds.y() + 0.5);
        assert!(speaker_test.bounds.max_y() <= privacy.bounds.y() + 0.5);
        assert!(privacy.bounds.max_y() <= start.bounds.y() + 0.5);

        Ok(())
    }

    #[test]
    fn rendered_camera_preview_refreshes_same_sequence_and_reuses_image_handle() -> sui::Result<()>
    {
        use sui::{Event, WindowEvent};

        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let first = {
            let mut chat = shared.chat_mut();
            chat.media_call_details_open = true;
            chat.media_setup.start();
            publish_test_camera_frame(&chat, 0, 2, 1, [255, 0, 0, 255, 255, 0, 0, 255])
        };

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Stable camera preview resource")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        window.snapshot()?;
        let handle = shared
            .chat()
            .media_preview_registration
            .handle()
            .expect("preview image handle");
        assert_eq!(
            shared.chat().media_preview_registration.revision(),
            Some(first.revision)
        );

        let replacement =
            publish_test_camera_frame(&shared.chat(), 0, 2, 1, [0, 0, 255, 255, 0, 0, 255, 255]);
        window
            .root()
            .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(800.0, 600.0))))?;
        app.run_until_idle()?;
        window.snapshot()?;
        assert_eq!(
            shared.chat().media_preview_registration.revision(),
            Some(replacement.revision),
            "a restarted capture can reuse sequence zero without drawing the stale frame"
        );

        shared.chat_mut().media_call.mic_enabled = false;
        window
            .root()
            .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(801.0, 600.0))))?;
        app.run_until_idle()?;
        window.snapshot()?;
        assert_eq!(
            shared.chat().media_preview_registration.handle(),
            Some(handle),
            "panel rebuilds must overwrite one stable image resource"
        );

        Ok(())
    }

    #[test]
    fn built_app_camera_preview_registers_stable_image_resource() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            chat.media_call_details_open = true;
            chat.media_setup.start();
            publish_test_camera_frame(&chat, 11, 2, 1, [255, 0, 0, 255, 0, 255, 0, 255]);
        }

        let app = crate::build_application(shared.clone())?;
        let mut runtime = app.build()?;
        let window_id = runtime.window_ids()[0];
        runtime.handle_event(
            window_id,
            Event::Window(sui::WindowEvent::Resized(Size::new(800.0, 600.0))),
        )?;
        let output = runtime.render(window_id)?;

        let handle = shared
            .chat()
            .media_preview_registration
            .handle()
            .expect("preview image handle");
        assert!(
            output.frame.image_registry.contains(handle),
            "the production app widget should register its stable preview image in the frame"
        );
        let mut renderer = sui::WgpuRenderer::new();
        renderer.render(&output.frame)?;
        Ok(())
    }

    #[test]
    fn activity_review_popup_exposes_recovery_actions() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.models_loading = false;
            chat.push_activity(
                "chat connection",
                "Agent channel storage could not open through SIFS: device authorization was denied",
                ActivityTone::Warn,
            );
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Activity detail")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Check SIFS")
                .count()?,
            1,
            "the empty recovery card should expose one SIFS diagnostic action"
        );
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Review")
            .click()?;
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Check SIFS")
                .count()?,
            2,
            "opening Activity should add the popup SIFS diagnostic action"
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Connection")
                .count()?,
            1,
            "storage recovery should expose connection settings"
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Show detail")
                .count()?,
            0,
            "recovery activity now uses direct action buttons instead of the detail rail"
        );

        Ok(())
    }

    #[test]
    fn connection_problem_summary_hides_parser_plumbing() {
        let summary = connection_problem_summary(
            "chat unavailable: failed to parse command: open agent channel SIFS store failed: device authorization was denied",
        )
        .expect("connection summary");

        assert_eq!(
            summary,
            "Agent channel storage could not open through SIFS; device authorization was denied."
        );
        assert!(!summary.contains("failed to parse command"));
    }

    #[test]
    fn connection_recovery_display_detail_hides_low_level_storage_errors() {
        let chat_list =
            connection_recovery_display_detail("chat list failed: device authorization was denied")
                .expect("chat list display detail");
        assert_eq!(chat_list, "Sinomo could not load saved chats.");
        assert!(!chat_list.contains("device authorization"));
        assert!(!chat_list.contains("SIFS"));
        assert!(!chat_list.contains("storage"));

        let channel = connection_recovery_display_detail(
            "channel create failed: device authorization was denied",
        )
        .expect("channel display detail");
        assert_eq!(channel, "Sinomo could not open this chat channel.");
        assert!(!channel.contains("device authorization"));
        assert!(!channel.contains("SIFS"));
        assert!(!channel.contains("storage"));

        let sync = connection_recovery_display_detail(
            "publish reconciled agent session checkpoint failed: clustered DFS object has no committed metadata-shard assignment",
        )
        .expect("chat sync display detail");
        assert_eq!(sync, "Sinomo could not synchronize this chat.");

        assert!(connection_recovery_display_detail("provider unavailable").is_none());
    }

    #[test]
    fn chat_history_storage_failures_offer_sifs_diagnostics() {
        assert!(detail_should_offer_sifs_check(
            "chat list failed: The local cluster service is not reachable."
        ));
        assert!(detail_should_offer_sifs_check(
            "chat save failed: daemon not reachable"
        ));
        assert!(!detail_should_offer_sifs_check(
            "provider unavailable: missing API key"
        ));
    }

    #[test]
    fn collapsed_activity_detail_uses_connection_summary() {
        let visible = activity_visible_detail("chat list failed: device authorization was denied");
        assert_eq!(visible, "Sinomo could not load saved chats.");
        assert!(!visible.contains("device authorization"));
        assert!(!visible.contains("storage"));

        let neutral = activity_visible_detail("context assembled: 4.0 KB");
        assert_eq!(neutral, "context assembled: 4.0 KB");
    }

    #[test]
    fn activity_status_summary_keeps_health_card_actionable() {
        assert_eq!(
            activity_status_summary("chat list: device authorization was denied"),
            "Chat history needs recovery"
        );
        assert_eq!(
            activity_status_summary(
                "chat connection: Agent channel storage could not open through SIFS; device authorization was denied.",
            ),
            "Channel storage needs recovery"
        );
        assert_eq!(
            activity_status_summary("runtime control metadata is detached"),
            "Runtime controls need refresh"
        );

        let neutral = activity_status_summary("context assembled: 4.0 KB");
        assert_eq!(neutral, "context assembled: 4.0 KB");
    }

    #[test]
    fn activity_work_summary_hides_zero_count_noise() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);

        assert_eq!(activity_work_summary(&state), "Idle · no queued work");
        assert_eq!(usage_summary(&state), None);

        state.control.active_turn = true;
        state.control.task_count = 2;
        state.control.follow_up_count = 1;
        state.tool_calls.push(ChatToolActivity {
            step_index: 0,
            call_index: Some(0),
            call_id: Some("call-test".to_string()),
            tool: "shell".to_string(),
            input: "cargo check".to_string(),
            input_preview: "cargo check".to_string(),
            status: "running".to_string(),
            process_command: None,
            process_status: None,
            exit_code: None,
            elapsed_ms: None,
            stdout: String::new(),
            stderr: String::new(),
        });

        assert_eq!(
            activity_work_summary(&state),
            "Active · 2 tasks · 1 queued turn · 1 tool update"
        );
    }

    #[test]
    fn usage_summary_only_renders_real_usage() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        assert_eq!(usage_summary(&state), None);

        state.usage = Some(ChatUsageSnapshot {
            provider: "DeepSeek".to_string(),
            model: "deepseek-v4-flash".to_string(),
            input_tokens: Some(1200),
            output_tokens: Some(42),
        });

        assert_eq!(usage_summary(&state), Some("1.2k in · 42 out".to_string()));
    }

    #[test]
    fn streaming_zero_output_usage_reads_as_waiting_and_collapses_activity() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.streaming = true;
        let session_id = state.session_id.clone();
        let usage = ChatUsageSnapshot {
            provider: "DeepSeek".to_string(),
            model: "deepseek-v4-flash".to_string(),
            input_tokens: Some(7067),
            output_tokens: Some(0),
        };

        assert_eq!(
            usage_activity_detail(&usage, true),
            (
                "7067 input · waiting for output".to_string(),
                ActivityTone::Running
            )
        );

        state.apply_event(ChatEvent::Usage {
            session_id: session_id.clone(),
            usage: usage.clone(),
        });
        state.apply_event(ChatEvent::Usage { session_id, usage });

        let usage_entries = state
            .activity
            .iter()
            .filter(|entry| entry.title == "model usage")
            .collect::<Vec<_>>();
        assert_eq!(usage_entries.len(), 1);
        assert_eq!(usage_entries[0].detail, "7067 input · waiting for output");
        assert_eq!(usage_entries[0].tone, ActivityTone::Running);
    }

    #[test]
    fn empty_turn_done_marks_placeholder_as_system_info() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.set_draft("hi".to_string());
        state.submit();

        state.apply_event(ChatEvent::TurnDone {
            session_id: state.session_id.clone(),
        });

        assert_eq!(state.messages.len(), 2);
        assert_eq!(state.messages[1].text, "(no response)");
        assert_eq!(state.messages[1].source, ChatMessageSource::System);
        assert_eq!(state.messages[1].kind, ChatMessageKind::Info);
    }

    #[test]
    fn turn_error_replaces_pending_agent_placeholder() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.set_draft("hi".to_string());
        state.submit();

        state.apply_event(ChatEvent::Error {
            session_id: Some(state.session_id.clone()),
            error: "authentication required for codex: no Sinomo Codex ChatGPT sign-in found; run `sinomo providers auth codex`"
                .to_string(),
        });

        assert!(!state.streaming, "provider errors end the visible stream");
        assert!(
            !state.control.active_turn,
            "provider errors clear the active turn marker"
        );
        assert_eq!(state.messages.len(), 2, "the placeholder is reused");
        assert_eq!(state.messages[1].role, ChatRole::Agent);
        assert_eq!(state.messages[1].source, ChatMessageSource::System);
        assert_eq!(state.messages[1].kind, ChatMessageKind::Error);
        assert!(
            state.messages[1]
                .text
                .contains("authentication required for codex"),
            "the provider setup error is visible in the transcript"
        );
        assert!(
            state
                .activity
                .iter()
                .any(|entry| entry.title == "agent error" && entry.tone == ActivityTone::Bad)
        );

        state.apply_event(ChatEvent::TurnDone {
            session_id: state.session_id.clone(),
        });
        assert!(
            !state.messages[1].text.contains("(no response)"),
            "TurnDone must not overwrite an already-visible error"
        );
    }

    #[test]
    fn control_events_update_notice_without_touching_streaming_state() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.streaming = true;

        state.apply_event(ChatEvent::ControlStatus(
            sinomo_cli::app_api::ChatControlStatus {
                session_id: "sess_ui_control".to_string(),
                accepted: true,
                runtime_attached: true,
                sequence: 3,
                event_count: 1,
                active_turn: true,
                task_count: 0,
                terminal_result_count: 0,
                tasks: Vec::new(),
                queued_input: None,
                removed_input_id: None,
            },
        ));
        assert!(state.streaming);
        assert_eq!(
            state.notice.as_deref(),
            Some("agent control accepted at sequence 3")
        );
        assert!(state.control.runtime_attached);
        assert_eq!(state.runtime_health().label, "RUNNING");

        state.apply_event(ChatEvent::ControlState(
            sinomo_cli::app_api::ChatControlState {
                session_id: "sess_ui_control".to_string(),
                accepted: true,
                steering_count: 2,
                follow_up_count: 1,
                ..Default::default()
            },
        ));
        assert!(state.streaming);
        assert_eq!(
            state.notice.as_deref(),
            Some("agent control: 2 steering, 1 follow-ups")
        );
    }

    #[test]
    fn accepted_control_status_clears_detached_runtime_warning() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.control.runtime_attached = false;

        state.apply_event(ChatEvent::ControlStatus(
            sinomo_cli::app_api::ChatControlStatus {
                session_id: "sess_ui_control_done".to_string(),
                accepted: true,
                runtime_attached: true,
                sequence: 4,
                event_count: 1,
                active_turn: false,
                task_count: 0,
                terminal_result_count: 1,
                tasks: Vec::new(),
                queued_input: None,
                removed_input_id: None,
            },
        ));

        let health = state.runtime_health();
        assert_ne!(
            health.detail, "runtime control metadata is detached",
            "completed accepted control status should not leave a stale runtime warning"
        );
        assert_ne!(health.label, "DEGRADED");
    }

    #[test]
    fn structured_runtime_events_update_context_usage_and_activity() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.streaming = true;
        let session_id = state.session_id.clone();

        state.apply_event(ChatEvent::Context {
            session_id: session_id.clone(),
            context: ChatContextSnapshot {
                step_index: 0,
                provider: "codex".to_string(),
                model: "gpt-5.5-codex".to_string(),
                total_bytes: 4096,
                max_context_bytes: Some(8192),
                compaction_threshold_bytes: Some(7000),
                lanes: vec![sinomo_cli::app_api::ChatContextLane {
                    kind: "recent_tail".to_string(),
                    label: "recent transcript".to_string(),
                    bytes: 1024,
                    priority: 2,
                }],
                included_count: 3,
                omitted_count: 1,
            },
        });
        assert_eq!(
            state.context.as_ref().map(|ctx| ctx.total_bytes),
            Some(4096)
        );
        assert_eq!(state.notice.as_deref(), Some("context assembled: 4.0 KB"));

        state.apply_event(ChatEvent::ToolCallCompleted {
            session_id: session_id.clone(),
            call: ChatToolCallSnapshot {
                step_index: 0,
                call_index: Some(0),
                call_id: Some("call_1".to_string()),
                tool: "run_command".to_string(),
                input: Some("{\"cmd\":\"date\"}".to_string()),
                input_preview: Some("{\"cmd\":\"date\"}".to_string()),
            },
        });
        state.apply_event(ChatEvent::Process {
            session_id: session_id.clone(),
            process: ChatProcessSnapshot {
                process_id: "proc_1".to_string(),
                command: "date".to_string(),
                status: "completed".to_string(),
                exit_code: Some(0),
                elapsed_ms: 12,
                stdout: "Mon Jun 15".to_string(),
                stderr: String::new(),
                stdout_tail: "Mon Jun 15".to_string(),
                stderr_tail: String::new(),
            },
        });
        state.apply_event(ChatEvent::Usage {
            session_id: session_id.clone(),
            usage: ChatUsageSnapshot {
                provider: "codex".to_string(),
                model: "gpt-5.5-codex".to_string(),
                input_tokens: Some(120),
                output_tokens: Some(18),
            },
        });

        assert_eq!(
            state.usage.as_ref().and_then(|usage| usage.input_tokens),
            Some(120)
        );
        assert!(
            state
                .activity
                .iter()
                .any(|entry| entry.title.contains("run_command"))
        );
        assert!(
            state
                .activity
                .iter()
                .any(|entry| entry.title == "date" && entry.detail.contains("exit 0"))
        );
        assert!(
            state
                .activity
                .iter()
                .any(|entry| entry.title == "model usage" && entry.detail.contains("120 input"))
        );
    }

    #[test]
    fn tool_call_monitor_tracks_started_streaming_and_completed_calls() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        let session_id = state.session_id.clone();

        state.apply_event(ChatEvent::ToolCallStarted {
            session_id: session_id.clone(),
            call: ChatToolCallSnapshot {
                step_index: 0,
                call_index: Some(0),
                call_id: Some("call_1".to_string()),
                tool: "run_command".to_string(),
                input: None,
                input_preview: None,
            },
        });
        assert_eq!(state.tool_calls.len(), 1);
        assert_eq!(state.tool_calls[0].status, "running");

        state.apply_event(ChatEvent::ToolCallDelta {
            session_id: session_id.clone(),
            step_index: 0,
            call_id: Some("call_1".to_string()),
            tool: Some("run_command".to_string()),
            arguments_delta: "{\"cmd\":\"date\"}".to_string(),
        });
        assert_eq!(state.tool_calls.len(), 1);
        assert_eq!(state.tool_calls[0].status, "streaming");
        assert!(state.tool_calls[0].input_preview.contains("date"));

        state.apply_event(ChatEvent::ToolCallCompleted {
            session_id: session_id.clone(),
            call: ChatToolCallSnapshot {
                step_index: 0,
                call_index: Some(0),
                call_id: Some("call_1".to_string()),
                tool: "run_command".to_string(),
                input: Some("{\"cmd\":\"date\"}".to_string()),
                input_preview: Some("{\"cmd\":\"date\"}".to_string()),
            },
        });
        assert_eq!(state.tool_calls.len(), 1);
        assert_eq!(state.tool_calls[0].status, "requested");
        assert_eq!(state.tool_calls[0].tool, "run_command");
        assert!(state.activity.iter().any(|entry| {
            entry.title.contains("tool requested") && entry.tone == ActivityTone::Neutral
        }));
        assert!(
            !state
                .activity
                .iter()
                .any(|entry| entry.title.contains("tool requested")
                    && entry.tone == ActivityTone::Good),
            "a fully assembled tool request is not evidence that any tool executed successfully"
        );
    }

    #[test]
    fn tool_call_render_state_keeps_full_payloads_and_edit_diffs() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        let session_id = state.session_id.clone();
        let patch = format!(
            "*** Begin Patch\n*** Update File: crates/sinomo-app/src/ui/chat.rs\n@@\n-{}\n+{}\n*** End Patch\n",
            "old line\n".repeat(80),
            "new line\n".repeat(80)
        );
        let input = serde_json::to_string_pretty(&serde_json::json!({ "patch": patch }))
            .expect("serialize patch input");

        state.apply_event(ChatEvent::ToolCallCompleted {
            session_id: session_id.clone(),
            call: ChatToolCallSnapshot {
                step_index: 2,
                call_index: Some(0),
                call_id: Some("call_patch".to_string()),
                tool: "apply_patch".to_string(),
                input: Some(input.clone()),
                input_preview: Some(compact_line(&input, 80)),
            },
        });
        state.apply_event(ChatEvent::Process {
            session_id,
            process: ChatProcessSnapshot {
                process_id: "proc_patch".to_string(),
                command: "apply_patch".to_string(),
                status: "completed".to_string(),
                exit_code: Some(0),
                elapsed_ms: 20,
                stdout: "patched 1 file".repeat(80),
                stderr: String::new(),
                stdout_tail: "patched 1 file".to_string(),
                stderr_tail: String::new(),
            },
        });

        let call = state.tool_calls.first().expect("tool call retained");
        assert_eq!(call.input, input);
        assert!(
            call.stdout.len() > call.input_preview.len(),
            "full output should be retained for expansion"
        );
        let edit = chat_render::file_edit_render(&call.tool, &call.input).expect("edit render");
        assert!(edit.code.is_diff);
        assert_eq!(edit.code.language.as_deref(), Some("diff"));
        assert!(edit.code.text.contains("*** Update File"));
    }

    #[test]
    fn tool_chain_and_long_details_are_expanders() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        let long_input = serde_json::to_string_pretty(&serde_json::json!({
            "cmd": "cargo test",
            "notes": "x".repeat(900)
        }))
        .expect("serialize input");
        state.tool_calls = vec![
            ChatToolActivity {
                step_index: 0,
                call_index: Some(0),
                call_id: Some("call_a".to_string()),
                tool: "exec_command".to_string(),
                input: long_input,
                input_preview: "cargo test".to_string(),
                status: "completed".to_string(),
                process_command: Some("cargo test".to_string()),
                process_status: Some("completed".to_string()),
                exit_code: Some(0),
                elapsed_ms: Some(10),
                stdout: "ok".to_string(),
                stderr: String::new(),
            },
            ChatToolActivity {
                step_index: 1,
                call_index: Some(0),
                call_id: Some("call_b".to_string()),
                tool: "read_file".to_string(),
                input: "{\"path\":\"Cargo.toml\"}".to_string(),
                input_preview: "Cargo.toml".to_string(),
                status: "running".to_string(),
                process_command: None,
                process_status: None,
                exit_code: None,
                elapsed_ms: None,
                stdout: String::new(),
                stderr: String::new(),
            },
        ];

        assert!(state.tool_chain_collapsible());
        assert!(!state.tool_chain_expanded());
        state.toggle_tool_chain_expanded();
        assert!(state.tool_chain_expanded());

        let key = tool_detail_key(&state.tool_calls[0], ToolDetailSection::Input);
        assert!(!state.tool_detail_expanded(&key));
        state.toggle_tool_detail(key.clone());
        assert!(state.tool_detail_expanded(&key));
    }

    #[test]
    fn rendered_tool_expanders_toggle_from_semantics() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.models_loading = false;
            chat.messages = vec![
                ChatMessage::user("Patch the chat renderer"),
                ChatMessage::model(
                    "## Plan\n\n- Parse markdown\n- Render tool calls\n\n```rust\nfn main() {}\n```",
                ),
            ];
            let patch = format!(
                "*** Begin Patch\n*** Update File: src/lib.rs\n@@\n-{}\n+{}\n*** End Patch\n",
                "old\n".repeat(80),
                "new\n".repeat(80)
            );
            let input = serde_json::to_string_pretty(&serde_json::json!({ "patch": patch }))
                .expect("serialize patch input");
            chat.tool_calls = vec![
                ChatToolActivity {
                    step_index: 0,
                    call_index: Some(0),
                    call_id: Some("call_patch".to_string()),
                    tool: "apply_patch".to_string(),
                    input,
                    input_preview: "patch chat renderer".to_string(),
                    status: "completed".to_string(),
                    process_command: Some("apply_patch".to_string()),
                    process_status: Some("completed".to_string()),
                    exit_code: Some(0),
                    elapsed_ms: Some(15),
                    stdout: "patched".to_string(),
                    stderr: String::new(),
                },
                ChatToolActivity {
                    step_index: 1,
                    call_index: Some(0),
                    call_id: Some("call_test".to_string()),
                    tool: "exec_command".to_string(),
                    input: "{\"cmd\":\"cargo test -p sinomo-app\"}".to_string(),
                    input_preview: "cargo test -p sinomo-app".to_string(),
                    status: "running".to_string(),
                    process_command: None,
                    process_status: None,
                    exit_code: None,
                    elapsed_ms: None,
                    stdout: String::new(),
                    stderr: String::new(),
                },
            ];
            chat.stick_bottom = true;
            chat.scroll = f32::MAX;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat tool expanders")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert!(!shared.chat().tool_chain_expanded());
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Toggle tool activity")
            .click()?;
        assert!(shared.chat().tool_chain_expanded());

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Toggle tool detail")
            .click()?;
        assert!(
            !shared.chat().expanded_tool_details.is_empty(),
            "clicking the long detail expander should persist expanded state"
        );

        Ok(())
    }

    #[test]
    fn runtime_health_surfaces_degraded_cluster_and_persistence_state() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.notice = Some("chat save failed: DFS services shard replay delayed".to_string());

        let health = state.runtime_health();
        assert_eq!(health.label, "DEGRADED");
        assert_eq!(health.tone, ActivityTone::Warn);
        assert!(
            health.detail.contains("chat save failed"),
            "health detail should preserve the operator-relevant degraded signal"
        );

        state.notice = None;
        state.control.active_turn = true;
        state.control.task_count = 1;
        state.control.runtime_attached = false;
        let health = state.runtime_health();
        assert_eq!(health.label, "DEGRADED");
        assert_eq!(health.tone, ActivityTone::Warn);
        assert!(health.detail.contains("detached"));
    }

    #[test]
    fn runtime_health_prioritizes_failed_tasks_and_errors() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.control.tasks.push(ChatTaskSnapshot {
            task_id: "task_failed".to_string(),
            kind: "verification".to_string(),
            summary: "cluster readiness gate".to_string(),
            status: "failed".to_string(),
            progress_summary: None,
            result_ref: None,
            active_op_id: None,
        });

        let health = state.runtime_health();
        assert_eq!(health.label, "ATTENTION");
        assert_eq!(health.tone, ActivityTone::Bad);
        assert!(health.detail.contains("cluster readiness gate"));

        state.control.tasks.clear();
        state.apply_event(ChatEvent::Error {
            session_id: Some(state.session_id.clone()),
            error: "provider stream failed".to_string(),
        });
        let health = state.runtime_health();
        assert_eq!(health.label, "ATTENTION");
        assert_eq!(health.tone, ActivityTone::Bad);
        assert!(
            state
                .activity
                .iter()
                .any(|entry| entry.title == "agent error" && entry.tone == ActivityTone::Bad)
        );
    }

    #[test]
    fn connection_worker_errors_are_activity_not_agent_failures() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);

        state.apply_event(ChatEvent::Error {
            session_id: Some(state.session_id.clone()),
            error: "chat unavailable: failed to parse command: open agent channel SIFS store failed: device authorization was denied".to_string(),
        });

        assert!(
            state.activity.iter().any(|entry| {
                entry.title == "chat connection"
                    && entry.tone == ActivityTone::Warn
                    && entry.detail.contains("Agent channel storage")
                    && !entry.detail.contains("failed to parse command")
            }),
            "storage-derived worker errors should read as chat connection recovery"
        );
        assert!(
            state
                .activity
                .iter()
                .all(|entry| entry.title != "agent error"),
            "connection failures should not be labeled as generic agent failures"
        );
        let health = state.runtime_health();
        assert_eq!(health.label, "DEGRADED");
        assert_eq!(health.tone, ActivityTone::Warn);
        assert!(health.detail.contains("chat connection"));
        assert!(!health.detail.contains("failed to parse command"));
    }

    #[test]
    fn runtime_recovery_target_routes_to_actionable_settings_section() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.providers.clear();
        assert_eq!(
            state.runtime_recovery_target(),
            Some(RuntimeRecoveryTarget::Providers)
        );
        assert_eq!(
            RuntimeRecoveryTarget::Providers.footer_label(),
            "Provider setup"
        );
        assert_eq!(
            state.recovery_empty_detail(RuntimeRecoveryTarget::Providers),
            "No language model provider is configured yet."
        );

        seed_default_provider(&mut state);
        state.notice = Some("chat save failed: DFS services shard replay delayed".to_string());
        assert_eq!(
            state.runtime_recovery_target(),
            Some(RuntimeRecoveryTarget::Connection)
        );
        assert_eq!(
            RuntimeRecoveryTarget::Connection.footer_label(),
            "Connection"
        );
        assert!(
            state
                .recovery_empty_detail(RuntimeRecoveryTarget::Connection)
                .contains("Sinomo could not save chat history")
        );

        state.notice = Some("chat list failed: device authorization was denied".to_string());
        assert_eq!(
            state.runtime_recovery_target(),
            Some(RuntimeRecoveryTarget::Connection)
        );
        assert_eq!(
            state.recovery_empty_title(RuntimeRecoveryTarget::Connection),
            "Chat list needs connection attention"
        );
        assert_eq!(
            state.recovery_empty_detail(RuntimeRecoveryTarget::Connection),
            "Sinomo could not load saved chats."
        );
        assert!(
            !state
                .recovery_empty_detail(RuntimeRecoveryTarget::Connection)
                .contains("device authorization")
        );
        assert!(
            !state
                .recovery_empty_detail(RuntimeRecoveryTarget::Connection)
                .contains("storage")
        );

        state.notice = Some("channel create failed: device authorization was denied".to_string());
        assert_eq!(
            state.runtime_recovery_target(),
            Some(RuntimeRecoveryTarget::Connection)
        );
        assert_eq!(
            state.recovery_empty_title(RuntimeRecoveryTarget::Connection),
            "Channel needs connection attention"
        );
        assert_eq!(
            state.recovery_empty_detail(RuntimeRecoveryTarget::Connection),
            "Sinomo could not open this chat channel."
        );
        assert_eq!(
            state.recovery_empty_hint(RuntimeRecoveryTarget::Connection),
            "Open Connection settings if channel creation keeps failing; inspect SIFS from Files for storage diagnostics."
        );

        state.notice = Some(
            "authentication required for codex: no Sinomo Codex ChatGPT sign-in found".to_string(),
        );
        assert_eq!(
            state.runtime_recovery_target(),
            Some(RuntimeRecoveryTarget::Providers)
        );

        state.notice = None;
        state.info.provider = "deepseek".to_string();
        state.info.model = "deepseek-chat".to_string();
        assert_eq!(state.runtime_recovery_target(), None);
    }

    #[test]
    fn retry_chat_list_clears_stale_connection_warning_after_success() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.notice = Some("chat list failed: device authorization was denied".to_string());
        state.push_activity(
            "chat list",
            "device authorization was denied",
            ActivityTone::Warn,
        );

        state.retry_chat_list_with_waker(None);
        wait_for(&mut state, Duration::from_secs(5), |state| {
            !state.session_list_loading
        });

        assert!(state.notice.is_none());
        assert!(
            state
                .activity
                .iter()
                .all(|entry| entry.title != "chat list")
        );
        assert_eq!(state.runtime_recovery_target(), None);
    }

    #[test]
    fn rendered_retry_chat_list_button_uses_existing_refresh_path() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.notice = Some("chat list failed: device authorization was denied".to_string());
            chat.push_activity(
                "chat list",
                "device authorization was denied",
                ActivityTone::Warn,
            );
            chat.messages.push(ChatMessage::user("Existing context"));
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat retry")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Review")
            .click()?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Retry chat list")
            .click()?;

        let deadline = Instant::now() + Duration::from_secs(5);
        while shared.chat().session_list_loading && Instant::now() < deadline {
            shared.chat_mut().pump_session_list();
            app.run_until_idle()?;
            std::thread::sleep(Duration::from_millis(10));
        }

        let chat = shared.chat();
        assert!(!chat.session_list_loading, "retry worker should complete");
        assert!(chat.notice.is_none());
        assert!(chat.activity.iter().all(|entry| entry.title != "chat list"));
        assert_eq!(chat.runtime_recovery_target(), None);

        Ok(())
    }

    #[test]
    fn rendered_empty_recovery_card_suppresses_duplicate_footer_actions() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.notice = Some("chat list failed: device authorization was denied".to_string());
            chat.push_activity(
                "chat list",
                "device authorization was denied",
                ActivityTone::Warn,
            );
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat recovery card duplicate actions")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Retry chat list")
                .count()?,
            0,
            "the footer retry button should not duplicate the recovery card action"
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Connection")
                .count()?,
            0,
            "the footer settings button should not duplicate the recovery card action"
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Retry now")
                .with_description("Retry the chat list")
                .count()?,
            1,
            "the card keeps a single primary retry action"
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Connection settings")
                .with_description("Connection settings")
                .count()?,
            1,
            "the card keeps a single settings action"
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Check SIFS")
                .with_description("Open SIFS browser")
                .count()?,
            1,
            "the card exposes the SIFS diagnostic path without duplicating it in the footer"
        );

        Ok(())
    }

    #[test]
    fn rendered_recovery_card_retry_button_uses_existing_refresh_path() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.notice = Some("chat list failed: device authorization was denied".to_string());
            chat.push_activity(
                "chat list",
                "device authorization was denied",
                ActivityTone::Warn,
            );
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat recovery card retry")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Retry now")
            .with_description("Retry the chat list")
            .click()?;

        let chat = shared.chat();
        assert!(chat.notice.is_none());
        assert!(chat.activity.iter().all(|entry| entry.title != "chat list"));
        assert_eq!(chat.runtime_recovery_target(), None);

        Ok(())
    }

    #[test]
    fn rendered_recovery_card_settings_button_opens_connection_settings() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.notice = Some("chat list failed: device authorization was denied".to_string());
            chat.push_activity(
                "chat list",
                "device authorization was denied",
                ActivityTone::Warn,
            );
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat recovery card settings")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Connection settings")
            .with_description("Connection settings")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        assert_eq!(
            shared.settings().selected,
            Some(SettingsSection::Connection)
        );

        Ok(())
    }

    #[test]
    fn rendered_recovery_card_check_sifs_opens_files() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.notice = Some("chat list failed: device authorization was denied".to_string());
            chat.push_activity(
                "chat list",
                "device authorization was denied",
                ActivityTone::Warn,
            );
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat recovery card SIFS")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Check SIFS")
            .with_description("Open SIFS browser")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Files);

        Ok(())
    }

    #[test]
    fn recovery_card_pointer_check_sifs_opens_files() -> sui::Result<()> {
        use sui::{
            Event, Point, PointerButton, PointerButtons, PointerEvent, PointerEventKind,
            WindowEvent,
        };

        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.notice = Some("chat list failed: device authorization was denied".to_string());
            chat.push_activity(
                "chat list",
                "device authorization was denied",
                ActivityTone::Warn,
            );
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat recovery card pointer")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        let root = window.root();
        root.dispatch_event(Event::Window(WindowEvent::Resized(Size::new(
            1280.0, 720.0,
        ))))?;
        app.run_until_idle()?;

        let snapshot = window.snapshot()?;
        let check_sifs = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::Button
                    && node.name.as_deref() == Some("Check SIFS")
                    && node.description.as_deref() == Some("Open SIFS browser")
            })
            .expect("Check SIFS recovery button");
        let pos = Point::new(
            check_sifs.bounds.x() + check_sifs.bounds.width() * 0.5,
            check_sifs.bounds.y() + check_sifs.bounds.height() * 0.5,
        );
        root.dispatch_event(Event::Pointer(PointerEvent::new(
            PointerEventKind::Move,
            pos,
        )))?;
        let mut down = PointerEvent::new(PointerEventKind::Down, pos);
        down.button = Some(PointerButton::Primary);
        down.buttons = PointerButtons::new(1);
        root.dispatch_event(Event::Pointer(down))?;
        let mut up = PointerEvent::new(PointerEventKind::Up, pos);
        up.button = Some(PointerButton::Primary);
        root.dispatch_event(Event::Pointer(up))?;
        app.run_until_idle()?;

        assert_eq!(shared.borrow().active_tab, Tab::Files);

        Ok(())
    }

    #[test]
    fn rendered_channel_recovery_card_hides_chat_list_retry() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.notice =
                Some("channel create failed: device authorization was denied".to_string());
            chat.push_activity(
                "channel create",
                "device authorization was denied",
                ActivityTone::Warn,
            );
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Channel recovery card settings")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Retry now")
                .count()?,
            0,
            "channel creation failures should not offer a chat-list retry action"
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Connection settings")
            .with_description("Connection settings")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        assert_eq!(
            shared.settings().selected,
            Some(SettingsSection::Connection)
        );

        Ok(())
    }

    #[test]
    fn rendered_channel_recovery_footer_offers_sifs_check_without_retry() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.notice =
                Some("channel create failed: device authorization was denied".to_string());
            chat.push_activity(
                "channel create",
                "device authorization was denied",
                ActivityTone::Warn,
            );
            chat.messages.push(ChatMessage::user("Existing context"));
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Channel recovery footer settings")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Retry chat list")
                .count()?,
            0,
            "the activity popup should not offer chat-list retry for channel failures"
        );
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Review")
            .click()?;
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Check SIFS")
                .count()?,
            1,
            "the activity popup should expose the SIFS diagnostic path for channel failures"
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Check SIFS")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Files);

        Ok(())
    }

    #[test]
    fn rendered_channel_recovery_footer_settings_opens_connection_settings() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.notice =
                Some("channel create failed: device authorization was denied".to_string());
            chat.push_activity(
                "channel create",
                "device authorization was denied",
                ActivityTone::Warn,
            );
            chat.messages.push(ChatMessage::user("Existing context"));
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Channel recovery footer connection")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Review")
            .click()?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Connection")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        assert_eq!(
            shared.settings().selected,
            Some(SettingsSection::Connection)
        );

        Ok(())
    }

    #[test]
    fn rendered_recovery_card_provider_button_opens_provider_setup() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            chat.providers.clear();
            chat.models_loading = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat recovery card provider")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Set up provider")
                .count()?,
            0,
            "the composer setup button should not duplicate the recovery card action"
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Provider setup")
            .with_description("Provider setup")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        let settings = shared.settings();
        assert_eq!(settings.selected, Some(SettingsSection::Providers));
        assert!(settings.provider_add_open);

        Ok(())
    }

    #[test]
    fn context_lane_summary_formats_bytes_and_share() {
        assert_eq!(
            context_lane_summary("recent turn tail", 2_048, 8_192),
            "recent turn tail · 2.0 KB · 25%"
        );
        assert_eq!(context_lane_summary("empty", 0, 0), "empty · 0 B · 0%");
        let long = context_lane_summary(
            "very long prompt reference label that should compact",
            1_048_576,
            2_097_152,
        );
        assert!(long.starts_with("very long prompt referenc... · 1.0 MB · 50%"));
    }

    #[test]
    fn context_overview_summary_describes_lifecycle_state() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.info.provider = "openai".to_string();
        state.info.model = "gpt-5".to_string();
        state.activity.clear();
        state.notice = None;

        assert_eq!(
            context_overview_summary(&state),
            "Context will assemble when you send a message"
        );

        state.messages.push(ChatMessage::user("hello"));
        assert_eq!(
            context_overview_summary(&state),
            "Context will refresh on the next send"
        );

        state.streaming = true;
        assert_eq!(
            context_overview_summary(&state),
            "Assembling context for this response"
        );

        state.streaming = false;
        state.context = Some(ChatContextSnapshot {
            step_index: 0,
            provider: "openai".to_string(),
            model: "gpt-5".to_string(),
            total_bytes: 4096,
            max_context_bytes: Some(8192),
            compaction_threshold_bytes: Some(7000),
            lanes: vec![sinomo_cli::app_api::ChatContextLane {
                kind: "recent_tail".to_string(),
                label: "recent transcript".to_string(),
                bytes: 1024,
                priority: 2,
            }],
            included_count: 3,
            omitted_count: 1,
        });
        assert_eq!(
            context_overview_summary(&state),
            "Context 4.0 KB of 8.0 KB · 3 sources · 1 omitted"
        );
    }

    #[test]
    fn context_overview_summary_routes_blocked_context_to_setup() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.providers.clear();
        state.models_loading = false;
        state.activity.clear();
        state.notice = None;

        assert_eq!(context_overview_summary(&state), "Provider setup needed");

        seed_default_provider(&mut state);
        state.info.provider = "openai".to_string();
        state.info.model = "gpt-5".to_string();
        state.activity.push(ChatActivityEntry {
            title: "chat list".to_string(),
            detail: "SIFS unavailable: daemon not reachable".to_string(),
            tone: ActivityTone::Warn,
        });

        assert_eq!(
            context_overview_summary(&state),
            "Cluster connection needed"
        );
    }

    #[test]
    fn can_send_requires_nonempty_draft() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        assert!(!state.can_send(), "empty draft cannot send");
        state.set_draft("ok".to_string());
        seed_default_provider(&mut state);
        assert!(state.can_send(), "a non-empty draft can send");
        state.streaming = true;
        assert!(
            state.can_send(),
            "a non-empty draft can queue while streaming"
        );
        state.pending_session_reset = Some(SessionId::generate().to_string());
        assert!(
            state.can_send(),
            "a pending new-session ack must not disable the Send button"
        );
    }

    #[test]
    fn latest_model_answer_can_be_reused_as_draft() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        let answer = "# Draft\n\nFinished text ready to revise.";
        state.messages = vec![
            ChatMessage::user("write a short draft"),
            ChatMessage::model("old answer"),
            ChatMessage::system_info("cluster connected"),
            ChatMessage::model(String::new()),
            ChatMessage::model(answer),
        ];

        assert_eq!(state.latest_reusable_answer(), Some(answer));
        assert!(state.can_reuse_latest_answer());

        state.use_latest_answer_as_draft();

        assert_eq!(state.input, answer);
        assert_eq!(state.cursor, answer.len());
        assert!(
            !state.can_reuse_latest_answer(),
            "a populated draft should hide the reuse action"
        );
        assert!(
            state.draft_revision > 0,
            "loading the answer should refresh the composer host"
        );

        state.clear_draft();
        state.streaming = true;
        assert!(
            !state.can_reuse_latest_answer(),
            "active streams use the interrupt action instead"
        );
    }

    #[test]
    fn attachments_can_be_queued_removed_and_sent_without_text() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        let attachment = temp_attachment_file("queue", "report.txt", b"hello");

        state
            .queue_attachment_path(&format!("\"{}\"", attachment.path.display()))
            .expect("queue attachment");
        assert!(
            state.attachment_operation_pending(),
            "path validation stays off the UI thread"
        );
        wait_for(&mut state, Duration::from_secs(2), |state| {
            !state.attachment_operation_pending()
        });
        assert_eq!(state.attachments.len(), 1);
        assert_eq!(state.attachments[0].file_name, "report.txt");
        assert!(!state.can_send(), "provider setup is still required");

        seed_default_provider(&mut state);
        assert!(
            state.can_send(),
            "a queued attachment can be sent without text"
        );

        let visible = visible_user_message_text("", &state.attachments);
        assert!(visible.contains("Attachments:"));
        assert!(visible.contains("report.txt"));

        state.remove_attachment(0);
        assert!(state.attachments.is_empty());
        assert!(
            !state.can_send(),
            "removing the only attachment disables send"
        );
    }

    #[test]
    fn submit_sends_file_only_attachment_turn() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        let attachment = temp_attachment_file("submit", "image.png", b"png");
        state
            .queue_attachment_path(attachment.path.to_str().expect("utf8 path"))
            .expect("queue attachment");
        wait_for(&mut state, Duration::from_secs(2), |state| {
            !state.attachment_operation_pending()
        });

        state.submit();

        assert!(state.streaming, "attachment-only submit starts a turn");
        assert!(state.input.is_empty());
        assert!(state.attachments.is_empty());
        assert_eq!(state.messages.len(), 2);
        assert!(state.messages[0].text.contains("image.png"));
    }

    #[test]
    fn manual_attachment_path_is_explicit_fallback_and_clears_when_hidden() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);

        assert!(!state.attachment_controls_visible());
        state.open_attachment_surface();
        assert!(state.attachment_controls_visible());
        assert!(!state.attachment_manual_path_open);
        state.toggle_manual_attachment_path();
        assert!(state.attachment_manual_path_open);
        state.set_attachment_path_input("C:\\tmp\\draft.txt".to_string());

        state.toggle_manual_attachment_path();
        assert!(!state.attachment_manual_path_open);
        assert!(
            state.attachment_path_input.is_empty(),
            "hiding the fallback must not leave an invisible unsent path draft"
        );
    }

    #[test]
    fn native_picker_attachment_selection_uses_background_validation_lane() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        let first = temp_attachment_file("picker-first", "first.txt", b"first");
        let second = temp_attachment_file("picker-second", "second.rs", b"second");
        state.session_id = "visible-chat".to_string();
        state.pending_attachment_picker = Some(PendingAttachmentPicker {
            request_id: 41,
            session_id: state.session_id.clone(),
            draft_generation: state.attachment_draft_generation,
        });

        state.apply_attachment_picker_result(NativePickerResult {
            request_id: 41,
            context_generation: state.attachment_draft_generation,
            outcome: NativePickerOutcome::Selected(vec![first.path.clone(), second.path.clone()]),
        });

        assert!(state.attachments.is_empty());
        assert_eq!(state.pending_attachment_validations.len(), 1);
        wait_for(&mut state, Duration::from_secs(2), |state| {
            !state.attachment_operation_pending()
        });
        assert_eq!(state.attachments.len(), 2);
        assert_eq!(state.attachments[0].file_name, "first.txt");
        assert_eq!(state.attachments[1].file_name, "second.rs");
        assert!(state.attachment_picker_error.is_none());
    }

    #[test]
    fn attachment_validation_failure_is_recoverable_and_blocks_early_send() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.set_draft("Review this file".to_string());
        let missing = std::env::temp_dir().join(format!(
            "sinomo-missing-attachment-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));

        state
            .queue_local_attachment_path(missing)
            .expect("queue background validation");
        assert!(!state.can_send(), "pending attachment disables send");
        state.submit();
        assert_eq!(
            state.notice.as_deref(),
            Some("wait for selected files to finish attaching")
        );

        wait_for(&mut state, Duration::from_secs(2), |state| {
            !state.attachment_operation_pending()
        });
        assert!(state.attachments.is_empty());
        assert!(
            state
                .attachment_picker_error
                .as_deref()
                .is_some_and(|error| error.contains("attachment unavailable"))
        );
        assert!(
            state.can_send(),
            "the text draft can be sent after recovering from the failed attachment"
        );
    }

    #[test]
    fn late_native_attachment_picker_result_is_rejected_after_visible_session_changes() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.session_id = "first-chat".to_string();
        state.pending_attachment_picker = Some(PendingAttachmentPicker {
            request_id: 7,
            session_id: state.session_id.clone(),
            draft_generation: state.attachment_draft_generation,
        });
        state.session_id = "second-chat".to_string();

        state.apply_attachment_picker_result(NativePickerResult {
            request_id: 7,
            context_generation: state.attachment_draft_generation,
            outcome: NativePickerOutcome::Selected(vec![PathBuf::from("stale.txt")]),
        });

        assert!(state.pending_attachment_validations.is_empty());
        assert!(state.attachments.is_empty());
    }

    #[test]
    fn resetting_attachment_draft_rejects_late_local_validation_result() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.session_id = "old-chat".to_string();
        let old_generation = state.attachment_draft_generation;
        state
            .pending_attachment_validations
            .push(PendingLocalAttachmentValidation {
                request_id: 9,
                session_id: state.session_id.clone(),
                draft_generation: old_generation,
                ingress: LocalAttachmentIngress::OsDrop,
                paths: vec![PathBuf::from("stale.txt")],
            });

        state.reset_visible_session("new-chat".to_string());
        state.apply_attachment_validation_result(LocalAttachmentValidationResult {
            request_id: 9,
            session_id: "old-chat".to_string(),
            draft_generation: old_generation,
            outcomes: vec![LocalAttachmentValidationOutcome::Ready {
                path: PathBuf::from("stale.txt"),
                file_name: "stale.txt".to_string(),
                size_bytes: 5,
            }],
        });

        assert!(state.pending_attachment_validations.is_empty());
        assert!(state.attachments.is_empty());
    }

    #[test]
    fn sifs_attachment_snapshot_is_queued_in_memory_with_path_provenance() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state
            .queue_sifs_attachment_snapshot(
                "main.rs",
                "/workspace/src/main.rs",
                b"fn main() {}".to_vec(),
            )
            .expect("queue SIFS snapshot");

        let inputs = state.attachment_inputs();
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].file_name, "main.rs");
        match &inputs[0].source {
            ChatAttachmentSource::SifsSnapshot { provenance, bytes } => {
                assert_eq!(provenance.path, "/workspace/src/main.rs");
                assert_eq!(bytes.as_ref(), b"fn main() {}");
            }
            source => panic!("expected SIFS snapshot, got {source:?}"),
        }

        state
            .queue_sifs_attachment_snapshot_with_provenance(
                "main.rs",
                ChatSifsAttachmentProvenance {
                    path: "/workspace/src/main.rs".to_string(),
                    object_id: Some("object-main".to_string()),
                    version: Some("version-2".to_string()),
                    content_hash: Some("sha256:new".to_string()),
                    policy: Some("editable".to_string()),
                    unsaved_editor_buffer: true,
                },
                b"fn main() { println!(\"new\"); }".to_vec(),
            )
            .expect("replace SIFS snapshot");
        assert_eq!(state.attachments.len(), 1, "same SIFS path is refreshed");
        match &state.attachment_inputs()[0].source {
            ChatAttachmentSource::SifsSnapshot { provenance, bytes } => {
                assert!(bytes.as_ref().windows(3).any(|window| window == b"new"));
                assert_eq!(provenance.object_id.as_deref(), Some("object-main"));
                assert_eq!(provenance.version.as_deref(), Some("version-2"));
                assert!(provenance.unsaved_editor_buffer);
            }
            source => panic!("expected SIFS snapshot, got {source:?}"),
        }
    }

    #[test]
    fn local_attachment_viewer_snapshot_keeps_exact_displayed_bytes() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state
            .queue_local_attachment_snapshot(
                "notes.txt",
                "C:\\workspace\\notes.txt",
                b"viewer snapshot".to_vec(),
            )
            .expect("queue local viewer snapshot");

        match &state.attachment_inputs()[0].source {
            ChatAttachmentSource::LocalSnapshot { provenance, bytes } => {
                assert_eq!(provenance.original_path, "C:\\workspace\\notes.txt");
                assert_eq!(provenance.canonical_path, None);
                assert_eq!(bytes.as_ref(), b"viewer snapshot");
            }
            source => panic!("expected local snapshot, got {source:?}"),
        }
    }

    #[test]
    fn external_drag_opens_and_clears_attachment_drop_target() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);

        state.begin_external_attachment_hover();
        assert!(state.attachment_drop_hovered);
        assert_eq!(state.composer_popup(), Some(ComposerPopup::Attachments));

        state.cancel_external_attachment_hover();
        assert!(!state.attachment_drop_hovered);
    }

    fn test_window<W: Widget + 'static>(
        title: &str,
        root: W,
        size: Size,
    ) -> sui::Result<sui_testing::TestWindow> {
        let runtime = sui::Application::new()
            .window(sui::WindowBuilder::new().title(title).root(root))
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        window
            .root()
            .dispatch_event(Event::Window(sui::WindowEvent::Resized(size)))?;
        window.run_until_idle()?;
        Ok(window)
    }

    fn write_visual_debug_artifacts(
        label: &str,
        artifacts: &sui_testing::ArtifactBundle,
    ) -> sui::Result<()> {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("visual-inspection")
            .join(label);
        artifacts.write_to_dir(&dir)?;
        println!("wrote {}", dir.display());
        Ok(())
    }

    fn assert_debug_artifacts_have_visual_content(
        label: &str,
        artifacts: &sui_testing::ArtifactBundle,
    ) -> Rect {
        let scene = artifacts
            .snapshot
            .scene_summary
            .as_ref()
            .unwrap_or_else(|| panic!("{label} debug capture should include a scene summary"));
        assert!(
            scene.command_count > 8,
            "{label} debug capture should include painted scene commands, got {}",
            scene.command_count
        );
        assert!(
            scene.viewport.width > 0.0 && scene.viewport.height > 0.0,
            "{label} debug capture should have a non-empty viewport, got {:?}",
            scene.viewport
        );

        let screenshot = artifacts
            .screenshot
            .as_ref()
            .unwrap_or_else(|| panic!("{label} debug capture should include a screenshot"));
        assert!(
            screenshot.width() > 0 && screenshot.height() > 0,
            "{label} screenshot should be non-empty"
        );
        assert!(
            artifacts.semantics_overlay.is_some(),
            "{label} debug capture should include a semantics overlay"
        );
        assert!(
            artifacts.widget_overlay.is_some(),
            "{label} debug capture should include a widget overlay"
        );

        let pixels = screenshot.pixels();
        assert!(
            pixels.len() >= 4,
            "{label} screenshot should include at least one RGBA pixel"
        );
        let first = &pixels[0..4];
        let mut visible = 0usize;
        let mut varied = 0usize;
        for px in pixels.chunks_exact(4) {
            if px[3] > 0 {
                visible += 1;
            }
            if px != first {
                varied += 1;
            }
        }
        let pixel_count = pixels.len() / 4;
        assert!(
            visible > pixel_count / 2,
            "{label} screenshot should not be mostly transparent"
        );
        assert!(
            varied > 16,
            "{label} screenshot should not be a single flat color"
        );

        Rect::new(0.0, 0.0, scene.viewport.width, scene.viewport.height)
    }

    fn named_node_bounds(
        snapshot: &sui_testing::WindowSnapshot,
        role: SemanticsRole,
        name: &str,
    ) -> Rect {
        snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| node.role == role && node.name.as_deref() == Some(name))
            .unwrap_or_else(|| panic!("{role:?} named {name:?} should be present"))
            .bounds
    }

    fn assert_node_inside_viewport(
        label: &str,
        artifacts: &sui_testing::ArtifactBundle,
        viewport: Rect,
        role: SemanticsRole,
        name: &str,
    ) -> Rect {
        let bounds = named_node_bounds(&artifacts.snapshot, role, name);
        assert!(
            bounds.width() > 0.0 && bounds.height() > 0.0,
            "{label} node {name:?} should have a non-empty hit area, got {:?}",
            bounds
        );
        assert!(
            bounds.x() >= viewport.x() - 0.5
                && bounds.y() >= viewport.y() - 0.5
                && bounds.max_x() <= viewport.max_x() + 0.5
                && bounds.max_y() <= viewport.max_y() + 0.5,
            "{label} node {name:?} should fit inside {:?}, got {:?}",
            viewport,
            bounds
        );
        bounds
    }

    fn assert_non_overlapping(label: &str, left: Rect, right: Rect) {
        let overlap = left
            .intersection(right)
            .map(|rect| rect.width() * rect.height())
            .unwrap_or(0.0);
        assert!(
            overlap <= 0.5,
            "{label} controls should not overlap, got {left:?} and {right:?}"
        );
    }

    #[test]
    fn debug_artifacts_capture_chat_and_files_visual_surfaces() -> sui::Result<()> {
        let chat_shared = Shared::new(AppState::ready(Backend::local_fallback()));
        {
            let mut chat = chat_shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.messages = vec![
                ChatMessage::user("Review the SIFS placement report before rollout."),
                ChatMessage::model(
                    "The compact layout keeps the transcript readable while preserving action \
                     density for long-running sessions.",
                ),
            ];
            chat.set_draft("Summarize the current file browser status.".to_string());
            for (name, size) in [
                ("report-a.txt", 1_024),
                ("report-b.txt", 2_048),
                ("report-c.txt", 3_072),
                ("report-d.txt", 4_096),
                ("report-e.txt", 5_120),
            ] {
                chat.attachments.push(pending_attachment(name, size));
            }
            chat.set_composer_popup(Some(ComposerPopup::Attachments));
        }
        let chat_window = test_window(
            "Sinomo chat visual debug",
            view(chat_shared.clone()),
            Size::new(1280.0, 760.0),
        )?;
        let chat_artifacts = chat_window.capture_artifacts()?;
        write_visual_debug_artifacts("chat", &chat_artifacts)?;
        let chat_viewport = assert_debug_artifacts_have_visual_content("chat", &chat_artifacts);
        let attach = assert_node_inside_viewport(
            "chat",
            &chat_artifacts,
            chat_viewport,
            SemanticsRole::Button,
            "Attach files",
        );
        let send = assert_node_inside_viewport(
            "chat",
            &chat_artifacts,
            chat_viewport,
            SemanticsRole::Button,
            "Send message",
        );
        assert_non_overlapping("chat composer bar action", attach, send);
        assert_node_inside_viewport(
            "chat",
            &chat_artifacts,
            chat_viewport,
            SemanticsRole::Button,
            "Remove report-e.txt",
        );

        let files_shared = Shared::new(AppState::ready(Backend::local_fallback()));
        {
            let mut files = files_shared.files_mut();
            files.source = crate::ui::files::FilesSourceKind::Sifs;
            files.loaded = true;
            files.loading = false;
            files.error = None;
            files.clustered = true;
            files.node_id = "node-main".to_string();
            files.path = "/services/sinomo/agents".to_string();
            files.entries = vec![
                sinomo_cli::app_api::SifsEntry {
                    name: "agent-config.sobj".to_string(),
                    path: "/services/sinomo/agents/agent-config.sobj".to_string(),
                    is_dir: false,
                    is_link: false,
                    size: Some(8_192),
                },
                sinomo_cli::app_api::SifsEntry {
                    name: "memory".to_string(),
                    path: "/services/sinomo/agents/memory".to_string(),
                    is_dir: true,
                    is_link: false,
                    size: None,
                },
            ];
            files.entries_total_count = files.entries.len();
            files.selected = Some(0);
            files.selected_details = Some(sinomo_cli::app_api::SifsObjectDetails {
                file_system_id: "sifs".to_string(),
                object_id: "obj-agent-config".to_string(),
                path: "/services/sinomo/agents/agent-config.sobj".to_string(),
                kind: "file".to_string(),
                version: "version-42".to_string(),
                content_hash: Some("sha256:8df4e83a7f2d0c1b".to_string()),
                size_bytes: Some(8_192),
                link_count: 1,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                accessed_at_unix_nanos: 1_766_000_000_000_000_000,
                modified_at_unix_nanos: 1_766_000_030_000_000_000,
                created_at_unix_nanos: 1_766_000_000_000_000_000,
                changed_at_unix_nanos: 1_766_000_030_000_000_000,
                policy: "replicated-eventual".to_string(),
                current_locations: vec!["node-main".to_string(), "node-home".to_string()],
                local_pins: vec![sinomo_cli::app_api::SifsPinDetails {
                    pinned_by_node_id: "node-main".to_string(),
                    mode: "content".to_string(),
                    created_at_unix_nanos: Some(1_766_000_040_000_000_000),
                }],
                metadata_shard: None,
                health: Some(sinomo_cli::app_api::SifsObjectHealth {
                    agent_state: "healthy".to_string(),
                    version_count: 1,
                    receipt_count: 2,
                    intent_count: 0,
                    open_conflict_count: 0,
                    pending_repair_count: 0,
                    missing_replica_nodes: Vec::new(),
                }),
            });
        }
        let files_window = test_window(
            "Sinomo files visual debug",
            crate::ui::files::view(files_shared.clone()),
            Size::new(1280.0, 760.0),
        )?;
        let files_artifacts = files_window.capture_artifacts()?;
        write_visual_debug_artifacts("files", &files_artifacts)?;
        let files_viewport = assert_debug_artifacts_have_visual_content("files", &files_artifacts);
        let back = assert_node_inside_viewport(
            "files",
            &files_artifacts,
            files_viewport,
            SemanticsRole::Button,
            "Back",
        );
        let forward = assert_node_inside_viewport(
            "files",
            &files_artifacts,
            files_viewport,
            SemanticsRole::Button,
            "Forward",
        );
        assert_non_overlapping("files navigation", back, forward);
        assert_node_inside_viewport(
            "files",
            &files_artifacts,
            files_viewport,
            SemanticsRole::Button,
            "Refresh SIFS",
        );
        assert_node_inside_viewport(
            "files",
            &files_artifacts,
            files_viewport,
            SemanticsRole::ListItem,
            "agent-config.sobj",
        );

        Ok(())
    }

    #[test]
    fn rendered_attachment_controls_are_collapsed_until_opened() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let narrow_error = "C:\\very\\deep\\workspace\\that\\does\\not\\fit\\on\\one\\line\\missing-file.rs: attachment unavailable; choose another file";
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.models_loading = false;
            chat.attachment_picker_error = Some(narrow_error.to_string());
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat attachment controls")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        window
            .root()
            .dispatch_event(Event::Window(sui::WindowEvent::Resized(Size::new(
                420.0, 720.0,
            ))))?;
        window.run_until_idle()?;

        assert!(!shared.chat().attachment_controls_visible());
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Choose files")
                .count()?,
            0
        );

        // The bar opens the attachment popup with the native picker as the primary path. Manual
        // entry stays explicit and collapsed until requested.
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Attach files")
            .click()?;
        assert!(shared.chat().attachment_controls_visible());
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Choose files")
                .count()?,
            1
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Enter path manually")
                .count()?,
            1
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Add path")
                .count()?,
            0,
            "manual path entry must not be the primary interaction"
        );
        let snapshot = window.snapshot()?;
        let viewport = Rect::new(0.0, 0.0, 420.0, 720.0);
        for (name, bounds) in [
            (
                "Couldn’t attach",
                named_node_bounds(&snapshot, SemanticsRole::Text, "Couldn’t attach"),
            ),
            (
                narrow_error,
                named_node_bounds(&snapshot, SemanticsRole::Text, narrow_error),
            ),
        ] {
            assert!(
                bounds.width() > 0.0
                    && bounds.max_x() <= viewport.max_x() + 0.5
                    && bounds.max_y() <= viewport.max_y() + 0.5,
                "narrow attachment error {name:?} should remain readable inside {viewport:?}, got {bounds:?}"
            );
        }

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Enter path manually")
            .click()?;
        assert!(shared.chat().attachment_manual_path_open);
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Add path")
                .count()?,
            1
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Close attachments")
            .click()?;
        assert!(!shared.chat().attachment_controls_visible());

        Ok(())
    }

    #[test]
    fn rendered_attachment_drawer_exposes_wrapped_chips() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.models_loading = false;
            for (name, size) in [
                ("report-a.txt", 1_024),
                ("report-b.txt", 2_048),
                ("report-c.txt", 3_072),
                ("report-d.txt", 4_096),
                ("report-e.txt", 5_120),
            ] {
                chat.attachments.push(pending_attachment(name, size));
            }
            chat.set_composer_popup(Some(ComposerPopup::Attachments));
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat attachment wrapped chips")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        // Each attachment is removable from two surfaces at once: the popup
        // drawer chip and the composer field's context row chip.
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Remove report-a.txt")
                .count()?,
            2
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Remove report-e.txt")
                .count()?,
            2,
            "the open attachment drawer should expose later chips through wrapped layout"
        );

        Ok(())
    }

    #[test]
    fn rendered_attachment_chips_stay_visible_when_composer_is_narrow() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut chat = shared.chat_mut();
            seed_default_provider(&mut chat);
            chat.models_loading = false;
            chat.attachments
                .push(pending_attachment("report-a.txt", 1_024));
            chat.attachments
                .push(pending_attachment("report-b.txt", 2_048));
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Chat attachment responsive controls")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Remove report-a.txt")
                .count()?,
            1,
            "desktop-width composer should show removable attachment chips"
        );

        window
            .root()
            .dispatch_event(Event::Window(sui::WindowEvent::Resized(Size::new(
                560.0, 720.0,
            ))))?;
        window.run_until_idle()?;

        // Mesh: context chips ride on top of the field and wrap — a narrow
        // composer keeps every attachment visible and removable instead of
        // collapsing them behind a summary dropdown.
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Remove report-a.txt")
                .count()?,
            1,
            "narrow composer keeps attachment chips visible"
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Remove report-b.txt")
                .count()?,
            1,
            "narrow composer keeps later chips reachable through wrapping"
        );

        Ok(())
    }

    #[test]
    fn set_provider_resets_model_index() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        if state.providers.len() < 2 {
            return; // environment without multiple configured providers
        }
        state.model_idx = 3;
        state.set_provider(1);
        assert_eq!(state.provider_idx, 1);
        assert_eq!(state.model_idx, 0, "switching providers resets the model");
    }

    /// Issue 2: the default/initial provider's full model list must be fetched eagerly at
    /// construction. Configured accounts seed a usable default model immediately, then
    /// `ChatState::new` kicks the async full-list fetch.
    #[test]
    fn provider_loading_does_not_show_provider_setup_recovery() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.providers.clear();
        state.providers_loading = true;

        assert_eq!(state.runtime_recovery_target(), None);
        assert!(!state.shows_recovery_empty_card());
    }

    #[test]
    fn provider_list_load_applies_async_result() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.providers.clear();
        state.providers_loading = true;

        assert!(state.start_provider_list_load(backend.config.clone(), None));
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            if state.pump_provider_list() {
                assert!(!state.providers_loading);
                assert!(state.provider_list_rx.is_none());
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        panic!("provider list worker did not finish");
    }

    #[test]
    fn new_fetches_default_provider_models() {
        let backend = Backend::local_fallback();
        let state = ChatState::new(&backend);
        if state.providers.is_empty() {
            // No configured provider → nothing to fetch (correctly a no-op).
            assert!(!state.models_loading);
            return;
        }
        assert!(
            state
                .current_provider()
                .is_some_and(|p| !p.models.is_empty()),
            "providers are seeded with a usable default model"
        );
        assert!(
            state.models_loading,
            "the default provider's model fetch is kicked at construction"
        );
    }

    /// The runtime resolving a provider (Ready / RuntimeInfo) must also trigger a model fetch for
    /// the newly-resolved provider when its list is still empty, so the picker populates even if the
    /// worker reports a provider other than index 0.
    #[test]
    fn runtime_info_triggers_model_fetch_for_resolved_provider() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        let Some(provider_id) = state.providers.first().map(|p| p.id.clone()) else {
            return; // no configured provider in this environment
        };
        // Pretend the initial fetch already completed (so we can observe a fresh one being kicked).
        state.models_loading = false;
        state.apply_event(ChatEvent::RuntimeInfo(RuntimeInfo {
            agent: "main".to_string(),
            provider: provider_id,
            model: String::new(),
            account: None,
            effort: None,
        }));
        assert!(
            state.models_loading,
            "resolving the provider via RuntimeInfo re-kicks the model fetch when the list is empty"
        );
    }

    #[test]
    fn empty_model_fetch_preserves_seeded_provider_model() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.providers = vec![ProviderSummary {
            id: "deepseek".to_string(),
            account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
            name: "DeepSeek".to_string(),
            models: vec!["deepseek-v4-flash".to_string()],
        }];
        state.provider_idx = 0;
        state.model_idx = 0;
        state.models_loading = true;

        state.apply_event(ChatEvent::Models {
            provider: "deepseek".to_string(),
            models: Vec::new(),
        });

        assert_eq!(
            state.current_models(),
            vec!["deepseek-v4-flash".to_string()],
            "failed model discovery must not wipe the configured/default model"
        );
        assert_eq!(state.current_model(), "deepseek-v4-flash");
        assert!(!state.models_loading);
    }

    #[test]
    fn hidden_runtime_provider_does_not_leak_into_visible_model_selection() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.providers = vec![ProviderSummary {
            id: "deepseek".to_string(),
            account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
            name: "DeepSeek".to_string(),
            models: vec!["deepseek-v4-flash".to_string()],
        }];
        state.provider_idx = 0;
        state.model_idx = 99;
        state.models_loading = false;

        state.apply_event(ChatEvent::RuntimeInfo(RuntimeInfo {
            agent: "main".to_string(),
            provider: "github-copilot".to_string(),
            model: "gpt-5.4".to_string(),
            account: Some(sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string()),
            effort: None,
        }));

        assert_eq!(
            state.current_model(),
            "",
            "an unlisted runtime provider must not supply the selected provider's model"
        );
        state.model_idx = 0;
        assert_eq!(
            state.current_model(),
            "deepseek-v4-flash",
            "the visible provider keeps its own configured model"
        );
    }

    #[test]
    fn visible_model_selection_command_ignores_hidden_runtime_provider() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.providers = vec![ProviderSummary {
            id: "deepseek".to_string(),
            account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
            name: "DeepSeek".to_string(),
            models: vec!["deepseek-v4-flash".to_string()],
        }];
        state.provider_idx = 0;
        state.model_idx = 0;
        state.models_loading = false;
        state.info = RuntimeInfo {
            agent: "main".to_string(),
            provider: "github-copilot".to_string(),
            model: "gpt-5.4".to_string(),
            account: Some(sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string()),
            effort: None,
        };

        let Some(ChatCommand::SetModel {
            provider,
            model,
            account,
        }) = state.current_model_selection_command()
        else {
            panic!("visible provider/model should produce a model switch command");
        };

        assert_eq!(provider, "deepseek");
        assert_eq!(model, "deepseek-v4-flash");
        assert_eq!(account, sinomo_lm::DEFAULT_PROVIDER_ACCOUNT);
        assert!(
            !state.runtime_matches_selection(&provider, &model, &account),
            "submit must switch the worker before sending when the runtime is stale"
        );
    }

    #[test]
    fn submit_blocks_empty_visible_model_instead_of_using_hidden_runtime() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        state.providers = vec![ProviderSummary {
            id: "deepseek".to_string(),
            account: sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string(),
            name: "DeepSeek".to_string(),
            models: Vec::new(),
        }];
        state.provider_idx = 0;
        state.model_idx = 0;
        state.models_loading = false;
        state.info = RuntimeInfo {
            agent: "main".to_string(),
            provider: "github-copilot".to_string(),
            model: "gpt-5.4".to_string(),
            account: Some(sinomo_lm::DEFAULT_PROVIDER_ACCOUNT.to_string()),
            effort: None,
        };

        state.set_draft("do not send through hidden runtime".to_string());
        state.submit();

        assert!(
            !state.streaming,
            "send should be blocked without a visible model"
        );
        assert!(
            state.messages.is_empty(),
            "blocked sends should not add bubbles"
        );
        assert_eq!(
            state.notice.as_deref(),
            Some("choose a model before sending")
        );
    }

    /// Issue 1: Enter-to-send. The composer wires `TextArea::on_submit` to `ChatState::submit`, so a
    /// plain Enter starts a turn. `submit()` is the exact entry point that wiring calls; assert it
    /// begins a streaming turn from a non-empty draft (and is a no-op otherwise — covered above).
    #[test]
    fn submit_is_the_enter_to_send_entry_point() {
        let backend = Backend::local_fallback();
        let mut state = ChatState::new(&backend);
        seed_default_provider(&mut state);
        state.set_draft("ping".to_string());
        assert!(state.can_send(), "a non-empty draft is sendable on Enter");
        // What `on_submit(|_| s.chat_mut().submit())` runs:
        state.submit();
        assert!(state.streaming, "Enter-to-send starts a streaming turn");
        assert_eq!(
            state.messages.first().map(|m| m.text.as_str()),
            Some("ping")
        );
        assert!(state.input.is_empty(), "the draft is cleared after sending");
    }

    #[test]
    #[ignore]
    fn live_app_chat_streams_and_persists_backend_session() {
        if std::env::var_os("SINOMO_LIVE_APP_CHAT_SMOKE").is_none() {
            return;
        }
        let backend = Backend::connect(|_| {});
        let mut state = ChatState::new(&backend);
        wait_for(&mut state, Duration::from_secs(20), |state| {
            !state.session_id.is_empty()
        });

        let provider =
            std::env::var("SINOMO_LIVE_APP_CHAT_PROVIDER").unwrap_or_else(|_| "codex".to_string());
        let model =
            std::env::var("SINOMO_LIVE_APP_CHAT_MODEL").unwrap_or_else(|_| "gpt-5.5".to_string());

        state.handle.send_command(ChatCommand::SetModel {
            provider: provider.clone(),
            model: model.clone(),
            account: "default".to_string(),
        });
        wait_for(&mut state, Duration::from_secs(20), |state| {
            state.info.provider == provider && state.info.model == model
        });

        let prompt = "Reply only with this exact text: sinomo-app-live-ok";
        state.set_draft(prompt.to_string());
        state.submit();
        wait_for(&mut state, Duration::from_secs(90), |state| {
            !state.streaming
                && state.messages.iter().any(|message| {
                    message.role == ChatRole::Agent && message.text.contains("sinomo-app-live-ok")
                })
        });

        let session_id = state.session_id.clone();
        assert!(!session_id.is_empty(), "session id is assigned");

        let reloaded = load_conversation_messages(
            &state.data_dir,
            state.sifs_daemon.as_ref(),
            &session_id,
            ConversationKind::Chat,
        )
        .expect("reload backend session");
        assert!(
            reloaded
                .iter()
                .any(|message| message.role == ChatRole::User && message.text == prompt),
            "user prompt persisted"
        );
        assert!(
            reloaded
                .iter()
                .any(|message| message.role == ChatRole::Agent
                    && message.text.contains("sinomo-app-live-ok")),
            "agent response persisted"
        );
    }

    fn wait_for(state: &mut ChatState, timeout: Duration, predicate: impl Fn(&ChatState) -> bool) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            let events = state.handle.drain();
            for event in events {
                state.apply_event(event);
            }
            state.pump_session_list();
            state.pump_transcript_load();
            state.pump_channel_create();
            state.pump_session_rename();
            state.pump_channel_config();
            state.pump_delete();
            state.pump_attachment_lanes();
            if predicate(state) {
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        let snapshot = state
            .messages
            .iter()
            .map(|message| message.text.as_str())
            .collect::<Vec<_>>()
            .join("\n---\n");
        panic!(
            "condition was not met within {:?}; streaming={}, notice={:?}, messages={}",
            timeout, state.streaming, state.notice, snapshot
        );
    }
}
