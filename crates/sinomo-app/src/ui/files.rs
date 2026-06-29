//! "Files" tab — source-aware file manager for SIFS and local files.
//!
//! Standard file-manager UX (breadcrumb + listing) plus a SIFS inspector that
//! shows where an object lives and its placement / replication policy.
//!
//! Like the Settings tab, this is a fully *composed* `sui-widgets` tree: [`view`]
//! returns a responsive host ([`FilesView`]) over pre-built pods (wide split with
//! the inspector, narrow list-only), each assembled from stock `Breadcrumb` /
//! `IconButton` / `ListView` / `ScrollView` / `Separator` / `PanelSection` /
//! `PropertyRow` / `Label` widgets. State lives entirely in [`FilesState`]; the
//! interactive app lists SIFS on a background worker so tab layout never blocks
//! on DFS metadata or network work.
//!
//! The one new wrinkle vs. Settings: the file *listing* changes on navigation, so
//! the list/breadcrumb items aren't fixed at build time. `ListView::items()` takes
//! a fixed `Vec<ListItem>` (there is no reactive `items_when` builder), so we wrap
//! the listing in a small [`RebuildOnChange`] widget keyed on the current path: it
//! diffs the path on each frame and rebuilds its child pod from the new listing
//! when it changes. The selection (which only repaints the highlight and updates
//! the inspector) stays reactive via `ListView::selected_when` + the live-reading
//! [`InspectorView`] — so selecting a file does *not* rebuild the list (the scroll
//! position is preserved), only navigation does. Directory rows also expose an inline inspect
//! action so object placement remains reachable without changing paths.

use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use sui::{
    Alignment, ArrangeCtx, Background, Breadcrumb, BreadcrumbItem, Color, Constraints,
    DefaultTheme, Event, EventCtx, Flex, FlexItem, FontWeight, IconButton, IconGlyph, KeyState,
    ListItem, ListView, MeasureCtx, PaintCtx, Point, PointerButton, PointerEventKind, Rect,
    ScrollState, ScrollView, SemanticsAction, SemanticsCtx, SemanticsNode, SemanticsRole,
    SemanticsValue, Separator, Size, SizedBox, Stack, StackHostOptions, Widget, WidgetPod,
    WidgetPodMutVisitor, WidgetPodVisitor, WindowEvent, draw_glyph,
};

use sinomo_cli::app_api::{
    SifsBrowser, SifsEntry, SifsFileRead, SifsObjectDetails, SifsObjectHealth,
};
use sinomo_runtime::local::SinomoDataDir;
use time::format_description::well_known::Rfc3339;

use crate::backend::Backend;
use crate::state::{Shared, Tab};
use crate::theme::Palette;
use crate::ui::settings::SettingsSection;
use crate::ui::widgets;

/// Below this content width the tab collapses to a list-only column (the inspector is hidden).
const WIDE_THRESHOLD: f32 = 640.0;
const INSPECTOR_W: f32 = 340.0;
const ROW_H: f32 = 34.0;
const ROW_ACTION_SIDE: f32 = 24.0;
const ROW_ACTION_GAP: f32 = 4.0;
const ROW_ACTION_COUNT: f32 = 7.0;
const CONTENT_PAD: f32 = 16.0;
const HEADER_H: f32 = 78.0;
const HEADER_COMMAND_GAP: f32 = 8.0;
const HEADER_ROW_GAP: f32 = 6.0;
const FILES_STATUS_H: f32 = 38.0;
const LIST_HEADER_H: f32 = 36.0;
const LIST_FILTER_H: f32 = 72.0;
const LIST_TASK_STRIP_H: f32 = 88.0;
const PLACES_W: f32 = 190.0;
const PLACE_ROW_H: f32 = 42.0;
const FILE_PREVIEW_MAX_BYTES: u64 = 32 * 1024;
const FILE_PREVIEW_MAX_LINES: usize = 8;
const LOAD_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_NAV_HISTORY: usize = 64;

/// A shared closure that resolves the current widget theme — threaded into every control so the
/// Appearance toggle restyles the whole tab live, with no rebuild. (Same pattern as `settings.rs`.)
type ThemeReader = Rc<dyn Fn() -> DefaultTheme>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilesSourceKind {
    Sifs,
    Local,
}

impl FilesSourceKind {
    fn label(self) -> &'static str {
        match self {
            Self::Sifs => "SIFS",
            Self::Local => "Local files",
        }
    }

    fn path_label(self) -> &'static str {
        match self {
            Self::Sifs => "SIFS path",
            Self::Local => "Local path",
        }
    }

    fn operation_scope(self) -> &'static str {
        match self {
            Self::Sifs => "SIFS",
            Self::Local => "local",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FileActionPromptKind {
    GoTo,
    NewFile,
    NewFolder,
    Copy,
    CopySelected,
    CopyToSifs,
    CopyToLocal,
    TransferSelected,
    Move,
    MoveSelected,
    Rename,
    Delete,
    DeleteSelected,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum FileSortMode {
    #[default]
    NameAsc,
    NameDesc,
    Kind,
    SizeDesc,
    SizeAsc,
}

impl FileSortMode {
    const ALL: [Self; 5] = [
        Self::NameAsc,
        Self::NameDesc,
        Self::Kind,
        Self::SizeDesc,
        Self::SizeAsc,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::NameAsc => "Name A-Z",
            Self::NameDesc => "Name Z-A",
            Self::Kind => "Kind",
            Self::SizeDesc => "Size largest",
            Self::SizeAsc => "Size smallest",
        }
    }

    fn options() -> Vec<String> {
        Self::ALL
            .iter()
            .map(|mode| mode.label().to_string())
            .collect()
    }
}

impl FileActionPromptKind {
    fn title(self) -> &'static str {
        match self {
            Self::GoTo => "Go to path",
            Self::NewFile => "New file",
            Self::NewFolder => "New folder",
            Self::Copy => "Copy to",
            Self::CopySelected => "Copy selected",
            Self::CopyToSifs => "Copy to SIFS",
            Self::CopyToLocal => "Download",
            Self::TransferSelected => "Transfer selected",
            Self::Move => "Move to",
            Self::MoveSelected => "Move selected",
            Self::Rename => "Rename",
            Self::Delete => "Delete",
            Self::DeleteSelected => "Delete selected",
        }
    }

    fn primary_label(self) -> &'static str {
        match self {
            Self::GoTo => "Open path",
            Self::NewFile => "Create file",
            Self::NewFolder => "Create folder",
            Self::Copy => "Copy",
            Self::CopySelected => "Copy selected",
            Self::CopyToSifs => "Copy to SIFS",
            Self::CopyToLocal => "Download",
            Self::TransferSelected => "Transfer selected",
            Self::Move => "Move",
            Self::MoveSelected => "Move selected",
            Self::Rename => "Rename",
            Self::Delete => "Delete",
            Self::DeleteSelected => "Delete selected",
        }
    }
}

fn file_prompt_uses_marked_targets(kind: FileActionPromptKind) -> bool {
    matches!(
        kind,
        FileActionPromptKind::CopySelected
            | FileActionPromptKind::TransferSelected
            | FileActionPromptKind::MoveSelected
            | FileActionPromptKind::DeleteSelected
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FileActionTarget {
    path: String,
    is_dir: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FileActionPrompt {
    kind: FileActionPromptKind,
    input: String,
    target_name: String,
    target_path: Option<String>,
    target_is_dir: bool,
    targets: Vec<FileActionTarget>,
    error: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FilesTaskState {
    Running,
    CancelRequested,
    Cancelled,
    Completed,
    Failed,
}

impl FilesTaskState {
    fn label(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::CancelRequested => "cancelling",
            Self::Cancelled => "cancelled",
            Self::Completed => "done",
            Self::Failed => "failed",
        }
    }

    fn can_cancel(self) -> bool {
        self == Self::Running
    }
}

#[derive(Clone, Debug, PartialEq)]
struct FilesTask {
    id: u64,
    source: FilesSourceKind,
    title: String,
    detail: String,
    state: FilesTaskState,
    progress: Option<f32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FilesOperationKind {
    CreateFile,
    CreateFolder,
    Copy,
    CopyToSifs,
    CopyToLocal,
    Move,
    Rename,
    Delete,
    RefreshPlacement,
    RepairReplicas,
    PinContent,
    UnpinContent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FilesOperation {
    kind: FilesOperationKind,
    path: String,
    target: Option<String>,
    directory: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FilesQueuedOperation {
    operation: FilesOperation,
    title: String,
    detail: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FilesLocation {
    source: FilesSourceKind,
    path: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FilesOperationOutcome {
    refresh_path: String,
    select_path: Option<String>,
    message: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LocalObjectDetails {
    path: String,
    canonical_path: Option<String>,
    kind: String,
    size_bytes: Option<u64>,
    readonly: bool,
    accessed_at_unix_nanos: Option<u64>,
    modified_at_unix_nanos: Option<u64>,
    created_at_unix_nanos: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum FilesObjectDetails {
    Sifs(SifsObjectDetails),
    Local(LocalObjectDetails),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FilePlace {
    label: String,
    detail: String,
    source: FilesSourceKind,
    path: String,
    glyph: IconGlyph,
    action: FilePlaceAction,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FilePlaceAction {
    OpenPath,
    OpenPathPrompt,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FilesPreview {
    title: String,
    lines: Vec<String>,
    truncated: bool,
    binary: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum FilesOperationFailure {
    Cancelled(String),
    Error(String),
}

type FilesOperationResult = Result<FilesOperationOutcome, FilesOperationFailure>;

// ---------------------------------------------------------------------------
// State (data layer) — SIFS-backed browser snapshot + async load worker.
// ---------------------------------------------------------------------------

/// State backing the files tab.
pub struct FilesState {
    pub data_dir: Arc<SinomoDataDir>,
    daemon_address: Option<String>,
    app_runtime: tokio::runtime::Handle,
    pub source: FilesSourceKind,
    pub browser: Option<SifsBrowser>,
    pub error: Option<String>,
    pub clustered: bool,
    pub loaded: bool,
    pub loading: bool,
    pub list_revision: u64,
    pub node_id: String,
    pub path: String,
    sifs_path: String,
    local_path: String,
    history_back: Vec<FilesLocation>,
    history_forward: Vec<FilesLocation>,
    pub entries: Vec<SifsEntry>,
    pub selected: Option<usize>,
    pub selected_details: Option<SifsObjectDetails>,
    selected_local_details: Option<LocalObjectDetails>,
    pub details_loading: bool,
    pub details_error: Option<String>,
    selected_preview: Option<FilesPreview>,
    preview_loading: bool,
    preview_error: Option<String>,
    action_prompt: Option<FileActionPrompt>,
    marked_entries: BTreeSet<usize>,
    filter_query: String,
    sort_mode: FileSortMode,
    tasks: Vec<FilesTask>,
    pending_select_path: Option<String>,
    pub scroll: f32,
    pub inspector_details_open: bool,
    worker_tx: Option<Sender<FilesWorkerCommand>>,
    worker_rx: Option<Receiver<FilesWorkerResult>>,
    worker_result_tx: Option<Sender<FilesWorkerResult>>,
    worker_waker: Option<sui::UiHandle>,
    worker_cancellations: Arc<Mutex<BTreeSet<u64>>>,
    next_request_id: u64,
    pending_request_id: Option<u64>,
    pending_details_request_id: Option<u64>,
    pending_preview_request_id: Option<u64>,
}

impl FilesState {
    pub fn new(backend: &Backend) -> Self {
        let local_path = default_local_path(backend.data_dir.root());
        Self {
            data_dir: backend.data_dir.clone(),
            daemon_address: backend.connection.daemon_address().map(str::to_string),
            app_runtime: backend.app_runtime.handle().clone(),
            source: FilesSourceKind::Sifs,
            browser: None,
            error: None,
            clustered: backend.connection.is_clustered(),
            loaded: false,
            loading: false,
            list_revision: 0,
            node_id: String::new(),
            path: "/".to_string(),
            sifs_path: "/".to_string(),
            local_path,
            history_back: Vec::new(),
            history_forward: Vec::new(),
            entries: Vec::new(),
            selected: None,
            selected_details: None,
            selected_local_details: None,
            details_loading: false,
            details_error: None,
            selected_preview: None,
            preview_loading: false,
            preview_error: None,
            action_prompt: None,
            marked_entries: BTreeSet::new(),
            filter_query: String::new(),
            sort_mode: FileSortMode::default(),
            tasks: Vec::new(),
            pending_select_path: None,
            scroll: 0.0,
            inspector_details_open: false,
            worker_tx: None,
            worker_rx: None,
            worker_result_tx: None,
            worker_waker: None,
            worker_cancellations: Arc::new(Mutex::new(BTreeSet::new())),
            next_request_id: 0,
            pending_request_id: None,
            pending_details_request_id: None,
            pending_preview_request_id: None,
        }
    }

    pub(crate) fn connection_attention_detail(&self) -> Option<String> {
        if let Some(error) = self.error.as_deref().filter(|text| !text.is_empty()) {
            return Some(format!("SIFS unavailable: {error}"));
        }
        self.tasks
            .iter()
            .find(|task| {
                task.source == FilesSourceKind::Sifs && task.state == FilesTaskState::Failed
            })
            .map(|task| format!("{} failed: {}", task.title, task.detail))
    }

    fn ensure_loaded(&mut self, waker: Option<sui::UiHandle>) {
        if self.loaded {
            self.pump_load();
            return;
        }
        if let Some(waker) = waker {
            self.ensure_worker(waker);
            self.loaded = true;
            self.reload();
            return;
        }
        self.load_sync();
    }

    fn load_sync(&mut self) {
        self.loaded = true;
        self.loading = false;
        if self.source == FilesSourceKind::Local {
            self.browser = None;
            self.node_id = local_node_label();
            self.error = None;
            self.reload();
            return;
        }
        let result = self
            .daemon_address
            .as_deref()
            .ok_or_else(|| "SIFS browsing requires a daemon connection".to_string())
            .and_then(|address| SifsBrowser::open_daemon_on_runtime(address, &self.app_runtime));
        match result {
            Ok(browser) => {
                self.node_id = browser.node_id().to_string();
                self.browser = Some(browser);
                self.error = None;
            }
            Err(error) => {
                self.browser = None;
                self.node_id.clear();
                self.error = Some(error);
            }
        }
        self.reload();
    }

    fn reload(&mut self) {
        if self.worker_tx.is_some() {
            self.reload_async();
            return;
        }
        self.reload_sync();
    }

    fn reload_sync(&mut self) {
        self.selected = None;
        self.marked_entries.clear();
        self.clear_selected_details();
        self.loading = false;
        let path = self.path.clone();
        let listing = match self.source {
            FilesSourceKind::Sifs => self.browser.as_ref().map(|b| b.list_shallow(&path)),
            FilesSourceKind::Local => Some(list_local_dir(&path)),
        };
        match listing {
            Some(Ok(entries)) => {
                self.entries = entries;
                self.error = None;
                if self.source == FilesSourceKind::Local {
                    self.node_id = local_node_label();
                }
                if let Some(select_path) = self.pending_select_path.take()
                    && let Some(index) = self
                        .entries
                        .iter()
                        .position(|entry| entry.path == select_path)
                {
                    self.selected = Some(index);
                    self.request_selected_details();
                }
            }
            Some(Err(error)) => {
                self.entries.clear();
                self.error = Some(error);
            }
            None => {}
        }
        self.list_revision = self.list_revision.wrapping_add(1);
    }

    fn reload_async(&mut self) {
        let Some(tx) = self.worker_tx.clone() else {
            return;
        };
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        self.pending_request_id = Some(request_id);
        self.selected = None;
        self.marked_entries.clear();
        self.clear_selected_details();
        self.loading = true;
        self.error = None;
        self.entries.clear();
        self.list_revision = self.list_revision.wrapping_add(1);
        let command = FilesWorkerCommand::List {
            request_id,
            source: self.source,
            path: self.path.clone(),
        };
        if tx.send(command).is_err() {
            self.pending_request_id = None;
            self.loading = false;
            self.error = Some("SIFS browser worker stopped".to_string());
            self.list_revision = self.list_revision.wrapping_add(1);
            return;
        }
        if let Some(result_tx) = self.worker_result_tx.clone() {
            let path = self.path.clone();
            let source = self.source;
            let waker = self.worker_waker.clone();
            let _ = thread::Builder::new()
                .name("sinomo-app-files-timeout".to_string())
                .spawn(move || {
                    thread::sleep(LOAD_TIMEOUT);
                    let _ = result_tx.send(FilesWorkerResult::List {
                        request_id,
                        source,
                        path,
                        result: Err(format!(
                            "{} listing did not finish within {} seconds",
                            source.label(),
                            LOAD_TIMEOUT.as_secs()
                        )),
                    });
                    if let Some(waker) = waker {
                        waker.wake();
                    }
                });
        }
    }

    fn retry(&mut self, waker: Option<sui::UiHandle>) {
        self.reset_for_retry();
        self.ensure_loaded(waker);
    }

    fn refresh(&mut self, waker: Option<sui::UiHandle>) {
        if !self.loaded {
            self.ensure_loaded(waker);
            return;
        }
        if self.loading {
            return;
        }
        if let Some(waker) = waker {
            self.ensure_worker(waker);
        }
        self.reload();
    }

    fn reset_for_retry(&mut self) {
        self.browser = None;
        self.error = None;
        self.entries.clear();
        self.selected = None;
        self.marked_entries.clear();
        self.clear_selected_details();
        self.node_id.clear();
        self.inspector_details_open = false;
        self.loaded = self.source == FilesSourceKind::Local;
        self.loading = false;
        self.pending_request_id = None;
        self.pending_details_request_id = None;
        self.pending_preview_request_id = None;
        self.worker_tx = None;
        self.worker_rx = None;
        self.worker_result_tx = None;
        self.worker_waker = None;
        clear_all_cancelled_operations(&self.worker_cancellations);
        self.list_revision = self.list_revision.wrapping_add(1);
    }

    fn ensure_worker(&mut self, waker: sui::UiHandle) {
        if self.worker_tx.is_some() {
            return;
        }
        let (command_tx, command_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        let data_dir = self.data_dir.clone();
        let daemon_address = self.daemon_address.clone();
        let app_runtime = self.app_runtime.clone();
        let cancellations = self.worker_cancellations.clone();
        let timeout_tx = result_tx.clone();
        let timeout_waker = waker.clone();
        match thread::Builder::new()
            .name("sinomo-app-files".to_string())
            .spawn(move || {
                files_worker_loop(
                    data_dir,
                    daemon_address,
                    app_runtime,
                    command_rx,
                    result_tx,
                    cancellations,
                    waker,
                )
            }) {
            Ok(_) => {
                self.worker_tx = Some(command_tx);
                self.worker_rx = Some(result_rx);
                self.worker_result_tx = Some(timeout_tx);
                self.worker_waker = Some(timeout_waker);
            }
            Err(error) => {
                self.loaded = true;
                self.loading = false;
                self.error = Some(format!("cannot start SIFS browser worker: {error}"));
                self.list_revision = self.list_revision.wrapping_add(1);
            }
        }
    }

    pub fn pump_load(&mut self) -> bool {
        let mut changed = false;
        loop {
            let result = match self.worker_rx.as_ref().map(|rx| rx.try_recv()) {
                Some(Ok(result)) => result,
                Some(Err(TryRecvError::Empty)) | None => break,
                Some(Err(TryRecvError::Disconnected)) => {
                    if self.loading {
                        self.loading = false;
                        self.pending_request_id = None;
                        self.error = Some("SIFS browser worker stopped".to_string());
                        self.list_revision = self.list_revision.wrapping_add(1);
                        changed = true;
                    }
                    if self.details_loading {
                        self.details_loading = false;
                        self.pending_details_request_id = None;
                        self.selected_details = None;
                        self.details_error = Some("SIFS browser worker stopped".to_string());
                        changed = true;
                    }
                    if self.preview_loading {
                        self.preview_loading = false;
                        self.pending_preview_request_id = None;
                        self.selected_preview = None;
                        self.preview_error = Some("SIFS browser worker stopped".to_string());
                        changed = true;
                    }
                    self.worker_rx = None;
                    self.worker_tx = None;
                    self.worker_result_tx = None;
                    self.worker_waker = None;
                    break;
                }
            };
            if self.apply_worker_result(result) {
                changed = true;
            }
        }
        changed
    }

    fn apply_worker_result(&mut self, result: FilesWorkerResult) -> bool {
        match result {
            FilesWorkerResult::List {
                request_id,
                source,
                path,
                result,
            } => {
                if self.pending_request_id != Some(request_id) {
                    return false;
                }
                if self.source != source {
                    return false;
                }
                self.pending_request_id = None;
                self.loading = false;
                self.path = path;
                self.store_active_path();
                match result {
                    Ok(listing) => {
                        self.node_id = listing.node_id;
                        self.entries = listing.entries;
                        self.error = None;
                        if let Some(select_path) = self.pending_select_path.take()
                            && let Some(index) = self
                                .entries
                                .iter()
                                .position(|entry| entry.path == select_path)
                        {
                            self.selected = Some(index);
                            self.request_selected_details();
                        }
                    }
                    Err(error) => {
                        self.entries.clear();
                        self.error = Some(error);
                    }
                }
                self.list_revision = self.list_revision.wrapping_add(1);
                true
            }
            FilesWorkerResult::Details {
                request_id,
                source,
                path,
                result,
            } => {
                if self.pending_details_request_id != Some(request_id) {
                    return false;
                }
                if self.source != source {
                    return false;
                }
                self.pending_details_request_id = None;
                self.details_loading = false;
                let selected_path = self.selected_entry().map(|entry| entry.path.as_str());
                if selected_path != Some(path.as_str()) {
                    return false;
                }
                match result {
                    Ok(FilesObjectDetails::Sifs(details)) => {
                        self.selected_details = Some(details);
                        self.selected_local_details = None;
                        self.details_error = None;
                    }
                    Ok(FilesObjectDetails::Local(details)) => {
                        self.selected_details = None;
                        self.selected_local_details = Some(details);
                        self.details_error = None;
                    }
                    Err(error) => {
                        self.selected_details = None;
                        self.selected_local_details = None;
                        self.details_error = Some(error);
                    }
                }
                true
            }
            FilesWorkerResult::Preview {
                request_id,
                source,
                path,
                result,
            } => {
                if self.pending_preview_request_id != Some(request_id) {
                    return false;
                }
                if self.source != source {
                    return false;
                }
                self.pending_preview_request_id = None;
                self.preview_loading = false;
                let selected_path = self.selected_entry().map(|entry| entry.path.as_str());
                if selected_path != Some(path.as_str()) {
                    return false;
                }
                match result {
                    Ok(preview) => {
                        self.selected_preview = Some(preview);
                        self.preview_error = None;
                    }
                    Err(error) => {
                        self.selected_preview = None;
                        self.preview_error = Some(error);
                    }
                }
                true
            }
            FilesWorkerResult::TaskProgress {
                request_id,
                detail,
                progress,
            } => self.update_task_progress(request_id, detail, progress),
            FilesWorkerResult::Operation { request_id, result } => {
                self.apply_operation_result(request_id, result)
            }
        }
    }

    fn current_location(&self) -> FilesLocation {
        FilesLocation {
            source: self.source,
            path: self.path.clone(),
        }
    }

    fn can_go_back(&self) -> bool {
        !self.history_back.is_empty()
    }

    fn can_go_forward(&self) -> bool {
        !self.history_forward.is_empty()
    }

    fn can_modify_current_location(&self) -> bool {
        self.loaded && !self.loading && self.error.is_none()
    }

    fn push_history(stack: &mut Vec<FilesLocation>, location: FilesLocation) {
        if stack.last() == Some(&location) {
            return;
        }
        stack.push(location);
        if stack.len() > MAX_NAV_HISTORY {
            stack.remove(0);
        }
    }

    fn record_navigation_origin(&mut self) {
        let location = self.current_location();
        Self::push_history(&mut self.history_back, location);
        self.history_forward.clear();
    }

    fn normalize_location_path(
        &self,
        source: FilesSourceKind,
        path: &str,
    ) -> Result<String, String> {
        match source {
            FilesSourceKind::Sifs => Ok(self
                .browser
                .as_ref()
                .map(|b| b.normalize(path))
                .unwrap_or_else(|| normalize_path(path))),
            FilesSourceKind::Local => normalize_local_path(path),
        }
    }

    fn apply_location(
        &mut self,
        source: FilesSourceKind,
        path: String,
        waker: Option<sui::UiHandle>,
    ) {
        if self.source != source {
            self.store_active_path();
            self.source = source;
            self.error = None;
            self.entries.clear();
            self.selected = None;
            self.clear_selected_details();
            self.loading = false;
            self.pending_request_id = None;
            self.pending_details_request_id = None;
            self.pending_preview_request_id = None;
            self.list_revision = self.list_revision.wrapping_add(1);
            if let Some(waker) = waker {
                self.ensure_worker(waker);
            }
            self.loaded = true;
        }
        self.path = path;
        self.store_active_path();
        self.reload();
    }

    fn open_location(
        &mut self,
        source: FilesSourceKind,
        path: String,
        waker: Option<sui::UiHandle>,
    ) {
        let normalized = match self.normalize_location_path(source, &path) {
            Ok(path) => path,
            Err(error) => {
                self.error = Some(error);
                self.list_revision = self.list_revision.wrapping_add(1);
                return;
            }
        };
        if self.source == source && self.path == normalized {
            self.reload();
            return;
        }
        self.record_navigation_origin();
        self.apply_location(source, normalized, waker);
    }

    fn navigate(&mut self, path: String) {
        self.open_location(self.source, path, None);
    }

    fn go_up(&mut self) {
        let parent = match self.source {
            FilesSourceKind::Sifs => self
                .browser
                .as_ref()
                .and_then(|b| b.parent(&self.path))
                .or_else(|| parent_path(&self.path)),
            FilesSourceKind::Local => parent_local_path(&self.path),
        };
        if let Some(parent) = parent {
            self.navigate(parent);
        }
    }

    fn go_back(&mut self, waker: Option<sui::UiHandle>) {
        let Some(location) = self.history_back.pop() else {
            return;
        };
        let current = self.current_location();
        if current != location {
            Self::push_history(&mut self.history_forward, current);
        }
        self.apply_location(location.source, location.path, waker);
    }

    fn go_forward(&mut self, waker: Option<sui::UiHandle>) {
        let Some(location) = self.history_forward.pop() else {
            return;
        };
        let current = self.current_location();
        if current != location {
            Self::push_history(&mut self.history_back, current);
        }
        self.apply_location(location.source, location.path, waker);
    }

    fn switch_source(&mut self, source: FilesSourceKind, waker: Option<sui::UiHandle>) {
        if self.source == source {
            return;
        }
        let path = match source {
            FilesSourceKind::Sifs => self.sifs_path.clone(),
            FilesSourceKind::Local => self.local_path.clone(),
        };
        self.open_location(source, path, waker);
    }

    fn store_active_path(&mut self) {
        match self.source {
            FilesSourceKind::Sifs => self.sifs_path = self.path.clone(),
            FilesSourceKind::Local => self.local_path = self.path.clone(),
        }
    }

    fn clear_selected_details(&mut self) {
        self.selected_details = None;
        self.selected_local_details = None;
        self.details_loading = false;
        self.details_error = None;
        self.pending_details_request_id = None;
        self.selected_preview = None;
        self.preview_loading = false;
        self.preview_error = None;
        self.pending_preview_request_id = None;
    }

    fn selected_entry(&self) -> Option<&SifsEntry> {
        self.selected.and_then(|i| self.entries.get(i))
    }

    fn entry_marked(&self, index: usize) -> bool {
        self.marked_entries.contains(&index)
    }

    fn marked_count(&self) -> usize {
        self.marked_entries.len()
    }

    fn marked_targets(&self) -> Vec<FileActionTarget> {
        self.marked_entries
            .iter()
            .filter_map(|index| self.entries.get(*index))
            .map(|entry| FileActionTarget {
                path: entry.path.clone(),
                is_dir: entry.is_dir,
            })
            .collect()
    }

    fn toggle_marked_entry(&mut self, index: usize) -> bool {
        if index >= self.entries.len() {
            return false;
        }
        if !self.marked_entries.insert(index) {
            self.marked_entries.remove(&index);
        }
        true
    }

    fn clear_marked_entries(&mut self) {
        self.marked_entries.clear();
    }

    fn mark_visible_entries(&mut self) {
        let filter = normalized_files_filter(&self.filter_query);
        self.marked_entries = visible_file_indices(&self.entries, &filter, self.sort_mode)
            .into_iter()
            .collect();
    }

    fn request_selected_details(&mut self) {
        let Some(entry) = self.selected_entry().cloned() else {
            self.clear_selected_details();
            return;
        };
        let path = entry.path.clone();
        self.selected_details = None;
        self.details_error = None;
        if let Some(tx) = self.worker_tx.clone() {
            let request_id = self.next_request_id;
            self.next_request_id = self.next_request_id.wrapping_add(1);
            self.pending_details_request_id = Some(request_id);
            self.details_loading = true;
            if tx
                .send(FilesWorkerCommand::Details {
                    request_id,
                    source: self.source,
                    path,
                })
                .is_err()
            {
                self.pending_details_request_id = None;
                self.details_loading = false;
                self.details_error = Some("SIFS browser worker stopped".to_string());
            }
            self.request_selected_preview(entry);
            return;
        }

        self.pending_details_request_id = None;
        self.details_loading = false;
        match self.source {
            FilesSourceKind::Sifs => match self.browser.as_ref().map(|browser| browser.stat(&path))
            {
                Some(Ok(details)) => self.selected_details = Some(details),
                Some(Err(error)) => self.details_error = Some(error),
                None => {}
            },
            FilesSourceKind::Local => match local_details(&path) {
                Ok(details) => self.selected_local_details = Some(details),
                Err(error) => self.details_error = Some(error),
            },
        }
        self.request_selected_preview(entry);
    }

    fn request_selected_preview(&mut self, entry: SifsEntry) {
        self.selected_preview = None;
        self.preview_error = None;
        self.pending_preview_request_id = None;
        if entry.is_dir {
            self.preview_loading = false;
            return;
        }
        if let Some(tx) = self.worker_tx.clone() {
            let request_id = self.next_request_id;
            self.next_request_id = self.next_request_id.wrapping_add(1);
            self.pending_preview_request_id = Some(request_id);
            self.preview_loading = true;
            if tx
                .send(FilesWorkerCommand::Preview {
                    request_id,
                    source: self.source,
                    path: entry.path,
                })
                .is_err()
            {
                self.pending_preview_request_id = None;
                self.preview_loading = false;
                self.preview_error = Some("SIFS browser worker stopped".to_string());
            }
            return;
        }

        self.preview_loading = false;
        let result = match self.source {
            FilesSourceKind::Sifs => self
                .browser
                .as_ref()
                .ok_or_else(|| "SIFS browser is not open".to_string())
                .and_then(|browser| {
                    browser
                        .read_file_prefix(&entry.path, FILE_PREVIEW_MAX_BYTES)
                        .map(|read| preview_from_read(&entry.name, read))
                }),
            FilesSourceKind::Local => local_read_file_prefix(&entry.path, FILE_PREVIEW_MAX_BYTES)
                .map(|read| preview_from_read(&entry.name, read)),
        };
        match result {
            Ok(preview) => self.selected_preview = Some(preview),
            Err(error) => self.preview_error = Some(error),
        }
    }

    fn select_entry(&mut self, index: usize) -> bool {
        if index >= self.entries.len() {
            return false;
        }
        let changed = self.selected != Some(index);
        self.selected = Some(index);
        if changed || self.selected_details.is_none() {
            self.request_selected_details();
        }
        changed
    }

    fn open_action_prompt(&mut self, kind: FileActionPromptKind) {
        let targets = if file_prompt_uses_marked_targets(kind) {
            self.marked_targets()
        } else {
            Vec::new()
        };
        let selected = self.selected_entry().cloned();
        let (target_path, target_name, target_is_dir) = match selected {
            Some(entry) => (Some(entry.path), entry.name, entry.is_dir),
            None => (None, String::new(), false),
        };
        let input = match kind {
            FileActionPromptKind::GoTo => self.path.clone(),
            FileActionPromptKind::NewFile => "untitled.txt".to_string(),
            FileActionPromptKind::NewFolder => "New folder".to_string(),
            FileActionPromptKind::Copy => {
                if target_name.is_empty() {
                    String::new()
                } else {
                    suggested_copy_name(&target_name)
                }
            }
            FileActionPromptKind::CopySelected | FileActionPromptKind::MoveSelected => {
                self.path.clone()
            }
            FileActionPromptKind::CopyToSifs => {
                default_sifs_transfer_destination(&self.sifs_path, &target_name)
            }
            FileActionPromptKind::CopyToLocal => {
                default_local_transfer_destination(&self.local_path, &target_name)
            }
            FileActionPromptKind::TransferSelected => match self.source {
                FilesSourceKind::Sifs => self.local_path.clone(),
                FilesSourceKind::Local => self.sifs_path.clone(),
            },
            FileActionPromptKind::Move => target_name.clone(),
            FileActionPromptKind::Rename => target_name.clone(),
            FileActionPromptKind::Delete | FileActionPromptKind::DeleteSelected => String::new(),
        };
        let error = match kind {
            FileActionPromptKind::Copy
            | FileActionPromptKind::CopyToSifs
            | FileActionPromptKind::CopyToLocal
            | FileActionPromptKind::Move
            | FileActionPromptKind::Rename
            | FileActionPromptKind::Delete
                if target_path.is_none() =>
            {
                Some("select an item first".to_string())
            }
            FileActionPromptKind::CopySelected
            | FileActionPromptKind::TransferSelected
            | FileActionPromptKind::MoveSelected
            | FileActionPromptKind::DeleteSelected
                if targets.is_empty() =>
            {
                Some("mark one or more entries first".to_string())
            }
            _ => None,
        };
        self.action_prompt = Some(FileActionPrompt {
            kind,
            input,
            target_name,
            target_path,
            target_is_dir,
            targets,
            error,
        });
    }

    fn close_action_prompt(&mut self) {
        self.action_prompt = None;
    }

    fn set_action_prompt_input(&mut self, input: String) {
        if let Some(prompt) = &mut self.action_prompt {
            prompt.input = input;
            prompt.error = None;
        }
    }

    fn set_filter_query(&mut self, query: String) {
        self.filter_query = query;
        let filter = normalized_files_filter(&self.filter_query);
        let entries = &self.entries;
        self.marked_entries.retain(|index| {
            entries
                .get(*index)
                .is_some_and(|entry| file_entry_matches_filter(entry, &filter))
        });
        let selected_matches = self
            .selected_entry()
            .is_some_and(|entry| file_entry_matches_filter(entry, &filter));
        if !selected_matches {
            self.selected = None;
            self.clear_selected_details();
        }
    }

    fn set_sort_mode(&mut self, mode: FileSortMode) {
        self.sort_mode = mode;
    }

    fn submit_action_prompt(&mut self, waker: Option<sui::UiHandle>) {
        let Some(prompt) = self.action_prompt.clone() else {
            return;
        };
        if prompt.kind == FileActionPromptKind::GoTo {
            match normalize_jump_path(self.source, &self.path, &prompt.input) {
                Ok(path) => {
                    self.action_prompt = None;
                    if let Some(waker) = waker {
                        self.ensure_worker(waker);
                    }
                    self.navigate(path);
                }
                Err(error) => {
                    if let Some(current) = &mut self.action_prompt {
                        current.error = Some(error);
                    }
                }
            }
            return;
        }
        if prompt.kind == FileActionPromptKind::DeleteSelected {
            if prompt.targets.is_empty() {
                if let Some(current) = &mut self.action_prompt {
                    current.error = Some("mark one or more entries first".to_string());
                }
                return;
            }
            if prompt.input.trim() != "DELETE" {
                if let Some(current) = &mut self.action_prompt {
                    current.error = Some(format!(
                        "type DELETE to remove {}",
                        marked_entries_label(prompt.targets.len())
                    ));
                }
                return;
            }
            self.action_prompt = None;
            self.start_delete_targets(prompt.targets, waker);
            return;
        }
        if matches!(
            prompt.kind,
            FileActionPromptKind::CopySelected
                | FileActionPromptKind::TransferSelected
                | FileActionPromptKind::MoveSelected
        ) {
            match self.operations_from_marked_prompt(&prompt) {
                Ok(operations) => {
                    self.action_prompt = None;
                    self.start_marked_operations(operations, waker);
                }
                Err(error) => {
                    if let Some(current) = &mut self.action_prompt {
                        current.error = Some(error);
                    }
                }
            }
            return;
        }
        match self.operation_from_prompt(&prompt) {
            Ok((operation, title, detail)) => {
                self.action_prompt = None;
                self.start_operation(operation, title, detail, waker);
            }
            Err(error) => {
                if let Some(current) = &mut self.action_prompt {
                    current.error = Some(error);
                }
            }
        }
    }

    fn operation_from_prompt(
        &self,
        prompt: &FileActionPrompt,
    ) -> Result<(FilesOperation, String, String), String> {
        match prompt.kind {
            FileActionPromptKind::GoTo => unreachable!("Go to path is handled before operations"),
            FileActionPromptKind::DeleteSelected => {
                unreachable!("Delete selected queues individual delete operations")
            }
            FileActionPromptKind::CopySelected
            | FileActionPromptKind::TransferSelected
            | FileActionPromptKind::MoveSelected => {
                unreachable!("marked-entry prompts queue individual operations")
            }
            FileActionPromptKind::NewFile => {
                let path = self.join_child(&prompt.input)?;
                Ok((
                    FilesOperation {
                        kind: FilesOperationKind::CreateFile,
                        path: path.clone(),
                        target: None,
                        directory: false,
                    },
                    "Create file".to_string(),
                    path,
                ))
            }
            FileActionPromptKind::NewFolder => {
                let path = self.join_child(&prompt.input)?;
                Ok((
                    FilesOperation {
                        kind: FilesOperationKind::CreateFolder,
                        path: path.clone(),
                        target: None,
                        directory: true,
                    },
                    "Create folder".to_string(),
                    path,
                ))
            }
            FileActionPromptKind::Copy => {
                let from = prompt
                    .target_path
                    .clone()
                    .ok_or_else(|| "select an item first".to_string())?;
                let to = self.resolve_destination_path(&prompt.input)?;
                if from == to {
                    return Err("copy destination matches the source path".to_string());
                }
                if prompt.target_is_dir && destination_inside_source(self.source, &from, &to) {
                    return Err("copy destination cannot be inside the source folder".to_string());
                }
                Ok((
                    FilesOperation {
                        kind: FilesOperationKind::Copy,
                        path: from.clone(),
                        target: Some(to.clone()),
                        directory: prompt.target_is_dir,
                    },
                    "Copy".to_string(),
                    format!("{from} -> {to}"),
                ))
            }
            FileActionPromptKind::CopyToSifs => {
                if self.source != FilesSourceKind::Local {
                    return Err("select a local file or folder first".to_string());
                }
                let from = prompt
                    .target_path
                    .clone()
                    .ok_or_else(|| "select an item first".to_string())?;
                let to = resolve_sifs_destination_path(&self.sifs_path, &prompt.input)?;
                Ok((
                    FilesOperation {
                        kind: FilesOperationKind::CopyToSifs,
                        path: from.clone(),
                        target: Some(to.clone()),
                        directory: prompt.target_is_dir,
                    },
                    "Copy to SIFS".to_string(),
                    format!("{from} -> {to}"),
                ))
            }
            FileActionPromptKind::CopyToLocal => {
                if self.source != FilesSourceKind::Sifs {
                    return Err("select a SIFS file or folder first".to_string());
                }
                let from = prompt
                    .target_path
                    .clone()
                    .ok_or_else(|| "select an item first".to_string())?;
                let to = resolve_local_destination_path(&self.local_path, &prompt.input)?;
                Ok((
                    FilesOperation {
                        kind: FilesOperationKind::CopyToLocal,
                        path: from.clone(),
                        target: Some(to.clone()),
                        directory: prompt.target_is_dir,
                    },
                    "Download".to_string(),
                    format!("{from} -> {to}"),
                ))
            }
            FileActionPromptKind::Move => {
                let from = prompt
                    .target_path
                    .clone()
                    .ok_or_else(|| "select an item first".to_string())?;
                let to = self.resolve_destination_path(&prompt.input)?;
                if from == to {
                    return Err("move destination matches the source path".to_string());
                }
                Ok((
                    FilesOperation {
                        kind: FilesOperationKind::Move,
                        path: from.clone(),
                        target: Some(to.clone()),
                        directory: prompt.target_is_dir,
                    },
                    "Move".to_string(),
                    format!("{from} -> {to}"),
                ))
            }
            FileActionPromptKind::Rename => {
                let from = prompt
                    .target_path
                    .clone()
                    .ok_or_else(|| "select an item first".to_string())?;
                let to = self.join_child(&prompt.input)?;
                if from == to {
                    return Err("new name matches the current name".to_string());
                }
                Ok((
                    FilesOperation {
                        kind: FilesOperationKind::Rename,
                        path: from.clone(),
                        target: Some(to.clone()),
                        directory: prompt.target_is_dir,
                    },
                    "Rename".to_string(),
                    format!("{from} -> {to}"),
                ))
            }
            FileActionPromptKind::Delete => {
                let path = prompt
                    .target_path
                    .clone()
                    .ok_or_else(|| "select an item first".to_string())?;
                if prompt.input.trim() != "DELETE" {
                    return Err(format!("type DELETE to remove {}", prompt.target_name));
                }
                Ok((
                    FilesOperation {
                        kind: FilesOperationKind::Delete,
                        path: path.clone(),
                        target: None,
                        directory: prompt.target_is_dir,
                    },
                    "Delete".to_string(),
                    path,
                ))
            }
        }
    }

    fn operations_from_marked_prompt(
        &self,
        prompt: &FileActionPrompt,
    ) -> Result<Vec<FilesQueuedOperation>, String> {
        if prompt.targets.is_empty() {
            return Err("mark one or more entries first".to_string());
        }
        let (operation_kind, destination_source, title) = match prompt.kind {
            FileActionPromptKind::CopySelected => {
                (FilesOperationKind::Copy, self.source, "Copy selected")
            }
            FileActionPromptKind::MoveSelected => {
                (FilesOperationKind::Move, self.source, "Move selected")
            }
            FileActionPromptKind::TransferSelected => match self.source {
                FilesSourceKind::Sifs => (
                    FilesOperationKind::CopyToLocal,
                    FilesSourceKind::Local,
                    "Download selected",
                ),
                FilesSourceKind::Local => (
                    FilesOperationKind::CopyToSifs,
                    FilesSourceKind::Sifs,
                    "Copy selected to SIFS",
                ),
            },
            _ => return Err("prompt does not support marked entries".to_string()),
        };
        let destination_root =
            self.resolve_destination_folder(destination_source, &prompt.input)?;
        prompt
            .targets
            .iter()
            .map(|target| {
                let name = file_action_target_name(self.source, &target.path)?;
                let destination =
                    join_destination_child(destination_source, &destination_root, &name)?;
                if self.source == destination_source && target.path == destination {
                    return Err(format!("destination matches {}", target.path));
                }
                if self.source == destination_source
                    && target.is_dir
                    && destination_inside_source(self.source, &target.path, &destination)
                {
                    return Err(format!(
                        "destination cannot be inside the selected folder {}",
                        target.path
                    ));
                }
                Ok(FilesQueuedOperation {
                    operation: FilesOperation {
                        kind: operation_kind,
                        path: target.path.clone(),
                        target: Some(destination.clone()),
                        directory: target.is_dir,
                    },
                    title: title.to_string(),
                    detail: format!("{} -> {destination}", target.path),
                })
            })
            .collect()
    }

    fn resolve_destination_folder(
        &self,
        source: FilesSourceKind,
        input: &str,
    ) -> Result<String, String> {
        match source {
            FilesSourceKind::Sifs => {
                let base = if self.source == FilesSourceKind::Sifs {
                    &self.path
                } else {
                    &self.sifs_path
                };
                resolve_sifs_destination_folder(base, input)
            }
            FilesSourceKind::Local => {
                let base = if self.source == FilesSourceKind::Local {
                    &self.path
                } else {
                    &self.local_path
                };
                resolve_local_destination_path(base, input)
            }
        }
    }

    fn start_operation(
        &mut self,
        operation: FilesOperation,
        title: String,
        detail: String,
        waker: Option<sui::UiHandle>,
    ) {
        if let Some(waker) = waker {
            self.ensure_worker(waker);
        }
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        clear_cancelled_operation(&self.worker_cancellations, request_id);
        self.tasks.insert(
            0,
            FilesTask {
                id: request_id,
                source: self.source,
                title,
                detail,
                state: FilesTaskState::Running,
                progress: Some(0.02),
            },
        );
        self.trim_tasks();

        if let Some(tx) = self.worker_tx.clone() {
            if tx
                .send(FilesWorkerCommand::Operation {
                    request_id,
                    source: self.source,
                    operation,
                })
                .is_err()
            {
                self.finish_task(
                    request_id,
                    FilesTaskState::Failed,
                    "worker stopped".to_string(),
                );
            }
            return;
        }

        let result = self
            .execute_operation(operation, &self.path)
            .map_err(FilesOperationFailure::Error);
        self.apply_operation_result(request_id, result);
    }

    fn start_marked_operations(
        &mut self,
        operations: Vec<FilesQueuedOperation>,
        waker: Option<sui::UiHandle>,
    ) {
        self.clear_marked_entries();
        for queued in operations {
            self.start_operation(queued.operation, queued.title, queued.detail, waker.clone());
        }
    }

    fn start_delete_targets(
        &mut self,
        targets: Vec<FileActionTarget>,
        waker: Option<sui::UiHandle>,
    ) {
        self.clear_marked_entries();
        for target in targets {
            self.start_operation(
                FilesOperation {
                    kind: FilesOperationKind::Delete,
                    path: target.path.clone(),
                    target: None,
                    directory: target.is_dir,
                },
                "Delete".to_string(),
                target.path,
                waker.clone(),
            );
        }
    }

    fn start_placement_refresh(&mut self, waker: Option<sui::UiHandle>) {
        if self.source != FilesSourceKind::Sifs {
            return;
        }
        let Some(entry) = self.selected_entry().cloned() else {
            return;
        };
        self.start_operation(
            FilesOperation {
                kind: FilesOperationKind::RefreshPlacement,
                path: entry.path.clone(),
                target: None,
                directory: entry.is_dir,
            },
            "Refresh placement".to_string(),
            entry.path,
            waker,
        );
    }

    fn start_replica_repair(&mut self, waker: Option<sui::UiHandle>) {
        if self.source != FilesSourceKind::Sifs {
            return;
        }
        let Some(entry) = self.selected_entry().cloned() else {
            return;
        };
        self.start_operation(
            FilesOperation {
                kind: FilesOperationKind::RepairReplicas,
                path: entry.path.clone(),
                target: None,
                directory: entry.is_dir,
            },
            "Repair replicas".to_string(),
            entry.path,
            waker,
        );
    }

    fn start_pin_content(&mut self, waker: Option<sui::UiHandle>) {
        if self.source != FilesSourceKind::Sifs {
            return;
        }
        let Some(entry) = self.selected_entry().cloned() else {
            return;
        };
        self.start_operation(
            FilesOperation {
                kind: FilesOperationKind::PinContent,
                path: entry.path.clone(),
                target: None,
                directory: entry.is_dir,
            },
            "Pin content".to_string(),
            entry.path,
            waker,
        );
    }

    fn start_unpin_content(&mut self, waker: Option<sui::UiHandle>) {
        if self.source != FilesSourceKind::Sifs {
            return;
        }
        let Some(entry) = self.selected_entry().cloned() else {
            return;
        };
        self.start_operation(
            FilesOperation {
                kind: FilesOperationKind::UnpinContent,
                path: entry.path.clone(),
                target: None,
                directory: entry.is_dir,
            },
            "Unpin content".to_string(),
            entry.path,
            waker,
        );
    }

    fn join_child(&self, name: &str) -> Result<String, String> {
        match self.source {
            FilesSourceKind::Sifs => join_sifs_child(&self.path, name),
            FilesSourceKind::Local => join_local_child(&self.path, name),
        }
    }

    fn resolve_destination_path(&self, input: &str) -> Result<String, String> {
        match self.source {
            FilesSourceKind::Sifs => resolve_sifs_destination_path(&self.path, input),
            FilesSourceKind::Local => resolve_local_destination_path(&self.path, input),
        }
    }

    fn execute_operation(
        &self,
        operation: FilesOperation,
        current_path: &str,
    ) -> Result<FilesOperationOutcome, String> {
        if files_operation_is_cross_source(&operation) {
            if let Some(browser) = self.browser.as_ref() {
                return execute_cross_source_operation(
                    self.source,
                    browser,
                    operation,
                    current_path,
                    None,
                )
                .map_err(files_operation_failure_detail);
            }
            let browser = self
                .daemon_address
                .as_deref()
                .ok_or_else(|| "SIFS browsing requires a daemon connection".to_string())
                .and_then(|address| {
                    SifsBrowser::open_daemon_on_runtime(address, &self.app_runtime)
                })?;
            return execute_cross_source_operation(
                self.source,
                &browser,
                operation,
                current_path,
                None,
            )
            .map_err(files_operation_failure_detail);
        }
        match self.source {
            FilesSourceKind::Sifs => self
                .browser
                .as_ref()
                .ok_or_else(|| "SIFS browser is not open".to_string())
                .and_then(|browser| {
                    execute_sifs_operation(browser, operation, current_path, None)
                        .map_err(files_operation_failure_detail)
                }),
            FilesSourceKind::Local => execute_local_operation(operation, current_path),
        }
    }

    fn cancel_task(&mut self, task_id: u64) {
        if let Some(task) = self
            .tasks
            .iter_mut()
            .find(|task| task.id == task_id && task.state == FilesTaskState::Running)
        {
            task.state = FilesTaskState::CancelRequested;
            task.detail = "cancel requested; waiting for DFS call to return".to_string();
            mark_cancelled_operation(&self.worker_cancellations, task_id);
        }
    }

    fn finish_task(&mut self, task_id: u64, state: FilesTaskState, detail: String) {
        if let Some(task) = self.tasks.iter_mut().find(|task| task.id == task_id) {
            task.state = state;
            task.detail = detail;
            task.progress = Some(1.0);
        }
        self.trim_tasks();
    }

    fn update_task_progress(
        &mut self,
        task_id: u64,
        detail: String,
        progress: Option<f32>,
    ) -> bool {
        let Some(task) = self.tasks.iter_mut().find(|task| task.id == task_id) else {
            return false;
        };
        if task.state != FilesTaskState::Running {
            return false;
        }
        task.detail = detail;
        if let Some(progress) = progress {
            task.progress = Some(progress.clamp(0.0, 0.98));
        }
        true
    }

    fn trim_tasks(&mut self) {
        const MAX_TASKS: usize = 6;
        if self.tasks.len() > MAX_TASKS {
            self.tasks.truncate(MAX_TASKS);
        }
    }

    fn clear_finished_tasks(&mut self) {
        self.tasks.retain(|task| {
            matches!(
                task.state,
                FilesTaskState::Running | FilesTaskState::CancelRequested
            )
        });
    }

    fn apply_operation_result(&mut self, request_id: u64, result: FilesOperationResult) -> bool {
        let cancel_requested = self
            .tasks
            .iter()
            .find(|task| task.id == request_id)
            .is_some_and(|task| task.state == FilesTaskState::CancelRequested);
        clear_cancelled_operation(&self.worker_cancellations, request_id);
        match result {
            Ok(outcome) => {
                let detail = if cancel_requested {
                    format!(
                        "completed before cancellation could stop it: {}",
                        outcome.message
                    )
                } else {
                    outcome.message.clone()
                };
                self.finish_task(request_id, FilesTaskState::Completed, detail);
                self.path = outcome.refresh_path;
                self.pending_select_path = outcome.select_path;
                self.refresh(None);
            }
            Err(FilesOperationFailure::Cancelled(detail)) => {
                self.finish_task(request_id, FilesTaskState::Cancelled, detail);
            }
            Err(FilesOperationFailure::Error(error)) => {
                self.finish_task(request_id, FilesTaskState::Failed, error);
            }
        }
        true
    }

    /// Click a listing row: navigate into a directory, otherwise select the file. Returns whether
    /// the navigation changed the *path* (so the caller can relayout the rebuilt list/breadcrumb).
    fn activate_row(&mut self, index: usize) -> bool {
        let is_dir = self.entries.get(index).map(|e| e.is_dir).unwrap_or(false);
        if is_dir {
            let target = self.entries[index].path.clone();
            self.navigate(target);
            true
        } else {
            self.select_entry(index);
            false
        }
    }

    /// The breadcrumb path segments as `(label, target_path)` pairs, root first.
    fn crumbs(&self) -> Vec<(String, String)> {
        if self.source == FilesSourceKind::Local {
            return local_crumbs(&self.path);
        }
        let mut out = vec![("SIFS".to_string(), "/".to_string())];
        let mut acc = String::new();
        let mut parent: Option<String> = None;
        for part in self.path.split('/').filter(|s| !s.is_empty()) {
            acc.push('/');
            acc.push_str(part);
            out.push((crumb_label(part, parent.as_deref()), acc.clone()));
            parent = Some(part.to_string());
        }
        out
    }
}

struct FilesListing {
    node_id: String,
    entries: Vec<SifsEntry>,
}

enum FilesWorkerResult {
    List {
        request_id: u64,
        source: FilesSourceKind,
        path: String,
        result: Result<FilesListing, String>,
    },
    Details {
        request_id: u64,
        source: FilesSourceKind,
        path: String,
        result: Result<FilesObjectDetails, String>,
    },
    Preview {
        request_id: u64,
        source: FilesSourceKind,
        path: String,
        result: Result<FilesPreview, String>,
    },
    TaskProgress {
        request_id: u64,
        detail: String,
        progress: Option<f32>,
    },
    Operation {
        request_id: u64,
        result: FilesOperationResult,
    },
}

enum FilesWorkerCommand {
    List {
        request_id: u64,
        source: FilesSourceKind,
        path: String,
    },
    Details {
        request_id: u64,
        source: FilesSourceKind,
        path: String,
    },
    Preview {
        request_id: u64,
        source: FilesSourceKind,
        path: String,
    },
    Operation {
        request_id: u64,
        source: FilesSourceKind,
        operation: FilesOperation,
    },
}

struct FilesOperationProgress<'a> {
    request_id: u64,
    result_tx: &'a Sender<FilesWorkerResult>,
    cancellations: &'a Arc<Mutex<BTreeSet<u64>>>,
    waker: &'a sui::UiHandle,
}

impl FilesOperationProgress<'_> {
    fn report(&self, detail: impl Into<String>, progress: Option<f32>) {
        let _ = self.result_tx.send(FilesWorkerResult::TaskProgress {
            request_id: self.request_id,
            detail: detail.into(),
            progress,
        });
        self.waker.wake();
    }

    fn cancelled(&self) -> bool {
        operation_cancelled(self.cancellations, self.request_id)
    }
}

fn files_worker_loop(
    data_dir: Arc<SinomoDataDir>,
    daemon_address: Option<String>,
    app_runtime: tokio::runtime::Handle,
    command_rx: Receiver<FilesWorkerCommand>,
    result_tx: Sender<FilesWorkerResult>,
    cancellations: Arc<Mutex<BTreeSet<u64>>>,
    waker: sui::UiHandle,
) {
    let mut browser: Option<SifsBrowser> = None;
    let mut open_error: Option<String> = daemon_address
        .is_none()
        .then(|| "SIFS browsing requires a daemon connection".to_string());
    while let Ok(command) = command_rx.recv() {
        match command {
            FilesWorkerCommand::List {
                request_id,
                source,
                path,
            } => {
                let requested_path = normalize_source_path(source, &path);
                let (path, result) = if source == FilesSourceKind::Local {
                    let path = requested_path.unwrap_or_else(|error| {
                        let _ = result_tx.send(FilesWorkerResult::List {
                            request_id,
                            source,
                            path: path.clone(),
                            result: Err(error),
                        });
                        String::new()
                    });
                    if path.is_empty() {
                        waker.wake();
                        continue;
                    }
                    let result = list_local_dir(&path).map(|entries| FilesListing {
                        node_id: local_node_label(),
                        entries,
                    });
                    (path, result)
                } else if let Some(error) = &open_error {
                    let requested_path = requested_path.unwrap_or_else(|_| normalize_path(&path));
                    (requested_path, Err(error.clone()))
                } else {
                    let requested_path = requested_path.unwrap_or_else(|_| normalize_path(&path));
                    if browser.is_none() {
                        match daemon_address.as_deref() {
                            Some(address) => {
                                match SifsBrowser::open_daemon_on_runtime(address, &app_runtime) {
                                    Ok(opened) => browser = Some(opened),
                                    Err(error) => open_error = Some(error),
                                }
                            }
                            None => {
                                let _ = &data_dir;
                                open_error =
                                    Some("SIFS browsing requires a daemon connection".to_string());
                            }
                        }
                    }
                    match (&browser, &open_error) {
                        (Some(browser), _) => {
                            let path = browser.normalize(&requested_path);
                            let result = browser.list_shallow(&path).map(|entries| FilesListing {
                                node_id: browser.node_id().to_string(),
                                entries,
                            });
                            (path, result)
                        }
                        (None, Some(error)) => (requested_path, Err(error.clone())),
                        (None, None) => (
                            requested_path,
                            Err("SIFS browser did not start".to_string()),
                        ),
                    }
                };
                let _ = result_tx.send(FilesWorkerResult::List {
                    request_id,
                    source,
                    path,
                    result,
                });
            }
            FilesWorkerCommand::Details {
                request_id,
                source,
                path,
            } => {
                let requested_path = normalize_source_path(source, &path);
                let (path, result) = if source == FilesSourceKind::Local {
                    let path = requested_path.unwrap_or_else(|error| {
                        let _ = result_tx.send(FilesWorkerResult::Details {
                            request_id,
                            source,
                            path: path.clone(),
                            result: Err(error),
                        });
                        String::new()
                    });
                    if path.is_empty() {
                        waker.wake();
                        continue;
                    }
                    let result = local_details(&path).map(FilesObjectDetails::Local);
                    (path, result)
                } else if let Some(error) = &open_error {
                    let requested_path = requested_path.unwrap_or_else(|_| normalize_path(&path));
                    (requested_path, Err(error.clone()))
                } else {
                    let requested_path = requested_path.unwrap_or_else(|_| normalize_path(&path));
                    if browser.is_none() {
                        match daemon_address.as_deref() {
                            Some(address) => {
                                match SifsBrowser::open_daemon_on_runtime(address, &app_runtime) {
                                    Ok(opened) => browser = Some(opened),
                                    Err(error) => open_error = Some(error),
                                }
                            }
                            None => {
                                let _ = &data_dir;
                                open_error =
                                    Some("SIFS browsing requires a daemon connection".to_string());
                            }
                        }
                    }
                    match (&browser, &open_error) {
                        (Some(browser), _) => {
                            let path = browser.normalize(&requested_path);
                            let result = browser.stat(&path).map(FilesObjectDetails::Sifs);
                            (path, result)
                        }
                        (None, Some(error)) => (requested_path, Err(error.clone())),
                        (None, None) => (
                            requested_path,
                            Err("SIFS browser did not start".to_string()),
                        ),
                    }
                };
                let _ = result_tx.send(FilesWorkerResult::Details {
                    request_id,
                    source,
                    path,
                    result,
                });
            }
            FilesWorkerCommand::Preview {
                request_id,
                source,
                path,
            } => {
                let requested_path = normalize_source_path(source, &path);
                let (path, result) = if source == FilesSourceKind::Local {
                    let path = requested_path.unwrap_or_else(|error| {
                        let _ = result_tx.send(FilesWorkerResult::Preview {
                            request_id,
                            source,
                            path: path.clone(),
                            result: Err(error),
                        });
                        String::new()
                    });
                    if path.is_empty() {
                        waker.wake();
                        continue;
                    }
                    let name = Path::new(&path)
                        .file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or(&path)
                        .to_string();
                    let result = local_read_file_prefix(&path, FILE_PREVIEW_MAX_BYTES)
                        .map(|read| preview_from_read(&name, read));
                    (path, result)
                } else if let Some(error) = &open_error {
                    let requested_path = requested_path.unwrap_or_else(|_| normalize_path(&path));
                    (requested_path, Err(error.clone()))
                } else {
                    let requested_path = requested_path.unwrap_or_else(|_| normalize_path(&path));
                    if browser.is_none() {
                        match daemon_address.as_deref() {
                            Some(address) => {
                                match SifsBrowser::open_daemon_on_runtime(address, &app_runtime) {
                                    Ok(opened) => browser = Some(opened),
                                    Err(error) => open_error = Some(error),
                                }
                            }
                            None => {
                                let _ = &data_dir;
                                open_error =
                                    Some("SIFS browsing requires a daemon connection".to_string());
                            }
                        }
                    }
                    match (&browser, &open_error) {
                        (Some(browser), _) => {
                            let path = browser.normalize(&requested_path);
                            let name = path
                                .rsplit('/')
                                .find(|part| !part.is_empty())
                                .unwrap_or(&path)
                                .to_string();
                            let result = browser
                                .read_file_prefix(&path, FILE_PREVIEW_MAX_BYTES)
                                .map(|read| preview_from_read(&name, read));
                            (path, result)
                        }
                        (None, Some(error)) => (requested_path, Err(error.clone())),
                        (None, None) => (
                            requested_path,
                            Err("SIFS browser did not start".to_string()),
                        ),
                    }
                };
                let _ = result_tx.send(FilesWorkerResult::Preview {
                    request_id,
                    source,
                    path,
                    result,
                });
            }
            FilesWorkerCommand::Operation {
                request_id,
                source,
                operation,
            } => {
                send_task_progress(
                    &result_tx,
                    &waker,
                    request_id,
                    operation_progress_label(source, &operation, "Preparing"),
                    Some(0.05),
                );
                let current_path = operation_parent_path(source, &operation.path);
                let result = if operation_cancelled(&cancellations, request_id) {
                    Err(FilesOperationFailure::Cancelled(
                        files_operation_cancelled_detail(&operation),
                    ))
                } else if source == FilesSourceKind::Local
                    && !files_operation_requires_sifs_browser(source, &operation)
                {
                    let progress = FilesOperationProgress {
                        request_id,
                        result_tx: &result_tx,
                        cancellations: &cancellations,
                        waker: &waker,
                    };
                    execute_local_operation_with_progress(operation, &current_path, Some(&progress))
                } else if let Some(error) = &open_error {
                    Err(FilesOperationFailure::Error(error.clone()))
                } else {
                    if browser.is_none() {
                        send_task_progress(
                            &result_tx,
                            &waker,
                            request_id,
                            "Opening SIFS connection".to_string(),
                            Some(0.12),
                        );
                        match daemon_address.as_deref() {
                            Some(address) => {
                                match SifsBrowser::open_daemon_on_runtime(address, &app_runtime) {
                                    Ok(opened) => browser = Some(opened),
                                    Err(error) => open_error = Some(error),
                                }
                            }
                            None => {
                                let _ = &data_dir;
                                open_error =
                                    Some("SIFS browsing requires a daemon connection".to_string());
                            }
                        }
                    }
                    match (&browser, &open_error) {
                        (Some(browser), _) => {
                            let progress = FilesOperationProgress {
                                request_id,
                                result_tx: &result_tx,
                                cancellations: &cancellations,
                                waker: &waker,
                            };
                            send_task_progress(
                                &result_tx,
                                &waker,
                                request_id,
                                operation_progress_label(source, &operation, "Running"),
                                Some(0.35),
                            );
                            if files_operation_is_cross_source(&operation) {
                                execute_cross_source_operation(
                                    source,
                                    browser,
                                    operation,
                                    &current_path,
                                    Some(&progress),
                                )
                            } else {
                                execute_sifs_operation(
                                    browser,
                                    operation,
                                    &current_path,
                                    Some(&progress),
                                )
                            }
                        }
                        (None, Some(error)) => Err(FilesOperationFailure::Error(error.clone())),
                        (None, None) => Err(FilesOperationFailure::Error(
                            "SIFS browser did not start".to_string(),
                        )),
                    }
                };
                clear_cancelled_operation(&cancellations, request_id);
                let _ = result_tx.send(FilesWorkerResult::Operation { request_id, result });
            }
        }
        waker.wake();
    }
}

fn send_task_progress(
    result_tx: &Sender<FilesWorkerResult>,
    waker: &sui::UiHandle,
    request_id: u64,
    detail: String,
    progress: Option<f32>,
) {
    let _ = result_tx.send(FilesWorkerResult::TaskProgress {
        request_id,
        detail,
        progress,
    });
    waker.wake();
}

fn mark_cancelled_operation(cancellations: &Arc<Mutex<BTreeSet<u64>>>, request_id: u64) {
    if let Ok(mut cancellations) = cancellations.lock() {
        cancellations.insert(request_id);
    }
}

fn clear_cancelled_operation(cancellations: &Arc<Mutex<BTreeSet<u64>>>, request_id: u64) {
    if let Ok(mut cancellations) = cancellations.lock() {
        cancellations.remove(&request_id);
    }
}

fn clear_all_cancelled_operations(cancellations: &Arc<Mutex<BTreeSet<u64>>>) {
    if let Ok(mut cancellations) = cancellations.lock() {
        cancellations.clear();
    }
}

fn operation_cancelled(cancellations: &Arc<Mutex<BTreeSet<u64>>>, request_id: u64) -> bool {
    cancellations
        .lock()
        .is_ok_and(|cancellations| cancellations.contains(&request_id))
}

fn normalize_path(path: &str) -> String {
    let mut parts = Vec::new();
    let path = path.replace('\\', "/");
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            part => parts.push(part.to_string()),
        }
    }
    if parts.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", parts.join("/"))
    }
}

fn default_local_path(fallback: impl AsRef<Path>) -> String {
    std::env::current_dir()
        .or_else(|_| Ok::<PathBuf, std::io::Error>(fallback.as_ref().to_path_buf()))
        .and_then(|path| path.canonicalize().or(Ok(path)))
        .map(|path| display_local_path(path.to_string_lossy().into_owned()))
        .unwrap_or_else(|_| ".".to_string())
}

fn local_node_label() -> String {
    std::env::var("COMPUTERNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "this node".to_string())
}

fn normalize_source_path(source: FilesSourceKind, path: &str) -> Result<String, String> {
    match source {
        FilesSourceKind::Sifs => Ok(normalize_path(path)),
        FilesSourceKind::Local => normalize_local_path(path),
    }
}

fn parent_path(path: &str) -> Option<String> {
    let path = normalize_path(path);
    if path == "/" {
        return None;
    }
    let (parent, _) = path.rsplit_once('/')?;
    Some(if parent.is_empty() {
        "/".to_string()
    } else {
        parent.to_string()
    })
}

fn normalize_local_path(path: &str) -> Result<String, String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err("path cannot be empty".to_string());
    }
    if trimmed.bytes().any(|byte| byte == 0) {
        return Err("path cannot contain NUL bytes".to_string());
    }
    let path = PathBuf::from(trimmed);
    let path = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .map_err(|error| format!("cannot read current directory: {error}"))?
            .join(path)
    };
    Ok(path
        .canonicalize()
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned())
    .map(display_local_path)
}

fn parent_local_path(path: &str) -> Option<String> {
    Path::new(path)
        .parent()
        .map(|parent| parent.to_string_lossy().into_owned())
        .filter(|parent| !parent.is_empty())
}

fn operation_parent_path(source: FilesSourceKind, path: &str) -> String {
    match source {
        FilesSourceKind::Sifs => normalize_path(path)
            .rsplit_once('/')
            .map(|(parent, _)| {
                if parent.is_empty() {
                    "/".to_string()
                } else {
                    parent.to_string()
                }
            })
            .unwrap_or_else(|| "/".to_string()),
        FilesSourceKind::Local => parent_local_path(path).unwrap_or_else(|| path.to_string()),
    }
}

fn validate_child_name(name: &str) -> Result<&str, String> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err("name cannot be empty".to_string());
    }
    if trimmed == "." || trimmed == ".." {
        return Err("name must be a child item".to_string());
    }
    if trimmed.contains('/') || trimmed.contains('\\') || trimmed.bytes().any(|byte| byte == 0) {
        return Err("name cannot contain path separators".to_string());
    }
    Ok(trimmed)
}

fn join_sifs_child(parent: &str, name: &str) -> Result<String, String> {
    let name = validate_child_name(name)?;
    let parent = normalize_path(parent);
    Ok(if parent == "/" {
        format!("/{name}")
    } else {
        format!("{parent}/{name}")
    })
}

fn resolve_sifs_destination_path(current_path: &str, input: &str) -> Result<String, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("destination cannot be empty".to_string());
    }
    if trimmed.bytes().any(|byte| byte == 0) {
        return Err("destination cannot contain NUL bytes".to_string());
    }
    let candidate = if trimmed.starts_with('/') || trimmed.starts_with('\\') {
        trimmed.to_string()
    } else {
        let parent = normalize_path(current_path);
        if parent == "/" {
            format!("/{trimmed}")
        } else {
            format!("{parent}/{trimmed}")
        }
    };
    let normalized = normalize_path(&candidate);
    if normalized == "/" {
        return Err("destination must name a file or folder".to_string());
    }
    Ok(normalized)
}

fn resolve_sifs_destination_folder(current_path: &str, input: &str) -> Result<String, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("destination folder cannot be empty".to_string());
    }
    if trimmed.bytes().any(|byte| byte == 0) {
        return Err("destination folder cannot contain NUL bytes".to_string());
    }
    let candidate = if trimmed.starts_with('/') || trimmed.starts_with('\\') {
        trimmed.to_string()
    } else {
        let parent = normalize_path(current_path);
        if parent == "/" {
            format!("/{trimmed}")
        } else {
            format!("{parent}/{trimmed}")
        }
    };
    Ok(normalize_path(&candidate))
}

fn join_local_child(parent: &str, name: &str) -> Result<String, String> {
    let child = validated_local_relative_path(name, "name")?;
    Ok(Path::new(parent).join(child).to_string_lossy().into_owned())
}

fn resolve_local_destination_path(current_path: &str, input: &str) -> Result<String, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("destination cannot be empty".to_string());
    }
    if trimmed.bytes().any(|byte| byte == 0) {
        return Err("destination cannot contain NUL bytes".to_string());
    }
    let path = Path::new(trimmed);
    if path.is_absolute() {
        Ok(path.to_string_lossy().into_owned())
    } else {
        let child = validated_local_relative_path(trimmed, "destination")?;
        Ok(Path::new(current_path)
            .join(child)
            .to_string_lossy()
            .into_owned())
    }
}

fn join_destination_child(
    source: FilesSourceKind,
    parent: &str,
    name: &str,
) -> Result<String, String> {
    match source {
        FilesSourceKind::Sifs => join_sifs_child(parent, name),
        FilesSourceKind::Local => join_local_child(parent, name),
    }
}

fn file_action_target_name(source: FilesSourceKind, path: &str) -> Result<String, String> {
    match source {
        FilesSourceKind::Sifs => {
            let path = normalize_path(path);
            path.rsplit('/')
                .next()
                .filter(|name| !name.is_empty())
                .map(str::to_string)
                .ok_or_else(|| format!("cannot determine item name for {path}"))
        }
        FilesSourceKind::Local => Path::new(path)
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .filter(|name| !name.is_empty())
            .ok_or_else(|| format!("cannot determine item name for {path}")),
    }
}

fn destination_inside_source(source: FilesSourceKind, from: &str, to: &str) -> bool {
    match source {
        FilesSourceKind::Sifs => {
            let from = normalize_path(from);
            let to = normalize_path(to);
            to == from || to.starts_with(&format!("{from}/"))
        }
        FilesSourceKind::Local => {
            let from = Path::new(from);
            let to = Path::new(to);
            to == from || to.starts_with(from)
        }
    }
}

fn validated_local_relative_path(input: &str, label: &str) -> Result<PathBuf, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(format!("{label} cannot be empty"));
    }
    let path = Path::new(trimmed);
    if path.is_absolute() {
        return Err(format!(
            "use a relative {label} inside the current directory"
        ));
    }
    let mut has_name = false;
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Normal(part) => {
                has_name = true;
                normalized.push(part);
            }
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir
            | std::path::Component::RootDir
            | std::path::Component::Prefix(_) => {
                return Err(format!("{label} cannot leave the current directory"));
            }
        }
    }
    if !has_name {
        return Err(format!("{label} cannot be empty"));
    }
    Ok(normalized)
}

fn list_local_dir(path: &str) -> Result<Vec<SifsEntry>, String> {
    let read_dir = fs::read_dir(path).map_err(|error| format!("cannot list {path}: {error}"))?;
    let mut entries = Vec::new();
    for entry in read_dir {
        let entry = entry.map_err(|error| format!("cannot read entry in {path}: {error}"))?;
        let entry_path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| format!("cannot inspect {}: {error}", entry_path.display()))?;
        let metadata = entry.metadata().ok();
        let is_dir = file_type.is_dir();
        let is_link = file_type.is_symlink();
        let size = metadata
            .as_ref()
            .filter(|_| !is_dir)
            .map(|metadata| metadata.len());
        entries.push(SifsEntry {
            name: entry.file_name().to_string_lossy().into_owned(),
            path: entry_path.to_string_lossy().into_owned(),
            is_dir,
            is_link,
            size,
        });
    }
    Ok(sort_file_entries(entries))
}

fn local_details(path: &str) -> Result<LocalObjectDetails, String> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| format!("cannot inspect {path}: {error}"))?;
    let file_type = metadata.file_type();
    let kind = if file_type.is_dir() {
        "directory"
    } else if file_type.is_file() {
        "file"
    } else if file_type.is_symlink() {
        "symlink"
    } else {
        "item"
    }
    .to_string();
    Ok(LocalObjectDetails {
        path: path.to_string(),
        canonical_path: Path::new(path)
            .canonicalize()
            .ok()
            .map(|path| display_local_path(path.to_string_lossy().into_owned())),
        kind,
        size_bytes: file_type.is_file().then_some(metadata.len()),
        readonly: metadata.permissions().readonly(),
        accessed_at_unix_nanos: metadata.accessed().ok().and_then(system_time_unix_nanos),
        modified_at_unix_nanos: metadata.modified().ok().and_then(system_time_unix_nanos),
        created_at_unix_nanos: metadata.created().ok().and_then(system_time_unix_nanos),
    })
}

fn local_read_file_prefix(path: &str, max_bytes: u64) -> Result<SifsFileRead, String> {
    let file = fs::File::open(path).map_err(|error| format!("cannot open {path}: {error}"))?;
    let truncated = file
        .metadata()
        .ok()
        .map(|metadata| metadata.len() > max_bytes)
        .unwrap_or(false);
    let mut reader = file.take(max_bytes);
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .map_err(|error| format!("cannot read {path}: {error}"))?;
    Ok(SifsFileRead { bytes, truncated })
}

fn system_time_unix_nanos(time: std::time::SystemTime) -> Option<u64> {
    time.duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_nanos()).ok())
}

fn local_time_label(nanos: Option<u64>) -> String {
    nanos
        .map(unix_nanos_label)
        .unwrap_or_else(|| "not recorded".to_string())
}

fn local_crumbs(path: &str) -> Vec<(String, String)> {
    let path = Path::new(path);
    let mut out = Vec::new();
    let mut acc = PathBuf::new();
    let mut saw_root = false;

    for component in path.components() {
        match component {
            std::path::Component::Prefix(prefix) => {
                acc.push(prefix.as_os_str());
                let label = prefix.as_os_str().to_string_lossy().into_owned();
                out.push((label.clone(), format!("{label}\\")));
            }
            std::path::Component::RootDir => {
                saw_root = true;
                if acc.as_os_str().is_empty() {
                    acc.push(std::path::MAIN_SEPARATOR.to_string());
                } else {
                    acc.push(std::path::MAIN_SEPARATOR.to_string());
                }
                if out.is_empty() {
                    out.push(("Local".to_string(), acc.to_string_lossy().into_owned()));
                }
            }
            std::path::Component::Normal(part) => {
                acc.push(part);
                let label = part.to_string_lossy().into_owned();
                out.push((
                    crumb_label(&label, None),
                    acc.to_string_lossy().into_owned(),
                ));
            }
            std::path::Component::CurDir | std::path::Component::ParentDir => {}
        }
    }

    if out.is_empty() {
        out.push(("Local".to_string(), path.to_string_lossy().into_owned()));
    } else if !saw_root && out[0].0 != "Local" {
        let target = out[0].1.clone();
        out.insert(0, ("Local".to_string(), target));
    }
    out
}

fn suggested_copy_name(name: &str) -> String {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return "copy".to_string();
    }
    if let Some((stem, ext)) = trimmed.rsplit_once('.')
        && !stem.is_empty()
        && !ext.is_empty()
    {
        return format!("{stem} copy.{ext}");
    }
    format!("{trimmed} copy")
}

fn default_sifs_transfer_destination(parent: &str, name: &str) -> String {
    let name = name.trim();
    if name.is_empty() {
        return normalize_path(parent);
    }
    join_sifs_child(parent, name).unwrap_or_else(|_| {
        let parent = normalize_path(parent);
        if parent == "/" {
            format!("/{name}")
        } else {
            format!("{parent}/{name}")
        }
    })
}

fn default_local_transfer_destination(parent: &str, name: &str) -> String {
    let name = name.trim();
    if name.is_empty() {
        return parent.to_string();
    }
    Path::new(parent).join(name).to_string_lossy().into_owned()
}

fn normalize_jump_path(
    source: FilesSourceKind,
    current_path: &str,
    path: &str,
) -> Result<String, String> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err("path cannot be empty".to_string());
    }
    if trimmed.bytes().any(|byte| byte == 0) {
        return Err("path cannot contain NUL bytes".to_string());
    }
    match source {
        FilesSourceKind::Sifs => Ok(normalize_path(trimmed)),
        FilesSourceKind::Local => {
            let path = Path::new(trimmed);
            if path.is_absolute() {
                normalize_local_path(trimmed)
            } else {
                normalize_local_path(
                    &Path::new(current_path)
                        .join(path)
                        .to_string_lossy()
                        .into_owned(),
                )
            }
        }
    }
}

fn execute_sifs_operation(
    browser: &SifsBrowser,
    operation: FilesOperation,
    current_path: &str,
    progress: Option<&FilesOperationProgress<'_>>,
) -> FilesOperationResult {
    check_operation_cancelled(&operation, progress)?;
    match operation.kind {
        FilesOperationKind::CreateFile => {
            browser
                .create_file(&operation.path)
                .map_err(FilesOperationFailure::Error)?;
            Ok(FilesOperationOutcome {
                refresh_path: current_path.to_string(),
                select_path: Some(operation.path.clone()),
                message: format!("created {}", operation.path),
            })
        }
        FilesOperationKind::CreateFolder => {
            browser
                .create_dir(&operation.path)
                .map_err(FilesOperationFailure::Error)?;
            Ok(FilesOperationOutcome {
                refresh_path: current_path.to_string(),
                select_path: Some(operation.path.clone()),
                message: format!("created {}", operation.path),
            })
        }
        FilesOperationKind::Copy => {
            let target = operation.target.clone().ok_or_else(|| {
                FilesOperationFailure::Error("copy target is missing".to_string())
            })?;
            if operation.directory {
                if destination_inside_source(FilesSourceKind::Sifs, &operation.path, &target) {
                    return Err(FilesOperationFailure::Error(
                        "copy destination cannot be inside the source folder".to_string(),
                    ));
                }
                copy_sifs_directory_with_progress(browser, &operation.path, &target, progress)?;
            } else {
                copy_sifs_file_with_progress(browser, &operation.path, &target, progress)?;
            }
            Ok(FilesOperationOutcome {
                refresh_path: parent_path(&target).unwrap_or_else(|| current_path.to_string()),
                select_path: Some(target.clone()),
                message: format!("copied {} to {target}", operation.path),
            })
        }
        FilesOperationKind::CopyToSifs | FilesOperationKind::CopyToLocal => {
            execute_cross_source_operation(
                FilesSourceKind::Sifs,
                browser,
                operation,
                current_path,
                progress,
            )
        }
        FilesOperationKind::Move => {
            let target = operation.target.clone().ok_or_else(|| {
                FilesOperationFailure::Error("move target is missing".to_string())
            })?;
            browser
                .rename(&operation.path, &target)
                .map_err(FilesOperationFailure::Error)?;
            Ok(FilesOperationOutcome {
                refresh_path: parent_path(&target).unwrap_or_else(|| current_path.to_string()),
                select_path: Some(target.clone()),
                message: format!("moved {} to {target}", operation.path),
            })
        }
        FilesOperationKind::Rename => {
            let target = operation.target.clone().ok_or_else(|| {
                FilesOperationFailure::Error("rename target is missing".to_string())
            })?;
            browser
                .rename(&operation.path, &target)
                .map_err(FilesOperationFailure::Error)?;
            Ok(FilesOperationOutcome {
                refresh_path: current_path.to_string(),
                select_path: Some(target.clone()),
                message: format!("renamed {} to {target}", operation.path),
            })
        }
        FilesOperationKind::Delete => {
            if operation.directory {
                delete_sifs_directory_recursive_with_progress(browser, &operation.path, progress)?;
            } else {
                check_operation_cancelled(&operation, progress)?;
                browser
                    .delete(&operation.path, false)
                    .map_err(FilesOperationFailure::Error)?;
            }
            Ok(FilesOperationOutcome {
                refresh_path: current_path.to_string(),
                select_path: None,
                message: format!("deleted {}", operation.path),
            })
        }
        FilesOperationKind::RefreshPlacement => {
            browser
                .request_maintenance_refresh()
                .map_err(FilesOperationFailure::Error)?;
            let _ = browser.stat(&operation.path);
            Ok(FilesOperationOutcome {
                refresh_path: current_path.to_string(),
                select_path: Some(operation.path.clone()),
                message: format!("requested placement refresh for {}", operation.path),
            })
        }
        FilesOperationKind::RepairReplicas => {
            browser
                .request_maintenance_refresh()
                .map_err(FilesOperationFailure::Error)?;
            let _ = browser.stat(&operation.path);
            Ok(FilesOperationOutcome {
                refresh_path: current_path.to_string(),
                select_path: Some(operation.path.clone()),
                message: format!("requested replica repair for {}", operation.path),
            })
        }
        FilesOperationKind::PinContent => {
            let node_id = browser
                .pin_content(&operation.path)
                .map_err(FilesOperationFailure::Error)?;
            Ok(FilesOperationOutcome {
                refresh_path: current_path.to_string(),
                select_path: Some(operation.path.clone()),
                message: format!("pinned content for {} on {node_id}", operation.path),
            })
        }
        FilesOperationKind::UnpinContent => {
            let removed = browser
                .unpin_content(&operation.path)
                .map_err(FilesOperationFailure::Error)?;
            Ok(FilesOperationOutcome {
                refresh_path: current_path.to_string(),
                select_path: Some(operation.path.clone()),
                message: if removed {
                    format!("removed content pin for {}", operation.path)
                } else {
                    format!("no content pin was recorded for {}", operation.path)
                },
            })
        }
    }
}

fn files_operation_is_cross_source(operation: &FilesOperation) -> bool {
    matches!(
        operation.kind,
        FilesOperationKind::CopyToSifs | FilesOperationKind::CopyToLocal
    )
}

fn files_operation_requires_sifs_browser(
    source: FilesSourceKind,
    operation: &FilesOperation,
) -> bool {
    source == FilesSourceKind::Sifs || files_operation_is_cross_source(operation)
}

fn execute_cross_source_operation(
    source: FilesSourceKind,
    browser: &SifsBrowser,
    operation: FilesOperation,
    current_path: &str,
    progress: Option<&FilesOperationProgress<'_>>,
) -> FilesOperationResult {
    check_operation_cancelled(&operation, progress)?;
    match (source, operation.kind) {
        (FilesSourceKind::Local, FilesOperationKind::CopyToSifs) => {
            let target = operation.target.clone().ok_or_else(|| {
                FilesOperationFailure::Error("SIFS destination is missing".to_string())
            })?;
            if operation.directory {
                copy_local_directory_to_sifs_with_progress(
                    browser,
                    &operation.path,
                    &target,
                    progress,
                )?;
            } else {
                copy_local_file_to_sifs_with_progress(browser, &operation.path, &target, progress)?;
            }
            Ok(FilesOperationOutcome {
                refresh_path: current_path.to_string(),
                select_path: Some(operation.path.clone()),
                message: format!("copied {} to SIFS {target}", operation.path),
            })
        }
        (FilesSourceKind::Sifs, FilesOperationKind::CopyToLocal) => {
            let target = operation.target.clone().ok_or_else(|| {
                FilesOperationFailure::Error("local destination is missing".to_string())
            })?;
            if operation.directory {
                copy_sifs_directory_to_local_with_progress(
                    browser,
                    &operation.path,
                    &target,
                    progress,
                )?;
            } else {
                copy_sifs_file_to_local_with_progress(browser, &operation.path, &target, progress)?;
            }
            Ok(FilesOperationOutcome {
                refresh_path: current_path.to_string(),
                select_path: Some(operation.path.clone()),
                message: format!("downloaded {} to {target}", operation.path),
            })
        }
        (FilesSourceKind::Sifs, FilesOperationKind::CopyToSifs) => Err(
            FilesOperationFailure::Error("copy to SIFS starts from a local selection".to_string()),
        ),
        (FilesSourceKind::Local, FilesOperationKind::CopyToLocal) => Err(
            FilesOperationFailure::Error("download starts from a SIFS selection".to_string()),
        ),
        (_, _) => Err(FilesOperationFailure::Error(
            "operation is not a cross-source copy".to_string(),
        )),
    }
}

fn execute_local_operation(
    operation: FilesOperation,
    current_path: &str,
) -> Result<FilesOperationOutcome, String> {
    execute_local_operation_with_progress(operation, current_path, None)
        .map_err(files_operation_failure_detail)
}

fn execute_local_operation_with_progress(
    operation: FilesOperation,
    current_path: &str,
    progress: Option<&FilesOperationProgress<'_>>,
) -> FilesOperationResult {
    if let Some(progress) = progress
        && progress.cancelled()
    {
        return Err(FilesOperationFailure::Cancelled(
            files_operation_cancelled_detail(&operation),
        ));
    }
    match operation.kind {
        FilesOperationKind::CreateFile => {
            fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&operation.path)
                .map_err(|error| {
                    FilesOperationFailure::Error(format!(
                        "cannot create file {}: {error}",
                        operation.path
                    ))
                })?;
            Ok(FilesOperationOutcome {
                refresh_path: current_path.to_string(),
                select_path: Some(operation.path.clone()),
                message: format!("created {}", operation.path),
            })
        }
        FilesOperationKind::CreateFolder => {
            fs::create_dir(&operation.path).map_err(|error| {
                FilesOperationFailure::Error(format!(
                    "cannot create folder {}: {error}",
                    operation.path
                ))
            })?;
            Ok(FilesOperationOutcome {
                refresh_path: current_path.to_string(),
                select_path: Some(operation.path.clone()),
                message: format!("created {}", operation.path),
            })
        }
        FilesOperationKind::Copy => {
            let target = operation.target.clone().ok_or_else(|| {
                FilesOperationFailure::Error("copy target is missing".to_string())
            })?;
            if operation.directory {
                if destination_inside_source(FilesSourceKind::Local, &operation.path, &target) {
                    return Err(FilesOperationFailure::Error(
                        "copy destination cannot be inside the source folder".to_string(),
                    ));
                }
                copy_local_directory_with_progress(&operation.path, &target, progress)?;
            } else {
                copy_local_file_with_progress(&operation.path, &target, progress)?;
            }
            Ok(FilesOperationOutcome {
                refresh_path: parent_local_path(&target)
                    .unwrap_or_else(|| current_path.to_string()),
                select_path: Some(target.clone()),
                message: format!("copied {} to {target}", operation.path),
            })
        }
        FilesOperationKind::CopyToSifs | FilesOperationKind::CopyToLocal => Err(
            FilesOperationFailure::Error("cross-source copy requires SIFS".to_string()),
        ),
        FilesOperationKind::Move | FilesOperationKind::Rename => {
            let target = operation.target.clone().ok_or_else(|| {
                FilesOperationFailure::Error("target path is missing".to_string())
            })?;
            fs::rename(&operation.path, &target).map_err(|error| {
                FilesOperationFailure::Error(format!(
                    "cannot move {} to {target}: {error}",
                    operation.path
                ))
            })?;
            let action = if operation.kind == FilesOperationKind::Rename {
                "renamed"
            } else {
                "moved"
            };
            Ok(FilesOperationOutcome {
                refresh_path: parent_local_path(&target)
                    .unwrap_or_else(|| current_path.to_string()),
                select_path: Some(target.clone()),
                message: format!("{action} {} to {target}", operation.path),
            })
        }
        FilesOperationKind::Delete => {
            if operation.directory {
                delete_local_directory_recursive_with_progress(&operation.path, progress)?;
            } else {
                fs::remove_file(&operation.path).map_err(|error| {
                    FilesOperationFailure::Error(format!(
                        "cannot delete file {}: {error}",
                        operation.path
                    ))
                })?;
            }
            Ok(FilesOperationOutcome {
                refresh_path: current_path.to_string(),
                select_path: None,
                message: format!("deleted {}", operation.path),
            })
        }
        FilesOperationKind::RefreshPlacement => Err(FilesOperationFailure::Error(
            "placement refresh is only available for SIFS".to_string(),
        )),
        FilesOperationKind::RepairReplicas => Err(FilesOperationFailure::Error(
            "replica repair is only available for SIFS".to_string(),
        )),
        FilesOperationKind::PinContent | FilesOperationKind::UnpinContent => Err(
            FilesOperationFailure::Error("content pinning is only available for SIFS".to_string()),
        ),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FilesCopyPlanItem {
    source: String,
    target: String,
    is_dir: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FilesDeletePlanItem {
    path: String,
    is_dir: bool,
}

fn check_operation_cancelled(
    operation: &FilesOperation,
    progress: Option<&FilesOperationProgress<'_>>,
) -> Result<(), FilesOperationFailure> {
    check_progress_cancelled(progress, files_operation_cancelled_detail(operation))
}

fn check_progress_cancelled(
    progress: Option<&FilesOperationProgress<'_>>,
    detail: String,
) -> Result<(), FilesOperationFailure> {
    if progress.is_some_and(FilesOperationProgress::cancelled) {
        Err(FilesOperationFailure::Cancelled(detail))
    } else {
        Ok(())
    }
}

fn report_indexed_progress(
    progress: Option<&FilesOperationProgress<'_>>,
    action: &str,
    processed: usize,
    total: usize,
    path: &str,
    start: f32,
    span: f32,
) {
    let Some(progress) = progress else {
        return;
    };
    let fraction = if total == 0 {
        Some((start + span).min(0.98))
    } else {
        Some((start + span * (processed as f32 / total as f32)).min(0.98))
    };
    progress.report(format!("{action} {processed} of {total}: {path}"), fraction);
}

const PLAN_SCAN_REPORT_STEP: usize = 32;
const PLAN_SCAN_PROGRESS_START: f32 = 0.08;
const PLAN_SCAN_PROGRESS_END: f32 = 0.12;

fn report_plan_scan_progress(
    progress: Option<&FilesOperationProgress<'_>>,
    scope: &str,
    scanned: usize,
    path: &str,
) {
    let Some(progress) = progress else {
        return;
    };
    if scanned == 0 || scanned % PLAN_SCAN_REPORT_STEP != 0 {
        return;
    }
    let steps = (scanned / PLAN_SCAN_REPORT_STEP).min(8) as f32;
    let fraction = Some((PLAN_SCAN_PROGRESS_START + steps * 0.005).min(PLAN_SCAN_PROGRESS_END));
    progress.report(
        format!("Scanning {scope}: {scanned} items; latest {path}"),
        fraction,
    );
}

fn copy_sifs_file_with_progress(
    browser: &SifsBrowser,
    source_path: &str,
    target_path: &str,
    progress: Option<&FilesOperationProgress<'_>>,
) -> Result<(), FilesOperationFailure> {
    check_progress_cancelled(
        progress,
        format!("cancelled before copying {source_path} to {target_path}"),
    )?;
    if let Some(progress) = progress {
        progress.report(format!("Reading {source_path} from SIFS"), Some(0.42));
    }
    let bytes = browser
        .read_file(source_path)
        .map_err(FilesOperationFailure::Error)?;
    check_progress_cancelled(
        progress,
        format!("cancelled after reading {source_path}; {target_path} was not written"),
    )?;
    if let Some(progress) = progress {
        progress.report(
            format!(
                "Writing {} to {target_path}",
                human_size(Some(bytes.len() as u64))
            ),
            Some(0.78),
        );
    }
    browser
        .write_file(target_path, &bytes)
        .map_err(FilesOperationFailure::Error)
}

fn copy_sifs_directory_with_progress(
    browser: &SifsBrowser,
    source_path: &str,
    target_path: &str,
    progress: Option<&FilesOperationProgress<'_>>,
) -> Result<(), FilesOperationFailure> {
    check_progress_cancelled(
        progress,
        format!("cancelled before copying {source_path} to {target_path}"),
    )?;
    if let Some(progress) = progress {
        progress.report(format!("Scanning SIFS folder {source_path}"), Some(0.08));
    }
    let mut plan = Vec::new();
    collect_sifs_copy_plan(browser, source_path, target_path, &mut plan, progress)?;
    let total = plan.len().max(1);
    for (index, item) in plan.iter().enumerate() {
        check_progress_cancelled(
            progress,
            format!(
                "cancelled after copying {} of {} items to {target_path}; partial target remains",
                index, total
            ),
        )?;
        if item.is_dir {
            browser
                .create_dir_all(&item.target)
                .map_err(FilesOperationFailure::Error)?;
        } else {
            let bytes = browser
                .read_file(&item.source)
                .map_err(FilesOperationFailure::Error)?;
            check_progress_cancelled(
                progress,
                format!(
                    "cancelled after reading {}; partial target remains at {target_path}",
                    item.source
                ),
            )?;
            browser
                .write_file(&item.target, &bytes)
                .map_err(FilesOperationFailure::Error)?;
        }
        report_indexed_progress(
            progress,
            "Copied",
            index + 1,
            total,
            &item.target,
            0.12,
            0.82,
        );
    }
    Ok(())
}

fn collect_sifs_copy_plan(
    browser: &SifsBrowser,
    source_path: &str,
    target_path: &str,
    plan: &mut Vec<FilesCopyPlanItem>,
    progress: Option<&FilesOperationProgress<'_>>,
) -> Result<(), FilesOperationFailure> {
    check_progress_cancelled(progress, format!("cancelled while scanning {source_path}"))?;
    plan.push(FilesCopyPlanItem {
        source: source_path.to_string(),
        target: target_path.to_string(),
        is_dir: true,
    });
    report_plan_scan_progress(progress, "Scanned SIFS copy plan", plan.len(), source_path);
    let entries = browser
        .list_shallow(source_path)
        .map_err(FilesOperationFailure::Error)?;
    for entry in entries {
        check_progress_cancelled(progress, format!("cancelled while scanning {}", entry.path))?;
        let child_target =
            join_sifs_child(target_path, &entry.name).map_err(FilesOperationFailure::Error)?;
        if entry.is_dir {
            collect_sifs_copy_plan(browser, &entry.path, &child_target, plan, progress)?;
        } else {
            plan.push(FilesCopyPlanItem {
                source: entry.path.clone(),
                target: child_target,
                is_dir: false,
            });
            report_plan_scan_progress(progress, "Scanned SIFS copy plan", plan.len(), &entry.path);
        }
    }
    Ok(())
}

fn copy_local_file_to_sifs_with_progress(
    browser: &SifsBrowser,
    source_path: &str,
    target_path: &str,
    progress: Option<&FilesOperationProgress<'_>>,
) -> Result<(), FilesOperationFailure> {
    check_progress_cancelled(
        progress,
        format!("cancelled before copying {source_path} to SIFS {target_path}"),
    )?;
    let mut source = fs::File::open(source_path).map_err(|error| {
        FilesOperationFailure::Error(format!("cannot open {source_path}: {error}"))
    })?;
    let total_bytes = source.metadata().ok().map(|metadata| metadata.len());
    if let Some(progress) = progress {
        progress.report(format!("Reading {source_path} for SIFS import"), Some(0.18));
    }
    let mut bytes = Vec::new();
    source.read_to_end(&mut bytes).map_err(|error| {
        FilesOperationFailure::Error(format!("cannot read {source_path}: {error}"))
    })?;
    check_progress_cancelled(
        progress,
        format!("cancelled after reading {source_path}; SIFS {target_path} was not written"),
    )?;
    if let Some(progress) = progress {
        let size = total_bytes.unwrap_or(bytes.len() as u64);
        progress.report(
            format!("Writing {} to SIFS {target_path}", human_size(Some(size))),
            Some(0.78),
        );
    }
    browser
        .write_file(target_path, &bytes)
        .map_err(FilesOperationFailure::Error)
}

fn copy_local_directory_to_sifs_with_progress(
    browser: &SifsBrowser,
    source_path: &str,
    target_path: &str,
    progress: Option<&FilesOperationProgress<'_>>,
) -> Result<(), FilesOperationFailure> {
    check_progress_cancelled(
        progress,
        format!("cancelled before copying {source_path} to SIFS {target_path}"),
    )?;
    if let Some(progress) = progress {
        progress.report(
            format!("Scanning folder {source_path} for SIFS import"),
            Some(0.08),
        );
    }
    let mut plan = Vec::new();
    collect_local_to_sifs_copy_plan(Path::new(source_path), target_path, &mut plan, progress)?;
    let total = plan.len().max(1);
    for (index, item) in plan.iter().enumerate() {
        check_progress_cancelled(
            progress,
            format!(
                "cancelled after copying {} of {} items to SIFS {target_path}; partial target remains",
                index, total
            ),
        )?;
        if item.is_dir {
            browser
                .create_dir_all(&item.target)
                .map_err(FilesOperationFailure::Error)?;
        } else {
            copy_local_file_to_sifs_with_progress(browser, &item.source, &item.target, None)?;
        }
        report_indexed_progress(
            progress,
            "Imported",
            index + 1,
            total,
            &item.target,
            0.12,
            0.82,
        );
    }
    Ok(())
}

fn collect_local_to_sifs_copy_plan(
    source: &Path,
    target_sifs_path: &str,
    plan: &mut Vec<FilesCopyPlanItem>,
    progress: Option<&FilesOperationProgress<'_>>,
) -> Result<(), FilesOperationFailure> {
    check_progress_cancelled(
        progress,
        format!("cancelled while scanning {}", source.display()),
    )?;
    let metadata = fs::symlink_metadata(source).map_err(|error| {
        FilesOperationFailure::Error(format!("cannot inspect {}: {error}", source.display()))
    })?;
    if metadata.file_type().is_dir() {
        let source_display = source.to_string_lossy().into_owned();
        plan.push(FilesCopyPlanItem {
            source: source_display.clone(),
            target: target_sifs_path.to_string(),
            is_dir: true,
        });
        report_plan_scan_progress(progress, "Scanned import plan", plan.len(), &source_display);
        let read_dir = fs::read_dir(source).map_err(|error| {
            FilesOperationFailure::Error(format!("cannot list {}: {error}", source.display()))
        })?;
        for entry in read_dir {
            let entry = entry.map_err(|error| {
                FilesOperationFailure::Error(format!(
                    "cannot read entry in {}: {error}",
                    source.display()
                ))
            })?;
            let name = entry.file_name().to_string_lossy().into_owned();
            let child_target =
                join_sifs_child(target_sifs_path, &name).map_err(FilesOperationFailure::Error)?;
            collect_local_to_sifs_copy_plan(&entry.path(), &child_target, plan, progress)?;
        }
    } else {
        let source_display = source.to_string_lossy().into_owned();
        plan.push(FilesCopyPlanItem {
            source: source_display.clone(),
            target: target_sifs_path.to_string(),
            is_dir: false,
        });
        report_plan_scan_progress(progress, "Scanned import plan", plan.len(), &source_display);
    }
    Ok(())
}

fn copy_sifs_file_to_local_with_progress(
    browser: &SifsBrowser,
    source_path: &str,
    target_path: &str,
    progress: Option<&FilesOperationProgress<'_>>,
) -> Result<(), FilesOperationFailure> {
    check_progress_cancelled(
        progress,
        format!("cancelled before downloading {source_path} to {target_path}"),
    )?;
    if let Some(progress) = progress {
        progress.report(format!("Reading {source_path} from SIFS"), Some(0.42));
    }
    let bytes = browser
        .read_file(source_path)
        .map_err(FilesOperationFailure::Error)?;
    check_progress_cancelled(
        progress,
        format!("cancelled after reading {source_path}; {target_path} was not written"),
    )?;
    if let Some(progress) = progress {
        progress.report(
            format!(
                "Writing {} to {target_path}",
                human_size(Some(bytes.len() as u64))
            ),
            Some(0.78),
        );
    }
    write_bytes_to_local_file(target_path, &bytes)
}

fn copy_sifs_directory_to_local_with_progress(
    browser: &SifsBrowser,
    source_path: &str,
    target_path: &str,
    progress: Option<&FilesOperationProgress<'_>>,
) -> Result<(), FilesOperationFailure> {
    check_progress_cancelled(
        progress,
        format!("cancelled before downloading {source_path} to {target_path}"),
    )?;
    if let Some(progress) = progress {
        progress.report(
            format!("Scanning SIFS folder {source_path} for download"),
            Some(0.08),
        );
    }
    let mut plan = Vec::new();
    collect_sifs_to_local_copy_plan(
        browser,
        source_path,
        Path::new(target_path),
        &mut plan,
        progress,
    )?;
    let total = plan.len().max(1);
    for (index, item) in plan.iter().enumerate() {
        check_progress_cancelled(
            progress,
            format!(
                "cancelled after downloading {} of {} items to {target_path}; partial target remains",
                index, total
            ),
        )?;
        if item.is_dir {
            fs::create_dir_all(&item.target).map_err(|error| {
                FilesOperationFailure::Error(format!(
                    "cannot create folder {}: {error}",
                    item.target
                ))
            })?;
        } else {
            copy_sifs_file_to_local_with_progress(browser, &item.source, &item.target, None)?;
        }
        report_indexed_progress(
            progress,
            "Downloaded",
            index + 1,
            total,
            &item.target,
            0.12,
            0.82,
        );
    }
    Ok(())
}

fn collect_sifs_to_local_copy_plan(
    browser: &SifsBrowser,
    source_path: &str,
    target: &Path,
    plan: &mut Vec<FilesCopyPlanItem>,
    progress: Option<&FilesOperationProgress<'_>>,
) -> Result<(), FilesOperationFailure> {
    check_progress_cancelled(progress, format!("cancelled while scanning {source_path}"))?;
    plan.push(FilesCopyPlanItem {
        source: source_path.to_string(),
        target: target.to_string_lossy().into_owned(),
        is_dir: true,
    });
    report_plan_scan_progress(progress, "Scanned download plan", plan.len(), source_path);
    let entries = browser
        .list_shallow(source_path)
        .map_err(FilesOperationFailure::Error)?;
    for entry in entries {
        check_progress_cancelled(progress, format!("cancelled while scanning {}", entry.path))?;
        let child_target = target.join(&entry.name);
        if entry.is_dir {
            collect_sifs_to_local_copy_plan(browser, &entry.path, &child_target, plan, progress)?;
        } else {
            plan.push(FilesCopyPlanItem {
                source: entry.path.clone(),
                target: child_target.to_string_lossy().into_owned(),
                is_dir: false,
            });
            report_plan_scan_progress(progress, "Scanned download plan", plan.len(), &entry.path);
        }
    }
    Ok(())
}

fn write_bytes_to_local_file(target_path: &str, bytes: &[u8]) -> Result<(), FilesOperationFailure> {
    if let Some(parent) = Path::new(target_path).parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|error| {
            FilesOperationFailure::Error(format!(
                "cannot create destination folder {}: {error}",
                parent.display()
            ))
        })?;
    }
    let mut target_file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target_path)
        .map_err(|error| {
            FilesOperationFailure::Error(format!("cannot create {target_path}: {error}"))
        })?;
    target_file.write_all(bytes).map_err(|error| {
        FilesOperationFailure::Error(format!("cannot write {target_path}: {error}"))
    })?;
    target_file.flush().map_err(|error| {
        FilesOperationFailure::Error(format!("cannot finalize {target_path}: {error}"))
    })
}

fn delete_sifs_directory_recursive_with_progress(
    browser: &SifsBrowser,
    path: &str,
    progress: Option<&FilesOperationProgress<'_>>,
) -> Result<(), FilesOperationFailure> {
    if let Some(progress) = progress {
        progress.report(
            format!("Scanning SIFS folder {path} for delete"),
            Some(0.08),
        );
    }
    let mut plan = Vec::new();
    collect_sifs_delete_plan(browser, path, &mut plan, progress)?;
    let total = plan.len().max(1);
    for (index, item) in plan.iter().enumerate() {
        check_progress_cancelled(
            progress,
            format!(
                "cancelled after deleting {} of {} items from {path}",
                index, total
            ),
        )?;
        browser
            .delete(&item.path, item.is_dir)
            .map_err(FilesOperationFailure::Error)?;
        report_indexed_progress(
            progress,
            "Deleted",
            index + 1,
            total,
            &item.path,
            0.12,
            0.82,
        );
    }
    Ok(())
}

fn collect_sifs_delete_plan(
    browser: &SifsBrowser,
    path: &str,
    plan: &mut Vec<FilesDeletePlanItem>,
    progress: Option<&FilesOperationProgress<'_>>,
) -> Result<(), FilesOperationFailure> {
    check_progress_cancelled(progress, format!("cancelled while scanning {path}"))?;
    let entries = browser
        .list_shallow(path)
        .map_err(FilesOperationFailure::Error)?;
    for entry in entries {
        check_progress_cancelled(progress, format!("cancelled while scanning {}", entry.path))?;
        if entry.is_dir {
            collect_sifs_delete_plan(browser, &entry.path, plan, progress)?;
        } else {
            plan.push(FilesDeletePlanItem {
                path: entry.path.clone(),
                is_dir: false,
            });
            report_plan_scan_progress(
                progress,
                "Scanned SIFS delete plan",
                plan.len(),
                &entry.path,
            );
        }
    }
    plan.push(FilesDeletePlanItem {
        path: path.to_string(),
        is_dir: true,
    });
    report_plan_scan_progress(progress, "Scanned SIFS delete plan", plan.len(), path);
    Ok(())
}

fn copy_local_directory_with_progress(
    source_path: &str,
    target_path: &str,
    progress: Option<&FilesOperationProgress<'_>>,
) -> Result<(), FilesOperationFailure> {
    check_progress_cancelled(
        progress,
        format!("cancelled before copying {source_path} to {target_path}"),
    )?;
    if let Some(progress) = progress {
        progress.report(format!("Scanning folder {source_path}"), Some(0.08));
    }
    let mut plan = Vec::new();
    collect_local_copy_plan(
        Path::new(source_path),
        Path::new(target_path),
        &mut plan,
        progress,
    )?;
    let total = plan.len().max(1);
    for (index, item) in plan.iter().enumerate() {
        check_progress_cancelled(
            progress,
            format!(
                "cancelled after copying {} of {} items to {target_path}; partial target remains",
                index, total
            ),
        )?;
        if item.is_dir {
            fs::create_dir_all(&item.target).map_err(|error| {
                FilesOperationFailure::Error(format!(
                    "cannot create folder {}: {error}",
                    item.target
                ))
            })?;
        } else {
            copy_local_file_with_progress(&item.source, &item.target, None)?;
        }
        report_indexed_progress(
            progress,
            "Copied",
            index + 1,
            total,
            &item.target,
            0.12,
            0.82,
        );
    }
    Ok(())
}

fn collect_local_copy_plan(
    source: &Path,
    target: &Path,
    plan: &mut Vec<FilesCopyPlanItem>,
    progress: Option<&FilesOperationProgress<'_>>,
) -> Result<(), FilesOperationFailure> {
    check_progress_cancelled(
        progress,
        format!("cancelled while scanning {}", source.display()),
    )?;
    let metadata = fs::symlink_metadata(source).map_err(|error| {
        FilesOperationFailure::Error(format!("cannot inspect {}: {error}", source.display()))
    })?;
    if metadata.file_type().is_dir() {
        let source_display = source.to_string_lossy().into_owned();
        plan.push(FilesCopyPlanItem {
            source: source_display.clone(),
            target: target.to_string_lossy().into_owned(),
            is_dir: true,
        });
        report_plan_scan_progress(
            progress,
            "Scanned local copy plan",
            plan.len(),
            &source_display,
        );
        let read_dir = fs::read_dir(source).map_err(|error| {
            FilesOperationFailure::Error(format!("cannot list {}: {error}", source.display()))
        })?;
        for entry in read_dir {
            let entry = entry.map_err(|error| {
                FilesOperationFailure::Error(format!(
                    "cannot read entry in {}: {error}",
                    source.display()
                ))
            })?;
            collect_local_copy_plan(
                &entry.path(),
                &target.join(entry.file_name()),
                plan,
                progress,
            )?;
        }
    } else {
        let source_display = source.to_string_lossy().into_owned();
        plan.push(FilesCopyPlanItem {
            source: source_display.clone(),
            target: target.to_string_lossy().into_owned(),
            is_dir: false,
        });
        report_plan_scan_progress(
            progress,
            "Scanned local copy plan",
            plan.len(),
            &source_display,
        );
    }
    Ok(())
}

fn delete_local_directory_recursive_with_progress(
    path: &str,
    progress: Option<&FilesOperationProgress<'_>>,
) -> Result<(), FilesOperationFailure> {
    if let Some(progress) = progress {
        progress.report(format!("Scanning folder {path} for delete"), Some(0.08));
    }
    let mut plan = Vec::new();
    collect_local_delete_plan(Path::new(path), &mut plan, progress)?;
    let total = plan.len().max(1);
    for (index, item) in plan.iter().enumerate() {
        check_progress_cancelled(
            progress,
            format!(
                "cancelled after deleting {} of {} items from {path}",
                index, total
            ),
        )?;
        if item.is_dir {
            fs::remove_dir(&item.path).map_err(|error| {
                FilesOperationFailure::Error(format!("cannot delete folder {}: {error}", item.path))
            })?;
        } else {
            fs::remove_file(&item.path).map_err(|error| {
                FilesOperationFailure::Error(format!("cannot delete file {}: {error}", item.path))
            })?;
        }
        report_indexed_progress(
            progress,
            "Deleted",
            index + 1,
            total,
            &item.path,
            0.12,
            0.82,
        );
    }
    Ok(())
}

fn collect_local_delete_plan(
    path: &Path,
    plan: &mut Vec<FilesDeletePlanItem>,
    progress: Option<&FilesOperationProgress<'_>>,
) -> Result<(), FilesOperationFailure> {
    check_progress_cancelled(
        progress,
        format!("cancelled while scanning {}", path.display()),
    )?;
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        FilesOperationFailure::Error(format!("cannot inspect {}: {error}", path.display()))
    })?;
    if metadata.file_type().is_dir() {
        let read_dir = fs::read_dir(path).map_err(|error| {
            FilesOperationFailure::Error(format!("cannot list {}: {error}", path.display()))
        })?;
        for entry in read_dir {
            let entry = entry.map_err(|error| {
                FilesOperationFailure::Error(format!(
                    "cannot read entry in {}: {error}",
                    path.display()
                ))
            })?;
            collect_local_delete_plan(&entry.path(), plan, progress)?;
        }
        let path_display = path.to_string_lossy().into_owned();
        plan.push(FilesDeletePlanItem {
            path: path_display.clone(),
            is_dir: true,
        });
        report_plan_scan_progress(
            progress,
            "Scanned local delete plan",
            plan.len(),
            &path_display,
        );
    } else {
        let path_display = path.to_string_lossy().into_owned();
        plan.push(FilesDeletePlanItem {
            path: path_display.clone(),
            is_dir: false,
        });
        report_plan_scan_progress(
            progress,
            "Scanned local delete plan",
            plan.len(),
            &path_display,
        );
    }
    Ok(())
}

fn copy_local_file_with_progress(
    source_path: &str,
    target_path: &str,
    progress: Option<&FilesOperationProgress<'_>>,
) -> Result<(), FilesOperationFailure> {
    let mut source = fs::File::open(source_path).map_err(|error| {
        FilesOperationFailure::Error(format!("cannot open {source_path}: {error}"))
    })?;
    let total_bytes = source.metadata().ok().map(|metadata| metadata.len());
    if let Some(parent) = Path::new(target_path).parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|error| {
            FilesOperationFailure::Error(format!(
                "cannot create destination folder {}: {error}",
                parent.display()
            ))
        })?;
    }
    let mut target_file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target_path)
        .map_err(|error| {
            FilesOperationFailure::Error(format!("cannot create {target_path}: {error}"))
        })?;

    if let Some(progress) = progress {
        progress.report(
            format!(
                "Copying {} to {target_path}",
                total_bytes
                    .map_or_else(|| source_path.to_string(), |bytes| human_size(Some(bytes)))
            ),
            Some(0.08),
        );
    }

    const BUFFER_SIZE: usize = 1024 * 1024;
    const REPORT_STEP: u64 = 4 * 1024 * 1024;
    let mut buffer = vec![0_u8; BUFFER_SIZE];
    let mut copied = 0_u64;
    let mut next_report = REPORT_STEP;

    loop {
        if let Some(progress) = progress
            && progress.cancelled()
        {
            drop(target_file);
            let _ = fs::remove_file(target_path);
            return Err(FilesOperationFailure::Cancelled(format!(
                "cancelled after copying {} to {target_path}",
                human_size(Some(copied))
            )));
        }

        let read = source.read(&mut buffer).map_err(|error| {
            FilesOperationFailure::Error(format!("cannot read {source_path}: {error}"))
        })?;
        if read == 0 {
            break;
        }
        target_file.write_all(&buffer[..read]).map_err(|error| {
            FilesOperationFailure::Error(format!("cannot write {target_path}: {error}"))
        })?;
        copied += read as u64;

        if let Some(progress) = progress
            && (copied >= next_report || total_bytes.is_some_and(|total| copied >= total))
        {
            let fraction = total_bytes
                .filter(|total| *total > 0)
                .map(|total| 0.08 + 0.86 * (copied as f32 / total as f32));
            let detail = match total_bytes {
                Some(total) => format!(
                    "Copied {} of {} to {target_path}",
                    human_size(Some(copied)),
                    human_size(Some(total))
                ),
                None => format!("Copied {} to {target_path}", human_size(Some(copied))),
            };
            progress.report(detail, fraction);
            next_report = copied.saturating_add(REPORT_STEP);
        }
    }

    target_file.flush().map_err(|error| {
        FilesOperationFailure::Error(format!("cannot finalize {target_path}: {error}"))
    })?;
    Ok(())
}

fn files_operation_failure_detail(failure: FilesOperationFailure) -> String {
    match failure {
        FilesOperationFailure::Cancelled(detail) | FilesOperationFailure::Error(detail) => detail,
    }
}

fn files_operation_cancelled_detail(operation: &FilesOperation) -> String {
    match operation.kind {
        FilesOperationKind::CreateFile | FilesOperationKind::CreateFolder => {
            format!("cancelled before creating {}", operation.path)
        }
        FilesOperationKind::Copy => {
            let target = operation.target.as_deref().unwrap_or("target");
            format!("cancelled before copying {} to {target}", operation.path)
        }
        FilesOperationKind::CopyToSifs => {
            let target = operation.target.as_deref().unwrap_or("SIFS target");
            format!(
                "cancelled before copying {} to SIFS {target}",
                operation.path
            )
        }
        FilesOperationKind::CopyToLocal => {
            let target = operation.target.as_deref().unwrap_or("local target");
            format!(
                "cancelled before downloading {} to {target}",
                operation.path
            )
        }
        FilesOperationKind::Move => {
            let target = operation.target.as_deref().unwrap_or("target");
            format!("cancelled before moving {} to {target}", operation.path)
        }
        FilesOperationKind::Rename => {
            let target = operation.target.as_deref().unwrap_or("target");
            format!("cancelled before renaming {} to {target}", operation.path)
        }
        FilesOperationKind::Delete => format!("cancelled before deleting {}", operation.path),
        FilesOperationKind::RefreshPlacement => {
            format!(
                "cancelled before refreshing placement for {}",
                operation.path
            )
        }
        FilesOperationKind::RepairReplicas => {
            format!("cancelled before repairing replicas for {}", operation.path)
        }
        FilesOperationKind::PinContent => {
            format!("cancelled before pinning content for {}", operation.path)
        }
        FilesOperationKind::UnpinContent => {
            format!("cancelled before unpinning content for {}", operation.path)
        }
    }
}

fn operation_progress_label(
    source: FilesSourceKind,
    operation: &FilesOperation,
    phase: &str,
) -> String {
    let scope = source.operation_scope();
    match operation.kind {
        FilesOperationKind::CreateFile | FilesOperationKind::CreateFolder => {
            format!("{phase} create in {scope}: {}", operation.path)
        }
        FilesOperationKind::Copy => {
            let target = operation.target.as_deref().unwrap_or("target");
            format!("{phase} copy in {scope}: {} -> {target}", operation.path)
        }
        FilesOperationKind::CopyToSifs => {
            let target = operation.target.as_deref().unwrap_or("SIFS target");
            format!("{phase} SIFS import: {} -> {target}", operation.path)
        }
        FilesOperationKind::CopyToLocal => {
            let target = operation.target.as_deref().unwrap_or("local target");
            format!("{phase} SIFS download: {} -> {target}", operation.path)
        }
        FilesOperationKind::Move => {
            let target = operation.target.as_deref().unwrap_or("target");
            format!("{phase} move in {scope}: {} -> {target}", operation.path)
        }
        FilesOperationKind::Rename => {
            let target = operation.target.as_deref().unwrap_or("target");
            format!("{phase} rename in {scope}: {} -> {target}", operation.path)
        }
        FilesOperationKind::Delete => format!("{phase} delete in {scope}: {}", operation.path),
        FilesOperationKind::RefreshPlacement => {
            format!("{phase} placement refresh in {scope}: {}", operation.path)
        }
        FilesOperationKind::RepairReplicas => {
            format!("{phase} replica repair in {scope}: {}", operation.path)
        }
        FilesOperationKind::PinContent => {
            format!("{phase} content pin in {scope}: {}", operation.path)
        }
        FilesOperationKind::UnpinContent => {
            format!("{phase} content unpin in {scope}: {}", operation.path)
        }
    }
}

// ---------------------------------------------------------------------------
// Theme reader + small helpers (mirrors the Settings tab).
// ---------------------------------------------------------------------------

/// Clone the shared theme reader into a fresh `Fn() -> DefaultTheme` for one control.
fn clone_reader(reader: &ThemeReader) -> impl Fn() -> DefaultTheme + 'static {
    let reader = Rc::clone(reader);
    move || reader()
}

/// Resolve the app chrome palette for the current theme (used by the `Label` color readers and the
/// `InspectorView` so muted/accent text matches the bespoke chrome).
fn palette(shared: &Shared) -> Palette {
    Palette::from_theme(&shared.theme(), widgets::theme_is_dark(shared))
}

fn secondary_button_theme(reader: &ThemeReader) -> impl Fn() -> DefaultTheme + 'static {
    let reader = Rc::clone(reader);
    move || {
        let mut theme = reader();
        theme.palette.accent = theme.palette.control;
        theme.palette.accent_hover = theme.palette.control_hover;
        theme.palette.accent_pressed = theme.palette.control_active;
        theme.palette.accent_border = theme.palette.border;
        theme.palette.accent_border_hover = theme.palette.border_hover;
        theme.palette.accent_border_focus = theme.palette.border_focus;
        theme.palette.accent_text = theme.palette.text;
        theme
    }
}

fn human_size(size: Option<u64>) -> String {
    match size {
        None => "—".to_string(),
        Some(bytes) => {
            const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
            let mut value = bytes as f64;
            let mut unit = 0;
            while value >= 1024.0 && unit < UNITS.len() - 1 {
                value /= 1024.0;
                unit += 1;
            }
            if unit == 0 {
                format!("{bytes} B")
            } else {
                format!("{value:.1} {}", UNITS[unit])
            }
        }
    }
}

fn short(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let tail: String = s
            .chars()
            .rev()
            .take(max - 1)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        format!("…{tail}")
    }
}

fn middle_short(s: &str, max: usize) -> String {
    let len = s.chars().count();
    if len <= max {
        return s.to_string();
    }
    if max <= 1 {
        return "…".to_string();
    }
    let kept = max - 1;
    let head_len = kept / 2;
    let tail_len = kept - head_len;
    let head: String = s.chars().take(head_len).collect();
    let tail: String = s
        .chars()
        .rev()
        .take(tail_len)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("{head}…{tail}")
}

fn display_name_for_width(name: &str, width: f32, avg_char_width: f32) -> String {
    if width <= 0.0 {
        return String::new();
    }
    let max_chars = (width / avg_char_width.max(1.0)).floor() as usize;
    let max_chars = max_chars.clamp(4, 72);
    middle_short(name, max_chars)
}

fn crumb_label(segment: &str, parent: Option<&str>) -> String {
    if parent == Some("nodes") && looks_like_opaque_id(segment) {
        return format!("node {}", middle_short(segment, 16));
    }
    if segment.chars().count() > 22 {
        middle_short(segment, 22)
    } else {
        segment.to_string()
    }
}

fn looks_like_opaque_id(segment: &str) -> bool {
    segment.chars().count() >= 20
        && segment
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
}

fn sifs_unavailable_summary(error: &str) -> &'static str {
    let lower = error.to_ascii_lowercase();
    if lower.contains("device authorization") || lower.contains("denied") {
        "SIFS access needs authorization."
    } else if lower.contains("requires a daemon") || lower.contains("daemon connection") {
        "SIFS needs a daemon connection."
    } else if lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("did not finish")
    {
        "SIFS did not respond in time."
    } else if lower.contains("worker stopped") {
        "SIFS browser stopped unexpectedly."
    } else {
        "SIFS could not open through the local cluster service."
    }
}

fn source_unavailable_summary(source: FilesSourceKind, error: &str) -> String {
    match source {
        FilesSourceKind::Sifs => sifs_unavailable_summary(error).to_string(),
        FilesSourceKind::Local => format!("Local filesystem error: {error}"),
    }
}

fn sifs_task_failure_summary(error: &str) -> String {
    match sifs_unavailable_summary(error) {
        "SIFS access needs authorization." => "Authorize SIFS in Connection".to_string(),
        "SIFS needs a daemon connection." => "Check daemon in Connection".to_string(),
        "SIFS did not respond in time." => "Retry or inspect Cluster health".to_string(),
        "SIFS browser stopped unexpectedly." => "Retry the SIFS operation".to_string(),
        _ => "Inspect metadata or Cluster diagnostics".to_string(),
    }
}

fn sifs_metadata_error_next_step(error: &str) -> String {
    match sifs_unavailable_summary(error) {
        "SIFS access needs authorization." => {
            "Authorize SIFS in Connection, then refresh metadata".to_string()
        }
        "SIFS needs a daemon connection." => {
            "Check daemon in Connection, then retry metadata".to_string()
        }
        "SIFS did not respond in time." => {
            "Retry metadata, then inspect Cluster health if it stalls".to_string()
        }
        "SIFS browser stopped unexpectedly." => "Retry the SIFS metadata request".to_string(),
        _ => "Show metadata details, then refresh placement".to_string(),
    }
}

fn sifs_metadata_error_needs_connection(error: &str) -> bool {
    matches!(
        sifs_unavailable_summary(error),
        "SIFS access needs authorization." | "SIFS needs a daemon connection."
    )
}

fn file_preview_error_display(source: FilesSourceKind, error: &str) -> String {
    if source != FilesSourceKind::Sifs {
        return error.to_string();
    }
    match sifs_unavailable_summary(error) {
        "SIFS access needs authorization." => {
            "SIFS preview needs authorization. Open Connection settings, authorize this device, then retry preview.".to_string()
        }
        "SIFS needs a daemon connection." => {
            "SIFS preview needs a daemon connection. Check Connection settings, then retry preview."
                .to_string()
        }
        "SIFS did not respond in time." => {
            "SIFS preview timed out. Retry preview or inspect Cluster health.".to_string()
        }
        "SIFS browser stopped unexpectedly." => {
            "SIFS preview worker stopped. Retry preview after the Files tab reconnects.".to_string()
        }
        _ => error.to_string(),
    }
}

fn files_task_detail_display(task: &FilesTask) -> String {
    if task.state == FilesTaskState::Failed {
        match task.source {
            FilesSourceKind::Sifs => sifs_task_failure_summary(&task.detail),
            FilesSourceKind::Local => format!("Local filesystem error: {}", task.detail),
        }
    } else {
        task.detail.clone()
    }
}

fn files_task_detail_display_for_width(task: &FilesTask, width: f32) -> String {
    display_name_for_width(&files_task_detail_display(task), width, 6.2)
}

fn sifs_metadata_status_label(
    loading: bool,
    error: Option<&str>,
    details: Option<&SifsObjectDetails>,
) -> &'static str {
    if loading {
        "Loading metadata"
    } else if error.is_some() {
        "Metadata unavailable"
    } else if details.is_some() {
        "Metadata current"
    } else {
        "Metadata not loaded"
    }
}

fn sifs_metadata_status_color(
    pal: &Palette,
    loading: bool,
    error: Option<&str>,
    details: Option<&SifsObjectDetails>,
) -> Color {
    if loading {
        pal.accent
    } else if error.is_some() {
        pal.warn
    } else if details.is_some() {
        pal.good
    } else {
        pal.text_muted
    }
}

fn local_metadata_status_label(
    loading: bool,
    error: Option<&str>,
    details: Option<&LocalObjectDetails>,
) -> &'static str {
    if loading {
        "Loading metadata"
    } else if error.is_some() {
        "Metadata unavailable"
    } else if details.is_some() {
        "Metadata current"
    } else {
        "Metadata not loaded"
    }
}

fn local_metadata_status_color(
    pal: &Palette,
    loading: bool,
    error: Option<&str>,
    details: Option<&LocalObjectDetails>,
) -> Color {
    if loading {
        pal.accent
    } else if error.is_some() {
        pal.warn
    } else if details.is_some() {
        pal.good
    } else {
        pal.text_muted
    }
}

/// The right-aligned metadata for a listing row: `folder` for directories, else a human size.
fn entry_meta(entry: &SifsEntry) -> String {
    if entry.is_dir {
        "folder".to_string()
    } else if entry.is_link {
        "link".to_string()
    } else {
        human_size(entry.size)
    }
}

fn entry_kind_label(entry: &SifsEntry) -> &'static str {
    if entry.is_dir {
        "Folder"
    } else if entry.is_link {
        "Link"
    } else {
        "File"
    }
}

fn sort_file_entries(mut entries: Vec<SifsEntry>) -> Vec<SifsEntry> {
    entries.sort_by(|a, b| {
        (b.is_dir as u8)
            .cmp(&(a.is_dir as u8))
            .then_with(|| a.name.to_lowercase().cmp(&b.name.to_lowercase()))
    });
    entries
}

fn entry_icon(entry: &SifsEntry) -> IconGlyph {
    if entry.is_dir {
        IconGlyph::Folder
    } else if entry.is_link {
        IconGlyph::Link
    } else {
        IconGlyph::File
    }
}

fn details_size(entry: &SifsEntry, details: Option<&SifsObjectDetails>) -> String {
    human_size(
        details
            .and_then(|details| details.size_bytes)
            .or(entry.size),
    )
}

fn details_locations(details: &SifsObjectDetails) -> String {
    if details.current_locations.is_empty() {
        "none recorded".to_string()
    } else {
        details
            .current_locations
            .iter()
            .map(|node| short(node, 20))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn sifs_pin_summary(details: Option<&SifsObjectDetails>) -> String {
    let Some(details) = details else {
        return "pin state not loaded".to_string();
    };
    if details.local_pins.is_empty() {
        return "not pinned on this node".to_string();
    }
    details
        .local_pins
        .iter()
        .map(|pin| format!("{} pin on {}", pin.mode, short(&pin.pinned_by_node_id, 20)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn sifs_pin_detail_summary(details: &SifsObjectDetails) -> String {
    if details.local_pins.is_empty() {
        return "none recorded".to_string();
    }
    details
        .local_pins
        .iter()
        .map(|pin| {
            let created = pin
                .created_at_unix_nanos
                .map(unix_nanos_label)
                .unwrap_or_else(|| "unknown time".to_string());
            format!(
                "{} on {} since {}",
                pin.mode,
                short(&pin.pinned_by_node_id, 24),
                created
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn short_node_label(node_id: &str) -> String {
    if node_id.is_empty() {
        "local".to_string()
    } else {
        short(node_id, 18)
    }
}

fn sifs_replication_summary(details: Option<&SifsObjectDetails>, clustered: bool) -> String {
    let Some(details) = details else {
        return if clustered {
            "metadata not loaded; refresh placement to retry".to_string()
        } else {
            "local node only".to_string()
        };
    };
    let count = details.current_locations.len();
    if details.policy == "local-owned" || details.policy == "ephemeral-local" {
        return "local placement policy".to_string();
    }
    if details.policy.starts_with("replicated") {
        return match count {
            0 => "replication pending; no durable location recorded".to_string(),
            1 => "one durable location recorded; replica repair may still be pending".to_string(),
            count => format!("{count} durable locations recorded"),
        };
    }
    if count == 0 {
        format!("{}; no durable location recorded", details.policy)
    } else {
        format!("{}; {} location(s) recorded", details.policy, count)
    }
}

fn sifs_health_summary(details: Option<&SifsObjectDetails>) -> String {
    let Some(health) = details.and_then(|details| details.health.as_ref()) else {
        return "operator health not loaded".to_string();
    };
    if health.open_conflict_count > 0 {
        return format!(
            "{}; {} open",
            health.agent_state,
            plural(health.open_conflict_count, "conflict")
        );
    }
    if health.pending_repair_count > 0 {
        return format!(
            "{}; {}",
            health.agent_state,
            plural(health.pending_repair_count, "missing replica")
        );
    }
    if health.intent_count > 0 && health.agent_state == "pending-write" {
        return format!(
            "pending write; {} tracked",
            plural(health.intent_count, "intent")
        );
    }
    format!("{}; no repair backlog", health.agent_state)
}

fn sifs_compact_placement_summary(
    details: Option<&SifsObjectDetails>,
    clustered: bool,
    loading: bool,
    error: Option<&str>,
) -> String {
    if loading {
        return "loading placement".to_string();
    }
    if error.is_some() {
        return "placement unavailable".to_string();
    }
    let Some(details) = details else {
        return if clustered {
            "placement pending".to_string()
        } else {
            "local node only".to_string()
        };
    };

    let placement = match details.policy.as_str() {
        "local-owned" => "local".to_string(),
        "ephemeral-local" => "ephemeral".to_string(),
        _ => match details.current_locations.len() {
            0 => "no durable nodes".to_string(),
            1 => "1 node".to_string(),
            count => format!("{count} nodes"),
        },
    };
    let health = details
        .health
        .as_ref()
        .map(|health| {
            if health.open_conflict_count > 0 {
                plural(health.open_conflict_count, "conflict")
            } else if health.pending_repair_count > 0 {
                plural(health.pending_repair_count, "missing replica")
            } else if health.agent_state == "healthy" {
                "healthy".to_string()
            } else {
                health.agent_state.clone()
            }
        })
        .unwrap_or_else(|| "health pending".to_string());
    if health == "healthy" {
        return match details.current_locations.len() {
            0 => placement,
            _ => format!("{placement} healthy"),
        };
    }
    format!("{placement} · {health}")
}

fn sifs_compact_header_line(
    entry: &SifsEntry,
    details: Option<&SifsObjectDetails>,
    clustered: bool,
    loading: bool,
    error: Option<&str>,
) -> String {
    let placement = sifs_compact_placement_summary(details, clustered, loading, error);
    if entry.is_dir {
        return format!("directory · {placement}");
    }
    if entry.is_link {
        return format!("link · {placement}");
    }
    format!("{} · {placement}", details_size(entry, details))
}

fn sifs_missing_replica_summary(health: &SifsObjectHealth) -> String {
    if health.missing_replica_nodes.is_empty() {
        return "none".to_string();
    }
    health
        .missing_replica_nodes
        .iter()
        .map(|node| short(node, 20))
        .collect::<Vec<_>>()
        .join(", ")
}

fn sifs_placement_next_step(details: Option<&SifsObjectDetails>, clustered: bool) -> String {
    if !clustered {
        return "join a cluster to enable multi-node placement".to_string();
    }
    let Some(details) = details else {
        return "load SIFS metadata before changing placement".to_string();
    };
    if details.policy == "local-owned" {
        return "copy or move into a replicated SIFS namespace to spread across nodes".to_string();
    }
    if details.policy == "ephemeral-local" {
        return "copy into durable SIFS storage before relying on this object".to_string();
    }
    if details.policy.starts_with("replicated") {
        if details
            .health
            .as_ref()
            .is_some_and(|health| health.pending_repair_count > 0)
        {
            return "repair replicas, then refresh placement".to_string();
        }
        return match details.current_locations.len() {
            0 => "refresh metadata; no durable replica receipt is recorded yet".to_string(),
            1 => "keep peers online and refresh after replica repair settles".to_string(),
            _ => {
                "replica locations are visible; use details for object and version ids".to_string()
            }
        };
    }
    "inspect details before changing this object's placement".to_string()
}

fn sifs_inspector_next_step(
    details: Option<&SifsObjectDetails>,
    clustered: bool,
    loading: bool,
    error: Option<&str>,
) -> String {
    if loading {
        return "waiting for daemon metadata".to_string();
    }
    if let Some(error) = error {
        return sifs_metadata_error_next_step(error);
    }
    if details.is_none() && clustered {
        return "use Refresh placement to load durable-node metadata".to_string();
    }
    sifs_placement_next_step(details, clustered)
}

fn sifs_details_signature(details: Option<&SifsObjectDetails>) -> String {
    details
        .map(|details| {
            let health = details
                .health
                .as_ref()
                .map(|health| {
                    format!(
                        "{}|{}|{}|{}|{}|{}|{}",
                        health.agent_state,
                        health.version_count,
                        health.receipt_count,
                        health.intent_count,
                        health.open_conflict_count,
                        health.pending_repair_count,
                        health.missing_replica_nodes.join(",")
                    )
                })
                .unwrap_or_default();
            let pins = details
                .local_pins
                .iter()
                .map(|pin| {
                    format!(
                        "{}:{}:{:?}",
                        pin.mode, pin.pinned_by_node_id, pin.created_at_unix_nanos
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "{}|{}|{}|{}|{}",
                details.version,
                details.policy,
                details.current_locations.join(","),
                pins,
                health
            )
        })
        .unwrap_or_default()
}

fn unselected_inspector_title(source: FilesSourceKind) -> &'static str {
    match source {
        FilesSourceKind::Sifs => "Current SIFS location",
        FilesSourceKind::Local => "Current folder",
    }
}

fn unselected_inspector_status(source: FilesSourceKind, entries_empty: bool) -> &'static str {
    if entries_empty {
        return "No entries in this directory";
    }
    match source {
        FilesSourceKind::Sifs => "Select an object to inspect placement",
        FilesSourceKind::Local => "Select an object to inspect metadata",
    }
}

fn unselected_inspector_scope(files: &FilesState) -> String {
    match files.source {
        FilesSourceKind::Sifs => {
            if files.clustered {
                "cluster-backed SIFS namespace".to_string()
            } else {
                "local SIFS namespace".to_string()
            }
        }
        FilesSourceKind::Local => "host filesystem".to_string(),
    }
}

fn unselected_inspector_management(files: &FilesState) -> String {
    match files.source {
        FilesSourceKind::Sifs => {
            if files.entries.is_empty() {
                if files.clustered {
                    "Create a file or folder here; created SIFS objects expose placement and replica actions after selection.".to_string()
                } else {
                    "Create a file or folder here; join a cluster to manage multi-node replicas."
                        .to_string()
                }
            } else if files.clustered {
                "Select an object to refresh placement, repair replicas, or pin content on this node."
                    .to_string()
            } else {
                "Select an object for local SIFS metadata; cluster join enables replica management."
                    .to_string()
            }
        }
        FilesSourceKind::Local => {
            if files.entries.is_empty() {
                "Create a file or folder here, or switch places to browse another filesystem."
                    .to_string()
            } else {
                "Select an object to preview, copy, move, rename, delete, or inspect metadata."
                    .to_string()
            }
        }
    }
}

fn unselected_inspector_rows(files: &FilesState) -> Vec<(&'static str, String)> {
    vec![
        ("Path", files.path.clone()),
        ("Entries", files_entry_count_status(files)),
        ("Scope", unselected_inspector_scope(files)),
        (
            "Status",
            unselected_inspector_status(files.source, files.entries.is_empty()).to_string(),
        ),
        ("Next step", unselected_inspector_management(files)),
    ]
}

fn local_details_signature(details: Option<&LocalObjectDetails>) -> String {
    details
        .map(|details| {
            format!(
                "{}|{}|{}|{}",
                details.path,
                details.kind,
                details
                    .size_bytes
                    .map(|size| size.to_string())
                    .unwrap_or_default(),
                details.readonly
            )
        })
        .unwrap_or_default()
}

fn files_preview_signature(preview: Option<&FilesPreview>) -> String {
    preview
        .map(|preview| {
            format!(
                "{}|{}|{}|{}",
                preview.title,
                preview.truncated,
                preview.binary,
                preview.lines.join("\n")
            )
        })
        .unwrap_or_default()
}

fn normalized_files_filter(filter: &str) -> String {
    filter.trim().to_ascii_lowercase()
}

fn file_entry_matches_filter(entry: &SifsEntry, filter: &str) -> bool {
    if filter.is_empty() {
        return true;
    }
    entry.name.to_ascii_lowercase().contains(filter)
        || entry.path.to_ascii_lowercase().contains(filter)
        || entry_kind_label(entry)
            .to_ascii_lowercase()
            .contains(filter)
        || human_size(entry.size).to_ascii_lowercase().contains(filter)
}

fn filtered_file_indices(entries: &[SifsEntry], filter: &str) -> Vec<usize> {
    entries
        .iter()
        .enumerate()
        .filter_map(|(index, entry)| file_entry_matches_filter(entry, filter).then_some(index))
        .collect()
}

fn visible_file_indices(
    entries: &[SifsEntry],
    filter: &str,
    sort_mode: FileSortMode,
) -> Vec<usize> {
    let mut indices = filtered_file_indices(entries, filter);
    indices
        .sort_by(|left, right| compare_file_entries(&entries[*left], &entries[*right], sort_mode));
    indices
}

fn compare_file_entries(
    left: &SifsEntry,
    right: &SifsEntry,
    sort_mode: FileSortMode,
) -> std::cmp::Ordering {
    let dir_order = (right.is_dir as u8).cmp(&(left.is_dir as u8));
    if dir_order != std::cmp::Ordering::Equal {
        return dir_order;
    }
    match sort_mode {
        FileSortMode::NameAsc => compare_file_names(left, right),
        FileSortMode::NameDesc => compare_file_names(right, left),
        FileSortMode::Kind => entry_kind_rank(left)
            .cmp(&entry_kind_rank(right))
            .then_with(|| compare_file_names(left, right)),
        FileSortMode::SizeDesc => right
            .size
            .unwrap_or(0)
            .cmp(&left.size.unwrap_or(0))
            .then_with(|| compare_file_names(left, right)),
        FileSortMode::SizeAsc => left
            .size
            .unwrap_or(0)
            .cmp(&right.size.unwrap_or(0))
            .then_with(|| compare_file_names(left, right)),
    }
}

fn compare_file_names(left: &SifsEntry, right: &SifsEntry) -> std::cmp::Ordering {
    left.name
        .to_ascii_lowercase()
        .cmp(&right.name.to_ascii_lowercase())
        .then_with(|| left.name.cmp(&right.name))
}

fn entry_kind_rank(entry: &SifsEntry) -> u8 {
    if entry.is_dir {
        0
    } else if entry.is_link {
        1
    } else {
        2
    }
}

fn preview_from_read(name: &str, read: SifsFileRead) -> FilesPreview {
    if read.bytes.is_empty() {
        return FilesPreview {
            title: format!("Preview {}", name),
            lines: vec!["Empty file".to_string()],
            truncated: read.truncated,
            binary: false,
        };
    }
    if !preview_bytes_are_text(&read.bytes) {
        return FilesPreview {
            title: format!("Binary preview {}", name),
            lines: binary_preview_lines(&read.bytes),
            truncated: read.truncated,
            binary: true,
        };
    }
    let text = String::from_utf8_lossy(&read.bytes).replace("\r\n", "\n");
    let mut lines = text
        .lines()
        .take(FILE_PREVIEW_MAX_LINES)
        .map(|line| {
            if line.trim().is_empty() {
                "(blank line)".to_string()
            } else {
                middle_short(line, 140)
            }
        })
        .collect::<Vec<_>>();
    if lines.is_empty() {
        lines.push("(no visible text)".to_string());
    }
    let line_truncated = text.lines().nth(FILE_PREVIEW_MAX_LINES).is_some();
    FilesPreview {
        title: format!("Preview {}", name),
        lines,
        truncated: read.truncated || line_truncated,
        binary: false,
    }
}

fn preview_bytes_are_text(bytes: &[u8]) -> bool {
    if bytes.contains(&0) {
        return false;
    }
    let Ok(text) = std::str::from_utf8(bytes) else {
        return false;
    };
    !text
        .chars()
        .any(|ch| ch.is_control() && !matches!(ch, '\n' | '\r' | '\t'))
}

fn binary_preview_lines(bytes: &[u8]) -> Vec<String> {
    let mut lines = vec![format!(
        "{} loaded for inspection",
        human_size(Some(bytes.len() as u64))
    )];
    let hex = bytes
        .iter()
        .take(24)
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(" ");
    if !hex.is_empty() {
        lines.push(hex);
    }
    lines
}

fn unix_nanos_label(nanos: u64) -> String {
    if nanos == 0 {
        return "not recorded".to_string();
    }
    match time::OffsetDateTime::from_unix_timestamp_nanos(nanos as i128) {
        Ok(timestamp) => timestamp
            .format(&Rfc3339)
            .unwrap_or_else(|_| format!("{nanos} ns since epoch")),
        Err(_) => format!("{nanos} ns since epoch"),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SifsEntryRowHit {
    Row,
    Select,
    Inspect,
    Copy,
    Transfer,
    Move,
    Rename,
    Delete,
}

struct SifsEntryRow {
    shared: Shared,
    index: usize,
    entry: SifsEntry,
    hovered: bool,
    pressed: Option<SifsEntryRowHit>,
}

impl SifsEntryRow {
    fn new(shared: Shared, index: usize, entry: SifsEntry) -> Self {
        Self {
            shared,
            index,
            entry,
            hovered: false,
            pressed: None,
        }
    }

    fn active(&self) -> bool {
        self.shared.files().selected == Some(self.index)
    }

    fn marked(&self) -> bool {
        self.shared.files().entry_marked(self.index)
    }

    fn action_rect(&self, bounds: Rect, action: SifsEntryRowHit) -> Rect {
        let Some(slot) = row_action_slot(action) else {
            return Rect::ZERO;
        };
        let side = ROW_ACTION_SIDE.min(bounds.height()).max(0.0);
        let step = side + ROW_ACTION_GAP;
        Rect::new(
            bounds.max_x() - side - 2.0 - slot as f32 * step,
            bounds.y() + (bounds.height() - side) * 0.5,
            side,
            side,
        )
    }

    fn action_enabled(&self, action: SifsEntryRowHit) -> bool {
        let _ = action;
        true
    }

    fn hit_at(&self, bounds: Rect, position: Point) -> Option<SifsEntryRowHit> {
        if !bounds.contains(position) {
            return None;
        }
        for action in [
            SifsEntryRowHit::Select,
            SifsEntryRowHit::Inspect,
            SifsEntryRowHit::Copy,
            SifsEntryRowHit::Transfer,
            SifsEntryRowHit::Move,
            SifsEntryRowHit::Rename,
            SifsEntryRowHit::Delete,
        ] {
            if !self.action_enabled(action) {
                continue;
            }
            if self.action_rect(bounds, action).contains(position) {
                return Some(action);
            }
        }
        Some(SifsEntryRowHit::Row)
    }

    fn action_widget_id(parent: sui::WidgetId, action: SifsEntryRowHit) -> sui::WidgetId {
        let salt = match action {
            SifsEntryRowHit::Select => 8_309,
            SifsEntryRowHit::Inspect => 8_317,
            SifsEntryRowHit::Copy => 8_329,
            SifsEntryRowHit::Transfer => 8_333,
            SifsEntryRowHit::Move => 8_341,
            SifsEntryRowHit::Rename => 8_351,
            SifsEntryRowHit::Delete => 8_389,
            SifsEntryRowHit::Row => 8_401,
        };
        sui::WidgetId::new(parent.get().wrapping_mul(41).wrapping_add(salt))
    }
}

fn row_action_slot(action: SifsEntryRowHit) -> Option<usize> {
    match action {
        SifsEntryRowHit::Select => Some(0),
        SifsEntryRowHit::Inspect => Some(1),
        SifsEntryRowHit::Copy => Some(2),
        SifsEntryRowHit::Transfer => Some(3),
        SifsEntryRowHit::Move => Some(4),
        SifsEntryRowHit::Rename => Some(5),
        SifsEntryRowHit::Delete => Some(6),
        SifsEntryRowHit::Row => None,
    }
}

fn row_actions_width() -> f32 {
    ROW_ACTION_SIDE * ROW_ACTION_COUNT + ROW_ACTION_GAP * (ROW_ACTION_COUNT - 1.0) + 8.0
}

fn row_action_visible(
    entry: &SifsEntry,
    action: SifsEntryRowHit,
    hovered: bool,
    active: bool,
    marked: bool,
    pressed: Option<SifsEntryRowHit>,
) -> bool {
    hovered
        || active
        || marked
        || pressed.is_some()
        || (entry.is_dir && action == SifsEntryRowHit::Inspect)
        || (marked && action == SifsEntryRowHit::Select)
}

fn row_transfer_prompt_kind(source: FilesSourceKind) -> FileActionPromptKind {
    match source {
        FilesSourceKind::Sifs => FileActionPromptKind::CopyToLocal,
        FilesSourceKind::Local => FileActionPromptKind::CopyToSifs,
    }
}

fn row_transfer_glyph(source: FilesSourceKind) -> IconGlyph {
    match source {
        FilesSourceKind::Sifs => IconGlyph::Download,
        FilesSourceKind::Local => IconGlyph::Storage,
    }
}

fn row_transfer_label(source: FilesSourceKind, name: &str) -> String {
    match source {
        FilesSourceKind::Sifs => format!("Download {name}"),
        FilesSourceKind::Local => format!("Copy to SIFS {name}"),
    }
}

fn row_transfer_description(source: FilesSourceKind, path: &str) -> String {
    match source {
        FilesSourceKind::Sifs => format!("Download SIFS {path} to a local path"),
        FilesSourceKind::Local => format!("Copy local {path} into SIFS"),
    }
}

impl Widget for SifsEntryRow {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        match event {
            Event::Pointer(pointer) if pointer.kind == PointerEventKind::Move => {
                let hovered = ctx.bounds().contains(pointer.position);
                if self.hovered != hovered {
                    self.hovered = hovered;
                    ctx.request_paint();
                    ctx.request_semantics();
                }
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Down
                    && pointer.button == Some(PointerButton::Primary) =>
            {
                let Some(hit) = self.hit_at(ctx.bounds(), pointer.position) else {
                    return;
                };
                self.pressed = Some(hit);
                ctx.request_focus();
                ctx.request_pointer_capture(pointer.pointer_id);
                ctx.request_paint();
                ctx.set_handled();
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Up
                    && (pointer.button == Some(PointerButton::Primary)
                        || self.pressed.is_some()) =>
            {
                let hit = self.hit_at(ctx.bounds(), pointer.position);
                if let (Some(pressed), Some(hit)) = (self.pressed.take(), hit)
                    && pressed == hit
                {
                    match hit {
                        SifsEntryRowHit::Row => {
                            self.shared.files_mut().activate_row(self.index);
                        }
                        SifsEntryRowHit::Select => {
                            self.shared.files_mut().toggle_marked_entry(self.index);
                        }
                        SifsEntryRowHit::Inspect => {
                            self.shared.files_mut().select_entry(self.index);
                        }
                        SifsEntryRowHit::Copy => {
                            let mut files = self.shared.files_mut();
                            files.select_entry(self.index);
                            files.open_action_prompt(FileActionPromptKind::Copy);
                            request_files_overlay_rebuild(ctx);
                        }
                        SifsEntryRowHit::Transfer => {
                            let mut files = self.shared.files_mut();
                            files.select_entry(self.index);
                            let prompt_kind = row_transfer_prompt_kind(files.source);
                            files.open_action_prompt(prompt_kind);
                            request_files_overlay_rebuild(ctx);
                        }
                        SifsEntryRowHit::Move => {
                            let mut files = self.shared.files_mut();
                            files.select_entry(self.index);
                            files.open_action_prompt(FileActionPromptKind::Move);
                            request_files_overlay_rebuild(ctx);
                        }
                        SifsEntryRowHit::Rename => {
                            let mut files = self.shared.files_mut();
                            files.select_entry(self.index);
                            files.open_action_prompt(FileActionPromptKind::Rename);
                            request_files_overlay_rebuild(ctx);
                        }
                        SifsEntryRowHit::Delete => {
                            let mut files = self.shared.files_mut();
                            files.select_entry(self.index);
                            files.open_action_prompt(FileActionPromptKind::Delete);
                            request_files_overlay_rebuild(ctx);
                        }
                    }
                    ctx.request_measure();
                    ctx.request_paint();
                    ctx.request_semantics();
                }
                ctx.release_pointer_capture(pointer.pointer_id);
                ctx.set_handled();
            }
            Event::Pointer(pointer) if pointer.kind == PointerEventKind::Leave => {
                if self.hovered {
                    self.hovered = false;
                    ctx.request_paint();
                    ctx.request_semantics();
                }
            }
            Event::Pointer(pointer) if pointer.kind == PointerEventKind::Cancel => {
                if self.pressed.take().is_some() {
                    ctx.release_pointer_capture(pointer.pointer_id);
                    ctx.request_paint();
                    ctx.set_handled();
                }
                if self.hovered {
                    self.hovered = false;
                    ctx.request_paint();
                    ctx.request_semantics();
                }
            }
            Event::Keyboard(key)
                if ctx.is_focused()
                    && key.state == KeyState::Pressed
                    && matches!(key.key.as_str(), "Enter" | " ") =>
            {
                self.shared.files_mut().activate_row(self.index);
                ctx.request_measure();
                ctx.request_paint();
                ctx.request_semantics();
                ctx.set_handled();
            }
            _ => {}
        }
    }

    fn measure(&mut self, _ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        constraints.clamp(Size::new(
            if constraints.max.width.is_finite() {
                constraints.max.width
            } else {
                260.0
            },
            24.0,
        ))
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let theme = self.shared.theme();
        let pal = palette(&self.shared);
        let bounds = ctx.bounds();
        let active = self.active();
        let marked = self.marked();
        let icon_color = if active { pal.accent } else { pal.text_faint };
        let label_color = if active { pal.text } else { pal.text_muted };
        let title_style = pal.text_style(&theme, 12.5, FontWeight::MEDIUM, label_color);
        let meta_style = pal.text_style(&theme, 10.5, FontWeight::NORMAL, pal.text_faint);
        let source = self.shared.files().source;

        let leading = Rect::new(bounds.x() + 2.0, bounds.y(), 20.0, bounds.height());
        let icon_side = 15.0;
        draw_glyph(
            ctx,
            entry_icon(&self.entry),
            Rect::new(
                leading.x() + 1.0,
                leading.y() + (leading.height() - icon_side) * 0.5,
                icon_side,
                icon_side,
            ),
            icon_color,
        );

        for (action, glyph, color) in [
            (
                SifsEntryRowHit::Select,
                IconGlyph::Check,
                if marked { pal.accent } else { pal.text_muted },
            ),
            (
                SifsEntryRowHit::Inspect,
                IconGlyph::Search,
                if active { pal.accent } else { pal.text_muted },
            ),
            (SifsEntryRowHit::Copy, IconGlyph::Restore, pal.text_muted),
            (
                SifsEntryRowHit::Transfer,
                row_transfer_glyph(source),
                pal.accent,
            ),
            (
                SifsEntryRowHit::Move,
                IconGlyph::ChevronRight,
                pal.text_muted,
            ),
            (
                SifsEntryRowHit::Rename,
                IconGlyph::MoreHorizontal,
                pal.text_muted,
            ),
            (SifsEntryRowHit::Delete, IconGlyph::Trash, pal.warn),
        ] {
            if !self.action_enabled(action) {
                continue;
            }
            let rect = self.action_rect(bounds, action);
            if row_action_visible(
                &self.entry,
                action,
                self.hovered,
                active,
                marked,
                self.pressed,
            ) {
                let fill = if self.pressed == Some(action) {
                    pal.selected
                } else if action == SifsEntryRowHit::Select && marked {
                    pal.selected
                } else if active {
                    pal.field
                } else {
                    pal.hover
                };
                widgets::fill_round_rect(ctx, rect, fill, 6.0);
                draw_glyph(ctx, glyph, rect.inflate(-5.0, -5.0), color);
            }
        }

        let actions_w = row_actions_width();
        let meta_w = 74.0_f32.min(bounds.width() * 0.36).max(0.0);
        let meta_rect = Rect::new(
            (bounds.max_x() - actions_w - meta_w).max(bounds.x()),
            bounds.y(),
            meta_w,
            bounds.height(),
        );
        widgets::draw_text_in(ctx, meta_rect, &entry_meta(&self.entry), meta_style);

        let title_x = leading.max_x() + 5.0;
        let title_right = meta_rect.x() - 6.0;
        let title_rect = Rect::new(
            title_x,
            bounds.y(),
            (title_right - title_x).max(0.0),
            bounds.height(),
        );
        let display_name = display_name_for_width(&self.entry.name, title_rect.width(), 7.1);
        ctx.push_clip_rect(title_rect);
        widgets::draw_text_in(ctx, title_rect, &display_name, title_style);
        ctx.pop_clip();
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let row_id = ctx.widget_id();
        let mut node = SemanticsNode::new(row_id, SemanticsRole::ListItem, ctx.bounds());
        let source = self.shared.files().source;
        node.name = Some(self.entry.name.clone());
        let mut description = format!(
            "{}, {} {}",
            entry_kind_label(&self.entry),
            source.path_label(),
            self.entry.path
        );
        if self.entry.is_dir {
            description.push_str(". Activate opens the folder; Inspect selects it for management");
        }
        node.description = Some(description);
        node.state.focused = ctx.is_focused();
        node.state.hovered = self.hovered;
        node.state.selected = self.active() || self.marked();
        node.actions = vec![SemanticsAction::Focus, SemanticsAction::Activate];
        ctx.push(node);

        for (action, name, description) in [
            (
                SifsEntryRowHit::Select,
                if self.marked() {
                    format!("Unmark {}", self.entry.name)
                } else {
                    format!("Mark {}", self.entry.name)
                },
                "Include this entry in batch actions".to_string(),
            ),
            (
                SifsEntryRowHit::Inspect,
                format!("Inspect {}", self.entry.name),
                "Show SIFS placement and metadata".to_string(),
            ),
            (
                SifsEntryRowHit::Copy,
                format!("Copy {}", self.entry.name),
                format!(
                    "Copy {} to another {} path",
                    self.entry.path,
                    source.operation_scope()
                ),
            ),
            (
                SifsEntryRowHit::Transfer,
                row_transfer_label(source, &self.entry.name),
                row_transfer_description(source, &self.entry.path),
            ),
            (
                SifsEntryRowHit::Move,
                format!("Move {}", self.entry.name),
                format!(
                    "Move {} to another {} path",
                    self.entry.path,
                    source.operation_scope()
                ),
            ),
            (
                SifsEntryRowHit::Rename,
                format!("Rename {}", self.entry.name),
                format!("Rename {}", self.entry.path),
            ),
            (
                SifsEntryRowHit::Delete,
                format!("Delete {}", self.entry.name),
                format!("Delete {}", self.entry.path),
            ),
        ] {
            if !self.action_enabled(action) {
                continue;
            }
            let mut node = SemanticsNode::new(
                Self::action_widget_id(row_id, action),
                SemanticsRole::Button,
                self.action_rect(ctx.bounds(), action),
            );
            node.parent = Some(row_id);
            node.name = Some(name);
            node.description = Some(description);
            node.actions = vec![SemanticsAction::Activate];
            node.state.hovered = self.hovered;
            node.state.selected = self.active() || self.marked();
            ctx.push(node);
        }
    }

    fn accepts_focus(&self) -> bool {
        true
    }
}

// ---------------------------------------------------------------------------
// Breadcrumb header (path bar) — rebuilt on navigation by `RebuildOnChange`.
// ---------------------------------------------------------------------------

/// Build the header: navigation buttons + a `Breadcrumb` over the current path segments. Each
/// segment activates a navigate-to-that-segment; the up button goes to the parent.
fn header(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let crumbs = shared.files().crumbs();
    let current = crumbs.len().saturating_sub(1);
    let targets: Vec<String> = crumbs.iter().map(|(_, target)| target.clone()).collect();

    let back = {
        let s = shared.clone();
        IconButton::new(IconGlyph::ChevronLeft, "Back")
            .size(30.0)
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || s.files().can_go_back()
            })
            .on_press(move || {
                let waker = { s.borrow().waker.clone() };
                s.files_mut().go_back(waker);
            })
    };

    let forward = {
        let s = shared.clone();
        IconButton::new(IconGlyph::ChevronRight, "Forward")
            .size(30.0)
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || s.files().can_go_forward()
            })
            .on_press(move || {
                let waker = { s.borrow().waker.clone() };
                s.files_mut().go_forward(waker);
            })
    };

    let up = {
        let s = shared.clone();
        IconButton::new(IconGlyph::ChevronUp, "Up")
            .size(30.0)
            .theme_when(clone_reader(reader))
            .on_press(move || {
                s.files_mut().go_up();
            })
    };

    let breadcrumb = {
        let s = shared.clone();
        let path_label = shared.files().source.path_label();
        Breadcrumb::new(path_label)
            .items(
                crumbs
                    .iter()
                    .map(|(label, _)| BreadcrumbItem::new(label.clone())),
            )
            .current(current)
            .theme_when(clone_reader(reader))
            .on_activate(move |index, _label| {
                if let Some(target) = targets.get(index) {
                    s.files_mut().navigate(target.clone());
                }
            })
    };

    let source = {
        let s = shared.clone();
        sui::Select::new("files-source")
            .options(vec!["SIFS".to_string(), "Local".to_string()])
            .placeholder("Source")
            .theme_when(clone_reader(reader))
            .selected_when({
                let s = s.clone();
                move || {
                    Some(match s.files().source {
                        FilesSourceKind::Sifs => 0,
                        FilesSourceKind::Local => 1,
                    })
                }
            })
            .on_change(move |index, _label| {
                let source = if index == 1 {
                    FilesSourceKind::Local
                } else {
                    FilesSourceKind::Sifs
                };
                let waker = { s.borrow().waker.clone() };
                s.files_mut().switch_source(source, waker);
            })
    };

    let refresh = {
        let s = shared.clone();
        let label = format!("Refresh {}", shared.files().source.label());
        IconButton::new(IconGlyph::Redo, label)
            .size(30.0)
            .theme_when(clone_reader(reader))
            .selected_when({
                let s = s.clone();
                move || s.files().loading
            })
            .enabled_when({
                let s = s.clone();
                move || !s.files().loading
            })
            .on_press(move || {
                let waker = { s.borrow().waker.clone() };
                s.files_mut().refresh(waker);
            })
    };

    let go_to = {
        let s = shared.clone();
        IconButton::new(IconGlyph::Search, "Go to path")
            .size(30.0)
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || !s.files().loading
            })
            .on_press_with_ctx(move |ctx| {
                s.files_mut().open_action_prompt(FileActionPromptKind::GoTo);
                request_files_overlay_rebuild(ctx);
            })
    };

    let new_file = {
        let s = shared.clone();
        sui::Button::new("New file")
            .icon(IconGlyph::File)
            .min_width(96.0)
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || s.files().can_modify_current_location()
            })
            .on_press_with_ctx(move |ctx| {
                s.files_mut()
                    .open_action_prompt(FileActionPromptKind::NewFile);
                request_files_overlay_rebuild(ctx);
            })
    };

    let new_folder = {
        let s = shared.clone();
        sui::Button::new("New folder")
            .icon(IconGlyph::Folder)
            .min_width(112.0)
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || s.files().can_modify_current_location()
            })
            .on_press_with_ctx(move |ctx| {
                s.files_mut()
                    .open_action_prompt(FileActionPromptKind::NewFolder);
                request_files_overlay_rebuild(ctx);
            })
    };

    let copy = {
        let s = shared.clone();
        IconButton::new(IconGlyph::Restore, "Copy to")
            .size(30.0)
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || {
                    let files = s.files();
                    files.can_modify_current_location() && files.selected_entry().is_some()
                }
            })
            .on_press_with_ctx(move |ctx| {
                s.files_mut().open_action_prompt(FileActionPromptKind::Copy);
                request_files_overlay_rebuild(ctx);
            })
    };

    let move_to = {
        let s = shared.clone();
        IconButton::new(IconGlyph::ChevronRight, "Move to")
            .size(30.0)
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || {
                    let files = s.files();
                    files.can_modify_current_location() && files.selected_entry().is_some()
                }
            })
            .on_press_with_ctx(move |ctx| {
                s.files_mut().open_action_prompt(FileActionPromptKind::Move);
                request_files_overlay_rebuild(ctx);
            })
    };

    let refresh_placement = {
        let s = shared.clone();
        IconButton::new(IconGlyph::Storage, "Refresh placement")
            .size(30.0)
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || {
                    let files = s.files();
                    files.source == FilesSourceKind::Sifs
                        && files.can_modify_current_location()
                        && files.selected_entry().is_some()
                }
            })
            .on_press_with_ctx(move |ctx| {
                let waker = { s.borrow().waker.clone() };
                s.files_mut().start_placement_refresh(waker);
                request_files_overlay_rebuild(ctx);
            })
    };

    let rename = {
        let s = shared.clone();
        IconButton::new(IconGlyph::MoreHorizontal, "Rename")
            .size(30.0)
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || {
                    let files = s.files();
                    files.can_modify_current_location() && files.selected_entry().is_some()
                }
            })
            .on_press_with_ctx(move |ctx| {
                s.files_mut()
                    .open_action_prompt(FileActionPromptKind::Rename);
                request_files_overlay_rebuild(ctx);
            })
    };

    let delete = {
        let s = shared.clone();
        IconButton::new(IconGlyph::Trash, "Delete selected item")
            .size(30.0)
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || {
                    let files = s.files();
                    files.can_modify_current_location() && files.selected_entry().is_some()
                }
            })
            .on_press_with_ctx(move |ctx| {
                s.files_mut()
                    .open_action_prompt(FileActionPromptKind::Delete);
                request_files_overlay_rebuild(ctx);
            })
    };

    let navigation_row = Flex::horizontal()
        .main_gap(HEADER_COMMAND_GAP)
        .align_items(Alignment::Center)
        .with_item(back, FlexItem::new().no_shrink())
        .with_item(forward, FlexItem::new().no_shrink())
        .with_item(up, FlexItem::new().no_shrink())
        .with_item(
            SizedBox::new().width(118.0).height(30.0).with_child(source),
            FlexItem::fixed(118.0),
        )
        .with_item(breadcrumb, FlexItem::fill().min_width(180.0))
        .with_item(refresh, FlexItem::new().no_shrink())
        .with_item(go_to, FlexItem::new().no_shrink());

    let command_row = Flex::horizontal()
        .main_gap(HEADER_COMMAND_GAP)
        .align_items(Alignment::Center)
        .with_item(new_file, FlexItem::new().no_shrink())
        .with_item(new_folder, FlexItem::new().no_shrink())
        .with_item(copy, FlexItem::new().no_shrink())
        .with_item(move_to, FlexItem::new().no_shrink())
        .with_item(refresh_placement, FlexItem::new().no_shrink())
        .with_item(rename, FlexItem::new().no_shrink())
        .with_item(delete, FlexItem::new().no_shrink());

    Stack::vertical()
        .spacing(HEADER_ROW_GAP)
        .alignment(Alignment::Stretch)
        .with_child(SizedBox::new().height(30.0).with_child(navigation_row))
        .with_child(SizedBox::new().height(30.0).with_child(command_row))
}

// ---------------------------------------------------------------------------
// File listing — rebuilt on navigation by `RebuildOnChange`.
// ---------------------------------------------------------------------------

/// Build the file-list body for the *current* listing: an error/empty placeholder, or a `ListView`
/// of the entries wrapped in a `ScrollView`. Directory rows navigate; the inline inspect action
/// selects either files or directories for the SIFS inspector without changing the path.
fn listing(shared: &Shared, reader: &ThemeReader, scroll_state: &ScrollState) -> WidgetPod {
    let (loading, error, is_empty, has_tasks): (bool, Option<String>, bool, bool) = {
        let files = shared.files();
        (
            files.loading,
            files.error.clone(),
            files.entries.is_empty(),
            !files.tasks.is_empty(),
        )
    };
    let source = shared.files().source;

    if loading {
        let body = centered_note(
            shared,
            "▤",
            format!("Loading {}", source.label()),
            match source {
                FilesSourceKind::Sifs => "Opening the distributed filesystem...",
                FilesSourceKind::Local => "Reading the local directory...",
            },
        )
        .glyph(IconGlyph::Storage);
        return placeholder_with_task_strip(shared, body, has_tasks);
    }
    if let Some(error) = error {
        let retry = {
            let s = shared.clone();
            sui::Button::new(format!("Retry {}", source.label()))
                .icon(IconGlyph::Redo)
                .min_width(128.0)
                .theme_when(clone_reader(reader))
                .on_press(move || {
                    let waker = { s.borrow().waker.clone() };
                    s.files_mut().retry(waker);
                })
        };
        let settings = if source == FilesSourceKind::Sifs {
            let s = shared.clone();
            Some(
                sui::Button::new("Connection settings")
                    .icon(IconGlyph::Alert)
                    .min_width(160.0)
                    .theme_when(secondary_button_theme(reader))
                    .on_press(move || open_settings_section(&s, SettingsSection::Connection)),
            )
        } else {
            None
        };
        let browse_local = if source == FilesSourceKind::Sifs {
            let s = shared.clone();
            Some(
                sui::Button::new("Browse local files")
                    .icon(IconGlyph::Folder)
                    .min_width(156.0)
                    .theme_when(secondary_button_theme(reader))
                    .on_press(move || {
                        let waker = { s.borrow().waker.clone() };
                        s.files_mut().switch_source(FilesSourceKind::Local, waker);
                    }),
            )
        } else {
            None
        };
        let mut primary_actions = Stack::horizontal()
            .spacing(8.0)
            .alignment(Alignment::Center)
            .with_child(retry);
        if let Some(settings) = settings {
            primary_actions = primary_actions.with_child(settings);
        }
        let mut action_height = 32.0;
        let mut actions = Stack::vertical()
            .spacing(8.0)
            .alignment(Alignment::Center)
            .with_child(primary_actions);
        if let Some(browse_local) = browse_local {
            actions = actions.with_child(
                SizedBox::new()
                    .width(180.0)
                    .height(30.0)
                    .with_child(browse_local),
            );
            action_height = 70.0;
        }
        let body = centered_note(
            shared,
            "▤",
            format!("{} unavailable", source.label()),
            source_unavailable_summary(source, &error),
        )
        .glyph(IconGlyph::Storage)
        .action(actions)
        .action_height(action_height);
        return placeholder_with_task_strip(shared, body, has_tasks);
    }
    if is_empty {
        let create_file = {
            let s = shared.clone();
            sui::Button::new("Create file")
                .icon(IconGlyph::File)
                .min_width(116.0)
                .theme_when(clone_reader(reader))
                .on_press_with_ctx(move |ctx| {
                    s.files_mut()
                        .open_action_prompt(FileActionPromptKind::NewFile);
                    request_files_overlay_rebuild(ctx);
                })
        };
        let create_folder = {
            let s = shared.clone();
            sui::Button::new("Create folder")
                .icon(IconGlyph::Folder)
                .min_width(132.0)
                .theme_when(clone_reader(reader))
                .on_press_with_ctx(move |ctx| {
                    s.files_mut()
                        .open_action_prompt(FileActionPromptKind::NewFolder);
                    request_files_overlay_rebuild(ctx);
                })
        };
        let actions = Stack::horizontal()
            .spacing(8.0)
            .alignment(Alignment::Center)
            .with_child(create_file)
            .with_child(create_folder);
        let body = centered_note(shared, "▤", "Empty directory", "Nothing here yet.")
            .glyph(IconGlyph::Folder)
            .action(actions);
        return placeholder_with_task_strip(shared, body, has_tasks);
    }

    let scroll = files_list_body_rebuild(shared, reader, scroll_state);
    let filter = SizedBox::new()
        .height(LIST_FILTER_H)
        .with_child(InsetX::new(
            files_filter_bar(shared, reader),
            CONTENT_PAD * 0.5,
        ));
    let header = SizedBox::new()
        .height(LIST_HEADER_H)
        .with_child(FilesListHeader::new(shared.clone()));
    if has_tasks {
        let top_h = LIST_TASK_STRIP_H + LIST_FILTER_H + LIST_HEADER_H;
        let top = Stack::vertical()
            .alignment(Alignment::Stretch)
            .with_child(
                SizedBox::new()
                    .height(LIST_TASK_STRIP_H)
                    .with_child(FilesTaskStrip::new(shared.clone())),
            )
            .with_child(filter)
            .with_child(header);
        WidgetPod::new(widgets::Dock::new(scroll).top(top_h, top))
    } else {
        let top_h = LIST_FILTER_H + LIST_HEADER_H;
        let top = Stack::vertical()
            .alignment(Alignment::Stretch)
            .with_child(filter)
            .with_child(header);
        WidgetPod::new(widgets::Dock::new(scroll).top(top_h, top))
    }
}

fn placeholder_with_task_strip<W>(shared: &Shared, body: W, has_tasks: bool) -> WidgetPod
where
    W: Widget + 'static,
{
    if has_tasks {
        WidgetPod::new(
            widgets::Dock::new(body).top(
                LIST_TASK_STRIP_H,
                SizedBox::new()
                    .height(LIST_TASK_STRIP_H)
                    .with_child(FilesTaskStrip::new(shared.clone())),
            ),
        )
    } else {
        WidgetPod::new(body)
    }
}

fn files_filter_input(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let s = shared.clone();
    let value = shared.files().filter_query.clone();
    sui::TextInput::new("files-list-filter")
        .value(value)
        .placeholder("Filter current folder")
        .leading_icon(IconGlyph::Search)
        .theme_when(clone_reader(reader))
        .on_change(move |value| {
            s.files_mut().set_filter_query(value);
        })
}

fn files_sort_select(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let s = shared.clone();
    sui::Select::new("files-sort")
        .options(FileSortMode::options())
        .placeholder("Sort")
        .theme_when(clone_reader(reader))
        .selected_when({
            let s = s.clone();
            move || {
                let mode = s.files().sort_mode;
                FileSortMode::ALL
                    .iter()
                    .position(|candidate| *candidate == mode)
            }
        })
        .on_change(move |index, _label| {
            if let Some(mode) = FileSortMode::ALL.get(index).copied() {
                s.files_mut().set_sort_mode(mode);
            }
        })
}

fn files_mark_visible_button(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let s = shared.clone();
    IconButton::new(IconGlyph::Check, "Mark visible entries")
        .size(30.0)
        .theme_when(clone_reader(reader))
        .enabled_when({
            let s = s.clone();
            move || !s.files().loading && !s.files().entries.is_empty()
        })
        .on_press_with_ctx(move |ctx| {
            s.files_mut().mark_visible_entries();
            request_files_overlay_rebuild(ctx);
        })
}

fn files_clear_marks_button(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let s = shared.clone();
    IconButton::new(IconGlyph::Close, "Clear marked entries")
        .size(30.0)
        .theme_when(clone_reader(reader))
        .enabled_when({
            let s = s.clone();
            move || s.files().marked_count() > 0
        })
        .on_press_with_ctx(move |ctx| {
            s.files_mut().clear_marked_entries();
            request_files_overlay_rebuild(ctx);
        })
}

fn files_marked_summary_label(shared: &Shared) -> impl Widget + use<> {
    let s = shared.clone();
    sui::Label::dynamic("No marked entries", move || {
        let count = s.files().marked_count();
        if count == 0 {
            "No marked entries".to_string()
        } else {
            marked_entries_label(count)
        }
    })
    .semantic_name("Files marked entries")
    .font_size(11.0)
    .line_height(16.0)
}

fn files_copy_marked_button(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let s = shared.clone();
    sui::Button::new("Copy")
        .icon(IconGlyph::Restore)
        .min_width(76.0)
        .min_height(28.0)
        .theme_when(clone_reader(reader))
        .enabled_when({
            let s = s.clone();
            move || {
                let files = s.files();
                files.can_modify_current_location() && files.marked_count() > 0
            }
        })
        .on_press_with_ctx(move |ctx| {
            s.files_mut()
                .open_action_prompt(FileActionPromptKind::CopySelected);
            request_files_overlay_rebuild(ctx);
        })
}

fn files_transfer_marked_button(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let s = shared.clone();
    let (glyph, label, width) = match shared.files().source {
        FilesSourceKind::Sifs => (IconGlyph::Download, "Download", 100.0),
        FilesSourceKind::Local => (IconGlyph::Storage, "Copy to SIFS", 112.0),
    };
    sui::Button::new(label)
        .icon(glyph)
        .min_width(width)
        .min_height(28.0)
        .theme_when(clone_reader(reader))
        .enabled_when({
            let s = s.clone();
            move || {
                let files = s.files();
                files.can_modify_current_location() && files.marked_count() > 0
            }
        })
        .on_press_with_ctx(move |ctx| {
            s.files_mut()
                .open_action_prompt(FileActionPromptKind::TransferSelected);
            request_files_overlay_rebuild(ctx);
        })
}

fn files_move_marked_button(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let s = shared.clone();
    sui::Button::new("Move")
        .icon(IconGlyph::ChevronRight)
        .min_width(78.0)
        .min_height(28.0)
        .theme_when(clone_reader(reader))
        .enabled_when({
            let s = s.clone();
            move || {
                let files = s.files();
                files.can_modify_current_location() && files.marked_count() > 0
            }
        })
        .on_press_with_ctx(move |ctx| {
            s.files_mut()
                .open_action_prompt(FileActionPromptKind::MoveSelected);
            request_files_overlay_rebuild(ctx);
        })
}

fn files_delete_marked_button(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let s = shared.clone();
    sui::Button::new("Delete")
        .icon(IconGlyph::Trash)
        .min_width(82.0)
        .min_height(28.0)
        .theme_when(clone_reader(reader))
        .enabled_when({
            let s = s.clone();
            move || {
                let files = s.files();
                files.can_modify_current_location() && files.marked_count() > 0
            }
        })
        .on_press_with_ctx(move |ctx| {
            s.files_mut()
                .open_action_prompt(FileActionPromptKind::DeleteSelected);
            request_files_overlay_rebuild(ctx);
        })
}

fn files_filter_bar(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let filters = Stack::horizontal()
        .spacing(8.0)
        .alignment(Alignment::Center)
        .with_child(
            SizedBox::new()
                .width(240.0)
                .height(30.0)
                .with_child(files_filter_input(shared, reader)),
        )
        .with_child(
            SizedBox::new()
                .width(150.0)
                .height(30.0)
                .with_child(files_sort_select(shared, reader)),
        )
        .with_child(files_mark_visible_button(shared, reader))
        .with_child(files_clear_marks_button(shared, reader));

    let marked_actions = Stack::horizontal()
        .spacing(8.0)
        .alignment(Alignment::Center)
        .with_child(
            SizedBox::new()
                .width(116.0)
                .height(28.0)
                .with_child(files_marked_summary_label(shared)),
        )
        .with_child(files_copy_marked_button(shared, reader))
        .with_child(files_transfer_marked_button(shared, reader))
        .with_child(files_move_marked_button(shared, reader))
        .with_child(files_delete_marked_button(shared, reader));

    Stack::vertical()
        .spacing(6.0)
        .alignment(Alignment::Stretch)
        .with_child(SizedBox::new().height(30.0).with_child(filters))
        .with_child(SizedBox::new().height(28.0).with_child(marked_actions))
}

fn files_list_body_signature(shared: &Shared) -> (u64, String, FileSortMode) {
    let files = shared.files();
    (
        files.list_revision,
        files.filter_query.clone(),
        files.sort_mode,
    )
}

fn files_list_body_rebuild(
    shared: &Shared,
    reader: &ThemeReader,
    scroll_state: &ScrollState,
) -> impl Widget + use<> {
    let s = shared.clone();
    let r = Rc::clone(reader);
    let ss = scroll_state.clone();
    RebuildOnChange::new(
        {
            let s = s.clone();
            move || files_list_body_signature(&s)
        },
        move |_key| files_list_body(&s, &r, &ss),
    )
}

fn files_list_body(shared: &Shared, reader: &ThemeReader, scroll_state: &ScrollState) -> WidgetPod {
    let (entries, filter_query, source, sort_mode) = {
        let files = shared.files();
        (
            files.entries.clone(),
            files.filter_query.clone(),
            files.source,
            files.sort_mode,
        )
    };
    let filter = normalized_files_filter(&filter_query);
    let visible_indices = visible_file_indices(&entries, &filter, sort_mode);
    if visible_indices.is_empty() {
        return WidgetPod::new(
            centered_note(
                shared,
                "▤",
                "No matching entries",
                format!(
                    "No {} entries match \"{}\".",
                    source.label(),
                    filter_query.trim()
                ),
            )
            .glyph(IconGlyph::Search),
        );
    }

    // Build the row items: icon + name on the left, compact metadata and inspect action on the
    // right. The child row handles pointer hit-testing while `ListView` keeps selection and scroll.
    let items: Vec<ListItem> = visible_indices
        .iter()
        .filter_map(|index| {
            let entry = entries.get(*index)?.clone();
            Some(
                ListItem::new(entry.name.clone()).with_child(SifsEntryRow::new(
                    shared.clone(),
                    *index,
                    entry,
                )),
            )
        })
        .collect();

    let list = {
        let s = shared.clone();
        ListView::new("files-listing")
            .items(items)
            .row_height(ROW_H)
            .theme_when(clone_reader(reader))
            // Reflect the current file selection (the highlight is reactive — selecting a file does
            // NOT rebuild the list, only repaints + updates the inspector).
            .selected_when({
                let s = s.clone();
                move || {
                    let files = s.files();
                    let filter = normalized_files_filter(&files.filter_query);
                    visible_file_indices(&files.entries, &filter, files.sort_mode)
                        .into_iter()
                        .position(|entry_index| files.selected == Some(entry_index))
                }
            })
            .on_change(move |index, _label| {
                // Fallback for keyboard-driven ListView activation when no row child receives focus:
                // directory -> navigate, file -> select. Row children handle pointer activation.
                let mut files = s.files_mut();
                let filter = normalized_files_filter(&files.filter_query);
                let visible_indices =
                    visible_file_indices(&files.entries, &filter, files.sort_mode);
                if let Some(entry_index) = visible_indices.get(index) {
                    files.activate_row(*entry_index);
                }
            })
    };

    // Pad the list off the panel edges, then make it scrollable. `ScrollView` owns the scroll
    // position (the `FilesState::scroll` field is now vestigial but kept for state compatibility).
    let inset = InsetX::new(
        Stack::vertical()
            .alignment(Alignment::Stretch)
            .with_child(SizedBox::new().height(CONTENT_PAD * 0.5))
            .with_child(list)
            .with_child(SizedBox::new().height(CONTENT_PAD * 0.5)),
        CONTENT_PAD * 0.5,
    );
    WidgetPod::new(ScrollView::vertical(inset).state(scroll_state.clone()))
}

/// A centered empty/error placeholder (mark, title, subtitle) themed off the chrome palette.
fn centered_note(
    shared: &Shared,
    mark: impl Into<String>,
    title: impl Into<String>,
    subtitle: impl Into<String>,
) -> widgets::Placeholder {
    widgets::Placeholder::new(shared.clone(), mark, title, subtitle)
}

fn request_files_overlay_rebuild(ctx: &mut EventCtx) {
    ctx.request_measure();
    ctx.request_paint();
    ctx.request_semantics();
    ctx.request_hit_test();
}

fn open_settings_section(shared: &Shared, section: SettingsSection) {
    let mut app = shared.borrow_mut();
    app.active_tab = Tab::Settings;
    if let Some(ready) = app.ready.as_mut() {
        ready.settings.open(section);
    }
}

fn files_source_status(files: &FilesState) -> &'static str {
    match files.source {
        FilesSourceKind::Sifs if files.error.is_some() => "SIFS unavailable",
        FilesSourceKind::Sifs if files.loading => "Loading SIFS",
        FilesSourceKind::Sifs if files.clustered => "SIFS online",
        FilesSourceKind::Sifs => "Local SIFS",
        FilesSourceKind::Local if files.error.is_some() => "Local unavailable",
        FilesSourceKind::Local if files.loading => "Loading local",
        FilesSourceKind::Local => "Local files",
    }
}

fn files_entry_count_status(files: &FilesState) -> String {
    if files.loading {
        "loading directory".to_string()
    } else if files.error.is_some() {
        "listing unavailable".to_string()
    } else {
        let filter = normalized_files_filter(&files.filter_query);
        if filter.is_empty() {
            plural(files.entries.len(), "entry")
        } else {
            let shown = filtered_file_indices(&files.entries, &filter).len();
            format!("{shown}/{} entries", files.entries.len())
        }
    }
}

fn files_selection_status(files: &FilesState) -> String {
    let marked = files.marked_count();
    if marked > 0 {
        return if marked == 1 {
            "1 entry marked".to_string()
        } else {
            format!("{marked} entries marked")
        };
    }
    files
        .selected_entry()
        .map(|entry| format!("selected {}", entry.path))
        .unwrap_or_else(|| "no selection".to_string())
}

fn files_status_bar_selection(files: &FilesState) -> String {
    let marked = files.marked_count();
    if marked > 0 {
        return if marked == 1 {
            "1 marked".to_string()
        } else {
            format!("{marked} marked")
        };
    }
    files
        .selected_entry()
        .map(|entry| middle_short(&entry.name, 34))
        .unwrap_or_else(|| "no selection".to_string())
}

fn files_task_status(files: &FilesState) -> String {
    let running = files
        .tasks
        .iter()
        .filter(|task| task.state == FilesTaskState::Running)
        .count();
    let cancelling = files
        .tasks
        .iter()
        .filter(|task| task.state == FilesTaskState::CancelRequested)
        .count();
    let failed = files
        .tasks
        .iter()
        .filter(|task| task.state == FilesTaskState::Failed)
        .count();
    let cancelled = files
        .tasks
        .iter()
        .filter(|task| task.state == FilesTaskState::Cancelled)
        .count();
    let completed = files
        .tasks
        .iter()
        .filter(|task| task.state == FilesTaskState::Completed)
        .count();
    if running > 0 {
        if running == 1 {
            "1 task running".to_string()
        } else {
            format!("{running} tasks running")
        }
    } else if cancelling > 0 {
        if cancelling == 1 {
            "1 task cancelling".to_string()
        } else {
            format!("{cancelling} tasks cancelling")
        }
    } else if failed > 0 {
        if failed == 1 {
            "1 task failed".to_string()
        } else {
            format!("{failed} tasks failed")
        }
    } else if cancelled > 0 {
        if cancelled == 1 {
            "1 task cancelled".to_string()
        } else {
            format!("{cancelled} tasks cancelled")
        }
    } else if completed > 0 {
        if completed == 1 {
            "1 task done".to_string()
        } else {
            format!("{completed} tasks done")
        }
    } else {
        "idle".to_string()
    }
}

fn files_tasks_signature(tasks: &[FilesTask]) -> String {
    tasks
        .iter()
        .take(6)
        .map(|task| {
            format!(
                "{}:{:?}:{:.2}:{}",
                task.id,
                task.state,
                task.progress.unwrap_or(-1.0),
                task.detail
            )
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn files_tasks_have_finished(tasks: &[FilesTask]) -> bool {
    tasks.iter().any(|task| {
        matches!(
            task.state,
            FilesTaskState::Cancelled | FilesTaskState::Completed | FilesTaskState::Failed
        )
    })
}

fn files_task_queue_summary(tasks: &[FilesTask]) -> String {
    let running = tasks
        .iter()
        .filter(|task| task.state == FilesTaskState::Running)
        .count();
    let cancelling = tasks
        .iter()
        .filter(|task| task.state == FilesTaskState::CancelRequested)
        .count();
    let failed = tasks
        .iter()
        .filter(|task| task.state == FilesTaskState::Failed)
        .count();
    if running > 0 || cancelling > 0 {
        let active = running + cancelling;
        if active == 1 {
            "1 active".to_string()
        } else {
            format!("{active} active")
        }
    } else if failed > 0 {
        if failed == 1 {
            "1 failed".to_string()
        } else {
            format!("{failed} failed")
        }
    } else if tasks.is_empty() {
        "idle".to_string()
    } else if tasks.len() == 1 {
        "1 finished".to_string()
    } else {
        format!("{} finished", tasks.len())
    }
}

fn files_status_summary(files: &FilesState) -> String {
    format!(
        "{} | Path {} | {} | {} | {}",
        files_source_status(files),
        files.path,
        files_entry_count_status(files),
        files_selection_status(files),
        files_task_status(files),
    )
}

fn push_unique_place(
    places: &mut Vec<FilePlace>,
    label: impl Into<String>,
    detail: impl Into<String>,
    source: FilesSourceKind,
    path: impl Into<String>,
    glyph: IconGlyph,
) {
    push_unique_place_with_action(
        places,
        label,
        detail,
        source,
        path,
        glyph,
        FilePlaceAction::OpenPath,
    );
}

fn push_unique_place_with_action(
    places: &mut Vec<FilePlace>,
    label: impl Into<String>,
    detail: impl Into<String>,
    source: FilesSourceKind,
    path: impl Into<String>,
    glyph: IconGlyph,
    action: FilePlaceAction,
) {
    let path = path.into();
    if places
        .iter()
        .any(|place| place.source == source && place.path == path && place.action == action)
    {
        return;
    }
    places.push(FilePlace {
        label: label.into(),
        detail: detail.into(),
        source,
        path,
        glyph,
        action,
    });
}

fn local_home_path() -> Option<String> {
    std::env::var_os("USERPROFILE")
        .or_else(|| std::env::var_os("HOME"))
        .map(PathBuf::from)
        .map(|path| path.to_string_lossy().into_owned())
}

fn local_root_path(path: &str) -> Option<String> {
    let mut root = PathBuf::new();
    let mut saw_prefix = false;
    for component in Path::new(path).components() {
        match component {
            std::path::Component::Prefix(prefix) => {
                root.push(prefix.as_os_str());
                saw_prefix = true;
            }
            std::path::Component::RootDir => {
                root.push(std::path::MAIN_SEPARATOR.to_string());
                let label = root.to_string_lossy().into_owned();
                return (!label.is_empty()).then_some(display_local_path(label));
            }
            _ => break,
        }
    }
    if saw_prefix {
        let label = root.to_string_lossy().into_owned();
        (!label.is_empty()).then_some(display_local_path(label))
    } else {
        None
    }
}

fn display_local_path(path: String) -> String {
    #[cfg(windows)]
    {
        if let Some(rest) = path.strip_prefix(r"\\?\UNC\") {
            return format!(r"\\{rest}");
        }
        if let Some(rest) = path.strip_prefix(r"\\?\") {
            return rest.to_string();
        }
    }
    path
}

fn local_root_label(path: &str) -> String {
    let trimmed = path.trim_end_matches(['\\', '/']);
    if trimmed.ends_with(':') {
        return trimmed.to_string();
    }
    if path == "/" {
        return "Filesystem".to_string();
    }
    Path::new(trimmed)
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .filter(|label| !label.is_empty())
        .unwrap_or_else(|| trimmed.to_string())
}

fn system_drive_path() -> Option<String> {
    #[cfg(windows)]
    {
        std::env::var_os("SystemDrive").map(|drive| {
            let mut path = drive.to_string_lossy().into_owned();
            if !path.ends_with('\\') && !path.ends_with('/') {
                path.push('\\');
            }
            path
        })
    }
    #[cfg(not(windows))]
    {
        None
    }
}

fn files_places(files: &FilesState) -> Vec<FilePlace> {
    let mut places = Vec::new();
    push_unique_place(
        &mut places,
        "SIFS root",
        "cluster namespace",
        FilesSourceKind::Sifs,
        "/",
        IconGlyph::Storage,
    );
    push_unique_place(
        &mut places,
        "Services",
        "service data",
        FilesSourceKind::Sifs,
        "/services",
        IconGlyph::Storage,
    );
    push_unique_place(
        &mut places,
        "Users",
        "user data",
        FilesSourceKind::Sifs,
        "/users",
        IconGlyph::Folder,
    );
    push_unique_place(
        &mut places,
        "Sinomo",
        "runtime data",
        FilesSourceKind::Sifs,
        "/sinomo",
        IconGlyph::Folder,
    );
    push_unique_place(
        &mut places,
        "Cluster nodes",
        "remote node files",
        FilesSourceKind::Sifs,
        "/sinomo/nodes",
        IconGlyph::Storage,
    );
    if !files.node_id.trim().is_empty() {
        push_unique_place(
            &mut places,
            "This node files",
            "local-owned SIFS",
            FilesSourceKind::Sifs,
            format!("/sinomo/nodes/{}/files", files.node_id),
            IconGlyph::Folder,
        );
    }
    push_unique_place_with_action(
        &mut places,
        "SIFS path",
        "cluster path",
        FilesSourceKind::Sifs,
        files.sifs_path.clone(),
        IconGlyph::Search,
        FilePlaceAction::OpenPathPrompt,
    );
    push_unique_place(
        &mut places,
        "Current local",
        "host filesystem",
        FilesSourceKind::Local,
        files.local_path.clone(),
        IconGlyph::Folder,
    );
    if let Some(root) = local_root_path(&files.local_path) {
        push_unique_place(
            &mut places,
            local_root_label(&root),
            "current volume",
            FilesSourceKind::Local,
            root,
            IconGlyph::Storage,
        );
    }
    push_unique_place(
        &mut places,
        "Sinomo data",
        "local data dir",
        FilesSourceKind::Local,
        files.data_dir.root().to_string_lossy().into_owned(),
        IconGlyph::Folder,
    );
    if let Some(home) = local_home_path() {
        push_unique_place(
            &mut places,
            "Home",
            "local profile",
            FilesSourceKind::Local,
            home,
            IconGlyph::Folder,
        );
    }
    if let Some(system_drive) = system_drive_path() {
        push_unique_place(
            &mut places,
            local_root_label(&system_drive),
            "system drive",
            FilesSourceKind::Local,
            system_drive,
            IconGlyph::Storage,
        );
    }
    push_unique_place_with_action(
        &mut places,
        "Other path",
        "drive, UNC, or mount",
        FilesSourceKind::Local,
        files.local_path.clone(),
        IconGlyph::Search,
        FilePlaceAction::OpenPathPrompt,
    );
    places
}

fn file_place_active(files: &FilesState, place: &FilePlace) -> bool {
    place.action == FilePlaceAction::OpenPath
        && files.source == place.source
        && files.path == place.path
}

fn open_file_place(shared: &Shared, place: FilePlace, ctx: &mut EventCtx) {
    let waker = { shared.borrow().waker.clone() };
    {
        let mut files = shared.files_mut();
        match place.action {
            FilePlaceAction::OpenPath => files.open_location(place.source, place.path, waker),
            FilePlaceAction::OpenPathPrompt => {
                if files.source != place.source {
                    files.switch_source(place.source, waker);
                }
                files.open_action_prompt(FileActionPromptKind::GoTo);
            }
        }
    }
    request_files_overlay_rebuild(ctx);
}

fn plural(count: usize, singular: &str) -> String {
    if count == 1 {
        format!("1 {singular}")
    } else if singular == "entry" {
        format!("{count} entries")
    } else {
        format!("{count} {singular}s")
    }
}

fn marked_entries_label(count: usize) -> String {
    if count == 1 {
        "1 marked entry".to_string()
    } else {
        format!("{count} marked entries")
    }
}

struct FilesListHeader {
    shared: Shared,
}

impl FilesListHeader {
    fn new(shared: Shared) -> Self {
        Self { shared }
    }
}

impl Widget for FilesListHeader {
    fn measure(&mut self, _ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        constraints.clamp(Size::new(280.0, LIST_HEADER_H))
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let theme = self.shared.theme();
        let pal = palette(&self.shared);
        let bounds = ctx.bounds();
        let files = self.shared.files();

        ctx.fill_rect(bounds, pal.sidebar);
        widgets::hairline_bottom(ctx, bounds, pal.border);

        let x = bounds.x() + CONTENT_PAD;
        let y = bounds.y() + 8.0;
        let content_w = (bounds.width() - CONTENT_PAD * 2.0).max(0.0);
        let action_w = row_actions_width();
        let meta_w = 88.0_f32.min(content_w * 0.28).max(0.0);
        let action_x = bounds.max_x() - CONTENT_PAD - action_w;
        let meta_x = (action_x - meta_w).max(x + 120.0);
        let title_w = (meta_x - x - 12.0).max(0.0);

        let label_style = pal.text_style(&theme, 11.0, FontWeight::SEMIBOLD, pal.text_faint);
        widgets::draw_text_in(
            ctx,
            Rect::new(x, y, title_w, 16.0),
            "Name",
            label_style.clone(),
        );
        let count_x = (x + 44.0).min(x + title_w);
        widgets::draw_text_in(
            ctx,
            Rect::new(count_x, y, (title_w - 44.0).max(0.0), 16.0),
            &files_entry_count_status(&files),
            pal.text_style(&theme, 10.5, FontWeight::MEDIUM, pal.text_muted),
        );

        widgets::draw_text_in(
            ctx,
            Rect::new(meta_x, y, meta_w, 16.0),
            "Kind / size",
            label_style.clone(),
        );
        if content_w > 520.0 {
            widgets::draw_text_in(
                ctx,
                Rect::new(action_x, y, action_w, 16.0),
                "Actions",
                label_style,
            );
        }
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let files = self.shared.files();
        let summary = format!(
            "{} list columns: Name, Kind / size, Actions. Path {}. {}.",
            files.source.label(),
            files.path,
            files_entry_count_status(&files)
        );
        let mut node = SemanticsNode::new(ctx.widget_id(), SemanticsRole::Text, ctx.bounds());
        node.name = Some("Files list columns".to_string());
        node.value = Some(SemanticsValue::Text(
            "Name | Kind / size | Actions".to_string(),
        ));
        node.description = Some(summary);
        ctx.push(node);
    }
}

struct FilesStatusBar {
    shared: Shared,
}

impl FilesStatusBar {
    fn new(shared: Shared) -> Self {
        Self { shared }
    }

    fn chip_color(files: &FilesState, pal: &Palette) -> Color {
        if files.error.is_some() {
            pal.warn
        } else if files.loading {
            pal.accent
        } else if files.source == FilesSourceKind::Local {
            pal.good
        } else if files.clustered {
            pal.good
        } else {
            pal.text_faint
        }
    }
}

impl Widget for FilesStatusBar {
    fn measure(&mut self, _ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        constraints.clamp(Size::new(280.0, FILES_STATUS_H))
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let theme = self.shared.theme();
        let pal = palette(&self.shared);
        let bounds = ctx.bounds();
        let files = self.shared.files();

        ctx.fill_rect(bounds, pal.sidebar);
        widgets::hairline_top(ctx, bounds, pal.border);

        let x = bounds.x() + CONTENT_PAD;
        let y = bounds.y() + 7.0;
        let chip = Rect::new(x, y, 112.0_f32.min(bounds.width() * 0.28).max(86.0), 24.0);
        let chip_tone = Self::chip_color(&files, &pal);
        widgets::round_panel(ctx, chip, pal.field, chip_tone, 12.0);
        widgets::fill_round_rect(
            ctx,
            Rect::new(chip.x() + 10.0, chip.y() + 9.0, 6.0, 6.0),
            chip_tone,
            3.0,
        );
        widgets::draw_text_in(
            ctx,
            Rect::new(
                chip.x() + 22.0,
                chip.y(),
                chip.width() - 30.0,
                chip.height(),
            ),
            files_source_status(&files),
            pal.text_style(&theme, 11.0, FontWeight::SEMIBOLD, pal.text),
        );

        let right_w = 190.0_f32.min(bounds.width() * 0.28).max(0.0);
        let right = Rect::new(bounds.max_x() - CONTENT_PAD - right_w, y, right_w, 24.0);
        widgets::draw_text_in(
            ctx,
            right,
            &files_task_status(&files),
            pal.text_style(&theme, 11.0, FontWeight::MEDIUM, pal.text_faint),
        );

        let info_x = chip.max_x() + 12.0;
        let info_w = (right.x() - info_x - 12.0).max(0.0);
        let selected = files_status_bar_selection(&files);
        let info = format!(
            "{} | {} | {}",
            middle_short(&files.path, 64),
            files_entry_count_status(&files),
            selected,
        );
        widgets::draw_text_in(
            ctx,
            Rect::new(info_x, y, info_w, 24.0),
            &info,
            pal.text_style(&theme, 11.0, FontWeight::NORMAL, pal.text_muted),
        );
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let files = self.shared.files();
        let summary = files_status_summary(&files);
        let mut node = SemanticsNode::new(ctx.widget_id(), SemanticsRole::Text, ctx.bounds());
        node.name = Some("Files status".to_string());
        node.value = Some(SemanticsValue::Text(summary.clone()));
        node.description = Some(summary);
        ctx.push(node);
    }
}

struct FilesPlacesRail {
    shared: Shared,
    hovered: Option<usize>,
}

impl FilesPlacesRail {
    fn new(shared: Shared) -> Self {
        Self {
            shared,
            hovered: None,
        }
    }

    fn row_rect(bounds: Rect, index: usize) -> Rect {
        Rect::new(
            bounds.x() + 8.0,
            bounds.y() + 40.0 + index as f32 * PLACE_ROW_H,
            (bounds.width() - 16.0).max(0.0),
            PLACE_ROW_H - 6.0,
        )
    }

    fn place_widget_id(parent: sui::WidgetId, index: usize) -> sui::WidgetId {
        sui::WidgetId::new(
            parent
                .get()
                .wrapping_mul(43)
                .wrapping_add(12_401)
                .wrapping_add(index as u64),
        )
    }

    fn content_height(shared: &Shared) -> f32 {
        let count = {
            let files = shared.files();
            files_places(&files).len()
        };
        48.0 + count as f32 * PLACE_ROW_H + 8.0
    }

    fn place_at(&self, bounds: Rect, position: Point) -> Option<usize> {
        let places = {
            let files = self.shared.files();
            files_places(&files)
        };
        places
            .iter()
            .enumerate()
            .find(|(index, _)| Self::row_rect(bounds, *index).contains(position))
            .map(|(index, _)| index)
    }
}

impl Widget for FilesPlacesRail {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        match event {
            Event::Pointer(pointer) if pointer.kind == PointerEventKind::Move => {
                let hovered = self.place_at(ctx.bounds(), pointer.position);
                if self.hovered != hovered {
                    self.hovered = hovered;
                    ctx.request_paint();
                    ctx.request_semantics();
                }
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Up
                    && (pointer.button == Some(PointerButton::Primary)
                        || pointer.button.is_none()) =>
            {
                if let Some(index) = self.place_at(ctx.bounds(), pointer.position) {
                    let place = {
                        let files = self.shared.files();
                        files_places(&files).get(index).cloned()
                    };
                    if let Some(place) = place {
                        open_file_place(&self.shared, place, ctx);
                        self.hovered = None;
                        ctx.set_handled();
                    }
                }
            }
            Event::Pointer(pointer)
                if matches!(
                    pointer.kind,
                    PointerEventKind::Leave | PointerEventKind::Cancel
                ) =>
            {
                if self.hovered.take().is_some() {
                    ctx.request_paint();
                    ctx.request_semantics();
                }
            }
            _ => {}
        }
    }

    fn measure(&mut self, _ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        constraints.clamp(Size::new(PLACES_W, Self::content_height(&self.shared)))
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let theme = self.shared.theme();
        let pal = palette(&self.shared);
        let bounds = ctx.bounds();
        let files = self.shared.files();
        let places = files_places(&files);

        ctx.fill_rect(bounds, pal.sidebar);
        widgets::hairline_right(ctx, bounds, pal.border);
        widgets::draw_text_in(
            ctx,
            Rect::new(
                bounds.x() + CONTENT_PAD,
                bounds.y() + 14.0,
                bounds.width(),
                16.0,
            ),
            "PLACES",
            pal.text_style(&theme, 11.0, FontWeight::SEMIBOLD, pal.text_faint),
        );

        for (index, place) in places.iter().enumerate() {
            let row = Self::row_rect(bounds, index);
            let active = file_place_active(&files, place);
            let hovered = self.hovered == Some(index);
            let fill = if active {
                pal.selected
            } else if hovered {
                pal.hover
            } else {
                pal.sidebar
            };
            let border = if active { pal.accent } else { fill };
            widgets::round_panel(ctx, row, fill, border, 7.0);
            let icon_rect = Rect::new(row.x() + 10.0, row.y() + 9.0, 16.0, 16.0);
            let icon_color = if active { pal.accent } else { pal.text_muted };
            draw_glyph(ctx, place.glyph, icon_rect, icon_color);
            widgets::draw_text_in(
                ctx,
                Rect::new(row.x() + 34.0, row.y() + 5.0, row.width() - 42.0, 15.0),
                &place.label,
                pal.text_style(&theme, 11.0, FontWeight::SEMIBOLD, pal.text),
            );
            widgets::draw_text_in(
                ctx,
                Rect::new(row.x() + 34.0, row.y() + 20.0, row.width() - 42.0, 14.0),
                &place.detail,
                pal.text_style(&theme, 10.0, FontWeight::NORMAL, pal.text_faint),
            );
        }
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let files = self.shared.files();
        let places = files_places(&files);
        let root_id = ctx.widget_id();
        let mut root = SemanticsNode::new(root_id, SemanticsRole::Text, ctx.bounds());
        root.name = Some("Files places".to_string());
        root.description = Some("Pinned SIFS and local filesystem locations".to_string());
        ctx.push(root);

        for (index, place) in places.iter().enumerate() {
            let mut node = SemanticsNode::new(
                Self::place_widget_id(root_id, index),
                SemanticsRole::Button,
                Self::row_rect(ctx.bounds(), index),
            );
            node.parent = Some(root_id);
            node.name = Some(format!("Open {}", place.label));
            node.description = Some(format!("{}: {}", place.source.path_label(), place.path));
            node.actions = vec![SemanticsAction::Activate];
            node.state.hovered = self.hovered == Some(index);
            node.state.selected = file_place_active(&files, place);
            ctx.push(node);
        }
    }
}

struct FilesTaskStrip {
    shared: Shared,
    hovered_cancel: Option<u64>,
    hovered_recovery: Option<u64>,
    hovered_clear: bool,
}

impl FilesTaskStrip {
    fn new(shared: Shared) -> Self {
        Self {
            shared,
            hovered_cancel: None,
            hovered_recovery: None,
            hovered_clear: false,
        }
    }

    fn has_finished_tasks(files: &FilesState) -> bool {
        files.tasks.iter().any(|task| {
            matches!(
                task.state,
                FilesTaskState::Cancelled | FilesTaskState::Completed | FilesTaskState::Failed
            )
        })
    }

    fn clear_rect(bounds: Rect) -> Rect {
        Rect::new(
            bounds.max_x() - CONTENT_PAD - 100.0,
            bounds.y() + 7.0,
            100.0,
            22.0,
        )
    }

    fn cancel_rect(bounds: Rect, row_index: usize) -> Rect {
        Rect::new(
            bounds.max_x() - CONTENT_PAD - INSPECTOR_TASK_CANCEL_W,
            bounds.y() + 34.0 + row_index as f32 * 24.0,
            INSPECTOR_TASK_CANCEL_W,
            22.0,
        )
    }

    fn recovery_rect(bounds: Rect, row_index: usize) -> Rect {
        Rect::new(
            bounds.max_x() - CONTENT_PAD - 92.0,
            bounds.y() + 33.0 + row_index as f32 * 24.0,
            92.0,
            22.0,
        )
    }

    fn task_has_recovery(task: &FilesTask) -> bool {
        task.source == FilesSourceKind::Sifs && task.state == FilesTaskState::Failed
    }

    fn detail_display(task: &FilesTask, width: f32) -> String {
        let detail = files_task_detail_display(task);
        let detail = if Self::task_has_recovery(task) {
            detail
                .strip_suffix(" in Connection")
                .unwrap_or(detail.as_str())
        } else {
            detail.as_str()
        };
        display_name_for_width(detail, width, 6.2)
    }

    fn cancel_widget_id(parent: sui::WidgetId, task_id: u64) -> sui::WidgetId {
        sui::WidgetId::new(
            parent
                .get()
                .wrapping_mul(47)
                .wrapping_add(10_201)
                .wrapping_add(task_id),
        )
    }

    fn clear_widget_id(parent: sui::WidgetId) -> sui::WidgetId {
        sui::WidgetId::new(parent.get().wrapping_mul(47).wrapping_add(10_199))
    }

    fn recovery_widget_id(parent: sui::WidgetId, task_id: u64) -> sui::WidgetId {
        sui::WidgetId::new(
            parent
                .get()
                .wrapping_mul(47)
                .wrapping_add(10_301)
                .wrapping_add(task_id),
        )
    }

    fn task_at_cancel(&self, bounds: Rect, position: Point) -> Option<u64> {
        let files = self.shared.files();
        files
            .tasks
            .iter()
            .take(2)
            .enumerate()
            .find(|(index, task)| {
                task.state.can_cancel() && Self::cancel_rect(bounds, *index).contains(position)
            })
            .map(|(_, task)| task.id)
    }

    fn task_at_recovery(&self, bounds: Rect, position: Point) -> Option<u64> {
        let files = self.shared.files();
        files
            .tasks
            .iter()
            .take(2)
            .enumerate()
            .find(|(index, task)| {
                Self::task_has_recovery(task)
                    && Self::recovery_rect(bounds, *index).contains(position)
            })
            .map(|(_, task)| task.id)
    }

    fn clear_hit(&self, bounds: Rect, position: Point) -> bool {
        let files = self.shared.files();
        Self::has_finished_tasks(&files) && Self::clear_rect(bounds).contains(position)
    }
}

impl Widget for FilesTaskStrip {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        match event {
            Event::Pointer(pointer) if pointer.kind == PointerEventKind::Move => {
                let hovered = self.task_at_cancel(ctx.bounds(), pointer.position);
                let hovered_recovery = self.task_at_recovery(ctx.bounds(), pointer.position);
                let hovered_clear = self.clear_hit(ctx.bounds(), pointer.position);
                if self.hovered_cancel != hovered
                    || self.hovered_recovery != hovered_recovery
                    || self.hovered_clear != hovered_clear
                {
                    self.hovered_cancel = hovered;
                    self.hovered_recovery = hovered_recovery;
                    self.hovered_clear = hovered_clear;
                    ctx.request_paint();
                    ctx.request_semantics();
                }
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Up
                    && (pointer.button == Some(PointerButton::Primary)
                        || pointer.button.is_none()) =>
            {
                if self.clear_hit(ctx.bounds(), pointer.position) {
                    self.shared.files_mut().clear_finished_tasks();
                    self.hovered_cancel = None;
                    self.hovered_clear = false;
                    ctx.request_measure();
                    ctx.request_arrange();
                    ctx.request_paint();
                    ctx.request_hit_test();
                    ctx.request_semantics();
                    ctx.set_handled();
                } else if self
                    .task_at_recovery(ctx.bounds(), pointer.position)
                    .is_some()
                {
                    open_settings_section(&self.shared, SettingsSection::Connection);
                    self.hovered_cancel = None;
                    self.hovered_recovery = None;
                    self.hovered_clear = false;
                    ctx.request_measure();
                    ctx.request_arrange();
                    ctx.request_paint();
                    ctx.request_hit_test();
                    ctx.request_semantics();
                    ctx.set_handled();
                } else if let Some(task_id) = self.task_at_cancel(ctx.bounds(), pointer.position) {
                    self.shared.files_mut().cancel_task(task_id);
                    self.hovered_cancel = None;
                    self.hovered_recovery = None;
                    self.hovered_clear = false;
                    ctx.request_paint();
                    ctx.request_semantics();
                    ctx.set_handled();
                }
            }
            Event::Pointer(pointer)
                if matches!(
                    pointer.kind,
                    PointerEventKind::Leave | PointerEventKind::Cancel
                ) =>
            {
                let changed = self.hovered_cancel.take().is_some()
                    || self.hovered_recovery.take().is_some()
                    || self.hovered_clear;
                self.hovered_clear = false;
                if changed {
                    ctx.request_paint();
                    ctx.request_semantics();
                }
            }
            _ => {}
        }
    }

    fn measure(&mut self, _ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        constraints.clamp(Size::new(280.0, LIST_TASK_STRIP_H))
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let theme = self.shared.theme();
        let pal = palette(&self.shared);
        let bounds = ctx.bounds();
        let files = self.shared.files();

        ctx.fill_rect(bounds, pal.sidebar);
        widgets::hairline_bottom(ctx, bounds, pal.border);
        widgets::draw_text_in(
            ctx,
            Rect::new(
                bounds.x() + CONTENT_PAD,
                bounds.y() + 10.0,
                bounds.width(),
                16.0,
            ),
            "TASKS",
            pal.text_style(&theme, 11.0, FontWeight::SEMIBOLD, pal.text_faint),
        );
        if Self::has_finished_tasks(&files) {
            let clear = Self::clear_rect(bounds);
            let fill = if self.hovered_clear {
                pal.hover
            } else {
                pal.field
            };
            widgets::round_panel(ctx, clear, fill, pal.border, 7.0);
            widgets::draw_text_in(
                ctx,
                Rect::new(
                    clear.x() + 10.0,
                    clear.y() + 3.0,
                    clear.width() - 20.0,
                    16.0,
                ),
                "Clear finished",
                pal.text_style(&theme, 10.5, FontWeight::MEDIUM, pal.text),
            );
        }

        let title_style = pal.text_style(&theme, 11.5, FontWeight::MEDIUM, pal.text);
        let detail_style = pal.text_style(&theme, 10.5, FontWeight::NORMAL, pal.text_faint);
        for (index, task) in files.tasks.iter().take(2).enumerate() {
            let row_y = bounds.y() + 32.0 + index as f32 * 24.0;
            let cancel = Self::cancel_rect(bounds, index);
            let recovery = Self::recovery_rect(bounds, index);
            let text_right = if task.state.can_cancel() {
                cancel.x() - 8.0
            } else if Self::task_has_recovery(task) {
                recovery.x() - 8.0
            } else {
                bounds.max_x() - CONTENT_PAD
            };
            let text_w = (text_right - bounds.x() - CONTENT_PAD).max(0.0);
            let tone = match task.state {
                FilesTaskState::Running | FilesTaskState::CancelRequested => pal.accent,
                FilesTaskState::Completed | FilesTaskState::Cancelled => pal.good,
                FilesTaskState::Failed => pal.warn,
            };
            let dot = Rect::new(bounds.x() + CONTENT_PAD, row_y + 5.0, 6.0, 6.0);
            widgets::fill_round_rect(ctx, dot, tone, 3.0);
            widgets::draw_text_in(
                ctx,
                Rect::new(bounds.x() + CONTENT_PAD + 12.0, row_y, text_w, 14.0),
                &format!("{} · {}", task.title, task.state.label()),
                title_style.clone(),
            );
            widgets::draw_text_in(
                ctx,
                Rect::new(bounds.x() + CONTENT_PAD + 12.0, row_y + 13.0, text_w, 13.0),
                &Self::detail_display(task, text_w),
                detail_style.clone(),
            );
            let bar = Rect::new(
                bounds.x() + CONTENT_PAD + 12.0,
                row_y + 28.0,
                text_w.min(220.0),
                3.0,
            );
            if bar.width() > 0.0 {
                widgets::fill_round_rect(ctx, bar, pal.field, 1.5);
                let fraction = task_progress_fraction(task);
                widgets::fill_round_rect(
                    ctx,
                    Rect::new(bar.x(), bar.y(), bar.width() * fraction, bar.height()),
                    tone,
                    1.5,
                );
            }
            if task.state.can_cancel() {
                let fill = if self.hovered_cancel == Some(task.id) {
                    pal.hover
                } else {
                    pal.field
                };
                widgets::round_panel(ctx, cancel, fill, pal.border, 7.0);
                widgets::draw_text_in(
                    ctx,
                    Rect::new(
                        cancel.x() + 8.0,
                        cancel.y() + 3.0,
                        cancel.width() - 16.0,
                        16.0,
                    ),
                    "Cancel",
                    pal.text_style(&theme, 10.5, FontWeight::MEDIUM, pal.text),
                );
            } else if Self::task_has_recovery(task) {
                let fill = if self.hovered_recovery == Some(task.id) {
                    pal.hover
                } else {
                    pal.field
                };
                widgets::round_panel(ctx, recovery, fill, pal.border, 7.0);
                draw_glyph(
                    ctx,
                    IconGlyph::Alert,
                    Rect::new(recovery.x() + 8.0, recovery.y() + 5.0, 12.0, 12.0),
                    pal.warn,
                );
                widgets::draw_text_in(
                    ctx,
                    Rect::new(
                        recovery.x() + 24.0,
                        recovery.y() + 3.0,
                        recovery.width() - 30.0,
                        16.0,
                    ),
                    "Connection",
                    pal.text_style(&theme, 10.5, FontWeight::MEDIUM, pal.text),
                );
            }
        }
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let files = self.shared.files();
        if Self::has_finished_tasks(&files) {
            let mut node = SemanticsNode::new(
                Self::clear_widget_id(ctx.widget_id()),
                SemanticsRole::Button,
                Self::clear_rect(ctx.bounds()),
            );
            node.parent = Some(ctx.widget_id());
            node.name = Some("Clear finished tasks".to_string());
            node.description =
                Some("Dismiss completed, failed, and cancelled file operations".to_string());
            node.actions = vec![SemanticsAction::Activate];
            node.state.hovered = self.hovered_clear;
            ctx.push(node);
        }
        for (index, task) in files
            .tasks
            .iter()
            .take(2)
            .enumerate()
            .filter(|(_, task)| Self::task_has_recovery(task))
        {
            let mut node = SemanticsNode::new(
                Self::recovery_widget_id(ctx.widget_id(), task.id),
                SemanticsRole::Button,
                Self::recovery_rect(ctx.bounds(), index),
            );
            node.parent = Some(ctx.widget_id());
            node.name = Some(format!("Open Connection settings for {}", task.title));
            node.description = Some(format!("{}: {}", task.title, task.detail));
            node.actions = vec![SemanticsAction::Activate];
            node.state.hovered = self.hovered_recovery == Some(task.id);
            ctx.push(node);
        }
        for (index, task) in files
            .tasks
            .iter()
            .take(2)
            .enumerate()
            .filter(|(_, task)| task.state.can_cancel())
        {
            let mut node = SemanticsNode::new(
                Self::cancel_widget_id(ctx.widget_id(), task.id),
                SemanticsRole::Button,
                Self::cancel_rect(ctx.bounds(), index),
            );
            node.parent = Some(ctx.widget_id());
            node.name = Some(format!("Cancel {}", task.title));
            node.description = Some(task.detail.clone());
            node.actions = vec![SemanticsAction::Activate];
            ctx.push(node);
        }
    }
}

fn task_progress_fraction(task: &FilesTask) -> f32 {
    if let Some(progress) = task.progress {
        return progress.clamp(0.0, 1.0);
    }
    match task.state {
        FilesTaskState::Running => 0.45,
        FilesTaskState::CancelRequested => 0.75,
        FilesTaskState::Cancelled | FilesTaskState::Completed | FilesTaskState::Failed => 1.0,
    }
}

// ---------------------------------------------------------------------------
// Inspector — a live-reading custom widget (selection changes without a path change).
// ---------------------------------------------------------------------------

/// Renders the SIFS inspector for the *currently selected* entry, live-reading
/// `shared.files()` in `paint`. Diffs a summary on each `RedrawRequested` to self-invalidate so a
/// selection (which the `ListView` reports with only a repaint request) refreshes the inspector
/// without rebuilding the list. Mirrors the Settings `StatusReflector` pattern.
///
/// Kept as a bespoke painter (rather than a `PanelSection`/`PropertyRow` tree) because its content
/// is fully selection-driven and re-reads live: composing it from reactive `Label`s would still
/// need a per-frame diff to relayout when the *shape* changes (placement note appears/disappears),
/// so a single self-invalidating painter is simpler and matches `StatusReflector`.
struct InspectorView {
    shared: Shared,
    last_seen: RefCell<Option<String>>,
}

impl InspectorView {
    fn new(shared: Shared) -> Self {
        Self {
            shared,
            last_seen: RefCell::new(None),
        }
    }

    /// A short, single-line summary used for change detection.
    fn summary(&self) -> String {
        let files = self.shared.files();
        let task_signature = files_tasks_signature(&files.tasks);
        if files.loading {
            return format!(
                "loading|{:?}|{}|{}|{}",
                files.source, files.path, files.inspector_details_open, task_signature
            );
        }
        if let Some(error) = &files.error {
            return format!(
                "error|{:?}|{}|{}|{}|{}",
                files.source, files.path, error, files.inspector_details_open, task_signature
            );
        }
        match files.selected_entry() {
            Some(entry) => format!(
                "{:?}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
                files.source,
                entry.path,
                entry.is_dir,
                entry.is_link,
                files.clustered,
                files.details_loading,
                files.details_error.as_deref().unwrap_or(""),
                files.preview_loading,
                files.preview_error.as_deref().unwrap_or(""),
                files.inspector_details_open,
                sifs_details_signature(files.selected_details.as_ref()),
                local_details_signature(files.selected_local_details.as_ref()),
                files_preview_signature(files.selected_preview.as_ref()),
                task_signature
            ),
            None => format!(
                "unselected|{:?}|{}|{}|{}|{}",
                files.source,
                files.path,
                files.clustered,
                files.entries.len(),
                task_signature
            ),
        }
    }

    fn measured_content_height(&self, _ctx: &mut MeasureCtx, width: f32) -> f32 {
        let w = (width - INSPECTOR_PAD * 2.0).max(0.0);
        let mut y = INSPECTOR_STATE_PANEL_Y;
        let files = self.shared.files();

        let finish = |end_y: f32| (end_y + INSPECTOR_BOTTOM_PAD).max(INSPECTOR_MIN_CONTENT_H);

        if files.loading {
            let rows = sifs_loading_rows(&files.path, files.inspector_details_open);
            let panel_h = Self::state_panel_height(w, &rows);
            return finish(y + panel_h);
        }

        if let Some(error) = &files.error {
            let rows = sifs_unavailable_rows(error, files.inspector_details_open);
            let panel_h = Self::state_panel_height(w, &rows);
            return finish(y + panel_h);
        }

        let Some(entry) = files.selected_entry() else {
            let rows = unselected_inspector_rows(&files);
            let panel_h = Self::state_panel_height(w, &rows);
            return finish(y + panel_h);
        };

        y += 26.0;
        y += 34.0;

        let actions = inspector_actions(&files, entry);
        if !actions.is_empty() {
            let action_start = INSPECTOR_PAD + 4.0 + 30.0 + 26.0 + 30.0;
            y = (action_start + inspector_actions_height(actions.len())).max(y) + 12.0;
        }

        y += Self::info_row_height(w, &entry.path);

        if files.source == FilesSourceKind::Local {
            y += 10.0 + 24.0;
            y += 26.0;
            y += 10.0 + Self::info_row_height(w, "Host filesystem");

            let local = files.selected_local_details.as_ref();
            if let Some(local) = local {
                y += 4.0
                    + Self::info_row_height(
                        w,
                        if local.readonly {
                            "read-only"
                        } else {
                            "writable"
                        },
                    );
                if let Some(canonical) = &local.canonical_path
                    && canonical != &local.path
                {
                    y += 4.0 + Self::info_row_height(w, canonical);
                }
            } else if let Some(error) = &files.details_error {
                y += 4.0 + Self::info_row_height(w, error);
            } else if files.details_loading {
                y += 4.0 + Self::info_row_height(w, "Reading metadata");
            }

            if files.inspector_details_open {
                y += 14.0 + 22.0;
                if let Some(local) = local {
                    y += Self::info_row_height(w, &local.kind);
                    y += 4.0
                        + Self::info_row_height(w, &local_time_label(local.accessed_at_unix_nanos));
                    y += 4.0
                        + Self::info_row_height(w, &local_time_label(local.modified_at_unix_nanos));
                    y += 4.0
                        + Self::info_row_height(w, &local_time_label(local.created_at_unix_nanos));
                }
            }

            if !entry.is_dir {
                let preview_error = files
                    .preview_error
                    .as_deref()
                    .map(|error| file_preview_error_display(files.source, error));
                y += 10.0
                    + Self::preview_section_height(
                        w,
                        files.preview_loading,
                        preview_error.as_deref(),
                        files.selected_preview.as_ref(),
                    );
            }

            return finish(y);
        }

        y += 10.0 + 24.0;
        y += 36.0;
        y += 26.0;

        let details = files.selected_details.as_ref();
        let placement = sifs_replication_summary(details, files.clustered);
        y += 10.0 + Self::info_row_height(w, &placement);
        let pins = sifs_pin_summary(details);
        y += 4.0 + Self::info_row_height(w, &pins);
        let next_step = sifs_inspector_next_step(
            details,
            files.clustered,
            files.details_loading,
            files.details_error.as_deref(),
        );
        y += 4.0 + Self::info_row_height(w, &next_step);

        if files.inspector_details_open {
            y += 14.0 + 22.0;
            if let Some(error) = &files.details_error {
                y += Self::info_row_height(w, error);
                y += 4.0 + Self::info_row_height(w, "SIFS through daemon");
            } else if files.details_loading {
                y += Self::info_row_height(w, "Waiting for daemon metadata");
            } else if details.is_none() {
                y += Self::info_row_height(w, "Metadata has not returned for this object yet");
                y += 4.0 + Self::info_row_height(w, "SIFS through daemon");
            }

            let policy = details
                .map(|details| details.policy.as_str())
                .unwrap_or("not loaded");
            y += 4.0 + Self::info_row_height(w, policy);
            let locations = details
                .map(details_locations)
                .unwrap_or_else(|| "not reported".to_string());
            y += 4.0 + Self::info_row_height(w, &locations);

            if let Some(details) = details {
                y += 4.0 + Self::info_row_height(w, &sifs_pin_detail_summary(details));
                y += 4.0 + Self::info_row_height(w, &details.file_system_id);
                y += 4.0 + Self::info_row_height(w, &short(&details.object_id, 28));
                y += 4.0 + Self::info_row_height(w, &short(&details.version, 28));
                y += 4.0
                    + Self::info_row_height(
                        w,
                        &format!("{:o} · {} links", details.mode, details.link_count),
                    );
                y += 4.0
                    + Self::info_row_height(
                        w,
                        &format!("uid {} · gid {}", details.uid, details.gid),
                    );
                y += 4.0
                    + Self::info_row_height(w, &unix_nanos_label(details.modified_at_unix_nanos));
                if let Some(hash) = &details.content_hash {
                    y += 4.0 + Self::info_row_height(w, &short(hash, 28));
                }
            }
        }

        if !files.clustered {
            let lines = Self::estimated_line_count(
                "Join a cluster to spread this object across nodes and tune replication.",
                w,
                7.0,
            );
            y += lines as f32 * 17.0;
        }

        if !entry.is_dir {
            let preview_error = files
                .preview_error
                .as_deref()
                .map(|error| file_preview_error_display(files.source, error));
            y += 10.0
                + Self::preview_section_height(
                    w,
                    files.preview_loading,
                    preview_error.as_deref(),
                    files.selected_preview.as_ref(),
                );
        }

        finish(y)
    }

    fn state_panel_height(w: f32, rows: &[(&str, String)]) -> f32 {
        let value_w = (w - 24.0).max(0.0);
        44.0 + rows
            .iter()
            .map(|(_, value)| {
                let lines = Self::estimated_line_count(value, value_w, 6.6);
                15.0 + (lines as f32 * 16.0) + 8.0
            })
            .sum::<f32>()
    }

    fn info_row_height(w: f32, value: &str) -> f32 {
        let lines = Self::estimated_line_count(value, w, 7.2);
        18.0 + lines as f32 * 17.0 + 6.0
    }

    fn estimated_line_count(value: &str, width: f32, avg_char_width: f32) -> usize {
        let chars_per_line = (width / avg_char_width).floor().max(8.0) as usize;
        value
            .split('\n')
            .map(|line| line.chars().count().max(1).div_ceil(chars_per_line))
            .sum::<usize>()
            .max(1)
    }

    fn preview_section_height(
        w: f32,
        loading: bool,
        error: Option<&str>,
        preview: Option<&FilesPreview>,
    ) -> f32 {
        let inner_w = (w - 24.0).max(0.0);
        let mut height = 42.0;
        if loading {
            return height + 18.0 + 12.0;
        }
        if let Some(error) = error {
            let lines = Self::estimated_line_count(error, inner_w, 7.0).min(3);
            return height + lines as f32 * 17.0 + 12.0;
        }
        let Some(preview) = preview else {
            return height + 18.0 + 12.0;
        };

        if preview.binary {
            let value = preview.lines.join(" ");
            let lines = Self::estimated_line_count(&value, inner_w, 7.0).min(3);
            height += lines as f32 * 17.0;
        } else {
            for line in preview.lines.iter().take(FILE_PREVIEW_MAX_LINES) {
                height += Self::estimated_line_count(line, inner_w, 7.0).min(2) as f32 * 17.0;
            }
        }
        if preview.truncated {
            height += 17.0;
        }
        height + 12.0
    }
}

const INSPECTOR_PAD: f32 = CONTENT_PAD;
const INSPECTOR_DETAIL_BUTTON_W: f32 = 104.0;
const INSPECTOR_DETAIL_BUTTON_H: f32 = 24.0;
const INSPECTOR_ACTION_BUTTON_H: f32 = 28.0;
const INSPECTOR_ACTION_GAP: f32 = 8.0;
const INSPECTOR_TASK_PANEL_H: f32 = 160.0;
const INSPECTOR_TASK_ROW_H: f32 = 29.0;
const INSPECTOR_TASK_CANCEL_W: f32 = 58.0;
const INSPECTOR_TASK_PANEL_Y: f32 = INSPECTOR_PAD + 34.0;
const INSPECTOR_STATE_PANEL_Y: f32 = INSPECTOR_TASK_PANEL_Y + INSPECTOR_TASK_PANEL_H + 12.0;
const INSPECTOR_MIN_CONTENT_H: f32 = INSPECTOR_STATE_PANEL_Y + 170.0;
const INSPECTOR_BOTTOM_PAD: f32 = 18.0;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InspectorAction {
    OpenFolder,
    Copy,
    CopyToSifs,
    CopyToLocal,
    Move,
    Rename,
    Delete,
    RefreshPlacement,
    RepairReplicas,
    PinContent,
    UnpinContent,
    ConnectionSettings,
}

impl InspectorAction {
    fn label(self) -> &'static str {
        match self {
            Self::OpenFolder => "Open folder",
            Self::Copy => "Copy",
            Self::CopyToSifs => "Copy to SIFS",
            Self::CopyToLocal => "Download",
            Self::Move => "Move",
            Self::Rename => "Rename",
            Self::Delete => "Delete",
            Self::RefreshPlacement => "Refresh placement",
            Self::RepairReplicas => "Repair replicas",
            Self::PinContent => "Pin content",
            Self::UnpinContent => "Unpin content",
            Self::ConnectionSettings => "Connection settings",
        }
    }

    fn glyph(self) -> IconGlyph {
        match self {
            Self::OpenFolder => IconGlyph::Folder,
            Self::Copy => IconGlyph::Restore,
            Self::CopyToSifs => IconGlyph::Storage,
            Self::CopyToLocal => IconGlyph::Download,
            Self::Move => IconGlyph::ChevronRight,
            Self::Rename => IconGlyph::MoreHorizontal,
            Self::Delete => IconGlyph::Trash,
            Self::RefreshPlacement => IconGlyph::Storage,
            Self::RepairReplicas => IconGlyph::Redo,
            Self::PinContent => IconGlyph::Lock,
            Self::UnpinContent => IconGlyph::Unlock,
            Self::ConnectionSettings => IconGlyph::Alert,
        }
    }

    fn prompt_kind(self) -> Option<FileActionPromptKind> {
        match self {
            Self::Copy => Some(FileActionPromptKind::Copy),
            Self::CopyToSifs => Some(FileActionPromptKind::CopyToSifs),
            Self::CopyToLocal => Some(FileActionPromptKind::CopyToLocal),
            Self::Move => Some(FileActionPromptKind::Move),
            Self::Rename => Some(FileActionPromptKind::Rename),
            Self::Delete => Some(FileActionPromptKind::Delete),
            Self::OpenFolder
            | Self::RefreshPlacement
            | Self::RepairReplicas
            | Self::PinContent
            | Self::UnpinContent
            | Self::ConnectionSettings => None,
        }
    }

    fn semantic_name(self, entry: &SifsEntry) -> String {
        match self {
            Self::OpenFolder => "Open folder".to_string(),
            Self::RefreshPlacement => format!("Refresh placement {}", entry.name),
            Self::RepairReplicas => format!("Repair replicas {}", entry.name),
            Self::PinContent => format!("Pin content {}", entry.name),
            Self::UnpinContent => format!("Unpin content {}", entry.name),
            Self::ConnectionSettings => "Connection settings".to_string(),
            Self::CopyToSifs => format!("Copy to SIFS {}", entry.name),
            Self::CopyToLocal => format!("Download {}", entry.name),
            Self::Copy | Self::Move | Self::Rename | Self::Delete => {
                format!("{} {}", self.label(), entry.name)
            }
        }
    }
}

fn inspector_actions(files: &FilesState, entry: &SifsEntry) -> Vec<InspectorAction> {
    let mut actions = Vec::with_capacity(8);
    if entry.is_dir {
        actions.push(InspectorAction::OpenFolder);
    }
    actions.push(InspectorAction::Copy);
    match files.source {
        FilesSourceKind::Local => actions.push(InspectorAction::CopyToSifs),
        FilesSourceKind::Sifs => actions.push(InspectorAction::CopyToLocal),
    }
    actions.push(InspectorAction::Move);
    actions.push(InspectorAction::Rename);
    actions.push(InspectorAction::Delete);
    if files.source == FilesSourceKind::Sifs {
        if files
            .details_error
            .as_deref()
            .is_some_and(sifs_metadata_error_needs_connection)
        {
            actions.push(InspectorAction::ConnectionSettings);
        }
        actions.push(InspectorAction::RefreshPlacement);
        actions.push(InspectorAction::RepairReplicas);
        if !entry.is_dir && !entry.is_link {
            actions.push(InspectorAction::PinContent);
            actions.push(InspectorAction::UnpinContent);
        }
    }
    actions
}

fn sifs_loading_rows(path: &str, details_open: bool) -> Vec<(&'static str, String)> {
    let mut rows = vec![("Status", "Connecting to SIFS".to_string())];
    if details_open {
        rows.push(("Path", path.to_string()));
        rows.push(("Authority", "SIFS through daemon".to_string()));
    }
    rows
}

fn sifs_unavailable_rows(error: &str, details_open: bool) -> Vec<(&'static str, String)> {
    let mut rows = vec![
        ("Status", sifs_unavailable_summary(error).to_string()),
        (
            "Available",
            "Local files, drives, UNC paths, and mounted paths still work.".to_string(),
        ),
        (
            "Next",
            "Retry SIFS, open Connection, or browse local files".to_string(),
        ),
    ];
    if details_open {
        rows.push(("Reason", error.to_string()));
        rows.push(("Authority", "SIFS through daemon".to_string()));
    }
    rows
}

fn inspector_detail_button_label(details_open: bool) -> &'static str {
    if details_open {
        "Hide metadata"
    } else {
        "Show metadata"
    }
}

fn inspector_detail_button_rect(bounds: Rect) -> Rect {
    let content_x = bounds.x() + INSPECTOR_PAD;
    let content_w = (bounds.width() - INSPECTOR_PAD * 2.0).max(0.0);
    let button_w = INSPECTOR_DETAIL_BUTTON_W.min((content_w - 24.0).max(0.0));
    Rect::new(
        content_x + content_w - 12.0 - button_w,
        bounds.y() + INSPECTOR_STATE_PANEL_Y + 8.0,
        button_w,
        INSPECTOR_DETAIL_BUTTON_H,
    )
}

fn inspector_detail_button_widget_id(parent: sui::WidgetId) -> sui::WidgetId {
    sui::WidgetId::new(parent.get().wrapping_mul(43).wrapping_add(7_401))
}

fn inspector_actions_origin_y(bounds: Rect) -> f32 {
    bounds.y() + INSPECTOR_STATE_PANEL_Y + 26.0 + 30.0
}

fn inspector_action_button_rect(bounds: Rect, index: usize) -> Rect {
    let content_x = bounds.x() + INSPECTOR_PAD;
    let content_w = (bounds.width() - INSPECTOR_PAD * 2.0).max(0.0);
    let col_w = ((content_w - INSPECTOR_ACTION_GAP) * 0.5).max(0.0);
    let col = index % 2;
    let row = index / 2;
    Rect::new(
        content_x + col as f32 * (col_w + INSPECTOR_ACTION_GAP),
        inspector_actions_origin_y(bounds)
            + row as f32 * (INSPECTOR_ACTION_BUTTON_H + INSPECTOR_ACTION_GAP),
        col_w,
        INSPECTOR_ACTION_BUTTON_H,
    )
}

fn inspector_actions_height(action_count: usize) -> f32 {
    if action_count == 0 {
        return 0.0;
    }
    let rows = action_count.div_ceil(2) as f32;
    rows * INSPECTOR_ACTION_BUTTON_H + (rows - 1.0) * INSPECTOR_ACTION_GAP
}

fn inspector_action_button_widget_id(
    parent: sui::WidgetId,
    action: InspectorAction,
) -> sui::WidgetId {
    let salt = match action {
        InspectorAction::OpenFolder => 7_497,
        InspectorAction::Copy => 7_511,
        InspectorAction::CopyToSifs => 7_515,
        InspectorAction::CopyToLocal => 7_519,
        InspectorAction::Move => 7_523,
        InspectorAction::Rename => 7_541,
        InspectorAction::Delete => 7_553,
        InspectorAction::RefreshPlacement => 7_571,
        InspectorAction::RepairReplicas => 7_579,
        InspectorAction::PinContent => 7_587,
        InspectorAction::UnpinContent => 7_599,
        InspectorAction::ConnectionSettings => 7_603,
    };
    sui::WidgetId::new(parent.get().wrapping_mul(43).wrapping_add(salt))
}

fn inspector_task_panel_rect(bounds: Rect) -> Rect {
    let content_x = bounds.x() + INSPECTOR_PAD;
    let content_w = (bounds.width() - INSPECTOR_PAD * 2.0).max(0.0);
    Rect::new(
        content_x,
        bounds.y() + INSPECTOR_TASK_PANEL_Y,
        content_w,
        INSPECTOR_TASK_PANEL_H,
    )
}

fn inspector_task_clear_rect(bounds: Rect) -> Rect {
    let panel = inspector_task_panel_rect(bounds);
    Rect::new(panel.max_x() - 102.0, panel.y() + 8.0, 90.0, 22.0)
}

fn inspector_task_cancel_rect(bounds: Rect, row_index: usize) -> Rect {
    let panel = inspector_task_panel_rect(bounds);
    Rect::new(
        panel.max_x() - INSPECTOR_TASK_CANCEL_W - 10.0,
        panel.y() + 46.0 + row_index as f32 * INSPECTOR_TASK_ROW_H + 2.0,
        INSPECTOR_TASK_CANCEL_W,
        22.0,
    )
}

fn inspector_task_clear_widget_id(parent: sui::WidgetId) -> sui::WidgetId {
    sui::WidgetId::new(parent.get().wrapping_mul(43).wrapping_add(9_099))
}

fn inspector_task_panel_widget_id(parent: sui::WidgetId) -> sui::WidgetId {
    sui::WidgetId::new(parent.get().wrapping_mul(43).wrapping_add(9_100))
}

fn inspector_task_cancel_widget_id(parent: sui::WidgetId, task_id: u64) -> sui::WidgetId {
    sui::WidgetId::new(
        parent
            .get()
            .wrapping_mul(43)
            .wrapping_add(9_101)
            .wrapping_add(task_id),
    )
}

impl Widget for InspectorView {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        if let Event::Pointer(pointer) = event
            && pointer.kind == PointerEventKind::Up
            && pointer.button == Some(PointerButton::Primary)
        {
            let clear_finished = {
                let files = self.shared.files();
                files_tasks_have_finished(&files.tasks)
                    && inspector_task_clear_rect(ctx.bounds()).contains(pointer.position)
            };
            if clear_finished {
                self.shared.files_mut().clear_finished_tasks();
                ctx.request_measure();
                ctx.request_paint();
                ctx.request_semantics();
                ctx.set_handled();
                return;
            }

            let task_to_cancel = {
                let files = self.shared.files();
                files
                    .tasks
                    .iter()
                    .take(3)
                    .enumerate()
                    .find(|(index, task)| {
                        task.state.can_cancel()
                            && inspector_task_cancel_rect(ctx.bounds(), *index)
                                .contains(pointer.position)
                    })
                    .map(|(_, task)| task.id)
            };
            if let Some(task_id) = task_to_cancel {
                self.shared.files_mut().cancel_task(task_id);
                ctx.request_paint();
                ctx.request_semantics();
                ctx.set_handled();
                return;
            }

            let selected_action = {
                let files = self.shared.files();
                files.selected_entry().cloned().and_then(|entry| {
                    inspector_actions(&files, &entry)
                        .into_iter()
                        .enumerate()
                        .find(|(index, _)| {
                            inspector_action_button_rect(ctx.bounds(), *index)
                                .contains(pointer.position)
                        })
                        .map(|(_, action)| (action, entry.path))
                })
            };
            if let Some((action, path)) = selected_action {
                match action {
                    InspectorAction::OpenFolder => {
                        self.shared.files_mut().navigate(path);
                        ctx.request_measure();
                    }
                    InspectorAction::RefreshPlacement => {
                        let waker = { self.shared.borrow().waker.clone() };
                        self.shared.files_mut().start_placement_refresh(waker);
                        request_files_overlay_rebuild(ctx);
                    }
                    InspectorAction::RepairReplicas => {
                        let waker = { self.shared.borrow().waker.clone() };
                        self.shared.files_mut().start_replica_repair(waker);
                        request_files_overlay_rebuild(ctx);
                    }
                    InspectorAction::PinContent => {
                        let waker = { self.shared.borrow().waker.clone() };
                        self.shared.files_mut().start_pin_content(waker);
                        request_files_overlay_rebuild(ctx);
                    }
                    InspectorAction::UnpinContent => {
                        let waker = { self.shared.borrow().waker.clone() };
                        self.shared.files_mut().start_unpin_content(waker);
                        request_files_overlay_rebuild(ctx);
                    }
                    InspectorAction::ConnectionSettings => {
                        open_settings_section(&self.shared, SettingsSection::Connection);
                        ctx.request_measure();
                    }
                    InspectorAction::Copy
                    | InspectorAction::CopyToSifs
                    | InspectorAction::CopyToLocal
                    | InspectorAction::Move
                    | InspectorAction::Rename
                    | InspectorAction::Delete => {
                        if let Some(kind) = action.prompt_kind() {
                            self.shared.files_mut().open_action_prompt(kind);
                            request_files_overlay_rebuild(ctx);
                        }
                    }
                }
                ctx.request_paint();
                ctx.request_semantics();
                ctx.set_handled();
                return;
            }

            let can_toggle_details = {
                let files = self.shared.files();
                files.loading || files.error.is_some() || files.selected_entry().is_some()
            };
            if can_toggle_details
                && inspector_detail_button_rect(ctx.bounds()).contains(pointer.position)
            {
                let details_open = self.shared.files().inspector_details_open;
                self.shared.files_mut().inspector_details_open = !details_open;
                ctx.request_measure();
                ctx.request_paint();
                ctx.request_semantics();
                ctx.set_handled();
                return;
            }
        }

        if matches!(event, Event::Window(WindowEvent::RedrawRequested)) {
            let summary = self.summary();
            let mut last = self.last_seen.borrow_mut();
            if last.as_deref() != Some(summary.as_str()) {
                *last = Some(summary);
                ctx.request_paint();
                ctx.request_semantics();
            }
        }
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let width = if constraints.max.width.is_finite() {
            constraints.max.width
        } else {
            INSPECTOR_W
        };
        let height = self.measured_content_height(ctx, width);
        constraints.clamp(Size::new(width, height))
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let theme = self.shared.theme();
        let pal = palette(&self.shared);
        let files = self.shared.files();
        let bounds = ctx.bounds();

        // Sidebar surface + left hairline (matches the bespoke chrome).
        ctx.fill_rect(bounds, pal.sidebar);
        widgets::hairline_left(ctx, bounds, pal.border);

        let x = bounds.x() + INSPECTOR_PAD;
        let w = (bounds.width() - INSPECTOR_PAD * 2.0).max(0.0);
        let mut y = bounds.y() + INSPECTOR_PAD + 4.0;

        let header = pal.text_style(&theme, 11.0, FontWeight::SEMIBOLD, pal.text_faint);
        widgets::draw_text_in(ctx, Rect::new(x, y, w, 18.0), "INSPECTOR", header);
        y += 30.0;
        self.task_panel(ctx, &theme, &pal, bounds, &files.tasks);
        y += INSPECTOR_TASK_PANEL_H + 12.0;

        if files.loading {
            let details_open = files.inspector_details_open;
            let rows = sifs_loading_rows(&files.path, details_open);
            self.state_panel(
                ctx,
                &theme,
                &pal,
                x,
                y,
                w,
                "Loading SIFS",
                &rows,
                Some(details_open),
            );
            return;
        }

        if let Some(error) = &files.error {
            let details_open = files.inspector_details_open;
            let rows = sifs_unavailable_rows(error, details_open);
            self.state_panel(
                ctx,
                &theme,
                &pal,
                x,
                y,
                w,
                "SIFS unavailable",
                &rows,
                Some(details_open),
            );
            return;
        }

        let Some(entry) = files.selected_entry() else {
            let rows = unselected_inspector_rows(&files);
            self.state_panel(
                ctx,
                &theme,
                &pal,
                x,
                y,
                w,
                unselected_inspector_title(files.source),
                &rows,
                None,
            );
            return;
        };

        let details = files.selected_details.as_ref();

        // Name + kind.
        let details_open = files.inspector_details_open;
        let detail_rect = inspector_detail_button_rect(bounds);
        self.detail_button(ctx, &theme, &pal, detail_rect, details_open);
        let title_right_gap = INSPECTOR_DETAIL_BUTTON_W + 10.0;
        let name_style = pal.text_style(&theme, 16.0, FontWeight::SEMIBOLD, pal.text);
        let title_rect = Rect::new(x, y, (w - title_right_gap).max(0.0), 24.0);
        let display_name = display_name_for_width(&entry.name, title_rect.width(), 8.8);
        widgets::draw_text_in(ctx, title_rect, &display_name, name_style);
        y += 26.0;
        let kind = details
            .map(|details| details.kind.as_str())
            .unwrap_or_else(|| {
                if entry.is_dir {
                    "directory"
                } else if entry.is_link {
                    "symlink"
                } else {
                    "file"
                }
            });
        let kind_style = pal.text_style(&theme, 12.5, FontWeight::NORMAL, pal.text_muted);
        let kind_line = if files.source == FilesSourceKind::Sifs {
            sifs_compact_header_line(
                entry,
                details,
                files.clustered,
                files.details_loading,
                files.details_error.as_deref(),
            )
        } else {
            format!("{kind} · {}", details_size(entry, details))
        };
        let kind_width = if files.source == FilesSourceKind::Sifs {
            title_rect.width()
        } else {
            w
        };
        widgets::draw_text_in(
            ctx,
            Rect::new(x, y, kind_width, 20.0),
            &display_name_for_width(&kind_line, kind_width, 6.8),
            kind_style,
        );
        y += 34.0;

        let actions = inspector_actions(&files, entry);
        if !actions.is_empty() {
            self.action_buttons(ctx, &theme, &pal, bounds, &actions);
            y = (inspector_actions_origin_y(bounds) + inspector_actions_height(actions.len()))
                .max(y)
                + 12.0;
        }

        y = self.info_row(ctx, &theme, &pal, x, y, w, "Path", &entry.path);

        if files.source == FilesSourceKind::Local {
            y += 10.0;
            let sect = pal.text_style(&theme, 11.0, FontWeight::SEMIBOLD, pal.text_faint);
            widgets::draw_text_in(ctx, Rect::new(x, y, w, 16.0), "LOCAL FILE", sect);
            y += 24.0;

            let local = files.selected_local_details.as_ref();
            let status_label = local_metadata_status_label(
                files.details_loading,
                files.details_error.as_deref(),
                local,
            );
            let status_color = local_metadata_status_color(
                &pal,
                files.details_loading,
                files.details_error.as_deref(),
                local,
            );
            y = self.status_badge(ctx, &theme, &pal, x, y, w, status_label, status_color);
            y = self.info_row(
                ctx,
                &theme,
                &pal,
                x,
                y + 10.0,
                w,
                "Source",
                "Host filesystem",
            );
            if let Some(local) = local {
                y = self.info_row(
                    ctx,
                    &theme,
                    &pal,
                    x,
                    y + 4.0,
                    w,
                    "Permissions",
                    if local.readonly {
                        "read-only"
                    } else {
                        "writable"
                    },
                );
                if let Some(canonical) = &local.canonical_path
                    && canonical != &local.path
                {
                    y = self.info_row(ctx, &theme, &pal, x, y + 4.0, w, "Canonical", canonical);
                }
            } else if let Some(error) = &files.details_error {
                y = self.info_row(ctx, &theme, &pal, x, y + 4.0, w, "Reason", error);
            } else if files.details_loading {
                y = self.info_row(
                    ctx,
                    &theme,
                    &pal,
                    x,
                    y + 4.0,
                    w,
                    "Status",
                    "Reading metadata",
                );
            }

            if details_open {
                y += 14.0;
                let detail_heading =
                    pal.text_style(&theme, 11.0, FontWeight::SEMIBOLD, pal.text_faint);
                widgets::draw_text_in(
                    ctx,
                    Rect::new(x, y, w, 16.0),
                    "TECHNICAL DETAILS",
                    detail_heading,
                );
                y += 22.0;
                if let Some(local) = local {
                    y = self.info_row(ctx, &theme, &pal, x, y, w, "Kind", &local.kind);
                    y = self.info_row(
                        ctx,
                        &theme,
                        &pal,
                        x,
                        y + 4.0,
                        w,
                        "Accessed",
                        &local_time_label(local.accessed_at_unix_nanos),
                    );
                    y = self.info_row(
                        ctx,
                        &theme,
                        &pal,
                        x,
                        y + 4.0,
                        w,
                        "Modified",
                        &local_time_label(local.modified_at_unix_nanos),
                    );
                    let _ = self.info_row(
                        ctx,
                        &theme,
                        &pal,
                        x,
                        y + 4.0,
                        w,
                        "Created",
                        &local_time_label(local.created_at_unix_nanos),
                    );
                }
            }

            if !entry.is_dir {
                y += 10.0;
                let preview_error = files
                    .preview_error
                    .as_deref()
                    .map(|error| file_preview_error_display(files.source, error));
                let _ = self.preview_section(
                    ctx,
                    &theme,
                    &pal,
                    x,
                    y,
                    w,
                    files.preview_loading,
                    preview_error.as_deref(),
                    files.selected_preview.as_ref(),
                );
            }
            return;
        }

        // Placement section.
        y += 10.0;
        let sect = pal.text_style(&theme, 11.0, FontWeight::SEMIBOLD, pal.text_faint);
        widgets::draw_text_in(ctx, Rect::new(x, y, w, 16.0), "PLACEMENT", sect);
        y += 24.0;

        // node badge: a real Lucide HardDrive (Storage) icon + the node label, in a rounded pill.
        let node_label = short_node_label(&files.node_id);
        let badge_style = pal.text_style(&theme, 12.0, FontWeight::MEDIUM, pal.accent);
        const BADGE_ICON: f32 = 13.0;
        const BADGE_ICON_GAP: f32 = 7.0;
        let label_w = ctx
            .measure_text(node_label.clone(), badge_style.clone())
            .map(|m| m.width)
            .unwrap_or(80.0);
        // pill = left pad(10) + icon + gap + label + right pad(10)
        let badge_w = 10.0 + BADGE_ICON + BADGE_ICON_GAP + label_w + 10.0;
        let badge = Rect::new(x, y, badge_w.min(w), 26.0);
        widgets::round_panel(ctx, badge, pal.field, pal.border, 13.0);
        draw_glyph(
            ctx,
            IconGlyph::Storage,
            Rect::new(
                badge.x() + 10.0,
                badge.y() + (badge.height() - BADGE_ICON) / 2.0,
                BADGE_ICON,
                BADGE_ICON,
            ),
            pal.accent,
        );
        widgets::draw_text_in(
            ctx,
            Rect::new(
                badge.x() + 10.0 + BADGE_ICON + BADGE_ICON_GAP,
                badge.y(),
                (badge.width() - 10.0 - BADGE_ICON - BADGE_ICON_GAP - 6.0).max(0.0),
                badge.height(),
            ),
            &node_label,
            badge_style,
        );
        y += 36.0;

        let status_label = sifs_metadata_status_label(
            files.details_loading,
            files.details_error.as_deref(),
            details,
        );
        let status_color = sifs_metadata_status_color(
            &pal,
            files.details_loading,
            files.details_error.as_deref(),
            details,
        );
        y = self.status_badge(ctx, &theme, &pal, x, y, w, status_label, status_color);

        let policy = details
            .map(|details| details.policy.as_str())
            .unwrap_or("not loaded");
        let placement = sifs_replication_summary(details, files.clustered);
        y = self.info_row(ctx, &theme, &pal, x, y + 10.0, w, "Placement", &placement);
        let health = sifs_health_summary(details);
        y = self.info_row(ctx, &theme, &pal, x, y + 4.0, w, "Health", &health);
        let pins = sifs_pin_summary(details);
        y = self.info_row(ctx, &theme, &pal, x, y + 4.0, w, "Pin", &pins);
        let next_step = sifs_inspector_next_step(
            details,
            files.clustered,
            files.details_loading,
            files.details_error.as_deref(),
        );
        y = self.info_row(ctx, &theme, &pal, x, y + 4.0, w, "Next", &next_step);

        if details_open {
            y += 14.0;
            let detail_heading = pal.text_style(&theme, 11.0, FontWeight::SEMIBOLD, pal.text_faint);
            widgets::draw_text_in(
                ctx,
                Rect::new(x, y, w, 16.0),
                "TECHNICAL DETAILS",
                detail_heading,
            );
            y += 22.0;

            if let Some(error) = &files.details_error {
                y = self.info_row(ctx, &theme, &pal, x, y, w, "Reason", error);
                y = self.info_row(
                    ctx,
                    &theme,
                    &pal,
                    x,
                    y + 4.0,
                    w,
                    "Authority",
                    "SIFS through daemon",
                );
            } else if files.details_loading {
                y = self.info_row(
                    ctx,
                    &theme,
                    &pal,
                    x,
                    y,
                    w,
                    "Status",
                    "Waiting for daemon metadata",
                );
            } else if details.is_none() {
                y = self.info_row(
                    ctx,
                    &theme,
                    &pal,
                    x,
                    y,
                    w,
                    "Status",
                    "Metadata has not returned for this object yet",
                );
                y = self.info_row(
                    ctx,
                    &theme,
                    &pal,
                    x,
                    y + 4.0,
                    w,
                    "Authority",
                    "SIFS through daemon",
                );
            }

            y = self.info_row(ctx, &theme, &pal, x, y + 4.0, w, "Policy", policy);
            if let Some(health) = details.and_then(|details| details.health.as_ref()) {
                y = self.info_row(
                    ctx,
                    &theme,
                    &pal,
                    x,
                    y + 4.0,
                    w,
                    "Agent state",
                    &health.agent_state,
                );
                y = self.info_row(
                    ctx,
                    &theme,
                    &pal,
                    x,
                    y + 4.0,
                    w,
                    "Operator counts",
                    &format!(
                        "{} · {} · {}",
                        plural(health.version_count, "version"),
                        plural(health.receipt_count, "receipt"),
                        plural(health.intent_count, "intent")
                    ),
                );
                y = self.info_row(
                    ctx,
                    &theme,
                    &pal,
                    x,
                    y + 4.0,
                    w,
                    "Repair backlog",
                    &format!(
                        "{} · {}",
                        plural(health.pending_repair_count, "missing replica"),
                        plural(health.open_conflict_count, "conflict")
                    ),
                );
                y = self.info_row(
                    ctx,
                    &theme,
                    &pal,
                    x,
                    y + 4.0,
                    w,
                    "Missing nodes",
                    &sifs_missing_replica_summary(health),
                );
            }
            let locations = details
                .map(details_locations)
                .unwrap_or_else(|| "not reported".to_string());
            y = self.info_row(
                ctx,
                &theme,
                &pal,
                x,
                y + 4.0,
                w,
                "Durable nodes",
                &locations,
            );

            if let Some(details) = details {
                y = self.info_row(
                    ctx,
                    &theme,
                    &pal,
                    x,
                    y + 4.0,
                    w,
                    "Local pins",
                    &sifs_pin_detail_summary(details),
                );
                y = self.info_row(
                    ctx,
                    &theme,
                    &pal,
                    x,
                    y + 4.0,
                    w,
                    "File system",
                    &details.file_system_id,
                );
                y = self.info_row(
                    ctx,
                    &theme,
                    &pal,
                    x,
                    y + 4.0,
                    w,
                    "Object",
                    &short(&details.object_id, 28),
                );
                y = self.info_row(
                    ctx,
                    &theme,
                    &pal,
                    x,
                    y + 4.0,
                    w,
                    "Version",
                    &short(&details.version, 28),
                );
                y = self.info_row(
                    ctx,
                    &theme,
                    &pal,
                    x,
                    y + 4.0,
                    w,
                    "Mode",
                    &format!("{:o} · {} links", details.mode, details.link_count),
                );
                y = self.info_row(
                    ctx,
                    &theme,
                    &pal,
                    x,
                    y + 4.0,
                    w,
                    "Owner",
                    &format!("uid {} · gid {}", details.uid, details.gid),
                );
                y = self.info_row(
                    ctx,
                    &theme,
                    &pal,
                    x,
                    y + 4.0,
                    w,
                    "Modified",
                    &unix_nanos_label(details.modified_at_unix_nanos),
                );
                if let Some(hash) = &details.content_hash {
                    y = self.info_row(
                        ctx,
                        &theme,
                        &pal,
                        x,
                        y + 4.0,
                        w,
                        "Content",
                        &short(hash, 28),
                    );
                }
            }
        }

        if !files.clustered {
            let note = pal.text_style(&theme, 11.5, FontWeight::NORMAL, pal.text_faint);
            for line in widgets::wrap_lines(
                "Join a cluster to spread this object across nodes and tune replication.",
                w,
                |s| {
                    ctx.measure_text(s, note.clone())
                        .map(|m| m.width)
                        .unwrap_or(0.0)
                },
            ) {
                widgets::draw_text_in(ctx, Rect::new(x, y, w, 18.0), &line, note.clone());
                y += 17.0;
            }
        }

        if !entry.is_dir {
            y += 10.0;
            let preview_error = files
                .preview_error
                .as_deref()
                .map(|error| file_preview_error_display(files.source, error));
            let _ = self.preview_section(
                ctx,
                &theme,
                &pal,
                x,
                y,
                w,
                files.preview_loading,
                preview_error.as_deref(),
                files.selected_preview.as_ref(),
            );
        }
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let (selected_entry, selected_actions, details_open, tasks) = {
            let files = self.shared.files();
            let selected_entry = files.selected_entry().cloned();
            let selected_actions = selected_entry
                .as_ref()
                .map(|entry| inspector_actions(&files, entry))
                .unwrap_or_default();
            (
                selected_entry,
                selected_actions,
                (files.loading || files.error.is_some() || files.selected_entry().is_some())
                    .then_some(files.inspector_details_open),
                files.tasks.clone(),
            )
        };

        let mut task_panel = SemanticsNode::new(
            inspector_task_panel_widget_id(ctx.widget_id()),
            SemanticsRole::Text,
            inspector_task_panel_rect(ctx.bounds()),
        );
        task_panel.parent = Some(ctx.widget_id());
        task_panel.name = Some("File task queue".to_string());
        task_panel.description = Some(files_task_queue_summary(&tasks));
        ctx.push(task_panel);

        if files_tasks_have_finished(&tasks) {
            let mut node = SemanticsNode::new(
                inspector_task_clear_widget_id(ctx.widget_id()),
                SemanticsRole::Button,
                inspector_task_clear_rect(ctx.bounds()),
            );
            node.parent = Some(ctx.widget_id());
            node.name = Some("Clear finished file tasks".to_string());
            node.description =
                Some("Dismiss completed, failed, and cancelled file operations".to_string());
            node.actions = vec![SemanticsAction::Activate];
            ctx.push(node);
        }

        for (index, task) in tasks
            .iter()
            .take(3)
            .enumerate()
            .filter(|(_, task)| task.state.can_cancel())
        {
            let mut node = SemanticsNode::new(
                inspector_task_cancel_widget_id(ctx.widget_id(), task.id),
                SemanticsRole::Button,
                inspector_task_cancel_rect(ctx.bounds(), index),
            );
            node.parent = Some(ctx.widget_id());
            node.name = Some(format!("Cancel {}", task.title));
            node.description = Some(task.detail.clone());
            node.actions = vec![SemanticsAction::Activate];
            ctx.push(node);
        }

        if let Some(entry) = &selected_entry {
            for (index, action) in selected_actions.into_iter().enumerate() {
                let mut node = SemanticsNode::new(
                    inspector_action_button_widget_id(ctx.widget_id(), action),
                    SemanticsRole::Button,
                    inspector_action_button_rect(ctx.bounds(), index),
                );
                node.parent = Some(ctx.widget_id());
                node.name = Some(action.semantic_name(entry));
                node.description = Some(match action {
                    InspectorAction::OpenFolder => format!("Open {}", entry.path),
                    InspectorAction::RefreshPlacement => {
                        format!("Refresh SIFS placement metadata for {}", entry.path)
                    }
                    InspectorAction::RepairReplicas => {
                        format!("Request SIFS replica repair for {}", entry.path)
                    }
                    InspectorAction::PinContent => {
                        format!("Pin SIFS content on this node for {}", entry.path)
                    }
                    InspectorAction::UnpinContent => {
                        format!("Remove this node's SIFS content pin for {}", entry.path)
                    }
                    InspectorAction::ConnectionSettings => {
                        "Open Connection settings for SIFS authorization and daemon recovery"
                            .to_string()
                    }
                    InspectorAction::CopyToSifs => {
                        format!("Copy local {} into SIFS", entry.path)
                    }
                    InspectorAction::CopyToLocal => {
                        format!("Download SIFS {} to a local path", entry.path)
                    }
                    InspectorAction::Copy => format!("Copy {}", entry.path),
                    InspectorAction::Move => format!("Move {}", entry.path),
                    InspectorAction::Rename => format!("Rename {}", entry.path),
                    InspectorAction::Delete => format!("Delete {}", entry.path),
                });
                node.actions = vec![SemanticsAction::Activate];
                ctx.push(node);
            }
        }

        let Some(details_open) = details_open else {
            return;
        };

        let mut node = SemanticsNode::new(
            inspector_detail_button_widget_id(ctx.widget_id()),
            SemanticsRole::Button,
            inspector_detail_button_rect(ctx.bounds()),
        );
        node.parent = Some(ctx.widget_id());
        node.name = Some(inspector_detail_button_label(details_open).to_string());
        node.description = Some("File storage metadata and diagnostics".to_string());
        node.actions = vec![SemanticsAction::Activate];
        node.state.selected = details_open;
        ctx.push(node);
    }
}

impl InspectorView {
    #[allow(clippy::too_many_arguments)]
    fn state_panel(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        x: f32,
        y: f32,
        w: f32,
        title: &str,
        rows: &[(&str, String)],
        details_open: Option<bool>,
    ) {
        let label_style = pal.text_style(theme, 10.5, FontWeight::MEDIUM, pal.text_faint);
        let value_style = pal.text_style(theme, 11.5, FontWeight::NORMAL, pal.text);
        let value_w = (w - 24.0).max(0.0);
        let row_layout: Vec<(&str, Vec<String>)> = rows
            .iter()
            .map(|(label, value)| {
                let lines = widgets::wrap_lines(value, value_w, |s| {
                    ctx.measure_text(s, value_style.clone())
                        .map(|m| m.width)
                        .unwrap_or(0.0)
                });
                (*label, lines)
            })
            .collect();
        let panel_h = 44.0
            + row_layout
                .iter()
                .map(|(_, lines)| 15.0 + (lines.len().max(1) as f32 * 16.0) + 8.0)
                .sum::<f32>();
        let panel = Rect::new(x, y, w, panel_h);
        widgets::round_panel(ctx, panel, pal.panel, pal.border, 8.0);
        let detail_button =
            details_open.map(|open| (inspector_detail_button_rect(ctx.bounds()), open));
        let title_right_gap = if detail_button.is_some() {
            INSPECTOR_DETAIL_BUTTON_W + 10.0
        } else {
            0.0
        };
        widgets::draw_text_in(
            ctx,
            Rect::new(
                panel.x() + 12.0,
                panel.y() + 10.0,
                (panel.width() - 24.0 - title_right_gap).max(0.0),
                20.0,
            ),
            title,
            pal.text_style(theme, 13.5, FontWeight::SEMIBOLD, pal.text),
        );
        if let Some((rect, open)) = detail_button {
            self.detail_button(ctx, theme, pal, rect, open);
        }

        let mut row_y = panel.y() + 38.0;
        for (label, lines) in row_layout {
            widgets::draw_text_in(
                ctx,
                Rect::new(panel.x() + 12.0, row_y, value_w, 14.0),
                label,
                label_style.clone(),
            );
            row_y += 15.0;
            for line in lines.iter().take(3) {
                widgets::draw_text_in(
                    ctx,
                    Rect::new(panel.x() + 12.0, row_y, value_w, 16.0),
                    line,
                    value_style.clone(),
                );
                row_y += 16.0;
            }
            row_y += 8.0;
        }
    }

    fn detail_button(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        rect: Rect,
        details_open: bool,
    ) {
        widgets::round_panel(ctx, rect, pal.field, pal.border, 7.0);
        widgets::draw_text_in(
            ctx,
            Rect::new(rect.x() + 10.0, rect.y() + 4.0, rect.width() - 20.0, 18.0),
            inspector_detail_button_label(details_open),
            pal.text_style(theme, 11.0, FontWeight::MEDIUM, pal.text),
        );
    }

    fn status_badge(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        x: f32,
        y: f32,
        w: f32,
        label: &str,
        tone: Color,
    ) -> f32 {
        let text_style = pal.text_style(theme, 12.0, FontWeight::SEMIBOLD, tone);
        let text_w = ctx
            .measure_text(label.to_string(), text_style.clone())
            .map(|m| m.width)
            .unwrap_or(120.0);
        let badge_w = (text_w + 22.0).min(w).max(80.0);
        let badge = Rect::new(x, y, badge_w, 26.0);
        widgets::round_panel(ctx, badge, pal.field, tone, 13.0);
        widgets::draw_text_in(
            ctx,
            Rect::new(
                badge.x() + 11.0,
                badge.y() + 4.0,
                badge.width() - 22.0,
                18.0,
            ),
            label,
            text_style,
        );
        badge.max_y()
    }

    fn action_buttons(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        bounds: Rect,
        actions: &[InspectorAction],
    ) {
        for (index, action) in actions.iter().copied().enumerate() {
            self.action_button(
                ctx,
                theme,
                pal,
                inspector_action_button_rect(bounds, index),
                action,
            );
        }
    }

    fn action_button(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        rect: Rect,
        action: InspectorAction,
    ) {
        let (fill, border, text, icon) = match action {
            InspectorAction::OpenFolder => (pal.accent, pal.accent, pal.on_accent, pal.on_accent),
            InspectorAction::Delete => (pal.field, pal.border, pal.warn, pal.warn),
            InspectorAction::RefreshPlacement
            | InspectorAction::RepairReplicas
            | InspectorAction::PinContent
            | InspectorAction::UnpinContent
            | InspectorAction::ConnectionSettings => {
                (pal.field, pal.border, pal.accent, pal.accent)
            }
            InspectorAction::CopyToSifs | InspectorAction::CopyToLocal => {
                (pal.field, pal.border, pal.accent, pal.accent)
            }
            InspectorAction::Copy | InspectorAction::Move | InspectorAction::Rename => {
                (pal.field, pal.border, pal.text, pal.text_muted)
            }
        };
        widgets::round_panel(ctx, rect, fill, border, 7.0);
        let icon_rect = Rect::new(rect.x() + 9.0, rect.y() + 7.0, 14.0, 14.0);
        draw_glyph(ctx, action.glyph(), icon_rect, icon);
        widgets::draw_text_in(
            ctx,
            Rect::new(rect.x() + 30.0, rect.y() + 5.0, rect.width() - 38.0, 18.0),
            action.label(),
            pal.text_style(theme, 11.5, FontWeight::MEDIUM, text),
        );
    }

    fn task_panel(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        bounds: Rect,
        tasks: &[FilesTask],
    ) {
        let panel = inspector_task_panel_rect(bounds);
        widgets::round_panel(ctx, panel, pal.panel, pal.border, 8.0);
        let has_finished = files_tasks_have_finished(tasks);
        let title_right = if has_finished {
            inspector_task_clear_rect(bounds).x() - 8.0
        } else {
            panel.max_x() - 12.0
        };
        widgets::draw_text_in(
            ctx,
            Rect::new(
                panel.x() + 12.0,
                panel.y() + 10.0,
                (title_right - panel.x() - 12.0).max(0.0),
                18.0,
            ),
            "FILE TASKS",
            pal.text_style(theme, 11.0, FontWeight::SEMIBOLD, pal.text_faint),
        );
        if has_finished {
            let clear = inspector_task_clear_rect(bounds);
            widgets::round_panel(ctx, clear, pal.field, pal.border, 7.0);
            widgets::draw_text_in(
                ctx,
                Rect::new(clear.x() + 8.0, clear.y() + 3.0, clear.width() - 16.0, 16.0),
                "Clear",
                pal.text_style(theme, 10.5, FontWeight::MEDIUM, pal.text),
            );
        }
        let summary = files_task_queue_summary(tasks);
        let summary_style = pal.text_style(theme, 11.0, FontWeight::MEDIUM, pal.text_muted);
        widgets::draw_text_in(
            ctx,
            Rect::new(
                panel.x() + 12.0,
                panel.y() + 27.0,
                panel.width() - 24.0,
                16.0,
            ),
            &summary,
            summary_style,
        );

        if tasks.is_empty() {
            let badge = Rect::new(panel.x() + 12.0, panel.y() + 52.0, 68.0, 24.0);
            widgets::round_panel(ctx, badge, pal.field, pal.good, 12.0);
            widgets::draw_text_in(
                ctx,
                Rect::new(
                    badge.x() + 10.0,
                    badge.y() + 4.0,
                    badge.width() - 20.0,
                    16.0,
                ),
                "Idle",
                pal.text_style(theme, 11.0, FontWeight::SEMIBOLD, pal.good),
            );
            widgets::draw_text_in(
                ctx,
                Rect::new(
                    panel.x() + 12.0,
                    panel.y() + 84.0,
                    panel.width() - 24.0,
                    18.0,
                ),
                "No active file operations",
                pal.text_style(theme, 12.0, FontWeight::MEDIUM, pal.text),
            );
            widgets::draw_text_in(
                ctx,
                Rect::new(
                    panel.x() + 12.0,
                    panel.y() + 104.0,
                    panel.width() - 24.0,
                    32.0,
                ),
                "Worker queue empty",
                pal.text_style(theme, 11.0, FontWeight::NORMAL, pal.text_faint),
            );
            return;
        }

        let title_style = pal.text_style(theme, 11.5, FontWeight::MEDIUM, pal.text);
        let detail_style = pal.text_style(theme, 10.5, FontWeight::NORMAL, pal.text_faint);
        for (index, task) in tasks.iter().take(3).enumerate() {
            let y = panel.y() + 46.0 + index as f32 * INSPECTOR_TASK_ROW_H;
            let cancel = inspector_task_cancel_rect(bounds, index);
            let text_w = if task.state.can_cancel() {
                cancel.x() - panel.x() - 20.0
            } else {
                panel.width() - 24.0
            }
            .max(0.0);
            widgets::draw_text_in(
                ctx,
                Rect::new(panel.x() + 12.0, y, text_w, 14.0),
                &format!("{} · {}", task.title, task.state.label()),
                title_style.clone(),
            );
            widgets::draw_text_in(
                ctx,
                Rect::new(panel.x() + 12.0, y + 13.0, text_w, 13.0),
                &files_task_detail_display_for_width(task, text_w),
                detail_style.clone(),
            );
            let tone = match task.state {
                FilesTaskState::Running | FilesTaskState::CancelRequested => pal.accent,
                FilesTaskState::Completed | FilesTaskState::Cancelled => pal.good,
                FilesTaskState::Failed => pal.warn,
            };
            let bar = Rect::new(panel.x() + 12.0, y + 26.0, text_w.min(180.0), 2.0);
            if bar.width() > 0.0 {
                widgets::fill_round_rect(ctx, bar, pal.field, 1.0);
                let fraction = task_progress_fraction(task);
                widgets::fill_round_rect(
                    ctx,
                    Rect::new(bar.x(), bar.y(), bar.width() * fraction, bar.height()),
                    tone,
                    1.0,
                );
            }
            if task.state.can_cancel() {
                widgets::round_panel(ctx, cancel, pal.field, pal.border, 7.0);
                widgets::draw_text_in(
                    ctx,
                    Rect::new(
                        cancel.x() + 8.0,
                        cancel.y() + 3.0,
                        cancel.width() - 16.0,
                        16.0,
                    ),
                    "Cancel",
                    pal.text_style(theme, 10.5, FontWeight::MEDIUM, pal.text),
                );
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn info_row(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        x: f32,
        mut y: f32,
        w: f32,
        label: &str,
        value: &str,
    ) -> f32 {
        let label_style = pal.text_style(theme, 11.0, FontWeight::SEMIBOLD, pal.text_faint);
        widgets::draw_text_in(
            ctx,
            Rect::new(x, y, w, 16.0),
            &label.to_uppercase(),
            label_style,
        );
        y += 18.0;
        let value_style = pal.text_style(theme, 12.5, FontWeight::NORMAL, pal.text);
        for line in widgets::wrap_lines(value, w, |s| {
            ctx.measure_text(s, value_style.clone())
                .map(|m| m.width)
                .unwrap_or(0.0)
        }) {
            widgets::draw_text_in(ctx, Rect::new(x, y, w, 18.0), &line, value_style.clone());
            y += 17.0;
        }
        y + 6.0
    }

    #[allow(clippy::too_many_arguments)]
    fn preview_section(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        x: f32,
        y: f32,
        w: f32,
        loading: bool,
        error: Option<&str>,
        preview: Option<&FilesPreview>,
    ) -> f32 {
        let height = Self::preview_section_height(w, loading, error, preview);
        let panel = Rect::new(x, y, w, height);
        widgets::round_panel(ctx, panel, pal.panel, pal.border, 8.0);

        widgets::draw_text_in(
            ctx,
            Rect::new(
                panel.x() + 12.0,
                panel.y() + 10.0,
                panel.width() - 24.0,
                16.0,
            ),
            "PREVIEW",
            pal.text_style(theme, 11.0, FontWeight::SEMIBOLD, pal.text_faint),
        );

        let mut row_y = panel.y() + 34.0;
        let inner_w = (panel.width() - 24.0).max(0.0);
        let value_style = pal.text_style(theme, 11.5, FontWeight::NORMAL, pal.text);
        let muted_style = pal.text_style(theme, 10.8, FontWeight::NORMAL, pal.text_faint);

        if loading {
            widgets::draw_text_in(
                ctx,
                Rect::new(panel.x() + 12.0, row_y, inner_w, 18.0),
                "Reading bounded preview",
                value_style,
            );
            return panel.max_y();
        }

        if let Some(error) = error {
            for line in widgets::wrap_lines(error, inner_w, |s| {
                ctx.measure_text(s, value_style.clone())
                    .map(|m| m.width)
                    .unwrap_or(0.0)
            })
            .into_iter()
            .take(3)
            {
                widgets::draw_text_in(
                    ctx,
                    Rect::new(panel.x() + 12.0, row_y, inner_w, 18.0),
                    &line,
                    value_style.clone(),
                );
                row_y += 17.0;
            }
            return panel.max_y();
        }

        let Some(preview) = preview else {
            widgets::draw_text_in(
                ctx,
                Rect::new(panel.x() + 12.0, row_y, inner_w, 18.0),
                "Preview not loaded yet",
                value_style,
            );
            return panel.max_y();
        };

        if preview.binary {
            let value = preview.lines.join(" ");
            for line in widgets::wrap_lines(&value, inner_w, |s| {
                ctx.measure_text(s, value_style.clone())
                    .map(|m| m.width)
                    .unwrap_or(0.0)
            })
            .into_iter()
            .take(3)
            {
                widgets::draw_text_in(
                    ctx,
                    Rect::new(panel.x() + 12.0, row_y, inner_w, 18.0),
                    &line,
                    value_style.clone(),
                );
                row_y += 17.0;
            }
        } else {
            for line in preview.lines.iter().take(FILE_PREVIEW_MAX_LINES) {
                for wrapped in widgets::wrap_lines(line, inner_w, |s| {
                    ctx.measure_text(s, value_style.clone())
                        .map(|m| m.width)
                        .unwrap_or(0.0)
                })
                .into_iter()
                .take(2)
                {
                    widgets::draw_text_in(
                        ctx,
                        Rect::new(panel.x() + 12.0, row_y, inner_w, 18.0),
                        &wrapped,
                        value_style.clone(),
                    );
                    row_y += 17.0;
                }
            }
        }

        if preview.truncated {
            widgets::draw_text_in(
                ctx,
                Rect::new(panel.x() + 12.0, row_y, inner_w, 18.0),
                &format!("Preview truncated to {} KB.", FILE_PREVIEW_MAX_BYTES / 1024),
                muted_style,
            );
        }

        panel.max_y()
    }
}

/// The inspector wrapped in a `PanelSection`-less host so it can be a `SizedBox` child. We keep the
/// inspector as a single painter (above), so this is just the bare widget. A thin wrapper exists so
/// the composed pod reads cleanly.
fn inspector(shared: &Shared, scroll_state: &ScrollState) -> impl Widget + use<> {
    let s = shared.clone();
    Background::new(
        Color::TRANSPARENT,
        ScrollView::vertical(InspectorView::new(shared.clone()))
            .state(scroll_state.clone())
            .name("File inspector")
            .viewport_width_hint(true),
    )
    .brush_when(move || palette(&s).sidebar)
}

// ---------------------------------------------------------------------------
// RebuildOnChange<K> — a reusable "rebuild my child when a key changes" wrapper.
// ---------------------------------------------------------------------------

/// Hosts a child [`WidgetPod`] that is *rebuilt from scratch* whenever a watched key changes.
///
/// `sui-widgets` collection widgets (`ListView`, `Breadcrumb`, …) take a fixed item vector at build
/// time — there is no reactive `items_when` builder that re-reads the items each frame. So when the
/// underlying data set changes (here: the SIFS listing + breadcrumb segments change as the user
/// navigates), we can't just bind a reader; we have to *rebuild* the subtree. This widget does that:
///
/// * It owns a `build: Fn(&K) -> WidgetPod` closure and a `key: Fn() -> K` reader.
/// * On each `RedrawRequested` it reads the current key; if it differs from the last-built key it
///   rebuilds the child pod and requests measure + arrange + paint (the diff-on-redraw technique,
///   like the Settings `StatusReflector`).
/// * The headless test harness does **not** dispatch `RedrawRequested`, so — exactly like the
///   Settings host — it ALSO rebuilds on the pointer-up that triggered the navigation. A pointer
///   release is the only moment a navigation can happen, so this is both sufficient and cheap.
///
/// (Chat will reuse this pattern for its streaming transcript: same shape — a key that changes as
/// messages stream in, a child rebuilt from the current message list.)
struct RebuildOnChange<K: PartialEq + Clone> {
    key_fn: Box<dyn Fn() -> K>,
    build: Box<dyn Fn(&K) -> WidgetPod>,
    last_key: K,
    child: WidgetPod,
}

impl<K: PartialEq + Clone> RebuildOnChange<K> {
    fn new<KF, BF>(key_fn: KF, build: BF) -> Self
    where
        KF: Fn() -> K + 'static,
        BF: Fn(&K) -> WidgetPod + 'static,
    {
        let last_key = key_fn();
        let child = build(&last_key);
        Self {
            key_fn: Box::new(key_fn),
            build: Box::new(build),
            last_key,
            child,
        }
    }

    /// Rebuild the child pod if the watched key changed. Returns whether a rebuild happened.
    fn refresh(&mut self) -> bool {
        let key = (self.key_fn)();
        if key != self.last_key {
            self.child = (self.build)(&key);
            self.last_key = key;
            true
        } else {
            false
        }
    }
}

impl<K: PartialEq + Clone + 'static> Widget for RebuildOnChange<K> {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        // Rebuild on the pointer-up that triggers a navigation (the headless harness never sends
        // RedrawRequested, so this is the path the tests exercise) …
        let pointer_up = matches!(
            event,
            Event::Pointer(p) if matches!(p.kind, sui::PointerEventKind::Up)
        );
        // … and on each redraw, to catch a navigation driven from outside a pointer gesture.
        let redraw = matches!(event, Event::Window(WindowEvent::RedrawRequested));
        if (pointer_up || redraw) && self.refresh() {
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_paint();
        }
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        // A measure pass is the last chance to rebuild before layout, so refresh here too (keeps the
        // built child in sync when something else forces a measure, e.g. a resize or tab switch).
        self.refresh();
        self.child.measure(ctx, constraints)
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        self.child.arrange(ctx, bounds);
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.child.paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.child.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.child);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.child);
    }
}

// ---------------------------------------------------------------------------
// Split — left pane fills, right pane fixed width, a vertical separator between.
// ---------------------------------------------------------------------------

/// A horizontal split: the `left` pane fills the remaining width while the `right` pane keeps a
/// fixed `right_width`, with a 1px vertical [`Separator`] between them. (A plain horizontal `Stack`
/// measures children left-to-right and lets the first child consume the full width before the fixed
/// pane is measured, collapsing the right pane — this reserves the fixed width up front.)
struct Split {
    left: WidgetPod,
    separator: WidgetPod,
    right: WidgetPod,
    right_width: f32,
}

const SPLIT_SEPARATOR_W: f32 = 1.0;

impl Split {
    fn new<L, R>(left: L, right: R, right_width: f32, reader: &ThemeReader) -> Self
    where
        L: Widget + 'static,
        R: Widget + 'static,
    {
        Self {
            left: WidgetPod::new(left),
            separator: WidgetPod::new(Separator::vertical().theme_when(clone_reader(reader))),
            right: WidgetPod::new(right),
            right_width: right_width.max(0.0),
        }
    }

    /// (left_width, right_width) for a given total width.
    fn split_widths(&self, total: f32) -> (f32, f32) {
        let right = self.right_width.min((total - SPLIT_SEPARATOR_W).max(0.0));
        let left = (total - right - SPLIT_SEPARATOR_W).max(0.0);
        (left, right)
    }
}

impl Widget for Split {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let total = if constraints.max.width.is_finite() {
            constraints.max.width
        } else {
            self.right_width + 320.0 + SPLIT_SEPARATOR_W
        };
        let height = constraints.max.height;
        let (left_w, right_w) = self.split_widths(total);
        self.left
            .measure(ctx, Constraints::tight(Size::new(left_w, height)));
        self.separator.measure(
            ctx,
            Constraints::tight(Size::new(SPLIT_SEPARATOR_W, height)),
        );
        self.right
            .measure(ctx, Constraints::tight(Size::new(right_w, height)));
        Size::new(total, height)
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        let (left_w, right_w) = self.split_widths(bounds.width());
        self.left.arrange(
            ctx,
            Rect::new(bounds.x(), bounds.y(), left_w, bounds.height()),
        );
        self.separator.arrange(
            ctx,
            Rect::new(
                bounds.x() + left_w,
                bounds.y(),
                SPLIT_SEPARATOR_W,
                bounds.height(),
            ),
        );
        self.right.arrange(
            ctx,
            Rect::new(
                bounds.x() + left_w + SPLIT_SEPARATOR_W,
                bounds.y(),
                right_w,
                bounds.height(),
            ),
        );
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.left.paint(ctx);
        self.separator.paint(ctx);
        self.right.paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.left.semantics(ctx);
        self.separator.semantics(ctx);
        self.right.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.left);
        visitor.visit(&self.separator);
        visitor.visit(&self.right);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.left);
        visitor.visit(&mut self.separator);
        visitor.visit(&mut self.right);
    }
}

// ---------------------------------------------------------------------------
// FixedLeftSplit — left pane fixed width, right pane fills.
// ---------------------------------------------------------------------------

struct FixedLeftSplit {
    left: WidgetPod,
    separator: WidgetPod,
    right: WidgetPod,
    left_width: f32,
}

impl FixedLeftSplit {
    fn new<L, R>(left: L, right: R, left_width: f32, reader: &ThemeReader) -> Self
    where
        L: Widget + 'static,
        R: Widget + 'static,
    {
        Self {
            left: WidgetPod::new(left),
            separator: WidgetPod::new(Separator::vertical().theme_when(clone_reader(reader))),
            right: WidgetPod::new(right),
            left_width: left_width.max(0.0),
        }
    }

    fn split_widths(&self, total: f32) -> (f32, f32) {
        let left = self.left_width.min((total - SPLIT_SEPARATOR_W).max(0.0));
        let right = (total - left - SPLIT_SEPARATOR_W).max(0.0);
        (left, right)
    }
}

impl Widget for FixedLeftSplit {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let total = if constraints.max.width.is_finite() {
            constraints.max.width
        } else {
            self.left_width + 320.0 + SPLIT_SEPARATOR_W
        };
        let height = constraints.max.height;
        let (left_w, right_w) = self.split_widths(total);
        self.left
            .measure(ctx, Constraints::tight(Size::new(left_w, height)));
        self.separator.measure(
            ctx,
            Constraints::tight(Size::new(SPLIT_SEPARATOR_W, height)),
        );
        self.right
            .measure(ctx, Constraints::tight(Size::new(right_w, height)));
        Size::new(total, height)
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        let (left_w, right_w) = self.split_widths(bounds.width());
        self.left.arrange(
            ctx,
            Rect::new(bounds.x(), bounds.y(), left_w, bounds.height()),
        );
        self.separator.arrange(
            ctx,
            Rect::new(
                bounds.x() + left_w,
                bounds.y(),
                SPLIT_SEPARATOR_W,
                bounds.height(),
            ),
        );
        self.right.arrange(
            ctx,
            Rect::new(
                bounds.x() + left_w + SPLIT_SEPARATOR_W,
                bounds.y(),
                right_w,
                bounds.height(),
            ),
        );
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.left.paint(ctx);
        self.separator.paint(ctx);
        self.right.paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.left.semantics(ctx);
        self.separator.semantics(ctx);
        self.right.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.left);
        visitor.visit(&self.separator);
        visitor.visit(&self.right);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.left);
        visitor.visit(&mut self.separator);
        visitor.visit(&mut self.right);
    }
}

// ---------------------------------------------------------------------------
// Small layout helper (copied from settings.rs — horizontal inset without Padding).
// ---------------------------------------------------------------------------

/// Insets a single child horizontally by `pad` on each side (the vertical axis is left to the
/// caller). Keeps content off the panel edges without pulling in the container `Padding` widget.
struct InsetX {
    child: WidgetPod,
    pad: f32,
}

impl InsetX {
    fn new<W>(child: W, pad: f32) -> Self
    where
        W: Widget + 'static,
    {
        Self {
            child: WidgetPod::new(child),
            pad: pad.max(0.0),
        }
    }
}

impl Widget for InsetX {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let inner_max = Size::new(
            (constraints.max.width - self.pad * 2.0).max(0.0),
            constraints.max.height,
        );
        let inner = Constraints::new(Size::ZERO, inner_max);
        let child = self.child.measure(ctx, inner);
        Size::new(child.width + self.pad * 2.0, child.height)
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        let inner = Rect::new(
            bounds.x() + self.pad,
            bounds.y(),
            (bounds.width() - self.pad * 2.0).max(0.0),
            bounds.height(),
        );
        self.child.arrange(ctx, inner);
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.child.paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.child.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.child);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.child);
    }
}

// ---------------------------------------------------------------------------
// Composed pods (wide split / narrow list-only) + responsive host.
// ---------------------------------------------------------------------------

/// The header bar wrapped so it rebuilds (breadcrumb segments and history affordances) whenever
/// navigation state changes. A fixed-height titlebar surface over a hairline separator.
fn header_bar(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let s = shared.clone();
    let r = Rc::clone(reader);
    let rebuilt = RebuildOnChange::new(
        {
            let s = s.clone();
            move || {
                let files = s.files();
                (
                    files.source,
                    files.path.clone(),
                    files.history_back.len(),
                    files.history_forward.len(),
                    files.loading,
                )
            }
        },
        move |_key| WidgetPod::new(header(&s, &r)),
    );
    InsetX::new(rebuilt, CONTENT_PAD)
}

/// The file-list column wrapped so it rebuilds (rows) whenever the path changes. A shared
/// `scroll_state` is threaded so the position is consistent across the wide/narrow swap.
fn list_column(
    shared: &Shared,
    reader: &ThemeReader,
    scroll_state: &ScrollState,
) -> impl Widget + use<> {
    let s = shared.clone();
    let r = Rc::clone(reader);
    let ss = scroll_state.clone();
    RebuildOnChange::new(
        {
            let s = s.clone();
            move || {
                let files = s.files();
                (files.path.clone(), files.list_revision, files.tasks.clone())
            }
        },
        move |_key| listing(&s, &r, &ss),
    )
}

/// Wide split: header docked on top, body fills with [ list (flex) | vertical separator |
/// inspector (340) ]. We use the `Dock` helper so the body actually *fills* the remaining vertical
/// space (a bare vertical `Stack` only sizes children to their natural height, which would collapse
/// the list/inspector to the listing's content height); and a small [`Split`] so the inspector
/// keeps its fixed 340px while the list takes the rest (a horizontal `Stack` would let the list
/// greedily consume the full width before the inspector is measured).
fn wide_pod(
    shared: &Shared,
    reader: &ThemeReader,
    scroll_state: &ScrollState,
    places_scroll_state: &ScrollState,
    inspector_scroll_state: &ScrollState,
) -> WidgetPod {
    let places = ScrollView::vertical(FilesPlacesRail::new(shared.clone()))
        .state(places_scroll_state.clone());
    let list_with_places = FixedLeftSplit::new(
        places,
        list_column(shared, reader, scroll_state),
        PLACES_W,
        reader,
    );
    let body = Split::new(
        list_with_places,
        inspector(shared, inspector_scroll_state),
        INSPECTOR_W,
        reader,
    );
    let header = Stack::vertical()
        .alignment(Alignment::Stretch)
        .with_child(
            SizedBox::new()
                .height(HEADER_H)
                .with_child(header_bar(shared, reader)),
        )
        .with_child(Separator::horizontal().theme_when(clone_reader(reader)));
    WidgetPod::new(
        widgets::Dock::new(body)
            .top(HEADER_H + 1.0, header)
            .bottom(FILES_STATUS_H, FilesStatusBar::new(shared.clone())),
    )
}

/// Narrow: header docked on top, list-only body fills (the inspector is hidden).
fn narrow_pod(shared: &Shared, reader: &ThemeReader, scroll_state: &ScrollState) -> WidgetPod {
    let header = Stack::vertical()
        .alignment(Alignment::Stretch)
        .with_child(
            SizedBox::new()
                .height(HEADER_H)
                .with_child(header_bar(shared, reader)),
        )
        .with_child(Separator::horizontal().theme_when(clone_reader(reader)));
    WidgetPod::new(
        widgets::Dock::new(list_column(shared, reader, scroll_state))
            .top(HEADER_H + 1.0, header)
            .bottom(FILES_STATUS_H, FilesStatusBar::new(shared.clone())),
    )
}

fn files_action_dialog_signature(shared: &Shared) -> String {
    let files = shared.files();
    match &files.action_prompt {
        Some(prompt) => format!(
            "{:?}|{}|{}|{}",
            prompt.kind,
            prompt.target_path.as_deref().unwrap_or(""),
            prompt.targets.len(),
            prompt.error.as_deref().unwrap_or("")
        ),
        None => "closed".to_string(),
    }
}

fn go_to_path_description(source: FilesSourceKind) -> &'static str {
    match source {
        FilesSourceKind::Sifs => {
            "Open a SIFS path, e.g. /services, /users, or /sinomo/nodes/<node>/files"
        }
        FilesSourceKind::Local => {
            "Open drive, UNC, or mounted path, e.g. C:\\Users or \\\\server\\share"
        }
    }
}

fn go_to_path_placeholder(source: FilesSourceKind) -> &'static str {
    match source {
        FilesSourceKind::Sifs => "/services",
        FilesSourceKind::Local => "C:\\Users or \\\\server\\share",
    }
}

fn files_action_dialog(shared: &Shared, reader: &ThemeReader) -> Option<WidgetPod> {
    let prompt = shared.files().action_prompt.clone()?;
    let source = shared.files().source;
    let title = prompt.kind.title();
    let description = match prompt.kind {
        FileActionPromptKind::GoTo => go_to_path_description(source).to_string(),
        FileActionPromptKind::NewFile => format!("Create a file in {}", shared.files().path),
        FileActionPromptKind::NewFolder => format!("Create a folder in {}", shared.files().path),
        FileActionPromptKind::Copy => prompt
            .target_path
            .as_deref()
            .map(|path| format!("Copy {path} to another {} path", source.operation_scope()))
            .unwrap_or_else(|| "Select a file to copy".to_string()),
        FileActionPromptKind::CopySelected => {
            let label = marked_entries_label(prompt.targets.len());
            format!(
                "Copy {label} into a destination folder in {}",
                source.operation_scope()
            )
        }
        FileActionPromptKind::CopyToSifs => prompt
            .target_path
            .as_deref()
            .map(|path| format!("Copy local {path} into SIFS"))
            .unwrap_or_else(|| "Select a local file or folder to copy into SIFS".to_string()),
        FileActionPromptKind::CopyToLocal => prompt
            .target_path
            .as_deref()
            .map(|path| format!("Download SIFS {path} to a local path"))
            .unwrap_or_else(|| "Select a SIFS file or folder to download".to_string()),
        FileActionPromptKind::TransferSelected => {
            let label = marked_entries_label(prompt.targets.len());
            match source {
                FilesSourceKind::Sifs => format!("Download {label} into a local folder"),
                FilesSourceKind::Local => format!("Copy {label} into a SIFS folder"),
            }
        }
        FileActionPromptKind::Move => prompt
            .target_path
            .as_deref()
            .map(|path| format!("Move {path} to another {} path", source.operation_scope()))
            .unwrap_or_else(|| "Select an item to move".to_string()),
        FileActionPromptKind::MoveSelected => {
            let label = marked_entries_label(prompt.targets.len());
            format!(
                "Move {label} into a destination folder in {}",
                source.operation_scope()
            )
        }
        FileActionPromptKind::Rename => prompt
            .target_path
            .as_deref()
            .map(|path| format!("Rename {path}"))
            .unwrap_or_else(|| "Select an item to rename".to_string()),
        FileActionPromptKind::Delete => prompt
            .target_path
            .as_deref()
            .map(|path| format!("Delete {path}"))
            .unwrap_or_else(|| "Select an item to delete".to_string()),
        FileActionPromptKind::DeleteSelected => {
            let label = marked_entries_label(prompt.targets.len());
            format!("Delete {label} from {}", shared.files().path)
        }
    };
    let input_label = match prompt.kind {
        FileActionPromptKind::GoTo => "Path",
        FileActionPromptKind::Copy
        | FileActionPromptKind::CopySelected
        | FileActionPromptKind::CopyToSifs
        | FileActionPromptKind::CopyToLocal
        | FileActionPromptKind::TransferSelected
        | FileActionPromptKind::MoveSelected
        | FileActionPromptKind::Move => "Destination",
        FileActionPromptKind::Delete | FileActionPromptKind::DeleteSelected => "Type DELETE",
        _ => "Name",
    };
    let input = {
        let s = shared.clone();
        let placeholder = match prompt.kind {
            FileActionPromptKind::GoTo => go_to_path_placeholder(source),
            FileActionPromptKind::CopyToSifs => "/services/artifact",
            FileActionPromptKind::CopyToLocal => "C:\\Users\\you\\Downloads\\artifact",
            FileActionPromptKind::CopySelected | FileActionPromptKind::MoveSelected => match source
            {
                FilesSourceKind::Sifs => "/services/archive",
                FilesSourceKind::Local => "archive",
            },
            FileActionPromptKind::TransferSelected => match source {
                FilesSourceKind::Sifs => "C:\\Users\\you\\Downloads",
                FilesSourceKind::Local => "/services/imports",
            },
            _ => input_label,
        };
        sui::TextInput::new(format!("files-action-input-{title}"))
            .value(prompt.input.clone())
            .placeholder(placeholder)
            .min_width(320.0)
            .theme_when(clone_reader(reader))
            .on_change(move |value| {
                s.files_mut().set_action_prompt_input(value);
            })
    };
    let primary_label = prompt.kind.primary_label();
    let submit = {
        let s = shared.clone();
        sui::Button::new(primary_label)
            .min_width(112.0)
            .theme_when(clone_reader(reader))
            .on_press_with_ctx(move |ctx| {
                let waker = { s.borrow().waker.clone() };
                s.files_mut().submit_action_prompt(waker);
                request_files_overlay_rebuild(ctx);
            })
    };
    let cancel = {
        let s = shared.clone();
        sui::Button::new("Cancel")
            .min_width(112.0)
            .theme_when(secondary_button_theme(reader))
            .on_press_with_ctx(move |ctx| {
                s.files_mut().close_action_prompt();
                request_files_overlay_rebuild(ctx);
            })
    };
    let actions = Stack::horizontal()
        .spacing(8.0)
        .alignment(Alignment::End)
        .with_child(submit)
        .with_child(cancel);

    let dialog_theme = shared.theme();
    let mut body = Stack::vertical()
        .spacing(10.0)
        .alignment(Alignment::Stretch)
        .with_child(sui::Label::new(description).theme(dialog_theme.clone()))
        .with_child(input);
    if let Some(error) = prompt.error {
        body = body.with_child(sui::Label::new(error).theme(dialog_theme.clone()));
    }
    body = body.with_child(actions);

    let dismiss = {
        let s = shared.clone();
        move || {
            s.files_mut().close_action_prompt();
        }
    };

    Some(WidgetPod::new(
        sui::Dialog::new(title, body)
            .theme(dialog_theme)
            .description(input_label)
            .max_width(460.0)
            .dismiss_on_scrim(true)
            .on_dismiss(dismiss),
    ))
}

struct FilesDialogHost {
    shared: Shared,
    reader: ThemeReader,
    body: WidgetPod,
    dialog: Option<WidgetPod>,
    dialog_signature: String,
}

impl FilesDialogHost {
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
        let signature = files_action_dialog_signature(&self.shared);
        if signature == self.dialog_signature {
            return;
        }
        self.dialog_signature = signature;
        self.dialog = files_action_dialog(&self.shared, &self.reader);
    }
}

impl Widget for FilesDialogHost {
    fn event(&mut self, ctx: &mut EventCtx, _event: &Event) {
        let signature = files_action_dialog_signature(&self.shared);
        if signature != self.dialog_signature {
            ctx.request_measure();
            ctx.request_paint();
            ctx.request_semantics();
            ctx.request_hit_test();
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

/// The files tab: a thin responsive host owning two pre-built pods (wide split / narrow list-only)
/// and delegating to the one chosen by the available width. The choice is made in `measure`
/// (constraints-driven) and cached so `arrange`/`paint`/event routing all stay in sync. Mirrors the
/// Settings `SettingsView` host.
pub struct FilesView {
    shared: Shared,
    wide: WidgetPod,
    narrow: WidgetPod,
    use_wide: Cell<bool>,
}

impl FilesView {
    pub fn new(shared: Shared) -> Self {
        let theme_reader: ThemeReader = {
            let s = shared.clone();
            Rc::new(move || s.theme())
        };
        // One shared list scroll state keeps the file position consistent across the wide/narrow
        // swap. The inspector owns a separate scroll state because expanded technical details are
        // independent of the directory listing.
        let scroll_state = ScrollState::new();
        let places_scroll_state = ScrollState::new();
        let inspector_scroll_state = ScrollState::new();
        let wide = wide_pod(
            &shared,
            &theme_reader,
            &scroll_state,
            &places_scroll_state,
            &inspector_scroll_state,
        );
        let narrow = narrow_pod(&shared, &theme_reader, &scroll_state);
        Self {
            shared,
            wide,
            narrow,
            // Default to wide before the first measure.
            use_wide: Cell::new(true),
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
}

impl Widget for FilesView {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        // Navigation (a directory row / breadcrumb / up button) mutates the path and the inner
        // `RebuildOnChange` rebuilds the list + breadcrumb, but those nested rebuilds need a fresh
        // measure+arrange for the host. A pointer release inside the tab is the only moment a
        // navigation can happen, so request a relayout then (this is the path the headless tests
        // exercise, since the harness never dispatches RedrawRequested). Matches `SettingsView`.
        if let Event::Pointer(pointer) = event
            && matches!(pointer.kind, sui::PointerEventKind::Up)
        {
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_paint();
        }
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let waker = { self.shared.borrow().waker.clone() };
        self.shared.files_mut().ensure_loaded(waker);
        let use_wide = constraints.max.width >= WIDE_THRESHOLD;
        self.use_wide.set(use_wide);
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

/// Build the files tab content.
pub fn view(shared: Shared) -> impl Widget {
    let reader: ThemeReader = {
        let s = shared.clone();
        Rc::new(move || s.theme())
    };
    FilesDialogHost::new(FilesView::new(shared.clone()), shared, reader)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Backend;
    use crate::state::AppState;

    fn files_test_temp_dir(name: &str) -> PathBuf {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "sinomo-app-files-{name}-{}-{stamp}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create temp dir");
        root
    }

    #[test]
    fn crumbs_start_at_root_and_split_on_segments() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.path = "/services/sinomo".to_string();
        let crumbs = state.crumbs();
        assert_eq!(crumbs[0], ("SIFS".to_string(), "/".to_string()));
        assert_eq!(crumbs.last().unwrap().1, "/services/sinomo");
        // Every crumb after root is a progressively deeper absolute path.
        assert!(
            crumbs
                .iter()
                .skip(1)
                .all(|(_, target)| target.starts_with('/'))
        );
    }

    #[test]
    fn crumbs_compact_node_ids_without_changing_targets() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.path = "/sinomo/nodes/01kr3d54d45zkm0p0xgps8k7mt/files".to_string();

        let crumbs = state.crumbs();

        assert_eq!(crumbs[0], ("SIFS".to_string(), "/".to_string()));
        assert_eq!(crumbs[1], ("sinomo".to_string(), "/sinomo".to_string()));
        assert_eq!(
            crumbs[2],
            ("nodes".to_string(), "/sinomo/nodes".to_string())
        );
        assert_eq!(
            crumbs[3],
            (
                "node 01kr3d5…gps8k7mt".to_string(),
                "/sinomo/nodes/01kr3d54d45zkm0p0xgps8k7mt".to_string()
            )
        );
        assert_eq!(
            crumbs[4],
            (
                "files".to_string(),
                "/sinomo/nodes/01kr3d54d45zkm0p0xgps8k7mt/files".to_string()
            )
        );
    }

    #[test]
    fn crumb_label_compacts_long_non_node_segments() {
        assert_eq!(crumb_label("services", None), "services");
        assert_eq!(
            crumb_label("sinomo-app-chat-session-01ktsg9dw850xkprvhkn0kfr8t", None),
            "sinomo-app…rvhkn0kfr8t"
        );
    }

    #[test]
    fn display_name_for_width_keeps_generated_file_names_readable() {
        let name = "sinomo-app-chat-session-01ktsg9dw850xkprvhkn0kfr8t.json";

        assert_eq!(
            display_name_for_width("artifact.txt", 320.0, 7.1),
            "artifact.txt"
        );
        assert_eq!(
            display_name_for_width(name, 180.0, 8.8),
            "sinomo-ap…kfr8t.json"
        );
        assert_eq!(display_name_for_width(name, 40.0, 8.8), "s…on");
        assert_eq!(display_name_for_width(name, 0.0, 8.8), "");
    }

    #[test]
    fn new_defers_sifs_open_until_files_tab_is_rendered() {
        let backend = Backend::local_fallback();
        let state = FilesState::new(&backend);

        assert!(!state.loaded);
        assert!(!state.loading);
        assert!(state.browser.is_none());
        assert!(state.error.is_none());
        assert!(state.entries.is_empty());
        assert!(!state.inspector_details_open);
    }

    #[test]
    fn activate_directory_navigates_file_selects() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        // Seed a known listing so the test is independent of the SIFS backend contents.
        state.entries = vec![
            SifsEntry {
                name: "dir".to_string(),
                path: "/dir".to_string(),
                is_dir: true,
                is_link: false,
                size: None,
            },
            SifsEntry {
                name: "file.txt".to_string(),
                path: "/file.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(10),
            },
        ];
        state.selected = None;

        // Activating a file row selects it and reports no path change.
        let path_changed = state.activate_row(1);
        assert!(!path_changed, "selecting a file does not change the path");
        assert_eq!(state.selected, Some(1));

        let path_changed = state.activate_row(0);
        assert!(path_changed, "activating a directory navigates");
        assert_eq!(state.path, "/dir");
        assert_eq!(state.selected, None);
    }

    #[test]
    fn navigation_history_tracks_paths_and_sources() {
        let root = files_test_temp_dir("navigation-history");
        fs::create_dir(root.join("nested")).expect("create nested dir");

        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.loaded = true;
        state.loading = false;
        state.path = "/".to_string();
        state.sifs_path = "/".to_string();
        state.local_path = display_local_path(root.to_string_lossy().into_owned());

        state.navigate("/services".to_string());
        assert_eq!(state.path, "/services");
        assert!(state.can_go_back());
        assert!(!state.can_go_forward());

        state.navigate("/users".to_string());
        assert_eq!(state.path, "/users");

        state.go_back(None);
        assert_eq!(state.source, FilesSourceKind::Sifs);
        assert_eq!(state.path, "/services");
        assert!(state.can_go_back());
        assert!(state.can_go_forward());

        state.switch_source(FilesSourceKind::Local, None);
        assert_eq!(state.source, FilesSourceKind::Local);
        assert_eq!(state.path, state.local_path);
        assert!(state.can_go_back());
        assert!(!state.can_go_forward());

        state.go_back(None);
        assert_eq!(state.source, FilesSourceKind::Sifs);
        assert_eq!(state.path, "/services");
        assert!(state.can_go_forward());

        state.go_forward(None);
        assert_eq!(state.source, FilesSourceKind::Local);
        assert_eq!(state.path, state.local_path);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn select_entry_allows_directory_inspection_without_navigation() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.path = "/".to_string();
        state.entries = vec![SifsEntry {
            name: "dir".to_string(),
            path: "/dir".to_string(),
            is_dir: true,
            is_link: false,
            size: None,
        }];

        assert!(state.select_entry(0));
        assert_eq!(state.path, "/");
        assert_eq!(state.selected, Some(0));
        assert!(state.selected_entry().is_some_and(|entry| entry.is_dir));
        assert!(!state.select_entry(99));
        assert_eq!(state.selected, Some(0));
    }

    #[test]
    fn directory_inspect_action_is_visible_before_hover() {
        let dir = SifsEntry {
            name: "dir".to_string(),
            path: "/dir".to_string(),
            is_dir: true,
            is_link: false,
            size: None,
        };
        let file = SifsEntry {
            name: "file.txt".to_string(),
            path: "/file.txt".to_string(),
            is_dir: false,
            is_link: false,
            size: Some(10),
        };

        assert!(
            row_action_visible(&dir, SifsEntryRowHit::Inspect, false, false, false, None),
            "directory rows should always expose the inspect/manage affordance"
        );
        assert!(
            !row_action_visible(&file, SifsEntryRowHit::Inspect, false, false, false, None),
            "file rows should keep secondary actions quiet until hover or selection"
        );
        assert!(row_action_visible(
            &file,
            SifsEntryRowHit::Inspect,
            true,
            false,
            false,
            None
        ));
        assert!(row_action_visible(
            &file,
            SifsEntryRowHit::Select,
            false,
            false,
            true,
            None
        ));
    }

    #[test]
    fn selected_entry_tracks_selection() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.entries = vec![SifsEntry {
            name: "a.txt".to_string(),
            path: "/a.txt".to_string(),
            is_dir: false,
            is_link: false,
            size: Some(1),
        }];
        assert!(state.selected_entry().is_none());
        state.selected = Some(0);
        assert_eq!(
            state.selected_entry().map(|e| e.name.as_str()),
            Some("a.txt")
        );
    }

    #[test]
    fn switch_source_lists_local_files_and_preserves_sifs_path() {
        let root = files_test_temp_dir("source-switch");
        fs::write(root.join("alpha.txt"), b"alpha").expect("write file");
        fs::create_dir(root.join("nested")).expect("create nested dir");

        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.path = "/services".to_string();
        state.sifs_path = "/services".to_string();
        state.local_path = root.to_string_lossy().into_owned();

        state.switch_source(FilesSourceKind::Local, None);

        assert_eq!(state.source, FilesSourceKind::Local);
        assert_eq!(state.path, root.to_string_lossy());
        assert!(state.error.is_none());
        assert_eq!(state.node_id, local_node_label());
        assert!(
            state.entries.iter().any(|entry| {
                entry.name == "alpha.txt" && !entry.is_dir && entry.size == Some(5)
            })
        );
        assert!(
            state
                .entries
                .iter()
                .any(|entry| { entry.name == "nested" && entry.is_dir })
        );
        assert_eq!(files_source_status(&state), "Local files");

        state.switch_source(FilesSourceKind::Sifs, None);
        assert_eq!(state.source, FilesSourceKind::Sifs);
        assert_eq!(state.path, "/services");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn files_places_include_sifs_and_local_targets() {
        let root = files_test_temp_dir("places-model");
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.local_path = root.to_string_lossy().into_owned();
        state.sifs_path = "/services".to_string();
        state.node_id = "node-desktop".to_string();

        let places = files_places(&state);

        assert!(places.iter().any(|place| {
            place.label == "SIFS root" && place.source == FilesSourceKind::Sifs && place.path == "/"
        }));
        assert!(places.iter().any(|place| {
            place.label == "Services"
                && place.source == FilesSourceKind::Sifs
                && place.path == "/services"
        }));
        assert!(places.iter().any(|place| {
            place.label == "Cluster nodes"
                && place.source == FilesSourceKind::Sifs
                && place.path == "/sinomo/nodes"
        }));
        assert!(places.iter().any(|place| {
            place.label == "This node files"
                && place.detail == "local-owned SIFS"
                && place.source == FilesSourceKind::Sifs
                && place.path == "/sinomo/nodes/node-desktop/files"
        }));
        assert!(places.iter().any(|place| {
            place.label == "SIFS path"
                && place.source == FilesSourceKind::Sifs
                && place.path == "/services"
                && place.action == FilePlaceAction::OpenPathPrompt
        }));
        assert!(places.iter().any(|place| {
            place.label == "Current local"
                && place.source == FilesSourceKind::Local
                && place.path == root.to_string_lossy()
        }));
        let current_root = local_root_path(&state.local_path).expect("temp dir has root");
        assert!(places.iter().any(|place| {
            place.label == local_root_label(&current_root)
                && place.detail == "current volume"
                && place.source == FilesSourceKind::Local
                && place.path == current_root
                && place.action == FilePlaceAction::OpenPath
        }));
        assert!(places.iter().any(|place| {
            place.label == "Other path"
                && place.source == FilesSourceKind::Local
                && place.action == FilePlaceAction::OpenPathPrompt
        }));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn places_rail_opens_sifs_services() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/".to_string();
            files.sifs_path = "/".to_string();
            files.entries = vec![SifsEntry {
                name: "services".to_string(),
                path: "/services".to_string(),
                is_dir: true,
                is_link: false,
                size: None,
            }];
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new().title("Files places").root(
                    SizedBox::new()
                        .width(1120.0)
                        .height(720.0)
                        .with_child(view(shared.clone())),
                ),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        let snapshot = window.snapshot()?;
        let row = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == sui::SemanticsRole::ListItem && node.name.as_deref() == Some("dir")
            })
            .expect("directory row semantics should be present");
        assert!(
            row.description.as_deref().is_some_and(|description| {
                description.contains("Activate opens the folder; Inspect selects it for management")
            }),
            "directory row should explain the open versus inspect split"
        );

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Open Services")
            .click()?;

        let files = shared.files();
        assert_eq!(files.source, FilesSourceKind::Sifs);
        assert_eq!(files.path, "/services");

        Ok(())
    }

    #[test]
    fn local_copy_move_prompts_resolve_destination_paths() {
        let root = files_test_temp_dir("local-destinations");
        let absolute_target = root.join("elsewhere.txt");

        assert_eq!(
            resolve_local_destination_path(&root.to_string_lossy(), "archive/artifact.txt"),
            Ok(root
                .join("archive")
                .join("artifact.txt")
                .to_string_lossy()
                .into_owned())
        );
        assert_eq!(
            resolve_local_destination_path(
                &root.to_string_lossy(),
                &absolute_target.to_string_lossy()
            ),
            Ok(absolute_target.to_string_lossy().into_owned())
        );

        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.source = FilesSourceKind::Local;
        state.path = root.to_string_lossy().into_owned();
        state.entries = vec![SifsEntry {
            name: "artifact.txt".to_string(),
            path: root.join("artifact.txt").to_string_lossy().into_owned(),
            is_dir: false,
            is_link: false,
            size: Some(5),
        }];
        state.selected = Some(0);

        state.open_action_prompt(FileActionPromptKind::Copy);
        let prompt = state.action_prompt.as_ref().expect("copy prompt").clone();
        assert_eq!(prompt.input, "artifact copy.txt");
        let (operation, _, _) = state
            .operation_from_prompt(&prompt)
            .expect("copy operation");
        assert_eq!(operation.kind, FilesOperationKind::Copy);
        let expected_target = root
            .join("artifact copy.txt")
            .to_string_lossy()
            .into_owned();
        assert_eq!(operation.target.as_deref(), Some(expected_target.as_str()));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cross_source_prompts_resolve_sifs_and_local_destinations() {
        let root = files_test_temp_dir("cross-source-destinations");
        let local_source = root.join("artifact.txt");
        fs::write(&local_source, b"hello").expect("write local source");

        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.source = FilesSourceKind::Local;
        state.path = root.to_string_lossy().into_owned();
        state.local_path = state.path.clone();
        state.sifs_path = "/services".to_string();
        state.entries = vec![SifsEntry {
            name: "artifact.txt".to_string(),
            path: local_source.to_string_lossy().into_owned(),
            is_dir: false,
            is_link: false,
            size: Some(5),
        }];
        state.selected = Some(0);

        state.open_action_prompt(FileActionPromptKind::CopyToSifs);
        let prompt = state
            .action_prompt
            .as_ref()
            .expect("copy to SIFS prompt")
            .clone();
        assert_eq!(prompt.input, "/services/artifact.txt");
        let (operation, title, detail) = state
            .operation_from_prompt(&prompt)
            .expect("copy to SIFS operation");
        assert_eq!(operation.kind, FilesOperationKind::CopyToSifs);
        assert_eq!(operation.path, local_source.to_string_lossy());
        assert_eq!(operation.target.as_deref(), Some("/services/artifact.txt"));
        assert_eq!(title, "Copy to SIFS");
        assert!(detail.ends_with(" -> /services/artifact.txt"));

        state.source = FilesSourceKind::Sifs;
        state.path = "/services".to_string();
        state.local_path = root.to_string_lossy().into_owned();
        state.entries = vec![SifsEntry {
            name: "artifact.txt".to_string(),
            path: "/services/artifact.txt".to_string(),
            is_dir: false,
            is_link: false,
            size: Some(5),
        }];
        state.selected = Some(0);

        state.open_action_prompt(FileActionPromptKind::CopyToLocal);
        let prompt = state
            .action_prompt
            .as_ref()
            .expect("download prompt")
            .clone();
        assert_eq!(prompt.input, root.join("artifact.txt").to_string_lossy());
        let (operation, title, detail) = state
            .operation_from_prompt(&prompt)
            .expect("download operation");
        assert_eq!(operation.kind, FilesOperationKind::CopyToLocal);
        assert_eq!(operation.path, "/services/artifact.txt");
        assert_eq!(
            operation.target.as_deref(),
            Some(root.join("artifact.txt").to_string_lossy().as_ref())
        );
        assert_eq!(title, "Download");
        assert!(detail.starts_with("/services/artifact.txt -> "));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn local_copy_operation_writes_destination_file() {
        let root = files_test_temp_dir("local-copy-operation");
        let source = root.join("artifact.bin");
        let target = root.join("artifact copy.bin");
        let bytes = vec![7_u8; 1024 * 1024 + 17];
        fs::write(&source, &bytes).expect("write source");

        let outcome = execute_local_operation(
            FilesOperation {
                kind: FilesOperationKind::Copy,
                path: source.to_string_lossy().into_owned(),
                target: Some(target.to_string_lossy().into_owned()),
                directory: false,
            },
            &root.to_string_lossy(),
        )
        .expect("copy operation");

        assert_eq!(fs::read(&target).expect("read copied file"), bytes);
        assert_eq!(
            outcome.select_path.as_deref(),
            Some(target.to_string_lossy().as_ref())
        );
        assert_eq!(outcome.refresh_path, root.to_string_lossy().into_owned());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cross_source_copy_imports_and_downloads_files_through_sifs() {
        let backend = Backend::local_fallback();
        let browser = SifsBrowser::open(&backend.data_dir).expect("open SIFS browser");
        browser
            .create_dir_all("/services")
            .expect("create SIFS services");
        let root = files_test_temp_dir("cross-source-file-operation");
        let local_source = root.join("artifact.txt");
        let local_download = root.join("downloaded.txt");
        fs::write(&local_source, b"hello from local").expect("write local source");

        let import = execute_cross_source_operation(
            FilesSourceKind::Local,
            &browser,
            FilesOperation {
                kind: FilesOperationKind::CopyToSifs,
                path: local_source.to_string_lossy().into_owned(),
                target: Some("/services/imported.txt".to_string()),
                directory: false,
            },
            &root.to_string_lossy(),
            None,
        )
        .expect("import file to SIFS");

        assert_eq!(
            browser
                .read_file("/services/imported.txt")
                .expect("read SIFS import"),
            b"hello from local"
        );
        assert_eq!(import.refresh_path, root.to_string_lossy());
        assert_eq!(
            import.select_path.as_deref(),
            Some(local_source.to_string_lossy().as_ref())
        );

        let download = execute_cross_source_operation(
            FilesSourceKind::Sifs,
            &browser,
            FilesOperation {
                kind: FilesOperationKind::CopyToLocal,
                path: "/services/imported.txt".to_string(),
                target: Some(local_download.to_string_lossy().into_owned()),
                directory: false,
            },
            "/services",
            None,
        )
        .expect("download SIFS file");

        assert_eq!(
            fs::read(&local_download).expect("read local download"),
            b"hello from local"
        );
        assert_eq!(download.refresh_path, "/services");
        assert_eq!(
            download.select_path.as_deref(),
            Some("/services/imported.txt")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cross_source_copy_recurses_between_local_and_sifs_directories() {
        let backend = Backend::local_fallback();
        let browser = SifsBrowser::open(&backend.data_dir).expect("open SIFS browser");
        browser
            .create_dir_all("/services")
            .expect("create SIFS services");
        let root = files_test_temp_dir("cross-source-directory-operation");
        let source = root.join("workspace");
        let nested = source.join("nested");
        let download = root.join("downloaded-workspace");
        fs::create_dir_all(&nested).expect("create local tree");
        fs::write(source.join("README.md"), b"readme").expect("write root file");
        fs::write(nested.join("artifact.bin"), b"nested").expect("write nested file");

        execute_cross_source_operation(
            FilesSourceKind::Local,
            &browser,
            FilesOperation {
                kind: FilesOperationKind::CopyToSifs,
                path: source.to_string_lossy().into_owned(),
                target: Some("/services/workspace".to_string()),
                directory: true,
            },
            &root.to_string_lossy(),
            None,
        )
        .expect("import directory to SIFS");

        assert_eq!(
            browser
                .read_file("/services/workspace/README.md")
                .expect("read imported root file"),
            b"readme"
        );
        assert_eq!(
            browser
                .read_file("/services/workspace/nested/artifact.bin")
                .expect("read imported nested file"),
            b"nested"
        );

        execute_cross_source_operation(
            FilesSourceKind::Sifs,
            &browser,
            FilesOperation {
                kind: FilesOperationKind::CopyToLocal,
                path: "/services/workspace".to_string(),
                target: Some(download.to_string_lossy().into_owned()),
                directory: true,
            },
            "/services",
            None,
        )
        .expect("download SIFS directory");

        assert_eq!(
            fs::read(download.join("README.md")).expect("read downloaded root file"),
            b"readme"
        );
        assert_eq!(
            fs::read(download.join("nested").join("artifact.bin"))
                .expect("read downloaded nested file"),
            b"nested"
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn local_recursive_copy_operation_copies_directory_tree() {
        let root = files_test_temp_dir("local-copy-directory-operation");
        let source = root.join("workspace");
        let nested = source.join("nested");
        let target = root.join("workspace copy");
        fs::create_dir_all(&nested).expect("create source tree");
        fs::write(source.join("README.md"), b"hello").expect("write root file");
        fs::write(nested.join("artifact.bin"), b"nested").expect("write nested file");

        let outcome = execute_local_operation(
            FilesOperation {
                kind: FilesOperationKind::Copy,
                path: source.to_string_lossy().into_owned(),
                target: Some(target.to_string_lossy().into_owned()),
                directory: true,
            },
            &root.to_string_lossy(),
        )
        .expect("recursive copy operation");

        assert_eq!(
            fs::read(target.join("README.md")).expect("read copied root file"),
            b"hello"
        );
        assert_eq!(
            fs::read(target.join("nested").join("artifact.bin")).expect("read copied nested file"),
            b"nested"
        );
        assert_eq!(
            outcome.select_path.as_deref(),
            Some(target.to_string_lossy().as_ref())
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn local_recursive_delete_operation_removes_directory_tree() {
        let root = files_test_temp_dir("local-delete-directory-operation");
        let source = root.join("workspace");
        let nested = source.join("nested");
        fs::create_dir_all(&nested).expect("create source tree");
        fs::write(source.join("README.md"), b"hello").expect("write root file");
        fs::write(nested.join("artifact.bin"), b"nested").expect("write nested file");

        let outcome = execute_local_operation(
            FilesOperation {
                kind: FilesOperationKind::Delete,
                path: source.to_string_lossy().into_owned(),
                target: None,
                directory: true,
            },
            &root.to_string_lossy(),
        )
        .expect("recursive delete operation");

        assert!(!source.exists());
        assert_eq!(outcome.refresh_path, root.to_string_lossy().into_owned());
        assert_eq!(outcome.select_path, None);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn local_recursive_copy_rejects_destination_inside_source() {
        let root = files_test_temp_dir("local-copy-directory-loop");
        let source = root.join("workspace");
        let target = source.join("workspace copy");
        fs::create_dir_all(&source).expect("create source tree");

        let error = execute_local_operation(
            FilesOperation {
                kind: FilesOperationKind::Copy,
                path: source.to_string_lossy().into_owned(),
                target: Some(target.to_string_lossy().into_owned()),
                directory: true,
            },
            &root.to_string_lossy(),
        )
        .expect_err("recursive copy into itself should be rejected");

        assert_eq!(error, "copy destination cannot be inside the source folder");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn worker_result_updates_selected_local_details() {
        let root = files_test_temp_dir("local-details");
        let path = root.join("artifact.txt");
        fs::write(&path, b"hello").expect("write file");
        let path = path.to_string_lossy().into_owned();

        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.source = FilesSourceKind::Local;
        state.entries = vec![SifsEntry {
            name: "artifact.txt".to_string(),
            path: path.clone(),
            is_dir: false,
            is_link: false,
            size: Some(5),
        }];
        state.selected = Some(0);
        state.details_loading = true;
        state.pending_details_request_id = Some(13);

        let details = local_details(&path).expect("local details");
        let changed = state.apply_worker_result(FilesWorkerResult::Details {
            request_id: 13,
            source: FilesSourceKind::Local,
            path: path.clone(),
            result: Ok(FilesObjectDetails::Local(details)),
        });

        assert!(changed);
        assert!(!state.details_loading);
        assert!(state.selected_details.is_none());
        assert_eq!(
            state
                .selected_local_details
                .as_ref()
                .map(|details| details.kind.as_str()),
            Some("file")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn copy_move_prompts_resolve_destination_paths() {
        assert_eq!(
            suggested_copy_name("artifact.txt"),
            "artifact copy.txt".to_string()
        );
        assert_eq!(suggested_copy_name("artifact"), "artifact copy".to_string());
        assert_eq!(
            resolve_sifs_destination_path("/services", "archive/artifact.txt"),
            Ok("/services/archive/artifact.txt".to_string())
        );
        assert_eq!(
            resolve_sifs_destination_path("/services", "/users/alice/artifact.txt"),
            Ok("/users/alice/artifact.txt".to_string())
        );

        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.path = "/services".to_string();
        state.entries = vec![SifsEntry {
            name: "artifact.txt".to_string(),
            path: "/services/artifact.txt".to_string(),
            is_dir: false,
            is_link: false,
            size: Some(5),
        }];
        state.selected = Some(0);

        state.open_action_prompt(FileActionPromptKind::Copy);
        let prompt = state.action_prompt.as_ref().expect("copy prompt").clone();
        assert_eq!(prompt.input, "artifact copy.txt");
        let (operation, title, detail) = state
            .operation_from_prompt(&prompt)
            .expect("copy operation");
        assert_eq!(operation.kind, FilesOperationKind::Copy);
        assert_eq!(operation.path, "/services/artifact.txt");
        assert_eq!(
            operation.target.as_deref(),
            Some("/services/artifact copy.txt")
        );
        assert_eq!(title, "Copy");
        assert_eq!(
            detail,
            "/services/artifact.txt -> /services/artifact copy.txt"
        );

        state.open_action_prompt(FileActionPromptKind::Move);
        state.set_action_prompt_input("/users/alice/artifact.txt".to_string());
        let prompt = state.action_prompt.as_ref().expect("move prompt").clone();
        let (operation, title, detail) = state
            .operation_from_prompt(&prompt)
            .expect("move operation");
        assert_eq!(operation.kind, FilesOperationKind::Move);
        assert_eq!(
            operation.target.as_deref(),
            Some("/users/alice/artifact.txt")
        );
        assert_eq!(title, "Move");
        assert_eq!(
            detail,
            "/services/artifact.txt -> /users/alice/artifact.txt"
        );
    }

    #[test]
    fn copy_prompt_accepts_directories_and_move_allows_them() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.path = "/services".to_string();
        state.entries = vec![SifsEntry {
            name: "logs".to_string(),
            path: "/services/logs".to_string(),
            is_dir: true,
            is_link: false,
            size: None,
        }];
        state.selected = Some(0);

        state.open_action_prompt(FileActionPromptKind::Copy);
        let prompt = state.action_prompt.as_ref().expect("copy prompt").clone();
        let (operation, _, _) = state
            .operation_from_prompt(&prompt)
            .expect("directory copy operation");
        assert_eq!(operation.kind, FilesOperationKind::Copy);
        assert!(operation.directory);
        assert_eq!(operation.target.as_deref(), Some("/services/logs copy"));

        state.open_action_prompt(FileActionPromptKind::Move);
        state.set_action_prompt_input("/users/logs".to_string());
        let prompt = state.action_prompt.as_ref().expect("move prompt").clone();
        let (operation, _, _) = state
            .operation_from_prompt(&prompt)
            .expect("directory move operation");
        assert_eq!(operation.kind, FilesOperationKind::Move);
        assert!(operation.directory);
        assert_eq!(operation.target.as_deref(), Some("/users/logs"));
    }

    #[test]
    fn inspector_summary_reflects_unavailable_state() {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/".to_string();
            files.error = Some("device authorization was denied".to_string());
        }

        let inspector = InspectorView::new(shared.clone());

        assert_eq!(
            inspector.summary(),
            "error|Sifs|/|device authorization was denied|false|"
        );
        shared.files_mut().inspector_details_open = true;
        assert_eq!(
            inspector.summary(),
            "error|Sifs|/|device authorization was denied|true|"
        );
    }

    #[test]
    fn inspector_summary_reflects_loading_and_empty_state() {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loading = true;
            files.path = "/services".to_string();
        }
        let inspector = InspectorView::new(shared.clone());
        assert_eq!(inspector.summary(), "loading|Sifs|/services|false|");

        {
            let mut files = shared.files_mut();
            files.inspector_details_open = true;
        }
        assert_eq!(inspector.summary(), "loading|Sifs|/services|true|");

        {
            let mut files = shared.files_mut();
            files.loading = false;
            files.entries.clear();
        }
        assert_eq!(inspector.summary(), "unselected|Sifs|/services|false|0|");
    }

    #[test]
    fn inspector_summary_reflects_selected_details_disclosure() {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.entries = vec![SifsEntry {
                name: "artifact.txt".to_string(),
                path: "/artifact.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(5),
            }];
            files.selected = Some(0);
            files.clustered = true;
            files.details_error = Some("device authorization was denied".to_string());
        }

        let inspector = InspectorView::new(shared.clone());
        assert!(inspector.summary().contains(
            "/artifact.txt|false|false|true|false|device authorization was denied|false||false|"
        ));

        shared.files_mut().inspector_details_open = true;
        assert!(inspector.summary().contains(
            "/artifact.txt|false|false|true|false|device authorization was denied|false||true|"
        ));
    }

    #[test]
    fn unselected_inspector_copy_is_source_specific() {
        assert_eq!(
            unselected_inspector_title(FilesSourceKind::Sifs),
            "Current SIFS location"
        );
        assert_eq!(
            unselected_inspector_status(FilesSourceKind::Sifs, false),
            "Select an object to inspect placement"
        );
        assert_eq!(
            unselected_inspector_title(FilesSourceKind::Local),
            "Current folder"
        );
        assert_eq!(
            unselected_inspector_status(FilesSourceKind::Local, false),
            "Select an object to inspect metadata"
        );
        assert_eq!(
            unselected_inspector_status(FilesSourceKind::Local, true),
            "No entries in this directory"
        );
    }

    #[test]
    fn unselected_inspector_rows_explain_current_location_management() {
        let backend = Backend::local_fallback();
        let mut files = FilesState::new(&backend);
        files.loaded = true;
        files.loading = false;
        files.source = FilesSourceKind::Sifs;
        files.path = "/cluster-tests".to_string();
        files.clustered = true;
        files.entries.clear();

        let rows = unselected_inspector_rows(&files);
        assert!(rows.iter().any(|(label, value)| {
            *label == "Scope" && value == "cluster-backed SIFS namespace"
        }));
        assert!(rows.iter().any(|(label, value)| {
            *label == "Next step"
                && value.contains("created SIFS objects expose placement and replica actions")
        }));

        files.entries = vec![SifsEntry {
            name: "artifact.txt".to_string(),
            path: "/cluster-tests/artifact.txt".to_string(),
            is_dir: false,
            is_link: false,
            size: Some(5),
        }];
        let rows = unselected_inspector_rows(&files);
        assert!(rows.iter().any(|(label, value)| {
            *label == "Next step" && value.contains("refresh placement, repair replicas")
        }));

        files.source = FilesSourceKind::Local;
        files.clustered = false;
        files.entries.clear();
        let rows = unselected_inspector_rows(&files);
        assert!(
            rows.iter()
                .any(|(label, value)| *label == "Scope" && value == "host filesystem")
        );
        assert!(rows.iter().any(|(label, value)| {
            *label == "Next step" && value.contains("switch places to browse another filesystem")
        }));
    }

    #[test]
    fn sifs_unavailable_rows_hide_diagnostics_until_expanded() {
        let loading = sifs_loading_rows("/services", false);
        assert!(
            loading
                .iter()
                .any(|(label, value)| *label == "Status" && value == "Connecting to SIFS")
        );
        assert!(
            !loading
                .iter()
                .any(|(_, value)| value.contains("file storage"))
        );

        let collapsed = sifs_unavailable_rows("device authorization was denied", false);
        assert!(collapsed.iter().any(|(label, value)| {
            *label == "Status" && value == "SIFS access needs authorization."
        }));
        assert!(collapsed.iter().any(|(label, _)| *label == "Next"));
        assert!(
            collapsed
                .iter()
                .any(|(label, value)| { *label == "Available" && value.contains("UNC paths") })
        );
        assert!(
            collapsed
                .iter()
                .any(|(label, value)| { *label == "Next" && value.contains("browse local files") })
        );
        assert!(
            !collapsed
                .iter()
                .any(|(_, value)| value.contains("device authorization"))
        );
        assert!(!collapsed.iter().any(|(label, _)| *label == "Reason"));
        assert!(!collapsed.iter().any(|(label, _)| *label == "Authority"));

        let expanded = sifs_unavailable_rows("device authorization was denied", true);
        assert!(
            expanded
                .iter()
                .any(|(label, value)| *label == "Reason"
                    && value == "device authorization was denied")
        );
        assert!(
            expanded
                .iter()
                .any(|(label, value)| *label == "Authority" && value == "SIFS through daemon")
        );
    }

    #[test]
    fn selected_metadata_status_uses_compact_user_labels() {
        assert_eq!(
            sifs_metadata_status_label(true, None, None),
            "Loading metadata"
        );
        assert_eq!(
            sifs_metadata_status_label(false, Some("device authorization was denied"), None),
            "Metadata unavailable"
        );
        assert_eq!(
            sifs_metadata_status_label(false, None, None),
            "Metadata not loaded"
        );

        let details = SifsObjectDetails {
            file_system_id: "sifs".to_string(),
            object_id: "obj".to_string(),
            path: "/artifact.txt".to_string(),
            kind: "file".to_string(),
            version: "v1".to_string(),
            content_hash: None,
            size_bytes: Some(5),
            link_count: 1,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            policy: "replicated-eventual".to_string(),
            current_locations: vec!["node-a".to_string()],
            local_pins: Vec::new(),
            health: None,
        };
        assert_eq!(
            sifs_metadata_status_label(false, None, Some(&details)),
            "Metadata current"
        );
    }

    #[test]
    fn sifs_replication_summary_uses_user_facing_status() {
        assert_eq!(
            sifs_replication_summary(None, true),
            "metadata not loaded; refresh placement to retry"
        );
        assert_eq!(sifs_replication_summary(None, false), "local node only");

        let mut details = SifsObjectDetails {
            file_system_id: "sifs".to_string(),
            object_id: "obj".to_string(),
            path: "/artifact.txt".to_string(),
            kind: "file".to_string(),
            version: "v1".to_string(),
            content_hash: None,
            size_bytes: Some(5),
            link_count: 1,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            policy: "replicated-eventual".to_string(),
            current_locations: vec!["node-a".to_string()],
            local_pins: Vec::new(),
            health: None,
        };
        assert_eq!(
            sifs_replication_summary(Some(&details), true),
            "one durable location recorded; replica repair may still be pending"
        );
        details.current_locations.push("node-b".to_string());
        assert_eq!(
            sifs_replication_summary(Some(&details), true),
            "2 durable locations recorded"
        );
        details.policy = "local-owned".to_string();
        assert_eq!(
            sifs_replication_summary(Some(&details), true),
            "local placement policy"
        );
    }

    #[test]
    fn sifs_health_summary_uses_operator_repair_state() {
        assert_eq!(sifs_health_summary(None), "operator health not loaded");

        let mut details = SifsObjectDetails {
            file_system_id: "sifs".to_string(),
            object_id: "obj".to_string(),
            path: "/artifact.txt".to_string(),
            kind: "file".to_string(),
            version: "v1".to_string(),
            content_hash: None,
            size_bytes: Some(5),
            link_count: 1,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            policy: "replicated-eventual".to_string(),
            current_locations: vec!["node-a".to_string()],
            local_pins: Vec::new(),
            health: Some(SifsObjectHealth {
                agent_state: "pending-repair".to_string(),
                version_count: 2,
                receipt_count: 1,
                intent_count: 1,
                open_conflict_count: 0,
                pending_repair_count: 1,
                missing_replica_nodes: vec!["node-b-long-identifier".to_string()],
            }),
        };

        assert_eq!(
            sifs_health_summary(Some(&details)),
            "pending-repair; 1 missing replica"
        );
        assert_eq!(
            sifs_placement_next_step(Some(&details), true),
            "repair replicas, then refresh placement"
        );
        assert_eq!(
            details
                .health
                .as_ref()
                .map(sifs_missing_replica_summary)
                .as_deref(),
            Some("…e-b-long-identifier")
        );

        let first = sifs_details_signature(Some(&details));
        details.health.as_mut().unwrap().pending_repair_count = 0;
        assert_ne!(sifs_details_signature(Some(&details)), first);
    }

    #[test]
    fn sifs_compact_placement_summary_fits_selected_header() {
        assert_eq!(
            sifs_compact_placement_summary(None, true, true, None),
            "loading placement"
        );
        assert_eq!(
            sifs_compact_placement_summary(None, true, false, Some("metadata failed")),
            "placement unavailable"
        );
        assert_eq!(
            sifs_compact_placement_summary(None, true, false, None),
            "placement pending"
        );
        assert_eq!(
            sifs_compact_placement_summary(None, false, false, None),
            "local node only"
        );

        let mut details = SifsObjectDetails {
            file_system_id: "sifs".to_string(),
            object_id: "obj".to_string(),
            path: "/artifact.txt".to_string(),
            kind: "file".to_string(),
            version: "v1".to_string(),
            content_hash: None,
            size_bytes: Some(5),
            link_count: 1,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            policy: "replicated-eventual".to_string(),
            current_locations: vec!["node-a".to_string(), "node-b".to_string()],
            local_pins: Vec::new(),
            health: Some(SifsObjectHealth {
                agent_state: "healthy".to_string(),
                version_count: 1,
                receipt_count: 2,
                intent_count: 0,
                open_conflict_count: 0,
                pending_repair_count: 0,
                missing_replica_nodes: Vec::new(),
            }),
        };
        assert_eq!(
            sifs_compact_placement_summary(Some(&details), true, false, None),
            "2 nodes healthy"
        );

        details.health.as_mut().unwrap().pending_repair_count = 1;
        assert_eq!(
            sifs_compact_placement_summary(Some(&details), true, false, None),
            "2 nodes · 1 missing replica"
        );
    }

    #[test]
    fn sifs_placement_next_step_uses_policy_and_location_state() {
        assert_eq!(
            sifs_placement_next_step(None, true),
            "load SIFS metadata before changing placement"
        );
        assert_eq!(
            sifs_placement_next_step(None, false),
            "join a cluster to enable multi-node placement"
        );

        let mut details = SifsObjectDetails {
            file_system_id: "sifs".to_string(),
            object_id: "obj".to_string(),
            path: "/artifact.txt".to_string(),
            kind: "file".to_string(),
            version: "v1".to_string(),
            content_hash: None,
            size_bytes: Some(5),
            link_count: 1,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            policy: "replicated-eventual".to_string(),
            current_locations: Vec::new(),
            local_pins: Vec::new(),
            health: None,
        };

        assert_eq!(
            sifs_placement_next_step(Some(&details), true),
            "refresh metadata; no durable replica receipt is recorded yet"
        );
        details.current_locations.push("node-a".to_string());
        assert_eq!(
            sifs_placement_next_step(Some(&details), true),
            "keep peers online and refresh after replica repair settles"
        );
        details.current_locations.push("node-b".to_string());
        assert_eq!(
            sifs_placement_next_step(Some(&details), true),
            "replica locations are visible; use details for object and version ids"
        );
        details.policy = "local-owned".to_string();
        assert_eq!(
            sifs_placement_next_step(Some(&details), true),
            "copy or move into a replicated SIFS namespace to spread across nodes"
        );
    }

    #[test]
    fn sifs_inspector_next_step_uses_actionable_metadata_state() {
        assert_eq!(
            sifs_inspector_next_step(None, true, true, None),
            "waiting for daemon metadata"
        );
        assert_eq!(
            sifs_inspector_next_step(None, true, false, Some("metadata read failed")),
            "Show metadata details, then refresh placement"
        );
        assert_eq!(
            sifs_inspector_next_step(None, true, false, Some("device authorization was denied")),
            "Authorize SIFS in Connection, then refresh metadata"
        );
        assert_eq!(
            sifs_inspector_next_step(
                None,
                true,
                false,
                Some("SIFS browsing requires a daemon connection")
            ),
            "Check daemon in Connection, then retry metadata"
        );
        assert_eq!(
            sifs_inspector_next_step(None, true, false, None),
            "use Refresh placement to load durable-node metadata"
        );
        assert_eq!(
            sifs_inspector_next_step(None, false, false, None),
            "join a cluster to enable multi-node placement"
        );
    }

    #[test]
    fn sifs_preview_error_display_points_to_connection_recovery() {
        assert_eq!(
            file_preview_error_display(FilesSourceKind::Local, "access is denied"),
            "access is denied"
        );
        assert_eq!(
            file_preview_error_display(FilesSourceKind::Sifs, "device authorization was denied"),
            "SIFS preview needs authorization. Open Connection settings, authorize this device, then retry preview."
        );
        assert_eq!(
            file_preview_error_display(
                FilesSourceKind::Sifs,
                "SIFS browsing requires a daemon connection"
            ),
            "SIFS preview needs a daemon connection. Check Connection settings, then retry preview."
        );
    }

    #[test]
    fn sifs_details_signature_tracks_placement_changes() {
        let mut details = SifsObjectDetails {
            file_system_id: "sifs".to_string(),
            object_id: "obj".to_string(),
            path: "/artifact.txt".to_string(),
            kind: "file".to_string(),
            version: "v1".to_string(),
            content_hash: None,
            size_bytes: Some(5),
            link_count: 1,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            policy: "replicated-eventual".to_string(),
            current_locations: vec!["node-a".to_string()],
            local_pins: Vec::new(),
            health: None,
        };
        let first = sifs_details_signature(Some(&details));

        details.current_locations.push("node-b".to_string());
        assert_ne!(sifs_details_signature(Some(&details)), first);

        let second = sifs_details_signature(Some(&details));
        details.policy = "local-owned".to_string();
        assert_ne!(sifs_details_signature(Some(&details)), second);
    }

    #[test]
    fn sifs_pin_summary_surfaces_local_content_pins() {
        let mut details = SifsObjectDetails {
            file_system_id: "sifs".to_string(),
            object_id: "obj".to_string(),
            path: "/artifact.txt".to_string(),
            kind: "file".to_string(),
            version: "v1".to_string(),
            content_hash: None,
            size_bytes: Some(5),
            link_count: 1,
            mode: 0o644,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            policy: "replicated-eventual".to_string(),
            current_locations: vec!["node-a".to_string()],
            local_pins: vec![sinomo_cli::app_api::SifsPinDetails {
                pinned_by_node_id: "node-a".to_string(),
                mode: "content".to_string(),
                created_at_unix_nanos: Some(0),
            }],
            health: None,
        };

        assert_eq!(sifs_pin_summary(Some(&details)), "content pin on node-a");
        assert!(sifs_pin_detail_summary(&details).contains("content on node-a since"));

        let pinned = sifs_details_signature(Some(&details));
        details.local_pins.clear();
        assert_eq!(sifs_pin_summary(Some(&details)), "not pinned on this node");
        assert_ne!(sifs_details_signature(Some(&details)), pinned);
    }

    #[test]
    fn files_status_summary_reports_location_selection_and_tasks() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.loaded = true;
        state.loading = false;
        state.path = "/cluster-tests".to_string();
        state.entries = vec![SifsEntry {
            name: "artifact.txt".to_string(),
            path: "/cluster-tests/artifact.txt".to_string(),
            is_dir: false,
            is_link: false,
            size: Some(42),
        }];
        state.selected = Some(0);
        state.tasks = vec![FilesTask {
            id: 42,
            source: FilesSourceKind::Sifs,
            title: "Delete".to_string(),
            detail: "/cluster-tests/artifact.txt".to_string(),
            state: FilesTaskState::Running,
            progress: Some(0.35),
        }];

        assert_eq!(
            files_status_summary(&state),
            "Local SIFS | Path /cluster-tests | 1 entry | selected /cluster-tests/artifact.txt | 1 task running"
        );

        state.tasks[0].state = FilesTaskState::Cancelled;
        assert_eq!(
            files_status_summary(&state),
            "Local SIFS | Path /cluster-tests | 1 entry | selected /cluster-tests/artifact.txt | 1 task cancelled"
        );

        state.tasks[0].state = FilesTaskState::Completed;
        assert_eq!(
            files_status_summary(&state),
            "Local SIFS | Path /cluster-tests | 1 entry | selected /cluster-tests/artifact.txt | 1 task done"
        );

        state.marked_entries.insert(0);
        assert_eq!(files_status_bar_selection(&state), "1 marked");
        assert_eq!(
            files_status_summary(&state),
            "Local SIFS | Path /cluster-tests | 1 entry | 1 entry marked | 1 task done"
        );
    }

    #[test]
    fn clear_finished_tasks_retains_active_operations() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.tasks = vec![
            FilesTask {
                id: 1,
                source: FilesSourceKind::Sifs,
                title: "Copy".to_string(),
                detail: "running".to_string(),
                state: FilesTaskState::Running,
                progress: Some(0.4),
            },
            FilesTask {
                id: 2,
                source: FilesSourceKind::Sifs,
                title: "Delete".to_string(),
                detail: "done".to_string(),
                state: FilesTaskState::Completed,
                progress: Some(1.0),
            },
            FilesTask {
                id: 3,
                source: FilesSourceKind::Sifs,
                title: "Move".to_string(),
                detail: "failed".to_string(),
                state: FilesTaskState::Failed,
                progress: Some(1.0),
            },
            FilesTask {
                id: 4,
                source: FilesSourceKind::Sifs,
                title: "Rename".to_string(),
                detail: "cancelling".to_string(),
                state: FilesTaskState::CancelRequested,
                progress: Some(0.8),
            },
        ];

        state.clear_finished_tasks();

        assert_eq!(state.tasks.len(), 2);
        assert_eq!(state.tasks[0].id, 1);
        assert_eq!(state.tasks[1].id, 4);
    }

    #[test]
    fn sifs_unavailable_summary_uses_recovery_language() {
        assert_eq!(
            sifs_unavailable_summary("device authorization was denied"),
            "SIFS access needs authorization."
        );
        assert_eq!(
            sifs_unavailable_summary("SIFS browsing requires a daemon connection"),
            "SIFS needs a daemon connection."
        );
        assert_eq!(
            sifs_unavailable_summary("SIFS listing did not finish within 20 seconds"),
            "SIFS did not respond in time."
        );
    }

    #[test]
    fn task_detail_display_makes_sifs_failures_actionable() {
        let sifs_task = FilesTask {
            id: 1,
            source: FilesSourceKind::Sifs,
            title: "Refresh placement".to_string(),
            detail: "device authorization was denied".to_string(),
            state: FilesTaskState::Failed,
            progress: Some(1.0),
        };
        assert_eq!(
            files_task_detail_display(&sifs_task),
            "Authorize SIFS in Connection"
        );
        assert_eq!(
            FilesTaskStrip::detail_display(&sifs_task, 120.0),
            "Authorize SIFS"
        );
        let compact = files_task_detail_display_for_width(&sifs_task, 180.0);
        assert_eq!(compact, "Authorize SIFS in Connection");
        assert!(compact.chars().count() <= 29);

        let local_task = FilesTask {
            id: 2,
            source: FilesSourceKind::Local,
            title: "Copy".to_string(),
            detail: "access denied".to_string(),
            state: FilesTaskState::Failed,
            progress: Some(1.0),
        };
        assert_eq!(
            files_task_detail_display(&local_task),
            "Local filesystem error: access denied"
        );

        let running_task = FilesTask {
            id: 3,
            source: FilesSourceKind::Sifs,
            title: "Copy".to_string(),
            detail: "Copying 4.0 MB of 8.0 MB".to_string(),
            state: FilesTaskState::Running,
            progress: Some(0.5),
        };
        assert_eq!(
            files_task_detail_display(&running_task),
            "Copying 4.0 MB of 8.0 MB"
        );
    }

    #[test]
    fn connection_attention_reports_failed_sifs_tasks() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.tasks = vec![
            FilesTask {
                id: 1,
                source: FilesSourceKind::Sifs,
                title: "Refresh placement".to_string(),
                detail: "device authorization was denied".to_string(),
                state: FilesTaskState::Failed,
                progress: Some(1.0),
            },
            FilesTask {
                id: 2,
                source: FilesSourceKind::Local,
                title: "Delete".to_string(),
                detail: "access denied".to_string(),
                state: FilesTaskState::Failed,
                progress: Some(1.0),
            },
        ];

        assert_eq!(
            state.connection_attention_detail(),
            Some("Refresh placement failed: device authorization was denied".to_string())
        );

        state.error = Some("daemon connection refused".to_string());
        assert_eq!(
            state.connection_attention_detail(),
            Some("SIFS unavailable: daemon connection refused".to_string())
        );
    }

    #[test]
    fn middle_short_keeps_long_sifs_names_single_line() {
        let name = "sinomo-app-chat-session-01ktsg9dw850xkprvhkn0kfr8t.json";
        let compact = middle_short(name, 36);

        assert_eq!(compact.chars().count(), 36);
        assert!(compact.starts_with("sinomo-app-chat"));
        assert!(compact.ends_with("hkn0kfr8t.json"));
        assert!(compact.contains('…'));
    }

    #[test]
    fn files_secondary_button_theme_remaps_accent_slots() {
        let reader: ThemeReader = Rc::new(DefaultTheme::light);
        let base = reader();
        let secondary = secondary_button_theme(&reader)();

        assert_eq!(secondary.palette.accent, base.palette.control);
        assert_eq!(secondary.palette.accent_hover, base.palette.control_hover);
        assert_eq!(
            secondary.palette.accent_pressed,
            base.palette.control_active
        );
        assert_eq!(secondary.palette.accent_text, base.palette.text);
    }

    #[test]
    fn worker_result_updates_listing_snapshot() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.loading = true;
        state.pending_request_id = Some(7);
        state.path = "/".to_string();

        let changed = state.apply_worker_result(FilesWorkerResult::List {
            request_id: 7,
            source: FilesSourceKind::Sifs,
            path: "/services".to_string(),
            result: Ok(FilesListing {
                node_id: "node-a".to_string(),
                entries: vec![SifsEntry {
                    name: "sinomo".to_string(),
                    path: "/services/sinomo".to_string(),
                    is_dir: true,
                    is_link: false,
                    size: None,
                }],
            }),
        });

        assert!(changed);
        assert!(!state.loading);
        assert_eq!(state.pending_request_id, None);
        assert_eq!(state.path, "/services");
        assert_eq!(state.node_id, "node-a");
        assert_eq!(state.entries.len(), 1);
        assert_eq!(state.list_revision, 1);
    }

    #[test]
    fn worker_error_clears_loading_snapshot() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.loading = true;
        state.pending_request_id = Some(9);
        state.entries = vec![SifsEntry {
            name: "old".to_string(),
            path: "/old".to_string(),
            is_dir: false,
            is_link: false,
            size: Some(1),
        }];

        let changed = state.apply_worker_result(FilesWorkerResult::List {
            request_id: 9,
            source: FilesSourceKind::Sifs,
            path: "/".to_string(),
            result: Err("timed out".to_string()),
        });

        assert!(changed);
        assert!(!state.loading);
        assert_eq!(state.pending_request_id, None);
        assert_eq!(state.error.as_deref(), Some("timed out"));
        assert!(state.entries.is_empty());
    }

    #[test]
    fn worker_result_updates_selected_sifs_details() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.entries = vec![SifsEntry {
            name: "artifact.txt".to_string(),
            path: "/services/artifact.txt".to_string(),
            is_dir: false,
            is_link: false,
            size: None,
        }];
        state.selected = Some(0);
        state.details_loading = true;
        state.pending_details_request_id = Some(11);

        let changed = state.apply_worker_result(FilesWorkerResult::Details {
            request_id: 11,
            source: FilesSourceKind::Sifs,
            path: "/services/artifact.txt".to_string(),
            result: Ok(FilesObjectDetails::Sifs(SifsObjectDetails {
                file_system_id: "sifs".to_string(),
                object_id: "obj_artifact".to_string(),
                path: "/services/artifact.txt".to_string(),
                kind: "file".to_string(),
                version: "v2".to_string(),
                content_hash: Some("blake3:abc".to_string()),
                size_bytes: Some(5),
                link_count: 1,
                mode: 0o644,
                uid: 0,
                gid: 0,
                accessed_at_unix_nanos: 0,
                modified_at_unix_nanos: 0,
                created_at_unix_nanos: 0,
                changed_at_unix_nanos: 0,
                policy: "replicated-eventual".to_string(),
                current_locations: vec!["node-a".to_string(), "node-b".to_string()],
                local_pins: Vec::new(),
                health: None,
            })),
        });

        assert!(changed);
        assert!(!state.details_loading);
        assert_eq!(state.pending_details_request_id, None);
        assert_eq!(
            state
                .selected_details
                .as_ref()
                .map(|details| details.policy.as_str()),
            Some("replicated-eventual")
        );
    }

    #[test]
    fn local_read_file_prefix_caps_large_files() {
        let root = files_test_temp_dir("preview-prefix");
        let path = root.join("large.txt");
        fs::write(&path, b"abcdef").expect("write file");

        let read = local_read_file_prefix(&path.to_string_lossy(), 3).expect("prefix read");

        assert_eq!(read.bytes, b"abc");
        assert!(read.truncated);

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn preview_from_read_formats_text_empty_and_binary_files() {
        let text = preview_from_read(
            "notes.txt",
            SifsFileRead {
                bytes: b"alpha\n\nbeta\n".to_vec(),
                truncated: false,
            },
        );
        assert_eq!(text.title, "Preview notes.txt");
        assert_eq!(text.lines, vec!["alpha", "(blank line)", "beta"]);
        assert!(!text.binary);
        assert!(!text.truncated);

        let empty = preview_from_read(
            "empty.txt",
            SifsFileRead {
                bytes: Vec::new(),
                truncated: false,
            },
        );
        assert_eq!(empty.lines, vec!["Empty file"]);
        assert!(!empty.binary);

        let binary = preview_from_read(
            "image.bin",
            SifsFileRead {
                bytes: vec![0, 1, 2, 3],
                truncated: true,
            },
        );
        assert_eq!(binary.title, "Binary preview image.bin");
        assert!(binary.binary);
        assert!(binary.truncated);
        assert!(binary.lines.iter().any(|line| line.contains("00 01 02 03")));
    }

    #[test]
    fn worker_result_updates_selected_preview_and_ignores_stale_preview() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.entries = vec![SifsEntry {
            name: "artifact.txt".to_string(),
            path: "/services/artifact.txt".to_string(),
            is_dir: false,
            is_link: false,
            size: Some(5),
        }];
        state.selected = Some(0);
        state.preview_loading = true;
        state.pending_preview_request_id = Some(22);

        let changed = state.apply_worker_result(FilesWorkerResult::Preview {
            request_id: 22,
            source: FilesSourceKind::Sifs,
            path: "/services/artifact.txt".to_string(),
            result: Ok(preview_from_read(
                "artifact.txt",
                SifsFileRead {
                    bytes: b"hello".to_vec(),
                    truncated: false,
                },
            )),
        });

        assert!(changed);
        assert!(!state.preview_loading);
        assert_eq!(state.pending_preview_request_id, None);
        assert_eq!(
            state
                .selected_preview
                .as_ref()
                .map(|preview| preview.lines.as_slice()),
            Some(&["hello".to_string()][..])
        );

        state.preview_loading = true;
        state.pending_preview_request_id = Some(23);
        let changed = state.apply_worker_result(FilesWorkerResult::Preview {
            request_id: 22,
            source: FilesSourceKind::Sifs,
            path: "/services/artifact.txt".to_string(),
            result: Err("late error".to_string()),
        });

        assert!(!changed);
        assert!(state.preview_loading);
        assert_eq!(state.pending_preview_request_id, Some(23));
        assert_eq!(state.preview_error, None);
    }

    #[test]
    fn worker_progress_updates_running_task_detail_and_fraction() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.tasks = vec![FilesTask {
            id: 21,
            source: FilesSourceKind::Sifs,
            title: "Copy".to_string(),
            detail: "queued".to_string(),
            state: FilesTaskState::Running,
            progress: Some(0.02),
        }];

        let changed = state.apply_worker_result(FilesWorkerResult::TaskProgress {
            request_id: 21,
            detail: "Copied 4.0 MB of 8.0 MB".to_string(),
            progress: Some(0.51),
        });

        assert!(changed);
        assert_eq!(state.tasks[0].detail, "Copied 4.0 MB of 8.0 MB");
        assert_eq!(state.tasks[0].progress, Some(0.51));
        assert_eq!(task_progress_fraction(&state.tasks[0]), 0.51);

        state.tasks[0].state = FilesTaskState::CancelRequested;
        let changed = state.apply_worker_result(FilesWorkerResult::TaskProgress {
            request_id: 21,
            detail: "late progress".to_string(),
            progress: Some(0.9),
        });

        assert!(!changed);
        assert_eq!(state.tasks[0].detail, "Copied 4.0 MB of 8.0 MB");
        assert_eq!(state.tasks[0].progress, Some(0.51));
    }

    #[test]
    fn cancelled_operation_result_marks_task_cancelled() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.loaded = true;
        state.loading = false;
        state.path = "/services".to_string();
        state.tasks = vec![FilesTask {
            id: 12,
            source: FilesSourceKind::Sifs,
            title: "Delete".to_string(),
            detail: "/services/artifact.txt".to_string(),
            state: FilesTaskState::CancelRequested,
            progress: Some(0.75),
        }];
        mark_cancelled_operation(&state.worker_cancellations, 12);

        let changed = state.apply_operation_result(
            12,
            Err(FilesOperationFailure::Cancelled(
                "cancelled before deleting /services/artifact.txt".to_string(),
            )),
        );

        assert!(changed);
        assert_eq!(state.tasks[0].state, FilesTaskState::Cancelled);
        assert_eq!(
            state.tasks[0].detail,
            "cancelled before deleting /services/artifact.txt"
        );
        assert_eq!(state.path, "/services");
        assert!(!operation_cancelled(&state.worker_cancellations, 12));
        assert_eq!(files_task_status(&state), "1 task cancelled");
    }

    #[test]
    fn retry_resets_stale_sifs_worker_snapshot() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        let (tx, _rx) = mpsc::channel();
        let (_result_tx, result_rx) = mpsc::channel();

        state.loaded = true;
        state.loading = true;
        state.error = Some("device authorization was denied".to_string());
        state.node_id = "node-old".to_string();
        state.entries = vec![SifsEntry {
            name: "old".to_string(),
            path: "/old".to_string(),
            is_dir: false,
            is_link: false,
            size: Some(1),
        }];
        state.selected = Some(0);
        state.inspector_details_open = true;
        state.pending_request_id = Some(42);
        state.worker_tx = Some(tx);
        state.worker_rx = Some(result_rx);
        state.path = "/services".to_string();

        state.reset_for_retry();

        assert!(!state.loaded);
        assert!(!state.loading);
        assert!(state.error.is_none());
        assert!(state.entries.is_empty());
        assert_eq!(state.selected, None);
        assert_eq!(state.pending_request_id, None);
        assert!(state.worker_tx.is_none());
        assert!(state.worker_rx.is_none());
        assert_eq!(state.node_id, "");
        assert_eq!(state.path, "/services");
        assert!(!state.inspector_details_open);
    }

    #[test]
    fn refresh_queues_current_sifs_path_without_navigation() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        let (tx, rx) = mpsc::channel();
        let (_result_tx, result_rx) = mpsc::channel();

        state.loaded = true;
        state.loading = false;
        state.path = "/services".to_string();
        state.entries = vec![SifsEntry {
            name: "old".to_string(),
            path: "/services/old".to_string(),
            is_dir: false,
            is_link: false,
            size: Some(1),
        }];
        state.selected = Some(0);
        state.worker_tx = Some(tx);
        state.worker_rx = Some(result_rx);

        state.refresh(None);

        assert!(state.loading);
        assert_eq!(state.path, "/services");
        assert_eq!(state.selected, None);
        assert!(state.entries.is_empty());
        assert_eq!(state.pending_request_id, Some(0));
        assert_eq!(state.list_revision, 1);
        match rx.try_recv().expect("refresh command") {
            FilesWorkerCommand::List {
                request_id,
                source,
                path,
            } => {
                assert_eq!(request_id, 0);
                assert_eq!(source, FilesSourceKind::Sifs);
                assert_eq!(path, "/services");
            }
            FilesWorkerCommand::Details { .. } => panic!("expected list command"),
            FilesWorkerCommand::Preview { .. } => panic!("expected list command"),
            FilesWorkerCommand::Operation { .. } => panic!("expected list command"),
        }
    }

    #[test]
    fn placement_refresh_queues_sifs_worker_operation() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        let (tx, rx) = mpsc::channel();
        let (_result_tx, result_rx) = mpsc::channel();

        state.loaded = true;
        state.loading = false;
        state.path = "/services".to_string();
        state.entries = vec![SifsEntry {
            name: "artifact.txt".to_string(),
            path: "/services/artifact.txt".to_string(),
            is_dir: false,
            is_link: false,
            size: Some(1),
        }];
        state.selected = Some(0);
        state.worker_tx = Some(tx);
        state.worker_rx = Some(result_rx);

        state.start_placement_refresh(None);

        assert_eq!(state.tasks[0].title, "Refresh placement");
        match rx.try_recv().expect("placement refresh command") {
            FilesWorkerCommand::Operation {
                request_id,
                source,
                operation,
            } => {
                assert_eq!(request_id, state.tasks[0].id);
                assert_eq!(source, FilesSourceKind::Sifs);
                assert_eq!(operation.kind, FilesOperationKind::RefreshPlacement);
                assert_eq!(operation.path, "/services/artifact.txt");
            }
            FilesWorkerCommand::List { .. } => panic!("expected operation command"),
            FilesWorkerCommand::Details { .. } => panic!("expected operation command"),
            FilesWorkerCommand::Preview { .. } => panic!("expected operation command"),
        }
    }

    #[test]
    fn path_normalization_keeps_sifs_paths_absolute() {
        assert_eq!(
            normalize_path("/users/../services/./sinomo"),
            "/services/sinomo"
        );
        assert_eq!(normalize_path("users\\sinomo"), "/users/sinomo");
        assert_eq!(
            normalize_jump_path(FilesSourceKind::Sifs, "/", " services\\sinomo/.. ").as_deref(),
            Ok("/services")
        );
        assert_eq!(
            normalize_jump_path(FilesSourceKind::Sifs, "/", "   "),
            Err("path cannot be empty".to_string())
        );
        assert_eq!(
            parent_path("/services/sinomo"),
            Some("/services".to_string())
        );
        assert_eq!(parent_path("/"), None);
    }

    #[test]
    fn local_jump_paths_resolve_against_current_file_manager_path() {
        let root = files_test_temp_dir("local-jump");
        let nested = root.join("nested");
        fs::create_dir_all(&nested).expect("create nested");
        let root_label = root.to_string_lossy().into_owned();
        let nested_label = nested.to_string_lossy().into_owned();
        let expected_nested = normalize_local_path(&nested_label).expect("normalize nested");

        assert_eq!(
            normalize_jump_path(FilesSourceKind::Local, &root_label, "nested").as_deref(),
            Ok(expected_nested.as_str())
        );
        assert_eq!(
            normalize_jump_path(FilesSourceKind::Local, &root_label, &nested_label).as_deref(),
            Ok(expected_nested.as_str())
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn file_filter_matches_name_path_kind_and_size() {
        let entry = SifsEntry {
            name: "Artifact.bin".to_string(),
            path: "/services/releases/Artifact.bin".to_string(),
            is_dir: false,
            is_link: false,
            size: Some(1024),
        };
        let dir = SifsEntry {
            name: "logs".to_string(),
            path: "/services/logs".to_string(),
            is_dir: true,
            is_link: false,
            size: None,
        };

        assert!(file_entry_matches_filter(
            &entry,
            &normalized_files_filter("artifact")
        ));
        assert!(file_entry_matches_filter(
            &entry,
            &normalized_files_filter("releases")
        ));
        assert!(file_entry_matches_filter(
            &entry,
            &normalized_files_filter("1.0 kb")
        ));
        assert!(file_entry_matches_filter(
            &dir,
            &normalized_files_filter("folder")
        ));
        assert!(!file_entry_matches_filter(
            &entry,
            &normalized_files_filter("missing")
        ));
    }

    #[test]
    fn visible_file_indices_sort_entries_without_losing_original_indices() {
        let entries = vec![
            SifsEntry {
                name: "logs".to_string(),
                path: "/services/logs".to_string(),
                is_dir: true,
                is_link: false,
                size: None,
            },
            SifsEntry {
                name: "small.txt".to_string(),
                path: "/services/small.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(5),
            },
            SifsEntry {
                name: "large.bin".to_string(),
                path: "/services/large.bin".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(4096),
            },
            SifsEntry {
                name: "alias".to_string(),
                path: "/services/alias".to_string(),
                is_dir: false,
                is_link: true,
                size: None,
            },
        ];

        assert_eq!(
            visible_file_indices(&entries, "", FileSortMode::NameAsc),
            vec![0, 3, 2, 1]
        );
        assert_eq!(
            visible_file_indices(&entries, "", FileSortMode::NameDesc),
            vec![0, 1, 2, 3]
        );
        assert_eq!(
            visible_file_indices(&entries, "", FileSortMode::Kind),
            vec![0, 3, 2, 1]
        );
        assert_eq!(
            visible_file_indices(&entries, "", FileSortMode::SizeDesc),
            vec![0, 2, 1, 3]
        );
        assert_eq!(
            visible_file_indices(
                &entries,
                &normalized_files_filter("txt"),
                FileSortMode::SizeDesc
            ),
            vec![1]
        );
    }

    #[test]
    fn marked_entries_filter_and_queue_batch_delete_tasks() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.loaded = true;
        state.loading = false;
        state.path = "/services".to_string();
        state.entries = vec![
            SifsEntry {
                name: "artifact.txt".to_string(),
                path: "/services/artifact.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(5),
            },
            SifsEntry {
                name: "notes.md".to_string(),
                path: "/services/notes.md".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(9),
            },
            SifsEntry {
                name: "logs".to_string(),
                path: "/services/logs".to_string(),
                is_dir: true,
                is_link: false,
                size: None,
            },
        ];

        state.mark_visible_entries();
        assert_eq!(state.marked_count(), 3);
        state.set_filter_query("artifact".to_string());
        assert_eq!(state.marked_count(), 1);

        state.clear_marked_entries();
        state.set_filter_query(String::new());
        assert!(state.toggle_marked_entry(0));
        assert!(state.toggle_marked_entry(2));
        state.open_action_prompt(FileActionPromptKind::DeleteSelected);
        {
            let prompt = state
                .action_prompt
                .as_ref()
                .expect("delete selected prompt");
            assert_eq!(prompt.kind, FileActionPromptKind::DeleteSelected);
            assert_eq!(prompt.targets.len(), 2);
            assert_eq!(prompt.targets[0].path, "/services/artifact.txt");
            assert_eq!(prompt.targets[1].path, "/services/logs");
        }

        state.submit_action_prompt(None);
        assert_eq!(
            state
                .action_prompt
                .as_ref()
                .and_then(|prompt| prompt.error.as_deref()),
            Some("type DELETE to remove 2 marked entries")
        );

        let (tx, rx) = mpsc::channel();
        state.worker_tx = Some(tx);
        state.set_action_prompt_input("DELETE".to_string());
        state.submit_action_prompt(None);

        assert!(state.action_prompt.is_none());
        assert_eq!(state.marked_count(), 0);
        assert_eq!(state.tasks.len(), 2);
        for expected in ["/services/artifact.txt", "/services/logs"] {
            match rx.try_recv().expect("delete operation command") {
                FilesWorkerCommand::Operation {
                    source, operation, ..
                } => {
                    assert_eq!(source, FilesSourceKind::Sifs);
                    assert_eq!(operation.kind, FilesOperationKind::Delete);
                    assert_eq!(operation.path, expected);
                }
                FilesWorkerCommand::List { .. } => panic!("expected operation command"),
                FilesWorkerCommand::Details { .. } => panic!("expected operation command"),
                FilesWorkerCommand::Preview { .. } => panic!("expected operation command"),
            }
        }
    }

    #[test]
    fn marked_entries_queue_batch_copy_move_and_transfer_tasks() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.loaded = true;
        state.loading = false;
        state.path = "/services".to_string();
        state.sifs_path = "/services".to_string();
        state.local_path = files_test_temp_dir("batch-download")
            .to_string_lossy()
            .into_owned();
        state.entries = vec![
            SifsEntry {
                name: "artifact.txt".to_string(),
                path: "/services/artifact.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(5),
            },
            SifsEntry {
                name: "notes.md".to_string(),
                path: "/services/notes.md".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(9),
            },
            SifsEntry {
                name: "logs".to_string(),
                path: "/services/logs".to_string(),
                is_dir: true,
                is_link: false,
                size: None,
            },
        ];
        assert!(state.toggle_marked_entry(0));
        assert!(state.toggle_marked_entry(2));

        state.open_action_prompt(FileActionPromptKind::CopySelected);
        state.set_action_prompt_input("/users/archive".to_string());
        let prompt = state.action_prompt.as_ref().expect("copy selected").clone();
        let operations = state
            .operations_from_marked_prompt(&prompt)
            .expect("copy selected operations");
        assert_eq!(operations.len(), 2);
        assert_eq!(operations[0].operation.kind, FilesOperationKind::Copy);
        assert_eq!(
            operations[0].operation.target.as_deref(),
            Some("/users/archive/artifact.txt")
        );
        assert_eq!(
            operations[1].operation.target.as_deref(),
            Some("/users/archive/logs")
        );

        state.open_action_prompt(FileActionPromptKind::MoveSelected);
        state.set_action_prompt_input("/users/moved".to_string());
        let prompt = state.action_prompt.as_ref().expect("move selected").clone();
        let operations = state
            .operations_from_marked_prompt(&prompt)
            .expect("move selected operations");
        assert_eq!(operations[0].operation.kind, FilesOperationKind::Move);
        assert_eq!(
            operations[0].operation.target.as_deref(),
            Some("/users/moved/artifact.txt")
        );

        state.open_action_prompt(FileActionPromptKind::TransferSelected);
        let prompt = state
            .action_prompt
            .as_ref()
            .expect("transfer selected")
            .clone();
        let operations = state
            .operations_from_marked_prompt(&prompt)
            .expect("transfer selected operations");
        assert_eq!(
            operations[0].operation.kind,
            FilesOperationKind::CopyToLocal
        );
        let expected_download = Path::new(&state.local_path)
            .join("artifact.txt")
            .to_string_lossy()
            .into_owned();
        assert_eq!(
            operations[0].operation.target.as_deref(),
            Some(expected_download.as_str())
        );

        let (tx, rx) = mpsc::channel();
        state.worker_tx = Some(tx);
        state.open_action_prompt(FileActionPromptKind::CopySelected);
        state.set_action_prompt_input("/services/archive".to_string());
        state.submit_action_prompt(None);

        assert!(state.action_prompt.is_none());
        assert_eq!(state.marked_count(), 0);
        assert_eq!(state.tasks.len(), 2);
        for expected in ["/services/archive/artifact.txt", "/services/archive/logs"] {
            match rx.try_recv().expect("copy operation command") {
                FilesWorkerCommand::Operation {
                    source, operation, ..
                } => {
                    assert_eq!(source, FilesSourceKind::Sifs);
                    assert_eq!(operation.kind, FilesOperationKind::Copy);
                    assert_eq!(operation.target.as_deref(), Some(expected));
                }
                FilesWorkerCommand::List { .. } => panic!("expected operation command"),
                FilesWorkerCommand::Details { .. } => panic!("expected operation command"),
                FilesWorkerCommand::Preview { .. } => panic!("expected operation command"),
            }
        }

        let _ = fs::remove_dir_all(&state.local_path);
    }

    #[test]
    fn setting_filter_clears_hidden_selection() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.loaded = true;
        state.loading = false;
        state.path = "/services".to_string();
        state.entries = vec![
            SifsEntry {
                name: "logs".to_string(),
                path: "/services/logs".to_string(),
                is_dir: true,
                is_link: false,
                size: None,
            },
            SifsEntry {
                name: "artifact.txt".to_string(),
                path: "/services/artifact.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(42),
            },
        ];
        state.selected = Some(0);
        state.selected_details = Some(SifsObjectDetails {
            file_system_id: "sifs".to_string(),
            object_id: "obj".to_string(),
            path: "/services/logs".to_string(),
            kind: "directory".to_string(),
            version: "v1".to_string(),
            content_hash: None,
            size_bytes: None,
            link_count: 1,
            mode: 0o755,
            uid: 0,
            gid: 0,
            accessed_at_unix_nanos: 0,
            modified_at_unix_nanos: 0,
            created_at_unix_nanos: 0,
            changed_at_unix_nanos: 0,
            policy: "replicated-eventual".to_string(),
            current_locations: Vec::new(),
            local_pins: Vec::new(),
            health: None,
        });

        state.set_filter_query("artifact".to_string());

        assert_eq!(state.selected, None);
        assert!(state.selected_details.is_none());
        assert_eq!(files_entry_count_status(&state), "1/2 entries");
    }

    #[test]
    fn unavailable_placeholder_opens_connection_settings() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.error = Some("device authorization was denied".to_string());
            files.entries.clear();
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files unavailable")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        let snapshot = window.snapshot()?;

        for name in ["New file", "New folder"] {
            assert!(
                snapshot.accessibility.nodes.iter().any(|node| {
                    node.role == sui::SemanticsRole::Button
                        && node.name.as_deref() == Some(name)
                        && node.state.disabled
                }),
                "{name} should be disabled while the current SIFS location is unavailable"
            );
        }
        for name in ["Retry SIFS", "Connection settings", "Browse local files"] {
            assert!(
                snapshot.accessibility.nodes.iter().any(|node| {
                    node.role == sui::SemanticsRole::Button
                        && node.name.as_deref() == Some(name)
                        && !node.state.disabled
                }),
                "{name} should stay available for SIFS recovery"
            );
        }

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Connection settings")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        assert_eq!(
            shared.settings().selected,
            Some(SettingsSection::Connection)
        );

        Ok(())
    }

    #[test]
    fn unavailable_placeholder_browses_local_files() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let local_path = shared.files().local_path.clone();
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/services".to_string();
            files.sifs_path = "/services".to_string();
            files.error = Some("SIFS browsing requires a daemon connection".to_string());
            files.entries.clear();
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files unavailable local fallback")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Browse local files")
            .click()?;

        let files = shared.files();
        assert_eq!(files.source, FilesSourceKind::Local);
        assert_eq!(files.path, local_path);
        assert!(files.loaded);
        assert!(!files.loading);
        assert_eq!(files.sifs_path, "/services");

        Ok(())
    }

    #[test]
    fn unavailable_placeholder_retries_sifs() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/services".to_string();
            files.error = Some("device authorization was denied".to_string());
            files.entries = vec![SifsEntry {
                name: "old".to_string(),
                path: "/services/old".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(1),
            }];
            files.selected = Some(0);
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files retry")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Retry SIFS")
            .click()?;

        let files = shared.files();
        assert!(files.loaded);
        assert!(
            files.loading
                || files.error.as_deref() == Some("SIFS browsing requires a daemon connection")
        );
        assert_ne!(
            files.error.as_deref(),
            Some("device authorization was denied")
        );
        assert!(files.entries.is_empty());
        assert_eq!(files.selected, None);
        assert_eq!(files.path, "/services");

        Ok(())
    }

    #[test]
    fn header_exposes_refresh_sifs_action() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/".to_string();
            files.entries = vec![SifsEntry {
                name: "services".to_string(),
                path: "/services".to_string(),
                is_dir: true,
                is_link: false,
                size: None,
            }];
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files refresh")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::Button)
                .with_name("Back")
                .count()?,
            1
        );
        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::Button)
                .with_name("Forward")
                .count()?,
            1
        );
        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::Button)
                .with_name("Refresh SIFS")
                .count()?,
            1
        );
        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::Button)
                .with_name("Go to path")
                .count()?,
            1
        );

        Ok(())
    }

    #[test]
    fn header_back_forward_buttons_navigate_history() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/services".to_string();
            files.sifs_path = "/services".to_string();
            files.history_back.push(FilesLocation {
                source: FilesSourceKind::Sifs,
                path: "/".to_string(),
            });
            files.entries = vec![SifsEntry {
                name: "artifact.txt".to_string(),
                path: "/services/artifact.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(4),
            }];
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files history")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Back")
            .click()?;
        assert_eq!(shared.files().source, FilesSourceKind::Sifs);
        assert_eq!(shared.files().path, "/");
        assert!(shared.files().can_go_forward());

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Forward")
            .click()?;
        assert_eq!(shared.files().source, FilesSourceKind::Sifs);
        assert_eq!(shared.files().path, "/services");

        Ok(())
    }

    #[test]
    fn header_go_to_path_opens_current_path_prompt() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/services".to_string();
            files.entries.clear();
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files go to path")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Go to path")
            .click()?;

        let files = shared.files();
        let prompt = files.action_prompt.as_ref().expect("go to path prompt");
        assert_eq!(prompt.kind, FileActionPromptKind::GoTo);
        assert_eq!(prompt.input, "/services");
        assert_eq!(prompt.error, None);

        Ok(())
    }

    #[test]
    fn go_to_path_prompt_navigates_or_reports_validation_error() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.loaded = true;
        state.loading = false;
        state.path = "/".to_string();

        state.open_action_prompt(FileActionPromptKind::GoTo);
        state.set_action_prompt_input("services/../cluster-tests".to_string());
        state.submit_action_prompt(None);

        assert_eq!(state.path, "/cluster-tests");
        assert!(state.action_prompt.is_none());

        state.open_action_prompt(FileActionPromptKind::GoTo);
        state.set_action_prompt_input("   ".to_string());
        state.submit_action_prompt(None);

        assert_eq!(state.path, "/cluster-tests");
        let prompt = state
            .action_prompt
            .as_ref()
            .expect("validation keeps prompt");
        assert_eq!(prompt.kind, FileActionPromptKind::GoTo);
        assert_eq!(prompt.error.as_deref(), Some("path cannot be empty"));
    }

    #[test]
    fn go_to_path_dialog_copy_explains_supported_path_forms() {
        assert_eq!(
            go_to_path_description(FilesSourceKind::Sifs),
            "Open a SIFS path, e.g. /services, /users, or /sinomo/nodes/<node>/files"
        );
        assert_eq!(go_to_path_placeholder(FilesSourceKind::Sifs), "/services");
        assert!(go_to_path_description(FilesSourceKind::Local).contains("UNC"));
        assert!(go_to_path_description(FilesSourceKind::Local).contains("mounted path"));
        assert!(go_to_path_placeholder(FilesSourceKind::Local).contains("\\\\server\\share"));
    }

    #[test]
    fn action_dialog_signature_does_not_change_while_typing() {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/".to_string();
            files.open_action_prompt(FileActionPromptKind::GoTo);
        }

        let opened = files_action_dialog_signature(&shared);
        shared
            .files_mut()
            .set_action_prompt_input("/services".to_string());

        assert_eq!(
            files_action_dialog_signature(&shared),
            opened,
            "live text edits should not rebuild and replace the focused dialog input"
        );

        shared
            .files_mut()
            .set_action_prompt_input("   ".to_string());
        shared.files_mut().submit_action_prompt(None);

        assert_ne!(
            files_action_dialog_signature(&shared),
            opened,
            "validation errors still need to rebuild the dialog content"
        );
    }

    #[test]
    fn files_status_bar_exposes_current_path_state() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/cluster-tests".to_string();
            files.entries = vec![SifsEntry {
                name: "artifact.txt".to_string(),
                path: "/cluster-tests/artifact.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(42),
            }];
            files.selected = Some(0);
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files status")
                    .root(view(shared)),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        let snapshot = window.snapshot()?;
        let status = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == sui::SemanticsRole::Text
                    && node.name.as_deref() == Some("Files status")
            })
            .expect("files status semantics");
        assert_eq!(
            status.value,
            Some(sui::SemanticsValue::Text(
                "Local SIFS | Path /cluster-tests | 1 entry | selected /cluster-tests/artifact.txt | idle"
                    .to_string(),
            ))
        );

        Ok(())
    }

    #[test]
    fn files_list_header_exposes_column_context() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/services".to_string();
            files.entries = vec![
                SifsEntry {
                    name: "artifact.txt".to_string(),
                    path: "/services/artifact.txt".to_string(),
                    is_dir: false,
                    is_link: false,
                    size: Some(42),
                },
                SifsEntry {
                    name: "workspace".to_string(),
                    path: "/services/workspace".to_string(),
                    is_dir: true,
                    is_link: false,
                    size: None,
                },
            ];
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files list columns")
                    .root(view(shared)),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        let snapshot = window.snapshot()?;
        let header = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == sui::SemanticsRole::Text
                    && node.name.as_deref() == Some("Files list columns")
            })
            .expect("files list header semantics");
        assert_eq!(
            header.value,
            Some(sui::SemanticsValue::Text(
                "Name | Kind / size | Actions".to_string(),
            ))
        );
        assert_eq!(
            header.description.as_deref(),
            Some("SIFS list columns: Name, Kind / size, Actions. Path /services. 2 entries.")
        );

        Ok(())
    }

    #[test]
    fn filtered_file_rows_map_to_original_entries() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/services".to_string();
            files.entries = vec![
                SifsEntry {
                    name: "logs".to_string(),
                    path: "/services/logs".to_string(),
                    is_dir: true,
                    is_link: false,
                    size: None,
                },
                SifsEntry {
                    name: "artifact.txt".to_string(),
                    path: "/services/artifact.txt".to_string(),
                    is_dir: false,
                    is_link: false,
                    size: Some(42),
                },
                SifsEntry {
                    name: "notes.md".to_string(),
                    path: "/services/notes.md".to_string(),
                    is_dir: false,
                    is_link: false,
                    size: Some(5),
                },
            ];
            files.filter_query = "artifact".to_string();
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files filtered rows")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        let snapshot = window.snapshot()?;

        assert!(
            snapshot.accessibility.nodes.iter().any(|node| {
                node.role == sui::SemanticsRole::Text
                    && node.name.as_deref() == Some("Files status")
                    && node.value
                        == Some(sui::SemanticsValue::Text(
                            "Local SIFS | Path /services | 1/3 entries | no selection | idle"
                                .to_string(),
                        ))
            }),
            "status should show filtered and total entry counts"
        );
        assert!(
            !snapshot
                .accessibility
                .nodes
                .iter()
                .any(|node| node.name.as_deref() == Some("logs")),
            "non-matching entries should not be rendered"
        );

        window
            .get_by_role(sui::SemanticsRole::ListItem)
            .with_name("artifact.txt")
            .with_description("File, SIFS path /services/artifact.txt")
            .click()?;

        assert_eq!(shared.files().selected, Some(1));

        Ok(())
    }

    #[test]
    fn rendered_sorted_file_rows_select_original_entry() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/services".to_string();
            files.entries = vec![
                SifsEntry {
                    name: "small.txt".to_string(),
                    path: "/services/small.txt".to_string(),
                    is_dir: false,
                    is_link: false,
                    size: Some(5),
                },
                SifsEntry {
                    name: "large.bin".to_string(),
                    path: "/services/large.bin".to_string(),
                    is_dir: false,
                    is_link: false,
                    size: Some(4096),
                },
            ];
            files.sort_mode = FileSortMode::SizeDesc;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files sorted rows")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::ComboBox)
                .with_name("files-sort")
                .count()?,
            1
        );

        window
            .get_by_role(sui::SemanticsRole::ListItem)
            .with_name("large.bin")
            .with_description("File, SIFS path /services/large.bin")
            .click()?;

        assert_eq!(shared.files().selected, Some(1));

        Ok(())
    }

    #[test]
    fn empty_directory_placeholder_exposes_create_actions() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/cluster-tests".to_string();
            files.entries.clear();
            files.selected = None;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files empty directory")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Create file")
            .click()?;
        {
            let files = shared.files();
            let prompt = files.action_prompt.as_ref().expect("create file prompt");
            assert_eq!(prompt.kind, FileActionPromptKind::NewFile);
            assert_eq!(prompt.input, "untitled.txt");
            assert_eq!(prompt.target_path, None);
        }

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Cancel")
            .click()?;
        assert!(shared.files().action_prompt.is_none());

        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/cluster-tests".to_string();
            files.entries.clear();
            files.selected = None;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files empty directory folder")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Create folder")
            .click()?;
        {
            let files = shared.files();
            let prompt = files.action_prompt.as_ref().expect("create folder prompt");
            assert_eq!(prompt.kind, FileActionPromptKind::NewFolder);
            assert_eq!(prompt.input, "New folder");
            assert_eq!(prompt.target_path, None);
        }

        Ok(())
    }

    #[test]
    fn directory_inspect_button_selects_without_navigation() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/".to_string();
            files.node_id = "node-a".to_string();
            files.entries = vec![SifsEntry {
                name: "dir".to_string(),
                path: "/dir".to_string(),
                is_dir: true,
                is_link: false,
                size: None,
            }];
            files.selected = None;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files inspect directory")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Inspect dir")
            .click()?;

        let files = shared.files();
        assert_eq!(files.path, "/");
        assert_eq!(files.selected, Some(0));
        assert!(files.selected_entry().is_some_and(|entry| entry.is_dir));

        Ok(())
    }

    #[test]
    fn files_header_keeps_commands_usable_for_long_paths() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let long_path =
            "C:\\Program Files\\WindowsApps\\OpenAI.Codex_26.623.5546.0_x64__2p2nqsd0c76g0\\app";
        {
            let mut files = shared.files_mut();
            files.source = FilesSourceKind::Local;
            files.loaded = true;
            files.loading = false;
            files.path = long_path.to_string();
            files.entries = vec![SifsEntry {
                name: "chrome.dll".to_string(),
                path: Path::new(long_path)
                    .join("chrome.dll")
                    .to_string_lossy()
                    .into_owned(),
                is_dir: false,
                is_link: false,
                size: Some(294_300_000),
            }];
            files.selected = Some(0);
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new().title("Files long header").root(
                    SizedBox::new()
                        .width(1280.0)
                        .height(720.0)
                        .with_child(view(shared.clone())),
                ),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        let snapshot = window.snapshot()?;

        for label in ["Rename", "Delete"] {
            let node = snapshot
                .accessibility
                .nodes
                .iter()
                .find(|node| {
                    node.role == sui::SemanticsRole::Button && node.name.as_deref() == Some(label)
                })
                .unwrap_or_else(|| panic!("{label} command should be present"));
            assert!(
                node.bounds.width() >= 28.0 && node.bounds.height() >= 28.0,
                "{label} command should keep a usable hit target, got {:?}",
                node.bounds
            );
            assert!(
                node.bounds.max_x() <= 1280.0 && node.bounds.max_y() <= HEADER_H,
                "{label} command should stay inside the header, got {:?}",
                node.bounds
            );
        }

        Ok(())
    }

    #[test]
    fn row_action_buttons_open_copy_transfer_move_rename_and_delete_prompts() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/".to_string();
            files.entries = vec![SifsEntry {
                name: "artifact.txt".to_string(),
                path: "/artifact.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(5),
            }];
            files.selected = None;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new().title("Files row actions").root(
                    SizedBox::new()
                        .width(520.0)
                        .height(720.0)
                        .with_child(view(shared.clone())),
                ),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Copy artifact.txt")
            .click()?;
        {
            let files = shared.files();
            let prompt = files.action_prompt.as_ref().expect("copy prompt");
            assert_eq!(prompt.kind, FileActionPromptKind::Copy);
            assert_eq!(prompt.target_path.as_deref(), Some("/artifact.txt"));
            assert_eq!(prompt.input, "artifact copy.txt");
            assert_eq!(files.selected, Some(0));
        }

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Cancel")
            .click()?;
        assert!(shared.files().action_prompt.is_none());

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Download artifact.txt")
            .click()?;
        {
            let files = shared.files();
            let prompt = files.action_prompt.as_ref().expect("download prompt");
            assert_eq!(prompt.kind, FileActionPromptKind::CopyToLocal);
            assert_eq!(prompt.target_path.as_deref(), Some("/artifact.txt"));
            assert!(prompt.input.ends_with("artifact.txt"));
            assert_eq!(files.selected, Some(0));
        }

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Cancel")
            .click()?;
        assert!(shared.files().action_prompt.is_none());

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Move artifact.txt")
            .click()?;
        {
            let files = shared.files();
            let prompt = files.action_prompt.as_ref().expect("move prompt");
            assert_eq!(prompt.kind, FileActionPromptKind::Move);
            assert_eq!(prompt.target_path.as_deref(), Some("/artifact.txt"));
            assert_eq!(prompt.input, "artifact.txt");
            assert_eq!(files.selected, Some(0));
        }

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Cancel")
            .click()?;
        assert!(shared.files().action_prompt.is_none());

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Rename artifact.txt")
            .click()?;
        {
            let files = shared.files();
            let prompt = files.action_prompt.as_ref().expect("rename prompt");
            assert_eq!(prompt.kind, FileActionPromptKind::Rename);
            assert_eq!(prompt.target_path.as_deref(), Some("/artifact.txt"));
            assert_eq!(prompt.input, "artifact.txt");
            assert_eq!(files.selected, Some(0));
        }

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Cancel")
            .click()?;
        assert!(shared.files().action_prompt.is_none());

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Delete artifact.txt")
            .click()?;
        {
            let files = shared.files();
            let prompt = files.action_prompt.as_ref().expect("delete prompt");
            assert_eq!(prompt.kind, FileActionPromptKind::Delete);
            assert_eq!(prompt.target_path.as_deref(), Some("/artifact.txt"));
            assert_eq!(prompt.target_name, "artifact.txt");
            assert_eq!(files.selected, Some(0));
        }

        Ok(())
    }

    #[test]
    fn batch_mark_controls_open_delete_selected_prompt() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/services".to_string();
            files.entries = vec![
                SifsEntry {
                    name: "artifact.txt".to_string(),
                    path: "/services/artifact.txt".to_string(),
                    is_dir: false,
                    is_link: false,
                    size: Some(5),
                },
                SifsEntry {
                    name: "logs".to_string(),
                    path: "/services/logs".to_string(),
                    is_dir: true,
                    is_link: false,
                    size: None,
                },
            ];
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new().title("Files batch actions").root(
                    SizedBox::new()
                        .width(620.0)
                        .height(720.0)
                        .with_child(view(shared.clone())),
                ),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Mark artifact.txt")
            .click()?;
        assert_eq!(shared.files().marked_count(), 1);
        assert!(shared.files().entry_marked(0));

        let snapshot = window.snapshot()?;
        assert!(snapshot.accessibility.nodes.iter().any(|node| {
            node.role == sui::SemanticsRole::Text
                && node.name.as_deref() == Some("Files marked entries")
                && node.value == Some(sui::SemanticsValue::Text("1 marked entry".to_string()))
        }));

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Copy")
            .click()?;
        {
            let files = shared.files();
            let prompt = files.action_prompt.as_ref().expect("copy selected prompt");
            assert_eq!(prompt.kind, FileActionPromptKind::CopySelected);
            assert_eq!(prompt.targets.len(), 1);
            assert_eq!(prompt.targets[0].path, "/services/artifact.txt");
            assert_eq!(prompt.input, "/services");
        }
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Cancel")
            .click()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Download")
            .click()?;
        {
            let files = shared.files();
            let prompt = files
                .action_prompt
                .as_ref()
                .expect("transfer selected prompt");
            assert_eq!(prompt.kind, FileActionPromptKind::TransferSelected);
            assert_eq!(prompt.targets.len(), 1);
            assert_eq!(prompt.targets[0].path, "/services/artifact.txt");
            assert_eq!(prompt.input, files.local_path);
        }
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Cancel")
            .click()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Move")
            .click()?;
        {
            let files = shared.files();
            let prompt = files.action_prompt.as_ref().expect("move selected prompt");
            assert_eq!(prompt.kind, FileActionPromptKind::MoveSelected);
            assert_eq!(prompt.targets.len(), 1);
            assert_eq!(prompt.targets[0].path, "/services/artifact.txt");
            assert_eq!(prompt.input, "/services");
        }
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Cancel")
            .click()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Delete")
            .click()?;
        {
            let files = shared.files();
            let prompt = files
                .action_prompt
                .as_ref()
                .expect("delete selected prompt");
            assert_eq!(prompt.kind, FileActionPromptKind::DeleteSelected);
            assert_eq!(prompt.targets.len(), 1);
            assert_eq!(prompt.targets[0].path, "/services/artifact.txt");
        }

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Cancel")
            .click()?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Mark visible entries")
            .click()?;
        assert_eq!(shared.files().marked_count(), 2);

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Clear marked entries")
            .click()?;
        assert_eq!(shared.files().marked_count(), 0);

        Ok(())
    }

    #[test]
    fn local_row_action_exposes_copy_to_sifs_prompt() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let root = files_test_temp_dir("row-copy-to-sifs");
        let artifact = root.join("artifact.txt");
        fs::write(&artifact, b"hello").expect("write local artifact");
        let root_label = display_local_path(root.to_string_lossy().into_owned());
        let artifact_label = display_local_path(artifact.to_string_lossy().into_owned());
        {
            let mut files = shared.files_mut();
            files.source = FilesSourceKind::Local;
            files.loaded = true;
            files.loading = false;
            files.path = root_label.clone();
            files.local_path = root_label;
            files.sifs_path = "/services".to_string();
            files.entries = vec![SifsEntry {
                name: "artifact.txt".to_string(),
                path: artifact_label.clone(),
                is_dir: false,
                is_link: false,
                size: Some(5),
            }];
            files.selected = None;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files local row transfer")
                    .root(
                        SizedBox::new()
                            .width(520.0)
                            .height(720.0)
                            .with_child(view(shared.clone())),
                    ),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Copy to SIFS artifact.txt")
            .click()?;

        {
            let files = shared.files();
            let prompt = files.action_prompt.as_ref().expect("copy to SIFS prompt");
            assert_eq!(prompt.kind, FileActionPromptKind::CopyToSifs);
            assert_eq!(prompt.target_path.as_deref(), Some(artifact_label.as_str()));
            assert_eq!(prompt.input, "/services/artifact.txt");
            assert_eq!(files.selected, Some(0));
        }

        let _ = fs::remove_dir_all(root);
        Ok(())
    }

    #[test]
    fn task_strip_cancel_marks_running_task() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/".to_string();
            files.entries = vec![SifsEntry {
                name: "artifact.txt".to_string(),
                path: "/artifact.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(5),
            }];
            files.tasks = vec![FilesTask {
                id: 77,
                source: FilesSourceKind::Sifs,
                title: "Delete".to_string(),
                detail: "/artifact.txt".to_string(),
                state: FilesTaskState::Running,
                progress: Some(0.35),
            }];
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new().title("Files task strip").root(
                    SizedBox::new()
                        .width(520.0)
                        .height(720.0)
                        .with_child(view(shared.clone())),
                ),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Cancel Delete")
            .click()?;

        assert_eq!(
            shared.files().tasks[0].state,
            FilesTaskState::CancelRequested
        );
        assert!(operation_cancelled(
            &shared.files().worker_cancellations,
            77
        ));

        Ok(())
    }

    #[test]
    fn task_strip_stays_visible_for_empty_directories() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/empty".to_string();
            files.entries.clear();
            files.tasks = vec![FilesTask {
                id: 78,
                source: FilesSourceKind::Sifs,
                title: "Copy".to_string(),
                detail: "/services/artifact.bin -> /empty/artifact.bin".to_string(),
                state: FilesTaskState::Running,
                progress: Some(0.35),
            }];
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files empty task strip")
                    .root(
                        SizedBox::new()
                            .width(520.0)
                            .height(720.0)
                            .with_child(view(shared.clone())),
                    ),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Cancel Copy")
            .click()?;

        assert_eq!(
            shared.files().tasks[0].state,
            FilesTaskState::CancelRequested
        );
        assert!(operation_cancelled(
            &shared.files().worker_cancellations,
            78
        ));

        Ok(())
    }

    #[test]
    fn failed_task_recovery_stays_visible_when_listing_unavailable() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/services".to_string();
            files.entries.clear();
            files.error = Some("device authorization was denied".to_string());
            files.tasks = vec![FilesTask {
                id: 91,
                source: FilesSourceKind::Sifs,
                title: "Refresh placement".to_string(),
                detail: "device authorization was denied".to_string(),
                state: FilesTaskState::Failed,
                progress: Some(1.0),
            }];
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files unavailable task strip")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Open Connection settings for Refresh placement")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        assert_eq!(
            shared.settings().selected,
            Some(SettingsSection::Connection)
        );

        Ok(())
    }

    #[test]
    fn task_strip_clear_finished_dismisses_terminal_history() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/".to_string();
            files.entries = vec![SifsEntry {
                name: "artifact.txt".to_string(),
                path: "/artifact.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(5),
            }];
            files.tasks = vec![
                FilesTask {
                    id: 77,
                    source: FilesSourceKind::Sifs,
                    title: "Delete".to_string(),
                    detail: "deleted /artifact.txt".to_string(),
                    state: FilesTaskState::Completed,
                    progress: Some(1.0),
                },
                FilesTask {
                    id: 78,
                    source: FilesSourceKind::Sifs,
                    title: "Copy".to_string(),
                    detail: "/artifact.txt -> /copy.txt".to_string(),
                    state: FilesTaskState::Running,
                    progress: Some(0.35),
                },
            ];
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new().title("Files task clear").root(
                    SizedBox::new()
                        .width(520.0)
                        .height(720.0)
                        .with_child(view(shared.clone())),
                ),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Clear finished tasks")
            .click()?;

        let files = shared.files();
        assert_eq!(files.tasks.len(), 1);
        assert_eq!(files.tasks[0].id, 78);
        assert_eq!(files.tasks[0].state, FilesTaskState::Running);

        Ok(())
    }

    #[test]
    fn inspector_task_queue_is_visible_when_idle() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/".to_string();
            files.entries = vec![SifsEntry {
                name: "artifact.txt".to_string(),
                path: "/artifact.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(5),
            }];
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files idle task queue")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::Text)
                .with_name("File task queue")
                .with_description("idle")
                .count()?,
            1
        );

        Ok(())
    }

    #[test]
    fn inspector_task_queue_clear_finished_dismisses_terminal_history() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/".to_string();
            files.entries = vec![SifsEntry {
                name: "artifact.txt".to_string(),
                path: "/artifact.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(5),
            }];
            files.tasks = vec![
                FilesTask {
                    id: 77,
                    source: FilesSourceKind::Sifs,
                    title: "Delete".to_string(),
                    detail: "deleted /artifact.txt".to_string(),
                    state: FilesTaskState::Completed,
                    progress: Some(1.0),
                },
                FilesTask {
                    id: 78,
                    source: FilesSourceKind::Sifs,
                    title: "Copy".to_string(),
                    detail: "/artifact.txt -> /copy.txt".to_string(),
                    state: FilesTaskState::Running,
                    progress: Some(0.35),
                },
            ];
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files inspector task clear")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Clear finished file tasks")
            .click()?;

        let files = shared.files();
        assert_eq!(files.tasks.len(), 1);
        assert_eq!(files.tasks[0].id, 78);
        assert_eq!(files.tasks[0].state, FilesTaskState::Running);

        Ok(())
    }

    #[test]
    fn task_strip_failed_sifs_task_opens_connection_settings() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/".to_string();
            files.entries = vec![SifsEntry {
                name: "artifact.txt".to_string(),
                path: "/artifact.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(5),
            }];
            files.tasks = vec![FilesTask {
                id: 91,
                source: FilesSourceKind::Sifs,
                title: "Refresh placement".to_string(),
                detail: "device authorization was denied".to_string(),
                state: FilesTaskState::Failed,
                progress: Some(1.0),
            }];
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new().title("Files task recovery").root(
                    SizedBox::new()
                        .width(520.0)
                        .height(720.0)
                        .with_child(view(shared.clone())),
                ),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Open Connection settings for Refresh placement")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        assert_eq!(
            shared.settings().selected,
            Some(SettingsSection::Connection)
        );

        Ok(())
    }

    #[test]
    fn selected_directory_inspector_exposes_open_folder_action() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/".to_string();
            files.node_id = "node-a".to_string();
            files.entries = vec![SifsEntry {
                name: "dir".to_string(),
                path: "/dir".to_string(),
                is_dir: true,
                is_link: false,
                size: None,
            }];
            files.selected = None;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files inspector open folder")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Inspect dir")
            .click()?;
        assert_eq!(shared.files().path, "/");
        assert!(
            window
                .get_by_role(sui::SemanticsRole::Button)
                .with_name("Copy dir")
                .count()?
                >= 1,
            "selected directories should expose recursive copy"
        );
        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::Button)
                .with_name("Pin content dir")
                .count()?,
            0,
            "directories do not have file content pins"
        );
        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::Button)
                .with_name("Unpin content dir")
                .count()?,
            0,
            "directories do not have file content pins"
        );

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Open folder")
            .click()?;

        let files = shared.files();
        assert_eq!(files.path, "/dir");
        assert_eq!(files.selected, None);

        Ok(())
    }

    #[test]
    fn selected_file_inspector_exposes_management_actions() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let (tx, rx) = mpsc::channel();
        let (_result_tx, result_rx) = mpsc::channel();
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/services".to_string();
            files.node_id = "node-a".to_string();
            files.entries = vec![SifsEntry {
                name: "artifact.txt".to_string(),
                path: "/services/artifact.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(5),
            }];
            files.selected = Some(0);
            files.worker_tx = Some(tx);
            files.worker_rx = Some(result_rx);
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files inspector selected file actions")
                    .root(
                        SizedBox::new()
                            .width(INSPECTOR_W)
                            .height(720.0)
                            .with_child(InspectorView::new(shared.clone())),
                    ),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Copy artifact.txt")
            .click()?;
        {
            let files = shared.files();
            let prompt = files.action_prompt.as_ref().expect("copy prompt");
            assert_eq!(prompt.kind, FileActionPromptKind::Copy);
            assert_eq!(
                prompt.target_path.as_deref(),
                Some("/services/artifact.txt")
            );
        }
        shared.files_mut().close_action_prompt();

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Download artifact.txt")
            .click()?;
        {
            let files = shared.files();
            let prompt = files.action_prompt.as_ref().expect("download prompt");
            assert_eq!(prompt.kind, FileActionPromptKind::CopyToLocal);
            assert_eq!(
                prompt.target_path.as_deref(),
                Some("/services/artifact.txt")
            );
            assert!(prompt.input.ends_with("artifact.txt"));
        }
        shared.files_mut().close_action_prompt();

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Refresh placement artifact.txt")
            .click()?;

        assert_eq!(shared.files().tasks[0].title, "Refresh placement");
        match rx.try_recv().expect("placement refresh command") {
            FilesWorkerCommand::Operation {
                request_id,
                source,
                operation,
            } => {
                assert_eq!(request_id, shared.files().tasks[0].id);
                assert_eq!(source, FilesSourceKind::Sifs);
                assert_eq!(operation.kind, FilesOperationKind::RefreshPlacement);
                assert_eq!(operation.path, "/services/artifact.txt");
            }
            FilesWorkerCommand::List { .. } => panic!("expected operation command"),
            FilesWorkerCommand::Details { .. } => panic!("expected operation command"),
            FilesWorkerCommand::Preview { .. } => panic!("expected operation command"),
        }

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Repair replicas artifact.txt")
            .click()?;

        assert_eq!(shared.files().tasks[0].title, "Repair replicas");
        match rx.try_recv().expect("replica repair command") {
            FilesWorkerCommand::Operation {
                request_id,
                source,
                operation,
            } => {
                assert_eq!(request_id, shared.files().tasks[0].id);
                assert_eq!(source, FilesSourceKind::Sifs);
                assert_eq!(operation.kind, FilesOperationKind::RepairReplicas);
                assert_eq!(operation.path, "/services/artifact.txt");
            }
            FilesWorkerCommand::List { .. } => panic!("expected operation command"),
            FilesWorkerCommand::Details { .. } => panic!("expected operation command"),
            FilesWorkerCommand::Preview { .. } => panic!("expected operation command"),
        }

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Pin content artifact.txt")
            .click()?;

        assert_eq!(shared.files().tasks[0].title, "Pin content");
        match rx.try_recv().expect("content pin command") {
            FilesWorkerCommand::Operation {
                request_id,
                source,
                operation,
            } => {
                assert_eq!(request_id, shared.files().tasks[0].id);
                assert_eq!(source, FilesSourceKind::Sifs);
                assert_eq!(operation.kind, FilesOperationKind::PinContent);
                assert_eq!(operation.path, "/services/artifact.txt");
            }
            FilesWorkerCommand::List { .. } => panic!("expected operation command"),
            FilesWorkerCommand::Details { .. } => panic!("expected operation command"),
            FilesWorkerCommand::Preview { .. } => panic!("expected operation command"),
        }

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Unpin content artifact.txt")
            .click()?;

        assert_eq!(shared.files().tasks[0].title, "Unpin content");
        match rx.try_recv().expect("content unpin command") {
            FilesWorkerCommand::Operation {
                request_id,
                source,
                operation,
            } => {
                assert_eq!(request_id, shared.files().tasks[0].id);
                assert_eq!(source, FilesSourceKind::Sifs);
                assert_eq!(operation.kind, FilesOperationKind::UnpinContent);
                assert_eq!(operation.path, "/services/artifact.txt");
            }
            FilesWorkerCommand::List { .. } => panic!("expected operation command"),
            FilesWorkerCommand::Details { .. } => panic!("expected operation command"),
            FilesWorkerCommand::Preview { .. } => panic!("expected operation command"),
        }

        Ok(())
    }

    #[test]
    fn selected_sifs_metadata_error_offers_connection_recovery() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/services".to_string();
            files.entries = vec![SifsEntry {
                name: "artifact.txt".to_string(),
                path: "/services/artifact.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(5),
            }];
            files.selected = Some(0);
            files.details_error = Some("device authorization was denied".to_string());
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files inspector SIFS metadata recovery")
                    .root(
                        SizedBox::new()
                            .width(INSPECTOR_W)
                            .height(720.0)
                            .with_child(InspectorView::new(shared.clone())),
                    ),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Connection settings")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        assert_eq!(
            shared.settings().selected,
            Some(SettingsSection::Connection)
        );
        Ok(())
    }

    #[test]
    fn selected_local_file_inspector_exposes_copy_to_sifs() -> sui::Result<()> {
        let root = files_test_temp_dir("local-inspector-copy-to-sifs");
        let path = root.join("artifact.txt");
        fs::write(&path, b"hello").expect("write local file");

        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.source = FilesSourceKind::Local;
            files.loaded = true;
            files.loading = false;
            files.path = root.to_string_lossy().into_owned();
            files.local_path = files.path.clone();
            files.sifs_path = "/services".to_string();
            files.node_id = local_node_label();
            files.entries = vec![SifsEntry {
                name: "artifact.txt".to_string(),
                path: path.to_string_lossy().into_owned(),
                is_dir: false,
                is_link: false,
                size: Some(5),
            }];
            files.selected = Some(0);
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files inspector local copy to SIFS")
                    .root(
                        SizedBox::new()
                            .width(INSPECTOR_W)
                            .height(720.0)
                            .with_child(InspectorView::new(shared.clone())),
                    ),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Copy to SIFS artifact.txt")
            .click()?;

        let files = shared.files();
        let prompt = files.action_prompt.as_ref().expect("copy to SIFS prompt");
        assert_eq!(prompt.kind, FileActionPromptKind::CopyToSifs);
        assert_eq!(
            prompt.target_path.as_deref(),
            Some(path.to_string_lossy().as_ref())
        );
        assert_eq!(prompt.input, "/services/artifact.txt");

        let _ = fs::remove_dir_all(root);
        Ok(())
    }

    #[test]
    fn inspector_details_button_toggles_sifs_diagnostics() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.error = Some("device authorization was denied".to_string());
            files.entries.clear();
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files inspector details")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Show metadata")
            .click()?;
        assert!(shared.files().inspector_details_open);

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Hide metadata")
            .click()?;
        assert!(!shared.files().inspector_details_open);

        Ok(())
    }

    #[test]
    fn selected_file_details_button_toggles_sifs_diagnostics() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/".to_string();
            files.node_id = "node-a".to_string();
            files.entries = vec![SifsEntry {
                name: "artifact.txt".to_string(),
                path: "/artifact.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(5),
            }];
            files.selected = Some(0);
            files.details_error = Some("device authorization was denied".to_string());
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files selected inspector details")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Show metadata")
            .click()?;
        assert!(shared.files().inspector_details_open);

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Hide metadata")
            .click()?;
        assert!(!shared.files().inspector_details_open);

        Ok(())
    }

    #[test]
    fn expanded_inspector_details_publish_scroll_range() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/services".to_string();
            files.node_id = "node-a".to_string();
            files.clustered = true;
            files.entries = vec![SifsEntry {
                name: "sinomo-app-chat-session-01ktsg9dv850xkpryhkn0kfr8t.json".to_string(),
                path: "/services/sinomo-app-chat-session-01ktsg9dv850xkpryhkn0kfr8t.json"
                    .to_string(),
                is_dir: false,
                is_link: false,
                size: Some(4096),
            }];
            files.selected = Some(0);
            files.inspector_details_open = true;
            files.selected_details = Some(SifsObjectDetails {
                file_system_id: "sifs-primary".to_string(),
                object_id: "object-01ktsg9dv850xkpryhkn0kfr8t".to_string(),
                path: "/services/sinomo-app-chat-session-01ktsg9dv850xkpryhkn0kfr8t.json"
                    .to_string(),
                kind: "file".to_string(),
                version: "version-01ktsga2xj2yz8dh05ncdvph1q".to_string(),
                content_hash: Some(
                    "sha256:4d96739c4ce08aefbe9f0ea6a97d8a7a71510f008bd023f3b93bbf13e3f70d26"
                        .to_string(),
                ),
                size_bytes: Some(4096),
                link_count: 1,
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                accessed_at_unix_nanos: 0,
                modified_at_unix_nanos: 0,
                created_at_unix_nanos: 0,
                changed_at_unix_nanos: 0,
                policy: "replicated-eventual".to_string(),
                current_locations: vec!["node-a".to_string(), "node-b".to_string()],
                local_pins: Vec::new(),
                health: None,
            });
        }
        let scroll_state = ScrollState::new();

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files scrollable inspector")
                    .root(
                        SizedBox::new()
                            .width(INSPECTOR_W)
                            .height(280.0)
                            .with_child(inspector(&shared, &scroll_state)),
                    ),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        let _snapshot = window.snapshot()?;
        assert!(
            scroll_state.content_size().height > scroll_state.viewport_size().height,
            "expanded inspector details should exceed the short viewport"
        );
        assert!(
            scroll_state.max_offset().y > 0.0,
            "expanded inspector details should be vertically scrollable"
        );

        Ok(())
    }
}
