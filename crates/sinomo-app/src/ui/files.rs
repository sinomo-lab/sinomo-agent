//! "Files" tab — source-aware file manager for SIFS and local files.
//!
//! Standard file-manager UX (breadcrumb + listing) plus a SIFS inspector that
//! shows where an object lives and its placement / replication policy.
//!
//! Like the Settings tab, this is a fully *composed* `sui-widgets` tree: [`view`]
//! returns a responsive host ([`FilesView`]) over pre-built pods (wide split with
//! the inspector, narrow list-only), each assembled from stock `Breadcrumb` /
//! `IconButton` / `VirtualTable` / `ScrollView` / `SideSheet` / `PanelSection` /
//! `DetailRow` / `StatusBadge` widgets. State lives entirely in [`FilesState`]; the
//! interactive app lists SIFS on a background worker so tab layout never blocks
//! on DFS metadata or network work.
//!
//! The file listing changes on navigation and sort. It is rebuilt by
//! [`RebuildOnChange`] and rendered with [`VirtualTable`], so large folders only
//! paint visible rows while the header stays pinned. Sorting and pagination are
//! requested through the worker/daemon boundary instead of doing heavy listing
//! work on the UI path.

use std::cell::Cell;
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
    Alignment, ArrangeCtx, Background, Breadcrumb, BreadcrumbItem, ButtonAppearance,
    CheckboxIndicatorState, Color, Constraints, ControlSize, DefaultTheme, DetailRow, Dock,
    EmptyState, Event, EventCtx, EventPhase, FixedPaneSplit, Flex, FlexItem, FlexWrap, FontWeight,
    IconButton, IconGlyph, Insets, KeyState, Label, LeadingLabelCellPaint, ListItem, ListView,
    MeasureCtx, PaintCtx, PanelSection, PlacementBadge, PlacementBadgePaint, PointerEventKind,
    RebuildOnChange, Rect, RichText, ScrollState, ScrollView, SectionLabel, SegmentedControl,
    SegmentedControlItem, SemanticRegion, SemanticTone, SemanticsCtx, SemanticsRole, Separator,
    SideSheet, SideSheetPlacement, Size, SizedBox, Stack, StackHostOptions, StatusBadge, StatusBar,
    StatusBarSegment, Surface, SurfaceBorder, SurfaceRole, SwitchView, TextCellPaint, TextSurface,
    TextWrap, Tooltip, TooltipPlacement, TrailingSlotRow, VirtualTable, VirtualTableColumn,
    VirtualTableRowActivationKind, VirtualTableRowContext, VirtualTableSortDirection, Widget,
    WidgetPod, WidgetPodMutVisitor, WidgetPodVisitor, WindowEvent,
    containers::Padding as WidgetPadding, paint_checkbox_indicator, paint_leading_label_cell,
    paint_placement_badge_with, paint_text_cell,
};

use sinomo_cli::app_api::{
    SifsBrowser, SifsEntry, SifsEntryPage, SifsFileRead, SifsHealthSummary, SifsListOptions,
    SifsListSortDirection, SifsListSortKey, SifsMetadataShardDetails, SifsObjectDetails,
    SifsObjectHealth, SifsReplaceFileIfVersionOutcome,
};
use sinomo_runtime::local::SinomoDataDir;
use time::format_description::well_known::Rfc3339;

use crate::backend::Backend;
use crate::native_picker::{NativePickerKind, NativePickerLane, NativePickerOutcome};
use crate::state::{Shared, Tab};
use crate::theme::Palette;
use crate::ui::settings::SettingsSection;
use crate::ui::widgets;

/// Below this content width the tab collapses to a list-only column (the inspector is hidden).
const WIDE_THRESHOLD: f32 = 640.0;
const INSPECTOR_W: f32 = 340.0;
const ROW_THUMBNAIL_SIDE: f32 = 20.0;
const CONTENT_PAD: f32 = 16.0;
const HEADER_H: f32 = 44.0;
const HEADER_COMMAND_GAP: f32 = 4.0;
const HEADER_PAD_Y: f32 = 8.0;
/// Minimum inner width for the complete desktop command row. Below this, the wide Files layout
/// keeps its inspector but uses a picker-first row with secondary commands in an overflow sheet.
const FULL_HEADER_INNER_MIN_W: f32 = 860.0;
const FILES_STATUS_H: f32 = 34.0;
const LIST_TASK_STRIP_H: f32 = 88.0;
const FILES_LIST_PAGE_SIZE: usize = 512;
const PLACES_W: f32 = 190.0;
const PLACE_ROW_H: f32 = 38.0;
const FILE_PREVIEW_MAX_BYTES: u64 = 32 * 1024;
const FILE_PREVIEW_MAX_LINES: usize = 8;
/// The text surface keeps the entire accepted document resident. Reject larger files explicitly
/// instead of silently presenting a partial document that could be saved over the original.
const FILE_DOCUMENT_MAX_BYTES: u64 = 2 * 1024 * 1024;
/// Chat attachments may be binary, but still need a bounded read on the Files worker lane.
const FILE_ATTACHMENT_MAX_BYTES: u64 = 32 * 1024 * 1024;
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

#[allow(dead_code)]
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

#[allow(dead_code)]
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
    fn sifs_options(self, offset: usize, limit: Option<usize>) -> SifsListOptions {
        let (sort_key, sort_direction) = match self {
            Self::NameAsc => (SifsListSortKey::Name, SifsListSortDirection::Ascending),
            Self::NameDesc => (SifsListSortKey::Name, SifsListSortDirection::Descending),
            Self::Kind => (SifsListSortKey::Kind, SifsListSortDirection::Ascending),
            Self::SizeDesc => (SifsListSortKey::Size, SifsListSortDirection::Descending),
            Self::SizeAsc => (SifsListSortKey::Size, SifsListSortDirection::Ascending),
        };
        SifsListOptions {
            sort_key,
            sort_direction,
            offset,
            limit,
        }
    }

    fn next_for_column(self, column: FilesSortColumn) -> Self {
        match column {
            FilesSortColumn::Name => match self {
                Self::NameAsc => Self::NameDesc,
                _ => Self::NameAsc,
            },
            FilesSortColumn::KindSize => match self {
                Self::SizeDesc => Self::SizeAsc,
                _ => Self::SizeDesc,
            },
        }
    }

    fn name_sort_direction(self) -> Option<VirtualTableSortDirection> {
        match self {
            Self::NameAsc => Some(VirtualTableSortDirection::Ascending),
            Self::NameDesc => Some(VirtualTableSortDirection::Descending),
            _ => None,
        }
    }

    fn meta_sort_direction(self) -> Option<VirtualTableSortDirection> {
        match self {
            Self::Kind => Some(VirtualTableSortDirection::Ascending),
            Self::SizeAsc => Some(VirtualTableSortDirection::Ascending),
            Self::SizeDesc => Some(VirtualTableSortDirection::Descending),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FilesSortColumn {
    Name,
    KindSize,
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
    MakeMetadataPrimary,
    PinMetadata,
    UnpinMetadata,
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
struct LocalFileFingerprint {
    canonical_path: PathBuf,
    len: u64,
    modified_at_unix_nanos: Option<u64>,
    content_digest: String,
    readonly: bool,
    symlink: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SifsFileFingerprint {
    object_id: String,
    version: String,
    content_hash: Option<String>,
    size_bytes: Option<u64>,
    policy: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FilesDocumentLoad {
    source: FilesSourceKind,
    path: String,
    title: String,
    text: String,
    local_fingerprint: Option<LocalFileFingerprint>,
    sifs_fingerprint: Option<SifsFileFingerprint>,
    read_only_reason: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum FilesDocumentSave {
    Local {
        fingerprint: LocalFileFingerprint,
        maintenance_warning: Option<String>,
        requires_reload: bool,
    },
    Sifs {
        fingerprint: SifsFileFingerprint,
        maintenance_warning: Option<String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FilesDocument {
    source: FilesSourceKind,
    path: String,
    title: String,
    text: String,
    saved_text: String,
    local_fingerprint: Option<LocalFileFingerprint>,
    sifs_fingerprint: Option<SifsFileFingerprint>,
    read_only_reason: Option<String>,
    loading: bool,
    load_error: Option<String>,
    saving: bool,
    save_error: Option<String>,
    save_warning: Option<String>,
}

impl FilesDocument {
    fn loading(source: FilesSourceKind, path: String, title: String) -> Self {
        Self {
            source,
            path,
            title,
            text: String::new(),
            saved_text: String::new(),
            local_fingerprint: None,
            sifs_fingerprint: None,
            read_only_reason: None,
            loading: true,
            load_error: None,
            saving: false,
            save_error: None,
            save_warning: None,
        }
    }

    fn dirty(&self) -> bool {
        !self.loading && self.load_error.is_none() && self.text != self.saved_text
    }

    fn editable(&self) -> bool {
        !self.loading && self.load_error.is_none() && self.read_only_reason.is_none()
    }
}

/// Immutable snapshot handed to Chat after the Files worker has read and verified it. Chat owns
/// what happens next; Files never reaches into chat state from a file-manager callback.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FilesAttachmentIntent {
    pub file_name: String,
    pub bytes: Vec<u8>,
    pub provenance: FilesAttachmentProvenance,
    /// True when the bytes come from the open editor rather than a persisted file version.
    pub is_unsaved_editor_buffer: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FilesAttachmentProvenance {
    Local {
        path: String,
        canonical_path: String,
        modified_at_unix_nanos: Option<u64>,
    },
    Sifs {
        path: String,
        object_id: String,
        version: String,
        content_hash: Option<String>,
        policy: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FilesPickerAction {
    OpenFile,
    OpenFolder,
    ImportFiles,
    ImportFolder,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum FilesOperationFailure {
    Cancelled(String),
    Error(String),
}

type FilesOperationResult = Result<FilesOperationOutcome, FilesOperationFailure>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CompactFilesOverlay {
    Commands,
    Inspector,
}

// ---------------------------------------------------------------------------
// State (data layer) — SIFS-backed browser snapshot + async load worker.
// ---------------------------------------------------------------------------

/// State backing the files tab.
pub struct FilesState {
    pub data_dir: Arc<SinomoDataDir>,
    daemon_address: Option<String>,
    daemon_unavailable_detail: Option<String>,
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
    pub entries_total_count: usize,
    pub selected: Option<usize>,
    pub selected_details: Option<SifsObjectDetails>,
    selected_local_details: Option<LocalObjectDetails>,
    list_slow_after_secs: Option<u64>,
    pub details_loading: bool,
    pub details_error: Option<String>,
    sifs_health: Option<SifsHealthSummary>,
    sifs_health_error: Option<String>,
    selected_preview: Option<FilesPreview>,
    preview_loading: bool,
    preview_error: Option<String>,
    document: Option<FilesDocument>,
    document_revision: u64,
    document_close_confirmation: bool,
    pending_document_open: Option<(FilesSourceKind, String, String)>,
    attachment_loading: bool,
    attachment_error: Option<String>,
    ready_attachment_intent: Option<FilesAttachmentIntent>,
    picker_lane: NativePickerLane,
    picker_action: Option<FilesPickerAction>,
    picker_context_generation: u64,
    picker_error: Option<String>,
    external_file_hovered: bool,
    action_prompt: Option<FileActionPrompt>,
    marked_entries: BTreeSet<usize>,
    sort_mode: FileSortMode,
    tasks: Vec<FilesTask>,
    pending_select_path: Option<String>,
    pub scroll: f32,
    pub inspector_details_open: bool,
    compact_overlay: Option<CompactFilesOverlay>,
    worker_tx: Option<Sender<FilesWorkerCommand>>,
    worker_rx: Option<Receiver<FilesWorkerResult>>,
    worker_result_tx: Option<Sender<FilesWorkerResult>>,
    worker_waker: Option<sui::UiHandle>,
    worker_cancellations: Arc<Mutex<BTreeSet<u64>>>,
    next_request_id: u64,
    pending_request_id: Option<u64>,
    pending_append_request_id: Option<u64>,
    completed_request_id: Option<u64>,
    pending_details_request_id: Option<u64>,
    pending_preview_request_id: Option<u64>,
    pending_document_request_id: Option<u64>,
    pending_document_save_request_id: Option<u64>,
    pending_document_saved_text: Option<String>,
    pending_attachment_request_id: Option<u64>,
    pending_attachment_source_path: Option<(FilesSourceKind, String)>,
    pending_import_request_ids: BTreeSet<u64>,
}

impl FilesState {
    pub fn new(backend: &Backend) -> Self {
        let local_path = default_local_path(backend.data_dir.root());
        Self {
            data_dir: backend.data_dir.clone(),
            daemon_address: backend.connection.daemon_address().map(str::to_string),
            daemon_unavailable_detail: backend.connection_issue_detail(),
            app_runtime: backend.app_runtime.handle().clone(),
            source: FilesSourceKind::Sifs,
            browser: None,
            error: backend.connection_issue_detail(),
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
            entries_total_count: 0,
            selected: None,
            selected_details: None,
            selected_local_details: None,
            list_slow_after_secs: None,
            details_loading: false,
            details_error: None,
            sifs_health: None,
            sifs_health_error: None,
            selected_preview: None,
            preview_loading: false,
            preview_error: None,
            document: None,
            document_revision: 0,
            document_close_confirmation: false,
            pending_document_open: None,
            attachment_loading: false,
            attachment_error: None,
            ready_attachment_intent: None,
            picker_lane: NativePickerLane::new(),
            picker_action: None,
            picker_context_generation: 0,
            picker_error: None,
            external_file_hovered: false,
            action_prompt: None,
            marked_entries: BTreeSet::new(),
            sort_mode: FileSortMode::default(),
            tasks: Vec::new(),
            pending_select_path: None,
            scroll: 0.0,
            inspector_details_open: false,
            compact_overlay: None,
            worker_tx: None,
            worker_rx: None,
            worker_result_tx: None,
            worker_waker: None,
            worker_cancellations: Arc::new(Mutex::new(BTreeSet::new())),
            next_request_id: 0,
            pending_request_id: None,
            pending_append_request_id: None,
            completed_request_id: None,
            pending_details_request_id: None,
            pending_preview_request_id: None,
            pending_document_request_id: None,
            pending_document_save_request_id: None,
            pending_document_saved_text: None,
            pending_attachment_request_id: None,
            pending_attachment_source_path: None,
            pending_import_request_ids: BTreeSet::new(),
        }
    }

    pub(crate) fn connection_attention_detail(&self) -> Option<String> {
        if let Some(error) = self.error.as_deref().filter(|text| !text.is_empty()) {
            return Some(format!("SIFS unavailable: {error}"));
        }
        if self.source == FilesSourceKind::Sifs {
            if let Some(error) = self
                .details_error
                .as_deref()
                .filter(|text| !text.is_empty())
            {
                return Some(format!("SIFS metadata unavailable: {error}"));
            }
        }
        self.tasks
            .iter()
            .find(|task| {
                task.source == FilesSourceKind::Sifs && task.state == FilesTaskState::Failed
            })
            .map(|task| format!("{} failed: {}", task.title, task.detail))
    }

    /// Replace endpoint-bound SIFS state after the daemon connection recovers or moves. Local
    /// browsing remains untouched; an already-open SIFS surface immediately retries on the new
    /// worker while a never-opened surface stays lazy.
    pub(crate) fn rebind_backend(
        &mut self,
        backend: &Backend,
        waker: Option<sui::UiHandle>,
    ) -> bool {
        let daemon_address = backend.connection.daemon_address().map(str::to_string);
        let daemon_unavailable_detail = backend.connection_issue_detail();
        let clustered = backend.connection.is_clustered();
        if self.daemon_address == daemon_address
            && self.daemon_unavailable_detail == daemon_unavailable_detail
            && self.clustered == clustered
        {
            return false;
        }

        let reopen_sifs = self.source == FilesSourceKind::Sifs && self.loaded;
        self.daemon_address = daemon_address;
        self.daemon_unavailable_detail = daemon_unavailable_detail;
        self.clustered = clustered;
        self.reset_for_retry();
        if reopen_sifs {
            self.ensure_loaded(waker);
        }
        true
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
            .ok_or_else(|| {
                self.daemon_unavailable_detail
                    .clone()
                    .unwrap_or_else(|| "SIFS browsing requires a daemon connection".to_string())
            })
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
        self.list_slow_after_secs = None;
        self.completed_request_id = None;
        self.pending_append_request_id = None;
        self.loading = false;
        let path = self.path.clone();
        let listing = match self.source {
            FilesSourceKind::Sifs => self.browser.as_ref().map(|b| {
                b.list_shallow_page(
                    &path,
                    self.sort_mode.sifs_options(0, Some(FILES_LIST_PAGE_SIZE)),
                )
            }),
            FilesSourceKind::Local => Some(list_local_dir_page(
                &path,
                self.sort_mode,
                0,
                Some(FILES_LIST_PAGE_SIZE),
            )),
        };
        match listing {
            Some(Ok(page)) => {
                self.entries_total_count = page.total_count;
                self.entries = page.entries;
                self.error = None;
                if self.source == FilesSourceKind::Local {
                    self.node_id = local_node_label();
                    self.sifs_health = None;
                    self.sifs_health_error = None;
                } else if let Some(browser) = self.browser.as_ref() {
                    match browser.health_summary() {
                        Ok(health) => {
                            self.sifs_health = health;
                            self.sifs_health_error = None;
                        }
                        Err(error) => {
                            self.sifs_health = None;
                            self.sifs_health_error = Some(error);
                        }
                    }
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
                self.entries_total_count = 0;
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
        self.list_slow_after_secs = None;
        self.completed_request_id = None;
        self.pending_append_request_id = None;
        self.loading = true;
        self.error = None;
        self.entries.clear();
        self.entries_total_count = 0;
        self.list_revision = self.list_revision.wrapping_add(1);
        let command = FilesWorkerCommand::List {
            request_id,
            source: self.source,
            path: self.path.clone(),
            sort_mode: self.sort_mode,
            offset: 0,
            limit: Some(FILES_LIST_PAGE_SIZE),
            append: false,
        };
        if tx.send(command).is_err() {
            self.pending_request_id = None;
            self.completed_request_id = None;
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
                        append: false,
                        result: FilesListResult::Slow {
                            elapsed_secs: LOAD_TIMEOUT.as_secs(),
                        },
                    });
                    if let Some(waker) = waker {
                        waker.wake();
                    }
                });
        }
    }

    pub(crate) fn retry(&mut self, waker: Option<sui::UiHandle>) {
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

    fn request_more_entries(&mut self, waker: Option<sui::UiHandle>) {
        if self.loading
            || self.pending_request_id.is_some()
            || self.pending_append_request_id.is_some()
            || self.error.is_some()
            || self.entries.len() >= self.entries_total_count
        {
            return;
        }
        if let Some(waker) = waker {
            self.ensure_worker(waker);
        }
        let Some(tx) = self.worker_tx.clone() else {
            return;
        };
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        self.pending_append_request_id = Some(request_id);
        let command = FilesWorkerCommand::List {
            request_id,
            source: self.source,
            path: self.path.clone(),
            sort_mode: self.sort_mode,
            offset: self.entries.len(),
            limit: Some(FILES_LIST_PAGE_SIZE),
            append: true,
        };
        if tx.send(command).is_err() {
            self.pending_append_request_id = None;
            self.error = Some("SIFS browser worker stopped".to_string());
            self.list_revision = self.list_revision.wrapping_add(1);
        }
    }

    fn reset_for_retry(&mut self) {
        self.browser = None;
        self.error = None;
        self.entries.clear();
        self.entries_total_count = 0;
        self.selected = None;
        self.marked_entries.clear();
        self.clear_selected_details();
        self.node_id.clear();
        self.sifs_health = None;
        self.sifs_health_error = None;
        self.list_slow_after_secs = None;
        self.inspector_details_open = false;
        self.loaded = self.source == FilesSourceKind::Local;
        self.loading = false;
        self.pending_request_id = None;
        self.pending_append_request_id = None;
        self.completed_request_id = None;
        self.pending_details_request_id = None;
        self.pending_preview_request_id = None;
        self.pending_document_request_id = None;
        self.pending_document_save_request_id = None;
        self.pending_document_saved_text = None;
        self.pending_attachment_request_id = None;
        self.pending_attachment_source_path = None;
        self.pending_import_request_ids.clear();
        self.attachment_loading = false;
        if let Some(document) = self.document.as_mut()
            && document.saving
        {
            document.saving = false;
            document.save_error = Some(
                "The connection changed before the save completed. Your edits are still open."
                    .to_string(),
            );
        }
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
        let daemon_unavailable_detail = self.daemon_unavailable_detail.clone();
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
                    daemon_unavailable_detail,
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
        let mut changed = self.poll_native_picker();
        loop {
            let result = match self.worker_rx.as_ref().map(|rx| rx.try_recv()) {
                Some(Ok(result)) => result,
                Some(Err(TryRecvError::Empty)) | None => break,
                Some(Err(TryRecvError::Disconnected)) => {
                    if self.loading {
                        self.loading = false;
                        self.pending_request_id = None;
                        self.completed_request_id = None;
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
                    if self.pending_document_request_id.take().is_some() {
                        if let Some(document) = self.document.as_mut() {
                            document.loading = false;
                            document.load_error = Some(
                                "The Files background worker stopped while reading the document."
                                    .to_string(),
                            );
                        }
                        self.document_revision = self.document_revision.wrapping_add(1);
                        changed = true;
                    }
                    if self.pending_document_save_request_id.take().is_some() {
                        self.pending_document_saved_text = None;
                        if let Some(document) = self.document.as_mut() {
                            document.saving = false;
                            document.save_error = Some(
                                "The Files background worker stopped while saving. Your edits are still open."
                                    .to_string(),
                            );
                        }
                        changed = true;
                    }
                    if self.pending_attachment_request_id.take().is_some() {
                        self.pending_attachment_source_path = None;
                        self.attachment_loading = false;
                        self.attachment_error = Some(
                            "The Files background worker stopped while reading the attachment."
                                .to_string(),
                        );
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
                append,
                result,
            } => {
                if append {
                    if self.pending_append_request_id != Some(request_id) {
                        return false;
                    }
                } else if self.pending_request_id != Some(request_id) {
                    return false;
                }
                if self.source != source {
                    return false;
                }
                match result {
                    FilesListResult::Ok(listing) => {
                        if append {
                            self.pending_append_request_id = None;
                        } else {
                            self.pending_request_id = None;
                            self.completed_request_id = Some(request_id);
                            self.loading = false;
                        }
                        self.list_slow_after_secs = None;
                        self.path = path;
                        self.store_active_path();
                        self.node_id = listing.node_id;
                        self.sifs_health = listing.health;
                        self.sifs_health_error = listing.health_error;
                        self.entries_total_count = listing.total_count;
                        if append {
                            self.entries.extend(listing.entries);
                        } else {
                            self.entries = listing.entries;
                        }
                        self.error = None;
                        if !append
                            && let Some(select_path) = self.pending_select_path.take()
                            && let Some(index) = self
                                .entries
                                .iter()
                                .position(|entry| entry.path == select_path)
                        {
                            self.selected = Some(index);
                            self.request_selected_details();
                        }
                    }
                    FilesListResult::Err(error) => {
                        if append {
                            self.pending_append_request_id = None;
                        } else {
                            self.pending_request_id = None;
                            self.completed_request_id = None;
                            self.loading = false;
                        }
                        self.list_slow_after_secs = None;
                        self.path = path;
                        self.store_active_path();
                        if !append {
                            self.entries.clear();
                            self.entries_total_count = 0;
                            self.sifs_health = None;
                            self.sifs_health_error = None;
                        }
                        self.error = Some(error);
                    }
                    FilesListResult::Slow { elapsed_secs } => {
                        self.list_slow_after_secs = Some(elapsed_secs);
                    }
                }
                self.list_revision = self.list_revision.wrapping_add(1);
                true
            }
            FilesWorkerResult::Health {
                request_id,
                source,
                health,
                health_error,
            } => {
                if self.source != source {
                    return false;
                }
                if self.pending_request_id.is_some()
                    || self.completed_request_id != Some(request_id)
                {
                    return false;
                }
                self.sifs_health = health;
                self.sifs_health_error = health_error;
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
            FilesWorkerResult::Document {
                request_id,
                source,
                path,
                result,
            } => {
                if self.pending_document_request_id != Some(request_id) {
                    return false;
                }
                let matches_open = self
                    .document
                    .as_ref()
                    .is_some_and(|document| document.source == source && document.path == path);
                if !matches_open {
                    return false;
                }
                self.pending_document_request_id = None;
                match result {
                    Ok(load) => {
                        self.document = Some(FilesDocument {
                            source: load.source,
                            path: load.path,
                            title: load.title,
                            saved_text: load.text.clone(),
                            text: load.text,
                            local_fingerprint: load.local_fingerprint,
                            sifs_fingerprint: load.sifs_fingerprint,
                            read_only_reason: load.read_only_reason,
                            loading: false,
                            load_error: None,
                            saving: false,
                            save_error: None,
                            save_warning: None,
                        });
                    }
                    Err(error) => {
                        if let Some(document) = self.document.as_mut() {
                            document.loading = false;
                            document.load_error = Some(error);
                        }
                    }
                }
                self.document_revision = self.document_revision.wrapping_add(1);
                true
            }
            FilesWorkerResult::DocumentSaved {
                request_id,
                source,
                path,
                result,
            } => {
                if self.pending_document_save_request_id != Some(request_id) {
                    return false;
                }
                let Some(document) = self.document.as_mut() else {
                    return false;
                };
                if document.source != source || document.path != path {
                    return false;
                }
                self.pending_document_save_request_id = None;
                let saved_text = self.pending_document_saved_text.take();
                document.saving = false;
                match result {
                    Ok(FilesDocumentSave::Local {
                        fingerprint,
                        maintenance_warning,
                        requires_reload,
                    }) => {
                        document.local_fingerprint = Some(fingerprint);
                        if let Some(saved_text) = saved_text.clone() {
                            document.saved_text = saved_text;
                        }
                        document.save_error = None;
                        document.save_warning = maintenance_warning;
                        document.read_only_reason = requires_reload.then(|| {
                            "The content was saved, but the host did not return a complete post-save metadata receipt. Reload before editing again."
                                .to_string()
                        });
                    }
                    Ok(FilesDocumentSave::Sifs {
                        fingerprint,
                        maintenance_warning,
                    }) => {
                        document.sifs_fingerprint = Some(fingerprint);
                        if let Some(saved_text) = saved_text {
                            document.saved_text = saved_text;
                        }
                        document.save_error = None;
                        document.save_warning = maintenance_warning;
                        self.pending_select_path = Some(path);
                        self.refresh(None);
                    }
                    Err(error) => {
                        document.save_error = Some(error);
                        document.save_warning = None;
                    }
                }
                true
            }
            FilesWorkerResult::Attachment {
                request_id,
                source,
                path,
                result,
            } => {
                if self.pending_attachment_request_id != Some(request_id) {
                    return false;
                }
                let still_requested =
                    self.pending_attachment_source_path.as_ref() == Some(&(source, path.clone()));
                self.pending_attachment_request_id = None;
                self.pending_attachment_source_path = None;
                self.attachment_loading = false;
                if !still_requested {
                    return false;
                }
                match result {
                    Ok(intent) => {
                        self.ready_attachment_intent = Some(intent);
                        self.attachment_error = None;
                    }
                    Err(error) => {
                        self.ready_attachment_intent = None;
                        self.attachment_error = Some(error);
                    }
                }
                true
            }
            FilesWorkerResult::ImportPrepared {
                request_id,
                destination_source,
                destination_path,
                result,
            } => {
                if !self.pending_import_request_ids.remove(&request_id) {
                    return false;
                }
                if self.source != destination_source || self.path != destination_path {
                    self.picker_error = Some(
                        "The destination changed before the import started. Nothing was copied."
                            .to_string(),
                    );
                    return true;
                }
                match result {
                    Ok(operations) => {
                        let waker = self.worker_waker.clone();
                        for (source, queued) in operations {
                            self.start_operation_for_source(
                                source,
                                queued.operation,
                                queued.title,
                                queued.detail,
                                waker.clone(),
                            );
                        }
                        self.picker_error = None;
                    }
                    Err(error) => self.picker_error = Some(error),
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
            FilesSourceKind::Local => {
                let path = Path::new(path);
                let candidate = if path.is_absolute() {
                    path.to_path_buf()
                } else {
                    Path::new(&self.path).join(path)
                };
                normalize_local_path(&candidate.to_string_lossy())
            }
        }
    }

    fn apply_location(
        &mut self,
        source: FilesSourceKind,
        path: String,
        waker: Option<sui::UiHandle>,
    ) {
        let location_changed = self.source != source || self.path != path;
        if self.source != source {
            self.store_active_path();
            self.source = source;
            self.error = None;
            self.entries.clear();
            self.entries_total_count = 0;
            self.selected = None;
            self.clear_selected_details();
            self.loading = false;
            self.pending_request_id = None;
            self.pending_append_request_id = None;
            self.completed_request_id = None;
            self.pending_details_request_id = None;
            self.pending_preview_request_id = None;
            self.list_revision = self.list_revision.wrapping_add(1);
            if let Some(waker) = waker {
                self.ensure_worker(waker);
            }
            self.loaded = true;
        }
        self.path = path;
        if location_changed {
            self.picker_context_generation = self.picker_context_generation.wrapping_add(1);
        }
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

    pub(crate) fn open_sifs_path(&mut self, path: impl Into<String>, waker: Option<sui::UiHandle>) {
        self.open_location(FilesSourceKind::Sifs, path.into(), waker);
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

    fn open_selected_document(&mut self, waker: Option<sui::UiHandle>) {
        let Some(entry) = self.selected_entry().cloned() else {
            return;
        };
        if entry.is_dir {
            return;
        }
        self.open_document(self.source, entry.path, entry.name, waker);
    }

    fn open_document(
        &mut self,
        source: FilesSourceKind,
        path: String,
        title: String,
        waker: Option<sui::UiHandle>,
    ) {
        if self.document.as_ref().is_some_and(FilesDocument::dirty) {
            self.pending_document_open = Some((source, path, title));
            self.document_close_confirmation = true;
            self.document_revision = self.document_revision.wrapping_add(1);
            return;
        }
        self.begin_document_load(source, path, title, waker);
    }

    fn begin_document_load(
        &mut self,
        source: FilesSourceKind,
        path: String,
        title: String,
        waker: Option<sui::UiHandle>,
    ) {
        if let Some(waker) = waker {
            self.ensure_worker(waker);
        }
        self.compact_overlay = None;
        self.action_prompt = None;
        self.document_close_confirmation = false;
        self.pending_document_open = None;
        self.pending_document_save_request_id = None;
        self.pending_document_saved_text = None;
        self.document = Some(FilesDocument::loading(source, path.clone(), title));
        self.document_revision = self.document_revision.wrapping_add(1);
        let Some(tx) = self.worker_tx.clone() else {
            if let Some(document) = self.document.as_mut() {
                document.loading = false;
                document.load_error = Some("The Files background worker is not ready. Retry after the folder finishes loading.".to_string());
            }
            self.document_revision = self.document_revision.wrapping_add(1);
            return;
        };
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        self.pending_document_request_id = Some(request_id);
        if tx
            .send(FilesWorkerCommand::ReadDocument {
                request_id,
                source,
                path,
            })
            .is_err()
        {
            self.pending_document_request_id = None;
            if let Some(document) = self.document.as_mut() {
                document.loading = false;
                document.load_error =
                    Some("The Files background worker stopped. Retry the document.".to_string());
            }
            self.document_revision = self.document_revision.wrapping_add(1);
        }
    }

    fn retry_document(&mut self, waker: Option<sui::UiHandle>) {
        let Some(document) = self.document.as_ref() else {
            return;
        };
        let request = (
            document.source,
            document.path.clone(),
            document.title.clone(),
        );
        self.begin_document_load(request.0, request.1, request.2, waker);
    }

    fn set_document_text(&mut self, value: String) {
        let Some(document) = self.document.as_mut() else {
            return;
        };
        if !document.editable() {
            return;
        }
        document.text = value;
        document.save_error = None;
    }

    fn save_document(&mut self, waker: Option<sui::UiHandle>) {
        if let Some(waker) = waker {
            self.ensure_worker(waker);
        }
        let Some(document) = self.document.as_mut() else {
            return;
        };
        if !document.editable() || !document.dirty() || document.saving {
            return;
        }
        let Some(tx) = self.worker_tx.clone() else {
            document.save_error = Some("The Files background worker is not available.".to_string());
            return;
        };
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        document.saving = true;
        document.save_error = None;
        document.save_warning = None;
        self.pending_document_saved_text = Some(document.text.clone());
        self.pending_document_save_request_id = Some(request_id);
        let command = match document.source {
            FilesSourceKind::Local => {
                let Some(expected) = document.local_fingerprint.clone() else {
                    document.saving = false;
                    self.pending_document_save_request_id = None;
                    self.pending_document_saved_text = None;
                    document.save_error = Some(
                        "The local file fingerprint is missing. Reload before saving.".to_string(),
                    );
                    return;
                };
                FilesWorkerCommand::SaveLocalDocument {
                    request_id,
                    path: document.path.clone(),
                    expected,
                    text: document.text.clone(),
                }
            }
            FilesSourceKind::Sifs => {
                let Some(expected) = document.sifs_fingerprint.clone() else {
                    document.saving = false;
                    self.pending_document_save_request_id = None;
                    self.pending_document_saved_text = None;
                    document.save_error = Some(
                        "The SIFS object version is missing. Reload before saving.".to_string(),
                    );
                    return;
                };
                FilesWorkerCommand::SaveSifsDocument {
                    request_id,
                    path: document.path.clone(),
                    expected,
                    text: document.text.clone(),
                }
            }
        };
        if tx.send(command).is_err() {
            self.pending_document_save_request_id = None;
            self.pending_document_saved_text = None;
            document.saving = false;
            document.save_error =
                Some("The Files background worker stopped before saving.".to_string());
        }
    }

    fn request_close_document(&mut self) {
        if self.document.as_ref().is_some_and(FilesDocument::dirty) {
            self.document_close_confirmation = true;
            self.document_revision = self.document_revision.wrapping_add(1);
            return;
        }
        self.close_document_now();
    }

    fn close_document_now(&mut self) {
        self.document = None;
        self.document_close_confirmation = false;
        self.pending_document_open = None;
        self.pending_document_request_id = None;
        self.pending_document_save_request_id = None;
        self.pending_document_saved_text = None;
        self.document_revision = self.document_revision.wrapping_add(1);
    }

    fn cancel_document_close(&mut self) {
        self.document_close_confirmation = false;
        self.pending_document_open = None;
        self.document_revision = self.document_revision.wrapping_add(1);
    }

    fn discard_document_and_continue(&mut self, waker: Option<sui::UiHandle>) {
        let pending = self.pending_document_open.take();
        self.document = None;
        self.document_close_confirmation = false;
        self.pending_document_request_id = None;
        self.pending_document_save_request_id = None;
        self.pending_document_saved_text = None;
        self.document_revision = self.document_revision.wrapping_add(1);
        if let Some((source, path, title)) = pending {
            self.begin_document_load(source, path, title, waker);
        }
    }

    fn start_attach_selected(&mut self, waker: Option<sui::UiHandle>) {
        let Some(entry) = self.selected_entry().cloned() else {
            return;
        };
        if entry.is_dir {
            self.attachment_error =
                Some("Folders cannot be attached directly. Choose a file.".to_string());
            return;
        }
        self.start_attach_path(self.source, entry.path, entry.name, waker);
    }

    fn start_attach_document(&mut self, waker: Option<sui::UiHandle>) {
        let Some(document) = self.document.as_ref() else {
            return;
        };
        let provenance = match document.source {
            FilesSourceKind::Local => {
                let Some(fingerprint) = document.local_fingerprint.as_ref() else {
                    self.attachment_error = Some(
                        "The document has no local snapshot fingerprint. Reload before attaching."
                            .to_string(),
                    );
                    return;
                };
                FilesAttachmentProvenance::Local {
                    path: document.path.clone(),
                    canonical_path: fingerprint.canonical_path.to_string_lossy().into_owned(),
                    modified_at_unix_nanos: fingerprint.modified_at_unix_nanos,
                }
            }
            FilesSourceKind::Sifs => {
                let Some(fingerprint) = document.sifs_fingerprint.as_ref() else {
                    self.attachment_error = Some(
                        "The document has no SIFS object version. Reload before attaching."
                            .to_string(),
                    );
                    return;
                };
                FilesAttachmentProvenance::Sifs {
                    path: document.path.clone(),
                    object_id: fingerprint.object_id.clone(),
                    version: fingerprint.version.clone(),
                    content_hash: fingerprint.content_hash.clone(),
                    policy: fingerprint.policy.clone(),
                }
            }
        };
        self.ready_attachment_intent = Some(FilesAttachmentIntent {
            file_name: document.title.clone(),
            bytes: document.text.as_bytes().to_vec(),
            provenance,
            is_unsaved_editor_buffer: document.dirty(),
        });
        self.attachment_error = None;
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn start_attach_path(
        &mut self,
        source: FilesSourceKind,
        path: String,
        file_name: String,
        waker: Option<sui::UiHandle>,
    ) {
        if let Some(waker) = waker {
            self.ensure_worker(waker);
        }
        let Some(tx) = self.worker_tx.clone() else {
            self.attachment_error =
                Some("The Files background worker is not available.".to_string());
            return;
        };
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        self.pending_attachment_request_id = Some(request_id);
        self.pending_attachment_source_path = Some((source, path.clone()));
        self.attachment_loading = true;
        self.attachment_error = None;
        self.ready_attachment_intent = None;
        if tx
            .send(FilesWorkerCommand::ReadAttachment {
                request_id,
                source,
                path,
                file_name,
            })
            .is_err()
        {
            self.pending_attachment_request_id = None;
            self.pending_attachment_source_path = None;
            self.attachment_loading = false;
            self.attachment_error = Some(
                "The Files background worker stopped before reading the attachment.".to_string(),
            );
        }
    }

    pub(crate) fn take_ready_attachment_intent(&mut self) -> Option<FilesAttachmentIntent> {
        self.ready_attachment_intent.take()
    }

    fn request_native_picker(&mut self, action: FilesPickerAction, waker: Option<sui::UiHandle>) {
        let (kind, title) = match action {
            FilesPickerAction::OpenFile => (NativePickerKind::OneFile, "Open a local file"),
            FilesPickerAction::OpenFolder => (NativePickerKind::OneFolder, "Open a local folder"),
            FilesPickerAction::ImportFiles => (
                NativePickerKind::MultipleFiles,
                "Import files into this folder",
            ),
            FilesPickerAction::ImportFolder => (
                NativePickerKind::OneFolder,
                "Import a folder into this folder",
            ),
        };
        self.picker_error = None;
        match self
            .picker_lane
            .request(kind, title, self.picker_context_generation, waker)
        {
            Ok(_) => self.picker_action = Some(action),
            Err(error) => self.picker_error = Some(error),
        }
    }

    fn poll_native_picker(&mut self) -> bool {
        let Some(result) = self.picker_lane.poll() else {
            return false;
        };
        let action = self.picker_action.take();
        if result.context_generation != self.picker_context_generation
            && matches!(
                action,
                Some(FilesPickerAction::ImportFiles | FilesPickerAction::ImportFolder)
            )
        {
            self.picker_error = Some(
                "The destination changed while the picker was open. Nothing was imported."
                    .to_string(),
            );
            return true;
        }
        match (action, result.outcome) {
            (_, NativePickerOutcome::Cancelled) => {}
            (_, NativePickerOutcome::Failed(error)) => self.picker_error = Some(error),
            (Some(FilesPickerAction::OpenFile), NativePickerOutcome::Selected(mut paths)) => {
                if let Some(path) = paths.pop() {
                    self.open_picked_local_file(path);
                }
            }
            (Some(FilesPickerAction::OpenFolder), NativePickerOutcome::Selected(mut paths)) => {
                if let Some(path) = paths.pop() {
                    match normalize_local_path(&path.to_string_lossy()) {
                        Ok(path) => self.open_location(
                            FilesSourceKind::Local,
                            path,
                            self.worker_waker.clone(),
                        ),
                        Err(error) => self.picker_error = Some(error),
                    }
                }
            }
            (
                Some(FilesPickerAction::ImportFiles | FilesPickerAction::ImportFolder),
                NativePickerOutcome::Selected(paths),
            ) => {
                if let Err(error) = self.import_external_paths(paths, self.worker_waker.clone()) {
                    self.picker_error = Some(error);
                }
            }
            (None, NativePickerOutcome::Selected(_)) => {
                self.picker_error = Some("The picker result no longer has an owner.".to_string())
            }
        }
        true
    }

    fn open_picked_local_file(&mut self, path: PathBuf) {
        let path_string = path.to_string_lossy().into_owned();
        let title = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| path_string.clone());
        if let Some(parent) = path.parent() {
            let parent = parent.to_string_lossy().into_owned();
            self.pending_select_path = Some(path_string.clone());
            self.open_location(FilesSourceKind::Local, parent, self.worker_waker.clone());
        }
        self.open_document(
            FilesSourceKind::Local,
            path_string,
            title,
            self.worker_waker.clone(),
        );
    }

    fn import_external_paths(
        &mut self,
        paths: Vec<PathBuf>,
        waker: Option<sui::UiHandle>,
    ) -> Result<(), String> {
        if paths.is_empty() {
            return Ok(());
        }
        if !self.can_modify_current_location() {
            return Err(
                "Wait for the destination folder to become available before importing.".to_string(),
            );
        }
        if let Some(waker) = waker {
            self.ensure_worker(waker);
        }
        let Some(tx) = self.worker_tx.clone() else {
            return Err("The Files background worker is not available.".to_string());
        };
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        self.pending_import_request_ids.insert(request_id);
        if tx
            .send(FilesWorkerCommand::PrepareImport {
                request_id,
                destination_source: self.source,
                destination_path: self.path.clone(),
                paths,
            })
            .is_err()
        {
            self.pending_import_request_ids.remove(&request_id);
            return Err(
                "The Files background worker stopped before preparing the import.".to_string(),
            );
        }
        Ok(())
    }

    pub(crate) fn begin_external_file_hover(&mut self, _path: PathBuf) -> bool {
        let changed = !self.external_file_hovered;
        self.external_file_hovered = true;
        changed
    }

    pub(crate) fn cancel_external_file_hover(&mut self) -> bool {
        let changed = self.external_file_hovered;
        self.external_file_hovered = false;
        changed
    }

    pub(crate) fn accept_external_file_drop(
        &mut self,
        path: PathBuf,
        waker: Option<sui::UiHandle>,
    ) -> bool {
        self.external_file_hovered = false;
        if let Err(error) = self.import_external_paths(vec![path], waker) {
            self.picker_error = Some(error);
        }
        true
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
        self.compact_overlay = None;
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
                Some("select one or more entries first".to_string())
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

    fn open_compact_overlay(&mut self, overlay: CompactFilesOverlay) {
        self.action_prompt = None;
        self.compact_overlay = Some(overlay);
    }

    fn close_compact_overlay(&mut self) {
        self.compact_overlay = None;
    }

    fn set_action_prompt_input(&mut self, input: String) {
        if let Some(prompt) = &mut self.action_prompt {
            prompt.input = input;
            prompt.error = None;
        }
    }

    fn set_sort_mode(&mut self, mode: FileSortMode) {
        if self.sort_mode == mode {
            return;
        }
        self.sort_mode = mode;
        self.reload();
    }

    fn toggle_sort_column(&mut self, column: FilesSortColumn) {
        let mode = self.sort_mode.next_for_column(column);
        self.set_sort_mode(mode);
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
                    current.error = Some("select one or more entries first".to_string());
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
                unreachable!("selected-entry prompts queue individual operations")
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
            return Err("select one or more entries first".to_string());
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
            _ => return Err("prompt does not support selected entries".to_string()),
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
        self.start_operation_for_source(self.source, operation, title, detail, waker);
    }

    fn start_operation_for_source(
        &mut self,
        source: FilesSourceKind,
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
                source,
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
                    source,
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
            .execute_operation_for_source(source, operation, &self.path)
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

    fn start_current_location_replica_repair(&mut self, waker: Option<sui::UiHandle>) {
        if self.source != FilesSourceKind::Sifs {
            return;
        }
        let path = self.path.clone();
        self.start_operation(
            FilesOperation {
                kind: FilesOperationKind::RepairReplicas,
                path: path.clone(),
                target: None,
                directory: true,
            },
            "Repair replicas".to_string(),
            path,
            waker,
        );
    }

    fn start_make_metadata_primary(&mut self, waker: Option<sui::UiHandle>) {
        if self.source != FilesSourceKind::Sifs {
            return;
        }
        let Some(entry) = self.selected_entry().cloned() else {
            return;
        };
        self.start_operation(
            FilesOperation {
                kind: FilesOperationKind::MakeMetadataPrimary,
                path: entry.path.clone(),
                target: None,
                directory: entry.is_dir,
            },
            "Make metadata primary".to_string(),
            entry.path,
            waker,
        );
    }

    fn start_pin_metadata(&mut self, waker: Option<sui::UiHandle>) {
        if self.source != FilesSourceKind::Sifs {
            return;
        }
        let Some(entry) = self.selected_entry().cloned() else {
            return;
        };
        self.start_operation(
            FilesOperation {
                kind: FilesOperationKind::PinMetadata,
                path: entry.path.clone(),
                target: None,
                directory: entry.is_dir,
            },
            "Pin metadata".to_string(),
            entry.path,
            waker,
        );
    }

    fn start_unpin_metadata(&mut self, waker: Option<sui::UiHandle>) {
        if self.source != FilesSourceKind::Sifs {
            return;
        }
        let Some(entry) = self.selected_entry().cloned() else {
            return;
        };
        self.start_operation(
            FilesOperation {
                kind: FilesOperationKind::UnpinMetadata,
                path: entry.path.clone(),
                target: None,
                directory: entry.is_dir,
            },
            "Unpin metadata".to_string(),
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

    fn execute_operation_for_source(
        &self,
        source: FilesSourceKind,
        operation: FilesOperation,
        current_path: &str,
    ) -> Result<FilesOperationOutcome, String> {
        if files_operation_is_cross_source(&operation) {
            if let Some(browser) = self.browser.as_ref() {
                return execute_cross_source_operation(
                    source,
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
            return execute_cross_source_operation(source, &browser, operation, current_path, None)
                .map_err(files_operation_failure_detail);
        }
        match source {
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
        let operation_source = self
            .tasks
            .iter()
            .find(|task| task.id == request_id)
            .map(|task| task.source);
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
                if operation_source == Some(self.source) {
                    self.path = outcome.refresh_path;
                    self.pending_select_path = outcome.select_path;
                }
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

    fn click_row(&mut self, index: usize, activation: VirtualTableRowActivationKind) -> bool {
        match activation {
            VirtualTableRowActivationKind::Single => {
                self.select_entry(index);
                false
            }
            VirtualTableRowActivationKind::Double => self.activate_row(index),
        }
    }

    /// Activate a listing row from keyboard or double-click: directories navigate and files open
    /// in the document surface.
    /// Returns whether the navigation changed the *path*.
    fn activate_row(&mut self, index: usize) -> bool {
        let is_dir = self.entries.get(index).map(|e| e.is_dir).unwrap_or(false);
        if is_dir {
            let target = self.entries[index].path.clone();
            self.navigate(target);
            true
        } else {
            self.select_entry(index);
            self.open_selected_document(self.worker_waker.clone());
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
    health: Option<SifsHealthSummary>,
    health_error: Option<String>,
    entries: Vec<SifsEntry>,
    total_count: usize,
}

enum FilesListResult {
    Ok(FilesListing),
    Err(String),
    Slow { elapsed_secs: u64 },
}

fn sifs_health_probe(browser: &SifsBrowser) -> (Option<SifsHealthSummary>, Option<String>) {
    match browser.health_summary() {
        Ok(health) => (health, None),
        Err(error) => (None, Some(error)),
    }
}

enum FilesWorkerResult {
    List {
        request_id: u64,
        source: FilesSourceKind,
        path: String,
        append: bool,
        result: FilesListResult,
    },
    Health {
        request_id: u64,
        source: FilesSourceKind,
        health: Option<SifsHealthSummary>,
        health_error: Option<String>,
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
    Document {
        request_id: u64,
        source: FilesSourceKind,
        path: String,
        result: Result<FilesDocumentLoad, String>,
    },
    DocumentSaved {
        request_id: u64,
        source: FilesSourceKind,
        path: String,
        result: Result<FilesDocumentSave, String>,
    },
    Attachment {
        request_id: u64,
        source: FilesSourceKind,
        path: String,
        result: Result<FilesAttachmentIntent, String>,
    },
    ImportPrepared {
        request_id: u64,
        destination_source: FilesSourceKind,
        destination_path: String,
        result: Result<Vec<(FilesSourceKind, FilesQueuedOperation)>, String>,
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
        sort_mode: FileSortMode,
        offset: usize,
        limit: Option<usize>,
        append: bool,
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
    ReadDocument {
        request_id: u64,
        source: FilesSourceKind,
        path: String,
    },
    SaveLocalDocument {
        request_id: u64,
        path: String,
        expected: LocalFileFingerprint,
        text: String,
    },
    SaveSifsDocument {
        request_id: u64,
        path: String,
        expected: SifsFileFingerprint,
        text: String,
    },
    ReadAttachment {
        request_id: u64,
        source: FilesSourceKind,
        path: String,
        file_name: String,
    },
    PrepareImport {
        request_id: u64,
        destination_source: FilesSourceKind,
        destination_path: String,
        paths: Vec<PathBuf>,
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
    daemon_unavailable_detail: Option<String>,
    app_runtime: tokio::runtime::Handle,
    command_rx: Receiver<FilesWorkerCommand>,
    result_tx: Sender<FilesWorkerResult>,
    cancellations: Arc<Mutex<BTreeSet<u64>>>,
    waker: sui::UiHandle,
) {
    let mut browser: Option<SifsBrowser> = None;
    let mut open_error: Option<String> = daemon_address.is_none().then(|| {
        daemon_unavailable_detail
            .clone()
            .unwrap_or_else(|| "SIFS browsing requires a daemon connection".to_string())
    });
    while let Ok(command) = command_rx.recv() {
        match command {
            FilesWorkerCommand::List {
                request_id,
                source,
                path,
                sort_mode,
                offset,
                limit,
                append,
            } => {
                let requested_path = normalize_source_path(source, &path);
                let (path, result) = if source == FilesSourceKind::Local {
                    let path = requested_path.unwrap_or_else(|error| {
                        let _ = result_tx.send(FilesWorkerResult::List {
                            request_id,
                            source,
                            path: path.clone(),
                            append,
                            result: FilesListResult::Err(error),
                        });
                        String::new()
                    });
                    if path.is_empty() {
                        waker.wake();
                        continue;
                    }
                    let result = list_local_dir_page(&path, sort_mode, offset, limit).map(|page| {
                        FilesListing {
                            node_id: local_node_label(),
                            health: None,
                            health_error: None,
                            entries: page.entries,
                            total_count: page.total_count,
                        }
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
                            let result = browser
                                .list_shallow_page(&path, sort_mode.sifs_options(offset, limit))
                                .map(|page| FilesListing {
                                    node_id: browser.node_id().to_string(),
                                    health: None,
                                    health_error: None,
                                    entries: page.entries,
                                    total_count: page.total_count,
                                });
                            let listed = result.is_ok();
                            let _ = result_tx.send(FilesWorkerResult::List {
                                request_id,
                                source,
                                path,
                                append,
                                result: match result {
                                    Ok(listing) => FilesListResult::Ok(listing),
                                    Err(error) => FilesListResult::Err(error),
                                },
                            });
                            waker.wake();
                            if listed {
                                let (health, health_error) = sifs_health_probe(browser);
                                let _ = result_tx.send(FilesWorkerResult::Health {
                                    request_id,
                                    source,
                                    health,
                                    health_error,
                                });
                                waker.wake();
                            }
                            continue;
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
                    append,
                    result: match result {
                        Ok(listing) => FilesListResult::Ok(listing),
                        Err(error) => FilesListResult::Err(error),
                    },
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
            FilesWorkerCommand::ReadDocument {
                request_id,
                source,
                path,
            } => {
                let requested_path = normalize_source_path(source, &path);
                let (path, result) = if source == FilesSourceKind::Local {
                    match requested_path {
                        Ok(path) => {
                            let result = load_local_text_document(&path, FILE_DOCUMENT_MAX_BYTES);
                            (path, result)
                        }
                        Err(error) => (path, Err(error)),
                    }
                } else if let Some(error) = &open_error {
                    (normalize_path(&path), Err(error.clone()))
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
                                open_error =
                                    Some("SIFS browsing requires a daemon connection".to_string())
                            }
                        }
                    }
                    match (&browser, &open_error) {
                        (Some(browser), _) => {
                            let path = browser.normalize(&requested_path);
                            let result =
                                load_sifs_text_document(browser, &path, FILE_DOCUMENT_MAX_BYTES);
                            (path, result)
                        }
                        (None, Some(error)) => (requested_path, Err(error.clone())),
                        (None, None) => (
                            requested_path,
                            Err("SIFS browser did not start".to_string()),
                        ),
                    }
                };
                let _ = result_tx.send(FilesWorkerResult::Document {
                    request_id,
                    source,
                    path,
                    result,
                });
            }
            FilesWorkerCommand::SaveLocalDocument {
                request_id,
                path,
                expected,
                text,
            } => {
                let result = save_local_text_document(&path, &expected, text.as_bytes());
                let _ = result_tx.send(FilesWorkerResult::DocumentSaved {
                    request_id,
                    source: FilesSourceKind::Local,
                    path,
                    result,
                });
            }
            FilesWorkerCommand::SaveSifsDocument {
                request_id,
                path,
                expected,
                text,
            } => {
                if browser.is_none() && open_error.is_none() {
                    match daemon_address.as_deref() {
                        Some(address) => {
                            match SifsBrowser::open_daemon_on_runtime(address, &app_runtime) {
                                Ok(opened) => browser = Some(opened),
                                Err(error) => open_error = Some(error),
                            }
                        }
                        None => {
                            open_error =
                                Some("SIFS browsing requires a daemon connection".to_string())
                        }
                    }
                }
                let result = match (&browser, &open_error) {
                    (Some(browser), _) => {
                        save_sifs_text_document(browser, &path, &expected, text.as_bytes())
                    }
                    (None, Some(error)) => Err(error.clone()),
                    (None, None) => Err("SIFS browser did not start".to_string()),
                };
                let _ = result_tx.send(FilesWorkerResult::DocumentSaved {
                    request_id,
                    source: FilesSourceKind::Sifs,
                    path,
                    result,
                });
            }
            FilesWorkerCommand::ReadAttachment {
                request_id,
                source,
                path,
                file_name,
            } => {
                let requested_path = normalize_source_path(source, &path);
                let (path, result) = if source == FilesSourceKind::Local {
                    match requested_path {
                        Ok(path) => {
                            let result =
                                load_local_attachment(&path, file_name, FILE_ATTACHMENT_MAX_BYTES);
                            (path, result)
                        }
                        Err(error) => (path, Err(error)),
                    }
                } else if let Some(error) = &open_error {
                    (normalize_path(&path), Err(error.clone()))
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
                                open_error =
                                    Some("SIFS browsing requires a daemon connection".to_string())
                            }
                        }
                    }
                    match (&browser, &open_error) {
                        (Some(browser), _) => {
                            let path = browser.normalize(&requested_path);
                            let result = load_sifs_attachment(
                                browser,
                                &path,
                                file_name,
                                FILE_ATTACHMENT_MAX_BYTES,
                            );
                            (path, result)
                        }
                        (None, Some(error)) => (requested_path, Err(error.clone())),
                        (None, None) => (
                            requested_path,
                            Err("SIFS browser did not start".to_string()),
                        ),
                    }
                };
                let _ = result_tx.send(FilesWorkerResult::Attachment {
                    request_id,
                    source,
                    path,
                    result,
                });
            }
            FilesWorkerCommand::PrepareImport {
                request_id,
                destination_source,
                destination_path,
                paths,
            } => {
                let result =
                    prepare_import_operations(destination_source, &destination_path, paths);
                let _ = result_tx.send(FilesWorkerResult::ImportPrepared {
                    request_id,
                    destination_source,
                    destination_path,
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
    // Keep this callback-safe: no canonicalize/current-dir/filesystem access. In particular, do
    // not fold leading `..` components; callers join relative paths to an explicit browsing base.
    Ok(display_local_path(
        PathBuf::from(trimmed).to_string_lossy().into_owned(),
    ))
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

fn list_local_dir_page(
    path: &str,
    sort_mode: FileSortMode,
    offset: usize,
    limit: Option<usize>,
) -> Result<SifsEntryPage, String> {
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
    sort_file_entries_by(&mut entries, sort_mode);
    let total_count = entries.len();
    let offset = offset.min(total_count);
    let entries = match limit {
        Some(limit) => entries.into_iter().skip(offset).take(limit).collect(),
        None => entries.into_iter().skip(offset).collect(),
    };
    Ok(SifsEntryPage {
        entries,
        total_count,
        offset,
        limit,
    })
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

fn local_file_fingerprint(path: &str) -> Result<LocalFileFingerprint, String> {
    let path_ref = Path::new(path);
    let link_metadata = fs::symlink_metadata(path_ref)
        .map_err(|error| format!("cannot inspect {path}: {error}"))?;
    if link_metadata.file_type().is_dir() {
        return Err(format!("{path} is a folder, not a file"));
    }
    let canonical_path = path_ref
        .canonicalize()
        .map_err(|error| format!("cannot resolve {path}: {error}"))?;
    let metadata = fs::metadata(&canonical_path)
        .map_err(|error| format!("cannot inspect {}: {error}", canonical_path.display()))?;
    if !metadata.is_file() {
        return Err(format!("{path} is not a regular file"));
    }
    Ok(LocalFileFingerprint {
        canonical_path,
        len: metadata.len(),
        modified_at_unix_nanos: metadata.modified().ok().and_then(system_time_unix_nanos),
        content_digest: String::new(),
        readonly: metadata.permissions().readonly(),
        symlink: link_metadata.file_type().is_symlink(),
    })
}

fn read_local_file_snapshot(
    path: &str,
    max_bytes: u64,
) -> Result<(Vec<u8>, LocalFileFingerprint), String> {
    let before = local_file_fingerprint(path)?;
    if before.len > max_bytes {
        return Err(format!(
            "{} is {:.1} MiB; the limit is {:.1} MiB",
            path,
            before.len as f64 / (1024.0 * 1024.0),
            max_bytes as f64 / (1024.0 * 1024.0),
        ));
    }
    let file = fs::File::open(&before.canonical_path)
        .map_err(|error| format!("cannot open {path}: {error}"))?;
    let mut bytes = Vec::with_capacity(before.len.min(max_bytes) as usize);
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| format!("cannot read {path}: {error}"))?;
    if bytes.len() as u64 > max_bytes {
        return Err(format!(
            "{path} grew beyond the {:.1} MiB limit while it was being read",
            max_bytes as f64 / (1024.0 * 1024.0),
        ));
    }
    let mut after = local_file_fingerprint(path)?;
    if after != before {
        return Err(format!(
            "{path} changed while it was being read. Retry to load a consistent snapshot."
        ));
    }
    after.content_digest = blake3::hash(&bytes).to_hex().to_string();
    Ok((bytes, after))
}

fn local_document_read_only_reason(fingerprint: &LocalFileFingerprint) -> Option<String> {
    if fingerprint.symlink {
        Some(
            "Editing through a symbolic link is disabled because an atomic replacement could change link semantics. Open the target file directly to edit it."
                .to_string(),
        )
    } else if fingerprint.readonly {
        Some("The host filesystem marks this file read-only.".to_string())
    } else {
        None
    }
}

fn load_local_text_document(path: &str, max_bytes: u64) -> Result<FilesDocumentLoad, String> {
    let (bytes, fingerprint) = read_local_file_snapshot(path, max_bytes)?;
    if !preview_bytes_are_text(&bytes) {
        return Err(
            "This file contains binary control bytes and cannot be opened in the text editor. It can still be attached to chat."
                .to_string(),
        );
    }
    let text = String::from_utf8(bytes).map_err(|_| {
        "This file is not valid UTF-8 and cannot be opened in the text editor. It can still be attached to chat."
            .to_string()
    })?;
    let title = Path::new(path)
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.to_string());
    let read_only_reason = local_document_read_only_reason(&fingerprint);
    Ok(FilesDocumentLoad {
        source: FilesSourceKind::Local,
        path: path.to_string(),
        title,
        text,
        local_fingerprint: Some(fingerprint),
        sifs_fingerprint: None,
        read_only_reason,
    })
}

fn sifs_file_fingerprint(details: &SifsObjectDetails) -> Result<SifsFileFingerprint, String> {
    if details.kind != "file" {
        return Err(format!(
            "{} is {}, not a regular file",
            details.path, details.kind
        ));
    }
    Ok(SifsFileFingerprint {
        object_id: details.object_id.clone(),
        version: details.version.clone(),
        content_hash: details.content_hash.clone(),
        size_bytes: details.size_bytes,
        policy: details.policy.clone(),
    })
}

fn read_sifs_file_snapshot(
    browser: &SifsBrowser,
    path: &str,
    max_bytes: u64,
) -> Result<(Vec<u8>, SifsFileFingerprint, u32), String> {
    let details = browser.stat(path)?;
    let fingerprint = sifs_file_fingerprint(&details)?;
    let size = fingerprint
        .size_bytes
        .ok_or_else(|| format!("SIFS did not report the size of {path}"))?;
    if size > max_bytes {
        return Err(format!(
            "{} is {:.1} MiB; the limit is {:.1} MiB",
            path,
            size as f64 / (1024.0 * 1024.0),
            max_bytes as f64 / (1024.0 * 1024.0),
        ));
    }
    let content_hash = fingerprint
        .content_hash
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("SIFS did not report the content hash of {path}"))?;
    let bytes = browser.read_object_version(
        &fingerprint.object_id,
        &fingerprint.version,
        content_hash,
        size,
    )?;
    if bytes.len() as u64 != size {
        return Err(format!(
            "SIFS returned {} bytes for {path}, but its selected version reports {size} bytes",
            bytes.len()
        ));
    }
    Ok((bytes, fingerprint, details.mode))
}

fn load_sifs_text_document(
    browser: &SifsBrowser,
    path: &str,
    max_bytes: u64,
) -> Result<FilesDocumentLoad, String> {
    let (bytes, fingerprint, mode) = read_sifs_file_snapshot(browser, path, max_bytes)?;
    if !preview_bytes_are_text(&bytes) {
        return Err(
            "This SIFS object contains binary control bytes and cannot be opened in the text editor. It can still be attached to chat."
                .to_string(),
        );
    }
    let text = String::from_utf8(bytes).map_err(|_| {
        "This SIFS object is not valid UTF-8 and cannot be opened in the text editor. It can still be attached to chat."
            .to_string()
    })?;
    let title = path
        .rsplit('/')
        .find(|part| !part.is_empty())
        .unwrap_or(path)
        .to_string();
    let read_only_reason = (mode & 0o222 == 0).then(|| {
        format!(
            "SIFS mode {:o} does not grant write permission for this file.",
            mode
        )
    });
    Ok(FilesDocumentLoad {
        source: FilesSourceKind::Sifs,
        path: path.to_string(),
        title,
        text,
        local_fingerprint: None,
        sifs_fingerprint: Some(fingerprint),
        read_only_reason,
    })
}

fn save_local_text_document(
    path: &str,
    expected: &LocalFileFingerprint,
    bytes: &[u8],
) -> Result<FilesDocumentSave, String> {
    if expected.symlink {
        return Err("Safe editing through a symbolic link is not supported.".to_string());
    }
    if expected.readonly {
        return Err("The file was opened read-only and cannot be saved.".to_string());
    }
    if bytes.len() as u64 > FILE_DOCUMENT_MAX_BYTES {
        return Err(format!(
            "The edited document exceeds the {:.1} MiB text-editor limit.",
            FILE_DOCUMENT_MAX_BYTES as f64 / (1024.0 * 1024.0),
        ));
    }
    let (_current_bytes, current) = read_local_file_snapshot(path, FILE_DOCUMENT_MAX_BYTES)?;
    if &current != expected {
        return Err(
            "Save conflict: the file changed on disk after it was opened. Your edits are still available; reload and merge before saving."
                .to_string(),
        );
    }
    if current.symlink || current.readonly {
        return Err(
            "The file is no longer safely writable. Your edits were not saved.".to_string(),
        );
    }
    let permissions = fs::metadata(&current.canonical_path)
        .map_err(|error| {
            format!(
                "cannot inspect {}: {error}",
                current.canonical_path.display()
            )
        })?
        .permissions();
    let fallback_name = current
        .canonical_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("document");
    sinomo_runtime::storage::durable_file::write_file_atomic(
        &current.canonical_path,
        bytes,
        sinomo_runtime::storage::durable_file::DurableAtomicFileOptions::new(
            fallback_name,
            "sinomo-app-edit",
        )
        .sync_all(),
    )
    .map_err(|error| format!("cannot safely save {path}: {error}"))?;
    let mut maintenance = Vec::new();
    if let Err(error) = fs::set_permissions(&current.canonical_path, permissions) {
        maintenance.push(format!(
            "could not restore the original permissions: {error}"
        ));
    }
    let digest = blake3::hash(bytes).to_hex().to_string();
    let (mut fingerprint, metadata_receipt_missing) = match local_file_fingerprint(path) {
        Ok(fingerprint) => (fingerprint, false),
        Err(error) => {
            maintenance.push(format!("could not refresh post-save metadata: {error}"));
            let mut fallback = current.clone();
            fallback.len = bytes.len() as u64;
            fallback.modified_at_unix_nanos = None;
            (fallback, true)
        }
    };
    fingerprint.content_digest = digest;
    let requires_reload = metadata_receipt_missing || fingerprint.symlink || fingerprint.readonly;
    Ok(FilesDocumentSave::Local {
        fingerprint,
        maintenance_warning: (!maintenance.is_empty()).then(|| maintenance.join(" ")),
        requires_reload,
    })
}

fn save_sifs_text_document(
    browser: &SifsBrowser,
    path: &str,
    expected: &SifsFileFingerprint,
    bytes: &[u8],
) -> Result<FilesDocumentSave, String> {
    if bytes.len() as u64 > FILE_DOCUMENT_MAX_BYTES {
        return Err(format!(
            "The edited document exceeds the {:.1} MiB text-editor limit.",
            FILE_DOCUMENT_MAX_BYTES as f64 / (1024.0 * 1024.0),
        ));
    }
    let outcome = browser
        .replace_file_if_version(path, &expected.object_id, &expected.version, bytes)
        .map_err(|error| {
            format!(
                "The SIFS save could not reach a definitive conditional-write result. Your edits are still open; retry or reload before saving again. {error}"
            )
        })?;
    match outcome {
        SifsReplaceFileIfVersionOutcome::Committed {
            receipt,
            maintenance_warning,
        } => Ok(FilesDocumentSave::Sifs {
            fingerprint: SifsFileFingerprint {
                object_id: receipt.object_id,
                version: receipt.version,
                content_hash: receipt.content_hash,
                size_bytes: receipt.size_bytes,
                policy: expected.policy.clone(),
            },
            maintenance_warning,
        }),
        SifsReplaceFileIfVersionOutcome::Conflict { detail } => Err(format!(
            "Save conflict: this SIFS file changed after it was opened. Your edits are still available; reload and merge before saving. {detail}"
        )),
        SifsReplaceFileIfVersionOutcome::NotFound { detail } => Err(format!(
            "The SIFS file no longer exists at this path. Your edits are still available and were not written. {detail}"
        )),
        SifsReplaceFileIfVersionOutcome::Rejected { detail } => Err(format!(
            "SIFS policy rejected the conditional save. Your edits are still available and were not written. {detail}"
        )),
    }
}

fn load_local_attachment(
    path: &str,
    file_name: String,
    max_bytes: u64,
) -> Result<FilesAttachmentIntent, String> {
    let (bytes, fingerprint) = read_local_file_snapshot(path, max_bytes)?;
    Ok(FilesAttachmentIntent {
        file_name,
        bytes,
        provenance: FilesAttachmentProvenance::Local {
            path: path.to_string(),
            canonical_path: fingerprint.canonical_path.to_string_lossy().into_owned(),
            modified_at_unix_nanos: fingerprint.modified_at_unix_nanos,
        },
        is_unsaved_editor_buffer: false,
    })
}

fn load_sifs_attachment(
    browser: &SifsBrowser,
    path: &str,
    file_name: String,
    max_bytes: u64,
) -> Result<FilesAttachmentIntent, String> {
    let (bytes, fingerprint, _mode) = read_sifs_file_snapshot(browser, path, max_bytes)?;
    Ok(FilesAttachmentIntent {
        file_name,
        bytes,
        provenance: FilesAttachmentProvenance::Sifs {
            path: path.to_string(),
            object_id: fingerprint.object_id,
            version: fingerprint.version,
            content_hash: fingerprint.content_hash,
            policy: fingerprint.policy,
        },
        is_unsaved_editor_buffer: false,
    })
}

fn prepare_import_operations(
    destination_source: FilesSourceKind,
    destination_path: &str,
    paths: Vec<PathBuf>,
) -> Result<Vec<(FilesSourceKind, FilesQueuedOperation)>, String> {
    let mut queued = Vec::with_capacity(paths.len());
    for path in paths {
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| format!("cannot inspect {}: {error}", path.display()))?;
        if metadata.file_type().is_symlink() {
            return Err(format!(
                "{} is a symbolic link. Resolve it to an explicit file or folder before importing.",
                path.display()
            ));
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| format!("{} has no importable file name", path.display()))?;
        let from = path.to_string_lossy().into_owned();
        let (source, kind, target, title) = match destination_source {
            FilesSourceKind::Sifs => (
                FilesSourceKind::Local,
                FilesOperationKind::CopyToSifs,
                join_sifs_child(destination_path, name)?,
                "Import to SIFS",
            ),
            FilesSourceKind::Local => (
                FilesSourceKind::Local,
                FilesOperationKind::Copy,
                join_local_child(destination_path, name)?,
                "Import local item",
            ),
        };
        queued.push((
            source,
            FilesQueuedOperation {
                operation: FilesOperation {
                    kind,
                    path: from.clone(),
                    target: Some(target.clone()),
                    directory: metadata.file_type().is_dir(),
                },
                title: title.to_string(),
                detail: format!("{from} -> {target}"),
            },
        ));
    }
    Ok(queued)
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
        FilesOperationKind::MakeMetadataPrimary => {
            let outcome = browser
                .make_metadata_primary(&operation.path)
                .map_err(FilesOperationFailure::Error)?;
            Ok(FilesOperationOutcome {
                refresh_path: current_path.to_string(),
                select_path: Some(operation.path.clone()),
                message: if outcome.changed {
                    format!(
                        "made local node primary for SIFS metadata shard {} (epoch {} -> {})",
                        outcome.shard_id, outcome.previous_epoch, outcome.new_epoch
                    )
                } else {
                    format!(
                        "local node already primary for SIFS metadata shard {}",
                        outcome.shard_id
                    )
                },
            })
        }
        FilesOperationKind::PinMetadata => {
            let node_id = browser
                .pin_metadata(&operation.path)
                .map_err(FilesOperationFailure::Error)?;
            Ok(FilesOperationOutcome {
                refresh_path: current_path.to_string(),
                select_path: Some(operation.path.clone()),
                message: format!("pinned metadata for {} on {node_id}", operation.path),
            })
        }
        FilesOperationKind::UnpinMetadata => {
            let removed = browser
                .unpin_metadata(&operation.path)
                .map_err(FilesOperationFailure::Error)?;
            Ok(FilesOperationOutcome {
                refresh_path: current_path.to_string(),
                select_path: Some(operation.path.clone()),
                message: if removed {
                    format!("removed metadata pin for {}", operation.path)
                } else {
                    format!("no metadata pin was recorded for {}", operation.path)
                },
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
        FilesOperationKind::MakeMetadataPrimary => Err(FilesOperationFailure::Error(
            "metadata primary transfer is only available for SIFS".to_string(),
        )),
        FilesOperationKind::PinMetadata
        | FilesOperationKind::UnpinMetadata
        | FilesOperationKind::PinContent
        | FilesOperationKind::UnpinContent => Err(FilesOperationFailure::Error(
            "pin management is only available for SIFS".to_string(),
        )),
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
    validate_local_copy_source(source_path)?;
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
    if metadata.file_type().is_symlink() {
        return Err(FilesOperationFailure::Error(format!(
            "{} is a symbolic link. Folder imports do not follow symbolic links; resolve it to an explicit file or folder and retry.",
            source.display()
        )));
    }
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
    if metadata.file_type().is_symlink() {
        return Err(FilesOperationFailure::Error(format!(
            "{} is a symbolic link. Folder copies do not follow symbolic links; resolve it to an explicit file or folder and retry.",
            source.display()
        )));
    }
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
    validate_local_copy_source(source_path)?;
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

fn validate_local_copy_source(source_path: &str) -> Result<(), FilesOperationFailure> {
    let metadata = fs::symlink_metadata(source_path).map_err(|error| {
        FilesOperationFailure::Error(format!("cannot inspect {source_path}: {error}"))
    })?;
    if metadata.file_type().is_symlink() {
        return Err(FilesOperationFailure::Error(format!(
            "{source_path} is a symbolic link. File copies and imports do not follow symbolic links; resolve it to an explicit file and retry."
        )));
    }
    if !metadata.file_type().is_file() {
        return Err(FilesOperationFailure::Error(format!(
            "{source_path} is not a regular file"
        )));
    }
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
        FilesOperationKind::MakeMetadataPrimary => {
            format!(
                "cancelled before making metadata primary for {}",
                operation.path
            )
        }
        FilesOperationKind::PinMetadata => {
            format!("cancelled before pinning metadata for {}", operation.path)
        }
        FilesOperationKind::UnpinMetadata => {
            format!("cancelled before unpinning metadata for {}", operation.path)
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
        FilesOperationKind::MakeMetadataPrimary => {
            format!(
                "{phase} metadata primary transfer in {scope}: {}",
                operation.path
            )
        }
        FilesOperationKind::PinMetadata => {
            format!("{phase} metadata pin in {scope}: {}", operation.path)
        }
        FilesOperationKind::UnpinMetadata => {
            format!("{phase} metadata unpin in {scope}: {}", operation.path)
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

/// Resolve the app chrome palette for file rows and preview-specific paint helpers.
fn palette(shared: &Shared) -> Palette {
    Palette::from_theme(&shared.theme(), widgets::theme_is_dark(shared))
}

fn toolbar_tooltip<W>(text: impl Into<String>, child: W, reader: &ThemeReader) -> Tooltip
where
    W: Widget + 'static,
{
    Tooltip::new(text, child)
        .placement(TooltipPlacement::Below)
        .theme(reader())
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
        _ => "Show details, then refresh placement".to_string(),
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

fn sifs_metadata_status_tone(
    loading: bool,
    error: Option<&str>,
    details: Option<&SifsObjectDetails>,
) -> SemanticTone {
    if loading {
        SemanticTone::Accent
    } else if error.is_some() {
        SemanticTone::Warning
    } else if details.is_some() {
        SemanticTone::Success
    } else {
        SemanticTone::Neutral
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

fn local_metadata_status_tone(
    loading: bool,
    error: Option<&str>,
    details: Option<&LocalObjectDetails>,
) -> SemanticTone {
    if loading {
        SemanticTone::Accent
    } else if error.is_some() {
        SemanticTone::Warning
    } else if details.is_some() {
        SemanticTone::Success
    } else {
        SemanticTone::Neutral
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

fn entry_row_meta(entry: &SifsEntry, files: &FilesState, active: bool) -> String {
    if active && files.source == FilesSourceKind::Sifs {
        return sifs_compact_header_line(
            entry,
            files.selected_details.as_ref(),
            files.clustered,
            files.details_loading,
            files.details_error.as_deref(),
        );
    }
    entry_meta(entry)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FileRowPlacementBadge {
    label: String,
    tone: SemanticTone,
    icon: IconGlyph,
    replicas: Option<(usize, usize)>,
}

fn file_row_placement_badge(
    _entry: &SifsEntry,
    files: &FilesState,
    row_index: usize,
) -> FileRowPlacementBadge {
    if files.source == FilesSourceKind::Local {
        return FileRowPlacementBadge {
            label: "this device".to_string(),
            tone: SemanticTone::Neutral,
            icon: IconGlyph::Storage,
            replicas: None,
        };
    }

    let active = files.selected == Some(row_index);
    if active && files.details_loading {
        return FileRowPlacementBadge {
            label: "syncing".to_string(),
            tone: SemanticTone::Accent,
            icon: IconGlyph::Redo,
            replicas: None,
        };
    }
    if active && files.details_error.is_some() {
        return FileRowPlacementBadge {
            label: "failed".to_string(),
            tone: SemanticTone::Danger,
            icon: IconGlyph::Alert,
            replicas: None,
        };
    }

    if active && let Some(details) = files.selected_details.as_ref() {
        return sifs_details_placement_badge(details);
    }

    FileRowPlacementBadge {
        label: if files.clustered {
            "on mesh".to_string()
        } else {
            "this node".to_string()
        },
        tone: if files.clustered {
            SemanticTone::Info
        } else {
            SemanticTone::Neutral
        },
        icon: IconGlyph::Storage,
        replicas: None,
    }
}

fn sifs_details_placement_badge(details: &SifsObjectDetails) -> FileRowPlacementBadge {
    let replicas = sifs_details_replica_dots(details);
    if let Some(health) = details.health.as_ref() {
        if health.open_conflict_count > 0 {
            return FileRowPlacementBadge {
                label: "conflict".to_string(),
                tone: SemanticTone::Warning,
                icon: IconGlyph::Alert,
                replicas,
            };
        }
        if sifs_repair_backlog_count(health) > 0 {
            return FileRowPlacementBadge {
                label: "repair".to_string(),
                tone: SemanticTone::Warning,
                icon: IconGlyph::Redo,
                replicas,
            };
        }
    }

    if details.policy == "local-owned" || details.policy == "ephemeral-local" {
        return FileRowPlacementBadge {
            label: "this device".to_string(),
            tone: SemanticTone::Neutral,
            icon: IconGlyph::Storage,
            replicas: None,
        };
    }

    if details.current_locations.is_empty() {
        return FileRowPlacementBadge {
            label: "queued".to_string(),
            tone: SemanticTone::Neutral,
            icon: IconGlyph::Storage,
            replicas,
        };
    }

    let (label, tone) = if details.policy.starts_with("replicated") {
        if details
            .health
            .as_ref()
            .is_some_and(|health| sifs_agent_state_is_current(&health.agent_state))
        {
            ("synced", SemanticTone::Success)
        } else {
            ("on mesh", SemanticTone::Info)
        }
    } else {
        ("on mesh", SemanticTone::Info)
    };

    FileRowPlacementBadge {
        label: label.to_string(),
        tone,
        icon: IconGlyph::Storage,
        replicas,
    }
}

fn sifs_details_replica_dots(details: &SifsObjectDetails) -> Option<(usize, usize)> {
    if !(details.policy.starts_with("replicated")) {
        return None;
    }
    let current = details.current_locations.len();
    let desired = sifs_replica_desired_count(details).max(current);
    (desired > 0).then_some((current, desired))
}

fn sort_file_entries_by(entries: &mut [SifsEntry], sort_mode: FileSortMode) {
    entries.sort_by(|left, right| compare_file_entries(left, right, sort_mode));
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

fn sifs_can_make_metadata_primary(details: &SifsObjectDetails) -> bool {
    details.metadata_shard.as_ref().is_some_and(|shard| {
        shard.local_is_replica && !shard.local_is_primary && !shard.local_node_id.is_empty()
    })
}

fn sifs_metadata_shard_summary(details: Option<&SifsObjectDetails>) -> String {
    let Some(shard) = details.and_then(|details| details.metadata_shard.as_ref()) else {
        return "metadata shard not loaded".to_string();
    };
    if shard.local_is_primary {
        format!("{} primary here; epoch {}", shard.shard_id, shard.epoch)
    } else if shard.local_is_replica {
        format!(
            "{} primary {}; local replica; epoch {}",
            shard.shard_id,
            short(&shard.primary_node_id, 18),
            shard.epoch
        )
    } else {
        format!(
            "{} primary {}; local not replica; epoch {}",
            shard.shard_id,
            short(&shard.primary_node_id, 18),
            shard.epoch
        )
    }
}

fn sifs_metadata_shard_detail_summary(shard: &SifsMetadataShardDetails) -> String {
    let replicas = if shard.replica_node_ids.is_empty() {
        "none".to_string()
    } else {
        shard
            .replica_node_ids
            .iter()
            .map(|node_id| short(node_id, 18))
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "primary {}; replicas {}; epoch {}",
        short(&shard.primary_node_id, 18),
        replicas,
        shard.epoch
    )
}

fn sifs_pin_summary(details: Option<&SifsObjectDetails>) -> String {
    let Some(details) = details else {
        return "pin state unknown; refresh metadata to manage pins".to_string();
    };
    if details.local_pins.is_empty() {
        return "no local pins on this node".to_string();
    }
    details
        .local_pins
        .iter()
        .map(|pin| {
            format!(
                "{} pinned on {}",
                pin.mode,
                short(&pin.pinned_by_node_id, 20)
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn sifs_has_local_pin(details: &SifsObjectDetails, mode: &str) -> bool {
    details.local_pins.iter().any(|pin| pin.mode == mode)
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
        if let Some(health) = details.health.as_ref() {
            let repair_count = sifs_repair_backlog_count(health);
            if health.open_conflict_count > 0 {
                return format!(
                    "replicated; {}, {} open",
                    sifs_replica_coverage_label(details),
                    plural(health.open_conflict_count, "conflict")
                );
            }
            if repair_count > 0 {
                return format!(
                    "replicated; {}, {} pending",
                    sifs_replica_coverage_label(details),
                    plural(repair_count, "repair")
                );
            }
            return match count {
                0 => "replication pending; no durable replica receipt yet".to_string(),
                _ => format!(
                    "replicated; {} current",
                    sifs_replica_coverage_label(details)
                ),
            };
        }
        return match count {
            0 => "replication pending; no durable location recorded".to_string(),
            1 => "one replica location recorded; repair status not loaded".to_string(),
            count => format!("{count} replica locations recorded; repair status not loaded"),
        };
    }
    if count == 0 {
        format!("{}; no durable location recorded", details.policy)
    } else {
        format!("{}; {} location(s) recorded", details.policy, count)
    }
}

fn sifs_repair_backlog_count(health: &SifsObjectHealth) -> usize {
    health
        .pending_repair_count
        .max(health.missing_replica_nodes.len())
}

fn sifs_replica_desired_count(details: &SifsObjectDetails) -> usize {
    let current = details.current_locations.len();
    details
        .health
        .as_ref()
        .map(|health| current + sifs_repair_backlog_count(health))
        .unwrap_or(current)
}

fn sifs_replica_coverage_label(details: &SifsObjectDetails) -> String {
    let current = details.current_locations.len();
    let desired = sifs_replica_desired_count(details);
    if desired > current {
        format!("{current}/{desired} replicas")
    } else {
        plural(current, "replica")
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
    let repair_count = sifs_repair_backlog_count(health);
    if repair_count > 0 {
        let missing = sifs_missing_replica_summary(health);
        if missing != "none" {
            return format!(
                "{}; {} pending for {}",
                health.agent_state,
                plural(repair_count, "repair"),
                missing
            );
        }
        return format!(
            "{}; {} pending",
            health.agent_state,
            plural(repair_count, "repair")
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

fn sifs_agent_state_is_current(state: &str) -> bool {
    state == "current" || state == "healthy"
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
        _ => {
            let current = details.current_locations.len();
            let desired = sifs_replica_desired_count(details);
            if desired > current {
                format!("{current}/{desired} replicas")
            } else {
                match current {
                    0 => "no replicas".to_string(),
                    1 => "1 replica".to_string(),
                    count => format!("{count} replicas"),
                }
            }
        }
    };
    let health = details
        .health
        .as_ref()
        .map(|health| {
            if health.open_conflict_count > 0 {
                plural(health.open_conflict_count, "conflict")
            } else if sifs_repair_backlog_count(health) > 0 {
                plural(sifs_repair_backlog_count(health), "repair")
            } else if sifs_agent_state_is_current(&health.agent_state) {
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
    if sifs_can_make_metadata_primary(details) {
        return "make this node primary if the current metadata primary is unavailable".to_string();
    }
    if details.policy == "local-owned" {
        return "copy or move into a replicated SIFS namespace to spread across nodes".to_string();
    }
    if details.policy == "ephemeral-local" {
        return "copy into durable SIFS storage before relying on this object".to_string();
    }
    if details.policy.starts_with("replicated") {
        if let Some(health) = details.health.as_ref() {
            if health.open_conflict_count > 0 {
                return "resolve SIFS conflicts before changing placement".to_string();
            }
            if sifs_repair_backlog_count(health) > 0 {
                return "repair replicas, then refresh placement".to_string();
            }
            if sifs_agent_state_is_current(&health.agent_state)
                && details.current_locations.len() > 1
            {
                return "placement is healthy; use details for object and version ids".to_string();
            }
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
            let shard = details
                .metadata_shard
                .as_ref()
                .map(|shard| {
                    format!(
                        "{}:{}:{}:{}:{}:{}",
                        shard.shard_id,
                        shard.primary_node_id,
                        shard.replica_node_ids.join(","),
                        shard.epoch,
                        shard.local_node_id,
                        shard.local_is_primary
                    )
                })
                .unwrap_or_default();
            format!(
                "{}|{}|{}|{}|{}|{}",
                details.version,
                details.policy,
                details.current_locations.join(","),
                pins,
                health,
                shard
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

fn unselected_inspector_health_summary(files: &FilesState) -> Option<String> {
    if files.source != FilesSourceKind::Sifs {
        return None;
    }
    if let Some(health) = files.sifs_health.as_ref() {
        return Some(sifs_health_status_summary(health));
    }
    if let Some(error) = files.sifs_health_error.as_deref() {
        return Some(sifs_health_error_status_summary(error));
    }
    Some("Not checked yet; refresh this folder to update health".to_string())
}

fn unselected_inspector_health_detail(files: &FilesState) -> Option<String> {
    if files.source != FilesSourceKind::Sifs {
        return None;
    }
    if let Some(health) = files.sifs_health.as_ref() {
        return Some(sifs_health_status_detail(health));
    }
    if let Some(error) = files.sifs_health_error.as_deref() {
        return Some(sifs_health_error_status_detail(error));
    }
    Some("DFS health not loaded".to_string())
}

fn unselected_inspector_rows(files: &FilesState) -> Vec<(&'static str, String)> {
    let mut rows = vec![
        ("Path", files.path.clone()),
        ("Entries", files_entry_count_status(files)),
        ("Scope", unselected_inspector_scope(files)),
        (
            "Status",
            unselected_inspector_status(files.source, files.entries.is_empty()).to_string(),
        ),
    ];
    if let Some(health) = unselected_inspector_health_summary(files) {
        rows.push(("SIFS health", health));
    }
    if files.external_file_hovered {
        rows.push((
            "Drop",
            "Release to import this host file or folder into the current location".to_string(),
        ));
    } else if let Some(error) = files.picker_error.as_ref() {
        rows.push(("Import", error.clone()));
    } else {
        rows.push((
            "Import",
            "Use the native picker or drag files and folders here; pasted paths remain available under Go to path."
                .to_string(),
        ));
    }
    if files.source == FilesSourceKind::Sifs && files.inspector_details_open {
        if let Some(detail) = files_sifs_connection_detail(files) {
            rows.push(("Daemon", detail));
        }
        if let Some(health) = unselected_inspector_health_detail(files) {
            rows.push(("DFS detail", health));
        }
    }
    if unselected_inspector_actions(files).len() <= 1 {
        rows.push(("Next step", unselected_inspector_management(files)));
    }
    rows
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
        let selected = match shared.files().source {
            FilesSourceKind::Sifs => 0,
            FilesSourceKind::Local => 1,
        };
        SegmentedControl::new("Files source")
            .items([
                SegmentedControlItem::new("SIFS")
                    .semantic_name("Browse SIFS")
                    .description("Browse distributed SIFS objects"),
                SegmentedControlItem::new("Local")
                    .semantic_name("Browse local files")
                    .description("Browse this node's local filesystem"),
            ])
            .selected(selected)
            .theme_when(clone_reader(reader))
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

    let open_local_file = {
        let s = shared.clone();
        IconButton::new(IconGlyph::FileText, "Open local file")
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || !s.files().picker_lane.is_pending()
            })
            .on_press(move || {
                let waker = { s.borrow().waker.clone() };
                s.files_mut()
                    .request_native_picker(FilesPickerAction::OpenFile, waker);
            })
    };

    let open_local_folder = {
        let s = shared.clone();
        IconButton::new(IconGlyph::Folder, "Choose local folder")
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || !s.files().picker_lane.is_pending()
            })
            .on_press(move || {
                let waker = { s.borrow().waker.clone() };
                s.files_mut()
                    .request_native_picker(FilesPickerAction::OpenFolder, waker);
            })
    };

    let import_files = {
        let s = shared.clone();
        IconButton::new(IconGlyph::Attach, "Import files")
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || {
                    let files = s.files();
                    files.can_modify_current_location() && !files.picker_lane.is_pending()
                }
            })
            .on_press(move || {
                let waker = { s.borrow().waker.clone() };
                s.files_mut()
                    .request_native_picker(FilesPickerAction::ImportFiles, waker);
            })
    };

    let new_file = {
        let s = shared.clone();
        IconButton::new(IconGlyph::File, "New file")
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
        IconButton::new(IconGlyph::Folder, "New folder")
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
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || {
                    let files = s.files();
                    files.can_modify_current_location()
                        && (files.marked_count() > 0 || files.selected_entry().is_some())
                }
            })
            .on_press_with_ctx(move |ctx| {
                let kind = if s.files().marked_count() > 0 {
                    FileActionPromptKind::CopySelected
                } else {
                    FileActionPromptKind::Copy
                };
                s.files_mut().open_action_prompt(kind);
                request_files_overlay_rebuild(ctx);
            })
    };

    let move_to = {
        let s = shared.clone();
        IconButton::new(IconGlyph::ChevronRight, "Move to")
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || {
                    let files = s.files();
                    files.can_modify_current_location()
                        && (files.marked_count() > 0 || files.selected_entry().is_some())
                }
            })
            .on_press_with_ctx(move |ctx| {
                let kind = if s.files().marked_count() > 0 {
                    FileActionPromptKind::MoveSelected
                } else {
                    FileActionPromptKind::Move
                };
                s.files_mut().open_action_prompt(kind);
                request_files_overlay_rebuild(ctx);
            })
    };

    let refresh_placement = {
        let s = shared.clone();
        IconButton::new(IconGlyph::Storage, "Refresh placement")
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

    let repair_replicas = {
        let s = shared.clone();
        IconButton::new(IconGlyph::Redo, "Repair replicas")
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
                s.files_mut().start_replica_repair(waker);
                request_files_overlay_rebuild(ctx);
            })
    };

    let rename = {
        let s = shared.clone();
        IconButton::new(IconGlyph::MoreHorizontal, "Rename")
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
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || {
                    let files = s.files();
                    files.can_modify_current_location()
                        && (files.marked_count() > 0 || files.selected_entry().is_some())
                }
            })
            .on_press_with_ctx(move |ctx| {
                let kind = if s.files().marked_count() > 0 {
                    FileActionPromptKind::DeleteSelected
                } else {
                    FileActionPromptKind::Delete
                };
                s.files_mut().open_action_prompt(kind);
                request_files_overlay_rebuild(ctx);
            })
    };

    let toolbar_row = Flex::horizontal()
        .main_gap(HEADER_COMMAND_GAP)
        .align_items(Alignment::Center)
        .with_item(
            toolbar_tooltip("Back", back, reader),
            FlexItem::new().no_shrink(),
        )
        .with_item(
            toolbar_tooltip("Forward", forward, reader),
            FlexItem::new().no_shrink(),
        )
        .with_item(
            toolbar_tooltip("Up", up, reader),
            FlexItem::new().no_shrink(),
        )
        .with_item(
            SizedBox::new().width(120.0).with_child(source),
            FlexItem::fixed(120.0),
        )
        .with_item(breadcrumb, FlexItem::fill().min_width(160.0))
        .with_item(
            toolbar_tooltip(
                format!("Refresh {}", shared.files().source.label()),
                refresh,
                reader,
            ),
            FlexItem::new().no_shrink(),
        )
        .with_item(
            toolbar_tooltip("Open local file", open_local_file, reader),
            FlexItem::new().no_shrink(),
        )
        .with_item(
            toolbar_tooltip("Choose local folder", open_local_folder, reader),
            FlexItem::new().no_shrink(),
        )
        .with_item(
            toolbar_tooltip("Import files", import_files, reader),
            FlexItem::new().no_shrink(),
        )
        .with_item(
            toolbar_tooltip("Go to path", go_to, reader),
            FlexItem::new().no_shrink(),
        )
        .with_item(
            toolbar_tooltip("New file", new_file, reader),
            FlexItem::new().no_shrink(),
        )
        .with_item(
            toolbar_tooltip("New folder", new_folder, reader),
            FlexItem::new().no_shrink(),
        )
        .with_item(
            toolbar_tooltip("Copy to", copy, reader),
            FlexItem::new().no_shrink(),
        )
        .with_item(
            toolbar_tooltip("Move to", move_to, reader),
            FlexItem::new().no_shrink(),
        )
        .with_item(
            toolbar_tooltip("Refresh placement", refresh_placement, reader),
            FlexItem::new().no_shrink(),
        )
        .with_item(
            toolbar_tooltip("Repair replicas", repair_replicas, reader),
            FlexItem::new().no_shrink(),
        )
        .with_item(
            toolbar_tooltip("Rename", rename, reader),
            FlexItem::new().no_shrink(),
        )
        .with_item(
            toolbar_tooltip("Delete selected item", delete, reader),
            FlexItem::new().no_shrink(),
        );

    Stack::vertical()
        .alignment(Alignment::Stretch)
        .with_child(SizedBox::new().height(HEADER_PAD_Y))
        .with_child(toolbar_row)
        .with_child(SizedBox::new().height(HEADER_PAD_Y))
}

/// Intermediate desktop header used while the inspector is still visible but the complete
/// command row would overflow. File/folder pickers stay first-class; path entry and mutation
/// commands remain available through the same keyboard-focusable overflow sheet as narrow Files.
fn picker_first_header(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let crumbs = shared.files().crumbs();
    let current = crumbs.len().saturating_sub(1);
    let targets: Vec<String> = crumbs.iter().map(|(_, target)| target.clone()).collect();

    let back = {
        let s = shared.clone();
        IconButton::new(IconGlyph::ChevronLeft, "Back")
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
            .theme_when(clone_reader(reader))
            .on_press(move || s.files_mut().go_up())
    };
    let source = {
        let s = shared.clone();
        let selected = match shared.files().source {
            FilesSourceKind::Sifs => 0,
            FilesSourceKind::Local => 1,
        };
        SegmentedControl::new("Files source")
            .items([
                SegmentedControlItem::new("SIFS")
                    .semantic_name("Browse SIFS")
                    .description("Browse distributed SIFS objects"),
                SegmentedControlItem::new("Local")
                    .semantic_name("Browse local files")
                    .description("Browse this node's local filesystem"),
            ])
            .selected(selected)
            .theme_when(clone_reader(reader))
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
    let breadcrumb = {
        let s = shared.clone();
        Breadcrumb::new(shared.files().source.path_label())
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
    let open_local_file = {
        let s = shared.clone();
        IconButton::new(IconGlyph::FileText, "Open local file")
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || !s.files().picker_lane.is_pending()
            })
            .on_press(move || {
                let waker = { s.borrow().waker.clone() };
                s.files_mut()
                    .request_native_picker(FilesPickerAction::OpenFile, waker);
            })
    };
    let open_local_folder = {
        let s = shared.clone();
        IconButton::new(IconGlyph::Folder, "Choose local folder")
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || !s.files().picker_lane.is_pending()
            })
            .on_press(move || {
                let waker = { s.borrow().waker.clone() };
                s.files_mut()
                    .request_native_picker(FilesPickerAction::OpenFolder, waker);
            })
    };
    let import_files = {
        let s = shared.clone();
        IconButton::new(IconGlyph::Attach, "Import files")
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || {
                    let files = s.files();
                    files.can_modify_current_location() && !files.picker_lane.is_pending()
                }
            })
            .on_press(move || {
                let waker = { s.borrow().waker.clone() };
                s.files_mut()
                    .request_native_picker(FilesPickerAction::ImportFiles, waker);
            })
    };
    let refresh = {
        let s = shared.clone();
        IconButton::new(
            IconGlyph::Redo,
            format!("Refresh {}", shared.files().source.label()),
        )
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
    let commands = {
        let s = shared.clone();
        IconButton::new(IconGlyph::MoreHorizontal, "More file commands")
            .theme_when(clone_reader(reader))
            .selected_when({
                let s = s.clone();
                move || s.files().compact_overlay == Some(CompactFilesOverlay::Commands)
            })
            .on_press_with_ctx(move |ctx| {
                s.files_mut()
                    .open_compact_overlay(CompactFilesOverlay::Commands);
                request_files_overlay_rebuild(ctx);
            })
    };

    let toolbar_row = Flex::horizontal()
        .main_gap(HEADER_COMMAND_GAP)
        .align_items(Alignment::Center)
        .with_item(back, FlexItem::new().no_shrink())
        .with_item(forward, FlexItem::new().no_shrink())
        .with_item(up, FlexItem::new().no_shrink())
        .with_item(
            SizedBox::new().width(120.0).with_child(source),
            FlexItem::fixed(120.0),
        )
        .with_item(breadcrumb, FlexItem::fill().min_width(96.0))
        .with_item(
            toolbar_tooltip("Open local file", open_local_file, reader),
            FlexItem::new().no_shrink(),
        )
        .with_item(
            toolbar_tooltip("Choose local folder", open_local_folder, reader),
            FlexItem::new().no_shrink(),
        )
        .with_item(
            toolbar_tooltip("Import files", import_files, reader),
            FlexItem::new().no_shrink(),
        )
        .with_item(
            toolbar_tooltip(
                format!("Refresh {}", shared.files().source.label()),
                refresh,
                reader,
            ),
            FlexItem::new().no_shrink(),
        )
        .with_item(
            toolbar_tooltip("More file commands", commands, reader),
            FlexItem::new().no_shrink(),
        );

    Stack::vertical()
        .alignment(Alignment::Stretch)
        .with_child(SizedBox::new().height(HEADER_PAD_Y))
        .with_child(toolbar_row)
        .with_child(SizedBox::new().height(HEADER_PAD_Y))
}

/// Narrow file-manager chrome keeps the essential navigation controls visible
/// and moves the remaining commands plus the hidden wide inspector into
/// explicit, keyboard-focusable overlays.
fn compact_header(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
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
    let up = {
        let s = shared.clone();
        IconButton::new(IconGlyph::ChevronUp, "Up")
            .size(30.0)
            .theme_when(clone_reader(reader))
            .on_press(move || s.files_mut().go_up())
    };
    let location = {
        let files = shared.files();
        let theme = shared.theme();
        let pal = palette(shared);
        let source_label = match files.source {
            FilesSourceKind::Sifs => "SIFS",
            FilesSourceKind::Local => "Local",
        };
        let leaf = match files.source {
            FilesSourceKind::Sifs => files
                .path
                .rsplit('/')
                .find(|segment| !segment.is_empty())
                .unwrap_or("/"),
            FilesSourceKind::Local => Path::new(&files.path)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(files.path.as_str()),
        };
        Label::new(format!("{source_label} · {}", middle_short(leaf, 18)))
            .semantic_name(format!(
                "Current {} path {}",
                files.source.label(),
                files.path
            ))
            .style(pal.text_style(&theme, 12.0, FontWeight::MEDIUM, pal.text))
    };
    let commands = {
        let s = shared.clone();
        IconButton::new(IconGlyph::MoreHorizontal, "More file commands")
            .size(30.0)
            .theme_when(clone_reader(reader))
            .selected_when({
                let s = s.clone();
                move || s.files().compact_overlay == Some(CompactFilesOverlay::Commands)
            })
            .on_press_with_ctx(move |ctx| {
                s.files_mut()
                    .open_compact_overlay(CompactFilesOverlay::Commands);
                request_files_overlay_rebuild(ctx);
            })
    };
    let inspector = {
        let s = shared.clone();
        IconButton::new(IconGlyph::FileText, "Open file inspector")
            .size(30.0)
            .theme_when(clone_reader(reader))
            .selected_when({
                let s = s.clone();
                move || s.files().compact_overlay == Some(CompactFilesOverlay::Inspector)
            })
            .on_press_with_ctx(move |ctx| {
                s.files_mut()
                    .open_compact_overlay(CompactFilesOverlay::Inspector);
                request_files_overlay_rebuild(ctx);
            })
    };

    Flex::horizontal()
        .main_gap(4.0)
        .align_items(Alignment::Center)
        .with_item(back, FlexItem::new().no_shrink())
        .with_item(up, FlexItem::new().no_shrink())
        .with_item(location, FlexItem::fill().min_width(72.0))
        .with_item(commands, FlexItem::new().no_shrink())
        .with_item(inspector, FlexItem::new().no_shrink())
}

// ---------------------------------------------------------------------------
// File listing — rebuilt on navigation by `RebuildOnChange`.
// ---------------------------------------------------------------------------

/// Build the file-list body for the current location: an error/empty placeholder, or a virtual
/// table whose pinned header owns sorting and whose rows support select/open interactions.
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
            reader,
            IconGlyph::Storage,
            format!("Loading {}", source.label()),
            match source {
                FilesSourceKind::Sifs => "Opening the distributed filesystem...",
                FilesSourceKind::Local => "Reading the local directory...",
            },
        );
        return placeholder_with_task_strip(shared, reader, body, has_tasks);
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
                    .min_width(190.0)
                    .theme_when(clone_reader(reader))
                    .appearance(sui::ButtonAppearance::Tonal)
                    .tone(SemanticTone::Neutral)
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
                    .min_width(180.0)
                    .theme_when(clone_reader(reader))
                    .appearance(sui::ButtonAppearance::Tonal)
                    .tone(SemanticTone::Neutral)
                    .on_press(move || {
                        let waker = { s.borrow().waker.clone() };
                        s.files_mut().switch_source(FilesSourceKind::Local, waker);
                    }),
            )
        } else {
            None
        };
        let mut primary_actions = Flex::horizontal()
            .wrap(FlexWrap::Wrap)
            .gap(8.0)
            .align_items(Alignment::Center)
            .with_child(retry);
        if let Some(settings) = settings {
            primary_actions = primary_actions.with_child(settings);
        }
        let mut action_height = 72.0;
        let mut actions = Stack::vertical()
            .spacing(8.0)
            .alignment(Alignment::Center)
            .with_child(primary_actions);
        if let Some(browse_local) = browse_local {
            actions = actions.with_child(
                SizedBox::new()
                    .width(200.0)
                    .height(32.0)
                    .with_child(browse_local),
            );
            action_height = 112.0;
        }
        let body = centered_note(
            reader,
            IconGlyph::Storage,
            format!("{} unavailable", source.label()),
            source_unavailable_summary(source, &error),
        )
        .action(actions)
        .action_height(action_height);
        return placeholder_with_task_strip(shared, reader, body, has_tasks);
    }
    if is_empty {
        let create_file = {
            let s = shared.clone();
            sui::Button::new("Create file")
                .icon(IconGlyph::File)
                .min_width(130.0)
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
                .min_width(150.0)
                .theme_when(clone_reader(reader))
                .on_press_with_ctx(move |ctx| {
                    s.files_mut()
                        .open_action_prompt(FileActionPromptKind::NewFolder);
                    request_files_overlay_rebuild(ctx);
                })
        };
        let actions = Flex::horizontal()
            .wrap(FlexWrap::Wrap)
            .gap(8.0)
            .align_items(Alignment::Center)
            .with_child(create_file)
            .with_child(create_folder);
        let body = centered_note(
            reader,
            IconGlyph::Folder,
            "Empty directory",
            "Nothing here yet.",
        )
        .action(actions)
        .action_height(72.0);
        return placeholder_with_task_strip(shared, reader, body, has_tasks);
    }

    let scroll = files_list_body_rebuild(shared, reader, scroll_state);
    if has_tasks {
        let top_h = LIST_TASK_STRIP_H;
        let top = Stack::vertical().alignment(Alignment::Stretch).with_child(
            SizedBox::new()
                .height(LIST_TASK_STRIP_H)
                .with_child(files_task_strip(shared, reader)),
        );
        WidgetPod::new(Dock::new(scroll).top(top_h, top))
    } else {
        WidgetPod::new(scroll)
    }
}

fn placeholder_with_task_strip<W>(
    shared: &Shared,
    reader: &ThemeReader,
    body: W,
    has_tasks: bool,
) -> WidgetPod
where
    W: Widget + 'static,
{
    if has_tasks {
        WidgetPod::new(
            Dock::new(body).top(
                LIST_TASK_STRIP_H,
                SizedBox::new()
                    .height(LIST_TASK_STRIP_H)
                    .with_child(files_task_strip(shared, reader)),
            ),
        )
    } else {
        WidgetPod::new(body)
    }
}

fn files_list_body_signature(shared: &Shared) -> (u64, FileSortMode, usize, usize) {
    let files = shared.files();
    (
        files.list_revision,
        files.sort_mode,
        files.entries.len(),
        files.entries_total_count,
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
        move |_key| files_list_body_table(&s, &r, &ss),
    )
}

fn files_list_body_table(
    shared: &Shared,
    reader: &ThemeReader,
    scroll_state: &ScrollState,
) -> WidgetPod {
    let _ = scroll_state;
    let (entries, source, sort_mode, total_count) = {
        let files = shared.files();
        (
            files.entries.clone(),
            files.source,
            files.sort_mode,
            files.entries_total_count,
        )
    };
    if entries.is_empty() {
        return WidgetPod::new(centered_note(
            reader,
            IconGlyph::Folder,
            "Empty directory",
            format!("No {} entries to show.", source.label()),
        ));
    }

    let row_entries = entries.clone();
    let paint_shared = shared.clone();
    let name_entries = entries.clone();
    let description_entries = entries.clone();
    let table = {
        let s = shared.clone();
        let cell_s = shared.clone();
        let header_s = shared.clone();
        let page_s = shared.clone();
        let description_s = shared.clone();
        VirtualTable::new("files-listing")
            .columns([
                VirtualTableColumn::new("").width(36.0),
                VirtualTableColumn::new("Name")
                    .min_width(180.0)
                    .sort_direction(sort_mode.name_sort_direction()),
                VirtualTableColumn::new("Placement").width(156.0),
                VirtualTableColumn::new("Kind / size")
                    .width(128.0)
                    .sort_direction(sort_mode.meta_sort_direction()),
            ])
            .row_count(entries.len())
            .theme_when(clone_reader(reader))
            .selected_when({
                let s = s.clone();
                move || {
                    s.files()
                        .selected
                        .filter(|index| *index < s.files().entries.len())
                }
            })
            .row_name(move |index| {
                name_entries
                    .get(index)
                    .map(|entry| entry.name.clone())
                    .unwrap_or_default()
            })
            .row_description(move |index| {
                let Some(entry) = description_entries.get(index) else {
                    return String::new();
                };
                let files = description_s.files();
                let kind = if entry.is_dir {
                    "Folder"
                } else if entry.is_link {
                    "Link"
                } else {
                    "File"
                };
                let mut description =
                    format!("{kind}, {} {}", files.source.path_label(), entry.path);
                if entry.is_dir {
                    description.push_str(". Single click selects; double click opens the folder");
                }
                if files.source == FilesSourceKind::Sifs && files.selected == Some(index) {
                    description.push_str(". Placement ");
                    description.push_str(&entry_row_meta(entry, &files, true));
                }
                description
            })
            .row_painter(move |ctx, row| {
                paint_file_table_row(ctx, &paint_shared, &row_entries, row);
            })
            .on_cell_activate(move |row_index, column_index, activation| {
                if column_index == 0 && activation == VirtualTableRowActivationKind::Single {
                    cell_s.files_mut().toggle_marked_entry(row_index);
                    true
                } else {
                    false
                }
            })
            .on_row_activate(move |index, activation| {
                s.files_mut().click_row(index, activation);
            })
            .on_header_activate(move |column| {
                let sort_column = match column {
                    1 => FilesSortColumn::Name,
                    3 => FilesSortColumn::KindSize,
                    _ => return,
                };
                header_s.files_mut().toggle_sort_column(sort_column);
            })
            .on_near_end(move || {
                if page_s.files().entries.len() < total_count {
                    let waker = { page_s.borrow().waker.clone() };
                    page_s.files_mut().request_more_entries(waker);
                }
            })
    };

    WidgetPod::new(table)
}

fn paint_file_table_row(
    ctx: &mut PaintCtx,
    shared: &Shared,
    entries: &[SifsEntry],
    row: &VirtualTableRowContext<'_>,
) {
    let Some(entry) = entries.get(row.row_index) else {
        return;
    };
    let theme = shared.theme();
    let pal = palette(shared);
    let files = shared.files();
    let active = files.selected == Some(row.row_index);
    let selected = files.entry_marked(row.row_index);
    let label_color = if active { pal.text } else { pal.text_muted };
    let icon_color = if active { pal.accent } else { pal.text_faint };

    let select_cell = row.column_rects.first().copied().unwrap_or(row.row_rect);
    let select_side = theme
        .metrics
        .checkbox_indicator_size
        .min(select_cell.height() - 6.0)
        .max(0.0);
    let select_rect = Rect::new(
        select_cell.x() + ((select_cell.width() - select_side) * 0.5).max(0.0),
        select_cell.y() + ((select_cell.height() - select_side) * 0.5).max(0.0),
        select_side,
        select_side,
    );
    paint_checkbox_indicator(
        ctx,
        &theme,
        select_rect,
        CheckboxIndicatorState::new(selected).hovered(row.hovered),
    );

    if let Some(name_cell) = row.column_rects.get(1).copied() {
        let label_width = (name_cell.width() - ROW_THUMBNAIL_SIDE - 22.0).max(0.0);
        let display_name = display_name_for_width(&entry.name, label_width, 7.1);
        paint_leading_label_cell(
            ctx,
            &theme,
            name_cell,
            &display_name,
            LeadingLabelCellPaint::new()
                .icon(entry_icon(entry))
                .icon_color(icon_color)
                .label_color(label_color)
                .label_weight(FontWeight::MEDIUM)
                .icon_slot_side(ROW_THUMBNAIL_SIDE)
                .icon_inset(2.0)
                .gap(8.0)
                .padding(6.0, 8.0),
        );
    }

    if let Some(placement_cell) = row.column_rects.get(2).copied() {
        let placement = file_row_placement_badge(entry, &files, row.row_index);
        paint_placement_badge_with(
            ctx,
            &theme,
            placement_cell,
            &placement.label,
            Some(placement.icon),
            placement.tone,
            placement.replicas,
            PlacementBadgePaint::new().padding(8.0, 5.0, 8.0, 5.0),
        );
    }

    if let Some(meta_cell) = row.column_rects.get(3).copied() {
        let meta_text = entry_row_meta(entry, &files, active);
        let display = display_name_for_width(&meta_text, (meta_cell.width() - 12.0).max(0.0), 6.2);
        paint_text_cell(
            ctx,
            &theme,
            meta_cell,
            &display,
            TextCellPaint::new()
                .text_token(theme.text.xs)
                .color(pal.text_faint)
                .padding(8.0, 8.0),
        );
    }
}

/// A centered empty/error placeholder themed off the chrome palette.
fn centered_note(
    reader: &ThemeReader,
    icon: IconGlyph,
    title: impl Into<String>,
    subtitle: impl Into<String>,
) -> EmptyState {
    EmptyState::new(title, subtitle)
        .icon(icon)
        .theme_when(clone_reader(reader))
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

fn sifs_health_has_attention(health: &SifsHealthSummary) -> bool {
    health.open_conflict_count > 0
        || health.pending_repair_count > 0
        || health.failed_namespace_job_count > 0
}

fn sifs_health_has_activity(health: &SifsHealthSummary) -> bool {
    health.pending_write_count > 0 || health.pending_namespace_job_count > 0
}

fn sifs_health_status_detail(health: &SifsHealthSummary) -> String {
    let mut parts = Vec::new();
    if health.open_conflict_count > 0 {
        parts.push(format!(
            "{} {}",
            health.open_conflict_count,
            if health.open_conflict_count == 1 {
                "conflict"
            } else {
                "conflicts"
            }
        ));
    }
    if health.pending_repair_count > 0 {
        parts.push(format!(
            "{} {}",
            health.pending_repair_count,
            if health.pending_repair_count == 1 {
                "repair"
            } else {
                "repairs"
            }
        ));
    }
    if health.failed_namespace_job_count > 0 {
        parts.push(format!(
            "{} failed namespace {}",
            health.failed_namespace_job_count,
            if health.failed_namespace_job_count == 1 {
                "job"
            } else {
                "jobs"
            }
        ));
    }
    if health.pending_write_count > 0 {
        parts.push(format!(
            "{} pending {}",
            health.pending_write_count,
            if health.pending_write_count == 1 {
                "write"
            } else {
                "writes"
            }
        ));
    }
    if health.pending_namespace_job_count > 0 {
        parts.push(format!(
            "{} namespace {}",
            health.pending_namespace_job_count,
            if health.pending_namespace_job_count == 1 {
                "job"
            } else {
                "jobs"
            }
        ));
    }
    if parts.is_empty() {
        format!(
            "DFS healthy; {} objects, {} shards",
            health.object_count, health.metadata_shard_count
        )
    } else {
        format!("DFS {}", parts.join(", "))
    }
}

fn sifs_health_error_status_detail(error: &str) -> String {
    let detail = error.split_whitespace().collect::<Vec<_>>().join(" ");
    if detail.is_empty() {
        "DFS health unavailable".to_string()
    } else {
        format!("DFS health unavailable: {detail}")
    }
}

fn sifs_health_status_summary(health: &SifsHealthSummary) -> String {
    if sifs_health_has_attention(health) {
        return "Needs attention".to_string();
    }
    if sifs_health_has_activity(health) {
        return "Syncing".to_string();
    }
    "Healthy".to_string()
}

fn sifs_health_error_status_summary(_error: &str) -> String {
    "Health unavailable; open Connection or Cluster".to_string()
}

fn files_source_status(files: &FilesState) -> &'static str {
    match files.source {
        FilesSourceKind::Sifs if files.error.is_some() => "SIFS unavailable",
        FilesSourceKind::Sifs if files.loading => "Loading SIFS",
        FilesSourceKind::Sifs if files.sifs_health_error.is_some() => "SIFS unknown",
        FilesSourceKind::Sifs
            if files
                .sifs_health
                .as_ref()
                .is_some_and(sifs_health_has_attention) =>
        {
            "SIFS degraded"
        }
        FilesSourceKind::Sifs
            if files
                .sifs_health
                .as_ref()
                .is_some_and(sifs_health_has_activity) =>
        {
            "SIFS syncing"
        }
        FilesSourceKind::Sifs if files_sifs_daemon_is_remote(files) => "Remote SIFS",
        FilesSourceKind::Sifs if files.clustered => "SIFS online",
        FilesSourceKind::Sifs => "Local SIFS",
        FilesSourceKind::Local if files.error.is_some() => "Local unavailable",
        FilesSourceKind::Local if files.loading => "Loading local",
        FilesSourceKind::Local => "Local files",
    }
}

fn files_source_tone(files: &FilesState) -> SemanticTone {
    match files.source {
        FilesSourceKind::Sifs if files.error.is_some() => SemanticTone::Warning,
        FilesSourceKind::Sifs if files.loading => SemanticTone::Accent,
        FilesSourceKind::Sifs if files.sifs_health_error.is_some() => SemanticTone::Warning,
        FilesSourceKind::Sifs
            if files
                .sifs_health
                .as_ref()
                .is_some_and(sifs_health_has_attention) =>
        {
            SemanticTone::Warning
        }
        FilesSourceKind::Sifs
            if files
                .sifs_health
                .as_ref()
                .is_some_and(sifs_health_has_activity) =>
        {
            SemanticTone::Accent
        }
        FilesSourceKind::Sifs if files.clustered || files_sifs_daemon_is_remote(files) => {
            SemanticTone::Success
        }
        FilesSourceKind::Local if files.error.is_some() => SemanticTone::Warning,
        FilesSourceKind::Local if files.loading => SemanticTone::Accent,
        FilesSourceKind::Local => SemanticTone::Success,
        FilesSourceKind::Sifs => SemanticTone::Neutral,
    }
}

fn files_sifs_connection_detail(files: &FilesState) -> Option<String> {
    if files.source != FilesSourceKind::Sifs {
        return None;
    }
    files.daemon_address.as_deref().map(|address| {
        let label = if daemon_address_is_remote(address) {
            "remote daemon"
        } else {
            "daemon"
        };
        format!("{label} {}", middle_short(address, 44))
    })
}

fn files_sifs_daemon_is_remote(files: &FilesState) -> bool {
    files
        .daemon_address
        .as_deref()
        .is_some_and(daemon_address_is_remote)
}

fn daemon_address_is_remote(address: &str) -> bool {
    let host = daemon_address_host(address);
    !matches!(host, "" | "localhost" | "::1") && !host.starts_with("127.") && host != "0.0.0.0"
}

fn daemon_address_host(address: &str) -> &str {
    let trimmed = address.trim();
    if let Some(rest) = trimmed.strip_prefix('[')
        && let Some((host, _)) = rest.split_once(']')
    {
        return host;
    }
    trimmed
        .rsplit_once(':')
        .map(|(host, _)| host.trim_matches(['[', ']']))
        .unwrap_or(trimmed)
}

fn files_entry_count_status(files: &FilesState) -> String {
    if files.loading {
        "loading directory".to_string()
    } else if files.error.is_some() {
        "listing unavailable".to_string()
    } else if files.entries_total_count > files.entries.len() {
        format!(
            "{}/{} entries loaded",
            files.entries.len(),
            files.entries_total_count
        )
    } else {
        plural(files.entries.len(), "entry")
    }
}

fn files_selection_status(files: &FilesState) -> String {
    let marked = files.marked_count();
    if marked > 0 {
        return if marked == 1 {
            "1 entry selected".to_string()
        } else {
            format!("{marked} entries selected")
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
            "1 selected".to_string()
        } else {
            format!("{marked} selected")
        };
    }
    files
        .selected_entry()
        .map(|entry| middle_short(&entry.name, 34))
        .unwrap_or_else(|| "no selection".to_string())
}

fn files_task_status(files: &FilesState) -> String {
    if files.external_file_hovered {
        return "drop to import".to_string();
    }
    if files.picker_lane.is_pending() {
        return "picker open".to_string();
    }
    if !files.pending_import_request_ids.is_empty() {
        return "preparing import".to_string();
    }
    if files.picker_error.is_some() {
        return "import needs attention".to_string();
    }
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
        .map(|task| {
            format!(
                "{:?}:{}:{}:{:?}:{:.2}:{}",
                task.source,
                task.id,
                task.title,
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
    let mut summary = files_source_status(files).to_string();
    summary.push_str(&format!(
        " | Path {} | {} | {} | {}",
        files.path,
        files_entry_count_status(files),
        files_selection_status(files),
        files_task_status(files),
    ));
    summary
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
        "Working folder",
        "app working dir",
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
        "1 selected entry".to_string()
    } else {
        format!("{count} selected entries")
    }
}

fn files_status_bar(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let badge_label = {
        let s = shared.clone();
        move || files_source_status(&s.files()).to_string()
    };
    let badge_tone = {
        let s = shared.clone();
        move || files_source_tone(&s.files())
    };
    let path_summary = {
        let s = shared.clone();
        move || {
            let files = s.files();
            let selected = files_status_bar_selection(&files);
            format!(
                "{} | {} | {}",
                middle_short(&files.path, 64),
                files_entry_count_status(&files),
                selected
            )
        }
    };
    let task_summary = {
        let s = shared.clone();
        move || files_task_status(&s.files())
    };
    let accessibility_summary = {
        let s = shared.clone();
        move || files_status_summary(&s.files())
    };
    let status_theme = clone_reader(reader);
    let body = Background::new(
        Color::TRANSPARENT,
        Stack::vertical()
            .alignment(Alignment::Stretch)
            .with_child(Separator::horizontal().theme_when(clone_reader(reader)))
            .with_child(
                SizedBox::new().height(FILES_STATUS_H - 1.0).with_child(
                    Flex::horizontal()
                        .gap(8.0)
                        .align_items(Alignment::Center)
                        .with_child(
                            SizedBox::new().width(146.0).with_child(
                                StatusBadge::dynamic("SIFS unknown", badge_label)
                                    .tone_when(badge_tone)
                                    .icon(IconGlyph::Storage)
                                    .theme_when(clone_reader(reader)),
                            ),
                        )
                        .with_item(
                            StatusBar::new()
                                .name("Files status")
                                .description_when(accessibility_summary)
                                .height(FILES_STATUS_H - 1.0)
                                .theme_when(status_theme)
                                .segment(
                                    StatusBarSegment::dynamic("Path /", path_summary).expand(true),
                                )
                                .segment(
                                    StatusBarSegment::dynamic("idle", task_summary)
                                        .min_width(144.0),
                                ),
                            FlexItem::flex(1.0).min_width(0.0),
                        ),
                ),
            ),
    )
    .brush_when({
        let s = shared.clone();
        move || palette(&s).sidebar
    });

    SizedBox::new().height(FILES_STATUS_H).with_child(body)
}

fn files_places_signature(
    shared: &Shared,
) -> (FilesSourceKind, String, String, String, String, String) {
    let files = shared.files();
    (
        files.source,
        files.path.clone(),
        files.sifs_path.clone(),
        files.local_path.clone(),
        files.node_id.clone(),
        files.data_dir.root().to_string_lossy().into_owned(),
    )
}

fn files_places_content_height(count: usize) -> f32 {
    48.0 + count as f32 * PLACE_ROW_H + 8.0
}

fn files_places_rail(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let s = shared.clone();
    let r = Rc::clone(reader);
    RebuildOnChange::new(
        {
            let s = s.clone();
            move || files_places_signature(&s)
        },
        move |_key| files_places_rail_rebuild(&s, &r),
    )
}

fn files_places_rail_rebuild(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let places = {
        let files = shared.files();
        files_places(&files)
    };
    let content_height = files_places_content_height(places.len());
    let selection_places = places.clone();
    let action_places = places.clone();
    let selection_shared = shared.clone();
    let action_shared = shared.clone();
    let items = places
        .iter()
        .map(|place| {
            ListItem::new(place.label.clone())
                .detail(place.detail.clone())
                .leading_icon(place.glyph)
        })
        .collect::<Vec<_>>();

    let list = ListView::new("Files places")
        .items(items)
        .row_height(PLACE_ROW_H)
        .theme_when(clone_reader(reader))
        .selected_when(move || {
            let files = selection_shared.files();
            selection_places
                .iter()
                .position(|place| file_place_active(&files, place))
        })
        .on_change_with_ctx(move |index, _label, ctx| {
            if let Some(place) = action_places.get(index).cloned() {
                open_file_place(&action_shared, place, ctx);
            }
        });

    let header = SizedBox::new().height(36.0).with_child(
        WidgetPadding::horizontal(
            CONTENT_PAD,
            Flex::horizontal()
                .align_items(Alignment::Center)
                .with_child(SectionLabel::new("Places").theme_when(clone_reader(reader))),
        )
        .fill_child_height(),
    );

    WidgetPod::new(SizedBox::new().height(content_height).with_child(
        Background::new(Color::TRANSPARENT, Dock::new(list).top(36.0, header)).brush_when({
            let s = shared.clone();
            move || palette(&s).sidebar
        }),
    ))
}

fn task_strip_has_recovery(task: &FilesTask) -> bool {
    task.source == FilesSourceKind::Sifs && task.state == FilesTaskState::Failed
}

fn task_strip_hidden_task_count(tasks: &[FilesTask]) -> usize {
    tasks.len().saturating_sub(2)
}

fn task_strip_overflow_label(hidden: usize) -> String {
    if hidden == 1 {
        "1 more in inspector".to_string()
    } else {
        format!("{hidden} more in inspector")
    }
}

fn task_strip_summary(tasks: &[FilesTask]) -> String {
    let base = files_task_queue_summary(tasks);
    let hidden = task_strip_hidden_task_count(tasks);
    if hidden == 0 {
        return base;
    }
    format!(
        "{base}; showing 2 of {}; {} in inspector",
        tasks.len(),
        plural(hidden, "more task")
    )
}

fn task_strip_detail_display(task: &FilesTask) -> String {
    let detail = files_task_detail_display(task);
    if task_strip_has_recovery(task) {
        detail
            .strip_suffix(" in Connection")
            .unwrap_or(detail.as_str())
            .to_string()
    } else {
        detail
    }
}

fn task_strip_button_padding() -> sui::Insets {
    sui::Insets {
        left: 8.0,
        right: 8.0,
        top: 2.0,
        bottom: 2.0,
    }
}

fn task_strip_clear_button(shared: &Shared, reader: &ThemeReader) -> sui::Button {
    let s = shared.clone();
    sui::Button::new("Clear finished")
        .semantic_name("Clear finished tasks")
        .description("Dismiss completed, failed, and cancelled file operations")
        .min_width(108.0)
        .min_height(22.0)
        .padding(task_strip_button_padding())
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .on_press_with_ctx(move |ctx| {
            s.files_mut().clear_finished_tasks();
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_paint();
            ctx.request_semantics();
        })
}

fn task_strip_cancel_button(
    shared: &Shared,
    reader: &ThemeReader,
    task: &FilesTask,
) -> sui::Button {
    let s = shared.clone();
    let task_id = task.id;
    sui::Button::new("Cancel")
        .semantic_name(format!("Cancel {}", task.title))
        .description(task.detail.clone())
        .min_width(58.0)
        .min_height(22.0)
        .padding(task_strip_button_padding())
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .on_press_with_ctx(move |ctx| {
            s.files_mut().cancel_task(task_id);
            ctx.request_paint();
            ctx.request_semantics();
        })
}

fn task_strip_recovery_button(
    shared: &Shared,
    reader: &ThemeReader,
    task: &FilesTask,
) -> sui::Button {
    let s = shared.clone();
    sui::Button::new("Connection")
        .icon(IconGlyph::Alert)
        .icon_size(12.0)
        .semantic_name(format!("Open Connection settings for {}", task.title))
        .description(format!("{}: {}", task.title, task.detail))
        .min_width(92.0)
        .min_height(22.0)
        .padding(task_strip_button_padding())
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .on_press_with_ctx(move |ctx| {
            open_settings_section(&s, SettingsSection::Connection);
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_paint();
            ctx.request_semantics();
        })
}

fn task_strip_action_slot(
    shared: &Shared,
    reader: &ThemeReader,
    task: &FilesTask,
) -> (SwitchView, f32) {
    let action_index = if task.state.can_cancel() {
        1
    } else if task_strip_has_recovery(task) {
        2
    } else {
        0
    };
    let width = match action_index {
        1 => 58.0,
        2 => 92.0,
        _ => 0.0,
    };
    let slot = SwitchView::new()
        .selected(action_index)
        .with_child(SizedBox::new())
        .with_child(task_strip_cancel_button(shared, reader, task))
        .with_child(task_strip_recovery_button(shared, reader, task));
    (slot, width)
}

fn task_strip_row(shared: &Shared, reader: &ThemeReader, task: &FilesTask) -> impl Widget + use<> {
    let theme = reader();
    let pal = Palette::from_theme(&theme, widgets::theme_is_dark(shared));
    let title_style = pal.text_style(&theme, 12.0, FontWeight::MEDIUM, pal.text);
    let detail_style = pal.text_style(&theme, 11.0, FontWeight::NORMAL, pal.text_faint);
    let title = format!("{} · {}", task.title, task.state.label());
    let detail = task_strip_detail_display(task);
    let progress = task_progress_fraction(task);
    let tone = task_progress_tone(task);
    let (action, action_w) = task_strip_action_slot(shared, reader, task);

    let body = Flex::vertical()
        .main_gap(1.0)
        .align_items(Alignment::Stretch)
        .with_child(Label::new(title).style(title_style))
        .with_child(Label::new(detail).style(detail_style))
        .with_child(
            SizedBox::new().height(3.0).with_child(
                sui::ProgressBar::new(format!("{} progress", task.title))
                    .value(progress as f64)
                    .tone(tone)
                    .height(3.0)
                    .min_width(80.0)
                    .theme_when(clone_reader(reader)),
            ),
        );

    TrailingSlotRow::new(body, action)
        .trailing_width(action_w)
        .trailing_height(22.0)
        .gap(8.0)
}

fn task_strip_content(
    shared: &Shared,
    reader: &ThemeReader,
    tasks: &[FilesTask],
) -> impl Widget + use<> {
    let theme = reader();
    let pal = Palette::from_theme(&theme, widgets::theme_is_dark(shared));
    let overflow_style = pal.text_style(&theme, 11.0, FontWeight::MEDIUM, pal.text_muted);
    let hidden = task_strip_hidden_task_count(tasks);
    let has_finished = files_tasks_have_finished(tasks);

    let mut header_body = Flex::horizontal()
        .main_gap(8.0)
        .align_items(Alignment::Center)
        .with_child(SectionLabel::new("Tasks").theme_when(clone_reader(reader)));
    if hidden > 0 {
        header_body = header_body.with_child(
            Label::new(task_strip_overflow_label(hidden))
                .style(overflow_style)
                .semantic_name(task_strip_overflow_label(hidden)),
        );
    }

    let clear_slot = SwitchView::new()
        .selected(if has_finished { 1 } else { 0 })
        .with_child(SizedBox::new())
        .with_child(task_strip_clear_button(shared, reader));
    let header = SizedBox::new().height(24.0).with_child(
        TrailingSlotRow::new(header_body, clear_slot)
            .trailing_width(if has_finished { 108.0 } else { 0.0 })
            .trailing_height(22.0)
            .gap(8.0),
    );

    let mut rows = Flex::vertical()
        .main_gap(4.0)
        .align_items(Alignment::Stretch)
        .with_child(header);
    for task in tasks.iter().take(2) {
        rows = rows.with_child(
            SizedBox::new()
                .height(27.0)
                .with_child(task_strip_row(shared, reader, task)),
        );
    }

    let s = shared.clone();
    let content = WidgetPadding::symmetric(CONTENT_PAD, 6.0, rows).fill_child_width();
    let panel =
        Background::new(Color::TRANSPARENT, content).brush_when(move || palette(&s).sidebar);
    Stack::vertical()
        .alignment(Alignment::Stretch)
        .with_child(
            SizedBox::new()
                .height(LIST_TASK_STRIP_H - 1.0)
                .with_child(panel),
        )
        .with_child(Separator::horizontal().theme_when(clone_reader(reader)))
}

fn files_task_strip(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let tasks = shared.files().tasks.clone();
    let summary = task_strip_summary(&tasks);
    SizedBox::new().height(LIST_TASK_STRIP_H).with_child(
        SemanticRegion::new(
            "File task strip",
            task_strip_content(shared, reader, &tasks),
        )
        .role(SemanticsRole::Text)
        .description(summary),
    )
}

struct InspectorTaskQueue {
    shared: Shared,
    reader: ThemeReader,
    signature: String,
    child: WidgetPod,
}

impl InspectorTaskQueue {
    fn new(shared: Shared, reader: &ThemeReader) -> Self {
        let (signature, child) = inspector_task_queue_snapshot(&shared, reader);
        Self {
            shared,
            reader: Rc::clone(reader),
            signature,
            child,
        }
    }

    fn refresh(&mut self) -> bool {
        let signature = {
            let files = self.shared.files();
            files_tasks_signature(&files.tasks)
        };
        if signature == self.signature {
            return false;
        }

        let (signature, child) = inspector_task_queue_snapshot(&self.shared, &self.reader);
        self.signature = signature;
        self.child = child;
        true
    }
}

fn inspector_task_queue_snapshot(shared: &Shared, reader: &ThemeReader) -> (String, WidgetPod) {
    let tasks = shared.files().tasks.clone();
    let signature = files_tasks_signature(&tasks);
    let summary = files_task_queue_summary(&tasks);
    let child = WidgetPod::new(
        SemanticRegion::new(
            "File task queue",
            inspector_task_queue_content(shared, reader, &tasks),
        )
        .role(SemanticsRole::Text)
        .description(summary),
    );
    (signature, child)
}

fn inspector_task_clear_button(shared: &Shared, reader: &ThemeReader) -> sui::Button {
    let s = shared.clone();
    sui::Button::new("Clear finished")
        .semantic_name("Clear finished file tasks")
        .description("Dismiss completed, failed, and cancelled file operations")
        .min_width(108.0)
        .min_height(22.0)
        .padding(task_strip_button_padding())
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .on_press_with_ctx(move |ctx| {
            s.files_mut().clear_finished_tasks();
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_paint();
            ctx.request_semantics();
        })
}

fn inspector_task_queue_content(
    shared: &Shared,
    reader: &ThemeReader,
    tasks: &[FilesTask],
) -> impl Widget + use<> {
    let theme = reader();
    let pal = Palette::from_theme(&theme, widgets::theme_is_dark(shared));
    let summary_style = pal.text_style(&theme, 11.0, FontWeight::MEDIUM, pal.text_muted);
    let has_finished = files_tasks_have_finished(tasks);

    let header_body = Flex::vertical()
        .main_gap(1.0)
        .align_items(Alignment::Stretch)
        .with_child(SectionLabel::new("File tasks").theme_when(clone_reader(reader)))
        .with_child(Label::new(files_task_queue_summary(tasks)).style(summary_style));
    let clear_slot = SwitchView::new()
        .selected(if has_finished { 1 } else { 0 })
        .with_child(SizedBox::new())
        .with_child(inspector_task_clear_button(shared, reader));
    let header = TrailingSlotRow::new(header_body, clear_slot)
        .trailing_width(if has_finished { 108.0 } else { 0.0 })
        .trailing_height(22.0)
        .gap(8.0);

    let mut rows = Flex::vertical()
        .main_gap(8.0)
        .align_items(Alignment::Stretch)
        .with_child(header);

    if tasks.is_empty() {
        let idle_body = Flex::vertical()
            .main_gap(8.0)
            .align_items(Alignment::Stretch)
            .with_child(
                SizedBox::new().height(24.0).with_child(
                    StatusBadge::new("Idle")
                        .tone(SemanticTone::Success)
                        .min_width(68.0)
                        .theme_when(clone_reader(reader)),
                ),
            )
            .with_child(
                Label::new("No active file operations").style(pal.text_style(
                    &theme,
                    12.0,
                    FontWeight::MEDIUM,
                    pal.text,
                )),
            )
            .with_child(Label::new("Worker queue empty").style(pal.text_style(
                &theme,
                11.0,
                FontWeight::NORMAL,
                pal.text_faint,
            )));
        rows = rows.with_child(idle_body);
    } else {
        for task in tasks {
            rows = rows.with_child(
                SizedBox::new()
                    .height(31.0)
                    .with_child(task_strip_row(shared, reader, task)),
            );
        }
    }

    Surface::panel(rows)
        .padding(sui::Insets {
            left: 12.0,
            right: 12.0,
            top: 10.0,
            bottom: 12.0,
        })
        .fill_width()
        .theme_when(clone_reader(reader))
}

impl Widget for InspectorTaskQueue {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        let pointer_up = matches!(
            event,
            Event::Pointer(pointer) if matches!(pointer.kind, PointerEventKind::Up)
        );
        let redraw = matches!(event, Event::Window(WindowEvent::RedrawRequested));
        if (pointer_up || redraw) && self.refresh() {
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_paint();
            ctx.request_semantics();
        }
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
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

fn task_progress_tone(task: &FilesTask) -> SemanticTone {
    match task.state {
        FilesTaskState::Running | FilesTaskState::CancelRequested => SemanticTone::Accent,
        FilesTaskState::Completed | FilesTaskState::Cancelled => SemanticTone::Success,
        FilesTaskState::Failed => SemanticTone::Warning,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RootInspectorAction {
    RefreshFolder,
    RepairReplicas,
    OpenCluster,
    ConnectionSettings,
}

impl RootInspectorAction {
    fn label(self) -> &'static str {
        match self {
            Self::RefreshFolder => "Refresh folder",
            Self::RepairReplicas => "Repair replicas",
            Self::OpenCluster => "Open Cluster",
            Self::ConnectionSettings => "Connection settings",
        }
    }

    fn glyph(self) -> IconGlyph {
        match self {
            Self::RefreshFolder => IconGlyph::Redo,
            Self::RepairReplicas => IconGlyph::Redo,
            Self::OpenCluster => IconGlyph::Storage,
            Self::ConnectionSettings => IconGlyph::Alert,
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::RefreshFolder => "Reload the current file listing and SIFS health summary",
            Self::RepairReplicas => "Request SIFS replica repair for the current location",
            Self::OpenCluster => "Open Cluster health and routing diagnostics",
            Self::ConnectionSettings => "Open Connection settings for daemon and SIFS recovery",
        }
    }
}

fn unselected_inspector_actions(files: &FilesState) -> Vec<RootInspectorAction> {
    if files.loading || files.error.is_some() || files.selected_entry().is_some() {
        return Vec::new();
    }

    let mut actions = vec![RootInspectorAction::RefreshFolder];
    if files.source == FilesSourceKind::Sifs {
        if files
            .sifs_health
            .as_ref()
            .is_some_and(|health| health.pending_repair_count > 0)
        {
            actions.push(RootInspectorAction::RepairReplicas);
        }
        if files.sifs_health.as_ref().is_some_and(|health| {
            sifs_health_has_attention(health) || sifs_health_has_activity(health)
        }) {
            actions.push(RootInspectorAction::OpenCluster);
        }
        if files.sifs_health_error.is_some() {
            actions.push(RootInspectorAction::ConnectionSettings);
        }
    }
    actions
}

fn open_cluster_tab(shared: &Shared) {
    shared.borrow_mut().active_tab = Tab::Cluster;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InspectorAction {
    OpenFolder,
    ViewFile,
    AttachToChat,
    Copy,
    CopyIdentifiers,
    CopyToSifs,
    CopyToLocal,
    Move,
    Rename,
    Delete,
    RefreshPlacement,
    RepairReplicas,
    MakeMetadataPrimary,
    PinMetadata,
    UnpinMetadata,
    PinContent,
    UnpinContent,
    ConnectionSettings,
}

impl InspectorAction {
    fn label(self) -> &'static str {
        match self {
            Self::OpenFolder => "Open folder",
            Self::ViewFile => "View or edit",
            Self::AttachToChat => "Attach to current chat",
            Self::Copy => "Copy",
            Self::CopyIdentifiers => "Copy IDs",
            Self::CopyToSifs => "Copy to SIFS",
            Self::CopyToLocal => "Download",
            Self::Move => "Move",
            Self::Rename => "Rename",
            Self::Delete => "Delete",
            Self::RefreshPlacement => "Refresh placement",
            Self::RepairReplicas => "Repair replicas",
            Self::MakeMetadataPrimary => "Make metadata primary",
            Self::PinMetadata => "Pin metadata",
            Self::UnpinMetadata => "Unpin metadata",
            Self::PinContent => "Pin content",
            Self::UnpinContent => "Unpin content",
            Self::ConnectionSettings => "Connection settings",
        }
    }

    fn glyph(self) -> IconGlyph {
        match self {
            Self::OpenFolder => IconGlyph::Folder,
            Self::ViewFile => IconGlyph::FileText,
            Self::AttachToChat => IconGlyph::Attach,
            Self::Copy | Self::CopyIdentifiers => IconGlyph::Restore,
            Self::CopyToSifs => IconGlyph::Storage,
            Self::CopyToLocal => IconGlyph::Download,
            Self::Move => IconGlyph::ChevronRight,
            Self::Rename => IconGlyph::MoreHorizontal,
            Self::Delete => IconGlyph::Trash,
            Self::RefreshPlacement => IconGlyph::Storage,
            Self::RepairReplicas => IconGlyph::Redo,
            Self::MakeMetadataPrimary => IconGlyph::ChevronUp,
            Self::PinMetadata => IconGlyph::Lock,
            Self::UnpinMetadata => IconGlyph::Unlock,
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
            | Self::ViewFile
            | Self::AttachToChat
            | Self::CopyIdentifiers
            | Self::RefreshPlacement
            | Self::RepairReplicas
            | Self::MakeMetadataPrimary
            | Self::PinMetadata
            | Self::UnpinMetadata
            | Self::PinContent
            | Self::UnpinContent
            | Self::ConnectionSettings => None,
        }
    }

    fn semantic_name(self, entry: &SifsEntry) -> String {
        match self {
            Self::OpenFolder => "Open folder".to_string(),
            Self::ViewFile => format!("View or edit {}", entry.name),
            Self::AttachToChat => format!("Attach {} to current chat", entry.name),
            Self::RefreshPlacement => format!("Refresh placement {}", entry.name),
            Self::RepairReplicas => format!("Repair replicas {}", entry.name),
            Self::MakeMetadataPrimary => format!("Make metadata primary {}", entry.name),
            Self::PinMetadata => format!("Pin metadata {}", entry.name),
            Self::UnpinMetadata => format!("Unpin metadata {}", entry.name),
            Self::PinContent => format!("Pin content {}", entry.name),
            Self::UnpinContent => format!("Unpin content {}", entry.name),
            Self::ConnectionSettings => "Connection settings".to_string(),
            Self::CopyIdentifiers => format!("Copy full identifiers for {}", entry.name),
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
    } else {
        actions.push(InspectorAction::ViewFile);
        actions.push(InspectorAction::AttachToChat);
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
        let metadata_needs_connection = files
            .details_error
            .as_deref()
            .is_some_and(sifs_metadata_error_needs_connection);
        if metadata_needs_connection {
            actions.push(InspectorAction::ConnectionSettings);
        }
        actions.push(InspectorAction::RefreshPlacement);
        if !metadata_needs_connection && !files.details_loading {
            if let Some(details) = files.selected_details.as_ref() {
                actions.push(InspectorAction::CopyIdentifiers);
                actions.push(InspectorAction::RepairReplicas);
                if sifs_can_make_metadata_primary(details) {
                    actions.push(InspectorAction::MakeMetadataPrimary);
                }
                if sifs_has_local_pin(details, "metadata") {
                    actions.push(InspectorAction::UnpinMetadata);
                } else {
                    actions.push(InspectorAction::PinMetadata);
                }
                if !entry.is_dir && !entry.is_link {
                    if sifs_has_local_pin(details, "content") {
                        actions.push(InspectorAction::UnpinContent);
                    } else {
                        actions.push(InspectorAction::PinContent);
                    }
                }
            }
        }
    }
    actions
}

fn sifs_identifiers_for_clipboard(details: &SifsObjectDetails) -> String {
    let mut lines = vec![
        format!("file_system_id: {}", details.file_system_id),
        format!("object_id: {}", details.object_id),
        format!("version: {}", details.version),
    ];
    if let Some(hash) = details
        .content_hash
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        lines.push(format!("content_hash: {hash}"));
    }
    lines.join("\n")
}

fn sifs_loading_rows(
    path: &str,
    details_open: bool,
    slow_after_secs: Option<u64>,
) -> Vec<(&'static str, String)> {
    let mut rows = vec![(
        "Status",
        if slow_after_secs.is_some() {
            "SIFS is still loading".to_string()
        } else {
            "Connecting to SIFS".to_string()
        },
    )];
    if let Some(seconds) = slow_after_secs {
        rows.push((
            "Wait",
            format!("Directory listing has been running for at least {seconds} seconds."),
        ));
    }
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
        "Hide details"
    } else {
        "Show details"
    }
}

// ---------------------------------------------------------------------------
// Composed inspector — stock controls own generic chrome, focus, and semantics.
// ---------------------------------------------------------------------------

/// The interactive Files inspector is rebuilt from a compact state signature. Generic cards,
/// fields, badges, and actions are composed from SUI widgets; only the file-list and placement
/// data remain app-owned.
struct InspectorView {
    child: WidgetPod,
}

impl InspectorView {
    fn new(shared: Shared) -> Self {
        let reader: ThemeReader = {
            let s = shared.clone();
            Rc::new(move || s.theme().with_size(ControlSize::Small))
        };
        let key_shared = shared.clone();
        let build_shared = shared.clone();
        let build_reader = Rc::clone(&reader);
        let child = RebuildOnChange::new(
            move || inspector_summary(&key_shared),
            move |_key| composed_inspector_content(&build_shared, &build_reader),
        );
        Self {
            child: WidgetPod::new(child),
        }
    }
}

impl Widget for InspectorView {
    fn event(&mut self, _ctx: &mut EventCtx, _event: &Event) {}

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
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

fn inspector_summary(shared: &Shared) -> String {
    let files = shared.files();
    if files.loading {
        return format!(
            "loading|{:?}|{}|{}|{:?}",
            files.source, files.path, files.inspector_details_open, files.list_slow_after_secs
        );
    }
    if let Some(error) = &files.error {
        return format!(
            "error|{:?}|{}|{}|{}",
            files.source, files.path, error, files.inspector_details_open
        );
    }
    match files.selected_entry() {
        Some(entry) => format!(
            "{:?}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
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
            files_preview_signature(files.selected_preview.as_ref())
        ),
        None => format!(
            "unselected|{:?}|{}|{}|{}|{}|{}",
            files.source,
            files.path,
            files.clustered,
            files.entries.len(),
            files.inspector_details_open,
            unselected_inspector_health_summary(&files).unwrap_or_default()
        ),
    }
}

fn detail_rows_widget(
    rows: impl IntoIterator<Item = (&'static str, String)>,
    reader: &ThemeReader,
) -> Stack {
    let mut body = Stack::vertical().alignment(Alignment::Stretch).spacing(4.0);
    for (label, value) in rows {
        body = body.with_child(
            DetailRow::new(label, value)
                .theme_when(clone_reader(reader))
                .max_value_lines(6),
        );
    }
    body
}

fn inspector_card<W>(
    title: impl Into<String>,
    child: W,
    reader: &ThemeReader,
    header_action: Option<sui::Button>,
    prominent_title: bool,
) -> Surface
where
    W: Widget + 'static,
{
    let theme = reader();
    let mut section = PanelSection::new(title, child)
        .theme_when(clone_reader(reader))
        .gap(10.0);
    if prominent_title {
        let mut title_style = theme.text_style(theme.palette.text);
        title_style.font_size = theme.text.xl.size;
        title_style.line_height = theme.text.xl.line_height;
        title_style.weight = FontWeight::SEMIBOLD;
        section = section.title_style(title_style);
    }
    if let Some(action) = header_action {
        section = section.header_action(action);
    }
    Surface::panel(section)
        .theme_when(clone_reader(reader))
        .padding(Insets {
            left: 12.0,
            top: 10.0,
            right: 12.0,
            bottom: 12.0,
        })
        .fill_width()
}

fn inspector_details_button(
    shared: &Shared,
    reader: &ThemeReader,
    details_open: bool,
) -> sui::Button {
    let label = inspector_detail_button_label(details_open);
    let s = shared.clone();
    sui::Button::new(label)
        .semantic_name(label)
        .description("File storage metadata and diagnostics")
        .icon(if details_open {
            IconGlyph::ChevronUp
        } else {
            IconGlyph::ChevronDown
        })
        .appearance(ButtonAppearance::Ghost)
        .tone(SemanticTone::Accent)
        .min_height(24.0)
        .theme_when(clone_reader(reader))
        .on_press_with_ctx(move |ctx| {
            let open = s.files().inspector_details_open;
            s.files_mut().inspector_details_open = !open;
            request_files_overlay_rebuild(ctx);
        })
}

fn root_inspector_buttons(
    shared: &Shared,
    reader: &ThemeReader,
    actions: Vec<RootInspectorAction>,
) -> Flex {
    let mut row = Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .gap(8.0)
        .align_items(Alignment::Center);
    for action in actions {
        let s = shared.clone();
        let (appearance, tone) = match action {
            RootInspectorAction::RefreshFolder | RootInspectorAction::OpenCluster => {
                (ButtonAppearance::Outline, SemanticTone::Neutral)
            }
            RootInspectorAction::RepairReplicas => (ButtonAppearance::Tonal, SemanticTone::Accent),
            RootInspectorAction::ConnectionSettings => {
                (ButtonAppearance::Tonal, SemanticTone::Warning)
            }
        };
        row = row.with_child(
            sui::Button::new(action.label())
                .semantic_name(action.label())
                .description(action.description())
                .icon(action.glyph())
                .appearance(appearance)
                .tone(tone)
                .min_width(132.0)
                .theme_when(clone_reader(reader))
                .on_press_with_ctx(move |ctx| {
                    activate_root_inspector_action(&s, action, ctx);
                }),
        );
    }
    row
}

fn activate_root_inspector_action(
    shared: &Shared,
    action: RootInspectorAction,
    ctx: &mut EventCtx,
) {
    match action {
        RootInspectorAction::RefreshFolder => shared.files_mut().reload(),
        RootInspectorAction::RepairReplicas => {
            let waker = { shared.borrow().waker.clone() };
            shared
                .files_mut()
                .start_current_location_replica_repair(waker);
        }
        RootInspectorAction::OpenCluster => open_cluster_tab(shared),
        RootInspectorAction::ConnectionSettings => {
            open_settings_section(shared, SettingsSection::Connection);
        }
    }
    request_files_overlay_rebuild(ctx);
}

fn inspector_action_description(action: InspectorAction, entry: &SifsEntry) -> String {
    match action {
        InspectorAction::OpenFolder => format!("Open {}", entry.path),
        InspectorAction::ViewFile => format!(
            "Open the complete text contents of {} in the selectable document viewer",
            entry.path
        ),
        InspectorAction::AttachToChat => format!(
            "Read a verified snapshot of {} and attach it to the current chat",
            entry.path
        ),
        InspectorAction::RefreshPlacement => {
            format!("Refresh SIFS placement metadata for {}", entry.path)
        }
        InspectorAction::RepairReplicas => {
            format!("Request SIFS replica repair for {}", entry.path)
        }
        InspectorAction::MakeMetadataPrimary => format!(
            "Transfer the SIFS metadata shard primary for {} to this node",
            entry.path
        ),
        InspectorAction::PinMetadata => {
            format!("Pin SIFS metadata on this node for {}", entry.path)
        }
        InspectorAction::UnpinMetadata => {
            format!("Remove this node's SIFS metadata pin for {}", entry.path)
        }
        InspectorAction::PinContent => {
            format!("Pin SIFS content on this node for {}", entry.path)
        }
        InspectorAction::UnpinContent => {
            format!("Remove this node's SIFS content pin for {}", entry.path)
        }
        InspectorAction::ConnectionSettings => {
            "Open Connection settings for SIFS authorization and daemon recovery".to_string()
        }
        InspectorAction::CopyToSifs => format!("Copy local {} into SIFS", entry.path),
        InspectorAction::CopyToLocal => format!("Download SIFS {} to a local path", entry.path),
        InspectorAction::Copy => format!("Copy {}", entry.path),
        InspectorAction::CopyIdentifiers => format!(
            "Copy the full SIFS object, version, and hash IDs for {}",
            entry.path
        ),
        InspectorAction::Move => format!("Move {}", entry.path),
        InspectorAction::Rename => format!("Rename {}", entry.path),
        InspectorAction::Delete => format!("Delete {}", entry.path),
    }
}

fn inspector_buttons(
    shared: &Shared,
    reader: &ThemeReader,
    entry: &SifsEntry,
    actions: Vec<InspectorAction>,
) -> Flex {
    let mut row = Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .gap(8.0)
        .align_items(Alignment::Center);
    for action in actions {
        let s = shared.clone();
        let action_entry = entry.clone();
        let path = entry.path.clone();
        let (appearance, tone) = match action {
            InspectorAction::OpenFolder | InspectorAction::ViewFile => {
                (ButtonAppearance::Filled, SemanticTone::Accent)
            }
            InspectorAction::AttachToChat => (ButtonAppearance::Tonal, SemanticTone::Accent),
            InspectorAction::Delete => (ButtonAppearance::Tonal, SemanticTone::Danger),
            InspectorAction::RefreshPlacement
            | InspectorAction::RepairReplicas
            | InspectorAction::MakeMetadataPrimary
            | InspectorAction::PinMetadata
            | InspectorAction::UnpinMetadata
            | InspectorAction::PinContent
            | InspectorAction::UnpinContent
            | InspectorAction::ConnectionSettings
            | InspectorAction::CopyToSifs
            | InspectorAction::CopyToLocal => (ButtonAppearance::Tonal, SemanticTone::Accent),
            InspectorAction::Copy
            | InspectorAction::CopyIdentifiers
            | InspectorAction::Move
            | InspectorAction::Rename => (ButtonAppearance::Outline, SemanticTone::Neutral),
        };
        row = row.with_child(
            sui::Button::new(action.label())
                .semantic_name(action.semantic_name(entry))
                .description(inspector_action_description(action, entry))
                .icon(action.glyph())
                .appearance(appearance)
                .tone(tone)
                .min_width(132.0)
                .theme_when(clone_reader(reader))
                .on_press_with_ctx(move |ctx| {
                    activate_inspector_action(&s, action, &action_entry, &path, ctx);
                }),
        );
    }
    row
}

fn activate_inspector_action(
    shared: &Shared,
    action: InspectorAction,
    _entry: &SifsEntry,
    path: &str,
    ctx: &mut EventCtx,
) {
    match action {
        InspectorAction::OpenFolder => shared.files_mut().navigate(path.to_string()),
        InspectorAction::ViewFile => {
            let waker = { shared.borrow().waker.clone() };
            shared.files_mut().open_selected_document(waker);
        }
        InspectorAction::AttachToChat => {
            let waker = { shared.borrow().waker.clone() };
            shared.files_mut().start_attach_selected(waker);
        }
        InspectorAction::RefreshPlacement => {
            let waker = { shared.borrow().waker.clone() };
            shared.files_mut().start_placement_refresh(waker);
        }
        InspectorAction::RepairReplicas => {
            let waker = { shared.borrow().waker.clone() };
            shared.files_mut().start_replica_repair(waker);
        }
        InspectorAction::MakeMetadataPrimary => {
            let waker = { shared.borrow().waker.clone() };
            shared.files_mut().start_make_metadata_primary(waker);
        }
        InspectorAction::PinMetadata => {
            let waker = { shared.borrow().waker.clone() };
            shared.files_mut().start_pin_metadata(waker);
        }
        InspectorAction::UnpinMetadata => {
            let waker = { shared.borrow().waker.clone() };
            shared.files_mut().start_unpin_metadata(waker);
        }
        InspectorAction::PinContent => {
            let waker = { shared.borrow().waker.clone() };
            shared.files_mut().start_pin_content(waker);
        }
        InspectorAction::UnpinContent => {
            let waker = { shared.borrow().waker.clone() };
            shared.files_mut().start_unpin_content(waker);
        }
        InspectorAction::ConnectionSettings => {
            open_settings_section(shared, SettingsSection::Connection);
        }
        InspectorAction::CopyIdentifiers => {
            let value = shared
                .files()
                .selected_details
                .as_ref()
                .map(sifs_identifiers_for_clipboard);
            if let Some(value) = value {
                ctx.set_clipboard_text(value);
            }
        }
        InspectorAction::Copy
        | InspectorAction::CopyToSifs
        | InspectorAction::CopyToLocal
        | InspectorAction::Move
        | InspectorAction::Rename
        | InspectorAction::Delete => {
            if let Some(kind) = action.prompt_kind() {
                shared.files_mut().open_action_prompt(kind);
            }
        }
    }
    request_files_overlay_rebuild(ctx);
}

fn preview_card(
    source: FilesSourceKind,
    loading: bool,
    error: Option<&str>,
    preview: Option<&FilesPreview>,
    reader: &ThemeReader,
) -> Surface {
    let theme = reader();
    let mut body = Stack::vertical().alignment(Alignment::Stretch).spacing(8.0);
    let (badge, badge_tone, text, muted) = if loading {
        (
            "Reading preview",
            SemanticTone::Accent,
            "Reading a bounded preview from the selected source".to_string(),
            None,
        )
    } else if let Some(error) = error {
        (
            "Preview unavailable",
            SemanticTone::Warning,
            file_preview_error_display(source, error),
            None,
        )
    } else if let Some(preview) = preview {
        let text = if preview.binary {
            preview.lines.join(" ")
        } else {
            preview
                .lines
                .iter()
                .take(FILE_PREVIEW_MAX_LINES)
                .cloned()
                .collect::<Vec<_>>()
                .join("\n")
        };
        (
            if preview.binary {
                "Binary preview"
            } else {
                "Text preview"
            },
            SemanticTone::Neutral,
            text,
            preview
                .truncated
                .then(|| format!("Preview truncated to {} KB.", FILE_PREVIEW_MAX_BYTES / 1024)),
        )
    } else {
        (
            "Preview pending",
            SemanticTone::Neutral,
            "Preview has not loaded yet".to_string(),
            None,
        )
    };
    body = body.with_child(
        StatusBadge::new(badge)
            .tone(badge_tone)
            .theme_when(clone_reader(reader)),
    );
    let mut body_style = theme.text_style(theme.palette.text);
    body_style.font_size = theme.text.sm.size;
    body_style.line_height = theme.text.sm.line_height;
    body = body.with_child(RichText::from_plain_text(text, body_style));
    if let Some(muted) = muted {
        let mut muted_style = theme.text_style(theme.palette.text_muted);
        muted_style.font_size = theme.text.xs.size;
        muted_style.line_height = theme.text.xs.line_height;
        body = body.with_child(RichText::from_plain_text(muted, muted_style));
    }
    inspector_card("Preview", body, reader, None, false)
}

fn composed_inspector_content(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let files = shared.files();
    let details_button = || inspector_details_button(shared, reader, files.inspector_details_open);

    if files.loading {
        let rows = sifs_loading_rows(
            &files.path,
            files.inspector_details_open,
            files.list_slow_after_secs,
        );
        return WidgetPod::new(inspector_card(
            "Loading SIFS",
            detail_rows_widget(rows, reader),
            reader,
            Some(details_button()),
            false,
        ));
    }

    if let Some(error) = &files.error {
        let rows = sifs_unavailable_rows(error, files.inspector_details_open);
        return WidgetPod::new(inspector_card(
            "SIFS unavailable",
            detail_rows_widget(rows, reader),
            reader,
            Some(details_button()),
            false,
        ));
    }

    let Some(entry) = files.selected_entry().cloned() else {
        let rows = unselected_inspector_rows(&files);
        let actions = unselected_inspector_actions(&files);
        let mut body = Stack::vertical()
            .alignment(Alignment::Stretch)
            .spacing(10.0)
            .with_child(detail_rows_widget(rows, reader));
        if !actions.is_empty() {
            body = body.with_child(root_inspector_buttons(shared, reader, actions));
        }
        let header_action = (files.source == FilesSourceKind::Sifs).then(details_button);
        return WidgetPod::new(inspector_card(
            unselected_inspector_title(files.source),
            body,
            reader,
            header_action,
            false,
        ));
    };

    let details = files.selected_details.as_ref();
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
    let kind_line = if files.source == FilesSourceKind::Sifs {
        sifs_compact_header_line(
            &entry,
            details,
            files.clustered,
            files.details_loading,
            files.details_error.as_deref(),
        )
    } else {
        format!("{kind} · {}", details_size(&entry, details))
    };
    let theme = reader();
    let mut kind_style = theme.text_style(theme.palette.text_muted);
    kind_style.font_size = theme.text.sm.size;
    kind_style.line_height = theme.text.sm.line_height;
    let actions = inspector_actions(&files, &entry);
    let mut selected_body = Stack::vertical()
        .alignment(Alignment::Stretch)
        .spacing(10.0)
        .with_child(Label::new(kind_line).style(kind_style))
        .with_child(inspector_buttons(shared, reader, &entry, actions))
        .with_child(
            DetailRow::new("Path", entry.path.clone())
                .theme_when(clone_reader(reader))
                .max_value_lines(6),
        );
    if files.attachment_loading {
        selected_body = selected_body.with_child(
            StatusBadge::new("Preparing chat attachment")
                .tone(SemanticTone::Accent)
                .theme_when(clone_reader(reader)),
        );
    } else if let Some(error) = files.attachment_error.as_deref() {
        selected_body = selected_body.with_child(
            Label::new(error.to_string()).style(theme.text_style(theme.palette.danger)),
        );
    }
    let selected_card = inspector_card(
        entry.name.clone(),
        selected_body,
        reader,
        Some(details_button()),
        true,
    );

    let mut content = Stack::vertical()
        .alignment(Alignment::Stretch)
        .spacing(12.0)
        .with_child(selected_card);

    if files.source == FilesSourceKind::Local {
        let local = files.selected_local_details.as_ref();
        let status_label = local_metadata_status_label(
            files.details_loading,
            files.details_error.as_deref(),
            local,
        );
        let status_tone = local_metadata_status_tone(
            files.details_loading,
            files.details_error.as_deref(),
            local,
        );
        let mut rows = vec![("Source", "Host filesystem".to_string())];
        if let Some(local) = local {
            rows.push((
                "Permissions",
                if local.readonly {
                    "read-only".to_string()
                } else {
                    "writable".to_string()
                },
            ));
            if let Some(canonical) = &local.canonical_path
                && canonical != &local.path
            {
                rows.push(("Canonical", canonical.clone()));
            }
        } else if let Some(error) = &files.details_error {
            rows.push(("Reason", error.clone()));
        } else if files.details_loading {
            rows.push(("Status", "Reading metadata".to_string()));
        }
        let body = Stack::vertical()
            .alignment(Alignment::Stretch)
            .spacing(8.0)
            .with_child(
                StatusBadge::new(status_label)
                    .tone(status_tone)
                    .theme_when(clone_reader(reader)),
            )
            .with_child(detail_rows_widget(rows, reader));
        content = content.with_child(inspector_card("Local file", body, reader, None, false));

        if files.inspector_details_open {
            let mut rows = Vec::new();
            if let Some(local) = local {
                rows.push(("Kind", local.kind.clone()));
                rows.push(("Accessed", local_time_label(local.accessed_at_unix_nanos)));
                rows.push(("Modified", local_time_label(local.modified_at_unix_nanos)));
                rows.push(("Created", local_time_label(local.created_at_unix_nanos)));
            }
            if !rows.is_empty() {
                content = content.with_child(inspector_card(
                    "Technical details",
                    detail_rows_widget(rows, reader),
                    reader,
                    None,
                    false,
                ));
            }
        }
    } else {
        let status_label = sifs_metadata_status_label(
            files.details_loading,
            files.details_error.as_deref(),
            details,
        );
        let status_tone = sifs_metadata_status_tone(
            files.details_loading,
            files.details_error.as_deref(),
            details,
        );
        let placement_rows = vec![
            (
                "Placement",
                sifs_replication_summary(details, files.clustered),
            ),
            ("Metadata", sifs_metadata_shard_summary(details)),
            ("Health", sifs_health_summary(details)),
            ("Pin", sifs_pin_summary(details)),
            (
                "Next",
                sifs_inspector_next_step(
                    details,
                    files.clustered,
                    files.details_loading,
                    files.details_error.as_deref(),
                ),
            ),
        ];
        let placement_body = Stack::vertical()
            .alignment(Alignment::Stretch)
            .spacing(8.0)
            .with_child(
                PlacementBadge::new(short_node_label(&files.node_id))
                    .icon(IconGlyph::Storage)
                    .tone(SemanticTone::Accent)
                    .theme_when(clone_reader(reader)),
            )
            .with_child(
                StatusBadge::new(status_label)
                    .tone(status_tone)
                    .theme_when(clone_reader(reader)),
            )
            .with_child(detail_rows_widget(placement_rows, reader));
        content = content.with_child(inspector_card(
            "Placement",
            placement_body,
            reader,
            None,
            false,
        ));

        if files.inspector_details_open {
            let policy = details
                .map(|details| details.policy.clone())
                .unwrap_or_else(|| "not loaded".to_string());
            let mut rows = Vec::new();
            if let Some(error) = &files.details_error {
                rows.push(("Reason", error.clone()));
                rows.push(("Authority", "SIFS through daemon".to_string()));
            } else if files.details_loading {
                rows.push(("Status", "Waiting for daemon metadata".to_string()));
            } else if details.is_none() {
                rows.push((
                    "Status",
                    "Metadata has not returned for this object yet".to_string(),
                ));
                rows.push(("Authority", "SIFS through daemon".to_string()));
            }
            rows.push(("Policy", policy));
            if let Some(shard) = details.and_then(|details| details.metadata_shard.as_ref()) {
                rows.push(("Shard", sifs_metadata_shard_detail_summary(shard)));
            }
            if let Some(health) = details.and_then(|details| details.health.as_ref()) {
                rows.push(("Agent state", health.agent_state.clone()));
                rows.push((
                    "Operator counts",
                    format!(
                        "{} · {} · {}",
                        plural(health.version_count, "version"),
                        plural(health.receipt_count, "receipt"),
                        plural(health.intent_count, "intent")
                    ),
                ));
                rows.push((
                    "Repair backlog",
                    format!(
                        "{} · {}",
                        plural(health.pending_repair_count, "missing replica"),
                        plural(health.open_conflict_count, "conflict")
                    ),
                ));
                rows.push(("Missing nodes", sifs_missing_replica_summary(health)));
            }
            rows.push((
                "Durable nodes",
                details
                    .map(details_locations)
                    .unwrap_or_else(|| "not reported".to_string()),
            ));
            if let Some(details) = details {
                rows.push(("Local pins", sifs_pin_detail_summary(details)));
                rows.push(("File system", details.file_system_id.clone()));
                rows.push(("Object", short(&details.object_id, 28)));
                rows.push(("Version", short(&details.version, 28)));
                rows.push((
                    "Mode",
                    format!("{:o} · {} links", details.mode, details.link_count),
                ));
                rows.push((
                    "Owner",
                    format!("uid {} · gid {}", details.uid, details.gid),
                ));
                rows.push(("Modified", unix_nanos_label(details.modified_at_unix_nanos)));
                if let Some(hash) = &details.content_hash {
                    rows.push(("Content", short(hash, 28)));
                }
            }
            content = content.with_child(inspector_card(
                "Technical details",
                detail_rows_widget(rows, reader),
                reader,
                None,
                false,
            ));
        }

        if !files.clustered {
            let mut note_style = theme.text_style(theme.palette.text_muted);
            note_style.font_size = theme.text.xs.size;
            note_style.line_height = theme.text.xs.line_height;
            content = content.with_child(
                Surface::panel(RichText::from_plain_text(
                    "Join a cluster to spread this object across nodes and tune replication.",
                    note_style,
                ))
                .appearance(sui::SurfaceAppearance::Soft)
                .tone(SemanticTone::Neutral)
                .theme_when(clone_reader(reader))
                .padding(Insets {
                    left: 12.0,
                    top: 10.0,
                    right: 12.0,
                    bottom: 10.0,
                })
                .fill_width(),
            );
        }
    }

    if !entry.is_dir {
        content = content.with_child(preview_card(
            files.source,
            files.preview_loading,
            files.preview_error.as_deref(),
            files.selected_preview.as_ref(),
            reader,
        ));
    }

    WidgetPod::new(content)
}

/// Assemble the wide inspector and the compact sheet body from the same stock SUI controls.
fn inspector(
    shared: &Shared,
    reader: &ThemeReader,
    scroll_state: &ScrollState,
) -> impl Widget + use<> {
    let header = WidgetPadding::horizontal(
        CONTENT_PAD,
        SizedBox::new().height(18.0).with_child(
            SectionLabel::new("Inspector")
                .semantic_name("Inspector")
                .theme_when(clone_reader(reader)),
        ),
    )
    .fill_child_height();
    let task_queue =
        WidgetPadding::horizontal(CONTENT_PAD, InspectorTaskQueue::new(shared.clone(), reader))
            .fill_child_width();
    let body = WidgetPadding::new(
        sui::Insets {
            left: 0.0,
            right: 0.0,
            top: CONTENT_PAD + 4.0,
            bottom: 0.0,
        },
        Stack::vertical()
            .alignment(Alignment::Stretch)
            .spacing(12.0)
            .with_child(header)
            .with_child(task_queue)
            .with_child(InspectorView::new(shared.clone())),
    )
    .fill_child_width();
    Surface::new(
        SurfaceRole::Sidebar,
        ScrollView::vertical(body)
            .state(scroll_state.clone())
            .name("File inspector")
            .viewport_width_hint(true),
    )
    .border(SurfaceBorder::Left)
    .theme_when(clone_reader(reader))
    .fill()
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
    Surface::window(
        WidgetPadding::horizontal(CONTENT_PAD, rebuilt)
            .fill_child_width()
            .fill_child_height(),
    )
    .theme_when(clone_reader(reader))
    .fill_width()
}

fn picker_first_header_bar(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
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
        move |_key| WidgetPod::new(picker_first_header(&s, &r)),
    );
    Surface::window(
        WidgetPadding::horizontal(CONTENT_PAD, rebuilt)
            .fill_child_width()
            .fill_child_height(),
    )
    .theme_when(clone_reader(reader))
    .fill_width()
}

/// Chooses only the desktop command-row density; the surrounding wide Files body and inspector
/// stay mounted. This avoids making the 640px inspector breakpoint also carry the much wider
/// complete-toolbar requirement.
struct FilesDesktopHeader {
    full: WidgetPod,
    picker_first: WidgetPod,
    use_full: Cell<bool>,
}

impl FilesDesktopHeader {
    fn new(shared: &Shared, reader: &ThemeReader) -> Self {
        Self {
            full: WidgetPod::new(header_bar(shared, reader)),
            picker_first: WidgetPod::new(picker_first_header_bar(shared, reader)),
            use_full: Cell::new(true),
        }
    }

    fn active(&self) -> &WidgetPod {
        if self.use_full.get() {
            &self.full
        } else {
            &self.picker_first
        }
    }

    fn active_mut(&mut self) -> &mut WidgetPod {
        if self.use_full.get() {
            &mut self.full
        } else {
            &mut self.picker_first
        }
    }
}

impl Widget for FilesDesktopHeader {
    fn event(&mut self, _ctx: &mut EventCtx, _event: &Event) {}

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        self.use_full
            .set(constraints.max.width >= FULL_HEADER_INNER_MIN_W + CONTENT_PAD * 2.0);
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
        visitor.visit(self.active_mut());
    }
}

fn compact_header_bar(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
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
                    files.loading,
                )
            }
        },
        move |_key| WidgetPod::new(compact_header(&s, &r)),
    );
    Surface::window(
        WidgetPadding::new(
            Insets {
                left: 8.0,
                top: 6.0,
                right: 8.0,
                bottom: 6.0,
            },
            rebuilt,
        )
        .fill_child_width()
        .fill_child_height(),
    )
    .theme_when(clone_reader(reader))
    .fill_width()
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
    let places_scroll =
        ScrollView::vertical(files_places_rail(shared, reader)).state(places_scroll_state.clone());
    let places = Background::new(Color::TRANSPARENT, places_scroll).brush_when({
        let s = shared.clone();
        move || palette(&s).sidebar
    });
    let list_with_places = FixedPaneSplit::horizontal(
        places,
        Separator::vertical().theme_when(clone_reader(reader)),
        list_column(shared, reader, scroll_state),
    )
    .fixed_first(PLACES_W)
    .divider_extent(1.0);
    let body = FixedPaneSplit::horizontal(
        list_with_places,
        Separator::vertical().theme_when(clone_reader(reader)),
        inspector(shared, reader, inspector_scroll_state),
    )
    .fixed_second(INSPECTOR_W)
    .divider_extent(1.0);
    let header = Stack::vertical()
        .alignment(Alignment::Stretch)
        .with_child(
            SizedBox::new()
                .height(HEADER_H)
                .with_child(FilesDesktopHeader::new(shared, reader)),
        )
        .with_child(Separator::horizontal().theme_when(clone_reader(reader)));
    WidgetPod::new(
        Dock::new(body)
            .top(HEADER_H + 1.0, header)
            .bottom(FILES_STATUS_H, files_status_bar(shared, reader)),
    )
}

/// Narrow: compact header + list body. Commands and the inspector remain
/// reachable through overlays owned by `FilesDialogHost`.
fn narrow_pod(shared: &Shared, reader: &ThemeReader, scroll_state: &ScrollState) -> WidgetPod {
    let header = Stack::vertical()
        .alignment(Alignment::Stretch)
        .with_child(
            SizedBox::new()
                .height(HEADER_H)
                .with_child(compact_header_bar(shared, reader)),
        )
        .with_child(Separator::horizontal().theme_when(clone_reader(reader)));
    WidgetPod::new(
        Dock::new(list_column(shared, reader, scroll_state))
            .top(HEADER_H + 1.0, header)
            .bottom(FILES_STATUS_H, files_status_bar(shared, reader)),
    )
}

fn compact_command_button<E, F>(
    label: impl Into<String>,
    reader: &ThemeReader,
    enabled: E,
    on_press: F,
) -> sui::Button
where
    E: Fn() -> bool + 'static,
    F: FnMut(&mut EventCtx) + 'static,
{
    sui::Button::new(label)
        .min_height(32.0)
        .theme_when(clone_reader(reader))
        .enabled_when(enabled)
        .on_press_with_ctx(on_press)
}

fn compact_files_commands(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let forward = {
        let enabled = shared.clone();
        let action = shared.clone();
        compact_command_button(
            "Forward",
            reader,
            move || enabled.files().can_go_forward(),
            move |ctx| {
                let waker = { action.borrow().waker.clone() };
                let mut files = action.files_mut();
                files.close_compact_overlay();
                files.go_forward(waker);
                drop(files);
                request_files_overlay_rebuild(ctx);
            },
        )
    };
    let refresh = {
        let enabled = shared.clone();
        let action = shared.clone();
        compact_command_button(
            "Refresh current folder",
            reader,
            move || !enabled.files().loading,
            move |ctx| {
                let waker = { action.borrow().waker.clone() };
                let mut files = action.files_mut();
                files.close_compact_overlay();
                files.refresh(waker);
                drop(files);
                request_files_overlay_rebuild(ctx);
            },
        )
    };
    let browse_sifs = {
        let enabled = shared.clone();
        let action = shared.clone();
        compact_command_button(
            "Browse SIFS",
            reader,
            move || {
                let files = enabled.files();
                files.source != FilesSourceKind::Sifs && !files.loading
            },
            move |ctx| {
                let waker = { action.borrow().waker.clone() };
                let mut files = action.files_mut();
                files.close_compact_overlay();
                files.switch_source(FilesSourceKind::Sifs, waker);
                drop(files);
                request_files_overlay_rebuild(ctx);
            },
        )
    };
    let browse_local = {
        let enabled = shared.clone();
        let action = shared.clone();
        compact_command_button(
            "Browse local files",
            reader,
            move || {
                let files = enabled.files();
                files.source != FilesSourceKind::Local && !files.loading
            },
            move |ctx| {
                let waker = { action.borrow().waker.clone() };
                let mut files = action.files_mut();
                files.close_compact_overlay();
                files.switch_source(FilesSourceKind::Local, waker);
                drop(files);
                request_files_overlay_rebuild(ctx);
            },
        )
    };
    let open_local_file = {
        let enabled = shared.clone();
        let action = shared.clone();
        compact_command_button(
            "Open local file...",
            reader,
            move || !enabled.files().picker_lane.is_pending(),
            move |ctx| {
                let waker = { action.borrow().waker.clone() };
                let mut files = action.files_mut();
                files.close_compact_overlay();
                files.request_native_picker(FilesPickerAction::OpenFile, waker);
                drop(files);
                request_files_overlay_rebuild(ctx);
            },
        )
    };
    let open_local_folder = {
        let enabled = shared.clone();
        let action = shared.clone();
        compact_command_button(
            "Choose local folder...",
            reader,
            move || !enabled.files().picker_lane.is_pending(),
            move |ctx| {
                let waker = { action.borrow().waker.clone() };
                let mut files = action.files_mut();
                files.close_compact_overlay();
                files.request_native_picker(FilesPickerAction::OpenFolder, waker);
                drop(files);
                request_files_overlay_rebuild(ctx);
            },
        )
    };
    let import_files = {
        let enabled = shared.clone();
        let action = shared.clone();
        compact_command_button(
            "Import files...",
            reader,
            move || {
                let files = enabled.files();
                files.can_modify_current_location() && !files.picker_lane.is_pending()
            },
            move |ctx| {
                let waker = { action.borrow().waker.clone() };
                let mut files = action.files_mut();
                files.close_compact_overlay();
                files.request_native_picker(FilesPickerAction::ImportFiles, waker);
                drop(files);
                request_files_overlay_rebuild(ctx);
            },
        )
    };
    let import_folder = {
        let enabled = shared.clone();
        let action = shared.clone();
        compact_command_button(
            "Import folder...",
            reader,
            move || {
                let files = enabled.files();
                files.can_modify_current_location() && !files.picker_lane.is_pending()
            },
            move |ctx| {
                let waker = { action.borrow().waker.clone() };
                let mut files = action.files_mut();
                files.close_compact_overlay();
                files.request_native_picker(FilesPickerAction::ImportFolder, waker);
                drop(files);
                request_files_overlay_rebuild(ctx);
            },
        )
    };
    let go_to = {
        let enabled = shared.clone();
        let action = shared.clone();
        compact_command_button(
            "Go to path",
            reader,
            move || !enabled.files().loading,
            move |ctx| {
                action
                    .files_mut()
                    .open_action_prompt(FileActionPromptKind::GoTo);
                request_files_overlay_rebuild(ctx);
            },
        )
    };
    let new_file = {
        let enabled = shared.clone();
        let action = shared.clone();
        compact_command_button(
            "New file",
            reader,
            move || enabled.files().can_modify_current_location(),
            move |ctx| {
                action
                    .files_mut()
                    .open_action_prompt(FileActionPromptKind::NewFile);
                request_files_overlay_rebuild(ctx);
            },
        )
    };
    let new_folder = {
        let enabled = shared.clone();
        let action = shared.clone();
        compact_command_button(
            "New folder",
            reader,
            move || enabled.files().can_modify_current_location(),
            move |ctx| {
                action
                    .files_mut()
                    .open_action_prompt(FileActionPromptKind::NewFolder);
                request_files_overlay_rebuild(ctx);
            },
        )
    };
    let copy = {
        let enabled = shared.clone();
        let action = shared.clone();
        compact_command_button(
            "Copy to",
            reader,
            move || {
                let files = enabled.files();
                files.can_modify_current_location()
                    && (files.marked_count() > 0 || files.selected_entry().is_some())
            },
            move |ctx| {
                let kind = if action.files().marked_count() > 0 {
                    FileActionPromptKind::CopySelected
                } else {
                    FileActionPromptKind::Copy
                };
                action.files_mut().open_action_prompt(kind);
                request_files_overlay_rebuild(ctx);
            },
        )
    };
    let move_to = {
        let enabled = shared.clone();
        let action = shared.clone();
        compact_command_button(
            "Move to",
            reader,
            move || {
                let files = enabled.files();
                files.can_modify_current_location()
                    && (files.marked_count() > 0 || files.selected_entry().is_some())
            },
            move |ctx| {
                let kind = if action.files().marked_count() > 0 {
                    FileActionPromptKind::MoveSelected
                } else {
                    FileActionPromptKind::Move
                };
                action.files_mut().open_action_prompt(kind);
                request_files_overlay_rebuild(ctx);
            },
        )
    };
    let refresh_placement = {
        let enabled = shared.clone();
        let action = shared.clone();
        compact_command_button(
            "Refresh placement",
            reader,
            move || {
                let files = enabled.files();
                files.source == FilesSourceKind::Sifs
                    && files.can_modify_current_location()
                    && files.selected_entry().is_some()
            },
            move |ctx| {
                let waker = { action.borrow().waker.clone() };
                let mut files = action.files_mut();
                files.close_compact_overlay();
                files.start_placement_refresh(waker);
                drop(files);
                request_files_overlay_rebuild(ctx);
            },
        )
    };
    let repair = {
        let enabled = shared.clone();
        let action = shared.clone();
        compact_command_button(
            "Repair replicas",
            reader,
            move || {
                let files = enabled.files();
                files.source == FilesSourceKind::Sifs
                    && files.can_modify_current_location()
                    && files.selected_entry().is_some()
            },
            move |ctx| {
                let waker = { action.borrow().waker.clone() };
                let mut files = action.files_mut();
                files.close_compact_overlay();
                files.start_replica_repair(waker);
                drop(files);
                request_files_overlay_rebuild(ctx);
            },
        )
    };
    let rename = {
        let enabled = shared.clone();
        let action = shared.clone();
        compact_command_button(
            "Rename",
            reader,
            move || {
                let files = enabled.files();
                files.can_modify_current_location() && files.selected_entry().is_some()
            },
            move |ctx| {
                action
                    .files_mut()
                    .open_action_prompt(FileActionPromptKind::Rename);
                request_files_overlay_rebuild(ctx);
            },
        )
    };
    let delete = {
        let enabled = shared.clone();
        let action = shared.clone();
        compact_command_button(
            "Delete selected item",
            reader,
            move || {
                let files = enabled.files();
                files.can_modify_current_location()
                    && (files.marked_count() > 0 || files.selected_entry().is_some())
            },
            move |ctx| {
                let kind = if action.files().marked_count() > 0 {
                    FileActionPromptKind::DeleteSelected
                } else {
                    FileActionPromptKind::Delete
                };
                action.files_mut().open_action_prompt(kind);
                request_files_overlay_rebuild(ctx);
            },
        )
    };

    let content = Stack::vertical()
        .alignment(Alignment::Stretch)
        .spacing(6.0)
        .with_child(forward)
        .with_child(refresh)
        .with_child(browse_sifs)
        .with_child(browse_local)
        .with_child(open_local_file)
        .with_child(open_local_folder)
        .with_child(import_files)
        .with_child(import_folder)
        .with_child(go_to)
        .with_child(new_file)
        .with_child(new_folder)
        .with_child(copy)
        .with_child(move_to)
        .with_child(refresh_placement)
        .with_child(repair)
        .with_child(rename)
        .with_child(delete);
    ScrollView::vertical(WidgetPadding::new(
        Insets {
            left: 2.0,
            top: 2.0,
            right: 8.0,
            bottom: 2.0,
        },
        content,
    ))
    .name("Compact file command overflow")
    .viewport_width_hint(true)
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
            .theme_when(clone_reader(reader))
            .appearance(sui::ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .on_press_with_ctx(move |ctx| {
                s.files_mut().close_action_prompt();
                request_files_overlay_rebuild(ctx);
            })
    };
    let actions = Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .gap(8.0)
        .align_items(Alignment::End)
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

fn document_status_text(shared: &Shared) -> String {
    let files = shared.files();
    let Some(document) = files.document.as_ref() else {
        return "Document closed".to_string();
    };
    if document.loading {
        return "Reading a verified snapshot...".to_string();
    }
    if document.load_error.is_some() {
        return "Document unavailable".to_string();
    }
    if document.saving {
        return "Saving safely...".to_string();
    }
    if document.save_error.is_some() {
        return "Save needs attention; your edits are still open".to_string();
    }
    if document.save_warning.is_some() {
        return match document.source {
            FilesSourceKind::Sifs => "Saved; SIFS maintenance needs attention".to_string(),
            FilesSourceKind::Local => {
                "Saved; host filesystem follow-up needs attention".to_string()
            }
        };
    }
    if document.dirty() {
        return "Unsaved changes".to_string();
    }
    if document.read_only_reason.is_some() {
        return "Read-only verified snapshot".to_string();
    }
    match document.source {
        FilesSourceKind::Sifs => "Saved to SIFS with version protection".to_string(),
        FilesSourceKind::Local => "Saved".to_string(),
    }
}

fn document_detail_text(shared: &Shared) -> String {
    let files = shared.files();
    let Some(document) = files.document.as_ref() else {
        return String::new();
    };
    if let Some(error) = &document.load_error {
        return error.clone();
    }
    if let Some(error) = &document.save_error {
        return error.clone();
    }
    if let Some(warning) = &document.save_warning {
        return match document.source {
            FilesSourceKind::Sifs => format!(
                "The new SIFS version is visible, but follow-up maintenance warned: {warning}"
            ),
            FilesSourceKind::Local => format!(
                "The local content was committed, but host filesystem follow-up warned: {warning}"
            ),
        };
    }
    if let Some(reason) = &document.read_only_reason {
        return reason.clone();
    }
    if document.dirty() {
        return format!(
            "{} - Attach sends this unsaved editor buffer and labels it as unpersisted; Ctrl+S saves with conflict protection.",
            document.path
        );
    }
    match document.source {
        FilesSourceKind::Local => format!(
            "{} - Ctrl+S saves by atomic same-folder replacement after checking path, size, and modified time.",
            document.path
        ),
        FilesSourceKind::Sifs => format!(
            "{} - Ctrl+S uses an expected object ID and version, so a concurrent SIFS update cannot be overwritten.",
            document.path
        ),
    }
}

fn files_document_sheet(shared: &Shared, reader: &ThemeReader) -> Option<WidgetPod> {
    let document = shared.files().document.clone()?;
    let theme = reader();
    let mut text_style = theme.mono_text_style(theme.palette.text);
    text_style.font_size = theme.text.sm.size;
    text_style.line_height = theme.text.sm.line_height;
    let editor_shared = shared.clone();
    let mut editor = TextSurface::new(format!("files-document:{}", document.path))
        .theme(theme.clone())
        .text_style(text_style)
        .padding(Insets::all(12.0))
        .min_width(0.0)
        .min_height(420.0)
        .wrap(TextWrap::NoWrap)
        .value(document.text.clone())
        .on_change(move |value| editor_shared.files_mut().set_document_text(value));
    if !document.editable() {
        editor = editor.read_only();
    }
    let status_shared = shared.clone();
    let detail_shared = shared.clone();
    let status = Surface::panel(
        Stack::vertical()
            .alignment(Alignment::Stretch)
            .spacing(3.0)
            .with_child(
                Label::dynamic("Document", move || document_status_text(&status_shared))
                    .style(theme.text_style(theme.palette.text)),
            )
            .with_child(
                Label::dynamic("Document path", move || {
                    document_detail_text(&detail_shared)
                })
                .style(theme.text_style(theme.palette.text_muted)),
            ),
    )
    .theme_when(clone_reader(reader))
    .padding(Insets {
        left: 12.0,
        top: 8.0,
        right: 12.0,
        bottom: 8.0,
    })
    .fill_width();
    let editor_body = Dock::new(
        Surface::field(editor)
            .theme_when(clone_reader(reader))
            .fill_width(),
    )
    .bottom(74.0, status)
    .fallback_width(780.0)
    .fallback_body_height(520.0);
    let body = SwitchView::new()
        .selected(if document.loading {
            0
        } else if document.load_error.is_some() {
            1
        } else {
            2
        })
        .with_child(centered_note(
            reader,
            IconGlyph::Hourglass,
            "Opening document",
            "Reading the complete selected version on the Files background worker",
        ))
        .with_child(centered_note(
            reader,
            IconGlyph::Alert,
            "Document unavailable",
            document
                .load_error
                .clone()
                .unwrap_or_else(|| "The document could not be opened".to_string()),
        ))
        .with_child(editor_body);

    let close_shared = shared.clone();
    let close = sui::Button::new("Close")
        .appearance(ButtonAppearance::Outline)
        .tone(SemanticTone::Neutral)
        .theme_when(clone_reader(reader))
        .on_press_with_ctx(move |ctx| {
            close_shared.files_mut().request_close_document();
            request_files_overlay_rebuild(ctx);
        });
    let attach_shared = shared.clone();
    let attach_enabled = shared.clone();
    let attach = sui::Button::new("Attach to current chat")
        .icon(IconGlyph::Attach)
        .appearance(ButtonAppearance::Tonal)
        .tone(SemanticTone::Accent)
        .theme_when(clone_reader(reader))
        .enabled_when(move || {
            let files = attach_enabled.files();
            files
                .document
                .as_ref()
                .is_some_and(|document| !document.loading && document.load_error.is_none())
                && !files.attachment_loading
        })
        .on_press_with_ctx(move |ctx| {
            let waker = { attach_shared.borrow().waker.clone() };
            attach_shared.files_mut().start_attach_document(waker);
            request_files_overlay_rebuild(ctx);
        });
    let mut sheet = SideSheet::new(document.title.clone(), body)
        .theme_when(clone_reader(reader))
        .description(format!(
            "Complete {} text document - {}",
            document.source.label(),
            document.path
        ))
        .placement(SideSheetPlacement::Right)
        .width(920.0)
        .modal(true)
        .dismiss_on_scrim(true)
        .action(attach);
    if document.load_error.is_some() {
        let retry_shared = shared.clone();
        sheet = sheet.action(
            sui::Button::new("Retry")
                .appearance(ButtonAppearance::Tonal)
                .tone(SemanticTone::Accent)
                .theme_when(clone_reader(reader))
                .on_press_with_ctx(move |ctx| {
                    let waker = { retry_shared.borrow().waker.clone() };
                    retry_shared.files_mut().retry_document(waker);
                    request_files_overlay_rebuild(ctx);
                }),
        );
    } else if !document.loading {
        let save_shared = shared.clone();
        let save_enabled = shared.clone();
        sheet = sheet.action(
            sui::Button::new("Save")
                .icon(IconGlyph::Check)
                .appearance(ButtonAppearance::Filled)
                .tone(SemanticTone::Accent)
                .theme_when(clone_reader(reader))
                .enabled_when(move || {
                    save_enabled
                        .files()
                        .document
                        .as_ref()
                        .is_some_and(|document| {
                            document.editable() && document.dirty() && !document.saving
                        })
                })
                .on_press_with_ctx(move |ctx| {
                    let waker = { save_shared.borrow().waker.clone() };
                    save_shared.files_mut().save_document(waker);
                    request_files_overlay_rebuild(ctx);
                }),
        );
    }
    let dismiss_shared = shared.clone();
    Some(WidgetPod::new(sheet.action(close).on_dismiss(move || {
        dismiss_shared.files_mut().request_close_document()
    })))
}

fn files_document_close_dialog(shared: &Shared, reader: &ThemeReader) -> Option<WidgetPod> {
    if !shared.files().document_close_confirmation {
        return None;
    }
    let title = shared
        .files()
        .document
        .as_ref()
        .map(|document| document.title.clone())
        .unwrap_or_else(|| "document".to_string());
    let discard_shared = shared.clone();
    let keep_shared = shared.clone();
    let actions = Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .gap(8.0)
        .align_items(Alignment::End)
        .with_child(
            sui::Button::new("Discard changes")
                .appearance(ButtonAppearance::Tonal)
                .tone(SemanticTone::Danger)
                .theme_when(clone_reader(reader))
                .on_press_with_ctx(move |ctx| {
                    let waker = { discard_shared.borrow().waker.clone() };
                    discard_shared
                        .files_mut()
                        .discard_document_and_continue(waker);
                    request_files_overlay_rebuild(ctx);
                }),
        )
        .with_child(
            sui::Button::new("Keep editing")
                .appearance(ButtonAppearance::Filled)
                .tone(SemanticTone::Accent)
                .theme_when(clone_reader(reader))
                .on_press_with_ctx(move |ctx| {
                    keep_shared.files_mut().cancel_document_close();
                    request_files_overlay_rebuild(ctx);
                }),
        );
    let body = Stack::vertical()
        .alignment(Alignment::Stretch)
        .spacing(12.0)
        .with_child(
            Label::new(format!(
                "{title} has unsaved changes. Discarding closes the current snapshot without writing it."
            ))
            .theme(reader()),
        )
        .with_child(actions);
    Some(WidgetPod::new(
        sui::Dialog::new("Unsaved changes", body)
            .theme(reader())
            .description("Choose whether to keep editing or discard the document changes")
            .max_width(480.0)
            .dismiss_on_scrim(false),
    ))
}

fn files_dialog_signature(shared: &Shared) -> String {
    let files = shared.files();
    if files.document_close_confirmation {
        return format!("document-confirm|{}", files.document_revision);
    }
    if files.document.is_some() {
        return format!("document|{}", files.document_revision);
    }
    drop(files);
    let action = files_action_dialog_signature(shared);
    if action != "closed" {
        return format!("action|{action}");
    }
    let files = shared.files();
    let Some(overlay) = files.compact_overlay else {
        return "closed".to_string();
    };
    format!(
        "compact|{overlay:?}|{:?}|{}|{}|{:?}|{}|{}|{}",
        files.source,
        files.path,
        files.list_revision,
        files.selected,
        files.details_loading,
        files.preview_loading,
        files.inspector_details_open
    )
}

fn compact_files_dialog(shared: &Shared, reader: &ThemeReader) -> Option<WidgetPod> {
    let overlay = shared.files().compact_overlay?;
    let (title, description, selected) = match overlay {
        CompactFilesOverlay::Commands => (
            "File commands",
            "Commands hidden from the compact file toolbar",
            0,
        ),
        CompactFilesOverlay::Inspector => (
            "File inspector",
            "Preview, placement, metadata, recovery actions, and file tasks",
            1,
        ),
    };
    let inspector_scroll_state = ScrollState::new();
    let body = SwitchView::new()
        .selected(selected)
        .with_child(compact_files_commands(shared, reader))
        .with_child(inspector(shared, reader, &inspector_scroll_state));
    let dismiss_shared = shared.clone();
    Some(WidgetPod::new(
        SideSheet::new(title, body)
            .theme_when(clone_reader(reader))
            .description(description)
            .placement(SideSheetPlacement::Right)
            .width(420.0)
            .modal(true)
            .dismiss_on_scrim(true)
            .on_dismiss(move || dismiss_shared.files_mut().close_compact_overlay()),
    ))
}

fn files_dialog(shared: &Shared, reader: &ThemeReader) -> Option<WidgetPod> {
    if shared.files().document_close_confirmation {
        files_document_close_dialog(shared, reader)
    } else if shared.files().document.is_some() {
        files_document_sheet(shared, reader)
    } else if shared.files().action_prompt.is_some() {
        files_action_dialog(shared, reader)
    } else {
        compact_files_dialog(shared, reader)
    }
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
        let signature = files_dialog_signature(&self.shared);
        if signature == self.dialog_signature {
            return;
        }
        self.dialog_signature = signature;
        self.dialog = files_dialog(&self.shared, &self.reader);
    }
}

impl Widget for FilesDialogHost {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        if ctx.phase() == EventPhase::Capture
            && let Event::Keyboard(key) = event
            && key.state == KeyState::Pressed
            && (key.modifiers.control || key.modifiers.meta)
            && key.key.eq_ignore_ascii_case("s")
            && self.shared.files().document.is_some()
        {
            let waker = { self.shared.borrow().waker.clone() };
            self.shared.files_mut().save_document(waker);
            ctx.set_handled();
            request_files_overlay_rebuild(ctx);
        }
        let signature = files_dialog_signature(&self.shared);
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
        Rc::new(move || s.theme().with_size(ControlSize::Small))
    };
    FilesDialogHost::new(FilesView::new(shared.clone()), shared, reader)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{Backend, Connection};
    use crate::state::AppState;
    use sinomo_cli::app_api::SifsPinDetails;

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

    fn test_sifs_details(local_pins: Vec<SifsPinDetails>) -> SifsObjectDetails {
        SifsObjectDetails {
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
            local_pins,
            metadata_shard: None,
            health: None,
        }
    }

    #[test]
    fn sifs_identifier_copy_keeps_full_unshortened_values() {
        let mut details = test_sifs_details(Vec::new());
        details.file_system_id = "sifs-01-full-file-system-id".to_string();
        details.object_id = "object-01-full-content-addressed-id".to_string();
        details.version = "version-01-full-version-id".to_string();
        details.content_hash = Some("sha256:full-content-hash".to_string());

        let copied = sifs_identifiers_for_clipboard(&details);

        assert!(copied.contains(&details.file_system_id));
        assert!(copied.contains(&details.object_id));
        assert!(copied.contains(&details.version));
        assert!(copied.contains(details.content_hash.as_deref().expect("content hash")));
        assert!(!copied.contains("..."));
    }

    fn degraded_sifs_health() -> SifsHealthSummary {
        SifsHealthSummary {
            file_system_count: 1,
            object_count: 776,
            version_count: 2482,
            pending_write_count: 43,
            open_conflict_count: 21,
            pending_repair_count: 18,
            pending_namespace_job_count: 0,
            failed_namespace_job_count: 0,
            metadata_shard_count: 3,
            repair_attempt_count: 18,
        }
    }

    #[test]
    fn compact_files_header_opens_command_overflow() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        let reader: ThemeReader = {
            let s = shared.clone();
            Rc::new(move || s.theme())
        };
        let body = SizedBox::new()
            .height(HEADER_H)
            .with_child(compact_header_bar(&shared, &reader));
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Compact files commands")
                    .root(FilesDialogHost::new(
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
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("More file commands")
            .click()?;
        app.run_until_idle()?;

        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::Dialog)
                .with_name("File commands")
                .count()?,
            1
        );
        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::Button)
                .with_name("Browse local files")
                .count()?,
            1
        );
        Ok(())
    }

    #[test]
    fn compact_files_header_opens_preview_and_inspector_sheet() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        let reader: ThemeReader = {
            let s = shared.clone();
            Rc::new(move || s.theme())
        };
        let body = SizedBox::new()
            .height(HEADER_H)
            .with_child(compact_header_bar(&shared, &reader));
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Compact files inspector")
                    .root(FilesDialogHost::new(body, shared, Rc::clone(&reader))),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        app.run_until_idle()?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Open file inspector")
            .click()?;
        app.run_until_idle()?;

        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::Dialog)
                .with_name("File inspector")
                .count()?,
            1
        );
        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::ScrollView)
                .with_name("File inspector")
                .count()?,
            1,
            "compact inspector sheet should expose the same preview and recovery surface"
        );
        Ok(())
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
            crumb_label("sinomo-agent-channel-01ktsg9dw850xkprvhkn0kfr8t", None),
            "sinomo-age…rvhkn0kfr8t"
        );
    }

    #[test]
    fn display_name_for_width_keeps_generated_file_names_readable() {
        let name = "sinomo-agent-channel-01ktsg9dw850xkprvhkn0kfr8t.json";

        assert_eq!(
            display_name_for_width("artifact.txt", 320.0, 7.1),
            "artifact.txt"
        );
        assert_eq!(
            display_name_for_width(name, 180.0, 8.8),
            "sinomo-ag…kfr8t.json"
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
            place.label == "Working folder"
                && place.detail == "app working dir"
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
    fn source_selector_uses_segmented_control_and_switches_to_local() -> sui::Result<()> {
        let root = files_test_temp_dir("source-selector");
        fs::write(root.join("local.txt"), b"local").expect("write local file");

        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/services".to_string();
            files.sifs_path = "/services".to_string();
            files.local_path = root.to_string_lossy().into_owned();
            files.entries = vec![SifsEntry {
                name: "artifact.txt".to_string(),
                path: "/services/artifact.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(5),
            }];
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files source selector")
                    .root(
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
        let group = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == sui::SemanticsRole::RadioGroup
                    && node.name.as_deref() == Some("Files source")
            })
            .expect("files source segmented control should expose a radio group");
        assert_eq!(
            group.value,
            Some(sui::SemanticsValue::Text("SIFS".to_string()))
        );
        let sifs = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == sui::SemanticsRole::RadioButton
                    && node.name.as_deref() == Some("Browse SIFS")
            })
            .expect("SIFS source option should be exposed");
        assert!(sifs.state.selected);
        assert_eq!(sifs.state.checked, Some(sui::ToggleState::Checked));
        let local = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == sui::SemanticsRole::RadioButton
                    && node.name.as_deref() == Some("Browse local files")
            })
            .expect("local source option should be exposed");
        assert_eq!(local.state.checked, Some(sui::ToggleState::Unchecked));

        window
            .get_by_role(sui::SemanticsRole::RadioButton)
            .with_name("Browse local files")
            .click()?;

        let files = shared.files();
        assert_eq!(files.source, FilesSourceKind::Local);
        assert_eq!(files.path, root.to_string_lossy());
        assert!(
            files
                .entries
                .iter()
                .any(|entry| entry.name == "local.txt" && !entry.is_dir)
        );

        let _ = fs::remove_dir_all(root);
        Ok(())
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
                node.role == sui::SemanticsRole::ListItem
                    && node.name.as_deref() == Some("services")
                    && node.description.as_deref().is_some_and(|description| {
                        description.contains("Single click selects; double click opens the folder")
                    })
            })
            .expect("directory row semantics should be present");
        assert!(
            row.description.as_deref().is_some_and(|description| {
                description.contains("Single click selects; double click opens the folder")
            }),
            "directory row should explain the select versus open split"
        );
        let places_list = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == sui::SemanticsRole::List
                    && node.name.as_deref() == Some("Files places")
            })
            .expect("places rail should be exposed as a navigable list");
        assert!(
            places_list.actions.contains(&sui::SemanticsAction::Focus),
            "places list should be focusable"
        );
        let services_place = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == sui::SemanticsRole::ListItem
                    && node.name.as_deref() == Some("Services")
                    && node.description.as_deref() == Some("service data")
            })
            .expect("services place should be exposed as a list item");
        assert!(
            services_place
                .actions
                .contains(&sui::SemanticsAction::Activate),
            "places entries should remain activatable"
        );

        window
            .get_by_role(sui::SemanticsRole::ListItem)
            .with_name("Services")
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
    fn local_folder_copy_plans_reject_nested_symbolic_links() {
        let root = files_test_temp_dir("local-copy-nested-symlink");
        let source = root.join("workspace");
        let nested = source.join("nested");
        let outside = root.join("outside.txt");
        let link = nested.join("outside-link.txt");
        fs::create_dir_all(&nested).expect("create source tree");
        fs::write(&outside, b"must not be imported").expect("write link target");

        #[cfg(unix)]
        let link_result = std::os::unix::fs::symlink(&outside, &link);
        #[cfg(windows)]
        let link_result = std::os::windows::fs::symlink_file(&outside, &link);
        #[cfg(not(any(unix, windows)))]
        {
            let _ = fs::remove_dir_all(root);
            return;
        }
        if let Err(error) = link_result {
            eprintln!("symbolic links are unavailable for this test: {error}");
            let _ = fs::remove_dir_all(root);
            return;
        }

        let mut local_plan = Vec::new();
        let local_error =
            collect_local_copy_plan(&source, &root.join("workspace copy"), &mut local_plan, None)
                .expect_err("nested symbolic link must stop a local folder copy plan");
        assert!(matches!(
            local_error,
            FilesOperationFailure::Error(message) if message.contains("symbolic link")
        ));
        assert!(!root.join("workspace copy").exists());

        let mut sifs_plan = Vec::new();
        let sifs_error =
            collect_local_to_sifs_copy_plan(&source, "/services/workspace", &mut sifs_plan, None)
                .expect_err("nested symbolic link must stop a SIFS folder import plan");
        assert!(matches!(
            sifs_error,
            FilesOperationFailure::Error(message) if message.contains("symbolic link")
        ));

        let direct_target = root.join("copied-link.txt");
        let direct_error = copy_local_file_with_progress(
            &link.to_string_lossy(),
            &direct_target.to_string_lossy(),
            None,
        )
        .expect_err("file copy must revalidate a symbolic-link source at use time");
        assert!(matches!(
            direct_error,
            FilesOperationFailure::Error(message) if message.contains("symbolic link")
        ));
        assert!(!direct_target.exists());

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

        assert_eq!(
            inspector_summary(&shared),
            "error|Sifs|/|device authorization was denied|false"
        );
        shared.files_mut().inspector_details_open = true;
        assert_eq!(
            inspector_summary(&shared),
            "error|Sifs|/|device authorization was denied|true"
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
        assert_eq!(
            inspector_summary(&shared),
            "loading|Sifs|/services|false|None"
        );

        {
            let mut files = shared.files_mut();
            files.inspector_details_open = true;
        }
        assert_eq!(
            inspector_summary(&shared),
            "loading|Sifs|/services|true|None"
        );

        {
            let mut files = shared.files_mut();
            files.loading = false;
            files.entries.clear();
        }
        assert_eq!(
            inspector_summary(&shared),
            "unselected|Sifs|/services|false|0|true|Not checked yet; refresh this folder to update health"
        );
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

        assert!(inspector_summary(&shared).contains(
            "/artifact.txt|false|false|true|false|device authorization was denied|false||false|"
        ));

        shared.files_mut().inspector_details_open = true;
        assert!(inspector_summary(&shared).contains(
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
            *label == "SIFS health"
                && value == "Not checked yet; refresh this folder to update health"
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
        assert!(!rows.iter().any(|(label, _)| *label == "SIFS health"));
    }

    #[test]
    fn unselected_sifs_inspector_rows_surface_namespace_health() {
        let backend = Backend::local_fallback();
        let mut files = FilesState::new(&backend);
        files.loaded = true;
        files.loading = false;
        files.source = FilesSourceKind::Sifs;
        files.clustered = true;
        files.path = "/".to_string();
        files.sifs_health = Some(degraded_sifs_health());

        let rows = unselected_inspector_rows(&files);
        assert!(
            rows.iter()
                .any(|(label, value)| { *label == "SIFS health" && value == "Needs attention" })
        );
        assert!(!rows.iter().any(|(label, _)| *label == "DFS detail"));

        files.inspector_details_open = true;
        let rows = unselected_inspector_rows(&files);
        assert!(rows.iter().any(|(label, value)| {
            *label == "DFS detail" && value == "DFS 21 conflicts, 18 repairs, 43 pending writes"
        }));

        files.sifs_health = None;
        files.sifs_health_error = Some("metadata root missing".to_string());
        let rows = unselected_inspector_rows(&files);
        assert!(rows.iter().any(|(label, value)| {
            *label == "SIFS health" && value == "Health unavailable; open Connection or Cluster"
        }));
        assert!(rows.iter().any(|(label, value)| {
            *label == "DFS detail" && value == "DFS health unavailable: metadata root missing"
        }));
    }

    #[test]
    fn current_sifs_location_details_button_reveals_diagnostics() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.source = FilesSourceKind::Sifs;
            files.clustered = true;
            files.path = "/".to_string();
            files.daemon_address = Some("127.0.0.1:21353".to_string());
            files.entries = vec![SifsEntry {
                name: "artifact.txt".to_string(),
                path: "/artifact.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(5),
            }];
            files.sifs_health = Some(degraded_sifs_health());
        }

        assert!(
            !unselected_inspector_rows(&shared.files())
                .iter()
                .any(|(label, _)| *label == "DFS detail")
        );

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new().title("Files root details").root(
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
            .with_name("Show details")
            .click()?;
        assert!(shared.files().inspector_details_open);
        let rows = unselected_inspector_rows(&shared.files());
        assert!(
            rows.iter()
                .any(|(label, value)| { *label == "Daemon" && value == "daemon 127.0.0.1:21353" })
        );
        assert!(rows.iter().any(|(label, value)| {
            *label == "DFS detail" && value == "DFS 21 conflicts, 18 repairs, 43 pending writes"
        }));

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Hide details")
            .click()?;
        assert!(!shared.files().inspector_details_open);

        Ok(())
    }

    #[test]
    fn unselected_sifs_inspector_actions_refresh_current_folder() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let (tx, rx) = mpsc::channel();
        let (_result_tx, result_rx) = mpsc::channel();
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.source = FilesSourceKind::Sifs;
            files.clustered = true;
            files.path = "/cluster-tests".to_string();
            files.entries = vec![SifsEntry {
                name: "artifact.txt".to_string(),
                path: "/cluster-tests/artifact.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(5),
            }];
            files.sifs_health = Some(degraded_sifs_health());
            files.worker_tx = Some(tx);
            files.worker_rx = Some(result_rx);
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files root refresh action")
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
            .with_name("Refresh folder")
            .click()?;

        match rx.try_recv().expect("folder refresh command") {
            FilesWorkerCommand::List {
                source,
                path,
                request_id,
                sort_mode,
                offset,
                limit,
                append,
            } => {
                assert_eq!(source, FilesSourceKind::Sifs);
                assert_eq!(path, "/cluster-tests");
                assert_eq!(sort_mode, FileSortMode::NameAsc);
                assert_eq!(offset, 0);
                assert_eq!(limit, Some(FILES_LIST_PAGE_SIZE));
                assert!(!append);
                assert_eq!(shared.files().pending_request_id, Some(request_id));
            }
            FilesWorkerCommand::Operation { .. } => panic!("expected list command"),
            FilesWorkerCommand::Details { .. } => panic!("expected list command"),
            FilesWorkerCommand::Preview { .. } => panic!("expected list command"),
            _ => panic!("expected list command"),
        }

        Ok(())
    }

    #[test]
    fn unselected_sifs_inspector_action_repairs_current_location() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let (tx, rx) = mpsc::channel();
        let (_result_tx, result_rx) = mpsc::channel();
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.source = FilesSourceKind::Sifs;
            files.clustered = true;
            files.path = "/cluster-tests".to_string();
            files.entries = vec![SifsEntry {
                name: "artifact.txt".to_string(),
                path: "/cluster-tests/artifact.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(5),
            }];
            files.sifs_health = Some(degraded_sifs_health());
            files.worker_tx = Some(tx);
            files.worker_rx = Some(result_rx);
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files root repair action")
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
            .with_name("Repair replicas")
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
                assert_eq!(operation.path, "/cluster-tests");
                assert!(operation.directory);
            }
            FilesWorkerCommand::List { .. } => panic!("expected operation command"),
            FilesWorkerCommand::Details { .. } => panic!("expected operation command"),
            FilesWorkerCommand::Preview { .. } => panic!("expected operation command"),
            _ => panic!("expected operation command"),
        }

        Ok(())
    }

    #[test]
    fn unselected_sifs_inspector_action_opens_cluster() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.source = FilesSourceKind::Sifs;
            files.clustered = true;
            files.path = "/".to_string();
            files.entries = vec![SifsEntry {
                name: "artifact.txt".to_string(),
                path: "/artifact.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(5),
            }];
            files.sifs_health = Some(degraded_sifs_health());
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files root recovery actions")
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
            .with_name("Open Cluster")
            .click()?;
        assert_eq!(shared.borrow().active_tab, Tab::Cluster);

        Ok(())
    }

    #[test]
    fn unselected_sifs_inspector_action_opens_connection_settings() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.source = FilesSourceKind::Sifs;
            files.clustered = true;
            files.path = "/".to_string();
            files.entries = vec![SifsEntry {
                name: "artifact.txt".to_string(),
                path: "/artifact.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(5),
            }];
            files.sifs_health_error = Some("metadata root missing".to_string());
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files root connection action")
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
    fn sifs_unavailable_rows_hide_diagnostics_until_expanded() {
        let loading = sifs_loading_rows("/services", false, None);
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
        let slow = sifs_loading_rows("/services", true, Some(20));
        assert!(
            slow.iter()
                .any(|(label, value)| *label == "Status" && value == "SIFS is still loading")
        );
        assert!(
            slow.iter()
                .any(|(label, value)| *label == "Wait" && value.contains("20 seconds"))
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
            metadata_shard: None,
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
            metadata_shard: None,
            health: None,
        };
        assert_eq!(
            sifs_replication_summary(Some(&details), true),
            "one replica location recorded; repair status not loaded"
        );
        details.current_locations.push("node-b".to_string());
        assert_eq!(
            sifs_replication_summary(Some(&details), true),
            "2 replica locations recorded; repair status not loaded"
        );
        details.health = Some(SifsObjectHealth {
            agent_state: "pending-repair".to_string(),
            version_count: 2,
            receipt_count: 1,
            intent_count: 1,
            open_conflict_count: 0,
            pending_repair_count: 1,
            missing_replica_nodes: vec!["node-c".to_string()],
        });
        assert_eq!(
            sifs_replication_summary(Some(&details), true),
            "replicated; 2/3 replicas, 1 repair pending"
        );
        details.health.as_mut().unwrap().pending_repair_count = 0;
        details
            .health
            .as_mut()
            .unwrap()
            .missing_replica_nodes
            .clear();
        assert_eq!(
            sifs_replication_summary(Some(&details), true),
            "replicated; 2 replicas current"
        );
        details.health.as_mut().unwrap().open_conflict_count = 1;
        assert_eq!(
            sifs_replication_summary(Some(&details), true),
            "replicated; 2 replicas, 1 conflict open"
        );
        details.policy = "local-owned".to_string();
        assert_eq!(
            sifs_replication_summary(Some(&details), true),
            "local placement policy"
        );
    }

    #[test]
    fn active_sifs_row_meta_surfaces_replica_repair_state() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.source = FilesSourceKind::Sifs;
        state.clustered = true;
        state.details_loading = false;
        state.details_error = None;
        state.selected = Some(0);
        state.entries = vec![SifsEntry {
            name: "artifact.txt".to_string(),
            path: "/artifact.txt".to_string(),
            is_dir: false,
            is_link: false,
            size: Some(5),
        }];
        let mut details = test_sifs_details(Vec::new());
        details.health = Some(SifsObjectHealth {
            agent_state: "pending-repair".to_string(),
            version_count: 1,
            receipt_count: 1,
            intent_count: 1,
            open_conflict_count: 0,
            pending_repair_count: 1,
            missing_replica_nodes: vec!["node-b".to_string()],
        });
        state.selected_details = Some(details);

        let entry = state.entries[0].clone();
        assert_eq!(
            entry_row_meta(&entry, &state, true),
            "5 B · 1/2 replicas · 1 repair"
        );
        assert_eq!(entry_row_meta(&entry, &state, false), "5 B");
    }

    #[test]
    fn file_row_placement_badge_maps_sifs_sync_truth() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.source = FilesSourceKind::Sifs;
        state.clustered = true;
        state.details_loading = false;
        state.details_error = None;
        state.selected = Some(0);
        state.entries = vec![SifsEntry {
            name: "artifact.txt".to_string(),
            path: "/artifact.txt".to_string(),
            is_dir: false,
            is_link: false,
            size: Some(5),
        }];
        let entry = state.entries[0].clone();

        let mut details = test_sifs_details(Vec::new());
        details.current_locations = vec!["node-a".to_string(), "node-b".to_string()];
        details.health = Some(SifsObjectHealth {
            agent_state: "current".to_string(),
            version_count: 1,
            receipt_count: 2,
            intent_count: 0,
            open_conflict_count: 0,
            pending_repair_count: 0,
            missing_replica_nodes: Vec::new(),
        });
        state.selected_details = Some(details.clone());
        assert_eq!(
            file_row_placement_badge(&entry, &state, 0),
            FileRowPlacementBadge {
                label: "synced".to_string(),
                tone: SemanticTone::Success,
                icon: IconGlyph::Storage,
                replicas: Some((2, 2)),
            }
        );

        details.health = Some(SifsObjectHealth {
            agent_state: "pending-repair".to_string(),
            version_count: 1,
            receipt_count: 1,
            intent_count: 1,
            open_conflict_count: 0,
            pending_repair_count: 1,
            missing_replica_nodes: vec!["node-c".to_string()],
        });
        state.selected_details = Some(details);
        assert_eq!(
            file_row_placement_badge(&entry, &state, 0),
            FileRowPlacementBadge {
                label: "repair".to_string(),
                tone: SemanticTone::Warning,
                icon: IconGlyph::Redo,
                replicas: Some((2, 3)),
            }
        );

        state.details_error = Some("metadata failed".to_string());
        assert_eq!(
            file_row_placement_badge(&entry, &state, 0),
            FileRowPlacementBadge {
                label: "failed".to_string(),
                tone: SemanticTone::Danger,
                icon: IconGlyph::Alert,
                replicas: None,
            }
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
            metadata_shard: None,
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
            "pending-repair; 1 repair pending for …e-b-long-identifier"
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
            metadata_shard: None,
            health: Some(SifsObjectHealth {
                agent_state: "current".to_string(),
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
            "2 replicas healthy"
        );

        details.health.as_mut().unwrap().pending_repair_count = 1;
        assert_eq!(
            sifs_compact_placement_summary(Some(&details), true, false, None),
            "2/3 replicas · 1 repair"
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
            metadata_shard: None,
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
        details.health = Some(SifsObjectHealth {
            agent_state: "current".to_string(),
            version_count: 1,
            receipt_count: 2,
            intent_count: 0,
            open_conflict_count: 0,
            pending_repair_count: 0,
            missing_replica_nodes: Vec::new(),
        });
        assert_eq!(
            sifs_placement_next_step(Some(&details), true),
            "placement is healthy; use details for object and version ids"
        );
        details.health.as_mut().unwrap().open_conflict_count = 1;
        assert_eq!(
            sifs_placement_next_step(Some(&details), true),
            "resolve SIFS conflicts before changing placement"
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
            "Show details, then refresh placement"
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
            metadata_shard: None,
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
        let mut details = test_sifs_details(vec![SifsPinDetails {
            pinned_by_node_id: "node-a".to_string(),
            mode: "content".to_string(),
            created_at_unix_nanos: Some(0),
        }]);

        assert_eq!(
            sifs_pin_summary(None),
            "pin state unknown; refresh metadata to manage pins"
        );
        assert_eq!(sifs_pin_summary(Some(&details)), "content pinned on node-a");
        assert!(sifs_pin_detail_summary(&details).contains("content on node-a since"));

        let pinned = sifs_details_signature(Some(&details));
        details.local_pins.clear();
        assert_eq!(
            sifs_pin_summary(Some(&details)),
            "no local pins on this node"
        );
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
        assert_eq!(files_status_bar_selection(&state), "1 selected");
        assert_eq!(
            files_status_summary(&state),
            "Local SIFS | Path /cluster-tests | 1 entry | 1 entry selected | 1 task done"
        );

        state.clustered = true;
        state.sifs_health = Some(SifsHealthSummary {
            file_system_count: 1,
            object_count: 776,
            version_count: 2482,
            pending_write_count: 43,
            open_conflict_count: 21,
            pending_repair_count: 18,
            pending_namespace_job_count: 0,
            failed_namespace_job_count: 0,
            metadata_shard_count: 3,
            repair_attempt_count: 18,
        });
        assert_eq!(files_source_status(&state), "SIFS degraded");
        assert_eq!(
            files_status_summary(&state),
            "SIFS degraded | Path /cluster-tests | 1 entry | 1 entry selected | 1 task done"
        );

        state.sifs_health = None;
        state.sifs_health_error = Some("metadata root missing".to_string());
        assert_eq!(files_source_status(&state), "SIFS unknown");
        assert_eq!(
            files_status_summary(&state),
            "SIFS unknown | Path /cluster-tests | 1 entry | 1 entry selected | 1 task done"
        );
    }

    #[test]
    fn files_status_summary_distinguishes_remote_sifs_daemon() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.loaded = true;
        state.loading = false;
        state.path = "/sinomo/nodes".to_string();
        state.daemon_address = Some("sodium.sinomo.net:21353".to_string());
        state.clustered = true;

        assert_eq!(daemon_address_host("127.0.0.1:21353"), "127.0.0.1");
        assert!(!daemon_address_is_remote("127.0.0.1:21353"));
        assert!(daemon_address_is_remote("sodium.sinomo.net:21353"));
        assert_eq!(files_source_status(&state), "Remote SIFS");
        assert_eq!(
            files_status_summary(&state),
            "Remote SIFS | Path /sinomo/nodes | 0 entries | no selection | idle"
        );

        state.daemon_address = Some("127.0.0.1:21353".to_string());
        assert_eq!(files_source_status(&state), "SIFS online");
        assert_eq!(
            files_sifs_connection_detail(&state).as_deref(),
            Some("daemon 127.0.0.1:21353")
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
        assert_eq!(task_strip_detail_display(&sifs_task), "Authorize SIFS");

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

        state.details_error = Some("selected object metadata failed".to_string());
        assert_eq!(
            state.connection_attention_detail(),
            Some("SIFS metadata unavailable: selected object metadata failed".to_string())
        );

        state.error = Some("daemon connection refused".to_string());
        assert_eq!(
            state.connection_attention_detail(),
            Some("SIFS unavailable: daemon connection refused".to_string())
        );
    }

    #[test]
    fn embedded_daemon_failure_initializes_files_error() {
        let mut backend = Backend::local_fallback();
        backend.connection = Connection::LocalFallback {
            reason: Some("embedded daemon exited before ready: duplicate config key".to_string()),
        };

        let state = FilesState::new(&backend);

        assert_eq!(
            state.error.as_deref(),
            Some(
                "embedded daemon failed: embedded daemon exited before ready: duplicate config key; daemon-backed chat and SIFS are unavailable"
            )
        );
        assert_eq!(
            state.connection_attention_detail().as_deref(),
            Some(
                "SIFS unavailable: embedded daemon failed: embedded daemon exited before ready: duplicate config key; daemon-backed chat and SIFS are unavailable"
            )
        );
    }

    #[test]
    fn middle_short_keeps_long_sifs_names_single_line() {
        let name = "sinomo-agent-channel-01ktsg9dw850xkprvhkn0kfr8t.json";
        let compact = middle_short(name, 36);

        assert_eq!(compact.chars().count(), 36);
        assert!(compact.starts_with("sinomo-agent"));
        assert!(compact.ends_with("hkn0kfr8t.json"));
        assert!(compact.contains('…'));
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
            append: false,
            result: FilesListResult::Ok(FilesListing {
                node_id: "node-a".to_string(),
                health: None,
                health_error: None,
                total_count: 1,
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

        let health_changed = state.apply_worker_result(FilesWorkerResult::Health {
            request_id: 7,
            source: FilesSourceKind::Sifs,
            health: Some(degraded_sifs_health()),
            health_error: None,
        });

        assert!(health_changed);
        assert!(state.sifs_health.is_some());
        assert_eq!(state.list_revision, 2);

        state.pending_request_id = Some(8);
        let stale_health_changed = state.apply_worker_result(FilesWorkerResult::Health {
            request_id: 7,
            source: FilesSourceKind::Sifs,
            health: None,
            health_error: Some("stale health".to_string()),
        });

        assert!(!stale_health_changed);
        assert!(state.sifs_health.is_some());
        assert_eq!(state.sifs_health_error, None);
        assert_eq!(state.list_revision, 2);
    }

    #[test]
    fn slow_listing_notice_keeps_request_alive_for_late_success() {
        let backend = Backend::local_fallback();
        let mut state = FilesState::new(&backend);
        state.loading = true;
        state.pending_request_id = Some(8);
        state.path = "/".to_string();

        let slow_changed = state.apply_worker_result(FilesWorkerResult::List {
            request_id: 8,
            source: FilesSourceKind::Sifs,
            path: "/services".to_string(),
            append: false,
            result: FilesListResult::Slow { elapsed_secs: 20 },
        });

        assert!(slow_changed);
        assert!(state.loading);
        assert_eq!(state.pending_request_id, Some(8));
        assert_eq!(state.error, None);
        assert_eq!(state.list_slow_after_secs, Some(20));

        let success_changed = state.apply_worker_result(FilesWorkerResult::List {
            request_id: 8,
            source: FilesSourceKind::Sifs,
            path: "/services".to_string(),
            append: false,
            result: FilesListResult::Ok(FilesListing {
                node_id: "node-a".to_string(),
                health: None,
                health_error: None,
                total_count: 1,
                entries: vec![SifsEntry {
                    name: "sinomo".to_string(),
                    path: "/services/sinomo".to_string(),
                    is_dir: true,
                    is_link: false,
                    size: None,
                }],
            }),
        });

        assert!(success_changed);
        assert!(!state.loading);
        assert_eq!(state.pending_request_id, None);
        assert_eq!(state.error, None);
        assert_eq!(state.list_slow_after_secs, None);
        assert_eq!(state.path, "/services");
        assert_eq!(state.entries.len(), 1);
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
            append: false,
            result: FilesListResult::Err("timed out".to_string()),
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
                metadata_shard: None,
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
                sort_mode,
                offset,
                limit,
                append,
            } => {
                assert_eq!(request_id, 0);
                assert_eq!(source, FilesSourceKind::Sifs);
                assert_eq!(path, "/services");
                assert_eq!(sort_mode, FileSortMode::NameAsc);
                assert_eq!(offset, 0);
                assert_eq!(limit, Some(FILES_LIST_PAGE_SIZE));
                assert!(!append);
            }
            FilesWorkerCommand::Details { .. } => panic!("expected list command"),
            FilesWorkerCommand::Preview { .. } => panic!("expected list command"),
            FilesWorkerCommand::Operation { .. } => panic!("expected list command"),
            _ => panic!("expected list command"),
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
            _ => panic!("expected operation command"),
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
    fn local_listing_page_sorts_and_paginates() {
        let root = files_test_temp_dir("local-page");
        fs::create_dir(root.join("folder")).expect("create folder");
        fs::write(root.join("small.txt"), b"small").expect("write small");
        fs::write(root.join("large.bin"), vec![0_u8; 4096]).expect("write large");
        let root_label = root.to_string_lossy().into_owned();

        let page =
            list_local_dir_page(&root_label, FileSortMode::SizeDesc, 1, Some(1)).expect("page");

        assert_eq!(page.total_count, 3);
        assert_eq!(page.offset, 1);
        assert_eq!(page.limit, Some(1));
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].name, "large.bin");

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn selected_entries_queue_batch_delete_tasks() {
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
            Some("type DELETE to remove 2 selected entries")
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
                _ => panic!("expected operation command"),
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
                _ => panic!("expected operation command"),
            }
        }

        let _ = fs::remove_dir_all(&state.local_path);
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
                node.role == sui::SemanticsRole::GenericContainer
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
    fn files_table_exposes_column_context() -> sui::Result<()> {
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
        let table = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == sui::SemanticsRole::Table
                    && node.name.as_deref() == Some("files-listing")
            })
            .expect("files table semantics");
        assert_eq!(
            table.value,
            Some(sui::SemanticsValue::Text("2 rows".to_string()))
        );
        assert_eq!(
            table.description.as_deref(),
            Some("Columns: Name, Placement, Kind / size")
        );

        Ok(())
    }

    #[test]
    fn rendered_file_rows_select_matching_entry() -> sui::Result<()> {
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

        window
            .get_by_role(sui::SemanticsRole::ListItem)
            .with_name("large.bin")
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
    fn directory_row_single_click_selects_without_navigation() -> sui::Result<()> {
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
            .get_by_role(sui::SemanticsRole::ListItem)
            .with_name("dir")
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

        for (label, name) in [("Rename", "Rename"), ("Delete", "Delete selected item")] {
            let node = snapshot
                .accessibility
                .nodes
                .iter()
                .find(|node| {
                    node.role == sui::SemanticsRole::Button && node.name.as_deref() == Some(name)
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
    fn files_toolbar_repairs_replicas_and_row_semantics_show_placement() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.source = FilesSourceKind::Sifs;
            files.loaded = true;
            files.loading = false;
            files.error = None;
            files.clustered = true;
            files.path = "/".to_string();
            files.entries = vec![SifsEntry {
                name: "artifact.txt".to_string(),
                path: "/artifact.txt".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(5),
            }];
            files.selected = Some(0);
            let mut details = test_sifs_details(Vec::new());
            details.health = Some(SifsObjectHealth {
                agent_state: "pending-repair".to_string(),
                version_count: 1,
                receipt_count: 1,
                intent_count: 1,
                open_conflict_count: 0,
                pending_repair_count: 1,
                missing_replica_nodes: vec!["node-b".to_string()],
            });
            files.selected_details = Some(details);
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files replica repair")
                    .root(
                        SizedBox::new()
                            .width(1280.0)
                            .height(720.0)
                            .with_child(view(shared.clone())),
                    ),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        app.run_until_idle()?;

        let snapshot = window.snapshot()?;
        let row = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == sui::SemanticsRole::ListItem
                    && node.name.as_deref() == Some("artifact.txt")
            })
            .expect("artifact row semantics");
        assert!(
            row.description.as_deref().is_some_and(
                |description| description.contains("Placement 5 B · 1/2 replicas · 1 repair")
            ),
            "selected SIFS row should expose compact placement metadata, got {:?}",
            row.description
        );

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Repair replicas")
            .click()?;

        let files = shared.files();
        assert!(
            files
                .tasks
                .iter()
                .any(|task| task.title == "Repair replicas"),
            "toolbar repair action should queue a background file task"
        );

        Ok(())
    }

    #[test]
    fn file_rows_do_not_expose_inline_copy_or_transfer_actions() -> sui::Result<()> {
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
                        .width(1180.0)
                        .height(720.0)
                        .with_child(view(shared.clone())),
                ),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        let snapshot = window.snapshot()?;
        for hidden_name in [
            "Copy artifact.txt",
            "Download artifact.txt",
            "Move artifact.txt",
            "Rename artifact.txt",
            "Delete artifact.txt",
        ] {
            assert!(
                !snapshot.accessibility.nodes.iter().any(|node| {
                    node.role == sui::SemanticsRole::Button
                        && node.name.as_deref() == Some(hidden_name)
                }),
                "{hidden_name} should not be exposed as an inline row action"
            );
        }

        window
            .get_by_role(sui::SemanticsRole::ListItem)
            .with_name("artifact.txt")
            .click()?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Copy to")
            .click()?;
        let files = shared.files();
        let prompt = files.action_prompt.as_ref().expect("copy prompt");
        assert_eq!(prompt.kind, FileActionPromptKind::Copy);
        assert_eq!(prompt.target_path.as_deref(), Some("/artifact.txt"));
        assert_eq!(files.selected, Some(0));

        Ok(())
    }

    #[test]
    fn selected_row_toolbar_actions_open_item_prompts() -> sui::Result<()> {
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
                        .width(1180.0)
                        .height(720.0)
                        .with_child(view(shared.clone())),
                ),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(sui::SemanticsRole::ListItem)
            .with_name("artifact.txt")
            .click()?;
        assert_eq!(shared.files().selected, Some(0));

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Copy to")
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
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Cancel")
            .click()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Move to")
            .click()?;
        {
            let files = shared.files();
            let prompt = files.action_prompt.as_ref().expect("move prompt");
            assert_eq!(prompt.kind, FileActionPromptKind::Move);
            assert_eq!(
                prompt.target_path.as_deref(),
                Some("/services/artifact.txt")
            );
            assert_eq!(prompt.input, "artifact.txt");
        }
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Cancel")
            .click()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Rename")
            .click()?;
        {
            let files = shared.files();
            let prompt = files.action_prompt.as_ref().expect("rename prompt");
            assert_eq!(prompt.kind, FileActionPromptKind::Rename);
            assert_eq!(
                prompt.target_path.as_deref(),
                Some("/services/artifact.txt")
            );
            assert_eq!(prompt.input, "artifact.txt");
        }
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Cancel")
            .click()?;

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Delete selected item")
            .click()?;
        {
            let files = shared.files();
            let prompt = files.action_prompt.as_ref().expect("delete prompt");
            assert_eq!(prompt.kind, FileActionPromptKind::Delete);
            assert_eq!(
                prompt.target_path.as_deref(),
                Some("/services/artifact.txt")
            );
        }

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Cancel")
            .click()?;

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
            .get_by_role(sui::SemanticsRole::Text)
            .with_name("File task strip")
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
    fn task_strip_announces_overflow_tasks() -> sui::Result<()> {
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
            files.tasks = (0..4)
                .map(|index| FilesTask {
                    id: 90 + index,
                    source: FilesSourceKind::Sifs,
                    title: format!("Copy batch {}", index + 1),
                    detail: format!("/artifact-{index}.bin -> /archive/artifact-{index}.bin"),
                    state: FilesTaskState::Running,
                    progress: Some((index as f32 + 1.0) / 10.0),
                })
                .collect();
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files task strip overflow")
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

        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::Text)
                .with_name("File task strip")
                .with_description("4 active; showing 2 of 4; 2 more tasks in inspector")
                .count()?,
            1
        );
        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::Button)
                .with_name("Cancel Copy batch 4")
                .count()?,
            0
        );

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
    fn inspector_task_queue_exposes_all_retained_tasks() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut files = shared.files_mut();
            files.loaded = true;
            files.loading = false;
            files.path = "/".to_string();
            files.tasks = (0..6)
                .map(|index| FilesTask {
                    id: 100 + index,
                    source: FilesSourceKind::Sifs,
                    title: format!("Copy batch {}", index + 1),
                    detail: format!("/artifact-{index}.bin -> /archive/artifact-{index}.bin"),
                    state: FilesTaskState::Running,
                    progress: Some((index as f32 + 1.0) / 10.0),
                })
                .collect();
        }
        let scroll_state = ScrollState::new();
        let reader: ThemeReader = Rc::new(DefaultTheme::light);

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files full task queue")
                    .root(
                        SizedBox::new()
                            .width(INSPECTOR_W)
                            .height(320.0)
                            .with_child(inspector(&shared, &reader, &scroll_state)),
                    ),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        let _snapshot = window.snapshot()?;
        assert!(
            scroll_state.max_offset().y > 0.0,
            "expanded task queue should remain scrollable through the inspector ScrollView"
        );

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Cancel Copy batch 6")
            .click()?;

        let files = shared.files();
        assert_eq!(files.tasks[5].state, FilesTaskState::CancelRequested);
        assert!(operation_cancelled(&files.worker_cancellations, 105));

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
            .get_by_role(sui::SemanticsRole::ListItem)
            .with_name("dir")
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
            let mut details = test_sifs_details(Vec::new());
            details.metadata_shard = Some(SifsMetadataShardDetails {
                shard_id: "root".to_string(),
                primary_node_id: "node-b".to_string(),
                replica_node_ids: vec!["node-a".to_string(), "node-b".to_string()],
                epoch: 7,
                local_node_id: "node-a".to_string(),
                local_is_primary: false,
                local_is_replica: true,
            });
            files.selected_details = Some(details);
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
            _ => panic!("expected operation command"),
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
            _ => panic!("expected operation command"),
        }

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Make metadata primary artifact.txt")
            .click()?;

        assert_eq!(shared.files().tasks[0].title, "Make metadata primary");
        match rx.try_recv().expect("metadata primary command") {
            FilesWorkerCommand::Operation {
                request_id,
                source,
                operation,
            } => {
                assert_eq!(request_id, shared.files().tasks[0].id);
                assert_eq!(source, FilesSourceKind::Sifs);
                assert_eq!(operation.kind, FilesOperationKind::MakeMetadataPrimary);
                assert_eq!(operation.path, "/services/artifact.txt");
            }
            FilesWorkerCommand::List { .. } => panic!("expected operation command"),
            FilesWorkerCommand::Details { .. } => panic!("expected operation command"),
            FilesWorkerCommand::Preview { .. } => panic!("expected operation command"),
            _ => panic!("expected operation command"),
        }

        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::Button)
                .with_name("Unpin metadata artifact.txt")
                .count()?,
            0,
            "unpinned metadata should expose pin, not unpin"
        );
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Pin metadata artifact.txt")
            .click()?;

        assert_eq!(shared.files().tasks[0].title, "Pin metadata");
        match rx.try_recv().expect("metadata pin command") {
            FilesWorkerCommand::Operation {
                request_id,
                source,
                operation,
            } => {
                assert_eq!(request_id, shared.files().tasks[0].id);
                assert_eq!(source, FilesSourceKind::Sifs);
                assert_eq!(operation.kind, FilesOperationKind::PinMetadata);
                assert_eq!(operation.path, "/services/artifact.txt");
            }
            FilesWorkerCommand::List { .. } => panic!("expected operation command"),
            FilesWorkerCommand::Details { .. } => panic!("expected operation command"),
            FilesWorkerCommand::Preview { .. } => panic!("expected operation command"),
            _ => panic!("expected operation command"),
        }

        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::Button)
                .with_name("Unpin content artifact.txt")
                .count()?,
            0,
            "unpinned content should expose pin, not unpin"
        );
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
            _ => panic!("expected operation command"),
        }

        Ok(())
    }

    #[test]
    fn selected_pinned_sifs_file_inspector_exposes_unpin_actions() -> sui::Result<()> {
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
            files.selected_details = Some(test_sifs_details(vec![
                SifsPinDetails {
                    pinned_by_node_id: "node-a".to_string(),
                    mode: "metadata".to_string(),
                    created_at_unix_nanos: Some(0),
                },
                SifsPinDetails {
                    pinned_by_node_id: "node-a".to_string(),
                    mode: "content".to_string(),
                    created_at_unix_nanos: Some(0),
                },
            ]));
            files.worker_tx = Some(tx);
            files.worker_rx = Some(result_rx);
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files inspector pinned file actions")
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

        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::Button)
                .with_name("Pin metadata artifact.txt")
                .count()?,
            0,
            "pinned metadata should expose unpin, not pin"
        );
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Unpin metadata artifact.txt")
            .click()?;

        assert_eq!(shared.files().tasks[0].title, "Unpin metadata");
        match rx.try_recv().expect("metadata unpin command") {
            FilesWorkerCommand::Operation {
                request_id,
                source,
                operation,
            } => {
                assert_eq!(request_id, shared.files().tasks[0].id);
                assert_eq!(source, FilesSourceKind::Sifs);
                assert_eq!(operation.kind, FilesOperationKind::UnpinMetadata);
                assert_eq!(operation.path, "/services/artifact.txt");
            }
            FilesWorkerCommand::List { .. } => panic!("expected operation command"),
            FilesWorkerCommand::Details { .. } => panic!("expected operation command"),
            FilesWorkerCommand::Preview { .. } => panic!("expected operation command"),
            _ => panic!("expected operation command"),
        }

        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::Button)
                .with_name("Pin content artifact.txt")
                .count()?,
            0,
            "pinned content should expose unpin, not pin"
        );
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
            _ => panic!("expected operation command"),
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

        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::Button)
                .with_name("Repair replicas artifact.txt")
                .count()?,
            0,
            "metadata connection errors should hide replica repair"
        );
        assert_eq!(
            window
                .get_by_role(sui::SemanticsRole::Button)
                .with_name("Pin metadata artifact.txt")
                .count()?,
            0,
            "metadata connection errors should hide pin actions"
        );

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
            .with_name("Show details")
            .click()?;
        assert!(shared.files().inspector_details_open);

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Hide details")
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
            .with_name("Show details")
            .click()?;
        assert!(shared.files().inspector_details_open);

        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Hide details")
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
                name: "sinomo-agent-channel-01ktsg9dv850xkpryhkn0kfr8t.json".to_string(),
                path: "/services/sinomo-agent/channels/01ktsg9dv850xkpryhkn0kfr8t.json".to_string(),
                is_dir: false,
                is_link: false,
                size: Some(4096),
            }];
            files.selected = Some(0);
            files.inspector_details_open = true;
            files.selected_details = Some(SifsObjectDetails {
                file_system_id: "sifs-primary".to_string(),
                object_id: "object-01ktsg9dv850xkpryhkn0kfr8t".to_string(),
                path: "/services/sinomo-agent/channels/01ktsg9dv850xkpryhkn0kfr8t.json".to_string(),
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
                metadata_shard: None,
                health: None,
            });
        }
        let scroll_state = ScrollState::new();
        let reader: ThemeReader = Rc::new(DefaultTheme::light);

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Files scrollable inspector")
                    .root(
                        SizedBox::new()
                            .width(INSPECTOR_W)
                            .height(280.0)
                            .with_child(inspector(&shared, &reader, &scroll_state)),
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

    #[test]
    fn local_document_load_is_complete_beyond_preview_limit() {
        let root = files_test_temp_dir("complete-document");
        let path = root.join("large-source.rs");
        let text = "let complete = true;\n".repeat(3_000);
        assert!(text.len() as u64 > FILE_PREVIEW_MAX_BYTES);
        fs::write(&path, text.as_bytes()).expect("write document");

        let loaded =
            load_local_text_document(path.to_string_lossy().as_ref(), FILE_DOCUMENT_MAX_BYTES)
                .expect("load complete document");

        assert_eq!(loaded.text, text);
        let expected_digest = blake3::hash(text.as_bytes()).to_hex().to_string();
        assert_eq!(
            loaded
                .local_fingerprint
                .as_ref()
                .map(|fingerprint| fingerprint.content_digest.as_str()),
            Some(expected_digest.as_str())
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn lexical_local_normalization_preserves_leading_relative_parents() {
        let normalized = normalize_local_path("../../folder/file.txt").expect("normalize path");
        let parent_count = Path::new(&normalized)
            .components()
            .filter(|component| matches!(component, std::path::Component::ParentDir))
            .count();
        assert_eq!(parent_count, 2, "normalized path was {normalized}");
        let normalized_path = Path::new(&normalized);
        assert_eq!(
            normalized_path.file_name(),
            Some(std::ffi::OsStr::new("file.txt"))
        );
        assert_eq!(
            normalized_path.parent().and_then(Path::file_name),
            Some(std::ffi::OsStr::new("folder"))
        );
    }

    #[test]
    fn local_document_rejects_oversize_and_binary_content_explicitly() {
        let root = files_test_temp_dir("document-bounds");
        let oversize = root.join("oversize.txt");
        fs::write(&oversize, vec![b'x'; FILE_DOCUMENT_MAX_BYTES as usize + 1])
            .expect("write oversize document");
        let error =
            load_local_text_document(oversize.to_string_lossy().as_ref(), FILE_DOCUMENT_MAX_BYTES)
                .expect_err("oversize document must be rejected");
        assert!(error.contains("limit"), "unexpected error: {error}");

        let binary = root.join("binary.dat");
        fs::write(&binary, b"text\0binary").expect("write binary document");
        let error =
            load_local_text_document(binary.to_string_lossy().as_ref(), FILE_DOCUMENT_MAX_BYTES)
                .expect_err("binary document must be rejected");
        assert!(error.contains("binary control bytes"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn local_safe_save_detects_same_length_content_change_by_digest() {
        let root = files_test_temp_dir("document-conflict");
        let path = root.join("notes.txt");
        fs::write(&path, b"first version").expect("write first version");
        let loaded =
            load_local_text_document(path.to_string_lossy().as_ref(), FILE_DOCUMENT_MAX_BYTES)
                .expect("load document");
        let expected = loaded.local_fingerprint.expect("local fingerprint");
        fs::write(&path, b"other version").expect("replace with same-length content");
        let (_, current) =
            read_local_file_snapshot(path.to_string_lossy().as_ref(), FILE_DOCUMENT_MAX_BYTES)
                .expect("read current snapshot");
        assert_eq!(current.len, expected.len);
        assert_ne!(current.content_digest, expected.content_digest);

        let error = save_local_text_document(
            path.to_string_lossy().as_ref(),
            &expected,
            b"edited version",
        )
        .expect_err("stale save must fail");
        assert!(error.contains("Save conflict"));
        assert_eq!(
            fs::read(&path).expect("read untouched conflict"),
            b"other version"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn local_safe_save_returns_committed_receipt() {
        let root = files_test_temp_dir("document-save");
        let path = root.join("notes.txt");
        fs::write(&path, b"before").expect("write before");
        let loaded =
            load_local_text_document(path.to_string_lossy().as_ref(), FILE_DOCUMENT_MAX_BYTES)
                .expect("load document");
        let expected = loaded.local_fingerprint.expect("local fingerprint");

        let result =
            save_local_text_document(path.to_string_lossy().as_ref(), &expected, b"after safely")
                .expect("safe save");
        let FilesDocumentSave::Local {
            fingerprint,
            requires_reload,
            ..
        } = result
        else {
            panic!("expected local receipt");
        };
        assert!(!requires_reload);
        assert_eq!(fingerprint.len, 12);
        assert_eq!(
            fingerprint.content_digest,
            blake3::hash(b"after safely").to_hex().to_string()
        );
        assert_eq!(
            fs::read(&path).expect("read saved document"),
            b"after safely"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn viewer_attachment_uses_dirty_editor_buffer_with_snapshot_provenance() {
        let backend = Backend::local_fallback();
        let mut files = FilesState::new(&backend);
        let root = files_test_temp_dir("editor-attachment");
        let path = root.join("draft.txt");
        fs::write(&path, b"persisted").expect("write persisted");
        let loaded =
            load_local_text_document(path.to_string_lossy().as_ref(), FILE_DOCUMENT_MAX_BYTES)
                .expect("load document");
        files.document = Some(FilesDocument {
            source: loaded.source,
            path: loaded.path,
            title: loaded.title,
            text: "unsaved editor text".to_string(),
            saved_text: loaded.text,
            local_fingerprint: loaded.local_fingerprint,
            sifs_fingerprint: None,
            read_only_reason: None,
            loading: false,
            load_error: None,
            saving: false,
            save_error: None,
            save_warning: None,
        });

        files.start_attach_document(None);
        let intent = files
            .take_ready_attachment_intent()
            .expect("ready attachment intent");
        assert_eq!(intent.bytes, b"unsaved editor text");
        assert!(intent.is_unsaved_editor_buffer);
        assert!(matches!(
            intent.provenance,
            FilesAttachmentProvenance::Local { .. }
        ));
        assert_eq!(fs::read(&path).expect("read persisted"), b"persisted");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn files_picker_toolbar_and_document_viewer_render_at_target_widths() -> sui::Result<()> {
        fn prepared_shared(with_document: bool) -> Shared {
            let backend = Backend::local_fallback();
            let shared = Shared::new(AppState::ready(backend));
            let path = r"C:\workspace\sinomo\src";
            let document_path = format!(r"{path}\service_handler.rs");
            {
                let mut files = shared.files_mut();
                files.source = FilesSourceKind::Local;
                files.loaded = true;
                files.loading = false;
                files.path = path.to_string();
                files.entries = vec![SifsEntry {
                    name: "service_handler.rs".to_string(),
                    path: document_path.clone(),
                    is_dir: false,
                    is_link: false,
                    size: Some(2_048),
                }];
                files.selected = Some(0);
                if with_document {
                    let saved_text =
                        "pub fn handle_request() {\n    tracing::info!(\"ready\");\n}\n";
                    files.document = Some(FilesDocument {
                        source: FilesSourceKind::Local,
                        path: document_path.clone(),
                        title: "service_handler.rs".to_string(),
                        text: format!(
                            "{saved_text}\n// Unsaved verification line that intentionally extends beyond the narrow editor viewport without wrapping."
                        ),
                        saved_text: saved_text.to_string(),
                        local_fingerprint: Some(LocalFileFingerprint {
                            canonical_path: PathBuf::from(&document_path),
                            len: saved_text.len() as u64,
                            modified_at_unix_nanos: Some(1),
                            content_digest: blake3::hash(saved_text.as_bytes())
                                .to_hex()
                                .to_string(),
                            readonly: false,
                            symlink: false,
                        }),
                        sifs_fingerprint: None,
                        read_only_reason: None,
                        loading: false,
                        load_error: None,
                        saving: false,
                        save_error: None,
                        save_warning: None,
                    });
                    files.document_revision = 1;
                }
            }
            shared
        }

        fn named_bounds(
            snapshot: &sui_testing::WindowSnapshot,
            role: sui::SemanticsRole,
            name: &str,
        ) -> Rect {
            snapshot
                .accessibility
                .nodes
                .iter()
                .find(|node| node.role == role && node.name.as_deref() == Some(name))
                .unwrap_or_else(|| panic!("missing {role:?} named {name}"))
                .bounds
        }

        fn assert_inside(bounds: Rect, width: f32, height: f32, name: &str) {
            assert!(
                bounds.x() >= 0.0
                    && bounds.y() >= 0.0
                    && bounds.max_x() <= width
                    && bounds.max_y() <= height,
                "{name} escaped {width}x{height}: {bounds:?}"
            );
        }

        let artifact_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("target")
            .join("codex")
            .join("files-workflow-layout");
        fs::create_dir_all(&artifact_dir).expect("create visual artifact directory");

        for width in [560.0_f32, 760.0_f32] {
            let shared = prepared_shared(false);
            let runtime = sui::Application::new()
                .window(
                    sui::WindowBuilder::new()
                        .title("Files picker toolbar")
                        .root(view(shared.clone())),
                )
                .build()?;
            let app = sui_testing::TestApp::from_runtime(runtime)?;
            let window = app.main_window()?;
            window
                .root()
                .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(width, 720.0))))?;
            window.run_until_idle()?;
            let mut snapshot = window.snapshot()?;
            window
                .capture_screenshot()?
                .write_png(artifact_dir.join(format!("picker-toolbar-{width:.0}.png")))?;
            if width < WIDE_THRESHOLD {
                let more =
                    named_bounds(&snapshot, sui::SemanticsRole::Button, "More file commands");
                let inspector =
                    named_bounds(&snapshot, sui::SemanticsRole::Button, "Open file inspector");
                let location = named_bounds(
                    &snapshot,
                    sui::SemanticsRole::Text,
                    r"Current Local files path C:\workspace\sinomo\src",
                );
                eprintln!(
                    "compact header width={width:.0} location={location:?} more={more:?} inspector={inspector:?}"
                );
                assert!(
                    more.x() >= width - 80.0,
                    "compact commands did not trail the filling location: {more:?}"
                );
                assert!(more.max_x() <= inspector.x());
                assert_inside(inspector, width, HEADER_H, "compact inspector");
                assert!(location.width() >= 200.0 && location.height() <= HEADER_H);
                assert_inside(more, width, 720.0, "compact command overflow");
                window
                    .get_by_role(sui::SemanticsRole::Button)
                    .with_name("More file commands")
                    .click()?;
                window.advance_time(1.0)?;
                window.run_until_idle()?;
                snapshot = window.snapshot()?;
                for name in [
                    "Open local file...",
                    "Choose local folder...",
                    "Import files...",
                    "Import folder...",
                ] {
                    let bounds = named_bounds(&snapshot, sui::SemanticsRole::Button, name);
                    eprintln!("toolbar width={width:.0} {name}: {bounds:?}");
                    assert_inside(bounds, width, 720.0, name);
                    assert!(bounds.width() >= 32.0 && bounds.height() >= 32.0);
                }
            } else {
                for name in [
                    "Open local file",
                    "Choose local folder",
                    "Import files",
                    "More file commands",
                ] {
                    let bounds = named_bounds(&snapshot, sui::SemanticsRole::Button, name);
                    eprintln!("toolbar width={width:.0} {name}: {bounds:?}");
                    assert_inside(bounds, width, HEADER_H, name);
                    assert!(bounds.width() >= 28.0 && bounds.height() >= 28.0);
                }
                window
                    .get_by_role(sui::SemanticsRole::Button)
                    .with_name("More file commands")
                    .click()?;
                window.advance_time(1.0)?;
                window.run_until_idle()?;
                snapshot = window.snapshot()?;
                for name in ["Go to path", "Rename", "Delete selected item"] {
                    let bounds = named_bounds(&snapshot, sui::SemanticsRole::Button, name);
                    eprintln!("overflow width={width:.0} {name}: {bounds:?}");
                    assert_inside(bounds, width, 720.0, name);
                    assert!(bounds.width() >= 32.0 && bounds.height() >= 32.0);
                }
            }
            window
                .capture_screenshot()?
                .write_png(artifact_dir.join(format!("command-overflow-{width:.0}.png")))?;

            let shared = prepared_shared(true);
            let runtime = sui::Application::new()
                .window(
                    sui::WindowBuilder::new()
                        .title("Files document viewer")
                        .root(view(shared)),
                )
                .build()?;
            let app = sui_testing::TestApp::from_runtime(runtime)?;
            let window = app.main_window()?;
            window
                .root()
                .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(width, 720.0))))?;
            window.run_until_idle()?;
            window.advance_time(1.0)?;
            window.run_until_idle()?;
            let snapshot = window.snapshot()?;
            let dialog = named_bounds(&snapshot, sui::SemanticsRole::Dialog, "service_handler.rs");
            let editor = named_bounds(
                &snapshot,
                sui::SemanticsRole::TextInput,
                r"files-document:C:\workspace\sinomo\src\service_handler.rs",
            );
            eprintln!("viewer width={width:.0} dialog={dialog:?} editor={editor:?}");
            assert_inside(dialog, width, 720.0, "document sheet");
            assert_inside(editor, width, 720.0, "document editor");
            assert!(
                editor.width() >= width - 80.0,
                "editor too narrow: {editor:?}"
            );
            assert!(editor.height() >= 360.0, "editor too short: {editor:?}");

            let mut actions = ["Attach to current chat", "Save", "Close"].map(|name| {
                let bounds = named_bounds(&snapshot, sui::SemanticsRole::Button, name);
                eprintln!("viewer width={width:.0} {name}: {bounds:?}");
                assert_inside(bounds, width, 720.0, name);
                assert!(bounds.width() >= 32.0 && bounds.height() >= 28.0);
                bounds
            });
            actions.sort_by(|left, right| left.x().total_cmp(&right.x()));
            for pair in actions.windows(2) {
                assert!(
                    pair[0].max_x() <= pair[1].x(),
                    "document actions overlap at width {width}: {:?} and {:?}",
                    pair[0],
                    pair[1]
                );
            }
            window
                .capture_screenshot()?
                .write_png(artifact_dir.join(format!("document-viewer-{width:.0}.png")))?;
        }
        Ok(())
    }
}
