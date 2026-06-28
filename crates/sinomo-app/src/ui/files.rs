//! "Files" tab — a SIFS distributed-filesystem browser.
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
use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread;
use std::time::Duration;

use sui::{
    Alignment, ArrangeCtx, Breadcrumb, BreadcrumbItem, Color, Constraints, DefaultTheme, Event,
    EventCtx, FontWeight, IconButton, IconGlyph, KeyState, ListItem, ListView, MeasureCtx,
    PaintCtx, Point, PointerButton, PointerEventKind, Rect, ScrollState, ScrollView,
    SemanticsAction, SemanticsCtx, SemanticsNode, SemanticsRole, SemanticsValue, Separator, Size,
    SizedBox, Stack, StackHostOptions, Widget, WidgetPod, WidgetPodMutVisitor, WidgetPodVisitor,
    WindowEvent, draw_glyph,
};

use sinomo_cli::app_api::{SifsBrowser, SifsEntry, SifsObjectDetails};
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
const CONTENT_PAD: f32 = 16.0;
const HEADER_H: f32 = 46.0;
const FILES_STATUS_H: f32 = 38.0;
const LIST_TASK_STRIP_H: f32 = 88.0;
const LOAD_TIMEOUT: Duration = Duration::from_secs(20);

/// A shared closure that resolves the current widget theme — threaded into every control so the
/// Appearance toggle restyles the whole tab live, with no rebuild. (Same pattern as `settings.rs`.)
type ThemeReader = Rc<dyn Fn() -> DefaultTheme>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FileActionPromptKind {
    NewFile,
    NewFolder,
    Rename,
    Delete,
}

impl FileActionPromptKind {
    fn title(self) -> &'static str {
        match self {
            Self::NewFile => "New file",
            Self::NewFolder => "New folder",
            Self::Rename => "Rename",
            Self::Delete => "Delete",
        }
    }

    fn primary_label(self) -> &'static str {
        match self {
            Self::NewFile => "Create file",
            Self::NewFolder => "Create folder",
            Self::Rename => "Rename",
            Self::Delete => "Delete",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FileActionPrompt {
    kind: FileActionPromptKind,
    input: String,
    target_name: String,
    target_path: Option<String>,
    target_is_dir: bool,
    error: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FilesTaskState {
    Running,
    CancelRequested,
    Completed,
    Failed,
}

impl FilesTaskState {
    fn label(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::CancelRequested => "cancelling",
            Self::Completed => "done",
            Self::Failed => "failed",
        }
    }

    fn can_cancel(self) -> bool {
        self == Self::Running
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FilesTask {
    id: u64,
    title: String,
    detail: String,
    state: FilesTaskState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FilesOperationKind {
    CreateFile,
    CreateFolder,
    Rename,
    Delete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FilesOperation {
    kind: FilesOperationKind,
    path: String,
    target: Option<String>,
    directory: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FilesOperationOutcome {
    refresh_path: String,
    select_path: Option<String>,
    message: String,
}

// ---------------------------------------------------------------------------
// State (data layer) — SIFS-backed browser snapshot + async load worker.
// ---------------------------------------------------------------------------

/// State backing the files tab.
pub struct FilesState {
    pub data_dir: Arc<SinomoDataDir>,
    daemon_address: Option<String>,
    app_runtime: tokio::runtime::Handle,
    pub browser: Option<SifsBrowser>,
    pub error: Option<String>,
    pub clustered: bool,
    pub loaded: bool,
    pub loading: bool,
    pub list_revision: u64,
    pub node_id: String,
    pub path: String,
    pub entries: Vec<SifsEntry>,
    pub selected: Option<usize>,
    pub selected_details: Option<SifsObjectDetails>,
    pub details_loading: bool,
    pub details_error: Option<String>,
    action_prompt: Option<FileActionPrompt>,
    tasks: Vec<FilesTask>,
    pending_select_path: Option<String>,
    pub scroll: f32,
    pub inspector_details_open: bool,
    worker_tx: Option<Sender<FilesWorkerCommand>>,
    worker_rx: Option<Receiver<FilesWorkerResult>>,
    worker_result_tx: Option<Sender<FilesWorkerResult>>,
    worker_waker: Option<sui::UiHandle>,
    next_request_id: u64,
    pending_request_id: Option<u64>,
    pending_details_request_id: Option<u64>,
}

impl FilesState {
    pub fn new(backend: &Backend) -> Self {
        Self {
            data_dir: backend.data_dir.clone(),
            daemon_address: backend.connection.daemon_address().map(str::to_string),
            app_runtime: backend.app_runtime.handle().clone(),
            browser: None,
            error: None,
            clustered: backend.connection.is_clustered(),
            loaded: false,
            loading: false,
            list_revision: 0,
            node_id: String::new(),
            path: "/".to_string(),
            entries: Vec::new(),
            selected: None,
            selected_details: None,
            details_loading: false,
            details_error: None,
            action_prompt: None,
            tasks: Vec::new(),
            pending_select_path: None,
            scroll: 0.0,
            inspector_details_open: false,
            worker_tx: None,
            worker_rx: None,
            worker_result_tx: None,
            worker_waker: None,
            next_request_id: 0,
            pending_request_id: None,
            pending_details_request_id: None,
        }
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
        self.clear_selected_details();
        self.loading = false;
        let path = self.path.clone();
        let listing = self.browser.as_ref().map(|b| b.list_shallow(&path));
        match listing {
            Some(Ok(entries)) => {
                self.entries = entries;
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
        self.clear_selected_details();
        self.loading = true;
        self.error = None;
        self.entries.clear();
        self.list_revision = self.list_revision.wrapping_add(1);
        let command = FilesWorkerCommand::List {
            request_id,
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
            let waker = self.worker_waker.clone();
            let _ = thread::Builder::new()
                .name("sinomo-app-files-timeout".to_string())
                .spawn(move || {
                    thread::sleep(LOAD_TIMEOUT);
                    let _ = result_tx.send(FilesWorkerResult::List {
                        request_id,
                        path,
                        result: Err(format!(
                            "SIFS listing did not finish within {} seconds",
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
        self.clear_selected_details();
        self.node_id.clear();
        self.inspector_details_open = false;
        self.loaded = false;
        self.loading = false;
        self.pending_request_id = None;
        self.pending_details_request_id = None;
        self.worker_tx = None;
        self.worker_rx = None;
        self.worker_result_tx = None;
        self.worker_waker = None;
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
                path,
                result,
            } => {
                if self.pending_request_id != Some(request_id) {
                    return false;
                }
                self.pending_request_id = None;
                self.loading = false;
                self.path = path;
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
                path,
                result,
            } => {
                if self.pending_details_request_id != Some(request_id) {
                    return false;
                }
                self.pending_details_request_id = None;
                self.details_loading = false;
                let selected_path = self.selected_entry().map(|entry| entry.path.as_str());
                if selected_path != Some(path.as_str()) {
                    return false;
                }
                match result {
                    Ok(details) => {
                        self.selected_details = Some(details);
                        self.details_error = None;
                    }
                    Err(error) => {
                        self.selected_details = None;
                        self.details_error = Some(error);
                    }
                }
                true
            }
            FilesWorkerResult::Operation { request_id, result } => {
                self.apply_operation_result(request_id, result)
            }
        }
    }

    fn navigate(&mut self, path: String) {
        let normalized = self
            .browser
            .as_ref()
            .map(|b| b.normalize(&path))
            .unwrap_or_else(|| normalize_path(&path));
        self.path = normalized;
        self.reload();
    }

    fn go_up(&mut self) {
        let parent = self
            .browser
            .as_ref()
            .and_then(|b| b.parent(&self.path))
            .or_else(|| parent_path(&self.path));
        if let Some(parent) = parent {
            self.navigate(parent);
        }
    }

    fn clear_selected_details(&mut self) {
        self.selected_details = None;
        self.details_loading = false;
        self.details_error = None;
        self.pending_details_request_id = None;
    }

    fn selected_entry(&self) -> Option<&SifsEntry> {
        self.selected.and_then(|i| self.entries.get(i))
    }

    fn request_selected_details(&mut self) {
        let Some(path) = self.selected_entry().map(|entry| entry.path.clone()) else {
            self.clear_selected_details();
            return;
        };
        self.selected_details = None;
        self.details_error = None;
        if let Some(tx) = self.worker_tx.clone() {
            let request_id = self.next_request_id;
            self.next_request_id = self.next_request_id.wrapping_add(1);
            self.pending_details_request_id = Some(request_id);
            self.details_loading = true;
            if tx
                .send(FilesWorkerCommand::Details { request_id, path })
                .is_err()
            {
                self.pending_details_request_id = None;
                self.details_loading = false;
                self.details_error = Some("SIFS browser worker stopped".to_string());
            }
            return;
        }

        self.pending_details_request_id = None;
        self.details_loading = false;
        match self.browser.as_ref().map(|browser| browser.stat(&path)) {
            Some(Ok(details)) => self.selected_details = Some(details),
            Some(Err(error)) => self.details_error = Some(error),
            None => {}
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
        let selected = self.selected_entry().cloned();
        let (target_path, target_name, target_is_dir) = match selected {
            Some(entry) => (Some(entry.path), entry.name, entry.is_dir),
            None => (None, String::new(), false),
        };
        let input = match kind {
            FileActionPromptKind::NewFile => "untitled.txt".to_string(),
            FileActionPromptKind::NewFolder => "New folder".to_string(),
            FileActionPromptKind::Rename => target_name.clone(),
            FileActionPromptKind::Delete => String::new(),
        };
        let error = match kind {
            FileActionPromptKind::Rename | FileActionPromptKind::Delete
                if target_path.is_none() =>
            {
                Some("select an item first".to_string())
            }
            _ => None,
        };
        self.action_prompt = Some(FileActionPrompt {
            kind,
            input,
            target_name,
            target_path,
            target_is_dir,
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

    fn submit_action_prompt(&mut self, waker: Option<sui::UiHandle>) {
        let Some(prompt) = self.action_prompt.clone() else {
            return;
        };
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
            FileActionPromptKind::NewFile => {
                let path = join_sifs_child(&self.path, &prompt.input)?;
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
                let path = join_sifs_child(&self.path, &prompt.input)?;
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
            FileActionPromptKind::Rename => {
                let from = prompt
                    .target_path
                    .clone()
                    .ok_or_else(|| "select an item first".to_string())?;
                let to = join_sifs_child(&self.path, &prompt.input)?;
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
        self.tasks.insert(
            0,
            FilesTask {
                id: request_id,
                title,
                detail,
                state: FilesTaskState::Running,
            },
        );
        self.trim_tasks();

        if let Some(tx) = self.worker_tx.clone() {
            if tx
                .send(FilesWorkerCommand::Operation {
                    request_id,
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
            .browser
            .as_ref()
            .ok_or_else(|| "SIFS browser is not open".to_string())
            .and_then(|browser| execute_files_operation(browser, operation, &self.path));
        self.apply_operation_result(request_id, result);
    }

    fn cancel_task(&mut self, task_id: u64) {
        if let Some(task) = self
            .tasks
            .iter_mut()
            .find(|task| task.id == task_id && task.state == FilesTaskState::Running)
        {
            task.state = FilesTaskState::CancelRequested;
            task.detail = "cancel requested; waiting for DFS call to return".to_string();
        }
    }

    fn finish_task(&mut self, task_id: u64, state: FilesTaskState, detail: String) {
        if let Some(task) = self.tasks.iter_mut().find(|task| task.id == task_id) {
            task.state = state;
            task.detail = detail;
        }
        self.trim_tasks();
    }

    fn trim_tasks(&mut self) {
        const MAX_TASKS: usize = 6;
        if self.tasks.len() > MAX_TASKS {
            self.tasks.truncate(MAX_TASKS);
        }
    }

    fn apply_operation_result(
        &mut self,
        request_id: u64,
        result: Result<FilesOperationOutcome, String>,
    ) -> bool {
        let cancel_requested = self
            .tasks
            .iter()
            .find(|task| task.id == request_id)
            .is_some_and(|task| task.state == FilesTaskState::CancelRequested);
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
            Err(error) => {
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
        path: String,
        result: Result<FilesListing, String>,
    },
    Details {
        request_id: u64,
        path: String,
        result: Result<SifsObjectDetails, String>,
    },
    Operation {
        request_id: u64,
        result: Result<FilesOperationOutcome, String>,
    },
}

enum FilesWorkerCommand {
    List {
        request_id: u64,
        path: String,
    },
    Details {
        request_id: u64,
        path: String,
    },
    Operation {
        request_id: u64,
        operation: FilesOperation,
    },
}

fn files_worker_loop(
    data_dir: Arc<SinomoDataDir>,
    daemon_address: Option<String>,
    app_runtime: tokio::runtime::Handle,
    command_rx: Receiver<FilesWorkerCommand>,
    result_tx: Sender<FilesWorkerResult>,
    waker: sui::UiHandle,
) {
    let mut browser: Option<SifsBrowser> = None;
    let mut open_error: Option<String> = daemon_address
        .is_none()
        .then(|| "SIFS browsing requires a daemon connection".to_string());
    while let Ok(command) = command_rx.recv() {
        match command {
            FilesWorkerCommand::List { request_id, path } => {
                let requested_path = normalize_path(&path);
                let (path, result) = if let Some(error) = &open_error {
                    (requested_path, Err(error.clone()))
                } else {
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
                    path,
                    result,
                });
            }
            FilesWorkerCommand::Details { request_id, path } => {
                let requested_path = normalize_path(&path);
                let (path, result) = if let Some(error) = &open_error {
                    (requested_path, Err(error.clone()))
                } else {
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
                            let result = browser.stat(&path);
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
                    path,
                    result,
                });
            }
            FilesWorkerCommand::Operation {
                request_id,
                operation,
            } => {
                let current_path = normalize_path(&operation.path)
                    .rsplit_once('/')
                    .map(|(parent, _)| {
                        if parent.is_empty() {
                            "/".to_string()
                        } else {
                            parent.to_string()
                        }
                    })
                    .unwrap_or_else(|| "/".to_string());
                let result = if let Some(error) = &open_error {
                    Err(error.clone())
                } else {
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
                            execute_files_operation(browser, operation, &current_path)
                        }
                        (None, Some(error)) => Err(error.clone()),
                        (None, None) => Err("SIFS browser did not start".to_string()),
                    }
                };
                let _ = result_tx.send(FilesWorkerResult::Operation { request_id, result });
            }
        }
        waker.wake();
    }
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

fn execute_files_operation(
    browser: &SifsBrowser,
    operation: FilesOperation,
    current_path: &str,
) -> Result<FilesOperationOutcome, String> {
    match operation.kind {
        FilesOperationKind::CreateFile => {
            browser.create_file(&operation.path)?;
            Ok(FilesOperationOutcome {
                refresh_path: current_path.to_string(),
                select_path: Some(operation.path.clone()),
                message: format!("created {}", operation.path),
            })
        }
        FilesOperationKind::CreateFolder => {
            browser.create_dir(&operation.path)?;
            Ok(FilesOperationOutcome {
                refresh_path: current_path.to_string(),
                select_path: Some(operation.path.clone()),
                message: format!("created {}", operation.path),
            })
        }
        FilesOperationKind::Rename => {
            let target = operation
                .target
                .clone()
                .ok_or_else(|| "rename target is missing".to_string())?;
            browser.rename(&operation.path, &target)?;
            Ok(FilesOperationOutcome {
                refresh_path: current_path.to_string(),
                select_path: Some(target.clone()),
                message: format!("renamed {} to {target}", operation.path),
            })
        }
        FilesOperationKind::Delete => {
            browser.delete(&operation.path, operation.directory)?;
            Ok(FilesOperationOutcome {
                refresh_path: current_path.to_string(),
                select_path: None,
                message: format!("deleted {}", operation.path),
            })
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
        "Metadata pending"
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

fn sifs_replication_summary(details: Option<&SifsObjectDetails>, clustered: bool) -> String {
    let Some(details) = details else {
        return if clustered {
            "waiting for SIFS metadata".to_string()
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
    Inspect,
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

    fn hit_at(&self, bounds: Rect, position: Point) -> Option<SifsEntryRowHit> {
        if !bounds.contains(position) {
            return None;
        }
        for action in [
            SifsEntryRowHit::Inspect,
            SifsEntryRowHit::Rename,
            SifsEntryRowHit::Delete,
        ] {
            if self.action_rect(bounds, action).contains(position) {
                return Some(action);
            }
        }
        Some(SifsEntryRowHit::Row)
    }

    fn action_widget_id(parent: sui::WidgetId, action: SifsEntryRowHit) -> sui::WidgetId {
        let salt = match action {
            SifsEntryRowHit::Inspect => 8_317,
            SifsEntryRowHit::Rename => 8_351,
            SifsEntryRowHit::Delete => 8_389,
            SifsEntryRowHit::Row => 8_401,
        };
        sui::WidgetId::new(parent.get().wrapping_mul(41).wrapping_add(salt))
    }
}

fn row_action_slot(action: SifsEntryRowHit) -> Option<usize> {
    match action {
        SifsEntryRowHit::Inspect => Some(0),
        SifsEntryRowHit::Rename => Some(1),
        SifsEntryRowHit::Delete => Some(2),
        SifsEntryRowHit::Row => None,
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
                        SifsEntryRowHit::Inspect => {
                            self.shared.files_mut().select_entry(self.index);
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
        let icon_color = if active { pal.accent } else { pal.text_faint };
        let label_color = if active { pal.text } else { pal.text_muted };
        let title_style = pal.text_style(&theme, 12.5, FontWeight::MEDIUM, label_color);
        let meta_style = pal.text_style(&theme, 10.5, FontWeight::NORMAL, pal.text_faint);

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

        let show_actions = self.hovered || active || self.pressed.is_some();
        for (action, glyph, color) in [
            (
                SifsEntryRowHit::Inspect,
                IconGlyph::Search,
                if active { pal.accent } else { pal.text_muted },
            ),
            (
                SifsEntryRowHit::Rename,
                IconGlyph::MoreHorizontal,
                pal.text_muted,
            ),
            (SifsEntryRowHit::Delete, IconGlyph::Trash, pal.warn),
        ] {
            let rect = self.action_rect(bounds, action);
            if show_actions {
                let fill = if self.pressed == Some(action) {
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

        let actions_w = ROW_ACTION_SIDE * 3.0 + ROW_ACTION_GAP * 2.0 + 8.0;
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
        ctx.push_clip_rect(title_rect);
        widgets::draw_text_in(ctx, title_rect, &self.entry.name, title_style);
        ctx.pop_clip();
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let row_id = ctx.widget_id();
        let mut node = SemanticsNode::new(row_id, SemanticsRole::ListItem, ctx.bounds());
        node.name = Some(self.entry.name.clone());
        node.description = Some(format!(
            "{}, SIFS path {}",
            entry_kind_label(&self.entry),
            self.entry.path
        ));
        node.state.focused = ctx.is_focused();
        node.state.hovered = self.hovered;
        node.state.selected = self.active();
        node.actions = vec![SemanticsAction::Focus, SemanticsAction::Activate];
        ctx.push(node);

        for (action, name, description) in [
            (
                SifsEntryRowHit::Inspect,
                format!("Inspect {}", self.entry.name),
                "Show SIFS placement and metadata".to_string(),
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
            node.state.selected = self.active();
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

/// Build the header: an up `IconButton` + a `Breadcrumb` over the current path segments. Each
/// segment activates a navigate-to-that-segment; the up button goes to the parent.
fn header(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let crumbs = shared.files().crumbs();
    let current = crumbs.len().saturating_sub(1);
    let targets: Vec<String> = crumbs.iter().map(|(_, target)| target.clone()).collect();

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
        Breadcrumb::new("SIFS path")
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

    let refresh = {
        let s = shared.clone();
        IconButton::new(IconGlyph::Redo, "Refresh SIFS")
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

    let new_file = {
        let s = shared.clone();
        sui::Button::new("New file")
            .icon(IconGlyph::File)
            .min_width(96.0)
            .theme_when(clone_reader(reader))
            .enabled_when({
                let s = s.clone();
                move || !s.files().loading
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
                move || !s.files().loading
            })
            .on_press_with_ctx(move |ctx| {
                s.files_mut()
                    .open_action_prompt(FileActionPromptKind::NewFolder);
                request_files_overlay_rebuild(ctx);
            })
    };

    let rename = {
        let s = shared.clone();
        sui::Button::new("Rename")
            .icon(IconGlyph::MoreHorizontal)
            .min_width(88.0)
            .theme_when(secondary_button_theme(reader))
            .enabled_when({
                let s = s.clone();
                move || !s.files().loading && s.files().selected_entry().is_some()
            })
            .on_press_with_ctx(move |ctx| {
                s.files_mut()
                    .open_action_prompt(FileActionPromptKind::Rename);
                request_files_overlay_rebuild(ctx);
            })
    };

    let delete = {
        let s = shared.clone();
        sui::Button::new("Delete")
            .icon(IconGlyph::Trash)
            .min_width(82.0)
            .theme_when(secondary_button_theme(reader))
            .enabled_when({
                let s = s.clone();
                move || !s.files().loading && s.files().selected_entry().is_some()
            })
            .on_press_with_ctx(move |ctx| {
                s.files_mut()
                    .open_action_prompt(FileActionPromptKind::Delete);
                request_files_overlay_rebuild(ctx);
            })
    };

    Stack::horizontal()
        .spacing(8.0)
        .alignment(Alignment::Center)
        .with_child(up)
        .with_child(breadcrumb)
        .with_child(refresh)
        .with_child(new_file)
        .with_child(new_folder)
        .with_child(rename)
        .with_child(delete)
}

// ---------------------------------------------------------------------------
// File listing — rebuilt on navigation by `RebuildOnChange`.
// ---------------------------------------------------------------------------

/// Build the file-list body for the *current* listing: an error/empty placeholder, or a `ListView`
/// of the entries wrapped in a `ScrollView`. Directory rows navigate; the inline inspect action
/// selects either files or directories for the SIFS inspector without changing the path.
fn listing(shared: &Shared, reader: &ThemeReader, scroll_state: &ScrollState) -> WidgetPod {
    let (loading, error, is_empty, entries, has_tasks): (
        bool,
        Option<String>,
        bool,
        Vec<SifsEntry>,
        bool,
    ) = {
        let files = shared.files();
        (
            files.loading,
            files.error.clone(),
            files.entries.is_empty(),
            files.entries.clone(),
            !files.tasks.is_empty(),
        )
    };

    if loading {
        return WidgetPod::new(
            centered_note(
                shared,
                "▤",
                "Loading SIFS",
                "Opening the distributed filesystem...",
            )
            .glyph(IconGlyph::Storage),
        );
    }
    if let Some(error) = error {
        let retry = {
            let s = shared.clone();
            sui::Button::new("Retry SIFS")
                .icon(IconGlyph::Redo)
                .min_width(112.0)
                .theme_when(clone_reader(reader))
                .on_press(move || {
                    let waker = { s.borrow().waker.clone() };
                    s.files_mut().retry(waker);
                })
        };
        let settings = {
            let s = shared.clone();
            sui::Button::new("Connection settings")
                .icon(IconGlyph::Alert)
                .min_width(160.0)
                .theme_when(secondary_button_theme(reader))
                .on_press(move || open_settings_section(&s, SettingsSection::Connection))
        };
        let actions = Stack::horizontal()
            .spacing(8.0)
            .alignment(Alignment::Center)
            .with_child(retry)
            .with_child(settings);
        return WidgetPod::new(
            centered_note(
                shared,
                "▤",
                "SIFS unavailable",
                sifs_unavailable_summary(&error),
            )
            .glyph(IconGlyph::Storage)
            .action(actions),
        );
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
        return WidgetPod::new(
            centered_note(shared, "▤", "Empty directory", "Nothing here yet.")
                .glyph(IconGlyph::Folder)
                .action(actions),
        );
    }

    // Build the row items: icon + name on the left, compact metadata and inspect action on the
    // right. The child row handles pointer hit-testing while `ListView` keeps selection and scroll.
    let items: Vec<ListItem> = entries
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, entry)| {
            ListItem::new(entry.name.clone()).with_child(SifsEntryRow::new(
                shared.clone(),
                index,
                entry,
            ))
        })
        .collect();

    let list = {
        let s = shared.clone();
        ListView::new("sifs-listing")
            .items(items)
            .row_height(ROW_H)
            .theme_when(clone_reader(reader))
            // Reflect the current file selection (the highlight is reactive — selecting a file does
            // NOT rebuild the list, only repaints + updates the inspector).
            .selected_when({
                let s = s.clone();
                move || s.files().selected
            })
            .on_change(move |index, _label| {
                // Fallback for keyboard-driven ListView activation when no row child receives focus:
                // directory -> navigate, file -> select. Row children handle pointer activation.
                s.files_mut().activate_row(index);
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
    let scroll = ScrollView::vertical(inset).state(scroll_state.clone());
    if has_tasks {
        WidgetPod::new(
            widgets::Dock::new(scroll).top(LIST_TASK_STRIP_H, FilesTaskStrip::new(shared.clone())),
        )
    } else {
        WidgetPod::new(scroll)
    }
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
    if files.error.is_some() {
        "SIFS unavailable"
    } else if files.loading {
        "Loading SIFS"
    } else if files.clustered {
        "SIFS online"
    } else {
        "Local SIFS"
    }
}

fn files_entry_count_status(files: &FilesState) -> String {
    if files.loading {
        "loading directory".to_string()
    } else if files.error.is_some() {
        "listing unavailable".to_string()
    } else {
        plural(files.entries.len(), "entry")
    }
}

fn files_selection_status(files: &FilesState) -> String {
    files
        .selected_entry()
        .map(|entry| format!("selected {}", entry.path))
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
    } else {
        "idle".to_string()
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

fn plural(count: usize, singular: &str) -> String {
    if count == 1 {
        format!("1 {singular}")
    } else {
        format!("{count} {singular}s")
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
        let selected = files
            .selected_entry()
            .map(|entry| middle_short(&entry.name, 34))
            .unwrap_or_else(|| "no selection".to_string());
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

struct FilesTaskStrip {
    shared: Shared,
    hovered_cancel: Option<u64>,
}

impl FilesTaskStrip {
    fn new(shared: Shared) -> Self {
        Self {
            shared,
            hovered_cancel: None,
        }
    }

    fn cancel_rect(bounds: Rect, row_index: usize) -> Rect {
        Rect::new(
            bounds.max_x() - CONTENT_PAD - INSPECTOR_TASK_CANCEL_W,
            bounds.y() + 34.0 + row_index as f32 * 24.0,
            INSPECTOR_TASK_CANCEL_W,
            22.0,
        )
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
}

impl Widget for FilesTaskStrip {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        match event {
            Event::Pointer(pointer) if pointer.kind == PointerEventKind::Move => {
                let hovered = self.task_at_cancel(ctx.bounds(), pointer.position);
                if self.hovered_cancel != hovered {
                    self.hovered_cancel = hovered;
                    ctx.request_paint();
                    ctx.request_semantics();
                }
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Up
                    && (pointer.button == Some(PointerButton::Primary)
                        || pointer.button.is_none()) =>
            {
                if let Some(task_id) = self.task_at_cancel(ctx.bounds(), pointer.position) {
                    self.shared.files_mut().cancel_task(task_id);
                    self.hovered_cancel = None;
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
                if self.hovered_cancel.take().is_some() {
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

        let title_style = pal.text_style(&theme, 11.5, FontWeight::MEDIUM, pal.text);
        let detail_style = pal.text_style(&theme, 10.5, FontWeight::NORMAL, pal.text_faint);
        for (index, task) in files.tasks.iter().take(2).enumerate() {
            let row_y = bounds.y() + 32.0 + index as f32 * 24.0;
            let cancel = Self::cancel_rect(bounds, index);
            let text_right = if task.state.can_cancel() {
                cancel.x() - 8.0
            } else {
                bounds.max_x() - CONTENT_PAD
            };
            let text_w = (text_right - bounds.x() - CONTENT_PAD).max(0.0);
            let tone = match task.state {
                FilesTaskState::Running | FilesTaskState::CancelRequested => pal.accent,
                FilesTaskState::Completed => pal.good,
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
                &middle_short(&task.detail, 74),
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
                let fraction = task_progress_fraction(task.state);
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
            }
        }
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let files = self.shared.files();
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

fn task_progress_fraction(state: FilesTaskState) -> f32 {
    match state {
        FilesTaskState::Running => 0.45,
        FilesTaskState::CancelRequested => 0.75,
        FilesTaskState::Completed | FilesTaskState::Failed => 1.0,
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
        if files.loading {
            return format!("loading|{}|{}", files.path, files.inspector_details_open);
        }
        if let Some(error) = &files.error {
            return format!(
                "error|{}|{}|{}",
                files.path, error, files.inspector_details_open
            );
        }
        match files.selected_entry() {
            Some(entry) => format!(
                "{}|{}|{}|{}|{}|{}|{}|{}",
                entry.path,
                entry.is_dir,
                entry.is_link,
                files.clustered,
                files.details_loading,
                files.details_error.as_deref().unwrap_or(""),
                files.inspector_details_open,
                files
                    .selected_details
                    .as_ref()
                    .map(|details| details.version.as_str())
                    .unwrap_or("")
            ),
            None => format!("unselected|{}|{}", files.path, files.entries.len()),
        }
    }
}

const INSPECTOR_PAD: f32 = CONTENT_PAD;
const INSPECTOR_STATE_PANEL_Y: f32 = INSPECTOR_PAD + 34.0;
const INSPECTOR_DETAIL_BUTTON_W: f32 = 104.0;
const INSPECTOR_DETAIL_BUTTON_H: f32 = 24.0;
const INSPECTOR_OPEN_BUTTON_W: f32 = 128.0;
const INSPECTOR_OPEN_BUTTON_H: f32 = 28.0;
const INSPECTOR_TASK_PANEL_H: f32 = 128.0;
const INSPECTOR_TASK_ROW_H: f32 = 28.0;
const INSPECTOR_TASK_CANCEL_W: f32 = 58.0;

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
            "Next",
            "Retry SIFS, then open connection settings".to_string(),
        ),
    ];
    if details_open {
        rows.push(("Reason", error.to_string()));
        rows.push(("Authority", "SIFS through daemon".to_string()));
    }
    rows
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

fn inspector_open_folder_button_rect(bounds: Rect) -> Rect {
    let content_x = bounds.x() + INSPECTOR_PAD;
    let content_w = (bounds.width() - INSPECTOR_PAD * 2.0).max(0.0);
    let button_w = INSPECTOR_OPEN_BUTTON_W.min(content_w);
    Rect::new(
        content_x,
        bounds.y() + INSPECTOR_PAD + 4.0 + 30.0 + 26.0 + 30.0,
        button_w,
        INSPECTOR_OPEN_BUTTON_H,
    )
}

fn inspector_open_folder_button_widget_id(parent: sui::WidgetId) -> sui::WidgetId {
    sui::WidgetId::new(parent.get().wrapping_mul(43).wrapping_add(7_497))
}

fn inspector_task_panel_rect(bounds: Rect) -> Rect {
    let content_x = bounds.x() + INSPECTOR_PAD;
    let content_w = (bounds.width() - INSPECTOR_PAD * 2.0).max(0.0);
    Rect::new(
        content_x,
        bounds.max_y() - INSPECTOR_PAD - INSPECTOR_TASK_PANEL_H,
        content_w,
        INSPECTOR_TASK_PANEL_H,
    )
}

fn inspector_task_cancel_rect(bounds: Rect, row_index: usize) -> Rect {
    let panel = inspector_task_panel_rect(bounds);
    Rect::new(
        panel.max_x() - INSPECTOR_TASK_CANCEL_W - 10.0,
        panel.y() + 34.0 + row_index as f32 * INSPECTOR_TASK_ROW_H + 3.0,
        INSPECTOR_TASK_CANCEL_W,
        22.0,
    )
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

            let selected_dir_path = {
                let files = self.shared.files();
                files
                    .selected_entry()
                    .filter(|entry| entry.is_dir)
                    .map(|entry| entry.path.clone())
            };
            if let Some(path) = selected_dir_path
                && inspector_open_folder_button_rect(ctx.bounds()).contains(pointer.position)
            {
                self.shared.files_mut().navigate(path);
                ctx.request_measure();
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

    fn measure(&mut self, _ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        constraints.max
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
        if !files.tasks.is_empty() {
            self.task_panel(ctx, &theme, &pal, bounds, &files.tasks);
        }

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
            let status = if files.entries.is_empty() {
                "No entries in this directory"
            } else {
                "Select an object to inspect placement"
            };
            self.state_panel(
                ctx,
                &theme,
                &pal,
                x,
                y,
                w,
                "SIFS browser",
                &[
                    ("Path", files.path.clone()),
                    ("Entries", files.entries.len().to_string()),
                    ("Status", status.to_string()),
                ],
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
        let display_name = middle_short(&entry.name, 36);
        widgets::draw_text_in(
            ctx,
            Rect::new(x, y, (w - title_right_gap).max(0.0), 24.0),
            &display_name,
            name_style,
        );
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
        widgets::draw_text_in(
            ctx,
            Rect::new(x, y, w, 20.0),
            &format!("{kind} · {}", details_size(entry, details)),
            kind_style,
        );
        y += 34.0;

        if entry.is_dir {
            self.open_folder_button(ctx, &theme, &pal, inspector_open_folder_button_rect(bounds));
            y += INSPECTOR_OPEN_BUTTON_H + 12.0;
        }

        y = self.info_row(ctx, &theme, &pal, x, y, w, "Path", &entry.path);

        // Placement section.
        y += 10.0;
        let sect = pal.text_style(&theme, 11.0, FontWeight::SEMIBOLD, pal.text_faint);
        widgets::draw_text_in(ctx, Rect::new(x, y, w, 16.0), "PLACEMENT", sect);
        y += 24.0;

        // node badge: a real Lucide HardDrive (Storage) icon + the node label, in a rounded pill.
        let node_label = if files.node_id.is_empty() {
            "local".to_string()
        } else {
            short(&files.node_id, 18)
        };
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
            .unwrap_or(if files.clustered {
                "cluster policy"
            } else {
                "local-owned"
            });
        y = self.info_row(ctx, &theme, &pal, x, y + 10.0, w, "Policy", policy);
        let replication = sifs_replication_summary(details, files.clustered);
        y = self.info_row(
            ctx,
            &theme,
            &pal,
            x,
            y + 4.0,
            w,
            "Replication",
            &replication,
        );
        let locations = details
            .map(details_locations)
            .unwrap_or_else(|| node_label.clone());
        y = self.info_row(ctx, &theme, &pal, x, y + 4.0, w, "Locations", &locations);

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
            }

            if let Some(details) = details {
                y = self.info_row(
                    ctx,
                    &theme,
                    &pal,
                    x,
                    y,
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
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let (selected_directory, details_open, tasks) = {
            let files = self.shared.files();
            (
                files.selected_entry().filter(|entry| entry.is_dir).cloned(),
                (files.loading || files.error.is_some() || files.selected_entry().is_some())
                    .then_some(files.inspector_details_open),
                files.tasks.clone(),
            )
        };

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

        if let Some(entry) = selected_directory {
            let mut node = SemanticsNode::new(
                inspector_open_folder_button_widget_id(ctx.widget_id()),
                SemanticsRole::Button,
                inspector_open_folder_button_rect(ctx.bounds()),
            );
            node.parent = Some(ctx.widget_id());
            node.name = Some("Open folder".to_string());
            node.description = Some(format!("Open {}", entry.path));
            node.actions = vec![SemanticsAction::Activate];
            ctx.push(node);
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
        node.name = Some(if details_open {
            "Hide details".to_string()
        } else {
            "Show details".to_string()
        });
        node.description = Some("File storage diagnostics".to_string());
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
            if details_open {
                "Hide details"
            } else {
                "Show details"
            },
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

    fn open_folder_button(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        rect: Rect,
    ) {
        widgets::round_panel(ctx, rect, pal.accent, pal.accent, 7.0);
        let icon = Rect::new(rect.x() + 10.0, rect.y() + 7.0, 14.0, 14.0);
        draw_glyph(ctx, IconGlyph::Folder, icon, pal.on_accent);
        widgets::draw_text_in(
            ctx,
            Rect::new(rect.x() + 30.0, rect.y() + 5.0, rect.width() - 38.0, 18.0),
            "Open folder",
            pal.text_style(theme, 11.5, FontWeight::MEDIUM, pal.on_accent),
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
        widgets::draw_text_in(
            ctx,
            Rect::new(
                panel.x() + 12.0,
                panel.y() + 10.0,
                panel.width() - 24.0,
                18.0,
            ),
            "TASKS",
            pal.text_style(theme, 11.0, FontWeight::SEMIBOLD, pal.text_faint),
        );

        let title_style = pal.text_style(theme, 11.5, FontWeight::MEDIUM, pal.text);
        let detail_style = pal.text_style(theme, 10.5, FontWeight::NORMAL, pal.text_faint);
        for (index, task) in tasks.iter().take(3).enumerate() {
            let y = panel.y() + 34.0 + index as f32 * INSPECTOR_TASK_ROW_H;
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
                &middle_short(&task.detail, 42),
                detail_style.clone(),
            );
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
}

/// The inspector wrapped in a `PanelSection`-less host so it can be a `SizedBox` child. We keep the
/// inspector as a single painter (above), so this is just the bare widget. A thin wrapper exists so
/// the composed pod reads cleanly.
fn inspector(shared: &Shared) -> impl Widget + use<> {
    InspectorView::new(shared.clone())
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

/// The header bar wrapped so it rebuilds (breadcrumb segments) whenever the path changes. A
/// fixed-height titlebar surface (up button + breadcrumb) over a hairline separator.
fn header_bar(shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
    let s = shared.clone();
    let r = Rc::clone(reader);
    let rebuilt = RebuildOnChange::new(
        {
            let s = s.clone();
            move || s.files().path.clone()
        },
        move |_path| WidgetPod::new(header(&s, &r)),
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
fn wide_pod(shared: &Shared, reader: &ThemeReader, scroll_state: &ScrollState) -> WidgetPod {
    let body = Split::new(
        list_column(shared, reader, scroll_state),
        inspector(shared),
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
            prompt.input,
            prompt.target_path.as_deref().unwrap_or(""),
            prompt.error.as_deref().unwrap_or("")
        ),
        None => "closed".to_string(),
    }
}

fn files_action_dialog(shared: &Shared, reader: &ThemeReader) -> Option<WidgetPod> {
    let prompt = shared.files().action_prompt.clone()?;
    let title = prompt.kind.title();
    let description = match prompt.kind {
        FileActionPromptKind::NewFile => format!("Create a file in {}", shared.files().path),
        FileActionPromptKind::NewFolder => format!("Create a folder in {}", shared.files().path),
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
    };
    let input_label = if prompt.kind == FileActionPromptKind::Delete {
        "Type DELETE"
    } else {
        "Name"
    };
    let input = {
        let s = shared.clone();
        sui::TextInput::new(format!("files-action-input-{title}"))
            .value(prompt.input.clone())
            .placeholder(input_label)
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
        // One shared scroll state so the list position is consistent across the wide/narrow swap.
        let scroll_state = ScrollState::new();
        let wide = wide_pod(&shared, &theme_reader, &scroll_state);
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
            "error|/|device authorization was denied|false"
        );
        shared.files_mut().inspector_details_open = true;
        assert_eq!(
            inspector.summary(),
            "error|/|device authorization was denied|true"
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
        assert_eq!(inspector.summary(), "loading|/services|false");

        {
            let mut files = shared.files_mut();
            files.inspector_details_open = true;
        }
        assert_eq!(inspector.summary(), "loading|/services|true");

        {
            let mut files = shared.files_mut();
            files.loading = false;
            files.entries.clear();
        }
        assert_eq!(inspector.summary(), "unselected|/services|0");
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
            "/artifact.txt|false|false|true|false|device authorization was denied|false|"
        ));

        shared.files_mut().inspector_details_open = true;
        assert!(inspector.summary().contains(
            "/artifact.txt|false|false|true|false|device authorization was denied|true|"
        ));
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
            "Metadata pending"
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
            "waiting for SIFS metadata"
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
            title: "Delete".to_string(),
            detail: "/cluster-tests/artifact.txt".to_string(),
            state: FilesTaskState::Running,
        }];

        assert_eq!(
            files_status_summary(&state),
            "Local SIFS | Path /cluster-tests | 1 entry | selected /cluster-tests/artifact.txt | 1 task running"
        );
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
            path: "/services/artifact.txt".to_string(),
            result: Ok(SifsObjectDetails {
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
            }),
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
            FilesWorkerCommand::List { request_id, path } => {
                assert_eq!(request_id, 0);
                assert_eq!(path, "/services");
            }
            FilesWorkerCommand::Details { .. } => panic!("expected list command"),
            FilesWorkerCommand::Operation { .. } => panic!("expected list command"),
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
            parent_path("/services/sinomo"),
            Some("/services".to_string())
        );
        assert_eq!(parent_path("/"), None);
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
                .with_name("Refresh SIFS")
                .count()?,
            1
        );

        Ok(())
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
    fn row_action_buttons_open_rename_and_delete_prompts() -> sui::Result<()> {
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
                sui::WindowBuilder::new()
                    .title("Files row actions")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

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
                title: "Delete".to_string(),
                detail: "/artifact.txt".to_string(),
                state: FilesTaskState::Running,
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
}
