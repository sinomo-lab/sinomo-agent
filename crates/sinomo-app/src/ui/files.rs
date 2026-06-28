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
    Alignment, ArrangeCtx, Breadcrumb, BreadcrumbItem, Constraints, DefaultTheme, Event, EventCtx,
    FontWeight, IconButton, IconGlyph, KeyState, ListItem, ListView, MeasureCtx, PaintCtx, Point,
    PointerButton, PointerEventKind, Rect, ScrollState, ScrollView, SemanticsAction, SemanticsCtx,
    SemanticsNode, SemanticsRole, Separator, Size, SizedBox, Stack, Widget, WidgetPod,
    WidgetPodMutVisitor, WidgetPodVisitor, WindowEvent, draw_glyph,
};

use sinomo_cli::app_api::{SifsBrowser, SifsEntry};
use sinomo_runtime::local::SinomoDataDir;

use crate::backend::Backend;
use crate::state::{Shared, Tab};
use crate::theme::Palette;
use crate::ui::settings::SettingsSection;
use crate::ui::widgets;

/// Below this content width the tab collapses to a list-only column (the inspector is hidden).
const WIDE_THRESHOLD: f32 = 640.0;
const INSPECTOR_W: f32 = 340.0;
const ROW_H: f32 = 34.0;
const CONTENT_PAD: f32 = 16.0;
const HEADER_H: f32 = 46.0;
const LOAD_TIMEOUT: Duration = Duration::from_secs(20);

/// A shared closure that resolves the current widget theme — threaded into every control so the
/// Appearance toggle restyles the whole tab live, with no rebuild. (Same pattern as `settings.rs`.)
type ThemeReader = Rc<dyn Fn() -> DefaultTheme>;

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
    pub scroll: f32,
    pub inspector_details_open: bool,
    worker_tx: Option<Sender<FilesWorkerCommand>>,
    worker_rx: Option<Receiver<FilesWorkerResult>>,
    worker_result_tx: Option<Sender<FilesWorkerResult>>,
    worker_waker: Option<sui::UiHandle>,
    next_request_id: u64,
    pending_request_id: Option<u64>,
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
            scroll: 0.0,
            inspector_details_open: false,
            worker_tx: None,
            worker_rx: None,
            worker_result_tx: None,
            worker_waker: None,
            next_request_id: 0,
            pending_request_id: None,
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
        self.scroll = 0.0;
        self.loading = false;
        let path = self.path.clone();
        let listing = self.browser.as_ref().map(|b| b.list_shallow(&path));
        match listing {
            Some(Ok(entries)) => {
                self.entries = entries;
                self.error = None;
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
        self.scroll = 0.0;
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
                    let _ = result_tx.send(FilesWorkerResult {
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
        self.node_id.clear();
        self.inspector_details_open = false;
        self.loaded = false;
        self.loading = false;
        self.pending_request_id = None;
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
        if self.pending_request_id != Some(result.request_id) {
            return false;
        }
        self.pending_request_id = None;
        self.loading = false;
        self.path = result.path;
        match result.result {
            Ok(listing) => {
                self.node_id = listing.node_id;
                self.entries = listing.entries;
                self.error = None;
            }
            Err(error) => {
                self.entries.clear();
                self.error = Some(error);
            }
        }
        self.list_revision = self.list_revision.wrapping_add(1);
        true
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

    fn selected_entry(&self) -> Option<&SifsEntry> {
        self.selected.and_then(|i| self.entries.get(i))
    }

    fn select_entry(&mut self, index: usize) -> bool {
        if index >= self.entries.len() {
            return false;
        }
        let changed = self.selected != Some(index);
        self.selected = Some(index);
        changed
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
        for part in self.path.split('/').filter(|s| !s.is_empty()) {
            acc.push('/');
            acc.push_str(part);
            out.push((part.to_string(), acc.clone()));
        }
        out
    }
}

struct FilesListing {
    node_id: String,
    entries: Vec<SifsEntry>,
}

struct FilesWorkerResult {
    request_id: u64,
    path: String,
    result: Result<FilesListing, String>,
}

enum FilesWorkerCommand {
    List { request_id: u64, path: String },
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
        let FilesWorkerCommand::List { request_id, path } = command;
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
                        open_error = Some("SIFS browsing requires a daemon connection".to_string());
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
        let _ = result_tx.send(FilesWorkerResult {
            request_id,
            path,
            result,
        });
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SifsEntryRowHit {
    Row,
    Inspect,
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

    fn show_inspect_button(&self) -> bool {
        self.hovered || self.active()
    }

    fn inspect_rect(&self, bounds: Rect) -> Rect {
        let side = 24.0_f32.min(bounds.height()).max(0.0);
        Rect::new(
            bounds.max_x() - side - 2.0,
            bounds.y() + (bounds.height() - side) * 0.5,
            side,
            side,
        )
    }

    fn hit_at(&self, bounds: Rect, position: Point) -> Option<SifsEntryRowHit> {
        if !bounds.contains(position) {
            return None;
        }
        if self.inspect_rect(bounds).contains(position) {
            Some(SifsEntryRowHit::Inspect)
        } else {
            Some(SifsEntryRowHit::Row)
        }
    }

    fn inspect_widget_id(parent: sui::WidgetId) -> sui::WidgetId {
        sui::WidgetId::new(parent.get().wrapping_mul(41).wrapping_add(8_317))
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
                    && pointer.button == Some(PointerButton::Primary) =>
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
        let show_inspect = self.show_inspect_button();
        let inspect = self.inspect_rect(bounds);
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

        if show_inspect {
            let fill = if self.pressed == Some(SifsEntryRowHit::Inspect) {
                pal.selected
            } else if active {
                pal.field
            } else {
                pal.hover
            };
            widgets::fill_round_rect(ctx, inspect, fill, 6.0);
            draw_glyph(
                ctx,
                IconGlyph::Search,
                inspect.inflate(-5.0, -5.0),
                if active { pal.accent } else { pal.text_muted },
            );
        }

        let inspect_gap = if show_inspect { 30.0 } else { 0.0 };
        let meta_w = 74.0_f32.min(bounds.width() * 0.36).max(0.0);
        let meta_rect = Rect::new(
            (bounds.max_x() - inspect_gap - meta_w).max(bounds.x()),
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

        let mut inspect = SemanticsNode::new(
            Self::inspect_widget_id(row_id),
            SemanticsRole::Button,
            self.inspect_rect(ctx.bounds()),
        );
        inspect.parent = Some(row_id);
        inspect.name = Some(format!("Inspect {}", self.entry.name));
        inspect.description = Some("Show SIFS placement and metadata".to_string());
        inspect.actions = vec![SemanticsAction::Activate];
        inspect.state.hovered = self.hovered;
        inspect.state.selected = self.active();
        ctx.push(inspect);
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

    Stack::horizontal()
        .spacing(8.0)
        .alignment(Alignment::Center)
        .with_child(up)
        .with_child(breadcrumb)
        .with_child(refresh)
}

// ---------------------------------------------------------------------------
// File listing — rebuilt on navigation by `RebuildOnChange`.
// ---------------------------------------------------------------------------

/// Build the file-list body for the *current* listing: an error/empty placeholder, or a `ListView`
/// of the entries wrapped in a `ScrollView`. Directory rows navigate; the inline inspect action
/// selects either files or directories for the SIFS inspector without changing the path.
fn listing(shared: &Shared, reader: &ThemeReader, scroll_state: &ScrollState) -> WidgetPod {
    let (loading, error, is_empty, entries): (bool, Option<String>, bool, Vec<SifsEntry>) = {
        let files = shared.files();
        (
            files.loading,
            files.error.clone(),
            files.entries.is_empty(),
            files.entries.clone(),
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
        return WidgetPod::new(
            centered_note(shared, "▤", "Empty directory", "Nothing here yet.")
                .glyph(IconGlyph::Folder),
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

fn open_settings_section(shared: &Shared, section: SettingsSection) {
    let mut app = shared.borrow_mut();
    app.active_tab = Tab::Settings;
    if let Some(ready) = app.ready.as_mut() {
        ready.settings.open(section);
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
                "{}|{}|{}|{}",
                entry.path, entry.is_dir, entry.is_link, files.clustered
            ),
            None => format!("unselected|{}|{}", files.path, files.entries.len()),
        }
    }
}

const INSPECTOR_PAD: f32 = CONTENT_PAD;
const INSPECTOR_STATE_PANEL_Y: f32 = INSPECTOR_PAD + 34.0;
const INSPECTOR_DETAIL_BUTTON_W: f32 = 104.0;
const INSPECTOR_DETAIL_BUTTON_H: f32 = 24.0;

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

impl Widget for InspectorView {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        if let Event::Pointer(pointer) = event
            && pointer.kind == PointerEventKind::Up
            && pointer.button == Some(PointerButton::Primary)
        {
            let can_toggle_details = {
                let files = self.shared.files();
                files.loading || files.error.is_some()
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

        // Name + kind.
        let name_style = pal.text_style(&theme, 16.0, FontWeight::SEMIBOLD, pal.text);
        let display_name = middle_short(&entry.name, 36);
        widgets::draw_text_in(ctx, Rect::new(x, y, w, 24.0), &display_name, name_style);
        y += 26.0;
        let kind = if entry.is_dir {
            "Directory"
        } else if entry.is_link {
            "Symlink"
        } else {
            "File"
        };
        let kind_style = pal.text_style(&theme, 12.5, FontWeight::NORMAL, pal.text_muted);
        widgets::draw_text_in(
            ctx,
            Rect::new(x, y, w, 20.0),
            &format!("{kind} · {}", human_size(entry.size)),
            kind_style,
        );
        y += 34.0;

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

        let row_label = pal.text_style(&theme, 12.5, FontWeight::NORMAL, pal.text_muted);
        let row_val = pal.text_style(&theme, 12.5, FontWeight::MEDIUM, pal.text);
        widgets::draw_text_in(
            ctx,
            Rect::new(x, y, 130.0, 20.0),
            "Replication",
            row_label.clone(),
        );
        let repl = if files.clustered {
            "cluster policy"
        } else {
            "1 (single node)"
        };
        widgets::draw_text_in(
            ctx,
            Rect::new(x + 130.0, y, w - 130.0, 20.0),
            repl,
            row_val.clone(),
        );
        y += 24.0;
        widgets::draw_text_in(ctx, Rect::new(x, y, 130.0, 20.0), "Mode", row_label);
        let mode = if files.clustered {
            "replicated"
        } else {
            "local-owned"
        };
        widgets::draw_text_in(ctx, Rect::new(x + 130.0, y, w - 130.0, 20.0), mode, row_val);
        y += 34.0;

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
        let details_open = {
            let files = self.shared.files();
            if !files.loading && files.error.is_none() {
                return;
            }
            files.inspector_details_open
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
                (files.path.clone(), files.list_revision)
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
    WidgetPod::new(widgets::Dock::new(body).top(HEADER_H + 1.0, header))
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
        widgets::Dock::new(list_column(shared, reader, scroll_state)).top(HEADER_H + 1.0, header),
    )
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
    FilesView::new(shared)
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

        let changed = state.apply_worker_result(FilesWorkerResult {
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

        let changed = state.apply_worker_result(FilesWorkerResult {
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
}
