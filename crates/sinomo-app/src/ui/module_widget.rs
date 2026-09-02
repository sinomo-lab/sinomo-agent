//! App-side adapter for asynchronously rendered module widgets.
//!
//! The SUI thread never invokes extension code or waits on IPC. It paints the
//! latest validated frame while a worker owns the isolated helper process.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use sinomo_runtime::module::UiExtensionCatalogEntry;
use sinomo_runtime::module_api::{
    UiColor, UiContentGrant, UiHostRequest, UiHostResult, UiInputEvent, UiModifiers,
    UiPaintCommand, UiPathElement, UiPointerPhase, UiSemanticsAction as WireSemanticsAction,
    UiSemanticsRole as WireSemanticsRole, UiThemeMode, UiThemeSnapshot, UiViewport, UiWidgetFrame,
    UiWidgetRequest, UiWidgetRequestKind, UiWidgetResponse, UiWidgetResponseKind, decode_sobj,
    encode_sobj,
};
use sui::{
    ArrangeCtx, Color, Constraints, DefaultTheme, Event, EventCtx, EventPhase, FontFamilyStack,
    FontWeight, ImageHandle, ImageSource, KeyState, MeasureCtx, PaintCtx, PathBuilder, Point,
    PointerEventKind, Rect, RegisteredImage, ScrollDelta, SemanticsAction, SemanticsActionRequest,
    SemanticsCtx, SemanticsNode, SemanticsRole, SemanticsValue, Size, StrokeStyle, TextStyle,
    ThemeColorScheme, Transform, Widget, WidgetId,
};

const UI_EXTENSION_HOST_BINARY_ENV: &str = "SINOMO_UI_EXTENSION_HOST_BINARY";
const UI_EXTENSION_HOST_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_IPC_FRAME_BYTES: usize = 72 * 1024 * 1024;
const WORKER_QUEUE_CAPACITY: usize = 32;

#[derive(Clone)]
pub(crate) struct ModuleWidgetSession {
    tx: SyncSender<WidgetWorkerCommand>,
    state: Arc<ModuleWidgetSharedState>,
}

impl ModuleWidgetSession {
    pub(crate) fn start(
        entry: UiExtensionCatalogEntry,
        content: UiContentGrant,
        initial_theme: DefaultTheme,
        waker: Option<sui::UiHandle>,
    ) -> Result<Self, String> {
        let component_path = entry.component_path.clone().ok_or_else(|| {
            format!(
                "UI contribution `{}` has no verified local component artifact",
                entry.contribution.contribution_id
            )
        })?;
        let sinomo_runtime::module_api::UiContributionPayload::CustomWidget { widget } =
            &entry.contribution.payload
        else {
            return Err("UI contribution is not a custom widget".to_string());
        };
        let descriptor = widget.clone();
        let (tx, rx) = mpsc::sync_channel(WORKER_QUEUE_CAPACITY);
        let state = Arc::new(ModuleWidgetSharedState::new(waker));
        let worker_state = Arc::clone(&state);
        let contribution_id = entry.contribution.contribution_id.clone();
        thread::Builder::new()
            .name(format!(
                "sinomo-ui-widget-{}",
                short_worker_label(&contribution_id)
            ))
            .spawn(move || {
                run_widget_worker(
                    rx,
                    worker_state,
                    WidgetWorkerInit {
                        contribution_id,
                        component_path: PathBuf::from(component_path),
                        content_hash: descriptor.component.content_hash,
                        entrypoint: descriptor.component.entrypoint,
                        initial_viewport: UiViewport {
                            width: descriptor.sizing.preferred_width.max(1) as f32,
                            height: descriptor.sizing.preferred_height.max(1) as f32,
                            scale_factor: 1.0,
                        },
                        initial_theme: ui_theme_snapshot(initial_theme),
                        content,
                    },
                );
            })
            .map_err(|error| format!("start UI widget worker: {error}"))?;
        Ok(Self { tx, state })
    }

    #[cfg(test)]
    pub(crate) fn from_frame(frame: UiWidgetFrame) -> Self {
        frame.validate().expect("test frame is valid");
        let (tx, _rx) = mpsc::sync_channel(1);
        let state = Arc::new(ModuleWidgetSharedState::new(None));
        state.install_frame(frame);
        Self { tx, state }
    }

    pub(crate) fn frame(&self) -> Option<Arc<UiWidgetFrame>> {
        self.state
            .frame
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn error(&self) -> Option<String> {
        self.state
            .error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn revision(&self) -> u64 {
        self.state.revision.load(Ordering::Acquire)
    }

    fn render(&self, viewport: UiViewport, theme: UiThemeSnapshot) {
        self.try_send(WidgetWorkerCommand::Render { viewport, theme });
    }

    fn event(&self, event: UiInputEvent) {
        self.try_send(WidgetWorkerCommand::Event(event));
    }

    fn try_send(&self, command: WidgetWorkerCommand) {
        match self.tx.try_send(command) {
            Ok(()) | Err(TrySendError::Full(_)) => {}
            Err(TrySendError::Disconnected(_)) => {
                self.state
                    .install_error("UI extension worker stopped".to_string());
            }
        }
    }
}

struct ModuleWidgetSharedState {
    frame: Mutex<Option<Arc<UiWidgetFrame>>>,
    error: Mutex<Option<String>>,
    revision: AtomicU64,
    waker: Option<sui::UiHandle>,
}

impl ModuleWidgetSharedState {
    fn new(waker: Option<sui::UiHandle>) -> Self {
        Self {
            frame: Mutex::new(None),
            error: Mutex::new(None),
            revision: AtomicU64::new(0),
            waker,
        }
    }

    fn install_frame(&self, frame: UiWidgetFrame) {
        *self
            .frame
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::new(frame));
        *self
            .error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        self.changed();
    }

    fn install_error(&self, error: String) {
        *self
            .error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error);
        self.changed();
    }

    fn changed(&self) {
        self.revision.fetch_add(1, Ordering::AcqRel);
        if let Some(waker) = &self.waker {
            waker.wake();
        }
    }
}

struct WidgetWorkerInit {
    contribution_id: String,
    component_path: PathBuf,
    content_hash: String,
    entrypoint: String,
    initial_viewport: UiViewport,
    initial_theme: UiThemeSnapshot,
    content: UiContentGrant,
}

enum WidgetWorkerCommand {
    Render {
        viewport: UiViewport,
        theme: UiThemeSnapshot,
    },
    Event(UiInputEvent),
}

fn run_widget_worker(
    rx: mpsc::Receiver<WidgetWorkerCommand>,
    state: Arc<ModuleWidgetSharedState>,
    init: WidgetWorkerInit,
) {
    let mut process = match UiExtensionHostProcess::spawn(
        &init.component_path,
        &init.content_hash,
        &init.entrypoint,
    ) {
        Ok(process) => process,
        Err(error) => {
            state.install_error(error);
            return;
        }
    };
    let mut request_id = 0u64;
    let mut viewport = init.initial_viewport;
    let mut theme = init.initial_theme;
    let content = init.content;
    let mut next_frame_at = match dispatch_widget_message(
        &mut process,
        &state,
        &content,
        &mut request_id,
        UiWidgetRequestKind::Initialize {
            contribution_id: init.contribution_id,
            viewport,
            theme: theme.clone(),
            content: Some(content.clone()),
        },
    ) {
        Ok(deadline) => deadline,
        Err(error) => {
            state.install_error(error);
            return;
        }
    };

    loop {
        let command = match next_frame_at {
            Some(deadline) => {
                let wait_ms = deadline.saturating_sub(monotonic_millis());
                match rx.recv_timeout(Duration::from_millis(wait_ms)) {
                    Ok(command) => command,
                    Err(mpsc::RecvTimeoutError::Timeout) => WidgetWorkerCommand::Render {
                        viewport,
                        theme: theme.clone(),
                    },
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
            None => match rx.recv() {
                Ok(command) => command,
                Err(_) => break,
            },
        };
        let message = match command {
            WidgetWorkerCommand::Render {
                viewport: next_viewport,
                theme: next_theme,
            } => {
                viewport = next_viewport;
                theme = next_theme;
                UiWidgetRequestKind::Render {
                    viewport,
                    theme: theme.clone(),
                    monotonic_time_ms: monotonic_millis(),
                }
            }
            WidgetWorkerCommand::Event(event) => UiWidgetRequestKind::Event { event },
        };
        match dispatch_widget_message(&mut process, &state, &content, &mut request_id, message) {
            Ok(deadline) => next_frame_at = deadline,
            Err(error) => {
                state.install_error(error);
                return;
            }
        }
    }
    let _ = process.request(&UiWidgetRequest {
        instance_id: "widget".to_string(),
        request_id: request_id.wrapping_add(1),
        message: UiWidgetRequestKind::Close,
    });
}

fn dispatch_widget_message(
    process: &mut UiExtensionHostProcess,
    state: &ModuleWidgetSharedState,
    content: &UiContentGrant,
    request_id: &mut u64,
    message: UiWidgetRequestKind,
) -> Result<Option<u64>, String> {
    *request_id = request_id.wrapping_add(1);
    let mut response = process.request(&UiWidgetRequest {
        instance_id: "widget".to_string(),
        request_id: *request_id,
        message,
    })?;
    let mut pending_results = VecDeque::new();
    let mut next_frame_at = None;
    for _ in 0..32 {
        match response.result {
            UiWidgetResponseKind::Frame { frame } => {
                frame.validate().map_err(|error| error.to_string())?;
                for request in &frame.host_requests {
                    match request {
                        UiHostRequest::RequestFrame {
                            not_before_monotonic_ms,
                        } => {
                            let now = monotonic_millis();
                            let bounded = (*not_before_monotonic_ms)
                                .max(now.saturating_add(16))
                                .min(now.saturating_add(60_000));
                            next_frame_at = Some(
                                next_frame_at
                                    .map_or(bounded, |existing: u64| existing.min(bounded)),
                            );
                        }
                        _ => {
                            if let Some(result) = service_host_request(request, content) {
                                pending_results.push_back(result);
                            }
                        }
                    }
                }
                state.install_frame(frame);
                let Some(result) = pending_results.pop_front() else {
                    return Ok(next_frame_at);
                };
                *request_id = request_id.wrapping_add(1);
                response = process.request(&UiWidgetRequest {
                    instance_id: "widget".to_string(),
                    request_id: *request_id,
                    message: UiWidgetRequestKind::HostResult { result },
                })?;
            }
            UiWidgetResponseKind::Closed => return Ok(None),
            UiWidgetResponseKind::Error { code, detail } => {
                return Err(format!("{code}: {detail}"));
            }
        }
    }
    Err("UI component exceeded the host-request continuation limit".to_string())
}

fn service_host_request(request: &UiHostRequest, content: &UiContentGrant) -> Option<UiHostResult> {
    match request {
        UiHostRequest::ReadContent {
            request_id,
            grant_id,
            offset,
            length,
        } => {
            if grant_id != &content.grant_id {
                return Some(UiHostResult::Error {
                    request_id: *request_id,
                    code: "invalid-content-grant".to_string(),
                    detail: "content grant does not belong to this widget instance".to_string(),
                });
            }
            let start = usize::try_from(*offset).unwrap_or(usize::MAX);
            let requested = usize::try_from(*length).unwrap_or(usize::MAX);
            if start > content.initial_bytes.len() {
                return Some(UiHostResult::Error {
                    request_id: *request_id,
                    code: "content-range-unavailable".to_string(),
                    detail: "requested range is outside the bytes granted to this preview"
                        .to_string(),
                });
            }
            let end = start
                .saturating_add(requested)
                .min(content.initial_bytes.len());
            Some(UiHostResult::Content {
                request_id: *request_id,
                offset: *offset,
                bytes: content.initial_bytes[start..end].to_vec(),
                eof: end as u64 >= content.size_bytes,
            })
        }
        UiHostRequest::InvokeAction { request_id, .. } => Some(UiHostResult::Error {
            request_id: *request_id,
            code: "read-only-preview".to_string(),
            detail: "the first custom preview protocol does not enable mutation actions"
                .to_string(),
        }),
        UiHostRequest::RequestFrame { .. } => None,
    }
}

struct UiExtensionHostProcess {
    child: Arc<Mutex<Child>>,
    stdin: ChildStdin,
    responses: mpsc::Receiver<Result<Vec<u8>, String>>,
}

impl UiExtensionHostProcess {
    fn spawn(artifact: &Path, content_hash: &str, entrypoint: &str) -> Result<Self, String> {
        let binary = ui_extension_host_binary()?;
        let mut child = Command::new(&binary)
            .arg("--artifact")
            .arg(artifact)
            .arg("--content-hash")
            .arg(content_hash)
            .arg("--entrypoint")
            .arg(entrypoint)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| {
                format!(
                    "start UI extension host {} for {}: {error}",
                    binary.display(),
                    artifact.display()
                )
            })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| "UI extension host has no stdin".to_string())?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| "UI extension host has no stdout".to_string())?;
        let (response_tx, responses) = mpsc::sync_channel(4);
        thread::Builder::new()
            .name("sinomo-ui-extension-responses".to_string())
            .spawn(move || {
                let mut stdout = BufReader::new(stdout);
                loop {
                    let response = read_ipc_frame(&mut stdout);
                    let terminal = response.is_err();
                    if response_tx.send(response).is_err() || terminal {
                        break;
                    }
                }
            })
            .map_err(|error| format!("start UI extension response reader: {error}"))?;
        Ok(Self {
            child: Arc::new(Mutex::new(child)),
            stdin,
            responses,
        })
    }

    fn request(&mut self, request: &UiWidgetRequest) -> Result<UiWidgetResponse, String> {
        let bytes = encode_sobj(request).map_err(|error| error.to_string())?;
        write_ipc_frame(&mut self.stdin, &bytes)?;
        self.stdin
            .flush()
            .map_err(|error| format!("flush UI extension request: {error}"))?;
        let bytes = match self
            .responses
            .recv_timeout(UI_EXTENSION_HOST_REQUEST_TIMEOUT)
        {
            Ok(response) => response?,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let _ = self
                    .child
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .kill();
                return Err(format!(
                    "UI extension host did not respond within {} ms",
                    UI_EXTENSION_HOST_REQUEST_TIMEOUT.as_millis()
                ));
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err("UI extension host response stream stopped".to_string());
            }
        };
        let response: UiWidgetResponse = decode_sobj(&bytes).map_err(|error| error.to_string())?;
        if response.instance_id != request.instance_id || response.request_id != request.request_id
        {
            return Err("UI extension host returned a mismatched response identity".to_string());
        }
        Ok(response)
    }
}

impl Drop for UiExtensionHostProcess {
    fn drop(&mut self) {
        let mut child = self
            .child
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match child.try_wait() {
            Ok(Some(_)) => {
                let _ = child.wait();
            }
            Ok(None) | Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

fn ui_extension_host_binary() -> Result<PathBuf, String> {
    if let Some(path) = std::env::var_os(UI_EXTENSION_HOST_BINARY_ENV) {
        let path = PathBuf::from(path);
        return path
            .is_file()
            .then_some(path)
            .ok_or_else(|| format!("{UI_EXTENSION_HOST_BINARY_ENV} points to a missing helper"));
    }
    let current = std::env::current_exe()
        .map_err(|error| format!("resolve graphical app executable: {error}"))?;
    let file_name = format!("sinomo-ui-extension-host{}", std::env::consts::EXE_SUFFIX);
    let direct = current.with_file_name(&file_name);
    if direct.is_file() {
        return Ok(direct);
    }
    if current
        .parent()
        .and_then(Path::file_name)
        .is_some_and(|name| name == "deps")
        && let Some(debug_root) = current.parent().and_then(Path::parent)
    {
        let candidate = debug_root.join(&file_name);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    Err(format!(
        "the required sibling UI extension helper `{file_name}` was not found beside {}; build/package sinomo-app and sinomo-ui-extension-host together or set {UI_EXTENSION_HOST_BINARY_ENV}",
        current.display()
    ))
}

fn write_ipc_frame(writer: &mut impl Write, bytes: &[u8]) -> Result<(), String> {
    if bytes.len() > MAX_IPC_FRAME_BYTES || bytes.len() > u32::MAX as usize {
        return Err(format!(
            "UI extension request is {} bytes; limit is {MAX_IPC_FRAME_BYTES}",
            bytes.len()
        ));
    }
    writer
        .write_all(&(bytes.len() as u32).to_le_bytes())
        .and_then(|_| writer.write_all(bytes))
        .map_err(|error| format!("write UI extension request: {error}"))
}

fn read_ipc_frame(reader: &mut impl Read) -> Result<Vec<u8>, String> {
    let mut length = [0u8; 4];
    reader
        .read_exact(&mut length)
        .map_err(|error| format!("read UI extension response length: {error}"))?;
    let length = u32::from_le_bytes(length) as usize;
    if length > MAX_IPC_FRAME_BYTES {
        return Err(format!(
            "UI extension response is {length} bytes; limit is {MAX_IPC_FRAME_BYTES}"
        ));
    }
    let mut bytes = vec![0; length];
    reader
        .read_exact(&mut bytes)
        .map_err(|error| format!("read UI extension response: {error}"))?;
    Ok(bytes)
}

fn monotonic_millis() -> u64 {
    use std::sync::OnceLock;
    use std::time::Instant;
    static START: OnceLock<Instant> = OnceLock::new();
    START
        .get_or_init(Instant::now)
        .elapsed()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn short_worker_label(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '-')
        .take(32)
        .collect()
}

pub(crate) struct ModuleWidget {
    session: ModuleWidgetSession,
    sizing: sinomo_runtime::module_api::UiWidgetSizing,
    theme: std::rc::Rc<dyn Fn() -> DefaultTheme>,
    image_slots: RefCell<BTreeMap<String, (ImageHandle, u64)>>,
    last_revision: Cell<u64>,
    arranged_size: Cell<Size>,
}

impl ModuleWidget {
    pub(crate) fn new(
        session: ModuleWidgetSession,
        sizing: sinomo_runtime::module_api::UiWidgetSizing,
        theme: std::rc::Rc<dyn Fn() -> DefaultTheme>,
    ) -> Self {
        Self {
            session,
            sizing,
            theme,
            image_slots: RefCell::new(BTreeMap::new()),
            last_revision: Cell::new(0),
            arranged_size: Cell::new(Size::ZERO),
        }
    }

    fn notice_frame_change(&self, ctx: &mut EventCtx) {
        let revision = self.session.revision();
        if revision != self.last_revision.get() {
            self.last_revision.set(revision);
            ctx.request_paint();
            ctx.request_semantics();
        }
    }
}

impl Widget for ModuleWidget {
    fn measure(&mut self, _ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        constraints.clamp(Size::new(
            self.sizing.preferred_width as f32,
            self.sizing.preferred_height as f32,
        ))
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        let size = bounds.size;
        if self.arranged_size.replace(size) != size {
            self.session.render(
                UiViewport {
                    width: size.width.max(1.0),
                    height: size.height.max(1.0),
                    scale_factor: ctx.dpi().scale_factor,
                },
                ui_theme_snapshot((self.theme)()),
            );
        }
    }

    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        self.notice_frame_change(ctx);
        if ctx.phase() != EventPhase::Target {
            return;
        }
        match event {
            Event::Pointer(pointer) if ctx.bounds().contains(pointer.position) => {
                if pointer.kind == PointerEventKind::Down {
                    ctx.request_focus();
                    ctx.request_pointer_capture(pointer.pointer_id);
                } else if matches!(
                    pointer.kind,
                    PointerEventKind::Up | PointerEventKind::Cancel
                ) {
                    ctx.release_pointer_capture(pointer.pointer_id);
                }
                let local = Point::new(
                    pointer.position.x - ctx.bounds().x(),
                    pointer.position.y - ctx.bounds().y(),
                );
                let input = match pointer.kind {
                    PointerEventKind::Down => Some(UiInputEvent::Pointer {
                        phase: UiPointerPhase::Down,
                        pointer_id: pointer.pointer_id,
                        position: wire_point(local),
                        buttons: u32::from(pointer.buttons.bits()),
                    }),
                    PointerEventKind::Move | PointerEventKind::Enter => {
                        Some(UiInputEvent::Pointer {
                            phase: UiPointerPhase::Move,
                            pointer_id: pointer.pointer_id,
                            position: wire_point(local),
                            buttons: u32::from(pointer.buttons.bits()),
                        })
                    }
                    PointerEventKind::Up => Some(UiInputEvent::Pointer {
                        phase: UiPointerPhase::Up,
                        pointer_id: pointer.pointer_id,
                        position: wire_point(local),
                        buttons: u32::from(pointer.buttons.bits()),
                    }),
                    PointerEventKind::Cancel | PointerEventKind::Leave => {
                        Some(UiInputEvent::Pointer {
                            phase: UiPointerPhase::Cancel,
                            pointer_id: pointer.pointer_id,
                            position: wire_point(local),
                            buttons: u32::from(pointer.buttons.bits()),
                        })
                    }
                    PointerEventKind::Scroll => {
                        let delta = match pointer.scroll_delta {
                            Some(ScrollDelta::Lines(delta)) | Some(ScrollDelta::Pixels(delta)) => {
                                delta
                            }
                            None => pointer.delta,
                        };
                        Some(UiInputEvent::Scroll {
                            delta_x: delta.x,
                            delta_y: delta.y,
                        })
                    }
                };
                if let Some(input) = input {
                    self.session.event(input);
                    ctx.set_handled();
                }
            }
            Event::Keyboard(keyboard) => {
                self.session.event(UiInputEvent::Key {
                    key: keyboard.key.clone(),
                    pressed: keyboard.state == KeyState::Pressed,
                    repeat: keyboard.repeat,
                    modifiers: UiModifiers {
                        shift: keyboard.modifiers.shift,
                        control: keyboard.modifiers.control,
                        alt: keyboard.modifiers.alt,
                        meta: keyboard.modifiers.meta,
                    },
                });
                if keyboard.state == KeyState::Pressed
                    && let Some(text) = keyboard.text.clone()
                {
                    self.session.event(UiInputEvent::Text { text });
                }
                ctx.set_handled();
            }
            Event::Semantics(semantics) => {
                if let Some(frame) = self.session.frame()
                    && let Some(node) = frame.semantics.iter().find(|node| {
                        guest_semantic_widget_id(ctx.widget_id(), &node.id) == semantics.target
                    })
                    && let Some(action) = wire_semantics_action(&semantics.action)
                {
                    self.session.event(UiInputEvent::SemanticsAction {
                        node_id: node.id.clone(),
                        action,
                    });
                    ctx.set_handled();
                }
            }
            _ => {}
        }
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let Some(frame) = self.session.frame() else {
            let theme = (self.theme)();
            let bounds = ctx.bounds();
            ctx.fill_rect(bounds, theme.palette.field);
            let error = self.session.error();
            let message = error
                .clone()
                .unwrap_or_else(|| "Loading custom preview…".to_string());
            let mut style = theme.text_style(if error.is_some() {
                theme.palette.danger_text
            } else {
                theme.palette.text_muted
            });
            style.font_size = theme.text.sm.size;
            style.line_height = theme.text.sm.line_height;
            ctx.draw_text(
                Rect::new(
                    bounds.x() + 12.0,
                    bounds.y() + 12.0,
                    (bounds.width() - 24.0).max(0.0),
                    (bounds.height() - 24.0).max(0.0),
                ),
                message,
                style,
            );
            return;
        };
        let bounds = ctx.bounds();
        ctx.push_clip_rect(bounds);
        ctx.push_transform(Transform::translation(bounds.x(), bounds.y()));
        self.register_frame_images(ctx, frame.as_ref());
        for command in &frame.commands {
            paint_wire_command(ctx, command, &self.image_slots.borrow(), frame.generation);
        }
        ctx.pop_transform();
        ctx.pop_clip();
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let frame = self.session.frame();
        let mut root = SemanticsNode::new(ctx.widget_id(), SemanticsRole::Canvas, ctx.bounds());
        root.name = Some("Module custom content".to_string());
        root.description = self.session.error();
        root.actions.push(SemanticsAction::Focus);
        ctx.push(root);
        let Some(frame) = frame else {
            return;
        };
        for source in &frame.semantics {
            let id = guest_semantic_widget_id(ctx.widget_id(), &source.id);
            let mut node = SemanticsNode::new(
                id,
                semantics_role(source.role),
                translate_wire_rect(source.bounds, ctx.bounds().origin),
            );
            node.parent = Some(match source.parent_id.as_deref() {
                Some(parent) => guest_semantic_widget_id(ctx.widget_id(), parent),
                None => ctx.widget_id(),
            });
            node.name = source.name.clone();
            node.value = source.value.clone().map(SemanticsValue::Text);
            node.actions = source
                .actions
                .iter()
                .copied()
                .map(semantics_action)
                .collect();
            ctx.push(node);
        }
    }

    fn accepts_focus(&self) -> bool {
        true
    }

    fn focus_changed(&mut self, _ctx: &mut EventCtx, focused: bool) {
        self.session.event(UiInputEvent::Focus { focused });
    }
}

impl ModuleWidget {
    fn register_frame_images(&self, ctx: &mut PaintCtx, frame: &UiWidgetFrame) {
        let mut slots = self.image_slots.borrow_mut();
        let reset = slots.len() != frame.images.len()
            || slots.iter().any(|(id, (_, generation))| {
                *generation != frame.generation || !frame.images.iter().any(|image| image.id == *id)
            });
        if reset {
            slots.clear();
        }
        for (index, image) in frame.images.iter().enumerate() {
            let handle = slots
                .entry(image.id.clone())
                .or_insert_with(|| (ctx.widget_image_handle(index as u64), frame.generation));
            if (reset || handle.1 != frame.generation || !ctx.image_registered(handle.0))
                && let Ok(registered) =
                    RegisteredImage::from_rgba8(image.width, image.height, image.rgba.clone())
            {
                ctx.register_image(handle.0, registered);
                handle.1 = frame.generation;
            }
        }
    }
}

fn paint_wire_command(
    ctx: &mut PaintCtx,
    command: &UiPaintCommand,
    images: &BTreeMap<String, (ImageHandle, u64)>,
    generation: u64,
) {
    match command {
        UiPaintCommand::Clear { color } => {
            let bounds = ctx.bounds();
            ctx.fill_rect(
                Rect::new(0.0, 0.0, bounds.width(), bounds.height()),
                sui_color(*color),
            );
        }
        UiPaintCommand::FillRect { rect, color } => {
            ctx.fill_rect(sui_rect(*rect), sui_color(*color));
        }
        UiPaintCommand::StrokeRect { rect, color, width } => {
            ctx.stroke_rect(sui_rect(*rect), sui_color(*color), StrokeStyle::new(*width));
        }
        UiPaintCommand::FillPath { path, color } => {
            ctx.fill(sui_path(path), sui_color(*color));
        }
        UiPaintCommand::StrokePath { path, color, width } => {
            ctx.stroke(sui_path(path), sui_color(*color), StrokeStyle::new(*width));
        }
        UiPaintCommand::DrawText { rect, text, style } => {
            let mut text_style = TextStyle::new(sui_color(style.color));
            text_style.font_size = style.font_size;
            text_style.line_height = style.font_size * 1.25;
            text_style.weight = FontWeight::new(style.font_weight);
            if style.monospace {
                text_style.font_families = Some(FontFamilyStack::new(
                    "ui-monospace",
                    &["Consolas", "monospace"],
                ));
            }
            ctx.draw_text(sui_rect(*rect), text.clone(), text_style);
        }
        UiPaintCommand::DrawImage { rect, image_id } => {
            if let Some((handle, image_generation)) = images.get(image_id)
                && *image_generation == generation
                && ctx.image_registered(*handle)
            {
                ctx.draw_image_source(sui_rect(*rect), ImageSource::new(*handle));
            }
        }
        UiPaintCommand::PushClipRect { rect } => ctx.push_clip_rect(sui_rect(*rect)),
        UiPaintCommand::PopClip => ctx.pop_clip(),
        UiPaintCommand::PushTransform { transform } => {
            let [xx, yx, xy, yy, dx, dy] = transform.matrix;
            ctx.push_transform(Transform::new(xx, yx, xy, yy, dx, dy));
        }
        UiPaintCommand::PopTransform => ctx.pop_transform(),
    }
}

fn sui_path(path: &sinomo_runtime::module_api::UiPath) -> sui::Path {
    let mut builder = PathBuilder::new();
    for element in &path.elements {
        match element {
            UiPathElement::MoveTo { point } => {
                builder.move_to(sui_point(*point));
            }
            UiPathElement::LineTo { point } => {
                builder.line_to(sui_point(*point));
            }
            UiPathElement::QuadraticTo { control, point } => {
                builder.quad_to(sui_point(*control), sui_point(*point));
            }
            UiPathElement::CubicTo {
                control1,
                control2,
                point,
            } => {
                builder.cubic_to(
                    sui_point(*control1),
                    sui_point(*control2),
                    sui_point(*point),
                );
            }
            UiPathElement::Close => {
                builder.close();
            }
        }
    }
    builder.build()
}

fn ui_theme_snapshot(theme: DefaultTheme) -> UiThemeSnapshot {
    UiThemeSnapshot {
        mode: match theme.colors.scheme {
            ThemeColorScheme::Light => UiThemeMode::Light,
            ThemeColorScheme::Dark => UiThemeMode::Dark,
            ThemeColorScheme::HighContrast => UiThemeMode::HighContrast,
        },
        accent: wire_color(theme.palette.accent),
        foreground: wire_color(theme.palette.text),
        muted_foreground: wire_color(theme.palette.text_muted),
        background: wire_color(theme.colors.base_100),
        surface: wire_color(theme.palette.surface),
        danger: wire_color(theme.palette.danger),
    }
}

fn wire_color(color: Color) -> UiColor {
    let [red, green, blue, alpha] = color.clamped().to_array();
    UiColor {
        red: (red * 255.0).round() as u8,
        green: (green * 255.0).round() as u8,
        blue: (blue * 255.0).round() as u8,
        alpha: (alpha * 255.0).round() as u8,
    }
}

fn sui_color(color: UiColor) -> Color {
    Color::srgba(
        f32::from(color.red) / 255.0,
        f32::from(color.green) / 255.0,
        f32::from(color.blue) / 255.0,
        f32::from(color.alpha) / 255.0,
    )
}

fn wire_point(point: Point) -> sinomo_runtime::module_api::UiPoint {
    sinomo_runtime::module_api::UiPoint {
        x: point.x,
        y: point.y,
    }
}

fn sui_point(point: sinomo_runtime::module_api::UiPoint) -> Point {
    Point::new(point.x, point.y)
}

fn sui_rect(rect: sinomo_runtime::module_api::UiRect) -> Rect {
    Rect::new(rect.x, rect.y, rect.width, rect.height)
}

fn translate_wire_rect(rect: sinomo_runtime::module_api::UiRect, origin: Point) -> Rect {
    Rect::new(
        origin.x + rect.x,
        origin.y + rect.y,
        rect.width,
        rect.height,
    )
}

fn guest_semantic_widget_id(owner: WidgetId, guest_id: &str) -> WidgetId {
    let digest = blake3::hash(guest_id.as_bytes());
    let mut raw = [0u8; 8];
    raw.copy_from_slice(&digest.as_bytes()[..8]);
    let value = u64::from_le_bytes(raw) ^ owner.get().rotate_left(17) ^ (1u64 << 63);
    WidgetId::new(value.max(1))
}

fn semantics_role(role: WireSemanticsRole) -> SemanticsRole {
    match role {
        WireSemanticsRole::GenericContainer => SemanticsRole::GenericContainer,
        WireSemanticsRole::Canvas => SemanticsRole::Canvas,
        WireSemanticsRole::Image => SemanticsRole::Image,
        WireSemanticsRole::Text => SemanticsRole::Text,
        WireSemanticsRole::Button => SemanticsRole::Button,
        WireSemanticsRole::Link => SemanticsRole::Link,
        WireSemanticsRole::Checkbox => SemanticsRole::CheckBox,
        WireSemanticsRole::Slider => SemanticsRole::Slider,
        WireSemanticsRole::List => SemanticsRole::List,
        WireSemanticsRole::ListItem => SemanticsRole::ListItem,
    }
}

fn semantics_action(action: WireSemanticsAction) -> SemanticsAction {
    match action {
        WireSemanticsAction::Focus => SemanticsAction::Focus,
        WireSemanticsAction::Activate => SemanticsAction::Activate,
        WireSemanticsAction::Increment => SemanticsAction::Increment,
        WireSemanticsAction::Decrement => SemanticsAction::Decrement,
        WireSemanticsAction::ScrollForward => SemanticsAction::Custom("ScrollForward".into()),
        WireSemanticsAction::ScrollBackward => SemanticsAction::Custom("ScrollBackward".into()),
    }
}

fn wire_semantics_action(action: &SemanticsActionRequest) -> Option<WireSemanticsAction> {
    match action {
        SemanticsActionRequest::Focus => Some(WireSemanticsAction::Focus),
        SemanticsActionRequest::Activate => Some(WireSemanticsAction::Activate),
        SemanticsActionRequest::Increment => Some(WireSemanticsAction::Increment),
        SemanticsActionRequest::Decrement => Some(WireSemanticsAction::Decrement),
        SemanticsActionRequest::Custom { name, .. } if name == "ScrollForward" => {
            Some(WireSemanticsAction::ScrollForward)
        }
        SemanticsActionRequest::Custom { name, .. } if name == "ScrollBackward" => {
            Some(WireSemanticsAction::ScrollBackward)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinomo_runtime::module_api::{UiPaintCommand, UiRect, UiSemanticsNode, UiTextStyle};
    use sui_testing::TestApp;

    fn frame() -> UiWidgetFrame {
        UiWidgetFrame {
            generation: 7,
            commands: vec![
                UiPaintCommand::FillRect {
                    rect: UiRect {
                        x: 0.0,
                        y: 0.0,
                        width: 240.0,
                        height: 120.0,
                    },
                    color: UiColor {
                        red: 16,
                        green: 32,
                        blue: 64,
                        alpha: 255,
                    },
                },
                UiPaintCommand::DrawText {
                    rect: UiRect {
                        x: 12.0,
                        y: 12.0,
                        width: 180.0,
                        height: 28.0,
                    },
                    text: "Fixture custom preview".to_string(),
                    style: UiTextStyle {
                        color: UiColor {
                            red: 255,
                            green: 255,
                            blue: 255,
                            alpha: 255,
                        },
                        font_size: 14.0,
                        font_weight: 500,
                        monospace: false,
                    },
                },
            ],
            semantics: vec![UiSemanticsNode {
                id: "fixture.preview".to_string(),
                parent_id: None,
                role: WireSemanticsRole::Canvas,
                bounds: UiRect {
                    x: 0.0,
                    y: 0.0,
                    width: 240.0,
                    height: 120.0,
                },
                name: Some("Fixture custom preview".to_string()),
                value: None,
                actions: vec![WireSemanticsAction::Focus],
            }],
            ..UiWidgetFrame::default()
        }
    }

    #[test]
    fn cached_custom_frame_renders_and_publishes_semantics() -> sui::Result<()> {
        let session = ModuleWidgetSession::from_frame(frame());
        let widget = ModuleWidget::new(
            session,
            sinomo_runtime::module_api::UiWidgetSizing::default(),
            std::rc::Rc::new(DefaultTheme::dark),
        );
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Custom module widget")
                    .root(
                        sui::SizedBox::new()
                            .width(240.0)
                            .height(120.0)
                            .with_child(widget),
                    ),
            )
            .build()?;
        let app = TestApp::from_runtime(runtime)?;
        app.main_window()?
            .get_by_role(SemanticsRole::Canvas)
            .with_name("Fixture custom preview")
            .expect()
            .to_be_visible()?;
        Ok(())
    }
}
