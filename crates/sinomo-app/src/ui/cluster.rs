//! "Cluster" tab - network-topology graph of known cluster nodes.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::thread;

use sui::{
    Alignment, ArrangeCtx, Button, ButtonAppearance, Color, Constraints, DefaultTheme, DetailRow,
    Dock, EmptyState, Event, EventCtx, Flex, FlexItem, FontWeight, IconGlyph, Insets, KeyState,
    Label, MeasureCtx, PaintCtx, PanelSection, Path, PathBuilder, Point, PointerButton,
    PointerEventKind, RebuildOnChange, Rect, ScrollView, SemanticTone, SemanticsAction,
    SemanticsCtx, SemanticsNode, SemanticsRole, Size, StatusBadge, StrokeStyle, Surface,
    SurfaceAppearance, SurfaceBorder, SurfaceElevation, TrailingSlotRow, Widget, WidgetPod,
    WidgetPodMutVisitor, WidgetPodVisitor, WindowEvent,
};

use crate::backend::Backend;
use crate::backend::cluster::{ClusterLinkInfo, ClusterNodeInfo, ClusterSnapshot};
use crate::state::{Shared, Tab};
use crate::theme::Palette;
use crate::ui::settings::SettingsSection;
use crate::ui::widgets;

const HEADER_H: f32 = 78.0;
const PAD: f32 = 22.0;
const NODE_R: f32 = 22.0;
const LAN_R: f32 = 18.0;
const PANEL_W: f32 = 360.0;
const PANEL_PAD: f32 = 16.0;
const DETAIL_PANEL_H: f32 = 620.0;
const DETAIL_BUTTON_H: f32 = 30.0;
const LEGEND_H: f32 = 32.0;
const VERTEX_TEXT_TOP_GAP: f32 = 6.0;
const VERTEX_TEXT_LINE_GAP: f32 = 2.0;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterSelection {
    Node(String),
    LanSegment(String),
}

pub struct ClusterState {
    pub snapshot: ClusterSnapshot,
    pub selected: Option<ClusterSelection>,
    pub diagnostics_open: bool,
    pub refreshing: bool,
    refresh_rx: Option<Receiver<ClusterSnapshot>>,
}

impl ClusterState {
    pub fn new(backend: &Backend) -> Self {
        Self {
            snapshot: backend.cluster_startup_snapshot(),
            selected: None,
            diagnostics_open: false,
            refreshing: false,
            refresh_rx: None,
        }
    }

    pub fn begin_refresh(&mut self, backend: Backend, waker: Option<sui::UiHandle>) -> bool {
        if self.refreshing {
            return false;
        }
        let (tx, rx) = mpsc::channel();
        match thread::Builder::new()
            .name("sinomo-app-cluster-refresh".to_string())
            .spawn(move || {
                let snapshot = backend.refresh_cluster_snapshot();
                let _ = tx.send(snapshot);
                if let Some(waker) = waker {
                    waker.wake();
                }
            }) {
            Ok(_) => {
                self.refreshing = true;
                self.refresh_rx = Some(rx);
                true
            }
            Err(error) => {
                self.refreshing = false;
                self.refresh_rx = None;
                self.snapshot
                    .warnings
                    .push(format!("cluster refresh failed: {error}"));
                true
            }
        }
    }

    /// Drop any result tied to the previous daemon identity and immediately refresh through the
    /// newly-bound backend. The last good topology remains visible while the replacement snapshot
    /// is loading.
    pub(crate) fn rebind_backend(
        &mut self,
        backend: Backend,
        waker: Option<sui::UiHandle>,
    ) -> bool {
        self.refresh_rx = None;
        self.refreshing = false;
        self.begin_refresh(backend, waker)
    }

    pub fn pump_refresh(&mut self) -> bool {
        let snapshot = match self.refresh_rx.as_ref().map(|rx| rx.try_recv()) {
            Some(Ok(snapshot)) => snapshot,
            Some(Err(TryRecvError::Empty)) | None => return false,
            Some(Err(TryRecvError::Disconnected)) => {
                self.refresh_rx = None;
                self.refreshing = false;
                self.snapshot
                    .warnings
                    .push("cluster refresh worker stopped".to_string());
                return true;
            }
        };
        self.refresh_rx = None;
        self.refreshing = false;
        if !selection_exists(&snapshot, self.selected.as_ref()) {
            self.selected = None;
            self.diagnostics_open = false;
        }
        self.snapshot = snapshot;
        true
    }
}

pub fn request_refresh(shared: &Shared) -> bool {
    let (backend, waker) = {
        let app = shared.borrow();
        let Some(ready) = app.ready.as_ref() else {
            return false;
        };
        (ready.backend.clone(), app.waker.clone())
    };
    shared.cluster_mut().begin_refresh(backend, waker)
}

pub fn pump_refresh(shared: &Shared) -> bool {
    if !shared.is_ready() {
        return false;
    }
    shared.cluster_mut().pump_refresh()
}

#[derive(Clone, Debug)]
struct Topology {
    vertices: Vec<GraphVertex>,
    edges: Vec<GraphEdge>,
    lan_segments: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Debug)]
struct GraphVertex {
    id: String,
    label: String,
    detail: String,
    kind: GraphVertexKind,
    availability: String,
    public_reachable: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GraphVertexKind {
    Node,
    LanSegment,
}

fn graph_vertex_selection(vertex: &GraphVertex) -> ClusterSelection {
    match vertex.kind {
        GraphVertexKind::Node => ClusterSelection::Node(vertex.id.clone()),
        GraphVertexKind::LanSegment => ClusterSelection::LanSegment(vertex.id.clone()),
    }
}

#[derive(Clone, Debug)]
struct GraphEdge {
    a: String,
    b: String,
    state: String,
    public_ip: bool,
    virtual_lan: bool,
}

#[derive(Clone, Debug)]
struct EdgeAccumulator {
    state: String,
    public_ip: bool,
    has_lan: bool,
}

impl EdgeAccumulator {
    fn from_link(link: &ClusterLinkInfo) -> Self {
        Self {
            state: link.state.clone(),
            public_ip: link.public_ip,
            has_lan: link_is_lan(link),
        }
    }

    fn merge(&mut self, link: &ClusterLinkInfo) {
        self.state = best_state(&self.state, &link.state).to_string();
        self.public_ip |= link.public_ip;
        self.has_lan |= link_is_lan(link);
    }
}

#[derive(Clone)]
struct LayoutVertex {
    vertex: GraphVertex,
    center: Point,
    radius: f32,
}

pub fn view(shared: Shared) -> impl Widget {
    ClusterView::new(shared)
}

struct ClusterView {
    shared: Shared,
    keyboard_focus: RefCell<Option<ClusterSelection>>,
    header: WidgetPod,
    empty: WidgetPod,
    legend: WidgetPod,
    detail: WidgetPod,
}

impl ClusterView {
    fn new(shared: Shared) -> Self {
        let reader = cluster_theme_reader(&shared);
        Self {
            header: WidgetPod::new(cluster_header_host(&shared, &reader)),
            empty: WidgetPod::new(cluster_empty_host(&shared, &reader)),
            legend: WidgetPod::new(cluster_legend(&reader)),
            detail: WidgetPod::new(cluster_detail_host(&shared, &reader)),
            shared,
            keyboard_focus: RefCell::new(None),
        }
    }

    fn palette(&self) -> (sui::DefaultTheme, Palette) {
        let theme = self.shared.theme();
        let pal = Palette::from_theme(&theme, widgets::theme_is_dark(&self.shared));
        (theme, pal)
    }

    fn focused_vertex_index(&self, topology: &Topology) -> Option<usize> {
        self.keyboard_focus
            .borrow()
            .as_ref()
            .and_then(|focused| {
                topology
                    .vertices
                    .iter()
                    .position(|vertex| graph_vertex_selection(vertex) == *focused)
            })
            .or_else(|| {
                let selected = self.shared.cluster().selected.clone()?;
                topology
                    .vertices
                    .iter()
                    .position(|vertex| graph_vertex_selection(vertex) == selected)
            })
            .or_else(|| (!topology.vertices.is_empty()).then_some(0))
    }

    fn move_keyboard_focus(&self, topology: &Topology, delta: isize) -> bool {
        if topology.vertices.is_empty() {
            *self.keyboard_focus.borrow_mut() = None;
            return false;
        }
        let current = self.focused_vertex_index(topology).unwrap_or(0);
        let len = topology.vertices.len() as isize;
        let next = (current as isize + delta).rem_euclid(len) as usize;
        *self.keyboard_focus.borrow_mut() = topology.vertices.get(next).map(graph_vertex_selection);
        true
    }

    fn activate_keyboard_vertex(&self, topology: &Topology) -> bool {
        let Some(index) = self.focused_vertex_index(topology) else {
            return false;
        };
        let Some(vertex) = topology.vertices.get(index) else {
            return false;
        };
        let selection = graph_vertex_selection(vertex);
        *self.keyboard_focus.borrow_mut() = Some(selection.clone());
        let mut cluster = self.shared.cluster_mut();
        if cluster.selected != Some(selection.clone()) {
            cluster.diagnostics_open = false;
        }
        cluster.selected = Some(selection);
        true
    }
}

impl Widget for ClusterView {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        match event {
            Event::Keyboard(key) if key.state == KeyState::Pressed && ctx.is_focused() => {
                let snapshot = self.shared.cluster().snapshot.clone();
                let topology = build_topology(&snapshot);
                let handled = match key.key.as_str() {
                    "ArrowRight" | "ArrowDown" => self.move_keyboard_focus(&topology, 1),
                    "ArrowLeft" | "ArrowUp" => self.move_keyboard_focus(&topology, -1),
                    "Home" if !topology.vertices.is_empty() => {
                        *self.keyboard_focus.borrow_mut() =
                            topology.vertices.first().map(graph_vertex_selection);
                        true
                    }
                    "End" if !topology.vertices.is_empty() => {
                        *self.keyboard_focus.borrow_mut() =
                            topology.vertices.last().map(graph_vertex_selection);
                        true
                    }
                    "Enter" | " " => self.activate_keyboard_vertex(&topology),
                    _ => false,
                };
                if handled {
                    ctx.request_measure();
                    ctx.request_arrange();
                    ctx.request_paint();
                    ctx.request_semantics();
                    ctx.set_handled();
                }
            }
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Up
                    && pointer.button == Some(PointerButton::Primary)
                    && ctx.bounds().contains(pointer.position) =>
            {
                let snapshot = self.shared.cluster().snapshot.clone();
                let topology = build_topology(&snapshot);
                if topology.vertices.is_empty() {
                    return;
                }
                let graph = graph_rect(ctx.bounds());
                if !graph.contains(pointer.position)
                    || detail_panel_rect(ctx.bounds()).contains(pointer.position)
                {
                    return;
                }
                let layout = layout_vertices(&topology, graph);
                let selection = hit_test(&layout, pointer.position);
                *self.keyboard_focus.borrow_mut() = selection.clone();
                let mut cluster = self.shared.cluster_mut();
                if cluster.selected != selection {
                    cluster.diagnostics_open = false;
                }
                cluster.selected = selection;
                drop(cluster);
                ctx.request_focus();
                ctx.request_measure();
                ctx.request_arrange();
                ctx.request_paint();
                ctx.request_semantics();
                ctx.set_handled();
            }
            Event::Window(WindowEvent::RedrawRequested) => {
                ctx.request_paint();
            }
            _ => {}
        }
    }

    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let size = constraints.max;
        let bounds = Rect::new(0.0, 0.0, size.width.max(0.0), size.height.max(0.0));
        let graph = graph_rect(bounds);
        let detail = detail_panel_rect(bounds);
        let legend = legend_rect(bounds);
        let has_vertices = !self.shared.cluster().snapshot.nodes.is_empty();

        self.header.measure(
            ctx,
            Constraints::tight(Size::new(bounds.width(), HEADER_H.min(bounds.height()))),
        );
        if has_vertices {
            self.empty.measure(ctx, Constraints::tight(Size::ZERO));
            self.legend
                .measure(ctx, Constraints::tight(rect_size(legend)));
            self.detail
                .measure(ctx, Constraints::tight(rect_size(detail)));
        } else {
            self.empty
                .measure(ctx, Constraints::tight(rect_size(graph)));
            self.legend.measure(ctx, Constraints::tight(Size::ZERO));
            self.detail.measure(ctx, Constraints::tight(Size::ZERO));
        }
        size
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        let header = Rect::new(
            bounds.x(),
            bounds.y(),
            bounds.width(),
            HEADER_H.min(bounds.height()),
        );
        self.header.arrange(ctx, header);
        if self.shared.cluster().snapshot.nodes.is_empty() {
            self.empty.arrange(ctx, graph_rect(bounds));
            self.legend.arrange(ctx, Rect::ZERO);
            self.detail.arrange(ctx, Rect::ZERO);
        } else {
            self.empty.arrange(ctx, Rect::ZERO);
            self.legend.arrange(ctx, legend_rect(bounds));
            self.detail.arrange(ctx, detail_panel_rect(bounds));
        }
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let (theme, pal) = self.palette();
        let snapshot = self.shared.cluster().snapshot.clone();
        let topology = build_topology(&snapshot);
        let bounds = ctx.bounds();
        let graph = graph_rect(bounds);
        let layout = layout_vertices(&topology, graph);
        let selected = {
            let cluster = self.shared.cluster();
            cluster.selected.clone()
        };

        ctx.fill_bounds(pal.window);
        self.header.paint(ctx);

        if topology.vertices.is_empty() {
            self.empty.paint(ctx);
            return;
        }

        paint_edges(ctx, &pal, &topology, &layout);
        paint_vertices(ctx, &theme, &pal, &layout, &selected);
        if ctx.is_focused()
            && let Some(index) = self.focused_vertex_index(&topology)
            && let Some(vertex) = layout.get(index)
        {
            ctx.stroke(
                Path::circle(vertex.center, vertex.radius + 5.0),
                theme.palette.focus_ring,
                StrokeStyle::new(2.0),
            );
        }
        self.legend.paint(ctx);
        self.detail.paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let snapshot = self.shared.cluster().snapshot.clone();
        let topology = build_topology(&snapshot);
        let layout = layout_vertices(&topology, graph_rect(ctx.bounds()));
        let focused_index = self.focused_vertex_index(&topology);
        let selected_vertex = self.shared.cluster().selected.clone();
        let mut node = SemanticsNode::new(ctx.widget_id(), SemanticsRole::List, ctx.bounds());
        node.name = Some("Cluster nodes and LAN segments".to_string());
        node.description = Some(
            "Use arrow keys to move between topology items and Enter or Space to select one"
                .to_string(),
        );
        node.state.focused = ctx.is_focused();
        node.actions = vec![SemanticsAction::Focus];
        ctx.push(node);

        for (index, vertex) in layout.iter().enumerate() {
            let selection = graph_vertex_selection(&vertex.vertex);
            let mut item = SemanticsNode::new(
                cluster_vertex_widget_id(ctx.widget_id(), &vertex.vertex),
                SemanticsRole::ListItem,
                Rect::new(
                    vertex.center.x - vertex.radius,
                    vertex.center.y - vertex.radius,
                    vertex.radius * 2.0,
                    vertex.radius * 2.0,
                ),
            );
            item.parent = Some(ctx.widget_id());
            item.name = Some(vertex.vertex.label.clone());
            item.description = Some(format!(
                "{}; {}; {}",
                match vertex.vertex.kind {
                    GraphVertexKind::Node => "Cluster node",
                    GraphVertexKind::LanSegment => "LAN segment",
                },
                vertex.vertex.detail,
                vertex.vertex.availability
            ));
            item.state.selected = selected_vertex.as_ref() == Some(&selection);
            item.state.focused = ctx.is_focused() && focused_index == Some(index);
            item.actions = vec![SemanticsAction::Focus, SemanticsAction::Activate];
            ctx.push(item);
        }

        self.header.semantics(ctx);
        if topology.vertices.is_empty() {
            self.empty.semantics(ctx);
        } else {
            self.legend.semantics(ctx);
            self.detail.semantics(ctx);
        }
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.header);
        visitor.visit(&self.empty);
        visitor.visit(&self.legend);
        visitor.visit(&self.detail);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.header);
        visitor.visit(&mut self.empty);
        visitor.visit(&mut self.legend);
        visitor.visit(&mut self.detail);
    }

    fn accepts_focus(&self) -> bool {
        true
    }

    fn focus_changed(&mut self, ctx: &mut EventCtx, _focused: bool) {
        ctx.request_paint();
        ctx.request_semantics();
    }
}

type ThemeReader = Rc<dyn Fn() -> DefaultTheme>;

fn cluster_theme_reader(shared: &Shared) -> ThemeReader {
    let shared = shared.clone();
    Rc::new(move || shared.theme())
}

fn clone_reader(reader: &ThemeReader) -> impl Fn() -> DefaultTheme + 'static {
    let reader = Rc::clone(reader);
    move || reader()
}

fn cluster_theme_signature(shared: &Shared) -> String {
    let app = shared.borrow();
    format!("{:?}:{:?}", app.theme_mode, app.text_scale)
}

fn cluster_header_signature(shared: &Shared) -> String {
    let theme = cluster_theme_signature(shared);
    let cluster = shared.cluster();
    format!("{theme}|{:?}|{}", cluster.snapshot, cluster.refreshing)
}

fn cluster_header_host(shared: &Shared, reader: &ThemeReader) -> RebuildOnChange<String> {
    let shared = shared.clone();
    let build_shared = shared.clone();
    let reader = Rc::clone(reader);
    RebuildOnChange::new(
        move || cluster_header_signature(&shared),
        move |_key| build_cluster_header(&build_shared, &reader),
    )
}

fn build_cluster_header(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let (snapshot, refreshing) = {
        let cluster = shared.cluster();
        (cluster.snapshot.clone(), cluster.refreshing)
    };
    let topology = build_topology(&snapshot);
    let theme = shared.theme();
    let pal = Palette::from_theme(&theme, widgets::theme_is_dark(shared));
    let summary = Flex::vertical()
        .align_items(Alignment::Stretch)
        .gap(2.0)
        .with_child(Label::new("Cluster").style(pal.text_style(
            &theme,
            theme.text.xl.size,
            FontWeight::SEMIBOLD,
            pal.text,
        )))
        .with_child(
            Label::new(cluster_header_summary(&snapshot, &topology)).style(pal.text_style(
                &theme,
                theme.text.sm.size,
                FontWeight::NORMAL,
                pal.text_muted,
            )),
        );
    let (status, tone) = if refreshing {
        ("Refreshing".to_string(), SemanticTone::Accent)
    } else {
        (
            cluster_health_summary(&snapshot),
            cluster_health_tone(&snapshot),
        )
    };
    let row = TrailingSlotRow::new(
        summary,
        StatusBadge::new(status)
            .tone(tone)
            .theme_when(clone_reader(reader)),
    )
    .trailing_width(142.0)
    .trailing_height(30.0)
    .gap(12.0);
    WidgetPod::new(
        Surface::window(row)
            .border(SurfaceBorder::Bottom)
            .padding(Insets {
                left: PAD,
                right: PAD,
                top: 10.0,
                bottom: 10.0,
            })
            .fill()
            .theme_when(clone_reader(reader)),
    )
}

fn cluster_empty_signature(shared: &Shared) -> String {
    let theme = cluster_theme_signature(shared);
    let cluster = shared.cluster();
    format!("{theme}|{:?}|{}", cluster.snapshot, cluster.refreshing)
}

fn cluster_empty_host(shared: &Shared, reader: &ThemeReader) -> RebuildOnChange<String> {
    let shared = shared.clone();
    let build_shared = shared.clone();
    let reader = Rc::clone(reader);
    RebuildOnChange::new(
        move || cluster_empty_signature(&shared),
        move |_key| build_cluster_empty(&build_shared, &reader),
    )
}

fn build_cluster_empty(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let (snapshot, refreshing) = {
        let cluster = shared.cluster();
        (cluster.snapshot.clone(), cluster.refreshing)
    };
    let title = if snapshot.warnings.is_empty() {
        "No cluster nodes"
    } else {
        "Cluster metadata unavailable"
    };
    let description = if refreshing {
        "Refreshing cluster membership and health in the background"
    } else {
        "Initialize or join a cluster to populate this view"
    };
    let detail = snapshot
        .warnings
        .first()
        .cloned()
        .unwrap_or_else(|| "Cluster nodes appear here after discovery completes.".to_string());
    let refresh_shared = shared.clone();
    let refresh = Button::new(if refreshing { "Refreshing" } else { "Refresh" })
        .icon(IconGlyph::Redo)
        .description("Refresh cluster health and topology")
        .appearance(ButtonAppearance::Tonal)
        .tone(SemanticTone::Accent)
        .enabled(!refreshing)
        .theme_when(clone_reader(reader))
        .on_press_with_ctx(move |ctx| {
            request_refresh(&refresh_shared);
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_paint();
        });
    WidgetPod::new(
        EmptyState::new(title, description)
            .name("Cluster empty state")
            .icon(IconGlyph::Link)
            .detail(detail)
            .action(refresh)
            .action_max_width(160.0)
            .transparent()
            .theme_when(clone_reader(reader)),
    )
}

fn cluster_legend(reader: &ThemeReader) -> Surface {
    let row = Flex::horizontal()
        .align_items(Alignment::Center)
        .gap(5.0)
        .with_child(
            StatusBadge::new("Online")
                .tone(SemanticTone::Success)
                .theme_when(clone_reader(reader)),
        )
        .with_child(
            StatusBadge::new("Offline")
                .tone(SemanticTone::Danger)
                .theme_when(clone_reader(reader)),
        )
        .with_child(
            StatusBadge::new("Unknown")
                .tone(SemanticTone::Neutral)
                .theme_when(clone_reader(reader)),
        )
        .with_child(
            StatusBadge::new("LAN")
                .tone(SemanticTone::Warning)
                .theme_when(clone_reader(reader)),
        )
        .with_child(
            StatusBadge::new("Public route")
                .tone(SemanticTone::Info)
                .theme_when(clone_reader(reader)),
        );
    Surface::panel(row)
        .appearance(SurfaceAppearance::Raised)
        .elevation(SurfaceElevation::Small)
        .padding(Insets::all(5.0))
        .fill()
        .theme_when(clone_reader(reader))
}

fn paint_edges(ctx: &mut PaintCtx, pal: &Palette, topology: &Topology, layout: &[LayoutVertex]) {
    let centers = layout
        .iter()
        .map(|vertex| (vertex.vertex.id.as_str(), vertex.center))
        .collect::<BTreeMap<_, _>>();
    for edge in &topology.edges {
        let (Some(a), Some(b)) = (centers.get(edge.a.as_str()), centers.get(edge.b.as_str()))
        else {
            continue;
        };
        let color = if edge.public_ip {
            pal.info
        } else if edge.state == "degraded" {
            pal.warn
        } else {
            pal.border_strong
        };
        let width = if edge.virtual_lan { 1.0 } else { 1.5 };
        let mut path = PathBuilder::new();
        path.move_to(*a).line_to(*b);
        ctx.stroke(path.build(), color, StrokeStyle::new(width));
    }
}

fn paint_vertices(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    layout: &[LayoutVertex],
    selected: &Option<ClusterSelection>,
) {
    for vertex in layout {
        let selected_here = matches!(
            (selected, &vertex.vertex.kind),
            (Some(ClusterSelection::Node(id)), GraphVertexKind::Node) if id == &vertex.vertex.id
        ) || matches!(
            (selected, &vertex.vertex.kind),
            (Some(ClusterSelection::LanSegment(id)), GraphVertexKind::LanSegment)
                if id == &vertex.vertex.id
        );
        let fill = vertex_fill(pal, &vertex.vertex);
        let border = if selected_here {
            pal.accent
        } else if vertex.vertex.kind == GraphVertexKind::LanSegment {
            pal.warn
        } else if vertex.vertex.availability == "unknown" {
            pal.border_strong
        } else {
            pal.border
        };
        ctx.fill(Path::circle(vertex.center, vertex.radius), fill);
        ctx.stroke(
            Path::circle(
                vertex.center,
                vertex.radius + if selected_here { 4.0 } else { 1.0 },
            ),
            border,
            StrokeStyle::new(if selected_here { 2.0 } else { 1.25 }),
        );
        if selected_here {
            // Selected nodes glow like the Mesh cluster map's self node — a
            // soft accent halo outside the ring (empty in Light, damped in Void).
            if let Some(glow) = pal.glow_accent.first {
                ctx.stroke(
                    Path::circle(vertex.center, vertex.radius + 6.0),
                    glow.color,
                    StrokeStyle::new(glow.blur * 0.35),
                );
            }
        }
        if vertex.vertex.kind == GraphVertexKind::Node && vertex.vertex.public_reachable {
            paint_public_reachability_badge(ctx, pal, vertex.center, vertex.radius);
        }

        let label_style = pal.text_style(theme, 12.0, FontWeight::MEDIUM, pal.text);
        let detail_style = pal.text_style(theme, 11.0, FontWeight::NORMAL, pal.text_faint);
        let text_layout = vertex_text_layout(
            vertex.center,
            vertex.radius,
            label_style.line_height,
            detail_style.line_height,
        );
        widgets::draw_centered(
            ctx,
            &vertex.vertex.label,
            label_style,
            vertex.center.x,
            text_layout.label_center_y,
        );
        widgets::draw_centered(
            ctx,
            &vertex.vertex.detail,
            detail_style,
            vertex.center.x,
            text_layout.detail_center_y,
        );
    }
}

#[derive(Clone, Copy, Debug)]
struct VertexTextLayout {
    label_center_y: f32,
    detail_center_y: f32,
}

fn vertex_text_layout(
    center: Point,
    radius: f32,
    label_line_height: f32,
    detail_line_height: f32,
) -> VertexTextLayout {
    let label_line_height = label_line_height.max(1.0);
    let detail_line_height = detail_line_height.max(1.0);
    let label_top = center.y + radius + VERTEX_TEXT_TOP_GAP;
    let label_bottom = label_top + label_line_height;
    let detail_top = label_bottom + VERTEX_TEXT_LINE_GAP;
    VertexTextLayout {
        label_center_y: label_top + label_line_height * 0.5,
        detail_center_y: detail_top + detail_line_height * 0.5,
    }
}

fn vertex_fill(pal: &Palette, vertex: &GraphVertex) -> Color {
    match vertex.kind {
        GraphVertexKind::LanSegment => lan_segment_color(pal),
        GraphVertexKind::Node => match vertex.availability.as_str() {
            "online" => pal.good,
            "offline" => pal.bad,
            "degraded" => pal.warn,
            _ => unknown_node_color(pal),
        },
    }
}

fn paint_public_reachability_badge(ctx: &mut PaintCtx, pal: &Palette, center: Point, radius: f32) {
    let badge_center = Point::new(center.x + radius * 0.58, center.y - radius * 0.58);
    ctx.fill(Path::circle(badge_center, 5.5), public_node_color(pal));
    ctx.stroke(
        Path::circle(badge_center, 6.4),
        pal.panel,
        StrokeStyle::new(1.8),
    );
}

/// Public/internet reachability rides the informational hue.
fn public_node_color(pal: &Palette) -> Color {
    pal.info
}

/// LAN segment hubs use the amber agent hue — a topology identity, not a
/// warning (labels next to them stay on ink tokens).
fn lan_segment_color(pal: &Palette) -> Color {
    pal.agent[3]
}

/// Unknown availability reads as disabled, not as a status.
fn unknown_node_color(pal: &Palette) -> Color {
    pal.text_disabled
}

fn cluster_detail_signature(shared: &Shared) -> String {
    let theme = cluster_theme_signature(shared);
    let cluster = shared.cluster();
    format!(
        "{theme}|{:?}|{:?}|{}|{}",
        cluster.snapshot, cluster.selected, cluster.diagnostics_open, cluster.refreshing
    )
}

fn cluster_detail_host(shared: &Shared, reader: &ThemeReader) -> RebuildOnChange<String> {
    let shared = shared.clone();
    let build_shared = shared.clone();
    let reader = Rc::clone(reader);
    RebuildOnChange::new(
        move || cluster_detail_signature(&shared),
        move |_key| build_cluster_detail(&build_shared, &reader),
    )
}

fn build_cluster_detail(shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let (snapshot, selection, diagnostics_open, refreshing) = {
        let cluster = shared.cluster();
        (
            cluster.snapshot.clone(),
            cluster.selected.clone(),
            cluster.diagnostics_open,
            cluster.refreshing,
        )
    };
    let topology = build_topology(&snapshot);
    let lines = selection
        .as_ref()
        .map(|selection| detail_lines(&snapshot, &topology, selection, diagnostics_open))
        .unwrap_or_else(|| overview_lines(&snapshot, &topology, diagnostics_open, refreshing));
    let title = lines
        .first()
        .cloned()
        .unwrap_or_else(|| "Cluster overview".to_string());
    let mut rows = Flex::vertical().align_items(Alignment::Stretch).gap(8.0);
    for line in lines.iter().skip(1).filter(|line| !line.trim().is_empty()) {
        let (label, value, max_lines) = if let Some(hint) = line.strip_prefix("hint:") {
            ("Guidance", hint.trim(), 3)
        } else if let Some((label, value)) = line.split_once(':') {
            (
                label.trim(),
                value.trim(),
                if diagnostics_open { 3 } else { 2 },
            )
        } else {
            ("Detail", line.trim(), 2)
        };
        rows.push(
            DetailRow::new(label, value)
                .max_value_lines(max_lines)
                .theme_when(clone_reader(reader)),
        );
    }
    let (status, tone) = cluster_detail_status(&snapshot, &topology, selection.as_ref());
    let details = PanelSection::new(
        title,
        ScrollView::vertical(rows)
            .name("Cluster detail fields")
            .theme(shared.theme())
            .viewport_width_hint(true),
    )
    .header_action(
        StatusBadge::new(status)
            .tone(tone)
            .theme_when(clone_reader(reader)),
    )
    .theme_when(clone_reader(reader));

    let refresh_shared = shared.clone();
    let refresh = Button::new(if refreshing { "Refreshing" } else { "Refresh" })
        .icon(IconGlyph::Redo)
        .description("Refresh cluster health and topology")
        .appearance(ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .min_height(DETAIL_BUTTON_H)
        .enabled(!refreshing)
        .theme_when(clone_reader(reader))
        .on_press_with_ctx(move |ctx| {
            request_refresh(&refresh_shared);
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_paint();
        });
    let warning_visible = if selection.is_some() {
        selected_node_needs_connection_settings(&snapshot, selection.as_ref())
    } else {
        !refreshing && cluster_actionable_warning(&snapshot).is_some()
    };
    let mut primary_actions = Flex::horizontal()
        .align_items(Alignment::Stretch)
        .gap(8.0)
        .with_item(refresh, FlexItem::fill().min_width(0.0));
    if warning_visible {
        let settings_shared = shared.clone();
        let description = if selection.is_some() {
            "Open connection settings for selected cluster node"
        } else {
            "Open connection settings for cluster warning"
        };
        primary_actions.push_item(
            Button::new("Connection settings")
                .icon(IconGlyph::Alert)
                .description(description)
                .appearance(ButtonAppearance::Filled)
                .tone(SemanticTone::Accent)
                .min_height(DETAIL_BUTTON_H)
                .theme_when(clone_reader(reader))
                .on_press_with_ctx(move |ctx| {
                    open_connection_settings(&settings_shared);
                    ctx.request_measure();
                    ctx.request_arrange();
                    ctx.request_paint();
                }),
            FlexItem::flex(1.35).min_width(0.0),
        );
    }

    let toggle_shared = shared.clone();
    let mut secondary_actions = Flex::horizontal()
        .align_items(Alignment::Stretch)
        .gap(8.0)
        .with_item(
            Button::new(if diagnostics_open {
                "Hide diagnostics"
            } else {
                "Show diagnostics"
            })
            .icon(if diagnostics_open {
                IconGlyph::ChevronUp
            } else {
                IconGlyph::ChevronDown
            })
            .description("Show technical cluster source and identifier details")
            .appearance(ButtonAppearance::Outline)
            .tone(SemanticTone::Neutral)
            .min_height(DETAIL_BUTTON_H)
            .theme_when(clone_reader(reader))
            .on_press_with_ctx(move |ctx| {
                let open = toggle_shared.cluster().diagnostics_open;
                toggle_shared.cluster_mut().diagnostics_open = !open;
                ctx.request_measure();
                ctx.request_arrange();
                ctx.request_paint();
                ctx.request_semantics();
            }),
            FlexItem::fill().min_width(0.0),
        );
    if selection
        .as_ref()
        .is_some_and(|selection| selected_copy_details(&snapshot, selection).is_some())
    {
        let copy_shared = shared.clone();
        let copy_selection = selection.clone().expect("selection checked above");
        let copy_value = selected_copy_details(&snapshot, &copy_selection).unwrap_or_default();
        secondary_actions.push_item(
            Button::new("Copy details")
                .semantic_name("Copy node details")
                .icon(IconGlyph::Restore)
                .description(format!(
                    "Copy the full node ID, address, and endpoints: {copy_value}"
                ))
                .appearance(ButtonAppearance::Outline)
                .tone(SemanticTone::Neutral)
                .min_height(DETAIL_BUTTON_H)
                .theme_when(clone_reader(reader))
                .on_press_with_ctx(move |ctx| {
                    if let Some(value) =
                        selected_copy_details(&copy_shared.cluster().snapshot, &copy_selection)
                    {
                        ctx.set_clipboard_text(value);
                    }
                    ctx.request_semantics();
                }),
            FlexItem::fill().min_width(0.0),
        );
    }

    let actions = Flex::vertical()
        .align_items(Alignment::Stretch)
        .gap(8.0)
        .with_item(primary_actions, FlexItem::fixed(DETAIL_BUTTON_H))
        .with_item(secondary_actions, FlexItem::fixed(DETAIL_BUTTON_H));
    let content = Dock::new(details).bottom(DETAIL_BUTTON_H * 2.0 + 8.0, actions);
    WidgetPod::new(
        Surface::panel(content)
            .name("Cluster inspector")
            .appearance(SurfaceAppearance::Raised)
            .elevation(SurfaceElevation::Medium)
            .padding(Insets::all(PANEL_PAD))
            .fill()
            .theme_when(clone_reader(reader)),
    )
}

fn cluster_detail_status(
    snapshot: &ClusterSnapshot,
    topology: &Topology,
    selection: Option<&ClusterSelection>,
) -> (String, SemanticTone) {
    match selection {
        Some(ClusterSelection::Node(id)) => snapshot
            .nodes
            .iter()
            .find(|node| &node.node_id == id)
            .map(|node| {
                (
                    node.availability.clone(),
                    availability_tone(&node.availability),
                )
            })
            .unwrap_or_else(|| ("Missing".to_string(), SemanticTone::Danger)),
        Some(ClusterSelection::LanSegment(id)) => (
            count_noun(
                topology
                    .lan_segments
                    .get(id)
                    .map(Vec::len)
                    .unwrap_or_default(),
                "member",
                "members",
            ),
            SemanticTone::Info,
        ),
        None => (
            cluster_health_summary(snapshot),
            cluster_health_tone(snapshot),
        ),
    }
}

fn availability_tone(availability: &str) -> SemanticTone {
    match availability.to_ascii_lowercase().as_str() {
        "online" | "healthy" => SemanticTone::Success,
        "offline" | "unavailable" => SemanticTone::Danger,
        "degraded" | "unknown" => SemanticTone::Warning,
        _ => SemanticTone::Neutral,
    }
}

fn cluster_health_tone(snapshot: &ClusterSnapshot) -> SemanticTone {
    let counts = availability_counts(snapshot);
    if counts.offline > 0 {
        SemanticTone::Danger
    } else if counts.unknown > 0 || counts.total == 0 {
        SemanticTone::Warning
    } else {
        SemanticTone::Success
    }
}

fn open_connection_settings(shared: &Shared) {
    let mut app = shared.borrow_mut();
    app.active_tab = Tab::Settings;
    if let Some(ready) = app.ready.as_mut() {
        ready.settings.open(SettingsSection::Connection);
    }
}

fn overview_lines(
    snapshot: &ClusterSnapshot,
    topology: &Topology,
    diagnostics_open: bool,
    refreshing: bool,
) -> Vec<String> {
    let mut lines = vec![
        "Cluster overview".to_string(),
        format!("connection: {}", source_summary(&snapshot.source)),
        format!("health: {}", cluster_health_summary(snapshot)),
        format!("public route: {}", public_reachability_summary(snapshot)),
        format!("mode: {}", cluster_route_mode_summary(snapshot)),
        format!("LAN: {}", lan_segment_summary(topology)),
    ];
    let warning = cluster_actionable_warning(snapshot);
    if refreshing {
        lines.push("status: refreshing current health".to_string());
    } else if let Some(warning) = warning {
        lines.push(format!(
            "attention: {}",
            cluster_attention_summary(snapshot, warning)
        ));
    }
    if diagnostics_open {
        if let Some(warning) = snapshot.warnings.first() {
            let label = if refreshing {
                "previous warning detail"
            } else {
                "warning detail"
            };
            lines.push(format!("{label}: {}", short(warning, 42)));
        }
        lines.push(format!(
            "topology: {} direct edges",
            direct_topology_edge_count(topology)
        ));
        lines.push(format!("diagnostics: {}", snapshot.source));
        lines.push(format!(
            "cluster: {}",
            snapshot
                .cluster_id
                .as_deref()
                .map(|id| short(id, 42))
                .unwrap_or("-".into())
        ));
        lines.push(format!(
            "local: {}",
            snapshot
                .local_node_id
                .as_deref()
                .map(|id| short(id, 42))
                .unwrap_or("-".into())
        ));
    } else {
        lines.push(if warning.is_some() {
            if refreshing {
                "hint: Refreshing current health; show diagnostics for previous warning detail."
                    .to_string()
            } else {
                "hint: Show diagnostics for warning detail, source, and cluster identifiers."
                    .to_string()
            }
        } else {
            "hint: Show diagnostics for source and cluster identifiers.".to_string()
        });
    }
    lines.push(
        "hint: Select a node or LAN segment for process and reachability details.".to_string(),
    );
    lines
}

#[derive(Clone, Copy, Default)]
struct AvailabilityCounts {
    total: usize,
    online: usize,
    offline: usize,
    unknown: usize,
}

fn availability_counts(snapshot: &ClusterSnapshot) -> AvailabilityCounts {
    let mut counts = AvailabilityCounts {
        total: snapshot.nodes.len(),
        ..AvailabilityCounts::default()
    };
    for node in &snapshot.nodes {
        match node.availability.to_ascii_lowercase().as_str() {
            "online" => counts.online += 1,
            "offline" | "unavailable" => counts.offline += 1,
            _ => counts.unknown += 1,
        }
    }
    counts
}

fn cluster_header_summary(snapshot: &ClusterSnapshot, topology: &Topology) -> String {
    let public = snapshot
        .nodes
        .iter()
        .filter(|node| node.public_reachable)
        .count();
    format!(
        "{} · {} · LAN {} · public {}/{} · {}",
        count_noun(snapshot.nodes.len(), "node", "nodes"),
        cluster_health_summary(snapshot).replace(" / ", "/"),
        count_noun(topology.lan_segments.len(), "segment", "segments"),
        public,
        snapshot.nodes.len(),
        source_summary(&snapshot.source)
    )
}

fn cluster_health_summary(snapshot: &ClusterSnapshot) -> String {
    let counts = availability_counts(snapshot);
    if counts.total == 0 {
        return "no nodes discovered".to_string();
    }

    let mut parts = vec![format!("{} / {} online", counts.online, counts.total)];
    if counts.offline > 0 {
        parts.push(plural_count(counts.offline, "offline"));
    }
    if counts.unknown > 0 {
        parts.push(plural_count(counts.unknown, "unknown"));
    }
    parts.join(", ")
}

fn direct_topology_edge_count(topology: &Topology) -> usize {
    topology
        .edges
        .iter()
        .filter(|edge| !edge.virtual_lan)
        .count()
}

fn lan_segment_summary(topology: &Topology) -> String {
    count_noun(topology.lan_segments.len(), "segment", "segments")
}

fn public_reachability_summary(snapshot: &ClusterSnapshot) -> String {
    let public = snapshot
        .nodes
        .iter()
        .filter(|node| node.public_reachable)
        .count();
    format!("{public} / {} nodes", snapshot.nodes.len())
}

fn cluster_route_mode_summary(snapshot: &ClusterSnapshot) -> &'static str {
    let counts = availability_counts(snapshot);
    if counts.total == 0 {
        return "unknown until nodes report endpoints";
    }

    let public = snapshot
        .nodes
        .iter()
        .filter(|node| node.public_reachable)
        .count();
    if public > 0 {
        return "public and private routing";
    }
    if counts.online == counts.total {
        return "private/LAN; public ingress not required";
    }
    "private or unverified routing"
}

fn cluster_attention_summary(snapshot: &ClusterSnapshot, warning: &str) -> String {
    let counts = availability_counts(snapshot);
    if counts.offline > 0 {
        return format!(
            "{} {}",
            counts.offline,
            node_plural(counts.offline, "offline")
        );
    }
    if counts.unknown > 0 {
        return format!(
            "{} {}",
            counts.unknown,
            node_plural(counts.unknown, "unknown")
        );
    }
    cluster_warning_summary(warning).to_string()
}

fn cluster_actionable_warning(snapshot: &ClusterSnapshot) -> Option<&str> {
    let warning = snapshot.warnings.first()?.as_str();
    let counts = availability_counts(snapshot);
    if counts.offline > 0 || counts.unknown > 0 {
        return Some(warning);
    }
    if counts.total > 0
        && counts.online == counts.total
        && cluster_warning_is_direct_probe_fallback(warning)
    {
        return None;
    }
    Some(warning)
}

fn cluster_warning_is_direct_probe_fallback(warning: &str) -> bool {
    let lower = warning.to_ascii_lowercase();
    lower.contains("daemon health aggregation unavailable")
        && lower.contains("showing direct cluster probes")
}

fn node_plural(count: usize, state: &str) -> String {
    if count == 1 {
        format!("node {state}")
    } else {
        format!("nodes {state}")
    }
}

fn count_noun(count: usize, singular: &str, plural: &str) -> String {
    if count == 1 {
        format!("1 {singular}")
    } else {
        format!("{count} {plural}")
    }
}

fn plural_count(count: usize, label: &str) -> String {
    format!("{} {}", count, label)
}

fn cluster_warning_summary(warning: &str) -> &'static str {
    let lower = warning.to_ascii_lowercase();
    if lower.contains("daemon health")
        || lower.contains("daemon")
        || lower.contains("offline")
        || lower.contains("health unknown")
    {
        "Cluster health needs attention"
    } else if lower.contains("network") || lower.contains("connect") {
        "Cluster connectivity needs attention"
    } else {
        "Cluster needs attention"
    }
}

fn detail_lines(
    snapshot: &ClusterSnapshot,
    topology: &Topology,
    selection: &ClusterSelection,
    diagnostics_open: bool,
) -> Vec<String> {
    match selection {
        ClusterSelection::Node(id) => snapshot
            .nodes
            .iter()
            .find(|node| &node.node_id == id)
            .map(|node| node_detail_lines(node, diagnostics_open))
            .unwrap_or_else(|| vec![id.clone(), "status: missing".to_string()]),
        ClusterSelection::LanSegment(id) => {
            let members = topology.lan_segments.get(id).cloned().unwrap_or_default();
            let mut lines = vec![
                id.replace('-', " "),
                format!("members: {}", members.len()),
                "basis: observed LAN connectivity clique".to_string(),
            ];
            if diagnostics_open {
                lines.push("diagnostics: member node IDs".to_string());
                lines.extend(
                    members
                        .iter()
                        .take(6)
                        .map(|member| format!("node: {}", short(member, 42))),
                );
            } else if !members.is_empty() {
                lines.push("hint: Show diagnostics for member node IDs.".to_string());
            }
            lines
        }
    }
}

fn node_detail_lines(node: &ClusterNodeInfo, diagnostics_open: bool) -> Vec<String> {
    let reachable_endpoints = node
        .endpoints
        .iter()
        .filter(|endpoint| endpoint.reachable)
        .count();
    let mut lines = vec![
        node_heading(node),
        format!("node: {}", short(&node.node_id, 26)),
        format!("health: {}", node.availability),
        format!("platform: {}", platform_summary(node.platform.as_deref())),
        format!("process: {}", node.process),
        format!("role: {} {}", node.member_role, node.member_state),
        format!("address: {}", node.address.as_deref().unwrap_or("-")),
        format!("route: {}", node_route_summary(node, reachable_endpoints)),
        format!(
            "endpoints: {} / {} reachable",
            reachable_endpoints,
            node.endpoints.len()
        ),
        format!(
            "next: {}",
            node_next_step_summary(node, reachable_endpoints)
        ),
    ];
    if diagnostics_open {
        lines.push("diagnostics: endpoint addresses".to_string());
        lines.extend(node.endpoints.iter().take(4).map(|endpoint| {
            let state = if endpoint.reachable {
                "reachable"
            } else {
                "unreachable"
            };
            format!(
                "{}: {} ({state})",
                endpoint.transport,
                short(&endpoint.address, 52)
            )
        }));
        lines.push(format!(
            "public: {}",
            if node.public_reachable {
                "reachable"
            } else {
                "not observed"
            }
        ));
        lines.push(format!("version: {}", node.daemon_version));
    }
    lines
}

fn selected_copy_details(
    snapshot: &ClusterSnapshot,
    selection: &ClusterSelection,
) -> Option<String> {
    match selection {
        ClusterSelection::Node(id) => {
            let node = snapshot.nodes.iter().find(|node| &node.node_id == id)?;
            let mut lines = vec![format!("node_id: {}", node.node_id)];
            if let Some(address) = node
                .address
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                lines.push(format!("address: {address}"));
            }
            lines.extend(
                node.endpoints
                    .iter()
                    .map(|endpoint| format!("{}: {}", endpoint.transport, endpoint.address)),
            );
            Some(lines.join("\n"))
        }
        ClusterSelection::LanSegment(_) => None,
    }
}

fn node_route_summary(node: &ClusterNodeInfo, reachable_endpoints: usize) -> &'static str {
    let availability = node.availability.to_ascii_lowercase();
    if matches!(availability.as_str(), "offline" | "unavailable") {
        return "node offline";
    }
    if node.endpoints.is_empty() {
        return "no endpoints published";
    }
    if reachable_endpoints == 0 {
        return "endpoints unreachable";
    }
    if node.public_reachable {
        "public route reachable"
    } else {
        "LAN/local route only"
    }
}

fn node_next_step_summary(node: &ClusterNodeInfo, reachable_endpoints: usize) -> &'static str {
    let availability = node.availability.to_ascii_lowercase();
    if matches!(availability.as_str(), "offline" | "unavailable") {
        return "start daemon on that node or check its route";
    }
    if node.endpoints.is_empty() {
        return "publish at least one daemon endpoint";
    }
    if reachable_endpoints == 0 {
        return "check endpoint reachability in diagnostics";
    }
    if node.public_reachable {
        "public route is reachable"
    } else {
        "LAN or local routing only"
    }
}

fn source_summary(source: &str) -> &'static str {
    if source.starts_with("daemon ") {
        "daemon connected"
    } else if source.trim().is_empty() {
        "not available"
    } else {
        "snapshot loaded"
    }
}

fn selected_node_needs_connection_settings(
    snapshot: &ClusterSnapshot,
    selection: Option<&ClusterSelection>,
) -> bool {
    let Some(ClusterSelection::Node(id)) = selection else {
        return false;
    };
    snapshot
        .nodes
        .iter()
        .find(|node| &node.node_id == id)
        .is_some_and(node_needs_connection_settings)
}

fn node_needs_connection_settings(node: &ClusterNodeInfo) -> bool {
    let availability = node.availability.to_ascii_lowercase();
    let reachable_endpoints = node
        .endpoints
        .iter()
        .filter(|endpoint| endpoint.reachable)
        .count();
    matches!(availability.as_str(), "offline" | "unavailable" | "unknown")
        || node.endpoints.is_empty()
        || reachable_endpoints == 0
}

fn detail_panel_rect(bounds: Rect) -> Rect {
    let panel_w = PANEL_W.min((bounds.width() - PAD * 2.0).max(220.0));
    let y = bounds.y() + HEADER_H + 18.0;
    let available_h = (bounds.max_y() - y - PAD).max(0.0);
    Rect::new(
        bounds.max_x() - panel_w - PAD,
        y,
        panel_w,
        DETAIL_PANEL_H.min(available_h),
    )
}

fn cluster_vertex_widget_id(parent: sui::WidgetId, vertex: &GraphVertex) -> sui::WidgetId {
    // A stable FNV-1a key keeps UIA identity attached to the node/LAN rather than its current
    // position in a freshly-sorted topology snapshot.
    let mut key = match vertex.kind {
        GraphVertexKind::Node => 0xcbf2_9ce4_8422_2325_u64,
        GraphVertexKind::LanSegment => 0x8422_2325_cbf2_9ce4_u64,
    };
    for byte in vertex.id.as_bytes() {
        key ^= u64::from(*byte);
        key = key.wrapping_mul(0x0000_0100_0000_01b3);
    }
    sui::WidgetId::new(
        parent
            .get()
            .wrapping_mul(67)
            .wrapping_add(12_101)
            .wrapping_add(key),
    )
}

fn legend_rect(bounds: Rect) -> Rect {
    let available_w = (bounds.width() - PAD * 2.0).max(0.0);
    let width = 430.0_f32.min(available_w);
    Rect::new(
        bounds.x() + PAD,
        bounds.max_y() - PAD - LEGEND_H,
        width,
        LEGEND_H,
    )
}

fn rect_size(rect: Rect) -> Size {
    Size::new(rect.width().max(0.0), rect.height().max(0.0))
}

fn graph_rect(bounds: Rect) -> Rect {
    let panel_w = PANEL_W.min((bounds.width() - PAD * 2.0).max(220.0));
    let reserve_panel = bounds.width() >= 900.0;
    let right_reserve = if reserve_panel { panel_w + PAD } else { 0.0 };
    Rect::new(
        bounds.x() + PAD,
        bounds.y() + HEADER_H + PAD,
        (bounds.width() - PAD * 2.0 - right_reserve).max(1.0),
        (bounds.height() - HEADER_H - PAD * 2.0 - 34.0).max(1.0),
    )
}

fn build_topology(snapshot: &ClusterSnapshot) -> Topology {
    let display_counts = node_display_name_counts(&snapshot.nodes);
    let mut vertices = snapshot
        .nodes
        .iter()
        .map(|node| GraphVertex {
            id: node.node_id.clone(),
            label: node_graph_label(node, &display_counts),
            detail: node_graph_detail(node),
            kind: GraphVertexKind::Node,
            availability: node.availability.clone(),
            public_reachable: node.public_reachable,
        })
        .collect::<Vec<_>>();

    let node_ids = vertices
        .iter()
        .map(|vertex| vertex.id.clone())
        .collect::<BTreeSet<_>>();
    let active_links = snapshot
        .links
        .iter()
        .filter(|link| link.state == "healthy" || link.state == "degraded")
        .filter(|link| {
            node_ids.contains(&link.source_node_id) && node_ids.contains(&link.target_node_id)
        })
        .collect::<Vec<_>>();

    let mut direct = BTreeMap::<(String, String), EdgeAccumulator>::new();
    let mut lan_pairs = BTreeSet::new();
    for link in &active_links {
        let pair = ordered_pair(&link.source_node_id, &link.target_node_id);
        if pair.0 == pair.1 {
            continue;
        }
        direct
            .entry(pair.clone())
            .and_modify(|edge| edge.merge(link))
            .or_insert_with(|| EdgeAccumulator::from_link(link));
        if link_is_lan(link) {
            lan_pairs.insert(pair);
        }
    }

    let lan_cliques = maximal_lan_cliques(&node_ids, &lan_pairs);
    let mut collapsed_lan_pairs = BTreeSet::new();
    let mut lan_segments = BTreeMap::new();
    let mut virtual_edges = Vec::new();
    for (index, clique) in lan_cliques.into_iter().enumerate() {
        let segment_id = format!("lan-segment-{}", index + 1);
        for i in 0..clique.len() {
            for j in (i + 1)..clique.len() {
                collapsed_lan_pairs.insert(ordered_pair(&clique[i], &clique[j]));
            }
        }
        vertices.push(GraphVertex {
            id: segment_id.clone(),
            label: format!("LAN {}", index + 1),
            detail: format!("{} nodes", clique.len()),
            kind: GraphVertexKind::LanSegment,
            availability: "online".to_string(),
            public_reachable: false,
        });
        for node_id in &clique {
            virtual_edges.push(GraphEdge {
                a: node_id.clone(),
                b: segment_id.clone(),
                state: "healthy".to_string(),
                public_ip: false,
                virtual_lan: true,
            });
        }
        lan_segments.insert(segment_id, clique);
    }

    let mut edges = Vec::new();
    for ((a, b), edge) in direct {
        if collapsed_lan_pairs.contains(&(a.clone(), b.clone())) && edge.has_lan && !edge.public_ip
        {
            continue;
        }
        edges.push(GraphEdge {
            a,
            b,
            state: edge.state,
            public_ip: edge.public_ip,
            virtual_lan: false,
        });
    }
    edges.extend(virtual_edges);

    Topology {
        vertices,
        edges,
        lan_segments,
    }
}

fn layout_vertices(topology: &Topology, rect: Rect) -> Vec<LayoutVertex> {
    if topology.vertices.is_empty() {
        return Vec::new();
    }
    if topology.vertices.len() == 1 {
        return vec![LayoutVertex {
            vertex: topology.vertices[0].clone(),
            center: Point::new(
                rect.x() + rect.width() / 2.0,
                rect.y() + rect.height() / 2.0,
            ),
            radius: NODE_R,
        }];
    }

    let center = Point::new(
        rect.x() + rect.width() / 2.0,
        rect.y() + rect.height() / 2.0,
    );
    let outer = (rect.width().min(rect.height()) * 0.38).max(72.0);
    let inner = (outer * 0.42).max(36.0);
    let nodes = topology
        .vertices
        .iter()
        .filter(|vertex| vertex.kind == GraphVertexKind::Node)
        .cloned()
        .collect::<Vec<_>>();
    let lans = topology
        .vertices
        .iter()
        .filter(|vertex| vertex.kind == GraphVertexKind::LanSegment)
        .cloned()
        .collect::<Vec<_>>();

    let mut out = Vec::new();
    for (index, vertex) in nodes.iter().enumerate() {
        let angle = angle_for(index, nodes.len(), -std::f32::consts::FRAC_PI_2);
        out.push(LayoutVertex {
            vertex: vertex.clone(),
            center: Point::new(
                center.x + angle.cos() * outer,
                center.y + angle.sin() * outer,
            ),
            radius: NODE_R,
        });
    }
    for (index, vertex) in lans.iter().enumerate() {
        let angle = angle_for(index, lans.len(), 0.0);
        out.push(LayoutVertex {
            vertex: vertex.clone(),
            center: if lans.len() == 1 {
                center
            } else {
                Point::new(
                    center.x + angle.cos() * inner,
                    center.y + angle.sin() * inner,
                )
            },
            radius: LAN_R,
        });
    }
    out
}

fn angle_for(index: usize, len: usize, offset: f32) -> f32 {
    if len == 0 {
        return offset;
    }
    offset + (index as f32 / len as f32) * std::f32::consts::TAU
}

fn hit_test(layout: &[LayoutVertex], point: Point) -> Option<ClusterSelection> {
    layout
        .iter()
        .rev()
        .find(|vertex| distance(vertex.center, point) <= vertex.radius + 6.0)
        .map(|vertex| match vertex.vertex.kind {
            GraphVertexKind::Node => ClusterSelection::Node(vertex.vertex.id.clone()),
            GraphVertexKind::LanSegment => ClusterSelection::LanSegment(vertex.vertex.id.clone()),
        })
}

fn selection_exists(snapshot: &ClusterSnapshot, selection: Option<&ClusterSelection>) -> bool {
    match selection {
        None => true,
        Some(ClusterSelection::Node(id)) => snapshot.nodes.iter().any(|node| &node.node_id == id),
        Some(ClusterSelection::LanSegment(id)) => {
            build_topology(snapshot).lan_segments.contains_key(id)
        }
    }
}

fn distance(a: Point, b: Point) -> f32 {
    let dx = a.x - b.x;
    let dy = a.y - b.y;
    (dx * dx + dy * dy).sqrt()
}

fn maximal_lan_cliques(
    node_ids: &BTreeSet<String>,
    lan_pairs: &BTreeSet<(String, String)>,
) -> Vec<Vec<String>> {
    let mut adj = node_ids
        .iter()
        .map(|node| (node.clone(), BTreeSet::<String>::new()))
        .collect::<BTreeMap<_, _>>();
    for (a, b) in lan_pairs {
        adj.entry(a.clone()).or_default().insert(b.clone());
        adj.entry(b.clone()).or_default().insert(a.clone());
    }
    let mut out = Vec::new();
    bron_kerbosch(
        BTreeSet::new(),
        node_ids.clone(),
        BTreeSet::new(),
        &adj,
        &mut out,
    );
    out.retain(|clique| clique.len() >= 3);
    out.sort_by(|left, right| right.len().cmp(&left.len()).then_with(|| left.cmp(right)));
    out
}

fn bron_kerbosch(
    r: BTreeSet<String>,
    mut p: BTreeSet<String>,
    mut x: BTreeSet<String>,
    adj: &BTreeMap<String, BTreeSet<String>>,
    out: &mut Vec<Vec<String>>,
) {
    if p.is_empty() && x.is_empty() {
        out.push(r.into_iter().collect());
        return;
    }
    let candidates = p.clone().into_iter().collect::<Vec<_>>();
    for v in candidates {
        let neighbors = adj.get(&v).cloned().unwrap_or_default();
        let mut next_r = r.clone();
        next_r.insert(v.clone());
        let next_p = p.intersection(&neighbors).cloned().collect();
        let next_x = x.intersection(&neighbors).cloned().collect();
        bron_kerbosch(next_r, next_p, next_x, adj, out);
        p.remove(&v);
        x.insert(v);
    }
}

fn ordered_pair(a: &str, b: &str) -> (String, String) {
    if a <= b {
        (a.to_string(), b.to_string())
    } else {
        (b.to_string(), a.to_string())
    }
}

fn link_is_lan(link: &ClusterLinkInfo) -> bool {
    link.privacy == "lan" || link.transport.starts_with("lan-")
}

fn best_state<'a>(left: &'a str, right: &'a str) -> &'a str {
    match (left, right) {
        ("healthy", _) | (_, "healthy") => "healthy",
        ("degraded", _) | (_, "degraded") => "degraded",
        _ => left,
    }
}

fn node_display_name_counts(nodes: &[ClusterNodeInfo]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for node in nodes {
        let name = node.display_name.trim();
        if !name.is_empty() {
            *counts.entry(name.to_string()).or_insert(0) += 1;
        }
    }
    counts
}

fn node_graph_label(node: &ClusterNodeInfo, display_counts: &BTreeMap<String, usize>) -> String {
    let name = node.display_name.trim();
    if name.is_empty() {
        short(&node.node_id, 11)
    } else if display_counts.get(name).copied().unwrap_or_default() > 1 {
        let suffix = node_id_suffix(&node.node_id, 6);
        let base_max = 17usize.saturating_sub(suffix.chars().count());
        format!("{} {suffix}", short(name, base_max.max(6)))
    } else {
        short(name, 18)
    }
}

fn node_graph_detail(node: &ClusterNodeInfo) -> String {
    let platform = platform_summary(node.platform.as_deref());
    if platform == "-" {
        node.availability.clone()
    } else {
        short(&format!("{} / {platform}", node.availability), 24)
    }
}

fn node_heading(node: &ClusterNodeInfo) -> String {
    let name = node.display_name.trim();
    if name.is_empty() {
        short(&node.node_id, 18)
    } else {
        let suffix = node_id_suffix(&node.node_id, 6);
        format!("{} {suffix}", short(name, 24))
    }
}

fn node_id_suffix(node_id: &str, count: usize) -> String {
    let len = node_id.chars().count();
    node_id
        .chars()
        .skip(len.saturating_sub(count))
        .collect::<String>()
}

fn platform_summary(platform: Option<&str>) -> String {
    let Some(platform) = platform.map(str::trim).filter(|value| !value.is_empty()) else {
        return "-".to_string();
    };
    platform
        .replace("x86_64", "x64")
        .replace("aarch64", "arm64")
}

fn short(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_string();
    }
    let head = max.saturating_sub(1);
    let trimmed = value.chars().take(head).collect::<String>();
    format!("{trimmed}...")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Backend;
    use crate::state::AppState;
    use crate::theme::{AppTextScale, AppThemeMode, resolve_theme, resolve_theme_with_preferences};

    fn node(id: &str) -> ClusterNodeInfo {
        ClusterNodeInfo {
            node_id: id.to_string(),
            display_name: id.to_string(),
            process: "daemon-main".to_string(),
            node_role: "workstation".to_string(),
            member_role: "voter".to_string(),
            member_state: "voter".to_string(),
            availability: "online".to_string(),
            daemon_version: "test".to_string(),
            commit: None,
            platform: None,
            pid: None,
            address: None,
            bind_address: None,
            rollout: None,
            observed_at: "now".to_string(),
            public_reachable: false,
            endpoints: Vec::new(),
        }
    }

    #[test]
    fn vertex_text_lines_follow_extra_large_theme_metrics_without_overlap() {
        let theme = resolve_theme_with_preferences(AppThemeMode::Dark, AppTextScale::ExtraLarge);
        let pal = Palette::from_theme(&theme, true);
        let label = pal.text_style(&theme, 12.0, FontWeight::MEDIUM, pal.text);
        let detail = pal.text_style(&theme, 11.0, FontWeight::NORMAL, pal.text_faint);
        let center = Point::new(120.0, 90.0);
        let layout = vertex_text_layout(center, NODE_R, label.line_height, detail.line_height);
        let label_top = layout.label_center_y - label.line_height * 0.5;
        let label_bottom = layout.label_center_y + label.line_height * 0.5;
        let detail_top = layout.detail_center_y - detail.line_height * 0.5;

        assert_eq!(label_top, center.y + NODE_R + VERTEX_TEXT_TOP_GAP);
        assert!(
            detail_top >= label_bottom + VERTEX_TEXT_LINE_GAP,
            "resolved label and detail line boxes must stay separated"
        );
        assert!(
            layout.detail_center_y > center.y + NODE_R + 40.0,
            "large text should expand the authored canvas spacing"
        );
    }

    fn link(a: &str, b: &str, privacy: &str, address: &str) -> ClusterLinkInfo {
        ClusterLinkInfo {
            source_node_id: a.to_string(),
            target_node_id: b.to_string(),
            endpoint_id: format!("endpoint-{b}"),
            transport: if privacy == "lan" {
                "lan-tcp".to_string()
            } else {
                "iroh".to_string()
            },
            address: address.to_string(),
            privacy: privacy.to_string(),
            state: "healthy".to_string(),
            latency_ms: Some(1),
            consecutive_failures: 0,
            public_ip: privacy == "internet-direct",
        }
    }

    fn endpoint(address: &str, reachable: bool) -> crate::backend::cluster::ClusterEndpointInfo {
        crate::backend::cluster::ClusterEndpointInfo {
            endpoint_id: "endpoint-a".to_string(),
            transport: "iroh".to_string(),
            address: address.to_string(),
            source: "member".to_string(),
            privacy: "internet".to_string(),
            priority: 10,
            latency_ms: Some(42),
            public_ip: true,
            reachable,
        }
    }

    #[test]
    fn selected_copy_details_preserves_full_id_address_and_endpoints() {
        let mut selected = node("node-01jz7x9m4k2p8q6r5s3t1v0w9y");
        selected.address = Some("https://10.20.30.40:21353/control".to_string());
        selected.endpoints = vec![endpoint("iroh://full-endpoint-address", true)];
        let snapshot = ClusterSnapshot {
            nodes: vec![selected.clone()],
            ..ClusterSnapshot::default()
        };

        let copied =
            selected_copy_details(&snapshot, &ClusterSelection::Node(selected.node_id.clone()))
                .expect("copyable node details");

        assert!(copied.contains(&selected.node_id));
        assert!(copied.contains("https://10.20.30.40:21353/control"));
        assert!(copied.contains("iroh://full-endpoint-address"));
        assert!(!copied.contains("..."));
    }

    #[test]
    fn keyboard_and_semantic_identity_follow_node_id_across_refresh_reordering() {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        let view = ClusterView::new(shared);
        let before = build_topology(&ClusterSnapshot {
            nodes: vec![node("node-a"), node("node-b")],
            ..ClusterSnapshot::default()
        });
        let after = build_topology(&ClusterSnapshot {
            nodes: vec![node("node-b"), node("node-c")],
            ..ClusterSnapshot::default()
        });
        *view.keyboard_focus.borrow_mut() = Some(ClusterSelection::Node("node-b".to_string()));

        assert_eq!(view.focused_vertex_index(&before), Some(1));
        assert_eq!(view.focused_vertex_index(&after), Some(0));
        let before_node = before
            .vertices
            .iter()
            .find(|vertex| vertex.id == "node-b")
            .expect("node-b before refresh");
        let after_node = after
            .vertices
            .iter()
            .find(|vertex| vertex.id == "node-b")
            .expect("node-b after refresh");
        let parent = sui::WidgetId::new(77);
        assert_eq!(
            cluster_vertex_widget_id(parent, before_node),
            cluster_vertex_widget_id(parent, after_node),
            "semantic identity must not depend on the row index"
        );
    }

    #[test]
    fn topology_collapses_three_node_lan_clique() {
        let snapshot = ClusterSnapshot {
            nodes: vec![node("node-a"), node("node-b"), node("node-c")],
            links: vec![
                link("node-a", "node-b", "lan", "192.168.1.2:21353"),
                link("node-b", "node-c", "lan", "192.168.1.3:21353"),
                link("node-a", "node-c", "lan", "192.168.1.4:21353"),
            ],
            ..ClusterSnapshot::default()
        };
        let topology = build_topology(&snapshot);

        assert_eq!(topology.lan_segments.len(), 1);
        assert_eq!(
            topology
                .edges
                .iter()
                .filter(|edge| edge.virtual_lan)
                .count(),
            3
        );
        assert!(
            topology
                .edges
                .iter()
                .all(|edge| edge.virtual_lan || edge.public_ip)
        );
    }

    #[test]
    fn rendered_cluster_nodes_are_semantic_and_keyboard_selectable() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        shared.cluster_mut().snapshot = ClusterSnapshot {
            nodes: vec![node("node-a"), node("node-b")],
            ..ClusterSnapshot::default()
        };
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Cluster keyboard navigation")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        app.run_until_idle()?;
        assert_eq!(
            window.get_by_role(SemanticsRole::ListItem).count()?,
            2,
            "each topology vertex should be exposed as a semantic row"
        );
        let topology = window
            .get_by_role(SemanticsRole::List)
            .with_name("Cluster nodes and LAN segments");
        topology.press("ArrowDown")?;
        topology.press("Enter")?;
        app.run_until_idle()?;

        assert_eq!(
            shared.cluster().selected,
            Some(ClusterSelection::Node("node-b".to_string()))
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Show diagnostics")
                .count()?,
            1,
            "keyboard selection should rebuild the stock inspector actions"
        );
        Ok(())
    }

    #[test]
    fn matching_subnet_without_observed_clique_is_not_collapsed() {
        let snapshot = ClusterSnapshot {
            nodes: vec![node("node-a"), node("node-b"), node("node-c")],
            links: vec![
                link("node-a", "node-b", "lan", "192.168.1.2:21353"),
                link("node-b", "node-c", "lan", "192.168.1.3:21353"),
            ],
            ..ClusterSnapshot::default()
        };
        let topology = build_topology(&snapshot);

        assert!(topology.lan_segments.is_empty());
        assert_eq!(topology.edges.len(), 2);
    }

    #[test]
    fn overview_lines_surface_cluster_inspector_summary() {
        let mut public = node("node-public");
        public.public_reachable = true;
        let snapshot = ClusterSnapshot {
            cluster_id: Some("cluster-test-123456789".to_string()),
            local_node_id: Some("node-public".to_string()),
            source: "daemon 127.0.0.1:21353".to_string(),
            nodes: vec![public, node("node-b")],
            links: vec![link(
                "node-public",
                "node-b",
                "internet-direct",
                "203.0.113.10:21353",
            )],
            ..ClusterSnapshot::default()
        };
        let topology = build_topology(&snapshot);
        let lines = overview_lines(&snapshot, &topology, false, false);

        assert_eq!(
            cluster_header_summary(&snapshot, &topology),
            "2 nodes · 2/2 online · LAN 0 segments · public 1/2 · daemon connected"
        );
        assert_eq!(lines[0], "Cluster overview");
        assert!(lines.iter().any(|line| line == "health: 2 / 2 online"));
        assert!(lines.iter().any(|line| line == "public route: 1 / 2 nodes"));
        assert!(
            lines
                .iter()
                .any(|line| line == "mode: public and private routing")
        );
        assert!(lines.iter().any(|line| line == "LAN: 0 segments"));
        assert!(
            lines
                .iter()
                .any(|line| line == "connection: daemon connected")
        );
        assert!(
            !lines.iter().any(|line| line.starts_with("topology:")),
            "direct topology edge counts belong behind diagnostics"
        );
        assert!(
            lines
                .iter()
                .any(|line| line == "hint: Show diagnostics for source and cluster identifiers.")
        );
        assert!(lines.iter().any(|line| line.starts_with("hint: Select")));
        assert!(
            !lines.iter().any(|line| line.contains("127.0.0.1")),
            "raw daemon source belongs in diagnostics"
        );
        assert!(
            !lines.iter().any(|line| line.contains("cluster-test")),
            "cluster identifiers belong in diagnostics"
        );

        let open = overview_lines(&snapshot, &topology, true, false);
        assert!(open.iter().any(|line| line == "topology: 1 direct edges"));
        assert!(
            open.iter()
                .any(|line| line == "diagnostics: daemon 127.0.0.1:21353")
        );
        assert!(
            open.iter()
                .any(|line| line == "cluster: cluster-test-123456789")
        );
        assert!(open.iter().any(|line| line == "local: node-public"));
    }

    #[test]
    fn overview_private_cluster_explains_lan_only_routing() {
        let snapshot = ClusterSnapshot {
            source: "daemon 127.0.0.1:21353".to_string(),
            nodes: vec![node("node-a"), node("node-b"), node("node-c")],
            ..ClusterSnapshot::default()
        };
        let topology = build_topology(&snapshot);
        let lines = overview_lines(&snapshot, &topology, false, false);

        assert!(lines.iter().any(|line| line == "health: 3 / 3 online"));
        assert!(lines.iter().any(|line| line == "public route: 0 / 3 nodes"));
        assert!(
            lines
                .iter()
                .any(|line| line == "mode: private/LAN; public ingress not required")
        );
        assert!(!lines.iter().any(|line| line.contains("attention:")));
    }

    #[test]
    fn overview_warning_keeps_raw_detail_behind_diagnostics() {
        let mut offline = node("node-b");
        offline.availability = "offline".to_string();
        let snapshot = ClusterSnapshot {
            source: "daemon 127.0.0.1:21353".to_string(),
            nodes: vec![node("node-a"), offline],
            warnings: vec!["daemon health unavailable: network error".to_string()],
            ..ClusterSnapshot::default()
        };
        let topology = build_topology(&snapshot);

        let closed = overview_lines(&snapshot, &topology, false, false);
        assert!(
            closed
                .iter()
                .any(|line| line == "attention: 1 node offline")
        );
        assert!(closed.iter().any(|line| line
            == "hint: Show diagnostics for warning detail, source, and cluster identifiers."));
        assert!(
            !closed.iter().any(|line| line.contains("network error")),
            "raw warning belongs in diagnostics"
        );

        let open = overview_lines(&snapshot, &topology, true, false);
        assert!(
            open.iter()
                .any(|line| line == "warning detail: daemon health unavailable: network error")
        );
    }

    #[test]
    fn overview_keeps_probe_health_counts_when_daemon_aggregation_warns() {
        let mut offline = node("node-c");
        offline.availability = "offline".to_string();
        let snapshot = ClusterSnapshot {
            source: "daemon 127.0.0.1:21353".to_string(),
            nodes: vec![node("node-a"), node("node-b"), offline],
            warnings: vec![
                "daemon health aggregation unavailable: rpc failed; showing direct cluster probes"
                    .to_string(),
            ],
            ..ClusterSnapshot::default()
        };
        let topology = build_topology(&snapshot);

        let closed = overview_lines(&snapshot, &topology, false, false);
        assert!(
            closed
                .iter()
                .any(|line| line == "health: 2 / 3 online, 1 offline"),
            "overview should trust direct probe rows instead of reducing to local health"
        );
        assert!(
            closed
                .iter()
                .any(|line| line == "attention: 1 node offline")
        );
        assert!(
            !closed.iter().any(|line| line.contains("rpc failed")),
            "raw aggregation error belongs behind diagnostics"
        );

        let open = overview_lines(&snapshot, &topology, true, false);
        assert!(
            open.iter()
                .any(|line| line
                    .starts_with("warning detail: daemon health aggregation unavailable"))
        );
    }

    #[test]
    fn refreshing_overview_does_not_surface_stale_warning_as_attention() {
        let mut offline = node("node-c");
        offline.availability = "offline".to_string();
        let snapshot = ClusterSnapshot {
            source: "daemon 127.0.0.1:21353".to_string(),
            nodes: vec![node("node-a"), node("node-b"), offline],
            warnings: vec![
                "daemon health aggregation unavailable: rpc failed; showing direct cluster probes"
                    .to_string(),
            ],
            ..ClusterSnapshot::default()
        };
        let topology = build_topology(&snapshot);

        let closed = overview_lines(&snapshot, &topology, false, true);
        assert!(
            closed
                .iter()
                .any(|line| line == "health: 2 / 3 online, 1 offline"),
            "the stale snapshot counts remain visible while the refresh runs"
        );
        assert!(
            closed
                .iter()
                .any(|line| line == "status: refreshing current health")
        );
        assert!(
            !closed.iter().any(|line| line.starts_with("attention:")),
            "stale refresh snapshots should not read as confirmed current attention"
        );

        let open = overview_lines(&snapshot, &topology, true, true);
        assert!(
            open.iter().any(|line| line
                .starts_with("previous warning detail: daemon health aggregation unavailable")),
            "diagnostics should still expose the stale warning source"
        );
        assert!(
            !open.iter().any(|line| line.starts_with("warning detail:")),
            "refreshing warnings should be labeled as previous evidence"
        );
    }

    #[test]
    fn overview_direct_probe_fallback_is_diagnostic_only_when_all_nodes_online() {
        let snapshot = ClusterSnapshot {
            source: "daemon 127.0.0.1:21353".to_string(),
            nodes: vec![node("node-a"), node("node-b"), node("node-c")],
            warnings: vec![
                "daemon health aggregation unavailable: rpc failed; showing direct cluster probes"
                    .to_string(),
            ],
            ..ClusterSnapshot::default()
        };
        let topology = build_topology(&snapshot);

        let closed = overview_lines(&snapshot, &topology, false, false);
        assert!(closed.iter().any(|line| line == "health: 3 / 3 online"));
        assert!(
            !closed.iter().any(|line| line.starts_with("attention:")),
            "direct probe fallback should not surface as attention when every node is online"
        );
        assert!(
            cluster_actionable_warning(&snapshot).is_none(),
            "all-online direct-probe fallback should stay diagnostics-only"
        );

        let open = overview_lines(&snapshot, &topology, true, false);
        assert!(
            open.iter()
                .any(|line| line
                    .starts_with("warning detail: daemon health aggregation unavailable")),
            "technical fallback detail should remain available behind diagnostics"
        );
    }

    #[test]
    fn pump_refresh_applies_snapshot_and_clears_missing_selection() {
        let (tx, rx) = std::sync::mpsc::channel();
        let mut state = ClusterState {
            snapshot: ClusterSnapshot {
                nodes: vec![node("node-a"), node("node-b")],
                ..ClusterSnapshot::default()
            },
            selected: Some(ClusterSelection::Node("node-b".to_string())),
            diagnostics_open: true,
            refreshing: true,
            refresh_rx: Some(rx),
        };
        tx.send(ClusterSnapshot {
            nodes: vec![node("node-a")],
            ..ClusterSnapshot::default()
        })
        .expect("send refreshed snapshot");

        assert!(state.pump_refresh());
        assert!(!state.refreshing);
        assert_eq!(state.snapshot.nodes.len(), 1);
        assert_eq!(state.selected, None);
        assert!(!state.diagnostics_open);
    }

    #[test]
    fn cluster_state_starts_with_lightweight_placeholder() {
        let backend = Backend::local_fallback();
        let state = ClusterState::new(&backend);

        assert!(state.snapshot.nodes.is_empty());
        assert!(!state.refreshing);
        assert!(
            state
                .snapshot
                .warnings
                .iter()
                .any(|warning| warning.contains("loading in the background")),
            "startup cluster state should not synchronously fetch live health"
        );
    }

    #[test]
    fn overview_refresh_action_is_exposed() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut cluster = shared.cluster_mut();
            cluster.snapshot = ClusterSnapshot {
                source: "daemon 127.0.0.1:21353".to_string(),
                nodes: vec![node("node-a")],
                ..ClusterSnapshot::default()
            };
            cluster.selected = None;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Cluster refresh action")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Refresh")
                .with_description("Refresh cluster health and topology")
                .count()?,
            1
        );

        Ok(())
    }

    #[test]
    fn warning_overview_action_opens_connection_settings() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut cluster = shared.cluster_mut();
            cluster.snapshot = ClusterSnapshot {
                source: "daemon 127.0.0.1:21353".to_string(),
                nodes: vec![node("node-a")],
                warnings: vec!["daemon health unavailable: network error".to_string()],
                ..ClusterSnapshot::default()
            };
            cluster.selected = None;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Cluster warning action")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Connection settings")
            .with_description("Open connection settings for cluster warning")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        assert_eq!(
            shared.settings().selected,
            Some(SettingsSection::Connection)
        );

        Ok(())
    }

    #[test]
    fn refreshing_overview_hides_warning_recovery_action() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let mut offline = node("node-b");
        offline.availability = "offline".to_string();
        {
            let mut cluster = shared.cluster_mut();
            cluster.snapshot = ClusterSnapshot {
                source: "daemon 127.0.0.1:21353".to_string(),
                nodes: vec![node("node-a"), offline],
                warnings: vec!["daemon health unavailable: network error".to_string()],
                ..ClusterSnapshot::default()
            };
            cluster.selected = None;
            cluster.refreshing = true;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Cluster refreshing warning action")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Connection settings")
                .with_description("Open connection settings for cluster warning")
                .count()?,
            0
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Refreshing")
                .with_description("Refresh cluster health and topology")
                .count()?,
            1
        );

        Ok(())
    }

    #[test]
    fn selected_offline_node_exposes_recovery_actions() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        let mut offline = node("node-offline");
        offline.availability = "offline".to_string();
        offline.endpoints = vec![endpoint("127.0.0.1:21353", false)];
        {
            let mut cluster = shared.cluster_mut();
            cluster.snapshot = ClusterSnapshot {
                source: "daemon 127.0.0.1:21353".to_string(),
                nodes: vec![offline],
                ..ClusterSnapshot::default()
            };
            cluster.selected = Some(ClusterSelection::Node("node-offline".to_string()));
            cluster.diagnostics_open = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Cluster selected node actions")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Refresh")
                .with_description("Refresh cluster health and topology")
                .count()?,
            1
        );
        assert_eq!(
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Show diagnostics")
                .with_description("Show technical cluster source and identifier details")
                .count()?,
            1
        );

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Connection settings")
            .with_description("Open connection settings for selected cluster node")
            .click()?;

        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        assert_eq!(
            shared.settings().selected,
            Some(SettingsSection::Connection)
        );

        Ok(())
    }

    #[test]
    fn selected_node_connection_action_requires_attention() {
        let mut healthy = node("node-healthy");
        healthy.endpoints = vec![endpoint("127.0.0.1:21353", true)];
        let mut offline = node("node-offline");
        offline.availability = "offline".to_string();
        let snapshot = ClusterSnapshot {
            nodes: vec![healthy, offline],
            ..ClusterSnapshot::default()
        };

        assert!(!selected_node_needs_connection_settings(
            &snapshot,
            Some(&ClusterSelection::Node("node-healthy".to_string()))
        ));
        assert!(selected_node_needs_connection_settings(
            &snapshot,
            Some(&ClusterSelection::Node("node-offline".to_string()))
        ));
        assert!(!selected_node_needs_connection_settings(
            &snapshot,
            Some(&ClusterSelection::LanSegment("lan-segment-1".to_string()))
        ));
    }

    #[test]
    fn overview_diagnostics_toggle_reveals_technical_details() -> sui::Result<()> {
        let backend = Backend::local_fallback();
        let shared = Shared::new(AppState::ready(backend));
        {
            let mut cluster = shared.cluster_mut();
            cluster.snapshot = ClusterSnapshot {
                cluster_id: Some("cluster-test-123456789".to_string()),
                local_node_id: Some("node-a".to_string()),
                source: "daemon 127.0.0.1:21353".to_string(),
                nodes: vec![node("node-a")],
                ..ClusterSnapshot::default()
            };
            cluster.selected = None;
            cluster.diagnostics_open = false;
        }

        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Cluster overview diagnostics")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Show diagnostics")
            .with_description("Show technical cluster source and identifier details")
            .click()?;

        assert!(shared.cluster().diagnostics_open);

        Ok(())
    }

    #[test]
    fn unknown_nodes_use_visible_neutral_graph_fill() {
        let mut unknown = node("node-unknown");
        unknown.availability = "unknown".to_string();
        let snapshot = ClusterSnapshot {
            nodes: vec![unknown],
            ..ClusterSnapshot::default()
        };
        let topology = build_topology(&snapshot);
        let theme = resolve_theme(AppThemeMode::Light);
        let pal = Palette::from_theme(&theme, false);
        let fill = vertex_fill(&pal, &topology.vertices[0]);

        assert_eq!(fill, unknown_node_color(&pal));
        assert_ne!(fill, pal.field);
    }

    #[test]
    fn public_reachability_does_not_override_health_fill() {
        let mut offline = node("node-public-offline");
        offline.availability = "offline".to_string();
        offline.public_reachable = true;
        let snapshot = ClusterSnapshot {
            nodes: vec![offline],
            ..ClusterSnapshot::default()
        };
        let topology = build_topology(&snapshot);
        let theme = resolve_theme(AppThemeMode::Light);
        let pal = Palette::from_theme(&theme, false);
        let fill = vertex_fill(&pal, &topology.vertices[0]);

        assert_eq!(fill, pal.bad);
        assert_ne!(fill, public_node_color(&pal));
    }

    #[test]
    fn duplicate_display_names_are_disambiguated_in_graph_labels() {
        let mut left = node("01kr4w089vqgdxpb84t6bq1qxn");
        left.display_name = "sinomo-node".to_string();
        left.platform = Some("linux/aarch64".to_string());
        left.availability = "offline".to_string();
        let mut right = node("01kr3d54d45zkm0p0xgps8k7mt");
        right.display_name = "sinomo-node".to_string();
        right.platform = Some("linux/x86_64".to_string());
        let snapshot = ClusterSnapshot {
            nodes: vec![left, right],
            ..ClusterSnapshot::default()
        };

        let topology = build_topology(&snapshot);
        let labels = topology
            .vertices
            .iter()
            .map(|vertex| vertex.label.as_str())
            .collect::<Vec<_>>();
        let details = topology
            .vertices
            .iter()
            .map(|vertex| vertex.detail.as_str())
            .collect::<Vec<_>>();

        assert!(labels.contains(&"sinomo-node bq1qxn"));
        assert!(labels.contains(&"sinomo-node s8k7mt"));
        assert!(details.contains(&"offline / linux/arm64"));
        assert!(details.contains(&"online / linux/x64"));
    }

    #[test]
    fn node_detail_surfaces_short_identity_and_platform_before_diagnostics() {
        let mut node = node("01kr4w089vqgdxpb84t6bq1qxn");
        node.display_name = "sinomo-node".to_string();
        node.platform = Some("linux/aarch64".to_string());

        let closed = node_detail_lines(&node, false);

        assert_eq!(closed[0], "sinomo-node bq1qxn");
        assert!(closed.iter().any(|line| line == "platform: linux/arm64"));
    }

    #[test]
    fn node_detail_keeps_endpoint_addresses_behind_diagnostics() {
        let mut node = node("node-a");
        node.endpoints = vec![endpoint(
            "iroh://e6e7579b45088dc226274eadd8dec9782eb042c1c753ba5be249c96c1c8c0e2e?relay=https://use1-1.relay.no.iroh-canary.iroh.link/",
            true,
        )];

        let closed = node_detail_lines(&node, false);
        assert!(
            closed
                .iter()
                .any(|line| line == "endpoints: 1 / 1 reachable")
        );
        assert!(
            closed
                .iter()
                .any(|line| line == "route: LAN/local route only")
        );
        assert!(
            closed
                .iter()
                .any(|line| line == "next: LAN or local routing only")
        );
        assert!(
            !closed.iter().any(|line| line.contains("iroh://")),
            "raw endpoint should not be in primary node details"
        );

        let open = node_detail_lines(&node, true);
        assert!(
            open.iter()
                .any(|line| line == "diagnostics: endpoint addresses")
        );
        assert!(
            open.iter().any(|line| line.contains("iroh://")),
            "diagnostics should expose the endpoint address"
        );
    }

    #[test]
    fn node_detail_guidance_prioritizes_offline_and_unreachable_nodes() {
        let mut offline = node("node-offline");
        offline.availability = "offline".to_string();
        offline.endpoints = vec![endpoint("127.0.0.1:21353", false)];
        let offline_lines = node_detail_lines(&offline, false);
        assert!(
            offline_lines
                .iter()
                .any(|line| line == "route: node offline")
        );
        assert!(
            offline_lines
                .iter()
                .any(|line| { line == "next: start daemon on that node or check its route" })
        );

        let mut unreachable = node("node-unreachable");
        unreachable.endpoints = vec![endpoint("127.0.0.1:21353", false)];
        let unreachable_lines = node_detail_lines(&unreachable, false);
        assert!(
            unreachable_lines
                .iter()
                .any(|line| line == "route: endpoints unreachable")
        );
        assert!(
            unreachable_lines
                .iter()
                .any(|line| line == "next: check endpoint reachability in diagnostics")
        );

        let mut public = node("node-public");
        public.public_reachable = true;
        public.endpoints = vec![endpoint("203.0.113.10:21353", true)];
        let public_lines = node_detail_lines(&public, false);
        assert!(
            public_lines
                .iter()
                .any(|line| line == "route: public route reachable")
        );
        assert!(
            public_lines
                .iter()
                .any(|line| line == "next: public route is reachable")
        );
    }
}
