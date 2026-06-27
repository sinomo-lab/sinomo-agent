//! "Cluster" tab - network-topology graph of known cluster nodes.

use std::collections::{BTreeMap, BTreeSet};

use sui::{
    ArrangeCtx, Color, Constraints, Event, EventCtx, FontWeight, IconGlyph, MeasureCtx, PaintCtx,
    Path, PathBuilder, Point, PointerButton, PointerEventKind, Rect, SemanticsAction, SemanticsCtx,
    SemanticsNode, SemanticsRole, Size, StrokeStyle, Widget, WindowEvent, draw_glyph,
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
const DETAIL_BUTTON_H: f32 = 28.0;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClusterSelection {
    Node(String),
    LanSegment(String),
}

pub struct ClusterState {
    pub snapshot: ClusterSnapshot,
    pub selected: Option<ClusterSelection>,
    pub diagnostics_open: bool,
}

impl ClusterState {
    pub fn new(backend: &Backend) -> Self {
        Self {
            snapshot: backend.cluster_snapshot(),
            selected: None,
            diagnostics_open: false,
        }
    }
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum GraphVertexKind {
    Node,
    LanSegment,
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
    bounds: Rect,
}

impl ClusterView {
    fn new(shared: Shared) -> Self {
        Self {
            shared,
            bounds: Rect::ZERO,
        }
    }

    fn palette(&self) -> (sui::DefaultTheme, Palette) {
        let theme = self.shared.theme();
        let pal = Palette::from_theme(&theme, widgets::theme_is_dark(&self.shared));
        (theme, pal)
    }

    fn open_connection_settings(&self) {
        let mut app = self.shared.borrow_mut();
        app.active_tab = Tab::Settings;
        if let Some(ready) = app.ready.as_mut() {
            ready.settings.open(SettingsSection::Connection);
        }
    }
}

impl Widget for ClusterView {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        match event {
            Event::Pointer(pointer)
                if pointer.kind == PointerEventKind::Up
                    && pointer.button == Some(PointerButton::Primary)
                    && ctx.bounds().contains(pointer.position) =>
            {
                let overview_warning_action = {
                    let cluster = self.shared.cluster();
                    cluster.selected.is_none() && !cluster.snapshot.warnings.is_empty()
                };
                if overview_warning_action
                    && cluster_warning_action_rect(ctx.bounds()).contains(pointer.position)
                {
                    self.open_connection_settings();
                    ctx.request_paint();
                    ctx.set_handled();
                    return;
                }

                let snapshot = self.shared.cluster().snapshot.clone();
                let topology = build_topology(&snapshot);
                let selected = self.shared.cluster().selected.clone();
                if overview_detail_toggle_visible(&snapshot, selected.as_ref())
                    && overview_detail_toggle_rect(ctx.bounds(), overview_warning_action)
                        .contains(pointer.position)
                {
                    let open = self.shared.cluster().diagnostics_open;
                    self.shared.cluster_mut().diagnostics_open = !open;
                    ctx.request_paint();
                    ctx.set_handled();
                    return;
                }
                if selected.is_some() && detail_toggle_rect(ctx.bounds()).contains(pointer.position)
                {
                    let open = self.shared.cluster().diagnostics_open;
                    self.shared.cluster_mut().diagnostics_open = !open;
                    ctx.request_paint();
                    ctx.set_handled();
                    return;
                }
                let graph = graph_rect(ctx.bounds());
                let layout = layout_vertices(&topology, graph);
                let selection = hit_test(&layout, pointer.position);
                let mut cluster = self.shared.cluster_mut();
                if cluster.selected != selection {
                    cluster.diagnostics_open = false;
                }
                cluster.selected = selection;
                ctx.request_paint();
                ctx.set_handled();
            }
            Event::Window(WindowEvent::RedrawRequested) => {
                ctx.request_paint();
            }
            _ => {}
        }
    }

    fn measure(&mut self, _ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        constraints.max
    }

    fn arrange(&mut self, _ctx: &mut ArrangeCtx, bounds: Rect) {
        self.bounds = bounds;
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let (theme, pal) = self.palette();
        let snapshot = self.shared.cluster().snapshot.clone();
        let topology = build_topology(&snapshot);
        let bounds = ctx.bounds();
        let graph = graph_rect(bounds);
        let layout = layout_vertices(&topology, graph);
        let selected = self.shared.cluster().selected.clone();
        let diagnostics_open = self.shared.cluster().diagnostics_open;

        ctx.fill_bounds(pal.window);
        paint_header(ctx, &theme, &pal, bounds, &snapshot, &topology);

        if topology.vertices.is_empty() {
            paint_empty(ctx, &theme, &pal, graph, &snapshot);
            return;
        }

        paint_edges(ctx, &pal, &topology, &layout);
        paint_vertices(ctx, &theme, &pal, &layout, &selected);
        paint_legend(ctx, &theme, &pal, bounds);
        paint_detail_panel(
            ctx,
            &theme,
            &pal,
            bounds,
            &snapshot,
            &topology,
            selected.as_ref(),
            diagnostics_open,
        );
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let mut node = SemanticsNode::new(
            ctx.widget_id(),
            SemanticsRole::GenericContainer,
            ctx.bounds(),
        );
        node.name = Some("Cluster topology. Select a node or LAN segment for details.".to_string());
        ctx.push(node);

        let (show_warning_action, show_overview_details, diagnostics_open) = {
            let cluster = self.shared.cluster();
            (
                cluster.selected.is_none() && !cluster.snapshot.warnings.is_empty(),
                overview_detail_toggle_visible(&cluster.snapshot, cluster.selected.as_ref()),
                cluster.diagnostics_open,
            )
        };
        if show_warning_action {
            let mut action = SemanticsNode::new(
                cluster_warning_action_widget_id(ctx.widget_id()),
                SemanticsRole::Button,
                cluster_warning_action_rect(ctx.bounds()),
            );
            action.parent = Some(ctx.widget_id());
            action.name = Some("Connection settings".to_string());
            action.description = Some("Open connection settings for cluster warning".to_string());
            action.actions = vec![SemanticsAction::Activate];
            ctx.push(action);
        }
        if show_overview_details {
            let mut action = SemanticsNode::new(
                cluster_detail_toggle_widget_id(ctx.widget_id()),
                SemanticsRole::Button,
                overview_detail_toggle_rect(ctx.bounds(), show_warning_action),
            );
            action.parent = Some(ctx.widget_id());
            action.name = Some(if diagnostics_open {
                "Hide diagnostics".to_string()
            } else {
                "Show diagnostics".to_string()
            });
            action.description =
                Some("Show technical cluster source and identifier details".to_string());
            action.actions = vec![SemanticsAction::Activate];
            ctx.push(action);
        }
    }
}

fn paint_header(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    bounds: Rect,
    snapshot: &ClusterSnapshot,
    topology: &Topology,
) {
    let title = pal.text_style(theme, 21.0, FontWeight::SEMIBOLD, pal.text);
    widgets::draw_text_in(
        ctx,
        Rect::new(bounds.x() + PAD, bounds.y() + 16.0, 280.0, 28.0),
        "Cluster",
        title,
    );

    let links = topology
        .edges
        .iter()
        .filter(|edge| !edge.virtual_lan)
        .count();
    let subtitle = format!(
        "{} nodes · {} topology edges · {} LAN segments · {}",
        snapshot.nodes.len(),
        links,
        topology.lan_segments.len(),
        source_summary(&snapshot.source)
    );
    let style = pal.text_style(theme, 13.0, FontWeight::NORMAL, pal.text_muted);
    widgets::draw_text_in(
        ctx,
        Rect::new(
            bounds.x() + PAD,
            bounds.y() + 44.0,
            bounds.width() - PAD * 2.0,
            22.0,
        ),
        &subtitle,
        style,
    );
    widgets::hairline_bottom(
        ctx,
        Rect::new(bounds.x(), bounds.y(), bounds.width(), HEADER_H),
        pal.border,
    );
}

fn paint_empty(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    rect: Rect,
    snapshot: &ClusterSnapshot,
) {
    let cx = rect.x() + rect.width() / 2.0;
    let cy = rect.y() + rect.height() / 2.0;
    let title = if snapshot.warnings.is_empty() {
        "No cluster nodes"
    } else {
        "Cluster metadata unavailable"
    };
    let detail = snapshot
        .warnings
        .first()
        .map(String::as_str)
        .unwrap_or("Initialize or join a cluster to populate this view");
    widgets::draw_centered(
        ctx,
        title,
        pal.text_style(theme, 18.0, FontWeight::SEMIBOLD, pal.text_muted),
        cx,
        cy - 12.0,
    );
    widgets::draw_centered(
        ctx,
        detail,
        pal.text_style(theme, 13.0, FontWeight::NORMAL, pal.text_faint),
        cx,
        cy + 18.0,
    );
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
            pal.accent
        } else if edge.state == "degraded" {
            pal.warn
        } else {
            pal.border_strong.with_alpha(0.76)
        };
        let width = if edge.virtual_lan { 1.6 } else { 2.0 };
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
            StrokeStyle::new(if selected_here { 2.2 } else { 1.1 }),
        );

        let label_style = pal.text_style(theme, 12.0, FontWeight::MEDIUM, pal.text);
        let label_y = vertex.center.y + vertex.radius + 14.0;
        widgets::draw_centered(
            ctx,
            &vertex.vertex.label,
            label_style,
            vertex.center.x,
            label_y,
        );
        let detail_style = pal.text_style(theme, 10.5, FontWeight::NORMAL, pal.text_faint);
        widgets::draw_centered(
            ctx,
            &vertex.vertex.detail,
            detail_style,
            vertex.center.x,
            label_y + 15.0,
        );
    }
}

fn paint_legend(ctx: &mut PaintCtx, theme: &sui::DefaultTheme, pal: &Palette, bounds: Rect) {
    let x = bounds.x() + PAD;
    let y = bounds.max_y() - 42.0;
    let style = pal.text_style(theme, 12.0, FontWeight::NORMAL, pal.text_faint);
    let items = [
        (pal.good, "online"),
        (public_node_color(), "public"),
        (pal.bad, "offline"),
        (unknown_node_color(pal), "unknown"),
        (lan_segment_color(), "LAN"),
    ];
    let mut cursor = x;
    for (color, label) in items {
        ctx.fill(Path::circle(Point::new(cursor + 6.0, y + 10.0), 5.0), color);
        widgets::draw_text_in(
            ctx,
            Rect::new(cursor + 16.0, y, 72.0, 22.0),
            label,
            style.clone(),
        );
        cursor += 92.0;
    }
}

fn vertex_fill(pal: &Palette, vertex: &GraphVertex) -> Color {
    match vertex.kind {
        GraphVertexKind::LanSegment => lan_segment_color(),
        GraphVertexKind::Node if vertex.public_reachable => public_node_color(),
        GraphVertexKind::Node => match vertex.availability.as_str() {
            "online" => pal.good,
            "offline" => pal.bad,
            "degraded" => pal.warn,
            _ => unknown_node_color(pal),
        },
    }
}

fn public_node_color() -> Color {
    Color::srgba(0.14, 0.52, 0.92, 1.0)
}

fn lan_segment_color() -> Color {
    Color::srgba(0.58, 0.41, 0.10, 1.0)
}

fn unknown_node_color(pal: &Palette) -> Color {
    if pal.dark {
        Color::srgba(0.28, 0.34, 0.42, 1.0)
    } else {
        Color::srgba(0.78, 0.84, 0.91, 1.0)
    }
}

fn paint_detail_panel(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    bounds: Rect,
    snapshot: &ClusterSnapshot,
    topology: &Topology,
    selection: Option<&ClusterSelection>,
    diagnostics_open: bool,
) {
    let panel_w = PANEL_W.min((bounds.width() - PAD * 2.0).max(220.0));
    let x = bounds.max_x() - panel_w - PAD;
    let y = bounds.y() + HEADER_H + 18.0;
    let panel = Rect::new(x, y, panel_w, 312.0);
    widgets::round_panel(ctx, panel, pal.panel.with_alpha(0.96), pal.border, 8.0);

    let lines = selection
        .map(|selection| detail_lines(snapshot, topology, selection, diagnostics_open))
        .unwrap_or_else(|| overview_lines(snapshot, topology, diagnostics_open));
    let show_warning_action = selection.is_none() && !snapshot.warnings.is_empty();
    let show_overview_details = overview_detail_toggle_visible(snapshot, selection);
    let title_style = pal.text_style(theme, 15.0, FontWeight::SEMIBOLD, pal.text);
    let label_style = pal.text_style(theme, 12.0, FontWeight::MEDIUM, pal.text_muted);
    let value_style = pal.text_style(theme, 12.0, FontWeight::NORMAL, pal.text);
    let hint_style = pal.text_style(theme, 11.5, FontWeight::NORMAL, pal.text_faint);
    let mut cy = panel.y() + PANEL_PAD;
    if let Some((title, rest)) = lines.split_first() {
        widgets::draw_text_in(
            ctx,
            Rect::new(
                panel.x() + PANEL_PAD,
                cy,
                panel.width() - PANEL_PAD * 2.0,
                22.0,
            ),
            title,
            title_style,
        );
        cy += 32.0;
        let content_bottom = if selection.is_some() {
            detail_toggle_rect(bounds).y() - 8.0
        } else if show_warning_action || show_overview_details {
            overview_action_row_y(bounds) - 8.0
        } else {
            panel.max_y() - PANEL_PAD
        };
        for line in rest {
            if cy + 18.0 > content_bottom {
                break;
            }
            if let Some(hint) = line.strip_prefix("hint:") {
                widgets::draw_text_in(
                    ctx,
                    Rect::new(
                        panel.x() + PANEL_PAD,
                        cy,
                        panel.width() - PANEL_PAD * 2.0,
                        18.0,
                    ),
                    hint.trim(),
                    hint_style.clone(),
                );
                cy += 22.0;
                continue;
            }
            let (label, value) = line
                .split_once(':')
                .map(|(label, value)| (label, value.trim()))
                .unwrap_or(("", line.as_str()));
            if !label.is_empty() {
                widgets::draw_text_in(
                    ctx,
                    Rect::new(panel.x() + PANEL_PAD, cy, 104.0, 18.0),
                    label,
                    label_style.clone(),
                );
                widgets::draw_text_in(
                    ctx,
                    Rect::new(
                        panel.x() + PANEL_PAD + 108.0,
                        cy,
                        panel.width() - PANEL_PAD * 2.0 - 108.0,
                        18.0,
                    ),
                    value,
                    value_style.clone(),
                );
            } else {
                widgets::draw_text_in(
                    ctx,
                    Rect::new(
                        panel.x() + PANEL_PAD,
                        cy,
                        panel.width() - PANEL_PAD * 2.0,
                        18.0,
                    ),
                    line,
                    value_style.clone(),
                );
            }
            cy += 21.0;
        }
    }
    if selection.is_some() {
        paint_detail_toggle(
            ctx,
            theme,
            pal,
            detail_toggle_rect(bounds),
            diagnostics_open,
        );
    } else {
        if show_warning_action {
            paint_cluster_warning_action(ctx, theme, pal, cluster_warning_action_rect(bounds));
        }
        if show_overview_details {
            paint_detail_toggle(
                ctx,
                theme,
                pal,
                overview_detail_toggle_rect(bounds, show_warning_action),
                diagnostics_open,
            );
        }
    }
}

fn overview_lines(
    snapshot: &ClusterSnapshot,
    topology: &Topology,
    diagnostics_open: bool,
) -> Vec<String> {
    let online = snapshot
        .nodes
        .iter()
        .filter(|node| node.availability == "online")
        .count();
    let public = snapshot
        .nodes
        .iter()
        .filter(|node| node.public_reachable)
        .count();
    let direct_edges = topology
        .edges
        .iter()
        .filter(|edge| !edge.virtual_lan)
        .count();
    let mut lines = vec![
        "Cluster overview".to_string(),
        format!("source: {}", source_summary(&snapshot.source)),
        format!("nodes: {online} / {} online", snapshot.nodes.len()),
        format!("public: {public} reachable"),
        format!("topology: {direct_edges} edges"),
        format!("LAN: {} segments", topology.lan_segments.len()),
    ];
    if let Some(warning) = snapshot.warnings.first() {
        lines.push(format!("warning: {}", short(warning, 42)));
    }
    if diagnostics_open {
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
        lines.push("hint: Show diagnostics for source and cluster identifiers.".to_string());
    }
    lines.push(
        "hint: Select a node or LAN segment for process and reachability details.".to_string(),
    );
    lines
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
        if node.display_name.is_empty() {
            short(&node.node_id, 18)
        } else {
            node.display_name.clone()
        },
        format!("node: {}", short(&node.node_id, 26)),
        format!("health: {}", node.availability),
        format!("process: {}", node.process),
        format!("role: {} {}", node.member_role, node.member_state),
        format!("address: {}", node.address.as_deref().unwrap_or("-")),
        format!(
            "endpoints: {} / {} reachable",
            reachable_endpoints,
            node.endpoints.len()
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
        lines.push(format!(
            "platform: {}",
            node.platform.as_deref().unwrap_or("-")
        ));
        lines.push(format!("version: {}", node.daemon_version));
    } else if !node.endpoints.is_empty() {
        lines.push("hint: Show diagnostics for endpoint addresses.".to_string());
    }
    lines
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

fn overview_detail_toggle_visible(
    _snapshot: &ClusterSnapshot,
    selection: Option<&ClusterSelection>,
) -> bool {
    selection.is_none()
}

fn detail_panel_rect(bounds: Rect) -> Rect {
    let panel_w = PANEL_W.min((bounds.width() - PAD * 2.0).max(220.0));
    Rect::new(
        bounds.max_x() - panel_w - PAD,
        bounds.y() + HEADER_H + 18.0,
        panel_w,
        312.0,
    )
}

fn detail_toggle_rect(bounds: Rect) -> Rect {
    let panel = detail_panel_rect(bounds);
    Rect::new(
        panel.x() + PANEL_PAD,
        panel.max_y() - PANEL_PAD - DETAIL_BUTTON_H,
        144.0,
        DETAIL_BUTTON_H,
    )
}

fn cluster_warning_action_rect(bounds: Rect) -> Rect {
    let panel = detail_panel_rect(bounds);
    Rect::new(
        panel.x() + PANEL_PAD,
        overview_action_row_y(bounds),
        172.0,
        DETAIL_BUTTON_H,
    )
}

fn overview_detail_toggle_rect(bounds: Rect, has_warning_action: bool) -> Rect {
    let panel = detail_panel_rect(bounds);
    let width = 144.0;
    let x = if has_warning_action {
        (cluster_warning_action_rect(bounds).max_x() + 8.0).min(panel.max_x() - PANEL_PAD - width)
    } else {
        panel.x() + PANEL_PAD
    };
    Rect::new(x, overview_action_row_y(bounds), width, DETAIL_BUTTON_H)
}

fn overview_action_row_y(bounds: Rect) -> f32 {
    let panel = detail_panel_rect(bounds);
    panel.max_y() - PANEL_PAD - DETAIL_BUTTON_H
}

fn cluster_warning_action_widget_id(parent: sui::WidgetId) -> sui::WidgetId {
    sui::WidgetId::new(parent.get().wrapping_mul(41).wrapping_add(9_301))
}

fn cluster_detail_toggle_widget_id(parent: sui::WidgetId) -> sui::WidgetId {
    sui::WidgetId::new(parent.get().wrapping_mul(41).wrapping_add(9_307))
}

fn paint_detail_toggle(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    rect: Rect,
    diagnostics_open: bool,
) {
    widgets::round_panel(ctx, rect, pal.field, pal.border, 7.0);
    widgets::draw_text_in(
        ctx,
        Rect::new(rect.x() + 10.0, rect.y() + 5.0, rect.width() - 20.0, 18.0),
        if diagnostics_open {
            "Hide diagnostics"
        } else {
            "Show diagnostics"
        },
        pal.text_style(theme, 11.5, FontWeight::MEDIUM, pal.text),
    );
}

fn paint_cluster_warning_action(
    ctx: &mut PaintCtx,
    theme: &sui::DefaultTheme,
    pal: &Palette,
    rect: Rect,
) {
    widgets::round_panel(ctx, rect, pal.accent, pal.accent, 7.0);
    draw_glyph(
        ctx,
        IconGlyph::Alert,
        Rect::new(rect.x() + 10.0, rect.y() + 7.0, 14.0, 14.0),
        pal.on_accent,
    );
    widgets::draw_text_in(
        ctx,
        Rect::new(rect.x() + 30.0, rect.y() + 5.0, rect.width() - 40.0, 18.0),
        "Connection settings",
        pal.text_style(theme, 11.5, FontWeight::MEDIUM, pal.on_accent),
    );
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
    let mut vertices = snapshot
        .nodes
        .iter()
        .map(|node| GraphVertex {
            id: node.node_id.clone(),
            label: node_label(node),
            detail: node.availability.clone(),
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

fn node_label(node: &ClusterNodeInfo) -> String {
    if node.display_name.is_empty() {
        short(&node.node_id, 11)
    } else {
        short(&node.display_name, 18)
    }
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
    use crate::theme::{AppThemeMode, resolve_theme};

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
        let lines = overview_lines(&snapshot, &topology, false);

        assert_eq!(lines[0], "Cluster overview");
        assert!(lines.iter().any(|line| line == "nodes: 2 / 2 online"));
        assert!(lines.iter().any(|line| line == "public: 1 reachable"));
        assert!(lines.iter().any(|line| line == "topology: 1 edges"));
        assert!(lines.iter().any(|line| line == "source: daemon connected"));
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

        let open = overview_lines(&snapshot, &topology, true);
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
                .any(|line| line == "hint: Show diagnostics for endpoint addresses.")
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
}
