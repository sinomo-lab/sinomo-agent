//! "Settings" tab — an iOS-Settings-style page for app + cluster configuration.

use std::cell::{Cell, RefCell};
use std::sync::Arc;

use sui::{
    Constraints, Event, EventCtx, EventPhase, FontWeight, MeasureCtx, PaintCtx, PointerButton,
    PointerEventKind, Rect, ScrollDelta, Size, Widget,
};

use sinomo_runtime::config::ConfigStore;
use sinomo_runtime::local::SinomoDataDir;

use crate::backend::Backend;
use crate::backend::config::{ConfigItem, build_config_store, resolve_items, set_bool};
use crate::state::Shared;
use crate::theme::{AppThemeMode, Palette};
use crate::ui::widgets;

const ROW_H: f32 = 46.0;
const CARD_W: f32 = 720.0;
const PAD: f32 = 22.0;

/// State backing the settings tab.
pub struct SettingsState {
    pub data_dir: Arc<SinomoDataDir>,
    pub config_store: Option<ConfigStore>,
    pub config_items: Vec<ConfigItem>,
    pub config_error: Option<String>,
    pub mode_label: String,
    pub status_detail: String,
    pub node_id: String,
    pub role: String,
    pub clustered: bool,
    pub cluster_summary: Option<String>,
    pub version: String,
    pub scroll: f32,
}

impl SettingsState {
    pub fn new(backend: &Backend) -> Self {
        let (config_store, config_items, config_error) = match build_config_store() {
            Ok(store) => {
                let items = resolve_items(&store, &backend.data_dir);
                (Some(store), items, None)
            }
            Err(error) => (None, Vec::new(), Some(error)),
        };

        let cluster_summary = match &backend.connection {
            crate::backend::Connection::Daemon(status) => status.cluster.as_ref().map(|cluster| {
                format!(
                    "cluster {} · {:?}",
                    short(&format!("{:?}", cluster.cluster_id), 14),
                    cluster.member_state
                )
            }),
            crate::backend::Connection::EmbeddedNode => None,
        };

        Self {
            data_dir: backend.data_dir.clone(),
            config_store,
            config_items,
            config_error,
            mode_label: backend.mode_label(),
            status_detail: backend.status_detail(),
            node_id: backend.node.node_id.to_string(),
            role: format!("{:?}", backend.node.role).to_lowercase(),
            clustered: backend.connection.is_clustered(),
            cluster_summary,
            version: env!("CARGO_PKG_VERSION").to_string(),
            scroll: 0.0,
        }
    }

    fn toggle_config(&mut self, index: usize) {
        let Some(item) = self.config_items.get(index) else {
            return;
        };
        let Some(current) = item.bool_value else {
            return;
        };
        let key = item.key.clone();
        if let Some(store) = &self.config_store
            && set_bool(store, &self.data_dir, &key, !current).is_ok()
        {
            self.config_items = resolve_items(store, &self.data_dir);
        }
    }
}

#[derive(Clone, PartialEq)]
enum Hit {
    ThemeMode(AppThemeMode),
    ConfigToggle(usize),
}

pub struct SettingsView {
    shared: Shared,
    hit_rects: RefCell<Vec<(Hit, Rect)>>,
    content_height: Cell<f32>,
    viewport_height: Cell<f32>,
}

impl SettingsView {
    pub fn new(shared: Shared) -> Self {
        Self {
            shared,
            hit_rects: RefCell::new(Vec::new()),
            content_height: Cell::new(0.0),
            viewport_height: Cell::new(0.0),
        }
    }

    fn palette(&self) -> (sui::DefaultTheme, Palette) {
        let theme = self.shared.theme();
        let pal = Palette::from_theme(&theme, widgets::theme_is_dark(&self.shared));
        (theme, pal)
    }
}

impl Widget for SettingsView {
    fn measure(&mut self, _ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        constraints.max
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let (theme, pal) = self.palette();
        let bounds = ctx.bounds();
        ctx.fill_bounds(pal.window);
        self.viewport_height.set(bounds.height());
        self.hit_rects.borrow_mut().clear();

        let card_w = CARD_W.min(bounds.width() - PAD * 2.0);
        let x = bounds.x() + (bounds.width() - card_w) / 2.0;
        let max_scroll = (self.content_height.get() - bounds.height()).max(0.0);
        let scroll = self.shared.settings().scroll.clamp(0.0, max_scroll);
        let mut y = bounds.y() + PAD - scroll;

        ctx.push_clip_rect(bounds);

        // APPEARANCE
        y = self.section_header(ctx, &theme, &pal, x, y, "APPEARANCE");
        y = self.theme_row(ctx, &theme, &pal, x, y, card_w);

        // CONNECTION
        y += 18.0;
        y = self.section_header(ctx, &theme, &pal, x, y, "CONNECTION");
        let settings = self.shared.settings();
        let conn_rows = vec![
            ("Mode", settings.mode_label.clone()),
            ("Status", settings.status_detail.clone()),
        ];
        y = self.value_card(ctx, &theme, &pal, x, y, card_w, &conn_rows);

        // NODE
        y += 18.0;
        y = self.section_header(ctx, &theme, &pal, x, y, "NODE");
        let node_rows = vec![
            ("Node ID", short(&settings.node_id, 30)),
            ("Role", settings.role.clone()),
        ];
        y = self.value_card(ctx, &theme, &pal, x, y, card_w, &node_rows);

        // CLUSTER
        y += 18.0;
        y = self.section_header(ctx, &theme, &pal, x, y, "CLUSTER");
        let cluster_rows = if settings.clustered {
            vec![(
                "Cluster",
                settings
                    .cluster_summary
                    .clone()
                    .unwrap_or_else(|| "connected".to_string()),
            )]
        } else {
            vec![
                ("State", "running locally".to_string()),
                ("Nodes", "this device only".to_string()),
            ]
        };
        y = self.value_card(ctx, &theme, &pal, x, y, card_w, &cluster_rows);

        // CONFIGURATION
        y += 18.0;
        y = self.section_header(ctx, &theme, &pal, x, y, "CLUSTER CONFIGURATION");
        if let Some(error) = &settings.config_error {
            let rows = vec![("Error", error.clone())];
            y = self.value_card(ctx, &theme, &pal, x, y, card_w, &rows);
        } else if settings.config_items.is_empty() {
            let rows = vec![("", "No configuration keys".to_string())];
            y = self.value_card(ctx, &theme, &pal, x, y, card_w, &rows);
        } else {
            y = self.config_card(ctx, &theme, &pal, x, y, card_w, &settings.config_items);
        }

        // ABOUT
        y += 18.0;
        y = self.section_header(ctx, &theme, &pal, x, y, "ABOUT");
        let about = vec![
            ("Sinomo App", format!("v{}", settings.version)),
            ("Toolkit", "SUI".to_string()),
        ];
        y = self.value_card(ctx, &theme, &pal, x, y, card_w, &about);

        drop(settings);
        ctx.pop_clip();

        // `y` is the screen-space bottom (scroll already applied); recover the
        // unscrolled content height (top-of-viewport to bottom + a trailing pad).
        let content_height = (y + scroll) - bounds.y() + PAD;
        self.content_height.set(content_height);
    }

    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        if ctx.phase() != EventPhase::Target {
            return;
        }
        if let Event::Pointer(p) = event {
            match p.kind {
                PointerEventKind::Down if p.button == Some(PointerButton::Primary) => {
                    let hit = self
                        .hit_rects
                        .borrow()
                        .iter()
                        .find(|(_, rect)| rect.contains(p.position))
                        .map(|(hit, _)| hit.clone());
                    if let Some(hit) = hit {
                        match hit {
                            Hit::ThemeMode(mode) => {
                                self.shared.borrow_mut().theme_mode = mode;
                            }
                            Hit::ConfigToggle(i) => {
                                self.shared.settings_mut().toggle_config(i);
                            }
                        }
                        ctx.request_paint();
                        ctx.set_handled();
                    }
                }
                PointerEventKind::Scroll => {
                    if let Some(delta) = p.scroll_delta {
                        let dy = match delta {
                            ScrollDelta::Lines(v) => v.y * 48.0,
                            ScrollDelta::Pixels(v) => v.y,
                        };
                        let max = (self.content_height.get() - self.viewport_height.get()).max(0.0);
                        let mut settings = self.shared.settings_mut();
                        settings.scroll = (settings.scroll - dy).clamp(0.0, max);
                        ctx.request_paint();
                    }
                }
                _ => {}
            }
        }
    }
}

impl SettingsView {
    fn section_header(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        x: f32,
        y: f32,
        title: &str,
    ) -> f32 {
        let style = pal.text_style(theme, 11.5, FontWeight::SEMIBOLD, pal.text_faint);
        widgets::draw_text_in(ctx, Rect::new(x + 6.0, y, 400.0, 18.0), title, style);
        y + 24.0
    }

    fn card_bg(&self, ctx: &mut PaintCtx, pal: &Palette, x: f32, y: f32, w: f32, h: f32) {
        widgets::round_panel(ctx, Rect::new(x, y, w, h), pal.panel, pal.border, 12.0);
    }

    #[allow(clippy::too_many_arguments)]
    fn value_card(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        x: f32,
        y: f32,
        w: f32,
        rows: &[(&str, String)],
    ) -> f32 {
        let h = rows.len() as f32 * ROW_H;
        self.card_bg(ctx, pal, x, y, w, h);
        let label_style = pal.text_style(theme, 13.5, FontWeight::MEDIUM, pal.text);
        let value_style = pal.text_style(theme, 13.0, FontWeight::NORMAL, pal.text_muted);
        for (i, (label, value)) in rows.iter().enumerate() {
            let ry = y + i as f32 * ROW_H;
            if i > 0 {
                ctx.fill_rect(Rect::new(x + 16.0, ry, w - 16.0, 1.0), pal.border);
            }
            widgets::draw_text_in(
                ctx,
                Rect::new(x + 16.0, ry, 220.0, ROW_H),
                label,
                label_style.clone(),
            );
            let vw = w - 16.0 - 236.0;
            let value = elide_to(value, vw, ctx, &value_style);
            let tw = ctx
                .measure_text(value.clone(), value_style.clone())
                .map(|m| m.width)
                .unwrap_or(0.0);
            widgets::draw_text_in(
                ctx,
                Rect::new(x + w - 16.0 - tw, ry, tw + 4.0, ROW_H),
                &value,
                value_style.clone(),
            );
        }
        y + h
    }

    fn theme_row(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        x: f32,
        y: f32,
        w: f32,
    ) -> f32 {
        let h = ROW_H;
        self.card_bg(ctx, pal, x, y, w, h);
        let label_style = pal.text_style(theme, 13.5, FontWeight::MEDIUM, pal.text);
        widgets::draw_text_in(
            ctx,
            Rect::new(x + 16.0, y, 200.0, h),
            "Appearance",
            label_style,
        );

        let modes = [
            AppThemeMode::Light,
            AppThemeMode::Dark,
            AppThemeMode::System,
        ];
        let current = self.shared.borrow().theme_mode;
        let seg_w = 84.0;
        let seg_h = 28.0;
        let total = seg_w * 3.0;
        let sx = x + w - 16.0 - total;
        let sy = y + (h - seg_h) / 2.0;
        widgets::round_panel(
            ctx,
            Rect::new(sx, sy, total, seg_h),
            pal.field,
            pal.border,
            seg_h / 2.0,
        );
        for (i, mode) in modes.iter().enumerate() {
            let seg = Rect::new(sx + i as f32 * seg_w, sy, seg_w, seg_h);
            let active = current == *mode;
            if active {
                widgets::fill_round_rect(
                    ctx,
                    seg.inflate(-2.0, -2.0),
                    pal.accent,
                    (seg_h - 4.0) / 2.0,
                );
            }
            let color = if active {
                pal.on_accent
            } else {
                pal.text_muted
            };
            let style = pal.text_style(theme, 12.0, FontWeight::MEDIUM, color);
            let label = mode.label();
            let tw = ctx
                .measure_text(label, style.clone())
                .map(|m| m.width)
                .unwrap_or(30.0);
            widgets::draw_text_in(
                ctx,
                Rect::new(
                    seg.x() + (seg.width() - tw) / 2.0,
                    seg.y(),
                    tw + 4.0,
                    seg.height(),
                ),
                label,
                style,
            );
            self.hit_rects
                .borrow_mut()
                .push((Hit::ThemeMode(*mode), seg));
        }
        y + h
    }

    #[allow(clippy::too_many_arguments)]
    fn config_card(
        &self,
        ctx: &mut PaintCtx,
        theme: &sui::DefaultTheme,
        pal: &Palette,
        x: f32,
        y: f32,
        w: f32,
        items: &[ConfigItem],
    ) -> f32 {
        let h = items.len() as f32 * ROW_H;
        self.card_bg(ctx, pal, x, y, w, h);
        let label_style = pal.text_style(theme, 13.0, FontWeight::MEDIUM, pal.text);
        let value_style = pal.text_style(theme, 12.0, FontWeight::NORMAL, pal.text_muted);
        let tag_style = pal.text_style(theme, 10.5, FontWeight::MEDIUM, pal.text_faint);

        for (i, item) in items.iter().enumerate() {
            let ry = y + i as f32 * ROW_H;
            if i > 0 {
                ctx.fill_rect(Rect::new(x + 16.0, ry, w - 16.0, 1.0), pal.border);
            }
            let key = item.key.trim_start_matches('/');
            widgets::draw_text_in(
                ctx,
                Rect::new(x + 16.0, ry + 4.0, w - 220.0, 20.0),
                key,
                label_style.clone(),
            );
            widgets::draw_text_in(
                ctx,
                Rect::new(x + 16.0, ry + 22.0, w - 220.0, 16.0),
                item.source,
                tag_style.clone(),
            );

            match item.bool_value {
                Some(on) => {
                    let track_w = 42.0;
                    let track_h = 24.0;
                    let tx = x + w - 16.0 - track_w;
                    let ty = ry + (ROW_H - track_h) / 2.0;
                    let track = Rect::new(tx, ty, track_w, track_h);
                    widgets::fill_round_rect(
                        ctx,
                        track,
                        if on { pal.accent } else { pal.field },
                        track_h / 2.0,
                    );
                    if !on {
                        widgets::round_panel(ctx, track, pal.field, pal.border, track_h / 2.0);
                    }
                    let knob = track_h - 6.0;
                    let kx = if on {
                        tx + track_w - knob - 3.0
                    } else {
                        tx + 3.0
                    };
                    widgets::fill_round_rect(
                        ctx,
                        Rect::new(kx, ty + 3.0, knob, knob),
                        if on { pal.on_accent } else { pal.text_muted },
                        knob / 2.0,
                    );
                    self.hit_rects
                        .borrow_mut()
                        .push((Hit::ConfigToggle(i), track));
                }
                None => {
                    let vw = 200.0;
                    let value = elide_to(&item.value, vw, ctx, &value_style);
                    let tw = ctx
                        .measure_text(value.clone(), value_style.clone())
                        .map(|m| m.width)
                        .unwrap_or(0.0);
                    widgets::draw_text_in(
                        ctx,
                        Rect::new(x + w - 16.0 - tw, ry, tw + 4.0, ROW_H),
                        &value,
                        value_style.clone(),
                    );
                }
            }
        }
        y + h
    }
}

fn elide_to(text: &str, max_w: f32, ctx: &PaintCtx, style: &sui::TextStyle) -> String {
    let full = ctx
        .measure_text(text, style.clone())
        .map(|m| m.width)
        .unwrap_or(0.0);
    if full <= max_w {
        return text.to_string();
    }
    let chars: Vec<char> = text.chars().collect();
    let mut n = chars.len();
    while n > 1 {
        n -= 1;
        let candidate: String = chars[..n].iter().collect::<String>() + "…";
        let w = ctx
            .measure_text(candidate.clone(), style.clone())
            .map(|m| m.width)
            .unwrap_or(0.0);
        if w <= max_w {
            return candidate;
        }
    }
    "…".to_string()
}

fn short(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let head: String = s.chars().take(max - 1).collect();
        format!("{head}…")
    }
}

/// Build the settings tab content.
pub fn view(shared: Shared) -> impl Widget {
    SettingsView::new(shared)
}
