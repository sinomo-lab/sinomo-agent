//! "Files" tab — a SIFS distributed-filesystem browser.
//!
//! Standard file-manager UX (breadcrumb + listing) plus a SIFS inspector that
//! shows where an object lives and its placement / replication policy.

use sui::{
    ArrangeCtx, Constraints, Event, EventCtx, EventPhase, FontWeight, MeasureCtx, PaintCtx, Point,
    PointerButton, PointerEventKind, Rect, ScrollDelta, Size, Widget,
};

use sinomo_cli::app_api::{SifsBrowser, SifsEntry};

use crate::backend::Backend;
use crate::state::Shared;
use crate::theme::Palette;
use crate::ui::widgets;

const BREADCRUMB_H: f32 = 46.0;
const INSPECTOR_W: f32 = 340.0;
const ROW_H: f32 = 32.0;
const PAD: f32 = 16.0;

/// State backing the files tab.
pub struct FilesState {
    pub browser: Option<SifsBrowser>,
    pub error: Option<String>,
    pub clustered: bool,
    pub node_id: String,
    pub path: String,
    pub entries: Vec<SifsEntry>,
    pub selected: Option<usize>,
    pub scroll: f32,
}

impl FilesState {
    pub fn new(backend: &Backend) -> Self {
        let (browser, error, node_id) = match SifsBrowser::open(&backend.data_dir) {
            Ok(browser) => {
                let node = browser.node_id().to_string();
                (Some(browser), None, node)
            }
            Err(error) => (None, Some(error), String::new()),
        };
        let mut state = Self {
            browser,
            error,
            clustered: backend.connection.is_clustered(),
            node_id,
            path: "/".to_string(),
            entries: Vec::new(),
            selected: None,
            scroll: 0.0,
        };
        state.reload();
        state
    }

    fn reload(&mut self) {
        self.selected = None;
        self.scroll = 0.0;
        let path = self.path.clone();
        let listing = self.browser.as_ref().map(|b| b.list(&path));
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
    }

    fn navigate(&mut self, path: String) {
        let normalized = self
            .browser
            .as_ref()
            .map(|b| b.normalize(&path))
            .unwrap_or(path);
        self.path = normalized;
        self.reload();
    }

    fn go_up(&mut self) {
        let parent = self.browser.as_ref().and_then(|b| b.parent(&self.path));
        if let Some(parent) = parent {
            self.navigate(parent);
        }
    }

    fn selected_entry(&self) -> Option<&SifsEntry> {
        self.selected.and_then(|i| self.entries.get(i))
    }
}

#[derive(Clone, PartialEq)]
enum Hit {
    Crumb(String),
    Row(usize),
    Up,
}

pub struct FilesView {
    shared: Shared,
    breadcrumb_rect: Rect,
    list_rect: Rect,
    inspector_rect: Rect,
    up_rect: Rect,
    crumb_rects: Vec<(String, Rect)>,
    crumb_widths: Vec<f32>,
    hovered: Option<usize>,
    row_h: f32,
}

impl FilesView {
    pub fn new(shared: Shared) -> Self {
        Self {
            shared,
            breadcrumb_rect: Rect::ZERO,
            list_rect: Rect::ZERO,
            inspector_rect: Rect::ZERO,
            up_rect: Rect::ZERO,
            crumb_rects: Vec::new(),
            crumb_widths: Vec::new(),
            hovered: None,
            row_h: ROW_H,
        }
    }

    fn palette(&self) -> (sui::DefaultTheme, Palette) {
        let theme = self.shared.theme();
        let pal = Palette::from_theme(&theme, widgets::theme_is_dark(&self.shared));
        (theme, pal)
    }

    fn crumbs(path: &str) -> Vec<(String, String)> {
        // (label, target_path)
        let mut out = vec![("SIFS".to_string(), "/".to_string())];
        let mut acc = String::new();
        for part in path.split('/').filter(|s| !s.is_empty()) {
            acc.push('/');
            acc.push_str(part);
            out.push((part.to_string(), acc.clone()));
        }
        out
    }

    fn row_index_at(&self, p: Point) -> Option<usize> {
        if !self.list_rect.contains(p) {
            return None;
        }
        let scroll = self.shared.files().scroll;
        let idx = ((p.y - self.list_rect.y() + scroll) / self.row_h).floor();
        if idx < 0.0 {
            return None;
        }
        let idx = idx as usize;
        if idx < self.shared.files().entries.len() {
            Some(idx)
        } else {
            None
        }
    }

    fn hit_test(&self, p: Point) -> Option<Hit> {
        if self.up_rect.contains(p) {
            return Some(Hit::Up);
        }
        for (path, rect) in &self.crumb_rects {
            if rect.contains(p) {
                return Some(Hit::Crumb(path.clone()));
            }
        }
        if let Some(i) = self.row_index_at(p) {
            return Some(Hit::Row(i));
        }
        None
    }
}

impl Widget for FilesView {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let theme = self.shared.theme();
        let style = sui::TextStyle {
            font_size: 13.0,
            weight: FontWeight::MEDIUM,
            ..theme.body_text_style()
        };
        let crumbs = Self::crumbs(&self.shared.files().path);
        self.crumb_widths = crumbs
            .iter()
            .map(|(label, _)| {
                ctx.layout()
                    .measure_text(label.clone(), style.clone())
                    .map(|m| m.width + 6.0)
                    .unwrap_or(40.0)
            })
            .collect();
        constraints.max
    }

    fn arrange(&mut self, _ctx: &mut ArrangeCtx, bounds: Rect) {
        self.breadcrumb_rect = Rect::new(bounds.x(), bounds.y(), bounds.width(), BREADCRUMB_H);
        let body_y = bounds.y() + BREADCRUMB_H;
        let body_h = (bounds.height() - BREADCRUMB_H).max(0.0);
        let list_w = (bounds.width() - INSPECTOR_W).max(120.0);
        self.list_rect = Rect::new(bounds.x(), body_y, list_w, body_h);
        self.inspector_rect = Rect::new(bounds.x() + list_w, body_y, INSPECTOR_W, body_h);

        // Up button + breadcrumb segments.
        self.up_rect = Rect::new(
            bounds.x() + PAD,
            bounds.y() + (BREADCRUMB_H - 28.0) / 2.0,
            28.0,
            28.0,
        );
        let mut x = self.up_rect.x() + self.up_rect.width() + 12.0;
        let y = bounds.y() + (BREADCRUMB_H - 24.0) / 2.0;
        self.crumb_rects.clear();
        for (i, (_, target)) in Self::crumbs(&self.shared.files().path)
            .into_iter()
            .enumerate()
        {
            let w = self.crumb_widths.get(i).copied().unwrap_or(40.0);
            self.crumb_rects.push((target, Rect::new(x, y, w, 24.0)));
            x += w + 16.0; // room for the separator
        }
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        let (theme, pal) = self.palette();
        ctx.fill_bounds(pal.window);
        self.paint_breadcrumb(ctx, &theme, &pal);
        self.paint_list(ctx, &theme, &pal);
        self.paint_inspector(ctx, &theme, &pal);
    }

    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        if ctx.phase() != EventPhase::Target {
            return;
        }
        if let Event::Pointer(p) = event {
            match p.kind {
                PointerEventKind::Move => {
                    let row = self.row_index_at(p.position);
                    if row != self.hovered {
                        self.hovered = row;
                        ctx.request_paint();
                    }
                }
                PointerEventKind::Down if p.button == Some(PointerButton::Primary) => {
                    if let Some(hit) = self.hit_test(p.position) {
                        match hit {
                            Hit::Up => self.shared.files_mut().go_up(),
                            Hit::Crumb(path) => self.shared.files_mut().navigate(path),
                            Hit::Row(i) => {
                                let is_dir = self
                                    .shared
                                    .files()
                                    .entries
                                    .get(i)
                                    .map(|e| e.is_dir)
                                    .unwrap_or(false);
                                if is_dir {
                                    let target = self.shared.files().entries[i].path.clone();
                                    self.shared.files_mut().navigate(target);
                                } else {
                                    self.shared.files_mut().selected = Some(i);
                                }
                            }
                        }
                        ctx.request_measure();
                        ctx.request_arrange();
                        ctx.request_paint();
                        ctx.set_handled();
                    }
                }
                PointerEventKind::Scroll => {
                    if self.list_rect.contains(p.position)
                        && let Some(delta) = p.scroll_delta
                    {
                        let dy = match delta {
                            ScrollDelta::Lines(v) => v.y * 48.0,
                            ScrollDelta::Pixels(v) => v.y,
                        };
                        let mut files = self.shared.files_mut();
                        let max = (files.entries.len() as f32 * self.row_h
                            - self.list_rect.height())
                        .max(0.0);
                        files.scroll = (files.scroll - dy).clamp(0.0, max);
                        ctx.request_paint();
                    }
                }
                _ => {}
            }
        }
    }
}

impl FilesView {
    fn paint_breadcrumb(&self, ctx: &mut PaintCtx, theme: &sui::DefaultTheme, pal: &Palette) {
        ctx.fill_rect(self.breadcrumb_rect, pal.titlebar);
        widgets::hairline_bottom(ctx, self.breadcrumb_rect, pal.border);

        // Up button.
        widgets::round_panel(ctx, self.up_rect, pal.field, pal.border, 8.0);
        let up_style = pal.text_style(theme, 15.0, FontWeight::SEMIBOLD, pal.text_muted);
        widgets::draw_centered(
            ctx,
            "↑",
            up_style,
            self.up_rect.x() + self.up_rect.width() / 2.0,
            self.up_rect.y() + self.up_rect.height() / 2.0,
        );

        let crumbs = Self::crumbs(&self.shared.files().path);
        let last = crumbs.len().saturating_sub(1);
        for (i, ((label, _), (_, rect))) in crumbs.iter().zip(self.crumb_rects.iter()).enumerate() {
            let active = i == last;
            let color = if active { pal.text } else { pal.text_muted };
            let weight = if active {
                FontWeight::SEMIBOLD
            } else {
                FontWeight::MEDIUM
            };
            let style = pal.text_style(theme, 13.0, weight, color);
            widgets::draw_text_in(ctx, *rect, label, style);
            if i != last {
                let sep = pal.text_style(theme, 13.0, FontWeight::NORMAL, pal.text_faint);
                widgets::draw_text_in(
                    ctx,
                    Rect::new(rect.x() + rect.width() + 2.0, rect.y(), 12.0, rect.height()),
                    "/",
                    sep,
                );
            }
        }
    }

    fn paint_list(&self, ctx: &mut PaintCtx, theme: &sui::DefaultTheme, pal: &Palette) {
        let area = self.list_rect;
        let files = self.shared.files();

        if let Some(error) = &files.error {
            let style = pal.text_style(theme, 13.0, FontWeight::NORMAL, pal.bad);
            widgets::draw_text_in(
                ctx,
                Rect::new(
                    area.x() + PAD,
                    area.y() + PAD,
                    area.width() - PAD * 2.0,
                    24.0,
                ),
                &format!("SIFS unavailable: {error}"),
                style,
            );
            return;
        }
        if files.entries.is_empty() {
            let cx = area.x() + area.width() / 2.0;
            let cy = area.y() + area.height() / 2.0;
            let mark = pal.text_style(theme, 30.0, FontWeight::BOLD, pal.text_faint);
            widgets::draw_centered(ctx, "▤", mark, cx, cy - 26.0);
            let title = pal.text_style(theme, 15.0, FontWeight::MEDIUM, pal.text_muted);
            widgets::draw_centered(ctx, "Empty directory", title, cx, cy + 4.0);
            return;
        }

        ctx.push_clip_rect(area);
        let scroll = files.scroll;
        let mut y = area.y() - scroll;
        for (i, entry) in files.entries.iter().enumerate() {
            let row = Rect::new(area.x(), y, area.width(), self.row_h);
            if y + self.row_h >= area.y() && y <= area.y() + area.height() {
                let selected = files.selected == Some(i);
                let hovered = self.hovered == Some(i);
                if selected {
                    widgets::fill_round_rect(ctx, row.inflate(-6.0, -1.0), pal.selected, 7.0);
                } else if hovered {
                    widgets::fill_round_rect(ctx, row.inflate(-6.0, -1.0), pal.hover, 7.0);
                }
                let glyph = if entry.is_dir {
                    "▸"
                } else if entry.is_link {
                    "↪"
                } else {
                    "•"
                };
                let gcolor = if entry.is_dir {
                    pal.accent
                } else {
                    pal.text_faint
                };
                let gs = pal.text_style(theme, 13.0, FontWeight::SEMIBOLD, gcolor);
                widgets::draw_text_in(
                    ctx,
                    Rect::new(row.x() + 16.0, row.y(), 18.0, self.row_h),
                    glyph,
                    gs,
                );

                let name_style = pal.text_style(theme, 13.5, FontWeight::MEDIUM, pal.text);
                widgets::draw_text_in(
                    ctx,
                    Rect::new(row.x() + 40.0, row.y(), area.width() - 200.0, self.row_h),
                    &entry.name,
                    name_style,
                );

                let meta = if entry.is_dir {
                    "folder".to_string()
                } else {
                    human_size(entry.size)
                };
                let meta_style = pal.text_style(theme, 12.0, FontWeight::NORMAL, pal.text_faint);
                widgets::draw_text_in(
                    ctx,
                    Rect::new(area.x() + area.width() - 150.0, row.y(), 134.0, self.row_h),
                    &meta,
                    meta_style,
                );
            }
            y += self.row_h;
            let _ = i;
        }
        ctx.pop_clip();
    }

    fn paint_inspector(&self, ctx: &mut PaintCtx, theme: &sui::DefaultTheme, pal: &Palette) {
        let area = self.inspector_rect;
        ctx.fill_rect(area, pal.sidebar);
        widgets::hairline_left(ctx, area, pal.border);
        let files = self.shared.files();

        let x = area.x() + PAD;
        let w = area.width() - PAD * 2.0;
        let mut y = area.y() + PAD + 4.0;

        let header = pal.text_style(theme, 11.0, FontWeight::SEMIBOLD, pal.text_faint);
        widgets::draw_text_in(ctx, Rect::new(x, y, w, 18.0), "INSPECTOR", header);
        y += 30.0;

        let Some(entry) = files.selected_entry() else {
            let hint = pal.text_style(theme, 13.0, FontWeight::NORMAL, pal.text_faint);
            widgets::draw_text_in(
                ctx,
                Rect::new(x, y + 8.0, w, 40.0),
                "Select a file to see its SIFS placement.",
                hint,
            );
            return;
        };

        // Name + kind.
        let name_style = pal.text_style(theme, 16.0, FontWeight::SEMIBOLD, pal.text);
        widgets::draw_text_in(ctx, Rect::new(x, y, w, 24.0), &entry.name, name_style);
        y += 26.0;
        let kind = if entry.is_dir {
            "Directory"
        } else if entry.is_link {
            "Symlink"
        } else {
            "File"
        };
        let kind_style = pal.text_style(theme, 12.5, FontWeight::NORMAL, pal.text_muted);
        widgets::draw_text_in(
            ctx,
            Rect::new(x, y, w, 20.0),
            &format!("{kind} · {}", human_size(entry.size)),
            kind_style,
        );
        y += 34.0;

        y = self.info_row(ctx, theme, pal, x, y, w, "Path", &entry.path);

        // Placement section.
        y += 10.0;
        let sect = pal.text_style(theme, 11.0, FontWeight::SEMIBOLD, pal.text_faint);
        widgets::draw_text_in(ctx, Rect::new(x, y, w, 16.0), "PLACEMENT", sect);
        y += 24.0;

        // node badge
        let node_label = if files.node_id.is_empty() {
            "local".to_string()
        } else {
            short(&files.node_id, 18)
        };
        let badge_style = pal.text_style(theme, 12.0, FontWeight::MEDIUM, pal.accent);
        let badge_w = ctx
            .measure_text(format!("◉ {node_label}"), badge_style.clone())
            .map(|m| m.width + 20.0)
            .unwrap_or(120.0);
        let badge = Rect::new(x, y, badge_w.min(w), 26.0);
        widgets::round_panel(ctx, badge, pal.field, pal.border, 13.0);
        widgets::draw_text_in(
            ctx,
            Rect::new(
                badge.x() + 10.0,
                badge.y(),
                badge.width() - 14.0,
                badge.height(),
            ),
            &format!("◉ {node_label}"),
            badge_style,
        );
        y += 36.0;

        let row_label = pal.text_style(theme, 12.5, FontWeight::NORMAL, pal.text_muted);
        let row_val = pal.text_style(theme, 12.5, FontWeight::MEDIUM, pal.text);
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
            let note = pal.text_style(theme, 11.5, FontWeight::NORMAL, pal.text_faint);
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

/// Build the files tab content.
pub fn view(shared: Shared) -> impl Widget {
    FilesView::new(shared)
}
