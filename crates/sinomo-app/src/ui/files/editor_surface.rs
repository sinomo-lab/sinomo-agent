//! A vertical scrollbar for an unwrapped SUI text surface. The surface remains
//! the scroll owner, including wheel input, selection and caret navigation.
use super::*;
use sui::{PointerEvent, Signal, TextSurfaceStatus, Vector};

pub(super) struct EditorSurface {
    pub(super) text: TextSurface,
    reader: ThemeReader,
    status: Signal<TextSurfaceStatus>,
    pub(super) scroll_y: Signal<f32>,
    padding: Insets,
    line_height: f32,
    drag: Option<(u64, f32)>,
}

impl EditorSurface {
    pub(super) fn new(
        text: TextSurface,
        reader: &ThemeReader,
        status: Signal<TextSurfaceStatus>,
        padding: Insets,
    ) -> Self {
        let right = padding.right + reader().metrics.scroll_bar_thickness;
        Self {
            text: text
                .padding(Insets { right, ..padding })
                .status(status.clone()),
            reader: reader.clone(),
            status,
            padding,
            scroll_y: Signal::new(0.),
            line_height: 1.,
            drag: None,
        }
    }

    fn thumb(&self, bounds: Rect, scroll: f32) -> Option<(Rect, f32, f32)> {
        let theme = (self.reader)();
        let viewport = bounds.height() - self.padding.top - self.padding.bottom;
        let content = self.status.get().line_count as f32 * self.line_height;
        if viewport <= 0. || content <= viewport {
            return None;
        }
        let height = (viewport * viewport / content)
            .max(theme.metrics.scroll_bar_min_thumb_length)
            .min(viewport);
        let travel = viewport - height;
        if travel <= 0. {
            return None;
        }
        let width = theme.metrics.scroll_bar_thickness;
        let thumb = Rect::new(
            bounds.max_x() - width,
            bounds.y() + self.padding.top + travel * (scroll / (content - viewport)).clamp(0., 1.),
            width,
            height,
        );
        Some((thumb, travel, content - viewport))
    }
}
impl Widget for EditorSurface {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        if let Event::Pointer(p) = event
            && ctx.phase() != EventPhase::Capture
        {
            let bounds = ctx.bounds();
            let scroll = self.scroll_y.get();
            if self.drag.is_some_and(|(id, _)| id == p.pointer_id)
                && matches!(p.kind, PointerEventKind::Up | PointerEventKind::Cancel)
            {
                self.drag = None;
                ctx.release_pointer_capture(p.pointer_id);
                ctx.set_handled();
                return;
            }
            if let Some((thumb, travel, max)) = self.thumb(bounds, scroll) {
                if p.kind == PointerEventKind::Down
                    && p.button == Some(PointerButton::Primary)
                    && bounds.contains(p.position)
                    && p.position.x >= thumb.x()
                {
                    let grab = if thumb.contains(p.position) {
                        p.position.y - thumb.y()
                    } else {
                        thumb.height() / 2.
                    };
                    self.drag = Some((p.pointer_id, grab));
                    ctx.request_pointer_capture(p.pointer_id);
                }
                if let Some((id, grab)) = self.drag
                    && id == p.pointer_id
                    && matches!(p.kind, PointerEventKind::Down | PointerEventKind::Move)
                {
                    let y = ((p.position.y - bounds.y() - self.padding.top - grab) / travel)
                        .clamp(0., 1.)
                        * max;
                    let mut wheel = PointerEvent::new(
                        PointerEventKind::Scroll,
                        Point::new(
                            bounds.x() + bounds.width() * 0.5,
                            bounds.y() + bounds.height() * 0.5,
                        ),
                    );
                    wheel.scroll_delta = Some(ScrollDelta::Pixels(Vector::new(0., scroll - y)));
                    self.text.event(ctx, &Event::Pointer(wheel));
                    self.scroll_y.set(y);
                    ctx.request_paint();
                    ctx.set_handled();
                    return;
                }
            }
        }
        self.text.event(ctx, event);
    }
    fn measure(&mut self, ctx: &mut MeasureCtx, c: Constraints) -> Size {
        if c.max.height == 0. {
            return c.clamp(Size::ZERO);
        }
        let style = super::source_editor::mono(&(self.reader)());
        self.line_height = style.line_height.max(style.font_size).max(1.);
        self.text.measure(ctx, c)
    }
    fn arrange(&mut self, ctx: &mut ArrangeCtx, r: Rect) {
        self.text.arrange(ctx, r);
    }
    fn paint(&self, ctx: &mut PaintCtx) {
        self.text.paint(ctx);
        let scroll = ctx.observe(&self.scroll_y);
        if let Some((thumb, _, _)) = self.thumb(ctx.bounds(), scroll) {
            ctx.fill_rect(thumb, (self.reader)().palette.text_muted.with_alpha(0.65));
        }
    }
    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.text.semantics(ctx);
        if let Some(info) = ctx.nodes().last().and_then(|n| n.editable_text.as_ref()) {
            self.scroll_y.set(info.scroll_y);
        }
    }
    fn command(&mut self, ctx: &mut EventCtx, c: &sui::Command<'_>) {
        self.text.command(ctx, c);
    }
    fn accepts_focus(&self) -> bool {
        true
    }
    fn focus_changed(&mut self, ctx: &mut EventCtx, f: bool) {
        self.text.focus_changed(ctx, f);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn long_json_preview_and_source_paint_scrolled_text_and_keep_selection() -> sui::Result<()> {
        let shared = Shared::new(crate::state::AppState::ready(Backend::local_fallback()));
        let lines = (0..400)
            .map(|i| format!("  \"row_{i:03}\": \"visible content\""))
            .collect::<Vec<_>>()
            .join(",\n");
        let text = format!("{{\n{lines}\n}}");
        shared
            .open_document_tab(
                FilesSourceKind::Local,
                "long.json".into(),
                "long.json".into(),
            )
            .unwrap();
        let shared = shared.active_scope();
        let mut doc = FilesDocument::loading(
            FilesSourceKind::Local,
            "long.json".into(),
            "long.json".into(),
        );
        doc.format = DocumentFormat::Json;
        doc.preview_open = true;
        doc.loading = false;
        doc.saved_text = text.clone();
        doc.text = text.clone();
        shared.files_mut().document = Some(doc);
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Long JSON")
                    .root(document_view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let win = app.main_window()?;
        win.root()
            .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(560., 720.))))?;
        win.advance_time(1.)?;
        for (mode, name) in [
            ("Preview", "formatted-document-preview"),
            ("Source", "files-document:long.json"),
        ] {
            win.get_by_role(SemanticsRole::RadioButton)
                .with_name(mode)
                .click()?;
            win.run_until_idle()?;
            let node = || -> sui::Result<SemanticsNode> {
                Ok(win
                    .snapshot()?
                    .accessibility
                    .nodes
                    .into_iter()
                    .find(|n| n.name.as_deref() == Some(name))
                    .unwrap())
            };
            let bounds = node()?.bounds;
            let before = node()?.editable_text.unwrap();
            let point = |y| Point::new(bounds.max_x() - 3., y);
            for (kind, position) in [
                (PointerEventKind::Down, point(bounds.y() + 17.)),
                (PointerEventKind::Move, point(bounds.max_y() - 11.)),
                (PointerEventKind::Up, point(bounds.max_y() - 11.)),
            ] {
                let mut event = PointerEvent::new(kind, position);
                event.pointer_id = 1;
                event.button = Some(PointerButton::Primary);
                win.root().dispatch_event(Event::Pointer(event))?;
            }
            win.run_until_idle()?;
            let after = node()?.editable_text.unwrap();
            assert!(
                after.scroll_y > bounds.height() * 3.,
                "{mode} did not scroll: {after:?}"
            );
            assert_eq!(after.selection, before.selection);
            let shot = win.capture_screenshot()?;
            // Count rows containing glyphs away from gutter, caret, borders and scrollbar.
            let rows = (bounds.y() as usize + bounds.height() as usize / 2
                ..bounds.max_y() as usize - 32)
                .filter(|&y| {
                    (bounds.x() as usize + 110..bounds.x() as usize + 230).any(|x| {
                        let i = (y * shot.width() as usize + x) * 4;
                        let px = &shot.pixels()[i..i + 3];
                        px.iter().any(|v| *v > 140)
                    })
                })
                .count();
            assert!(
                rows > 25,
                "{mode} painted only {rows} rows of visible glyphs"
            );
            shot.write_png(std::env::temp_dir().join(format!("sinomo-long-json-{mode}.png")))?;
        }
        assert_eq!(shared.files().document.as_ref().unwrap().text, text);
        Ok(())
    }
}
