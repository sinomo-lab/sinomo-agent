//! A fixed application destination, independent of closable workspace tabs.
use crate::state::{Shared, Tab, WorkspaceSelection};
use sui::{
    Constraints, Event, EventCtx, IconButton, MeasureCtx, PaintCtx, SemanticsCtx, Size, Widget,
};

pub(super) struct PinnedTab {
    shared: Shared,
    control: IconButton,
}

impl PinnedTab {
    pub fn new(shared: Shared, destination: Tab, compact: bool) -> Self {
        let selected = shared.clone();
        let activate = shared.clone();
        let theme = shared.clone();
        let control = IconButton::from_image(
            super::shell::tab_icons()[destination.index()].handle(),
            destination.title(),
        )
        .description(format!("Pinned {} tab", destination.title()))
        .size(if compact { 24.0 } else { 32.0 })
        .icon_size(18.0)
        .appearance(sui::ButtonAppearance::Ghost)
        .tone(sui::SemanticTone::Neutral)
        .selected_when(move || {
            selected.signals().workspace_selection.get() == WorkspaceSelection::Pinned(destination)
        })
        .theme_when(move || {
            let mut theme = theme.theme();
            theme.metrics.min_height = if compact { 24.0 } else { 32.0 };
            theme
        })
        .on_press_with_ctx(move |ctx| {
            activate.set_active_tab(destination);
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_semantics();
            ctx.request_paint();
        });
        Self { shared, control }
    }
}

impl Widget for PinnedTab {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        self.control.event(ctx, event);
    }
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        ctx.observe(&self.shared.signals().workspace_selection);
        self.control.measure(ctx, constraints)
    }
    fn paint(&self, ctx: &mut PaintCtx) {
        self.control.paint(ctx);
    }
    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.control.semantics(ctx);
    }
    fn accepts_focus(&self) -> bool {
        true
    }
    fn focus_changed(&mut self, ctx: &mut EventCtx, focused: bool) {
        self.control.focus_changed(ctx, focused);
    }
}
