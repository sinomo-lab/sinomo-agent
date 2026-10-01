//! Retained tab bodies and the document-style navigation strip.
use super::{chat, cluster, evolution, files, settings};
use crate::state::{Shared, Tab, WorkspaceSelection, WorkspaceTabKind};
use std::{cell::Cell, rc::Rc};
use sui::{
    ArrangeCtx, BrowserTabBar, Constraints, ContextMenu, EventCtx, IconButton, IconGlyph,
    MeasureCtx, MenuItem, PaintCtx, PointerButton, Rect, ScrollState, ScrollView, SemanticsCtx,
    Size, TrailingSlotRow, Widget, WidgetPod, WidgetPodMutVisitor, WidgetPodVisitor,
    containers::Padding,
};

pub(super) struct TabStrip {
    shared: Shared,
    child: WidgetPod,
    scroll: ScrollState,
    selected_bounds: Rc<Cell<Option<Rect>>>,
    last_selection: Option<(
        WorkspaceSelection,
        Vec<crate::state::WorkspaceTabId>,
        Size,
        Option<Rect>,
    )>,
}

fn relayout(ctx: &mut EventCtx) {
    ctx.request_measure();
    ctx.request_arrange();
    ctx.request_semantics();
    ctx.request_paint();
}

impl TabStrip {
    pub fn new(shared: Shared, compact: bool) -> Self {
        let labels = shared.clone();
        let selected = shared.clone();
        let select = shared.clone();
        let close = shared.clone();
        let theme = shared.clone();
        let tabs = BrowserTabBar::new("shell-tabs")
            .max_tab_width(220.0)
            .tabs_when(move || labels.signals().workspace_titles.get())
            .selected_when(move || match selected.signals().workspace_selection.get() {
                WorkspaceSelection::Pinned(_) => None,
                WorkspaceSelection::Open(id) => selected
                    .signals()
                    .workspace_tabs
                    .get()
                    .iter()
                    .position(|tab| *tab == id),
            })
            .theme_when(move || {
                let mut theme = theme.theme();
                theme.metrics.tab_height = 32.0;
                theme.metrics.tab_gap = 3.0;
                theme
            })
            .on_change_with_ctx(move |ctx, index, _| {
                if let Some(id) = select.signals().workspace_tabs.get().get(index) {
                    select.select_workspace_tab(*id);
                }
                relayout(ctx);
            })
            .on_close_with_ctx(move |ctx, index, _| {
                if let Some(id) = close.signals().workspace_tabs.get().get(index) {
                    close.close_workspace_tab(*id);
                }
                relayout(ctx);
            });
        let menu_shared = shared.clone();
        let menu_theme = shared.clone();
        let button_theme = shared.clone();
        let button_enabled = shared.clone();
        let menu_enabled = shared.clone();
        let add = ContextMenu::new(
            "New tab menu",
            IconButton::new(IconGlyph::Add, "New tab")
                .size(32.0)
                .icon_size(18.0)
                .appearance(sui::ButtonAppearance::Ghost)
                .enabled_when(move || !button_enabled.borrow().updates.view.busy)
                .theme_when(move || button_theme.theme()),
        )
        .items_when(move || {
            ["Chat", "Files"]
                .map(|label| {
                    let item = MenuItem::new(label);
                    if menu_enabled.borrow().updates.view.busy {
                        item.enabled(false)
                    } else {
                        item
                    }
                })
                .to_vec()
        })
        .activation_button(PointerButton::Primary)
        .anchor_to_pointer(false)
        .theme_when(move || menu_theme.theme())
        .on_activate_path_with_ctx(move |ctx, path, _| {
            if let Some(kind) = match path.as_slice() {
                [0] => Some(WorkspaceTabKind::Chat),
                [1] => Some(WorkspaceTabKind::Files),
                _ => None,
            } {
                menu_shared.new_workspace_tab(kind);
            }
            relayout(ctx);
        });
        let scroll_state = ScrollState::new();
        let selected_bounds = Rc::new(Cell::new(None));
        let tabs = MeasuredTabs {
            tabs,
            selected_bounds: selected_bounds.clone(),
        };
        let scroll = ScrollView::horizontal(Padding::all(4.0, tabs))
            .state(scroll_state.clone())
            .name("Primary navigation tabs")
            .scroll_bars(false);
        let mut pinned = sui::Flex::horizontal().gap(2.0);
        for tab in Tab::ALL {
            pinned = pinned.with_child(
                sui::Tooltip::new(
                    tab.title(),
                    super::pinned_tab::PinnedTab::new(shared.clone(), tab, compact),
                )
                .placement(sui::TooltipPlacement::Below)
                .theme(shared.theme()),
            );
        }
        let content = sui::Flex::horizontal()
            .align_items(sui::Alignment::Center)
            .with_child(Padding::symmetric(4.0, 4.0, pinned))
            .with_item(scroll, sui::FlexItem::flex(1.0).min_width(0.0));
        let child = WidgetPod::new(
            TrailingSlotRow::new(content, Padding::symmetric(2.0, 4.0, add))
                .trailing_width(36.0)
                .trailing_height(40.0),
        );
        Self {
            shared,
            child,
            scroll: scroll_state,
            selected_bounds,
            last_selection: None,
        }
    }
}

impl Widget for TabStrip {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let ids = ctx.observe(&self.shared.signals().workspace_tabs);
        ctx.observe(&self.shared.signals().workspace_titles);
        let selected = ctx.observe(&self.shared.signals().workspace_selection);
        let size = self.child.measure(ctx, constraints);
        let key = (selected, ids, size, self.selected_bounds.get());
        if self.last_selection.as_ref() != Some(&key) {
            self.last_selection = Some(key);
            if let Some(bounds) = self.selected_bounds.get() {
                let offset = self.scroll.current_offset().x;
                let viewport = self.scroll.viewport_size().width;
                let left = bounds.x();
                let right = bounds.max_x() + 8.0;
                let next = if left < offset {
                    left
                } else if right > offset + viewport {
                    right - viewport
                } else {
                    offset
                };
                if self.scroll.set_offset(sui::Vector::new(next, 0.0)) {
                    self.child.measure(ctx, constraints);
                }
            }
        }
        size
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

struct MeasuredTabs {
    tabs: BrowserTabBar,
    selected_bounds: Rc<Cell<Option<Rect>>>,
}

impl Widget for MeasuredTabs {
    fn event(&mut self, ctx: &mut EventCtx, event: &sui::Event) {
        self.tabs.event(ctx, event);
    }
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let size = self.tabs.measure(ctx, constraints);
        self.selected_bounds.set(self.tabs.selected_tab_bounds());
        size
    }
    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        self.tabs.arrange(ctx, bounds);
    }
    fn paint(&self, ctx: &mut PaintCtx) {
        self.tabs.paint(ctx);
    }
    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.tabs.semantics(ctx);
    }
    fn accepts_focus(&self) -> bool {
        self.tabs.accepts_focus()
    }
    fn focus_changed(&mut self, ctx: &mut EventCtx, focused: bool) {
        self.tabs.focus_changed(ctx, focused);
    }
}

pub(super) struct TabContents {
    shared: Shared,
    selected: WorkspaceSelection,
    children: Vec<(WorkspaceSelection, WidgetPod)>,
}

impl TabContents {
    pub fn new(shared: Shared) -> Self {
        let selected = shared.signals().workspace_selection.get();
        Self {
            shared,
            selected,
            children: Vec::new(),
        }
    }

    fn active(&self) -> Option<&WidgetPod> {
        self.children
            .iter()
            .find(|(key, _)| *key == self.selected)
            .map(|(_, pod)| pod)
    }
    fn active_mut(&mut self) -> Option<&mut WidgetPod> {
        self.children
            .iter_mut()
            .find(|(key, _)| *key == self.selected)
            .map(|(_, pod)| pod)
    }
}

impl Widget for TabContents {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let ids = ctx.observe(&self.shared.signals().workspace_tabs);
        self.selected = ctx.observe(&self.shared.signals().workspace_selection);
        self.children.retain(|(key, _)| match key {
            WorkspaceSelection::Pinned(_) => true,
            WorkspaceSelection::Open(id) => ids.contains(id),
        });
        if self.active().is_none() {
            let shared = self.shared.root_scope();
            let child = match self.selected {
                WorkspaceSelection::Pinned(Tab::Sinomo) => WidgetPod::new(chat::view(shared)),
                WorkspaceSelection::Pinned(Tab::Files) => WidgetPod::new(files::view(shared)),
                WorkspaceSelection::Pinned(Tab::Cluster) => WidgetPod::new(cluster::view(shared)),
                WorkspaceSelection::Pinned(Tab::Evolution) => {
                    WidgetPod::new(evolution::view(shared))
                }
                WorkspaceSelection::Pinned(Tab::Settings) => WidgetPod::new(settings::view(shared)),
                WorkspaceSelection::Open(id) => {
                    let Some(shared) = shared.workspace_scope(id) else {
                        return constraints.clamp(Size::ZERO);
                    };
                    match shared.workspace_kind() {
                        Some(WorkspaceTabKind::Chat) => WidgetPod::new(chat::view(shared)),
                        Some(WorkspaceTabKind::Document) => {
                            WidgetPod::new(files::document_view(shared))
                        }
                        _ => WidgetPod::new(files::view(shared)),
                    }
                }
            };
            self.children.push((self.selected, child));
        }
        self.active_mut().unwrap().measure(ctx, constraints)
    }
    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        if let Some(child) = self.active_mut() {
            child.arrange(ctx, bounds);
        }
    }
    fn paint(&self, ctx: &mut PaintCtx) {
        if let Some(child) = self.active() {
            child.paint(ctx);
        }
    }
    fn semantics(&self, ctx: &mut SemanticsCtx) {
        if let Some(child) = self.active() {
            child.semantics(ctx);
        }
    }
    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        if let Some(child) = self.active() {
            visitor.visit(child);
        }
    }
    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        if let Some(child) = self.active_mut() {
            visitor.visit(child);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{backend::Backend, state::AppState};
    use sui::{Event, SemanticsRole, WindowEvent};

    #[test]
    fn workspace_tabs_menu_opens_independent_tabs_and_closes_them() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        let app = sui_testing::TestApp::from_runtime(crate::build_runtime(shared.clone())?)?;
        let window = app.main_window()?;
        window
            .root()
            .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(
                1100.0, 760.0,
            ))))?;
        let before = window.snapshot()?;
        assert!(
            !before
                .accessibility
                .nodes
                .iter()
                .any(|node| node.name.as_deref() == Some("Close Chat tab"))
        );
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("New tab")
            .click()?;
        window
            .get_by_role(SemanticsRole::MenuItem)
            .with_name("Chat")
            .click()?;
        assert_eq!(shared.workspace_scopes().len(), 1);
        shared.active_scope().chat_mut().input = "tab draft".into();
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("New tab")
            .click()?;
        window
            .get_by_role(SemanticsRole::MenuItem)
            .with_name("Files")
            .click()?;
        assert_eq!(shared.workspace_scopes().len(), 2);
        assert_eq!(
            shared.active_scope().workspace_kind(),
            Some(WorkspaceTabKind::Files)
        );
        if std::env::var_os("SINOMO_TABS_SHOTS").is_some() {
            let dir =
                std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/visual-inspection");
            std::fs::create_dir_all(&dir).unwrap();
            window
                .capture_screenshot()?
                .write_png(dir.join("workspace-tabs-desktop.png"))?;
        }
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Close Files tab")
            .click()?;
        assert_eq!(shared.workspace_scopes().len(), 1);
        assert_eq!(shared.active_scope().chat().input, "tab draft");
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Close New chat tab")
            .click()?;
        assert!(shared.workspace_scopes().is_empty());
        Ok(())
    }

    #[test]
    fn workspace_tabs_overflow_keeps_pinned_icons_and_add_button_fixed() -> sui::Result<()> {
        for width in [1100.0, 620.0, 360.0, 320.0] {
            let shared = Shared::new(AppState::ready(Backend::local_fallback()));
            for index in 0..8 {
                let id = shared.new_workspace_tab(WorkspaceTabKind::Files).unwrap();
                shared.workspace_scope(id).unwrap().files_mut().path = format!("/Folder {index}");
            }
            let app = sui_testing::TestApp::from_runtime(crate::build_runtime(shared.clone())?)?;
            let window = app.main_window()?;
            window
                .root()
                .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(width, 720.0))))?;
            let snapshot = window.snapshot()?;
            let controls: Vec<_> = snapshot
                .accessibility
                .nodes
                .iter()
                .filter(|node| {
                    node.description
                        .as_deref()
                        .is_some_and(|description| description.starts_with("Pinned "))
                        || node.name.as_deref() == Some("New tab")
                })
                .map(|node| (node.name.clone(), node.bounds))
                .collect();
            assert_eq!(controls.len(), 6);
            for (_, bounds) in &controls {
                assert!(bounds.x() >= 0.0 && bounds.max_x() <= width);
            }
            let close = snapshot
                .accessibility
                .nodes
                .iter()
                .find(|node| node.name.as_deref() == Some("Close Folder 7 tab"))
                .unwrap();
            let scroll = snapshot
                .accessibility
                .nodes
                .iter()
                .find(|node| node.name.as_deref() == Some("Primary navigation tabs"))
                .unwrap();
            assert!(
                close.bounds.x() >= scroll.bounds.x()
                    && close.bounds.max_x() <= scroll.bounds.max_x(),
                "close must be revealed at {width}: {:?} / {:?}",
                close.bounds,
                scroll.bounds
            );
            if std::env::var_os("SINOMO_TABS_SHOTS").is_some() {
                let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("target/visual-inspection");
                std::fs::create_dir_all(&dir).unwrap();
                window
                    .capture_screenshot()?
                    .write_png(dir.join(format!("workspace-tabs-overflow-{width:.0}.png")))?;
            }
            window
                .get_by_role(SemanticsRole::ScrollView)
                .with_name("Primary navigation tabs")
                .scroll_pixels(sui::Vector::new(600.0, 0.0))?;
            let scrolled = window.snapshot()?;
            for (name, bounds) in controls {
                assert!(
                    scrolled
                        .accessibility
                        .nodes
                        .iter()
                        .any(|node| node.name == name && node.bounds == bounds)
                );
            }
            window
                .get_by_role(SemanticsRole::Button)
                .with_name("Settings")
                .click()?;
            assert_eq!(shared.borrow().active_workspace_tab, None);
            assert_eq!(shared.active_tab(), Tab::Settings);
            let selected = window.snapshot()?;
            assert!(
                selected
                    .accessibility
                    .nodes
                    .iter()
                    .any(
                        |node| node.description.as_deref() == Some("Pinned Settings tab")
                            && node.state.selected
                    )
            );
        }
        Ok(())
    }
}
