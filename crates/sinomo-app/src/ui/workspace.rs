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
    tab_width_limit: Rc<Cell<f32>>,
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
        let tab_width_limit = Rc::new(Cell::new(220.0));
        let theme_width_limit = tab_width_limit.clone();
        let tabs = BrowserTabBar::new("shell-tabs")
            .max_tab_width(220.0)
            .text_overflow(sui::TextOverflow::Ellipsis)
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
                theme.metrics.tab_min_width =
                    theme.metrics.tab_min_width.min(theme_width_limit.get());
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
            tabs: Some(tabs),
            selected_bounds: selected_bounds.clone(),
            width_limit: tab_width_limit.clone(),
        };
        let scroll = ScrollView::horizontal(Padding::all(4.0, tabs))
            .state(scroll_state.clone())
            .name("Primary navigation tabs")
            .scroll_bars(false);
        let mut pinned = sui::Flex::horizontal().gap(2.0);
        if compact {
            let destinations = shared.clone();
            let nav_theme = shared.clone();
            let navigation = ContextMenu::new(
                "Application navigation",
                sui::Button::new("Navigate")
                    .icon(IconGlyph::MoreHorizontal)
                    .appearance(sui::ButtonAppearance::Ghost)
                    .enabled_when({
                        let shared = shared.clone();
                        move || !shared.borrow().updates.view.busy
                    })
                    .theme_when(move || nav_theme.theme()),
            )
            .items_when(|| {
                Tab::ALL
                    .into_iter()
                    .map(|tab| MenuItem::new(tab.title()))
                    .collect()
            })
            .activation_button(PointerButton::Primary)
            .anchor_to_pointer(false)
            .theme_when({
                let shared = shared.clone();
                move || shared.theme()
            })
            .on_activate_path_with_ctx(move |ctx, path, _| {
                if let [index] = path.as_slice()
                    && let Some(tab) = Tab::ALL.get(*index)
                {
                    destinations.set_active_tab(*tab);
                }
                relayout(ctx);
            });
            pinned = pinned.with_child(navigation);
        } else {
            for tab in Tab::ALL {
                let control = super::pinned_tab::PinnedTab::new(shared.clone(), tab, compact);
                #[cfg(target_os = "android")]
                {
                    pinned = pinned.with_child(control);
                }
                #[cfg(not(target_os = "android"))]
                {
                    pinned = pinned.with_child(
                        sui::Tooltip::new(tab.title(), control)
                            .placement(sui::TooltipPlacement::Below)
                            .theme(shared.theme()),
                    );
                }
            }
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
            tab_width_limit,
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
        // The horizontal scroller measures its content without a width bound.
        // Cap each tab to the actual viewport so revealing Close cannot scroll
        // the beginning of the selected document's name off the screen.
        let limit = (self.scroll.viewport_size().width - 8.0).clamp(0.0, 220.0);
        if self.tab_width_limit.replace(limit) != limit {
            self.child.measure(ctx, constraints);
        }
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
    tabs: Option<BrowserTabBar>,
    selected_bounds: Rc<Cell<Option<Rect>>>,
    width_limit: Rc<Cell<f32>>,
}

impl Widget for MeasuredTabs {
    fn event(&mut self, ctx: &mut EventCtx, event: &sui::Event) {
        self.tabs.as_mut().unwrap().event(ctx, event);
    }
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        // SUI 0.6 exposes this setting through its consuming builder; taking
        // and returning the same widget preserves its interaction state.
        self.tabs = self
            .tabs
            .take()
            .map(|tabs| tabs.max_tab_width(self.width_limit.get()));
        let tabs = self.tabs.as_mut().unwrap();
        let size = tabs.measure(ctx, constraints);
        self.selected_bounds.set(tabs.selected_tab_bounds());
        size
    }
    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        self.tabs.as_mut().unwrap().arrange(ctx, bounds);
    }
    fn paint(&self, ctx: &mut PaintCtx) {
        self.tabs.as_ref().unwrap().paint(ctx);
    }
    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.tabs.as_ref().unwrap().semantics(ctx);
    }
    fn accepts_focus(&self) -> bool {
        self.tabs.as_ref().unwrap().accepts_focus()
    }
    fn focus_changed(&mut self, ctx: &mut EventCtx, focused: bool) {
        self.tabs.as_mut().unwrap().focus_changed(ctx, focused);
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
    fn mobile_p2_document_tab_fits_viewport_with_extra_large_text() -> sui::Result<()> {
        let root = sinomo_testdir::TestDir::new("app-mobile-p2-tabs");
        let backend = Backend::local_fallback_for_data_dir(
            sinomo_runtime::local::SinomoDataDir::new(root.to_path_buf()),
        );
        let shared = Shared::new(AppState::ready(backend));
        shared.set_text_scale(crate::theme::AppTextScale::ExtraLarge);
        let name = "mobile-smoke-with-a-long-document-name.txt";
        shared
            .open_document_tab(
                super::files::FilesSourceKind::Sifs,
                format!("/{name}"),
                name.into(),
            )
            .unwrap();
        let app = sui_testing::TestApp::from_runtime(crate::build_runtime(shared.clone())?)?;
        let window = app.main_window()?;
        for width in [380.0, 320.0, 520.0, 380.0] {
            window
                .root()
                .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(width, 780.0))))?;
            window.run_until_idle()?;
            let snapshot = window.snapshot()?;
            let viewport = snapshot
                .accessibility
                .nodes
                .iter()
                .find(|node| node.name.as_deref() == Some("Primary navigation tabs"))
                .unwrap()
                .bounds;
            let tab = snapshot
                .accessibility
                .nodes
                .iter()
                .find(|node| {
                    node.role == SemanticsRole::Button && node.name.as_deref() == Some(name)
                })
                .unwrap()
                .bounds;
            let close = snapshot
                .accessibility
                .nodes
                .iter()
                .find(|node| node.name.as_deref() == Some(&format!("Close {name} tab")))
                .unwrap()
                .bounds;
            assert!(
                tab.x() >= viewport.x() - 0.5 && tab.max_x() <= viewport.max_x() + 0.5,
                "both edges of the selected tab must fit at {width}: tab={tab:?}, viewport={viewport:?}"
            );
            assert!(close.x() >= viewport.x() && close.max_x() <= viewport.max_x());
        }
        Ok(())
    }

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
                        || matches!(node.name.as_deref(), Some("New tab" | "Navigate"))
                })
                .map(|node| (node.name.clone(), node.bounds))
                .collect();
            let compact = width <= 620.0;
            assert_eq!(controls.len(), if compact { 2 } else { 6 });
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
            if compact {
                window
                    .get_by_role(SemanticsRole::Button)
                    .with_name("Navigate")
                    .click()?;
                window
                    .get_by_role(SemanticsRole::MenuItem)
                    .with_name("Settings")
                    .click()?;
            } else {
                window
                    .get_by_role(SemanticsRole::Button)
                    .with_name("Settings")
                    .click()?;
            }
            assert_eq!(shared.borrow().active_workspace_tab, None);
            assert_eq!(shared.active_tab(), Tab::Settings);
            let selected = window.snapshot()?;
            if !compact {
                assert!(selected.accessibility.nodes.iter().any(
                    |node| node.description.as_deref() == Some("Pinned Settings tab")
                        && node.state.selected
                ));
            }
        }
        Ok(())
    }
}
