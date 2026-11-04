//! Generic conversation panel slot. Discovery and module rendering run off the
//! UI thread. The host owns navigation and discards providers on withdrawal.
use super::module_widget::{ModuleWidget, ModuleWidgetSession};
use crate::state::Shared;
use sinomo_runtime::{
    module::{UiExtensionCatalogEntry, UiExtensionCatalogResponse},
    module_api::{UiContributionPayload, UiWidgetSizing, decode_sobj, panels::*},
};
use std::{
    cell::Cell,
    rc::Rc,
    sync::mpsc::{self, Receiver},
    time::{Duration, Instant},
};
use sui::{
    Alignment, ArrangeCtx, Button, Constraints, DefaultTheme, Event, EventCtx, Flex, Label,
    MeasureCtx, PaintCtx, Rect, SemanticsCtx, Size, Widget, WidgetPod, WidgetPodMutVisitor,
    WidgetPodVisitor,
};

pub(crate) struct ConversationPanels {
    shared: Shared,
    theme: Rc<dyn Fn() -> DefaultTheme>,
    context: (String, Option<String>),
    rx: Option<Receiver<Result<UiExtensionCatalogResponse, String>>>,
    entries: Vec<UiExtensionCatalogEntry>,
    selected: Rc<Cell<Option<usize>>>,
    built: Option<usize>,
    pod: WidgetPod,
    generation: u64,
    next_discovery: Instant,
    error: Option<String>,
    panel_session: Option<(String, ModuleWidgetSession)>,
}
impl ConversationPanels {
    pub(crate) fn new(shared: Shared, theme: Rc<dyn Fn() -> DefaultTheme>) -> Self {
        Self {
            shared,
            theme,
            context: (String::new(), None),
            rx: None,
            entries: vec![],
            selected: Rc::new(Cell::new(None)),
            built: None,
            pod: WidgetPod::new(Label::new("")),
            generation: 0,
            next_discovery: Instant::now(),
            error: None,
            panel_session: None,
        }
    }
    fn sync(&mut self) -> bool {
        let (id, address, waker) = super::chat::module_panel_context(&self.shared);
        let mut changed = false;
        if self.context != (id.clone(), address.clone()) {
            self.context = (id, address);
            self.rx = None;
            self.entries.clear();
            self.selected.set(None);
            self.built = None;
            self.error = None;
            self.panel_session = None;
            self.next_discovery = Instant::now();
            changed = true;
        }
        if let Some(result) = self.rx.as_ref().and_then(|rx| rx.try_recv().ok()) {
            self.next_discovery = Instant::now() + Duration::from_secs(2);
            match result {
                Ok(catalog) => {
                    let entries:Vec<_>=catalog.entries.into_iter().filter(|e|matches!(&e.contribution.payload,UiContributionPayload::Declarative{schema_id,..} if schema_id==PANEL_SCHEMA)).collect();
                    if catalog.registry_generation != self.generation || self.entries != entries {
                        self.generation = catalog.registry_generation;
                        self.entries = entries;
                        self.selected.set(None);
                        self.built = None;
                        changed = true;
                    }
                    changed |= self.error.take().is_some();
                }
                Err(error) => {
                    changed |= self.error.as_ref() != Some(&error);
                    self.error = Some(error);
                }
            }
        }
        if self.rx.is_none()
            && Instant::now() >= self.next_discovery
            && let Some(address) = self.context.1.clone()
        {
            let (tx, rx) = mpsc::sync_channel(1);
            self.rx = Some(rx);
            if let Err(error) = std::thread::Builder::new()
                .name("conversation-panel-catalog".into())
                .spawn(move || {
                    loop {
                        let result = crate::backend::ui_extensions::ui_extension_catalog_at_address(
                            &address,
                            Some(CONVERSATION_PANELS),
                        );
                        match tx.try_send(result) {
                            Ok(()) => {
                                if let Some(waker) = &waker {
                                    waker.wake();
                                }
                            }
                            Err(mpsc::TrySendError::Full(_)) => {}
                            Err(mpsc::TrySendError::Disconnected(_)) => break,
                        }
                        std::thread::sleep(Duration::from_secs(2));
                    }
                })
            {
                self.rx = None;
                self.next_discovery = Instant::now() + Duration::from_secs(2);
                self.error = Some(error.to_string());
                changed = true;
            }
        }
        if self.selected.get() != self.built {
            changed = true;
        }
        if changed {
            self.rebuild();
        }
        changed
    }
    fn rebuild(&mut self) {
        self.built = self.selected.get();
        let mut header = Flex::horizontal().gap(6.);
        for (index, entry) in self.entries.iter().enumerate() {
            let UiContributionPayload::Declarative { payload_sobj, .. } =
                &entry.contribution.payload
            else {
                continue;
            };
            let Ok(definition) = decode_sobj::<SessionPanelDefinition>(payload_sobj) else {
                continue;
            };
            let selected = self.selected.clone();
            header = header.with_child(
                Button::new(definition.label)
                    .theme_when({
                        let theme = self.theme.clone();
                        move || theme()
                    })
                    .on_press_with_ctx(move |ctx| {
                        selected.set(if selected.get() == Some(index) {
                            None
                        } else {
                            Some(index)
                        });
                        ctx.request_measure();
                        ctx.request_arrange();
                        ctx.request_paint();
                    }),
            );
        }
        let mut body = Flex::vertical()
            .align_items(Alignment::Stretch)
            .with_child(header);
        if let Some(error) = &self.error
            && !self.entries.is_empty()
        {
            body = body.with_child(Label::new(format!("Panel catalog unavailable: {error}")));
        }
        if let Some(entry) = self.built.and_then(|i| self.entries.get(i))
            && let Some(address) = self.context.1.clone()
        {
            let (_, _, waker) = super::chat::module_panel_context(&self.shared);
            let key = format!(
                "{}:{}:{}",
                entry.module_id, entry.contribution.contribution_id, self.generation
            );
            let session = if let Some((existing, session)) = &self.panel_session
                && existing == &key
            {
                Ok(session.clone())
            } else {
                ModuleWidgetSession::start_panel(
                    address,
                    self.context.0.clone(),
                    entry.clone(),
                    self.generation,
                    (self.theme)(),
                    waker,
                )
                .inspect(|session| self.panel_session = Some((key, session.clone())))
            };
            match session {
                Ok(session) => {
                    body = body.with_child(ModuleWidget::new(
                        session,
                        UiWidgetSizing {
                            minimum_width: 64,
                            minimum_height: 160,
                            preferred_width: 960,
                            preferred_height: 300,
                        },
                        self.theme.clone(),
                    ))
                }
                Err(error) => body = body.with_child(Label::new(error)),
            }
        }
        if self.built.is_none() {
            self.panel_session = None;
        }
        self.pod = WidgetPod::new(body);
    }
}
impl Widget for ConversationPanels {
    fn event(&mut self, ctx: &mut EventCtx, _: &Event) {
        if self.sync() {
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_paint();
            ctx.request_semantics();
        }
    }
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        self.sync();
        if self.entries.is_empty() {
            return Size::ZERO;
        }
        self.pod.measure(
            ctx,
            Constraints::new(
                Size::ZERO,
                Size::new(
                    constraints.max.width,
                    constraints
                        .max
                        .height
                        .min(if self.selected.get().is_some() {
                            340.
                        } else {
                            36.
                        }),
                ),
            ),
        )
    }
    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        self.pod.arrange(ctx, bounds);
    }
    fn paint(&self, ctx: &mut PaintCtx) {
        if !self.entries.is_empty() {
            self.pod.paint(ctx);
        }
    }
    fn semantics(&self, ctx: &mut SemanticsCtx) {
        if !self.entries.is_empty() {
            self.pod.semantics(ctx);
        }
    }
    fn visit_children(&self, v: &mut dyn WidgetPodVisitor) {
        if !self.entries.is_empty() {
            v.visit(&self.pod);
        }
    }
    fn visit_children_mut(&mut self, v: &mut dyn WidgetPodMutVisitor) {
        if !self.entries.is_empty() {
            v.visit(&mut self.pod);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{backend::Backend, state::AppState};
    use sinomo_runtime::module_api::*;
    #[test]
    fn panel_slot_selects_module_content_and_withdraws_on_generation_change() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        let mut view = ConversationPanels::new(shared.clone(), Rc::new(DefaultTheme::dark));
        let (id, address, _) = super::super::chat::module_panel_context(&shared);
        view.context = (id, address);
        view.entries = vec![UiExtensionCatalogEntry {
            module_id: "module.test".into(),
            module_version: "1.0.0".into(),
            module_backend: "native-dylib".into(),
            component_path: None,
            contribution: UiExtensionContribution {
                contribution_id: "module.test.panel".into(),
                extension_point_id: CONVERSATION_PANELS.into(),
                extension_point_version: "1.0.0".into(),
                group: String::new(),
                order: 0,
                before: vec![],
                after: vec![],
                when: vec![],
                payload: UiContributionPayload::Declarative {
                    schema_id: PANEL_SCHEMA.into(),
                    payload_sobj: encode_sobj(&SessionPanelDefinition {
                        label: "Agent timeline".into(),
                        service_id: "test-panel".into(),
                    })
                    .unwrap(),
                },
            },
        }];
        let selection = view.selected.clone();
        view.rebuild();
        let (tx, rx) = mpsc::channel();
        view.rx = Some(rx);
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Module panel slot")
                    .root(view),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Agent timeline")
            .click()?;
        app.run_until_idle()?;
        assert_eq!(selection.get(), Some(0));
        tx.send(Ok(UiExtensionCatalogResponse {
            registry_generation: 2,
            entries: vec![],
            warnings: vec![],
        }))
        .unwrap();
        window
            .root()
            .dispatch_event(Event::Window(sui::WindowEvent::Resized(Size::new(
                800., 600.,
            ))))?;
        app.run_until_idle()?;
        assert_eq!(selection.get(), None);
        window
            .get_by_role(sui::SemanticsRole::Button)
            .with_name("Agent timeline")
            .expect()
            .to_be_hidden()?;
        Ok(())
    }
}
