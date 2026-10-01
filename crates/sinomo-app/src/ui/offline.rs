//! The shared "daemon offline" panel.
//!
//! Chat, SIFS files, cluster, and evolution all run through the daemon, so when it is unreachable
//! they show this one panel — the same icon, wording, and recovery actions — instead of each
//! surface rendering its own error. The panel follows [`DaemonAvailability`] reactively: the
//! reconnect button reports an in-flight reconnect and the detail line tracks the latest failure.

use std::rc::Rc;

use sui::{
    Align, Alignment, Button, ButtonAppearance, Constraints, DefaultTheme, Flex, FlexWrap,
    FontWeight, IconGlyph, MeasureCtx, PaintCtx, Paragraph, RebuildOnChange, SemanticTone,
    SemanticsCtx, SemanticsNode, SemanticsRole, Size, Stack, TextAlign, TextStyle, VerticalAlign,
    Widget, WidgetPod,
};
use sui_lucide::LucideIcon;

use crate::state::{DaemonAvailability, Shared, Tab};
use crate::ui::settings::SettingsSection;
use crate::ui::widgets::MaxWidth;

type ThemeReader = Rc<dyn Fn() -> DefaultTheme>;
type ActionFactory = Rc<dyn Fn() -> Button>;

/// Title shared by every surface, so the app names the problem the same way everywhere.
pub(crate) const DAEMON_OFFLINE_TITLE: &str = "Not connected to the Sinomo daemon";
pub(crate) const RECONNECT_LABEL: &str = "Reconnect";
pub(crate) const RECONNECTING_LABEL: &str = "Reconnecting…";
pub(crate) const CONNECTION_SETTINGS_LABEL: &str = "Connection settings";

const PANEL_MAX_WIDTH: f32 = 460.0;
const ICON_SIZE: f32 = 40.0;

/// Builder for a surface's offline panel. `summary` says what the surface needs the daemon for.
pub(crate) struct DaemonOfflinePanel {
    shared: Shared,
    reader: ThemeReader,
    name: String,
    summary: String,
    extra_actions: Vec<ActionFactory>,
}

impl DaemonOfflinePanel {
    pub(crate) fn new(
        shared: &Shared,
        reader: &ThemeReader,
        name: impl Into<String>,
        summary: impl Into<String>,
    ) -> Self {
        Self {
            shared: shared.clone(),
            reader: Rc::clone(reader),
            name: name.into(),
            summary: summary.into(),
            extra_actions: Vec::new(),
        }
    }

    /// Add a secondary action after the standard reconnect and settings buttons.
    pub(crate) fn action(mut self, action: impl Fn() -> Button + 'static) -> Self {
        self.extra_actions.push(Rc::new(action));
        self
    }

    pub(crate) fn build(self) -> impl Widget + use<> {
        let Self {
            shared,
            reader,
            name,
            summary,
            extra_actions,
        } = self;
        let source = shared.signals().daemon.clone();
        RebuildOnChange::key_from(source, move |daemon| {
            WidgetPod::new(panel_content(
                &shared,
                &reader,
                &name,
                &summary,
                &extra_actions,
                daemon,
            ))
        })
    }
}

/// Show `surface` while the daemon is available and `offline` while it is not. Both stay
/// mounted, so the surface keeps its widget state (scroll, focus, drafts) across a reconnect.
pub(crate) fn daemon_gate(
    shared: &Shared,
    surface: impl Widget + 'static,
    offline: impl Widget + 'static,
) -> sui::SwitchView {
    let offline_selected = shared
        .signals()
        .daemon
        .select_named("daemon gate", |daemon| usize::from(!daemon.is_available()));
    sui::SwitchView::new()
        .selected_from(offline_selected)
        .with_child(surface)
        .with_child(offline)
}

fn panel_content(
    shared: &Shared,
    reader: &ThemeReader,
    name: &str,
    summary: &str,
    extra_actions: &[ActionFactory],
    daemon: &DaemonAvailability,
) -> impl Widget + use<> {
    let (detail, reconnecting) = match daemon {
        DaemonAvailability::Available => (None, false),
        DaemonAvailability::Unavailable {
            detail,
            reconnecting,
        } => (detail.clone(), *reconnecting),
    };
    let theme = reader();

    let mut title_style = theme.text_style(theme.palette.text);
    title_style.font_size = theme.text.lg.size;
    title_style.line_height = theme.text.lg.line_height;
    title_style.weight = FontWeight::SEMIBOLD;
    let mut summary_style = theme.text_style(theme.palette.text_muted);
    summary_style.font_size = theme.text.sm.size;
    summary_style.line_height = theme.text.sm.line_height;
    let mut detail_style = summary_style.clone();
    detail_style.font_size = theme.text.xs.size;
    detail_style.line_height = theme.text.xs.line_height;

    let reconnect = {
        let shared = shared.clone();
        Button::new(if reconnecting {
            RECONNECTING_LABEL
        } else {
            RECONNECT_LABEL
        })
        .icon(IconGlyph::Redo)
        .primary_action()
        .enabled(!reconnecting)
        .theme_when(clone_reader(reader))
        .on_press(move || {
            shared.request_connection_probe(true);
        })
    };
    let settings = {
        let shared = shared.clone();
        Button::new(CONNECTION_SETTINGS_LABEL)
            .appearance(ButtonAppearance::Tonal)
            .tone(SemanticTone::Neutral)
            .theme_when(clone_reader(reader))
            .on_press(move || open_connection_settings(&shared))
    };
    let mut actions = Flex::horizontal()
        .wrap(FlexWrap::Wrap)
        .gap(8.0)
        .justify(sui::FlexJustify::Center)
        .align_items(Alignment::Center)
        .with_child(reconnect)
        .with_child(settings);
    for action in extra_actions {
        actions = actions.with_child(action().theme_when(clone_reader(reader)));
    }

    let mut column = Stack::vertical()
        .gap(8.0)
        .alignment(Alignment::Center)
        .with_child(OfflineIcon {
            color: theme.palette.text_muted,
        })
        .with_child(CenteredText::new(DAEMON_OFFLINE_TITLE, title_style))
        .with_child(CenteredText::new(summary, summary_style));
    if let Some(detail) = detail.filter(|detail| !detail.trim().is_empty()) {
        column = column.with_child(CenteredText::new(detail, detail_style));
    }
    column = column
        .with_child(sui::SizedBox::new().height(4.0))
        .with_child(actions);

    sui::SemanticRegion::new(name, Align::center(MaxWidth::new(PANEL_MAX_WIDTH, column)))
}

fn open_connection_settings(shared: &Shared) {
    if shared.is_ready() {
        shared.settings_mut().open(SettingsSection::Connection);
    }
    shared.set_active_tab(Tab::Settings);
}

fn clone_reader(reader: &ThemeReader) -> impl Fn() -> DefaultTheme + 'static {
    let reader = Rc::clone(reader);
    move || reader()
}

/// The cloud-off glyph, painted as vector paths so the panel needs no registered image resource.
struct OfflineIcon {
    color: sui::Color,
}

impl Widget for OfflineIcon {
    fn measure(&mut self, _ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        constraints.clamp(Size::new(ICON_SIZE, ICON_SIZE))
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        LucideIcon::CloudOff.paint(ctx, ctx.bounds(), self.color);
    }
}

/// Wrapped text with every line centered, as one accessible text node.
struct CenteredText {
    text: String,
    style: TextStyle,
    paragraph: Option<Paragraph>,
}

impl CenteredText {
    fn new(text: impl Into<String>, style: TextStyle) -> Self {
        Self {
            text: text.into(),
            style,
            paragraph: None,
        }
    }
}

impl Widget for CenteredText {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let paragraph = Paragraph::new(
            ctx,
            self.text.clone(),
            &self.style,
            TextAlign::Center,
            constraints.max.width,
        );
        let size = paragraph.size();
        self.paragraph = Some(paragraph);
        constraints.clamp(Size::new(size.width.ceil(), size.height.ceil()))
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        if let Some(paragraph) = &self.paragraph {
            paragraph.paint(ctx, ctx.bounds(), VerticalAlign::Top);
        }
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        let mut node = SemanticsNode::new(ctx.widget_id(), SemanticsRole::Text, ctx.bounds());
        node.name = Some(self.text.clone());
        ctx.push(node);
    }
}

/// A backend whose app-started daemon failed, the way the app ends up offline in production.
#[cfg(test)]
pub(crate) fn offline_test_backend() -> crate::backend::Backend {
    let mut backend = crate::backend::Backend::local_fallback();
    backend.connection = crate::backend::Connection::LocalFallback {
        reason: Some(OFFLINE_TEST_REASON.to_string()),
    };
    backend
}

#[cfg(test)]
pub(crate) const OFFLINE_TEST_REASON: &str = "daemon failed to start: port in use";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::AppState;

    fn panel_app(shared: &Shared) -> sui::Result<sui_testing::TestApp> {
        let reader: ThemeReader = {
            let shared = shared.clone();
            Rc::new(move || shared.theme())
        };
        let panel = DaemonOfflinePanel::new(
            shared,
            &reader,
            "Test offline",
            "This surface runs through the daemon.",
        )
        .action(|| Button::new("Extra action"))
        .build();
        let runtime = sui::Application::new()
            .window(sui::WindowBuilder::new().title("Offline panel").root(panel))
            .build()?;
        sui_testing::TestApp::from_runtime(runtime)
    }

    #[test]
    fn offline_panel_names_the_problem_and_offers_recovery() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(offline_test_backend()));
        let app = panel_app(&shared)?;
        let window = app.main_window()?;
        app.run_until_idle()?;

        for text in [
            DAEMON_OFFLINE_TITLE,
            "This surface runs through the daemon.",
            OFFLINE_TEST_REASON,
        ] {
            window
                .get_by_role(SemanticsRole::Text)
                .with_name(text)
                .expect()
                .to_be_visible()?;
        }
        for button in [RECONNECT_LABEL, CONNECTION_SETTINGS_LABEL, "Extra action"] {
            window
                .get_by_role(SemanticsRole::Button)
                .with_name(button)
                .expect()
                .to_be_visible()?;
        }
        Ok(())
    }

    #[test]
    fn offline_panel_reconnect_starts_a_probe_and_settings_opens_connectivity() -> sui::Result<()> {
        let shared = Shared::new(AppState::ready(offline_test_backend()));
        let app = panel_app(&shared)?;
        let window = app.main_window()?;
        app.run_until_idle()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name(RECONNECT_LABEL)
            .click()?;
        assert!(
            shared.connection_health().is_pending(),
            "reconnect requests a daemon probe"
        );
        assert_eq!(
            shared.daemon_availability(),
            DaemonAvailability::Unavailable {
                detail: Some(OFFLINE_TEST_REASON.to_string()),
                reconnecting: true,
            }
        );
        app.run_until_idle()?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name(RECONNECTING_LABEL)
            .expect()
            .to_be_visible()?;

        window
            .get_by_role(SemanticsRole::Button)
            .with_name(CONNECTION_SETTINGS_LABEL)
            .click()?;
        assert_eq!(shared.borrow().active_tab, Tab::Settings);
        assert_eq!(
            shared.settings().selected,
            Some(SettingsSection::Connection),
            "the settings action lands on Connectivity"
        );
        Ok(())
    }
}
