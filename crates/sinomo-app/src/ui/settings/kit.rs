//! Shared building blocks for settings pages.
//!
//! Every page uses the same frame (a left-aligned column holding the page header and its cards),
//! the same card, and one row pattern: a label with optional help text on the left and the
//! control, value, or status on the right. Technical detail that operators need but most people
//! do not — identifiers, schema names, raw flags — goes behind a [`details`] disclosure whose open
//! state lives in [`SettingsState`], so it survives page rebuilds.

use super::*;
use crate::ui::widgets::Boxed;

/// Widest a settings page column grows; wide list-and-detail pages opt out.
pub(super) const PAGE_MAX_W: f32 = 800.0;
/// Below this width a row stacks its control under its label.
const ROW_STACK_BELOW_W: f32 = 520.0;
const ROW_GAP: f32 = 16.0;
const ROW_STACK_GAP: f32 = 6.0;
const ROW_LABEL_MIN_W: f32 = 160.0;
const ROW_LABEL_SHARE: f32 = 0.45;
const CARD_ROW_GAP: f32 = 14.0;
const CARD_PAD: f32 = 18.0;
const COPY_FIELD_W: f32 = 260.0;

/// The page header: the section title and its one-line description.
pub(super) fn page_header(shared: &Shared, section: SettingsSection) -> impl Widget + use<> {
    let theme = shared.theme();
    let mut title_style = settings_text_style(shared, theme.text.xl);
    title_style.weight = sui::FontWeight::SEMIBOLD;
    let title_color = shared.clone();
    let header = Stack::vertical()
        .gap(4.0)
        .alignment(Alignment::Stretch)
        .with_child(
            Label::new(section.title())
                .text_style(title_style)
                .color_when(move || palette(&title_color).text),
        )
        .with_child(muted_label_with_token(
            shared,
            section.description(),
            theme.text.sm,
        ));
    SemanticRegion::new(section.title(), header)
        .description(section.description())
        .role(SemanticsRole::Heading)
}

/// A titled card that fills the page column.
pub(super) struct SettingsCard {
    title: String,
    description: Option<String>,
    header_action: Option<Boxed>,
    rows: Vec<Boxed>,
}

impl SettingsCard {
    pub(super) fn new(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            description: None,
            header_action: None,
            rows: Vec::new(),
        }
    }

    pub(super) fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// A control beside the title, such as an Add button or an Enabled switch.
    pub(super) fn header_action(mut self, action: impl Widget + 'static) -> Self {
        self.header_action = Some(Boxed::new(action));
        self
    }

    pub(super) fn row(mut self, row: impl Widget + 'static) -> Self {
        self.rows.push(Boxed::new(row));
        self
    }

    pub(super) fn build(self, shared: &Shared, reader: &ThemeReader) -> impl Widget + use<> {
        let theme = shared.theme();
        let title_style = {
            let mut style = settings_text_style(shared, theme.text.base);
            style.weight = sui::FontWeight::SEMIBOLD;
            style
        };
        let title = {
            let s = shared.clone();
            Label::new(self.title.clone())
                .text_style(title_style)
                .color_when(move || palette(&s).text)
        };
        let mut header = Stack::vertical()
            .gap(2.0)
            .alignment(Alignment::Stretch)
            .with_child(title);
        if let Some(description) = self.description {
            header = header.with_child(muted_label_with_token(shared, description, theme.text.sm));
        }
        let header = match self.header_action {
            Some(action) => Boxed::new(RowLayout::new(header, action)),
            None => Boxed::new(header),
        };
        let mut body = Stack::vertical()
            .gap(CARD_ROW_GAP)
            .alignment(Alignment::Stretch)
            .with_child(header);
        for row in self.rows {
            body = body.with_child(row);
        }
        SemanticRegion::new(
            self.title,
            Surface::panel(body)
                .padding(sui::Insets::all(CARD_PAD))
                .fill_width(true)
                .theme_when(clone_reader(reader)),
        )
    }
}

/// One setting: a label with optional help text, and the control or value it describes.
pub(super) struct SettingRow {
    label: String,
    help: Option<String>,
    help_widget: Option<Boxed>,
    control: Option<Boxed>,
}

impl SettingRow {
    pub(super) fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            help: None,
            help_widget: None,
            control: None,
        }
    }

    pub(super) fn help(mut self, help: impl Into<String>) -> Self {
        self.help = Some(help.into());
        self
    }

    /// Help that changes while editing, such as validation feedback; shown after any help text.
    pub(super) fn help_widget(mut self, help: impl Widget + 'static) -> Self {
        self.help_widget = Some(Boxed::new(help));
        self
    }

    pub(super) fn control(mut self, control: impl Widget + 'static) -> Self {
        self.control = Some(Boxed::new(control));
        self
    }

    /// A read-only value, right-aligned and wrapped, in the label's text size.
    pub(super) fn value(self, shared: &Shared, value: impl Into<String>) -> Self {
        let control = value_text(shared, value);
        self.control(control)
    }

    /// A value paired with a status chip, such as "Connected" or "Disabled".
    pub(super) fn status(
        self,
        reader: &ThemeReader,
        label: impl Into<String>,
        tone: SemanticTone,
    ) -> Self {
        let chip = StatusBadge::new(label)
            .tone(tone)
            .theme_when(clone_reader(reader));
        self.control(chip)
    }

    pub(super) fn build(self, shared: &Shared) -> impl Widget + use<> {
        let theme = shared.theme();
        let label_style = {
            let mut style = settings_text_style(shared, theme.text.base);
            style.color = palette(shared).text;
            style
        };
        let label_color = shared.clone();
        let mut text = Stack::vertical()
            .gap(2.0)
            .alignment(Alignment::Stretch)
            .with_child(
                Label::new(self.label.clone())
                    .text_style(label_style)
                    .color_when(move || palette(&label_color).text),
            );
        if let Some(help) = self.help.filter(|help| !help.trim().is_empty()) {
            text = text.with_child(muted_label_with_token(shared, help, theme.text.sm));
        }
        if let Some(help) = self.help_widget {
            text = text.with_child(help);
        }
        let control = self.control.unwrap_or_else(|| Boxed::new(SizedBox::new()));
        SemanticRegion::new(self.label, RowLayout::new(text, control))
    }
}

/// Right-aligned, wrapped value text that tracks the theme.
pub(super) fn value_text(shared: &Shared, value: impl Into<String>) -> RichText {
    let value = value.into();
    let shared = shared.clone();
    let document = move || {
        let mut style = settings_text_style(&shared, shared.theme().text.base);
        style.color = palette(&shared).text_muted;
        let mut document = TextDocument::from_plain_text(value.clone(), style);
        for paragraph in &mut document.paragraphs {
            paragraph.style.align = sui::TextAlign::Right;
        }
        document
    };
    RichText::new(document()).document_when(document)
}

/// A copyable identifier or path: a read-only field with a Copy button.
pub(super) fn copy_control(
    reader: &ThemeReader,
    name: &str,
    label: &str,
    value: String,
) -> impl Widget + use<> {
    let selection = SelectionScope::new();
    let field = TextInput::new(name)
        .value(value.clone())
        .read_only(true)
        .min_width(COPY_FIELD_W)
        .selection_scope(selection.clone())
        .copy_to_clipboard(true)
        .theme_when(clone_reader(reader));
    let field = read_only_text_menu(reader, name, field, selection, !value.is_empty());
    let copy = sui::Button::new("Copy")
        .semantic_name(format!("Copy {label}"))
        .description(format!("Copy the full {label} to the clipboard"))
        .icon(IconGlyph::Restore)
        .theme_when(clone_reader(reader))
        .appearance(sui::ButtonAppearance::Tonal)
        .tone(SemanticTone::Neutral)
        .on_press_with_ctx(move |ctx| ctx.set_clipboard_text(&value));
    Flex::horizontal()
        .gap(8.0)
        .align_items(Alignment::Center)
        .with_item(
            field,
            FlexItem::fixed(COPY_FIELD_W).shrink(1.0).min_width(120.0),
        )
        .with_item(copy, FlexItem::new().no_shrink())
}

/// A switch control for a setting row; the row shows the label, so the switch carries it only
/// as its accessible name.
pub(super) fn setting_switch(
    reader: &ThemeReader,
    name: &str,
    checked: bool,
    on_change: impl FnMut(bool) + 'static,
) -> Switch {
    Switch::new("")
        .semantic_name(name)
        .checked(checked)
        .theme_when(clone_reader(reader))
        .on_change(on_change)
}

/// The chevron button that opens or closes a disclosure. `on_toggle` flips the owning state.
pub(super) fn disclosure_toggle(
    reader: &ThemeReader,
    open: bool,
    show_label: &str,
    hide_label: &str,
    on_toggle: impl Fn() + 'static,
) -> sui::Button {
    sui::Button::new(if open { hide_label } else { show_label })
        .icon(if open {
            IconGlyph::ChevronUp
        } else {
            IconGlyph::ChevronDown
        })
        .appearance(sui::ButtonAppearance::Ghost)
        .tone(SemanticTone::Neutral)
        .theme_when(clone_reader(reader))
        .on_press_with_ctx(move |ctx| {
            on_toggle();
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_paint();
            ctx.request_semantics();
        })
}

/// Technical rows behind a "Show …" toggle. `id` names the disclosure; its open state is kept in
/// [`SettingsState`] so a page rebuild does not collapse it. `subject` says what the toggle reveals
/// ("Show identifiers", "Hide setting key"), so several disclosures on one card stay distinct.
/// The toggle's accessible description is [`details_toggle_description`] of the id.
pub(super) fn details(
    shared: &Shared,
    reader: &ThemeReader,
    id: &str,
    subject: &str,
    rows: Vec<Boxed>,
) -> impl Widget + use<> {
    let id: Rc<str> = Rc::from(id);
    let subject: Rc<str> = Rc::from(subject);
    let toggle = |open: bool| {
        let s = shared.clone();
        let id = Rc::clone(&id);
        sui::Button::new(if open {
            format!("Hide {subject}")
        } else {
            format!("Show {subject}")
        })
        // The visible label is the accessible name; the id rides in the description so tests and
        // automation can address one disclosure among several with the same subject.
        .description(details_toggle_description(&id))
        .icon(if open {
            IconGlyph::ChevronUp
        } else {
            IconGlyph::ChevronDown
        })
        .appearance(sui::ButtonAppearance::Ghost)
        .tone(SemanticTone::Neutral)
        .theme_when(clone_reader(reader))
        .on_press_with_ctx(move |ctx| {
            s.settings_mut().toggle_details(&id);
            ctx.request_measure();
            ctx.request_arrange();
            ctx.request_paint();
            ctx.request_semantics();
        })
    };
    let mut open_body = Stack::vertical()
        .gap(CARD_ROW_GAP)
        .alignment(Alignment::Stretch)
        .with_child(Align::new(
            Alignment::Start,
            Alignment::Center,
            toggle(true),
        ));
    for row in rows {
        open_body = open_body.with_child(row);
    }
    let selected = {
        let s = shared.clone();
        let id = Rc::clone(&id);
        move || usize::from(s.settings().details_open(&id))
    };
    SwitchView::new()
        .selected_when(selected)
        .with_child(Align::new(
            Alignment::Start,
            Alignment::Center,
            toggle(false),
        ))
        .with_child(open_body)
}

/// The accessible description of the toggle for disclosure `id`.
pub(super) fn details_toggle_description(id: &str) -> String {
    format!("Details · {id}")
}

impl SettingsState {
    pub(super) fn details_open(&self, id: &str) -> bool {
        self.details_open.contains(id)
    }

    pub(super) fn toggle_details(&mut self, id: &str) {
        if !self.details_open.remove(id) {
            self.details_open.insert(id.to_string());
        }
    }
}

/// Label column on the left, control on the right; stacked when narrow. The control is measured
/// first so short controls (switches, chips) leave the label more room.
struct RowLayout {
    text: WidgetPod,
    control: WidgetPod,
    stacked: bool,
}

impl RowLayout {
    fn new(text: impl Widget + 'static, control: Boxed) -> Self {
        Self {
            text: WidgetPod::new(text),
            control: WidgetPod::new(control),
            stacked: false,
        }
    }
}

impl Widget for RowLayout {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let width = if constraints.max.width.is_finite() {
            constraints.max.width
        } else {
            PAGE_MAX_W
        };
        let loose = |max_width: f32| {
            Constraints::new(Size::ZERO, Size::new(max_width.max(0.0), f32::INFINITY))
        };
        self.stacked = width < ROW_STACK_BELOW_W;
        let height = if self.stacked {
            let text = self.text.measure(ctx, loose(width));
            let control = self.control.measure(ctx, loose(width));
            text.height + ROW_STACK_GAP + control.height
        } else {
            let label_w = (width * ROW_LABEL_SHARE).max(ROW_LABEL_MIN_W);
            let control = self.control.measure(ctx, loose(width - label_w - ROW_GAP));
            let text = self
                .text
                .measure(ctx, loose(width - control.width - ROW_GAP));
            text.height.max(control.height)
        };
        constraints.clamp(Size::new(width, height))
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        let text = self.text.measured_size();
        let control = self.control.measured_size();
        if self.stacked {
            self.text.arrange(
                ctx,
                Rect::new(bounds.x(), bounds.y(), bounds.width(), text.height),
            );
            self.control.arrange(
                ctx,
                Rect::new(
                    bounds.x(),
                    bounds.y() + text.height + ROW_STACK_GAP,
                    control.width,
                    control.height,
                ),
            );
            return;
        }
        let height = bounds.height();
        self.text.arrange(
            ctx,
            Rect::new(
                bounds.x(),
                bounds.y() + (height - text.height) * 0.5,
                (bounds.width() - control.width - ROW_GAP).max(0.0),
                text.height,
            ),
        );
        self.control.arrange(
            ctx,
            Rect::new(
                bounds.max_x() - control.width,
                bounds.y() + (height - control.height) * 0.5,
                control.width,
                control.height,
            ),
        );
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.text.paint(ctx);
        self.control.paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.text.semantics(ctx);
        self.control.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.text);
        visitor.visit(&self.control);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.text);
        visitor.visit(&mut self.control);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Backend;
    use crate::state::AppState;

    fn node_bounds(snapshot: &sui_testing::WindowSnapshot, name: &str) -> Rect {
        snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| node.name.as_deref() == Some(name))
            .unwrap_or_else(|| panic!("{name:?} should be present"))
            .bounds
    }

    fn settings_app(shared: &Shared, width: f32) -> sui::Result<sui_testing::TestApp> {
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings kit")
                    .root(view(shared.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        app.main_window()?
            .root()
            .dispatch_event(sui::Event::Window(sui::WindowEvent::Resized(Size::new(
                width, 900.0,
            ))))?;
        app.run_until_idle()?;
        Ok(app)
    }

    fn appearance_shared() -> Shared {
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        {
            let mut app = shared.borrow_mut();
            app.active_tab = Tab::Settings;
            app.ready
                .as_mut()
                .expect("ready state")
                .settings
                .open(SettingsSection::Appearance);
        }
        shared
    }

    #[test]
    fn page_header_and_cards_share_one_left_aligned_column() -> sui::Result<()> {
        let shared = appearance_shared();
        let app = settings_app(&shared, 1440.0)?;
        let snapshot = app.main_window()?.snapshot()?;
        let header = snapshot
            .accessibility
            .nodes
            .iter()
            .find(|node| {
                node.role == SemanticsRole::Heading
                    && node.name.as_deref() == Some(SettingsSection::Appearance.title())
            })
            .expect("page heading")
            .bounds;
        let card = node_bounds(&snapshot, "Theme and text");
        assert!(
            (header.x() - card.x()).abs() <= 1.0,
            "the title {header:?} and the first card {card:?} start at the same edge"
        );
        assert!(
            card.width() <= PAGE_MAX_W + 0.5,
            "cards stay in the reading column, not the full window: {card:?}"
        );
        Ok(())
    }

    #[test]
    fn rows_put_the_control_beside_the_label_and_stack_when_narrow() -> sui::Result<()> {
        let shared = appearance_shared();
        let app = settings_app(&shared, 1440.0)?;
        let snapshot = app.main_window()?.snapshot()?;
        let label = node_bounds(&snapshot, "Text size");
        let control = node_bounds(&snapshot, "text-scale");
        assert!(
            control.x() > label.x() + 100.0 && control.y() < label.max_y(),
            "wide rows place the control on the right: label {label:?}, control {control:?}"
        );

        // The stacked layout engages below ROW_STACK_BELOW_W of row width.
        let shared = appearance_shared();
        let app = settings_app(&shared, 460.0)?;
        let snapshot = app.main_window()?.snapshot()?;
        let label = node_bounds(&snapshot, "Text size");
        let control = node_bounds(&snapshot, "text-scale");
        assert!(
            control.y() >= label.y() + 16.0,
            "narrow rows put the control under the label: label {label:?}, control {control:?}"
        );
        Ok(())
    }

    fn kit_app(root: impl Widget + 'static, width: f32) -> sui::Result<sui_testing::TestApp> {
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Settings kit widgets")
                    .root(SizedBox::new().size(Size::new(width, 400.0)).child(root)),
            )
            .build()?;
        sui_testing::TestApp::from_runtime(runtime)
    }

    #[test]
    fn form_rows_lay_out_inline_or_stacked_in_the_settings_style() -> sui::Result<()> {
        let shared = appearance_shared();
        let reader: ThemeReader = {
            let shared = shared.clone();
            Rc::new(move || shared.theme())
        };
        let rows = Stack::vertical()
            .alignment(Alignment::Stretch)
            .with_child(
                FormRow::new("Inline field", TextInput::new("inline-field"))
                    .theme_when(clone_reader(&reader)),
            )
            .with_child(
                FormRow::new("Stacked field", TextInput::new("stacked-field"))
                    .stacked()
                    .theme_when(clone_reader(&reader)),
            );
        let app = kit_app(rows, 760.0)?;
        let snapshot = app.main_window()?.snapshot()?;
        let label = node_bounds(&snapshot, "Inline field");
        let input = node_bounds(&snapshot, "inline-field");
        assert!(
            input.x() > label.x() + 100.0 && input.y() < label.max_y(),
            "an inline row puts its control beside the label: {label:?} {input:?}"
        );
        let label = node_bounds(&snapshot, "Stacked field");
        let input = node_bounds(&snapshot, "stacked-field");
        assert!(
            input.y() > label.y() + 8.0 && (input.x() - label.x()).abs() <= 1.0,
            "a stacked row puts its control under the label: {label:?} {input:?}"
        );
        Ok(())
    }

    #[test]
    fn disclosures_name_what_they_reveal() -> sui::Result<()> {
        let shared = appearance_shared();
        let reader: ThemeReader = {
            let shared = shared.clone();
            Rc::new(move || shared.theme())
        };
        let rows = Stack::vertical()
            .alignment(Alignment::Stretch)
            .with_child(details(
                &shared,
                &reader,
                "kit-a",
                "setting key",
                Vec::new(),
            ))
            .with_child(details(
                &shared,
                &reader,
                "kit-b",
                "module ID and backend",
                Vec::new(),
            ));
        let app = kit_app(rows, 760.0)?;
        let window = app.main_window()?;
        window
            .get_by_text("Show setting key")
            .expect()
            .to_be_visible()?;
        window
            .get_by_text("Show module ID and backend")
            .expect()
            .to_be_visible()?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_description(details_toggle_description("kit-a"))
            .click()?;
        app.run_until_idle()?;
        window
            .get_by_text("Hide setting key")
            .expect()
            .to_be_visible()?;
        assert!(shared.settings().details_open("kit-a"));
        assert!(!shared.settings().details_open("kit-b"));
        Ok(())
    }

    #[test]
    fn details_stay_open_across_page_rebuilds() {
        let shared = appearance_shared();
        assert!(!shared.settings().details_open("about-runtime"));
        shared.settings_mut().toggle_details("about-runtime");
        assert!(shared.settings().details_open("about-runtime"));
        // A rebuild reads the state again rather than resetting a widget-local flag.
        shared.publish_settings_signals();
        assert!(shared.settings().details_open("about-runtime"));
        shared.settings_mut().toggle_details("about-runtime");
        assert!(!shared.settings().details_open("about-runtime"));
    }
}

/// The settings row for code that builds rows from a label and a control, with the same builder
/// calls as SUI's `FormRow`: an inline row lays out like [`SettingRow`], and a stacked row puts
/// the label above a full-width control (text areas, long editors). The row is built on first
/// layout, after `theme_when` has supplied the theme.
pub(super) struct FormRow {
    label: String,
    control: Option<WidgetPod>,
    stacked: bool,
    reader: Option<ThemeReader>,
    built: Option<WidgetPod>,
}

impl FormRow {
    pub(super) fn new(label: impl Into<String>, control: impl Widget + 'static) -> Self {
        Self {
            label: label.into(),
            control: Some(WidgetPod::new(control)),
            stacked: false,
            reader: None,
            built: None,
        }
    }

    pub(super) fn stacked(mut self) -> Self {
        self.stacked = true;
        self
    }

    pub(super) fn inline(mut self) -> Self {
        self.stacked = false;
        self
    }

    pub(super) fn theme_when<F>(mut self, theme: F) -> Self
    where
        F: Fn() -> DefaultTheme + 'static,
    {
        self.reader = Some(Rc::new(theme));
        self
    }

    fn build(&mut self) -> WidgetPod {
        let reader: ThemeReader = self
            .reader
            .clone()
            .unwrap_or_else(|| Rc::new(DefaultTheme::default));
        let theme = reader();
        let style = sui::TextStyle {
            font_size: theme.text.base.size,
            line_height: theme.text.base.line_height,
            ..theme.body_text_style()
        };
        let label = {
            let reader = Rc::clone(&reader);
            Label::new(self.label.clone())
                .text_style(style)
                .color_when(move || reader().palette.text)
        };
        let control = self
            .control
            .take()
            .unwrap_or_else(|| WidgetPod::new(SizedBox::new()));
        let body = if self.stacked {
            Boxed::new(
                Stack::vertical()
                    .gap(ROW_STACK_GAP)
                    .alignment(Alignment::Stretch)
                    .with_child(label)
                    .with_child(PodWidget(control)),
            )
        } else {
            Boxed::new(RowLayout::new(label, Boxed::new(PodWidget(control))))
        };
        WidgetPod::new(SemanticRegion::new(self.label.clone(), body))
    }
}

impl Widget for FormRow {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        if self.built.is_none() {
            self.built = Some(self.build());
        }
        self.built
            .as_mut()
            .map_or(Size::ZERO, |built| built.measure(ctx, constraints))
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        if let Some(built) = &mut self.built {
            built.arrange(ctx, bounds);
        }
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        if let Some(built) = &self.built {
            built.paint(ctx);
        }
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        if let Some(built) = &self.built {
            built.semantics(ctx);
        }
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        if let Some(built) = &self.built {
            visitor.visit(built);
        }
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        if let Some(built) = &mut self.built {
            visitor.visit(built);
        }
    }
}

/// Hosts an already-built pod as a widget.
struct PodWidget(WidgetPod);

impl Widget for PodWidget {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        self.0.measure(ctx, constraints)
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        self.0.arrange(ctx, bounds);
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.0.paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.0.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.0);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.0);
    }
}
