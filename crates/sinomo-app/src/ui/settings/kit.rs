//! Shared building blocks for settings pages.
//!
//! Every page uses the same frame (a left-aligned column holding the page header and its cards),
//! the same card, and one row pattern: a label with optional help text on the left and the
//! control, value, or status on the right. Technical detail that operators need but most people
//! do not — identifiers, schema names, raw flags — goes behind a [`details`] disclosure whose open
//! state lives in [`SettingsState`], so it survives page rebuilds.
//!
//! Spacing, padding, and the reading-column width come from the theme's form metrics and are read
//! on each layout, as are text styles, so appearance changes restyle retained widgets in place.

use super::*;
use crate::ui::widgets::{Boxed, Restyle};

/// Below this width a row stacks its control under its label.
const ROW_STACK_BELOW_W: f32 = 520.0;
const ROW_LABEL_MIN_W: f32 = 160.0;
const ROW_LABEL_SHARE: f32 = 0.45;

/// A theme reader over the shared app theme, for builders that only receive [`Shared`].
pub(super) fn shared_reader(shared: &Shared) -> ThemeReader {
    let shared = shared.clone();
    Rc::new(move || shared.theme())
}

/// The page header: the section title and its one-line description.
pub(super) fn page_header(shared: &Shared, section: SettingsSection) -> impl Widget + use<> {
    let reader = shared_reader(shared);
    let title = {
        let s = shared.clone();
        Label::new(section.title())
            .text_style_when(move || {
                let mut style = settings_text_style(&s, TextSize::Xl);
                style.weight = sui::FontWeight::SEMIBOLD;
                style
            })
            .color_when({
                let s = shared.clone();
                move || palette(&s).text
            })
    };
    let header = Column::new(&reader, |theme| theme.metrics.form_section_description_gap)
        .with_child(title)
        .with_child(muted_label_with_token(
            shared,
            section.description(),
            TextSize::Sm,
        ));
    SemanticRegion::new(section.title(), header)
        .description(section.description())
        .role(SemanticsRole::Heading)
}

/// A card title or description: fixed, or following an observed value.
enum CardText {
    Fixed(String),
    Bound {
        current: Box<dyn Fn() -> String>,
        bind: Box<dyn FnOnce(Label) -> Label>,
    },
}

impl CardText {
    fn bound<O>(text: O) -> Self
    where
        O: Observable<String> + Clone + 'static,
    {
        let current = text.clone();
        Self::Bound {
            current: Box::new(move || current.get()),
            bind: Box::new(move |label: Label| label.text_from(text)),
        }
    }

    fn label(self) -> (Label, Option<Box<dyn Fn() -> String>>) {
        match self {
            CardText::Fixed(text) => (Label::new(text), None),
            CardText::Bound { current, bind } => {
                let label = bind(Label::new(current()));
                (label, Some(current))
            }
        }
    }
}

/// A titled card that fills the page column.
pub(super) struct SettingsCard {
    title: String,
    title_text: Option<CardText>,
    description: Option<CardText>,
    header_action: Option<Boxed>,
    rows: Vec<Boxed>,
}

impl SettingsCard {
    pub(super) fn new(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            title_text: None,
            description: None,
            header_action: None,
            rows: Vec::new(),
        }
    }

    /// A title that follows `title`, such as a name being edited in the card.
    pub(super) fn title_from<O>(mut self, title: O) -> Self
    where
        O: Observable<String> + Clone + 'static,
    {
        self.title_text = Some(CardText::bound(title));
        self
    }

    pub(super) fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(CardText::Fixed(description.into()));
        self
    }

    /// A description that follows `description`, such as a count or a loading message.
    pub(super) fn description_from<O>(mut self, description: O) -> Self
    where
        O: Observable<String> + Clone + 'static,
    {
        self.description = Some(CardText::bound(description));
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
        let title_text = self
            .title_text
            .unwrap_or_else(|| CardText::Fixed(self.title.clone()));
        let (title, title_name) = title_text.label();
        let title = {
            let style = shared.clone();
            let color = shared.clone();
            title
                .text_style_when(move || {
                    let mut style = settings_text_style(&style, TextSize::Base);
                    style.weight = sui::FontWeight::SEMIBOLD;
                    style
                })
                .color_when(move || palette(&color).text)
        };
        let mut header = Column::new(reader, |theme| theme.metrics.form_section_description_gap)
            .with_child(title);
        if let Some(description) = self.description {
            let (label, _) = description.label();
            header = header.with_child(muted_text(shared, label, TextSize::Sm));
        }
        let header = match self.header_action {
            Some(action) => Boxed::new(RowLayout::new(reader, header, action)),
            None => Boxed::new(header),
        };
        let mut body =
            Column::new(reader, |theme| theme.metrics.form_section_body_gap).with_child(header);
        for row in self.rows {
            body = body.with_child(row);
        }
        let region = SemanticRegion::new(
            self.title,
            Surface::panel(ThemedPadding::new(
                reader,
                |theme| theme.metrics.form_section_padding,
                body,
            ))
            .fill_width(true)
            .theme_when(clone_reader(reader)),
        );
        match title_name {
            Some(name) => region.name_when(name),
            None => region,
        }
    }
}

/// One setting: a label with optional help text, and the control or value it describes.
pub(super) struct SettingRow {
    label: String,
    help: Option<CardText>,
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
        let help = help.into();
        if !help.trim().is_empty() {
            self.help = Some(CardText::Fixed(help));
        }
        self
    }

    /// Help that follows `help`, such as a status detail reported in the background.
    pub(super) fn help_from<O>(mut self, help: O) -> Self
    where
        O: Observable<String> + Clone + 'static,
    {
        self.help = Some(CardText::bound(help));
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

    /// A read-only value that follows `value`.
    pub(super) fn value_from<O>(self, shared: &Shared, value: O) -> Self
    where
        O: Observable<String> + Clone + 'static,
    {
        let control = value_text_from(shared, value);
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
        let reader = shared_reader(shared);
        let label = {
            let style = shared.clone();
            let color = shared.clone();
            Label::new(self.label.clone())
                .text_style_when(move || settings_text_style(&style, TextSize::Base))
                .color_when(move || palette(&color).text)
        };
        let mut text = Column::new(&reader, |theme| theme.metrics.form_section_description_gap)
            .with_child(label);
        if let Some(help) = self.help {
            let (label, _) = help.label();
            text = text.with_child(muted_text(shared, label, TextSize::Sm));
        }
        if let Some(help) = self.help_widget {
            text = text.with_child(help);
        }
        let control = self.control.unwrap_or_else(|| Boxed::new(SizedBox::new()));
        SemanticRegion::new(self.label, RowLayout::new(&reader, text, control))
    }
}

/// A status chip whose label follows `label` and whose tone follows `tone`, kept mounted.
pub(super) fn status_badge_from<L, T>(
    reader: &ThemeReader,
    label: L,
    tone: T,
) -> Restyle<StatusBadge, SemanticTone>
where
    L: Observable<String> + Clone + 'static,
    T: Observable<SemanticTone> + 'static,
{
    let badge = StatusBadge::new(label.get())
        .label_from(label)
        .theme_when(clone_reader(reader));
    Restyle::new(badge, tone, |badge, tone| badge.tone(*tone))
}

/// Right-aligned, wrapped value text that tracks the theme.
pub(super) fn value_text(shared: &Shared, value: impl Into<String>) -> RichText {
    let value = value.into();
    let shared = shared.clone();
    let document = move || value_document(&shared, value.clone());
    RichText::new(document()).document_when(document)
}

/// [`value_text`] whose text follows `value`; the text stays mounted, so a selection survives
/// unrelated updates.
pub(super) fn value_text_from<O>(shared: &Shared, value: O) -> Restyle<RichText, String>
where
    O: Observable<String> + Clone + 'static,
{
    let current = value.clone();
    let shared = shared.clone();
    let document = move || value_document(&shared, current.get());
    Restyle::new(
        RichText::new(document()).document_when(document),
        value,
        |text, _| text,
    )
}

fn value_document(shared: &Shared, value: String) -> TextDocument {
    let mut style = settings_text_style(shared, TextSize::Base);
    style.color = palette(shared).text_muted;
    let mut document = TextDocument::from_plain_text(value, style);
    for paragraph in &mut document.paragraphs {
        paragraph.style.align = sui::TextAlign::Right;
    }
    document
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
        .with_item(field, FlexItem::new().shrink(1.0))
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

/// [`setting_switch`] that adopts `checked` whenever it changes elsewhere — a reset, a reload, or
/// another tree — while a toggle still shows at once, before any queued write completes.
pub(super) fn setting_switch_from<O>(
    reader: &ThemeReader,
    name: &str,
    checked: O,
    on_change: impl FnMut(bool) + 'static,
) -> Restyle<Switch, bool>
where
    O: Observable<bool> + 'static,
{
    synced_switch(
        setting_switch(reader, name, checked.get(), on_change),
        checked,
    )
}

/// Keep `switch` mounted while adopting each new value of `checked`.
pub(super) fn synced_switch<O>(switch: Switch, checked: O) -> Restyle<Switch, bool>
where
    O: Observable<bool> + 'static,
{
    Restyle::new(switch, checked, |mut switch, checked| {
        switch.set_checked(*checked);
        switch
    })
}

/// The chevron button that opens or closes a disclosure. `on_toggle` flips the owning state, and
/// the button's label and chevron follow `open`.
pub(super) fn disclosure_toggle<O>(
    reader: &ThemeReader,
    open: O,
    show_label: &str,
    hide_label: &str,
    on_toggle: impl Fn() + 'static,
) -> Restyle<sui::Button, bool>
where
    O: Observable<bool> + 'static,
{
    disclosure_restyle(
        disclosure_button(reader, show_label, on_toggle),
        open,
        show_label,
        hide_label,
    )
}

/// The unstyled button behind [`disclosure_toggle`], for callers that add more builders.
pub(super) fn disclosure_button(
    reader: &ThemeReader,
    show_label: &str,
    on_toggle: impl Fn() + 'static,
) -> sui::Button {
    sui::Button::new(show_label)
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

/// Keep a disclosure `button` mounted while its label and chevron follow `open`.
pub(super) fn disclosure_restyle<O>(
    button: sui::Button,
    open: O,
    show_label: &str,
    hide_label: &str,
) -> Restyle<sui::Button, bool>
where
    O: Observable<bool> + 'static,
{
    let show_label = show_label.to_string();
    let hide_label = hide_label.to_string();
    Restyle::new(button, open, move |mut button, open| {
        let label = if *open { &hide_label } else { &show_label };
        button.set_label(label.clone());
        button.icon(if *open {
            IconGlyph::ChevronUp
        } else {
            IconGlyph::ChevronDown
        })
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
    let mut open_body = Column::new(reader, |theme| theme.metrics.form_section_body_gap)
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

/// A vertical stack whose gap is a theme metric, read on each layout. A child that takes no
/// height — content a `Presence` has hidden — takes no gap either, and while such a child grows
/// or shrinks its gap follows it, so the rows around it move smoothly.
pub(super) struct Column {
    reader: ThemeReader,
    gap: fn(&DefaultTheme) -> f32,
    children: Vec<WidgetPod>,
}

impl Column {
    pub(super) fn new(reader: &ThemeReader, gap: fn(&DefaultTheme) -> f32) -> Self {
        Self {
            reader: Rc::clone(reader),
            gap,
            children: Vec::new(),
        }
    }

    pub(super) fn with_child(mut self, child: impl Widget + 'static) -> Self {
        self.push(child);
        self
    }

    pub(super) fn push(&mut self, child: impl Widget + 'static) {
        self.children.push(WidgetPod::new(child));
    }

    /// The offset of each child from the top, and the total height.
    fn offsets(&self) -> (Vec<f32>, f32) {
        let gap = (self.gap)(&(self.reader)());
        let mut offsets = Vec::with_capacity(self.children.len());
        let mut y = 0.0;
        let mut content = false;
        for child in &self.children {
            let height = child.measured_size().height;
            if height > 0.0 && content {
                y += gap.min(height);
            }
            offsets.push(y);
            y += height;
            content |= height > 0.0;
        }
        (offsets, y)
    }
}

impl Widget for Column {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let finite = constraints.max.width.is_finite();
        let child_constraints = Constraints::new(
            Size::new(if finite { constraints.max.width } else { 0.0 }, 0.0),
            Size::new(constraints.max.width, f32::INFINITY),
        );
        let mut width: f32 = 0.0;
        for child in &mut self.children {
            width = width.max(child.measure(ctx, child_constraints).width);
        }
        let (_, height) = self.offsets();
        let width = if finite { constraints.max.width } else { width };
        constraints.clamp(Size::new(width, height))
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        let (offsets, _) = self.offsets();
        for (child, offset) in self.children.iter_mut().zip(offsets) {
            let height = child.measured_size().height;
            child.arrange(
                ctx,
                Rect::new(bounds.x(), bounds.y() + offset, bounds.width(), height),
            );
        }
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        for child in &self.children {
            child.paint(ctx);
        }
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        for child in &self.children {
            child.semantics(ctx);
        }
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        for child in &self.children {
            visitor.visit(child);
        }
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        for child in &mut self.children {
            visitor.visit(child);
        }
    }
}

/// Insets a child by a theme metric, read on each layout.
pub(super) struct ThemedPadding {
    reader: ThemeReader,
    insets: fn(&DefaultTheme) -> sui::Insets,
    child: WidgetPod,
}

impl ThemedPadding {
    pub(super) fn new(
        reader: &ThemeReader,
        insets: fn(&DefaultTheme) -> sui::Insets,
        child: impl Widget + 'static,
    ) -> Self {
        Self {
            reader: Rc::clone(reader),
            insets,
            child: WidgetPod::new(child),
        }
    }

    fn insets(&self) -> sui::Insets {
        (self.insets)(&(self.reader)())
    }
}

impl Widget for ThemedPadding {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let insets = self.insets();
        let horizontal = insets.left + insets.right;
        let vertical = insets.top + insets.bottom;
        let shrink = |size: Size| {
            Size::new(
                (size.width - horizontal).max(0.0),
                (size.height - vertical).max(0.0),
            )
        };
        let child = self.child.measure(
            ctx,
            Constraints::new(shrink(constraints.min), shrink(constraints.max)),
        );
        constraints.clamp(Size::new(child.width + horizontal, child.height + vertical))
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        let insets = self.insets();
        self.child.arrange(
            ctx,
            Rect::new(
                bounds.x() + insets.left,
                bounds.y() + insets.top,
                (bounds.width() - insets.left - insets.right).max(0.0),
                (bounds.height() - insets.top - insets.bottom).max(0.0),
            ),
        );
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

/// Gives a child a height computed from the theme on each layout, such as a list's rows plus its
/// theme viewport padding.
pub(super) struct ThemedHeight {
    reader: ThemeReader,
    height: Box<dyn Fn(&DefaultTheme) -> f32>,
    child: WidgetPod,
}

impl ThemedHeight {
    pub(super) fn new(
        reader: &ThemeReader,
        height: impl Fn(&DefaultTheme) -> f32 + 'static,
        child: impl Widget + 'static,
    ) -> Self {
        Self {
            reader: Rc::clone(reader),
            height: Box::new(height),
            child: WidgetPod::new(child),
        }
    }
}

impl Widget for ThemedHeight {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let height = (self.height)(&(self.reader)());
        let tight = constraints.clamp(Size::new(constraints.max.width, height));
        let child = self.child.measure(
            ctx,
            Constraints::new(
                Size::new(constraints.min.width, tight.height),
                Size::new(constraints.max.width, tight.height),
            ),
        );
        constraints.clamp(Size::new(child.width, tight.height))
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

/// Caps a page at the theme's form section width, so settings read as one column.
pub(super) struct PageColumn {
    reader: ThemeReader,
    child: WidgetPod,
}

impl PageColumn {
    pub(super) fn new(reader: &ThemeReader, child: impl Widget + 'static) -> Self {
        Self {
            reader: Rc::clone(reader),
            child: WidgetPod::new(child),
        }
    }

    fn max_width(&self) -> f32 {
        (self.reader)().metrics.form_section_max_width
    }
}

impl Widget for PageColumn {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let max_width = constraints.max.width.min(self.max_width());
        let child = Constraints::new(
            Size::new(constraints.min.width.min(max_width), constraints.min.height),
            Size::new(max_width, constraints.max.height),
        );
        self.child.measure(ctx, child)
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        // A stretching parent can arrange this wider than it measured; keep the child capped.
        let width = bounds.width().min(self.max_width());
        self.child.arrange(
            ctx,
            Rect::new(bounds.x(), bounds.y(), width, bounds.height()),
        );
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

/// A field that takes the available width with a compact action after it, such as a Save icon
/// button. The action keeps the size its own theme gives it.
pub(super) struct FieldWithAction {
    field: WidgetPod,
    action: WidgetPod,
    gap: f32,
}

impl FieldWithAction {
    pub(super) fn new(field: impl Widget + 'static, action: impl Widget + 'static) -> Self {
        Self {
            field: WidgetPod::new(field),
            action: WidgetPod::new(action),
            gap: 8.0,
        }
    }

    pub(super) fn gap(mut self, gap: f32) -> Self {
        self.gap = gap.max(0.0);
        self
    }
}

impl Widget for FieldWithAction {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let loose = Constraints::new(Size::ZERO, Size::new(constraints.max.width, f32::INFINITY));
        let action = self.action.measure(ctx, loose);
        let field_width = if constraints.max.width.is_finite() {
            (constraints.max.width - action.width - self.gap).max(0.0)
        } else {
            self.field.measure(ctx, loose).width
        };
        let field = self.field.measure(
            ctx,
            Constraints::new(
                Size::new(field_width, 0.0),
                Size::new(field_width, f32::INFINITY),
            ),
        );
        constraints.clamp(Size::new(
            field_width + self.gap + action.width,
            field.height.max(action.height),
        ))
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        let action = self.action.measured_size();
        let field = self.field.measured_size();
        let field_width = (bounds.width() - action.width - self.gap).max(0.0);
        self.field.arrange(
            ctx,
            Rect::new(
                bounds.x(),
                bounds.y() + (bounds.height() - field.height) * 0.5,
                field_width,
                field.height,
            ),
        );
        self.action.arrange(
            ctx,
            Rect::new(
                bounds.max_x() - action.width,
                bounds.y() + (bounds.height() - action.height) * 0.5,
                action.width,
                action.height,
            ),
        );
    }

    fn paint(&self, ctx: &mut PaintCtx) {
        self.field.paint(ctx);
        self.action.paint(ctx);
    }

    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.field.semantics(ctx);
        self.action.semantics(ctx);
    }

    fn visit_children(&self, visitor: &mut dyn WidgetPodVisitor) {
        visitor.visit(&self.field);
        visitor.visit(&self.action);
    }

    fn visit_children_mut(&mut self, visitor: &mut dyn WidgetPodMutVisitor) {
        visitor.visit(&mut self.field);
        visitor.visit(&mut self.action);
    }
}

/// Label column on the left, control on the right; stacked when narrow. The control is measured
/// first so short controls (switches, chips) leave the label more room. Gaps are the theme's form
/// row and stacked property row gaps.
struct RowLayout {
    reader: ThemeReader,
    text: WidgetPod,
    control: WidgetPod,
    stacked: bool,
}

impl RowLayout {
    fn new(reader: &ThemeReader, text: impl Widget + 'static, control: Boxed) -> Self {
        Self {
            reader: Rc::clone(reader),
            text: WidgetPod::new(text),
            control: WidgetPod::new(control),
            stacked: false,
        }
    }

    fn gaps(&self) -> (f32, f32) {
        let metrics = (self.reader)().metrics;
        (metrics.form_row_gap, metrics.property_row_stacked_gap)
    }
}

impl Widget for RowLayout {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let (row_gap, stack_gap) = self.gaps();
        let width = if constraints.max.width.is_finite() {
            constraints.max.width
        } else {
            (self.reader)().metrics.form_section_max_width
        };
        let loose = |max_width: f32| {
            Constraints::new(Size::ZERO, Size::new(max_width.max(0.0), f32::INFINITY))
        };
        self.stacked = width < ROW_STACK_BELOW_W;
        let height = if self.stacked {
            let text = self.text.measure(ctx, loose(width));
            let control = self.control.measure(ctx, loose(width));
            text.height + stack_gap + control.height
        } else {
            let label_w = (width * ROW_LABEL_SHARE).max(ROW_LABEL_MIN_W);
            let control = self.control.measure(ctx, loose(width - label_w - row_gap));
            let text = self
                .text
                .measure(ctx, loose(width - control.width - row_gap));
            text.height.max(control.height)
        };
        constraints.clamp(Size::new(width, height))
    }

    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        let (row_gap, stack_gap) = self.gaps();
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
                    bounds.y() + text.height + stack_gap,
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
                (bounds.width() - control.width - row_gap).max(0.0),
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
        let column = shared
            .theme_sized(ControlSize::Small)
            .metrics
            .form_section_max_width;
        assert!(
            card.width() <= column + 0.5,
            "cards stay in the theme's reading column, not the full window: {card:?}"
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

    #[test]
    fn hidden_column_children_leave_no_gap() -> sui::Result<()> {
        let shared = appearance_shared();
        let reader = shared_reader(&shared);
        let column = Column::new(&reader, |theme| theme.metrics.form_section_body_gap)
            .with_child(SizedBox::new().height(20.0))
            .with_child(SizedBox::new().height(0.0))
            .with_child(SemanticRegion::new(
                "last",
                SizedBox::new().height(20.0).width(20.0),
            ));
        let app = kit_app(
            Align::new(Alignment::Start, Alignment::Start, column),
            300.0,
        )?;
        let snapshot = app.main_window()?.snapshot()?;
        let last = node_bounds(&snapshot, "last");
        let gap = reader().metrics.form_section_body_gap;
        assert!(
            (last.y() - (20.0 + gap)).abs() <= 0.5,
            "one gap separates the two visible children: {last:?}"
        );
        Ok(())
    }
}

/// The settings row for code that builds rows from a label and a control, with the same builder
/// calls as SUI's `FormRow`: an inline row lays out like [`SettingRow`], and a stacked row puts
/// the label above a full-width control (text areas, long editors). The row is built on first
/// layout, after `theme_when` has supplied the theme; its label style and gaps follow the theme.
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
        let label = {
            let style = Rc::clone(&reader);
            let color = Rc::clone(&reader);
            Label::new(self.label.clone())
                .text_style_when(move || {
                    let theme = style();
                    sui::TextStyle {
                        font_size: theme.text.base.size,
                        line_height: theme.text.base.line_height,
                        ..theme.body_text_style()
                    }
                })
                .color_when(move || color().palette.text)
        };
        let control = self
            .control
            .take()
            .unwrap_or_else(|| WidgetPod::new(SizedBox::new()));
        let body = if self.stacked {
            Boxed::new(
                Column::new(&reader, |theme| theme.metrics.property_row_stacked_gap)
                    .with_child(label)
                    .with_child(PodWidget(control)),
            )
        } else {
            Boxed::new(RowLayout::new(
                &reader,
                label,
                Boxed::new(PodWidget(control)),
            ))
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
