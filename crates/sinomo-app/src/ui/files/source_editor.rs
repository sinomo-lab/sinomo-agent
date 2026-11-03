//! Native source editor. All decorations use byte offsets in the unmodified document.
use super::*;
use std::{ops::Range, sync::OnceLock};
use sui::{Command, CommandKey, Signal, TextInput, TextSurfaceStatus, TextSurfaceStyleSpan};
use syntect::{
    easy::HighlightLines,
    parsing::{SyntaxReference, SyntaxSet},
};

const NAVIGATE: CommandKey<Navigation> = CommandKey::new("sinomo.files.navigate-source");
const COLOR_BYTES: usize = 128 * 1024;
const COLOR_SPANS: usize = 16_384;
const PADDING: f32 = 12.0;

#[derive(Clone, Debug)]
enum Navigation {
    Find(String, bool),
    Line(String),
}

fn search(text: &str, query: &str, selection: Range<usize>, forward: bool) -> Option<Range<usize>> {
    if query.is_empty() {
        return None;
    }
    let index = if forward {
        text.get(selection.end..)?
            .find(query)
            .map(|n| n + selection.end)
            .or_else(|| text.find(query))
    } else {
        text.get(..selection.start)?
            .rfind(query)
            .or_else(|| text.rfind(query))
    }?;
    Some(index..index + query.len())
}
fn line_start(text: &str, line: usize) -> Option<usize> {
    match line {
        0 => None,
        1 => Some(0),
        n => text
            .match_indices('\n')
            .nth(n - 2)
            .map(|(offset, _)| offset + 1),
    }
}
fn grammar(path: &str) -> Option<(&'static SyntaxSet, &'static SyntaxReference)> {
    static SYNTAX: OnceLock<SyntaxSet> = OnceLock::new();
    let name = path.rsplit(['/', '\\']).next()?;
    let set = if name.rsplit_once('.').is_some_and(|(_, ext)| {
        ["ps1", "psm1", "psd1"]
            .iter()
            .any(|suffix| ext.eq_ignore_ascii_case(suffix))
    }) {
        static POWERSHELL: OnceLock<SyntaxSet> = OnceLock::new();
        POWERSHELL.get_or_init(|| {
            let mut builder = syntect::parsing::SyntaxSetBuilder::new();
            builder.add(
                syntect::parsing::SyntaxDefinition::load_from_str(
                    include_str!("powershell.sublime-syntax"),
                    true,
                    None,
                )
                .expect("bundled PowerShell grammar"),
            );
            builder.build()
        })
    } else {
        SYNTAX.get_or_init(two_face::syntax::extra_newlines)
    };
    let syntax = set.find_syntax_by_extension(name).or_else(|| {
        name.rsplit_once('.')
            .and_then(|(_, ext)| set.find_syntax_by_extension(ext))
    })?;
    (syntax.name != "Plain Text").then_some((set, syntax))
}
type Tokens = Vec<(Range<usize>, [u8; 3])>;
#[derive(Default)]
struct Colors {
    language: String,
    tokens: Tokens,
    limited: bool,
}
fn colors(path: &str, text: &str, dark: bool, limited: bool) -> Colors {
    let Some((set, syntax)) = grammar(path) else {
        return Colors {
            language: "Text".into(),
            ..Default::default()
        };
    };
    let theme = &super::super::chat_render::theme_set().themes[if dark {
        "base16-ocean.dark"
    } else {
        "base16-ocean.light"
    }];
    let mut parser = HighlightLines::new(syntax, theme);
    let mut result = Colors {
        language: syntax.name.clone(),
        limited,
        ..Default::default()
    };
    let mut offset = 0;
    for line in text.split_inclusive('\n') {
        if line.len() > 8192 || result.tokens.len() >= COLOR_SPANS {
            result.limited = true;
            break;
        }
        let Ok(parts) = parser.highlight_line(line, set) else {
            result.limited = true;
            break;
        };
        for (style, part) in parts {
            let end = offset + part.len();
            if end > offset {
                if result.tokens.len() == COLOR_SPANS {
                    result.limited = true;
                    return result;
                }
                result.tokens.push((
                    offset..end,
                    [style.foreground.r, style.foreground.g, style.foreground.b],
                ));
            }
            offset = end;
        }
    }
    result
}
pub(super) fn mono(theme: &DefaultTheme) -> TextStyle {
    let mut style = theme.mono_text_style(theme.palette.text);
    style.font_size = theme.text.sm.size;
    style.line_height = theme.text.sm.line_height;
    style
}
type Revision = (u64, bool, u32, u32);
struct Source {
    surface: super::editor_surface::EditorSurface,
    path: String,
    shared: Shared,
    reader: ThemeReader,
    status: Signal<TextSurfaceStatus>,
    notice: Signal<String>,
    language: Signal<String>,
    gutter: f32,
    revision: Rc<Cell<u64>>,
    styled: Option<Revision>,
    job: Option<(Revision, mpsc::Receiver<Colors>)>,
}
impl Source {
    fn update_colors(&mut self, ctx: &mut MeasureCtx) {
        let style = mono(&(self.reader)());
        let key = (
            self.revision.get(),
            widgets::theme_is_dark(&self.shared),
            style.font_size.to_bits(),
            style.line_height.to_bits(),
        );
        if let Some((started, rx)) = &self.job {
            match rx.try_recv() {
                Ok(colors) => {
                    if *started == key {
                        let style = mono(&(self.reader)());
                        self.surface.text.set_style_spans(
                            colors
                                .tokens
                                .into_iter()
                                .map(|(range, rgb)| {
                                    let mut style = style.clone();
                                    style.color = Color::rgba(
                                        rgb[0] as f32 / 255.,
                                        rgb[1] as f32 / 255.,
                                        rgb[2] as f32 / 255.,
                                        1.,
                                    );
                                    TextSurfaceStyleSpan::new(range, style)
                                })
                                .collect(),
                        );
                        self.language.set(if colors.limited {
                            format!("{} · partial highlighting", colors.language)
                        } else {
                            colors.language
                        });
                        self.styled = Some(key);
                    }
                    self.job = None;
                }
                Err(mpsc::TryRecvError::Empty) => {
                    ctx.request_animation_frame();
                    return;
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    self.job = None;
                    self.styled = Some(key);
                }
            }
        }
        if self.styled == Some(key) {
            return;
        }
        self.surface.text.set_style_spans(Vec::new());
        let text = self.surface.text.current_value();
        let end = text.floor_char_boundary(text.len().min(COLOR_BYTES));
        let limited = end < text.len();
        let prefix = text[..end].to_owned();
        let path = self.path.clone();
        let (tx, rx) = mpsc::channel();
        if thread::Builder::new()
            .name("sinomo-source-colors".into())
            .spawn(move || {
                let _ = tx.send(colors(&path, &prefix, key.1, limited));
            })
            .is_ok()
        {
            self.job = Some((key, rx));
            ctx.request_animation_frame();
        } else {
            self.styled = Some(key);
        }
    }
}
impl Widget for Source {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        self.surface.event(ctx, event);
        if self.job.is_some() && matches!(event, Event::Wake(_)) {
            ctx.request_measure();
            ctx.request_paint();
        }
    }
    fn command(&mut self, ctx: &mut EventCtx, cmd: &Command<'_>) {
        let Some(action) = cmd.get(NAVIGATE) else {
            self.surface.command(ctx, cmd);
            return;
        };
        let text = self.surface.text.current_value();
        let result = match action {
            Navigation::Find(query, forward) => {
                search(text, query, self.status.get().selection, *forward).ok_or("No match")
            }
            Navigation::Line(input) => input
                .trim()
                .parse::<usize>()
                .ok()
                .and_then(|line| line_start(text, line))
                .map(|i| i..i)
                .ok_or("Line out of range"),
        };
        match result {
            Ok(range) => {
                self.surface.text.set_selection(range.start, range.end);
                self.notice.set(String::new());
                ctx.request_focus();
            }
            Err(message) => {
                self.notice.set(message.to_owned());
            }
        }
        ctx.request_measure();
        ctx.request_paint();
        ctx.request_semantics();
    }
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        // Flex probes a zero-height editor before assigning its viewport.
        // TextSurface consumes a pending caret reveal when measured.
        if constraints.max.height == 0. {
            return constraints.clamp(Size::ZERO);
        }
        self.update_colors(ctx);
        self.surface.measure(ctx, constraints)
    }
    fn arrange(&mut self, ctx: &mut ArrangeCtx, bounds: Rect) {
        self.surface.arrange(ctx, bounds);
    }
    fn paint(&self, ctx: &mut PaintCtx) {
        self.surface.paint(ctx);
        let status = self.status.get();
        let theme = (self.reader)();
        let mut style = mono(&theme);
        style.color = theme.palette.text_muted;
        let height = style.line_height.max(style.font_size).max(1.0);
        let scroll = ctx.observe(&self.surface.scroll_y);
        let first = status.visible_lines.start;
        let scroll = if (scroll / height).floor() as usize == first {
            scroll
        } else {
            first as f32 * height
        };
        let bounds = ctx.bounds();
        ctx.push_clip_rect(Rect::new(
            bounds.x(),
            bounds.y() + PADDING,
            self.gutter,
            (bounds.height() - 2. * PADDING).max(0.0),
        ));
        for line in status.visible_lines {
            ctx.draw_text(
                Rect::new(
                    bounds.x() + 3.,
                    bounds.y() + PADDING + line as f32 * height - scroll,
                    self.gutter - 4.,
                    height,
                ),
                (line + 1).to_string(),
                style.clone(),
            );
        }
        ctx.pop_clip();
    }
    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.surface.semantics(ctx);
    }
    fn accepts_focus(&self) -> bool {
        true
    }
    fn focus_changed(&mut self, ctx: &mut EventCtx, focused: bool) {
        self.surface.focus_changed(ctx, focused);
    }
}

/// Retains the editor (and its undo history) while the shortcut row opens and closes.
pub(super) struct SearchFields {
    mode: Signal<usize>,
    find: sui::FocusScopeState,
    line: Option<sui::FocusScopeState>,
    editor: Option<WidgetId>,
}
pub(super) type ActiveSearch = Rc<RefCell<Option<SearchFields>>>;

pub(super) struct SearchHost {
    child: WidgetPod,
    active: ActiveSearch,
}
impl SearchHost {
    pub(super) fn new(child: impl Widget + 'static, active: ActiveSearch) -> Self {
        Self {
            child: WidgetPod::new(child),
            active,
        }
    }
    pub(super) fn close_search(active: &ActiveSearch, ctx: &mut EventCtx) -> bool {
        let fields = active.borrow();
        let Some(fields) = fields.as_ref() else {
            return false;
        };
        if !fields.mode.set(0) {
            return false;
        }
        if let Some(editor) = fields.editor {
            ctx.request_focus_for(editor);
        }
        ctx.request_measure();
        true
    }
    pub(super) fn editor(
        active: &ActiveSearch,
        body: impl Widget + 'static,
        find: widgets::Boxed,
        line: Option<widgets::Boxed>,
        editor: Option<WidgetId>,
    ) -> WidgetPod {
        let find_focus = sui::FocusScopeState::new();
        let mode = Signal::new(0usize);
        let mut bar = SwitchView::new()
            .selected_from(mode.clone())
            .with_child(SizedBox::new().height(0.))
            .with_child(
                sui::FocusScope::new(find)
                    .state(find_focus.clone())
                    .restore_policy(sui::FocusRestorePolicy::FirstFocusable),
            );
        let line = line.map(|row| {
            let focus = sui::FocusScopeState::new();
            bar.push(
                sui::FocusScope::new(row)
                    .state(focus.clone())
                    .restore_policy(sui::FocusRestorePolicy::FirstFocusable),
            );
            focus
        });
        let child = Flex::vertical()
            .align_items(Alignment::Stretch)
            .with_child(bar)
            .with_item(body, FlexItem::fill());
        *active.borrow_mut() = Some(SearchFields {
            mode: mode.clone(),
            find: find_focus,
            line,
            editor,
        });
        WidgetPod::new(SearchLayout {
            mode,
            layout: child,
        })
    }
}
impl Widget for SearchHost {
    fn event(&mut self, ctx: &mut EventCtx, event: &Event) {
        if ctx.phase() != EventPhase::Capture {
            return;
        }
        let Event::Keyboard(key) = event else {
            return;
        };
        if key.state != KeyState::Pressed || key.is_composing {
            return;
        }
        let active = self.active.borrow();
        let Some(fields) = active.as_ref() else {
            return;
        };
        let shortcut = key.modifiers.control || key.modifiers.meta;
        if shortcut && key.key.eq_ignore_ascii_case("f") {
            fields.mode.set(1);
            fields.find.request_focus();
        } else if shortcut
            && key.key.eq_ignore_ascii_case("g")
            && let Some(line) = &fields.line
        {
            fields.mode.set(2);
            line.request_focus();
        } else if key.key == "Escape" && fields.mode.get() != 0 {
            fields.mode.set(0);
            if let Some(id) = fields.editor {
                ctx.request_focus_for(id);
            }
        } else {
            return;
        }
        request_files_overlay_rebuild(ctx);
        ctx.set_handled();
    }
    fn measure(&mut self, ctx: &mut MeasureCtx, c: Constraints) -> Size {
        self.child.measure(ctx, c)
    }
    fn arrange(&mut self, ctx: &mut ArrangeCtx, r: Rect) {
        self.child.arrange(ctx, r);
    }
    fn paint(&self, ctx: &mut PaintCtx) {
        self.child.paint(ctx);
    }
    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.child.semantics(ctx);
    }
    fn visit_children(&self, v: &mut dyn WidgetPodVisitor) {
        v.visit(&self.child);
    }
    fn visit_children_mut(&mut self, v: &mut dyn WidgetPodMutVisitor) {
        v.visit(&mut self.child);
    }
}

struct SearchLayout {
    mode: Signal<usize>,
    layout: Flex,
}
impl Widget for SearchLayout {
    fn measure(&mut self, ctx: &mut MeasureCtx, constraints: Constraints) -> Size {
        let open = ctx.observe(&self.mode) != 0;
        self.layout
            .set_flex_item(0, FlexItem::fixed(if open { 36. } else { 0. }));
        self.layout
            .measure(ctx, Constraints::tight(constraints.max))
    }
    fn arrange(&mut self, ctx: &mut ArrangeCtx, r: Rect) {
        self.layout.arrange(ctx, r);
    }
    fn paint(&self, ctx: &mut PaintCtx) {
        self.layout.paint(ctx);
    }
    fn semantics(&self, ctx: &mut SemanticsCtx) {
        self.layout.semantics(ctx);
    }
    fn visit_children(&self, v: &mut dyn WidgetPodVisitor) {
        self.layout.visit_children(v);
    }
    fn visit_children_mut(&mut self, v: &mut dyn WidgetPodMutVisitor) {
        self.layout.visit_children_mut(v);
    }
}

pub(super) fn view(
    document: &FilesDocument,
    shared: &Shared,
    reader: &ThemeReader,
    active: &ActiveSearch,
) -> WidgetPod {
    let status = Signal::new(TextSurfaceStatus::default());
    let notice = Signal::new(String::new());
    let language = Signal::new("Text".to_string());
    let target = Rc::new(Cell::new(None));
    let query = Rc::new(RefCell::new(String::new()));
    let number = Rc::new(RefCell::new(String::new()));
    let field =
        |name: &'static str, placeholder: &'static str, value: Rc<RefCell<String>>, line: bool| {
            let target = Rc::clone(&target);
            widgets::Boxed::new(
                TextInput::new(name)
                    .placeholder(placeholder)
                    .min_width(0.)
                    .theme_when(clone_reader(reader))
                    .on_change(move |new| *value.borrow_mut() = new)
                    .on_submit_with_ctx(move |ctx, value| {
                        if let Some(target) = target.get() {
                            ctx.post_command(
                                target,
                                NAVIGATE,
                                if line {
                                    Navigation::Line(value.into())
                                } else {
                                    Navigation::Find(value.into(), true)
                                },
                            );
                        }
                    }),
            )
        };
    let find = field("Find in file", "Find", Rc::clone(&query), false);
    let line = field("Go to line", "Line", Rc::clone(&number), true);
    let action = |name: &'static str, icon, kind: usize| {
        let target = Rc::clone(&target);
        let query = Rc::clone(&query);
        let number = Rc::clone(&number);
        IconButton::new(icon, name)
            .size(28.)
            .theme_when(clone_reader(reader))
            .on_press_with_ctx(move |ctx| {
                if let Some(id) = target.get() {
                    let action = if kind == 2 {
                        Navigation::Line(number.borrow().clone())
                    } else {
                        Navigation::Find(query.borrow().clone(), kind == 1)
                    };
                    ctx.post_command(id, NAVIGATE, action);
                }
            })
    };
    let controls = Flex::horizontal()
        .gap(4.)
        .align_items(Alignment::Center)
        .with_item(find, FlexItem::fill().min_width(48.))
        .with_child(action("Previous match", IconGlyph::ChevronUp, 0))
        .with_child(action("Next match", IconGlyph::ChevronDown, 1));
    let jump = Flex::horizontal()
        .align_items(Alignment::Center)
        .with_item(line, FlexItem::fixed(65.))
        .with_child(action("Jump to line", IconGlyph::Redo, 2));
    let position = sui::combine_named(
        "source editor position",
        (status.clone(), language.clone(), notice.clone()),
        |(at, lang, info)| {
            let detail = if info.is_empty() {
                String::new()
            } else {
                format!(" · {info}")
            };
            format!(
                "{lang} · Ln {}, Col {}{detail}",
                at.caret_line.max(1),
                at.caret_column.max(1)
            )
        },
    );
    let footer = Label::new("Source position")
        .text_from(position)
        .theme_when(clone_reader(reader));
    let theme = reader();
    let gutter = mono(&theme).font_size * 0.65 * 7. + 8.;
    let edited = Rc::new(Cell::new(0u64));
    let signal = Rc::clone(&edited);
    let changed = shared.clone();
    let mut surface = TextSurface::new(format!("files-document:{}", document.path))
        .value(document.text.clone())
        .read_only(!document.editable())
        .wrap(TextWrap::NoWrap)
        .text_style_when(|theme| mono(&theme))
        .theme_when(clone_reader(reader))
        .on_change(move |value| {
            signal.set(signal.get().wrapping_add(1));
            changed.files_mut().set_document_text(value);
        });
    surface.set_selection(0, 0);
    let editor = widgets::Boxed::new(Source {
        surface: super::editor_surface::EditorSurface::new(
            surface,
            reader,
            status.clone(),
            Insets {
                left: gutter,
                top: PADDING,
                right: PADDING,
                bottom: PADDING,
            },
        ),
        path: document.path.clone(),
        shared: shared.clone(),
        reader: Rc::clone(reader),
        status,
        notice,
        language,
        gutter,
        revision: edited,
        styled: None,
        job: None,
    });
    target.set(Some(editor.child_id()));
    SearchHost::editor(
        active,
        Dock::new(editor).bottom(20., footer),
        widgets::Boxed::new(controls),
        Some(widgets::Boxed::new(jump)),
        target.get(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_editor_recognizes_milestone_languages_and_preserves_byte_offsets() {
        for name in [
            "main.py",
            "lib.rs",
            "app.ts",
            "view.tsx",
            "app.js",
            "run.sh",
            "deploy.ps1",
            "workflow.yaml",
            "Cargo.toml",
            "package.json",
            "Dockerfile",
            "Makefile",
        ] {
            assert!(grammar(name).is_some(), "missing syntax for {name}");
        }
        let source = "\u{feff}def run():\r\n    return 'λ'\r\n";
        let result = colors("test.py", source, true, false);
        let reconstructed = result
            .tokens
            .iter()
            .map(|(range, _)| &source[range.clone()])
            .collect::<String>();
        assert_eq!(reconstructed, source);
        assert!(
            result
                .tokens
                .windows(2)
                .any(|parts| parts[0].1 != parts[1].1)
        );
        assert_eq!(
            search(source, "λ", 0..0, true),
            Some(source.find('λ').unwrap()..source.find('λ').unwrap() + 2)
        );
        assert_eq!(line_start(source, 3), Some(source.len()));
        assert_eq!(line_start(source, 4), None);
    }

    #[test]
    fn source_editor_find_and_go_to_line_in_rendered_document() -> sui::Result<()> {
        use crate::state::AppState;
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        let path = "/example.py";
        let source = format!(
            "\u{feff}# λ\r\n{}",
            (1..=80)
                .map(|i| format!("value_{i} = 'match'\r\n"))
                .collect::<String>()
        );
        let mut document =
            FilesDocument::loading(FilesSourceKind::Sifs, path.into(), "example.py".into());
        document.loading = false;
        document.text = source.clone();
        document.saved_text = source.clone();
        document.sifs_fingerprint = Some(SifsFileFingerprint {
            object_id: "object-source".into(),
            version: "v1".into(),
            content_hash: Some("hash".into()),
            size_bytes: Some(source.len() as u64),
            policy: "replicated-eventual".into(),
        });
        let (tx, rx) = mpsc::channel();
        shared.files_mut().worker_tx = Some(tx);
        shared.files_mut().document = Some(document);
        let reader: ThemeReader = {
            let shared = shared.clone();
            Rc::new(move || shared.theme())
        };
        let runtime =
            sui::Application::new()
                .window(sui::WindowBuilder::new().title("Source editor").root(
                    FilesDialogHost::new(SizedBox::new(), shared.clone(), reader),
                ))
                .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        window
            .root()
            .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(560., 720.))))?;
        app.run_until_idle()?;
        window.advance_time(1.)?;
        app.run_until_idle()?;
        let editor_name = format!("files-document:{path}");
        let state = || {
            window
                .snapshot()
                .unwrap()
                .accessibility
                .nodes
                .into_iter()
                .find(|n| n.name.as_deref() == Some(&editor_name))
                .unwrap()
                .editable_text
                .unwrap()
        };
        let find = window
            .get_by_role(SemanticsRole::TextInput)
            .with_name("Find in file");
        assert_eq!(find.count()?, 0);
        let editor = window
            .get_by_role(SemanticsRole::TextInput)
            .with_name(&editor_name);
        let shortcut = |name| {
            let mut key = sui::KeyboardEvent::new(name, KeyState::Pressed);
            key.modifiers.control = true;
            editor.dispatch_event(Event::Keyboard(key))?;
            app.run_until_idle()
        };
        shortcut("f")?;
        window.advance_time(0.05)?;
        app.run_until_idle()?;
        assert_eq!(window.focused().with_name("Find in file").count()?, 1);
        find.fill("match")?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Next match")
            .click()?;
        app.run_until_idle()?;
        let first = state().selection;
        assert_eq!(first.start, source.find("match").unwrap());
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Next match")
            .click()?;
        app.run_until_idle()?;
        assert!(state().selection.start > first.start);
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Previous match")
            .click()?;
        app.run_until_idle()?;
        assert_eq!(state().selection.start, first.start);
        editor.press("Escape")?;
        app.run_until_idle()?;
        assert_eq!(find.count()?, 0);
        shortcut("g")?;
        window
            .get_by_role(SemanticsRole::TextInput)
            .with_name("Go to line")
            .fill("70")?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Jump to line")
            .click()?;
        app.run_until_idle()?;
        assert_eq!(state().caret_offset, line_start(&source, 70).unwrap());
        assert!(state().scroll_y > 0.);
        assert_eq!(shared.files().document.as_ref().unwrap().text, source);
        window
            .capture_screenshot()?
            .write_png(std::env::temp_dir().join("sinomo-source-editor.png"))?;
        let offset = line_start(&source, 70).unwrap();
        window
            .get_by_role(SemanticsRole::TextInput)
            .with_name(&editor_name)
            .dispatch_event(Event::Ime(sui::ImeEvent::CompositionCommit {
                text: "# ".into(),
            }))?;
        app.run_until_idle()?;
        let mut expected = source.clone();
        expected.insert_str(offset, "# ");
        assert_eq!(shared.files().document.as_ref().unwrap().text, expected);
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Save")
            .click()?;
        let FilesWorkerCommand::SaveSifsDocument {
            request_id,
            path,
            expected: base,
            bytes,
        } = rx.try_recv().unwrap()
        else {
            panic!("expected version-protected save")
        };
        assert_eq!(base.version, "v1");
        assert_eq!(bytes, expected.as_bytes());
        shared
            .files_mut()
            .apply_worker_result(FilesWorkerResult::DocumentSaved {
                request_id,
                source: FilesSourceKind::Sifs,
                path,
                result: Err("Save conflict".into()),
            });
        let files = shared.files();
        let draft = files.document.as_ref().unwrap();
        assert_eq!(draft.text, expected);
        assert!(draft.dirty());
        assert!(draft.save_error.is_some());
        Ok(())
    }
}
