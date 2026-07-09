use std::sync::OnceLock;

use pulldown_cmark::{
    CodeBlockKind, Event as MarkdownEvent, HeadingLevel, Options, Parser, Tag, TagEnd,
};
use std::ops::Range;
use sui::{
    FontStyle, FontWeight, RichTextSourceMap, RichTextSourceSpan, TextDocument, TextParagraph,
    TextSpan, TextStyle,
};
use syntect::easy::HighlightLines;
use syntect::highlighting::{Style, ThemeSet};
use syntect::parsing::SyntaxSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ChatCodeBlock {
    pub language: Option<String>,
    pub text: String,
    pub is_diff: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ChatRichRenderBlock {
    Text {
        document: TextDocument,
        display_range: Range<usize>,
    },
    Code {
        code: ChatCodeBlock,
        display_range: Range<usize>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ChatMarkdownRender {
    pub blocks: Vec<ChatRichRenderBlock>,
    pub source_map: RichTextSourceMap,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ChatMarkdownStyles {
    pub body: TextStyle,
    pub headings: [TextStyle; 6],
    pub inline_code: TextStyle,
    pub link: TextStyle,
}

/// Parse Markdown into SUI rich-text documents while retaining a mapping from
/// rendered character offsets to the original Markdown source.
pub(crate) fn markdown_rich_render(
    markdown: &str,
    styles: ChatMarkdownStyles,
) -> ChatMarkdownRender {
    let mut options = Options::empty();
    options.insert(Options::ENABLE_STRIKETHROUGH);
    options.insert(Options::ENABLE_TABLES);
    options.insert(Options::ENABLE_TASKLISTS);

    let mut builder = RichMarkdownBuilder::new(markdown, styles);
    for (event, source_range) in Parser::new_ext(markdown, options).into_offset_iter() {
        builder.event(event, source_range);
    }
    builder.finish()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RichBlockKind {
    Paragraph,
    Heading(u8),
    Item,
}

struct RichMarkdownBuilder<'a> {
    source: &'a str,
    styles: ChatMarkdownStyles,
    blocks: Vec<ChatRichRenderBlock>,
    rendered: String,
    source_spans: Vec<RichTextSourceSpan>,
    current_kind: Option<RichBlockKind>,
    current_spans: Vec<TextSpan>,
    current_display_start: usize,
    pending_source_start: Option<usize>,
    strong_depth: usize,
    emphasis_depth: usize,
    link_depth: usize,
    lists: Vec<Option<u64>>,
    code_language: Option<String>,
    code_text: String,
    code_source_start: Option<usize>,
    code_content_range: Option<Range<usize>>,
    table_cell_index: usize,
}

impl<'a> RichMarkdownBuilder<'a> {
    fn new(source: &'a str, styles: ChatMarkdownStyles) -> Self {
        Self {
            source,
            styles,
            blocks: Vec::new(),
            rendered: String::new(),
            source_spans: Vec::new(),
            current_kind: None,
            current_spans: Vec::new(),
            current_display_start: 0,
            pending_source_start: None,
            strong_depth: 0,
            emphasis_depth: 0,
            link_depth: 0,
            lists: Vec::new(),
            code_language: None,
            code_text: String::new(),
            code_source_start: None,
            code_content_range: None,
            table_cell_index: 0,
        }
    }

    fn event(&mut self, event: MarkdownEvent<'_>, source_range: Range<usize>) {
        match event {
            MarkdownEvent::Start(Tag::Paragraph) => {
                self.start_block(RichBlockKind::Paragraph, source_range.start)
            }
            MarkdownEvent::End(TagEnd::Paragraph) => self.finish_text_block(source_range.end),
            MarkdownEvent::Start(Tag::Heading { level, .. }) => self.start_block(
                RichBlockKind::Heading(heading_level(level)),
                source_range.start,
            ),
            MarkdownEvent::End(TagEnd::Heading(_)) => self.finish_text_block(source_range.end),
            MarkdownEvent::Start(Tag::List(start)) => self.lists.push(start),
            MarkdownEvent::End(TagEnd::List(_)) => {
                self.finish_text_block(source_range.end);
                self.lists.pop();
            }
            MarkdownEvent::Start(Tag::Item) => {
                self.start_block(RichBlockKind::Item, source_range.start);
                let marker = self.next_list_marker();
                self.append_mapped_text(&marker, source_range.clone(), source_range);
            }
            MarkdownEvent::End(TagEnd::Item) => self.finish_text_block(source_range.end),
            MarkdownEvent::Start(Tag::Table(_)) => {
                self.start_block(RichBlockKind::Paragraph, source_range.start);
                self.table_cell_index = 0;
            }
            MarkdownEvent::End(TagEnd::Table) => self.finish_text_block(source_range.end),
            MarkdownEvent::Start(Tag::TableHead | Tag::TableRow) => {
                self.table_cell_index = 0;
            }
            MarkdownEvent::End(TagEnd::TableHead | TagEnd::TableRow) => {
                self.append_mapped_text("\n", source_range.clone(), source_range);
                self.table_cell_index = 0;
            }
            MarkdownEvent::Start(Tag::TableCell) => {
                if self.table_cell_index > 0 {
                    self.append_mapped_text("  |  ", source_range.clone(), source_range);
                }
            }
            MarkdownEvent::End(TagEnd::TableCell) => {
                self.table_cell_index += 1;
            }
            MarkdownEvent::Start(Tag::Strong) => {
                self.strong_depth += 1;
                self.note_source_start(source_range.start);
            }
            MarkdownEvent::End(TagEnd::Strong) => {
                self.strong_depth = self.strong_depth.saturating_sub(1);
                self.extend_last_source_span(source_range.end);
            }
            MarkdownEvent::Start(Tag::Emphasis) => {
                self.emphasis_depth += 1;
                self.note_source_start(source_range.start);
            }
            MarkdownEvent::End(TagEnd::Emphasis) => {
                self.emphasis_depth = self.emphasis_depth.saturating_sub(1);
                self.extend_last_source_span(source_range.end);
            }
            MarkdownEvent::Start(Tag::Strikethrough) => self.note_source_start(source_range.start),
            MarkdownEvent::End(TagEnd::Strikethrough) => {
                self.extend_last_source_span(source_range.end)
            }
            MarkdownEvent::Start(Tag::Link { .. }) => {
                self.link_depth += 1;
                self.note_source_start(source_range.start);
            }
            MarkdownEvent::End(TagEnd::Link) => {
                self.link_depth = self.link_depth.saturating_sub(1);
                self.extend_last_source_span(source_range.end);
            }
            MarkdownEvent::Start(Tag::CodeBlock(kind)) => {
                self.finish_text_block(source_range.start);
                self.code_language = code_block_language(kind);
                self.code_text.clear();
                self.code_source_start = Some(source_range.start);
                self.code_content_range = None;
            }
            MarkdownEvent::End(TagEnd::CodeBlock) => self.finish_code_block(source_range.end),
            MarkdownEvent::Text(text) => {
                if self.code_source_start.is_some() {
                    self.code_text.push_str(&text);
                    self.code_content_range = Some(match self.code_content_range.take() {
                        Some(range) => {
                            range.start.min(source_range.start)..range.end.max(source_range.end)
                        }
                        None => source_range,
                    });
                } else {
                    self.ensure_text_block(source_range.start);
                    self.append_mapped_text(&text, source_range.clone(), source_range);
                }
            }
            MarkdownEvent::Code(code) => {
                self.ensure_text_block(source_range.start);
                let content_range = markdown_inline_content_range(self.source, &source_range);
                let mut style = self.styles.inline_code.clone();
                self.apply_inline_style(&mut style);
                self.append_mapped_span(&code, style, source_range.clone(), content_range);
            }
            MarkdownEvent::SoftBreak | MarkdownEvent::HardBreak => {
                self.ensure_text_block(source_range.start);
                self.append_mapped_text("\n", source_range.clone(), source_range);
            }
            MarkdownEvent::Rule => {
                self.start_block(RichBlockKind::Paragraph, source_range.start);
                self.append_mapped_text("---", source_range.clone(), source_range.clone());
                self.finish_text_block(source_range.end);
            }
            MarkdownEvent::TaskListMarker(checked) => {
                self.ensure_text_block(source_range.start);
                self.append_mapped_text(
                    if checked { "[x] " } else { "[ ] " },
                    source_range.clone(),
                    source_range,
                );
            }
            MarkdownEvent::Html(html) | MarkdownEvent::InlineHtml(html) => {
                self.ensure_text_block(source_range.start);
                self.append_mapped_text(&html, source_range.clone(), source_range);
            }
            _ => {}
        }
    }

    fn finish(mut self) -> ChatMarkdownRender {
        self.finish_text_block(self.source.len());
        if self.code_source_start.is_some() {
            self.finish_code_block(self.source.len());
        }
        if self.blocks.is_empty() && !self.source.is_empty() {
            self.start_block(RichBlockKind::Paragraph, 0);
            self.append_mapped_text(self.source, 0..self.source.len(), 0..self.source.len());
            self.finish_text_block(self.source.len());
        }
        ChatMarkdownRender {
            blocks: self.blocks,
            source_map: RichTextSourceMap::new(self.source, self.rendered, self.source_spans),
        }
    }

    fn start_block(&mut self, kind: RichBlockKind, source_start: usize) {
        self.finish_text_block(source_start);
        if !self.rendered.is_empty() {
            self.rendered.push_str("\n\n");
        }
        self.current_kind = Some(kind);
        self.current_display_start = self.rendered.len();
        self.current_spans.clear();
        self.note_source_start(source_start);
    }

    fn ensure_text_block(&mut self, source_start: usize) {
        if self.current_kind.is_none() {
            self.start_block(RichBlockKind::Paragraph, source_start);
        }
    }

    fn finish_text_block(&mut self, source_end: usize) {
        let Some(_kind) = self.current_kind.take() else {
            return;
        };
        self.extend_last_source_span(source_end);
        if self.current_spans.is_empty() {
            self.pending_source_start = None;
            return;
        }
        let display_range = self.current_display_start..self.rendered.len();
        let spans = std::mem::take(&mut self.current_spans);
        self.blocks.push(ChatRichRenderBlock::Text {
            document: TextDocument {
                paragraphs: vec![TextParagraph::from_spans(spans)],
            },
            display_range,
        });
        self.pending_source_start = None;
    }

    fn finish_code_block(&mut self, source_end: usize) {
        let Some(source_start) = self.code_source_start.take() else {
            return;
        };
        if !self.rendered.is_empty() {
            self.rendered.push_str("\n\n");
        }
        let raw_text = std::mem::take(&mut self.code_text);
        let text = raw_text.trim_end_matches('\n').to_string();
        let trailing_bytes = raw_text.len().saturating_sub(text.len());
        let display_start = self.rendered.len();
        self.rendered.push_str(&text);
        let display_range = display_start..self.rendered.len();
        let mut content_range = self
            .code_content_range
            .take()
            .unwrap_or(source_start..source_end);
        content_range.end = content_range
            .end
            .saturating_sub(trailing_bytes)
            .max(content_range.start);
        self.source_spans.push(RichTextSourceSpan::new(
            display_range.clone(),
            source_start..source_end,
            content_range,
        ));
        let language = self.code_language.take();
        let is_diff = language
            .as_deref()
            .is_some_and(|language| matches!(language, "diff" | "patch"))
            || looks_like_diff(&text);
        self.blocks.push(ChatRichRenderBlock::Code {
            code: ChatCodeBlock {
                language,
                text,
                is_diff,
            },
            display_range,
        });
    }

    fn current_style(&self) -> TextStyle {
        let mut style = match self.current_kind.unwrap_or(RichBlockKind::Paragraph) {
            RichBlockKind::Heading(level) => self.styles.headings
                [usize::from(level.saturating_sub(1)).min(self.styles.headings.len() - 1)]
            .clone(),
            RichBlockKind::Paragraph | RichBlockKind::Item => self.styles.body.clone(),
        };
        self.apply_inline_style(&mut style);
        style
    }

    fn apply_inline_style(&self, style: &mut TextStyle) {
        if self.strong_depth > 0 {
            style.weight = FontWeight::BOLD;
        }
        if self.emphasis_depth > 0 {
            style.style = FontStyle::Italic;
        }
        if self.link_depth > 0 {
            style.color = self.styles.link.color;
        }
    }

    fn append_mapped_text(
        &mut self,
        text: &str,
        source_range: Range<usize>,
        content_source_range: Range<usize>,
    ) {
        self.append_mapped_span(
            text,
            self.current_style(),
            source_range,
            content_source_range,
        );
    }

    fn append_mapped_span(
        &mut self,
        text: &str,
        style: TextStyle,
        mut source_range: Range<usize>,
        content_source_range: Range<usize>,
    ) {
        if text.is_empty() {
            return;
        }
        if let Some(pending) = self.pending_source_start.take() {
            source_range.start = source_range.start.min(pending);
        }
        let display_start = self.rendered.len();
        self.rendered.push_str(text);
        let display_range = display_start..self.rendered.len();
        self.current_spans.push(TextSpan::new(text, style));
        self.source_spans.push(RichTextSourceSpan::new(
            display_range,
            source_range,
            content_source_range,
        ));
    }

    fn note_source_start(&mut self, source_start: usize) {
        self.pending_source_start = Some(
            self.pending_source_start
                .map(|pending| pending.min(source_start))
                .unwrap_or(source_start),
        );
    }

    fn extend_last_source_span(&mut self, source_end: usize) {
        if let Some(span) = self.source_spans.last_mut() {
            span.source_range.end = span.source_range.end.max(source_end);
        }
    }

    fn next_list_marker(&mut self) -> String {
        let indent = "  ".repeat(self.lists.len().saturating_sub(1));
        match self.lists.last_mut().and_then(Option::as_mut) {
            Some(next) => {
                let current = *next;
                *next += 1;
                format!("{indent}{current}. ")
            }
            None => format!("{indent}- "),
        }
    }
}

fn markdown_inline_content_range(source: &str, range: &Range<usize>) -> Range<usize> {
    let Some(fragment) = source.get(range.clone()) else {
        return range.clone();
    };
    let leading = fragment.chars().take_while(|ch| *ch == '`').count();
    let trailing = fragment.chars().rev().take_while(|ch| *ch == '`').count();
    let start = (range.start + leading).min(range.end);
    let end = range.end.saturating_sub(trailing).max(start);
    start..end
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HighlightSpan {
    pub text: String,
    pub rgb: Option<[u8; 3]>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FileEditRender {
    pub title: String,
    pub code: ChatCodeBlock,
}

pub(crate) fn highlight_code(
    language: Option<&str>,
    code: &str,
    dark: bool,
) -> Vec<Vec<HighlightSpan>> {
    let syntax_set = syntax_set();
    let theme_set = theme_set();
    let syntax = language
        .and_then(|language| syntax_set.find_syntax_by_token(language))
        .or_else(|| language.and_then(|language| syntax_set.find_syntax_by_extension(language)))
        .unwrap_or_else(|| syntax_set.find_syntax_plain_text());
    let theme_name = if dark {
        "base16-ocean.dark"
    } else {
        "base16-ocean.light"
    };
    let theme = theme_set
        .themes
        .get(theme_name)
        .or_else(|| theme_set.themes.values().next());
    let Some(theme) = theme else {
        return code
            .lines()
            .map(|line| {
                vec![HighlightSpan {
                    text: line.to_string(),
                    rgb: None,
                }]
            })
            .collect();
    };

    let mut highlighter = HighlightLines::new(syntax, theme);
    code.lines()
        .map(|line| match highlighter.highlight_line(line, syntax_set) {
            Ok(spans) => spans
                .into_iter()
                .map(|(style, text)| HighlightSpan {
                    text: text.to_string(),
                    rgb: Some(style_rgb(style)),
                })
                .collect(),
            Err(_) => vec![HighlightSpan {
                text: line.to_string(),
                rgb: None,
            }],
        })
        .collect()
}

pub(crate) fn file_edit_render(tool: &str, input: &str) -> Option<FileEditRender> {
    let value: serde_json::Value = serde_json::from_str(input).ok()?;
    let tool_name = tool.rsplit('.').next().unwrap_or(tool);
    let path = input_string(&value, &["path", "file", "file_path", "target"]);

    if let Some(patch) = input_string(&value, &["patch"]) {
        return Some(FileEditRender {
            title: "Patch".to_string(),
            code: ChatCodeBlock {
                language: Some("diff".to_string()),
                text: patch,
                is_diff: true,
            },
        });
    }

    if (tool_name == "apply_patch"
        || value.get("changes").is_some()
        || value.get("edits").is_some())
        && let Some(text) = structured_patch_preview(&value)
    {
        return Some(FileEditRender {
            title: "Structured patch".to_string(),
            code: ChatCodeBlock {
                language: Some("diff".to_string()),
                text,
                is_diff: true,
            },
        });
    }

    let old_text = input_string(&value, &["old_text", "old_string", "oldString", "old_str"]);
    let new_text = input_string(
        &value,
        &[
            "new_text",
            "new_string",
            "newString",
            "new_str",
            "replacement",
        ],
    );
    if tool_name == "edit_file" || (old_text.is_some() && new_text.is_some()) {
        let path = path.unwrap_or_else(|| "file".to_string());
        let old_text = old_text.unwrap_or_default();
        let new_text = new_text.unwrap_or_default();
        return Some(FileEditRender {
            title: format!("Edit {path}"),
            code: ChatCodeBlock {
                language: Some("diff".to_string()),
                text: exact_replacement_diff(&path, &old_text, &new_text),
                is_diff: true,
            },
        });
    }

    if (tool_name == "write_file" || value.get("content").is_some())
        && let Some(content) = input_string(&value, &["content"])
    {
        let language = path
            .as_deref()
            .and_then(language_for_path)
            .map(str::to_string);
        return Some(FileEditRender {
            title: path
                .map(|path| format!("Write {path}"))
                .unwrap_or_else(|| "Write file".to_string()),
            code: ChatCodeBlock {
                language: language.clone(),
                is_diff: language.as_deref() == Some("diff"),
                text: content,
            },
        });
    }

    None
}

pub(crate) fn language_for_path(path: &str) -> Option<&'static str> {
    match path.rsplit('.').next()? {
        "rs" => Some("rust"),
        "toml" => Some("toml"),
        "json" => Some("json"),
        "md" | "markdown" => Some("markdown"),
        "ts" | "tsx" => Some("typescript"),
        "js" | "jsx" => Some("javascript"),
        "py" => Some("python"),
        "sh" | "bash" | "zsh" => Some("bash"),
        "yaml" | "yml" => Some("yaml"),
        "diff" | "patch" => Some("diff"),
        "html" => Some("html"),
        "css" => Some("css"),
        _ => None,
    }
}

fn heading_level(level: HeadingLevel) -> u8 {
    match level {
        HeadingLevel::H1 => 1,
        HeadingLevel::H2 => 2,
        HeadingLevel::H3 => 3,
        HeadingLevel::H4 => 4,
        HeadingLevel::H5 => 5,
        HeadingLevel::H6 => 6,
    }
}

fn code_block_language(kind: CodeBlockKind<'_>) -> Option<String> {
    match kind {
        CodeBlockKind::Indented => None,
        CodeBlockKind::Fenced(info) => info
            .split_whitespace()
            .next()
            .map(str::to_string)
            .filter(|language| !language.is_empty()),
    }
}

fn looks_like_diff(text: &str) -> bool {
    let mut diff_lines = 0;
    for line in text.lines().take(24) {
        if line.starts_with("*** Begin Patch")
            || line.starts_with("+++ ")
            || line.starts_with("--- ")
            || line.starts_with("@@")
            || line.starts_with('+')
            || line.starts_with('-')
        {
            diff_lines += 1;
        }
    }
    diff_lines >= 2
}

fn input_string(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(serde_json::Value::as_str))
        .map(str::to_string)
}

fn structured_patch_preview(value: &serde_json::Value) -> Option<String> {
    let changes = value
        .get("changes")
        .or_else(|| value.get("edits"))?
        .as_array()?;
    if changes.is_empty() {
        return None;
    }

    let mut out = String::new();
    for change in changes.iter().take(8) {
        let action = input_string(change, &["action", "op", "type"]).unwrap_or_else(|| {
            if change.get("content").is_some() {
                "write".to_string()
            } else {
                "edit".to_string()
            }
        });
        let path = input_string(change, &["path", "file", "file_path"]).unwrap_or_default();
        out.push_str(&format!("--- {path}\n+++ {path}\n@@ {action} @@\n"));
        if let Some(old_text) = input_string(change, &["old_text", "old_string", "old"]) {
            for line in old_text.lines() {
                out.push('-');
                out.push_str(line);
                out.push('\n');
            }
        }
        if let Some(new_text) = input_string(
            change,
            &["new_text", "new_string", "new", "content", "replacement"],
        ) {
            for line in new_text.lines() {
                out.push('+');
                out.push_str(line);
                out.push('\n');
            }
        }
    }
    Some(out.trim_end().to_string())
}

fn exact_replacement_diff(path: &str, old_text: &str, new_text: &str) -> String {
    let mut out = format!("--- {path}\n+++ {path}\n@@ exact replacement @@\n");
    for line in old_text.lines() {
        out.push('-');
        out.push_str(line);
        out.push('\n');
    }
    for line in new_text.lines() {
        out.push('+');
        out.push_str(line);
        out.push('\n');
    }
    out.trim_end().to_string()
}

fn style_rgb(style: Style) -> [u8; 3] {
    [style.foreground.r, style.foreground.g, style.foreground.b]
}

fn syntax_set() -> &'static SyntaxSet {
    static SYNTAX_SET: OnceLock<SyntaxSet> = OnceLock::new();
    SYNTAX_SET.get_or_init(SyntaxSet::load_defaults_newlines)
}

fn theme_set() -> &'static ThemeSet {
    static THEME_SET: OnceLock<ThemeSet> = OnceLock::new();
    THEME_SET.get_or_init(ThemeSet::load_defaults)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sui::Color;

    fn markdown_styles() -> ChatMarkdownStyles {
        let body = TextStyle::new(Color::WHITE);
        let headings = std::array::from_fn(|_| {
            let mut style = body.clone();
            style.weight = FontWeight::BOLD;
            style
        });
        let mut inline_code = body.clone();
        inline_code.font_size = 12.0;
        let mut link = body.clone();
        link.color = Color::rgba(0.2, 0.7, 0.9, 1.0);
        ChatMarkdownStyles {
            body,
            headings,
            inline_code,
            link,
        }
    }

    #[test]
    fn markdown_rich_render_keeps_blocks_inline_styles_and_source() {
        let source = "# Plan\n\nUse **strong** and `code`.\n\n```rust\nfn main() {}\n```";
        let render = markdown_rich_render(source, markdown_styles());
        let rendered = render.source_map.rendered();

        assert!(matches!(
            render.blocks.first(),
            Some(ChatRichRenderBlock::Text { document, .. })
                if document.plain_text() == "Plan"
                    && document.paragraphs[0].spans[0].style.weight == FontWeight::BOLD
        ));
        assert!(render.blocks.iter().any(|block| matches!(
            block,
            ChatRichRenderBlock::Text { document, .. }
                if document.paragraphs[0]
                    .spans
                    .iter()
                    .any(|span| span.text == "strong" && span.style.weight == FontWeight::BOLD)
        )));
        assert!(matches!(
            render.blocks.last(),
            Some(ChatRichRenderBlock::Code { code, .. })
                if code.language.as_deref() == Some("rust") && code.text.contains("fn main")
        ));

        let strong_start = rendered.find("strong").expect("rendered strong");
        assert_eq!(
            render
                .source_map
                .copy_range(strong_start..strong_start + "strong".len()),
            "**strong**"
        );
        let code_start = rendered.find("fn main").expect("rendered code block");
        assert_eq!(
            render
                .source_map
                .copy_range(code_start..code_start + "fn main() {}".len()),
            "```rust\nfn main() {}\n```"
        );
    }

    #[test]
    fn markdown_rich_render_preserves_table_structure() {
        let render = markdown_rich_render(
            "| Name | State |\n| --- | --- |\n| alpha | ready |",
            markdown_styles(),
        );
        let rendered = render.source_map.rendered();

        assert!(rendered.contains("Name  |  State"), "{rendered:?}");
        assert!(rendered.contains("alpha  |  ready"), "{rendered:?}");
        assert!(rendered.contains('\n'), "{rendered:?}");
    }

    #[test]
    fn edit_file_input_becomes_diff_block() {
        let input = serde_json::json!({
            "path": "src/lib.rs",
            "old_text": "fn old() {}",
            "new_text": "fn new() {}"
        })
        .to_string();

        let rendered = file_edit_render("edit_file", &input).expect("edit diff");

        assert!(rendered.code.is_diff);
        assert_eq!(rendered.code.language.as_deref(), Some("diff"));
        assert!(rendered.code.text.contains("--- src/lib.rs"));
        assert!(rendered.code.text.contains("-fn old() {}"));
        assert!(rendered.code.text.contains("+fn new() {}"));
    }

    #[test]
    fn highlighted_code_keeps_text_segments() {
        let lines = highlight_code(Some("rust"), "fn main() {}", true);
        let joined = lines
            .into_iter()
            .flatten()
            .map(|span| span.text)
            .collect::<String>();

        assert_eq!(joined, "fn main() {}");
    }
}
