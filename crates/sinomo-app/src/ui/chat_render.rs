use std::sync::OnceLock;

use pulldown_cmark::{
    CodeBlockKind, Event as MarkdownEvent, HeadingLevel, Options, Parser, Tag, TagEnd,
};
use syntect::easy::HighlightLines;
use syntect::highlighting::{Style, ThemeSet};
use syntect::parsing::SyntaxSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ChatRenderBlock {
    Paragraph(String),
    Heading { level: u8, text: String },
    ListItem { marker: String, text: String },
    Code(ChatCodeBlock),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ChatCodeBlock {
    pub language: Option<String>,
    pub text: String,
    pub is_diff: bool,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MarkdownBlockKind {
    Paragraph,
    Heading(u8),
    Item,
}

#[derive(Default)]
struct MarkdownBlockBuilder {
    blocks: Vec<ChatRenderBlock>,
    current: Option<MarkdownBlockKind>,
    text: String,
    code_language: Option<String>,
    code: String,
    lists: Vec<Option<u64>>,
}

pub(crate) fn markdown_blocks(markdown: &str) -> Vec<ChatRenderBlock> {
    let mut options = Options::empty();
    options.insert(Options::ENABLE_STRIKETHROUGH);
    options.insert(Options::ENABLE_TABLES);

    let mut builder = MarkdownBlockBuilder::default();
    for event in Parser::new_ext(markdown, options) {
        match event {
            MarkdownEvent::Start(Tag::Paragraph) => {
                if builder.current.is_none() {
                    builder.start(MarkdownBlockKind::Paragraph);
                }
            }
            MarkdownEvent::End(TagEnd::Paragraph) => {
                if !matches!(builder.current, Some(MarkdownBlockKind::Item)) {
                    builder.finish();
                }
            }
            MarkdownEvent::Start(Tag::Heading { level, .. }) => {
                builder.start(MarkdownBlockKind::Heading(heading_level(level)));
            }
            MarkdownEvent::End(TagEnd::Heading(_)) => builder.finish(),
            MarkdownEvent::Start(Tag::List(start)) => builder.lists.push(start),
            MarkdownEvent::End(TagEnd::List(_)) => {
                builder.finish();
                builder.lists.pop();
            }
            MarkdownEvent::Start(Tag::Item) => {
                let marker = builder.next_list_marker();
                builder.start(MarkdownBlockKind::Item);
                builder.text.push_str(&marker);
                builder.text.push(' ');
            }
            MarkdownEvent::End(TagEnd::Item) => builder.finish(),
            MarkdownEvent::Start(Tag::CodeBlock(kind)) => {
                builder.finish();
                builder.code_language = code_block_language(kind);
                builder.code.clear();
            }
            MarkdownEvent::End(TagEnd::CodeBlock) => builder.finish_code(),
            MarkdownEvent::Text(text) => {
                if builder.code_language.is_some() || !builder.code.is_empty() {
                    builder.code.push_str(&text);
                } else {
                    builder.add_text(&text);
                }
            }
            MarkdownEvent::Code(code) => {
                builder.add_text("`");
                builder.add_text(&code);
                builder.add_text("`");
            }
            MarkdownEvent::SoftBreak | MarkdownEvent::HardBreak => builder.add_text("\n"),
            MarkdownEvent::Rule => {
                builder.finish();
                builder
                    .blocks
                    .push(ChatRenderBlock::Paragraph("---".to_string()));
            }
            _ => {}
        }
    }

    builder.finish();
    if builder.code_language.is_some() || !builder.code.is_empty() {
        builder.finish_code();
    }
    if builder.blocks.is_empty() && !markdown.trim().is_empty() {
        builder
            .blocks
            .push(ChatRenderBlock::Paragraph(markdown.trim().to_string()));
    }
    builder.blocks
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

impl MarkdownBlockBuilder {
    fn start(&mut self, kind: MarkdownBlockKind) {
        self.finish();
        self.current = Some(kind);
        self.text.clear();
    }

    fn add_text(&mut self, text: &str) {
        if text.is_empty() {
            return;
        }
        if self.current.is_none() {
            self.start(MarkdownBlockKind::Paragraph);
        }
        self.text.push_str(text);
    }

    fn finish(&mut self) {
        let Some(kind) = self.current.take() else {
            return;
        };
        let text = self.text.trim().to_string();
        self.text.clear();
        if text.is_empty() {
            return;
        }
        match kind {
            MarkdownBlockKind::Paragraph => self.blocks.push(ChatRenderBlock::Paragraph(text)),
            MarkdownBlockKind::Heading(level) => {
                self.blocks.push(ChatRenderBlock::Heading { level, text });
            }
            MarkdownBlockKind::Item => {
                let (marker, text) = split_list_marker(&text);
                self.blocks.push(ChatRenderBlock::ListItem { marker, text });
            }
        }
    }

    fn finish_code(&mut self) {
        let text = std::mem::take(&mut self.code);
        let language = self.code_language.take();
        if text.trim().is_empty() {
            return;
        }
        let is_diff = language
            .as_deref()
            .is_some_and(|language| matches!(language, "diff" | "patch"))
            || looks_like_diff(&text);
        self.blocks.push(ChatRenderBlock::Code(ChatCodeBlock {
            language,
            text: text.trim_matches('\n').to_string(),
            is_diff,
        }));
    }

    fn next_list_marker(&mut self) -> String {
        let indent = "  ".repeat(self.lists.len().saturating_sub(1));
        match self.lists.last_mut().and_then(Option::as_mut) {
            Some(next) => {
                let current = *next;
                *next += 1;
                format!("{indent}{current}.")
            }
            None => format!("{indent}-"),
        }
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

fn split_list_marker(text: &str) -> (String, String) {
    let mut parts = text.splitn(2, ' ');
    let marker = parts.next().unwrap_or("-").to_string();
    let body = parts.next().unwrap_or_default().trim().to_string();
    (marker, body)
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

    #[test]
    fn markdown_blocks_keep_headings_lists_and_code() {
        let blocks =
            markdown_blocks("# Plan\n\n- Read files\n- Patch code\n\n```rust\nfn main() {}\n```");

        assert!(matches!(
            blocks.first(),
            Some(ChatRenderBlock::Heading { level: 1, text }) if text == "Plan"
        ));
        assert!(matches!(
            blocks.get(1),
            Some(ChatRenderBlock::ListItem { marker, text })
                if marker == "-" && text == "Read files"
        ));
        assert!(matches!(
            blocks.last(),
            Some(ChatRenderBlock::Code(code))
                if code.language.as_deref() == Some("rust") && code.text.contains("fn main")
        ));
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
