use std::sync::OnceLock;

use syntect::easy::HighlightLines;
use syntect::highlighting::{Style, ThemeSet};
use syntect::parsing::SyntaxSet;

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
