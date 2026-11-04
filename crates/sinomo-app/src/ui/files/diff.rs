//! Passive unified patch review for document tabs and SIFS version history.
use super::*;
use sinomo_client::api::{
    DFS_BROWSER_RANGE_LIMIT, DaemonDfsBrowserReadObjectRangeRequest as Range,
};

const INPUT_LIMIT: usize = 512 * 1024;
const PATCH_LIMIT: usize = 2 * 1024 * 1024;
const PATCH_LINES: usize = 32_768;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Review {
    files: Vec<FileDiff>,
    truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FileDiff {
    name: String,
    hunks: Vec<String>,
}
impl FileDiff {
    fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            hunks: vec![String::new()],
        }
    }
}

fn counts(header: &str) -> Option<(usize, usize)> {
    let mut parts = header.strip_prefix("@@ ")?.split_whitespace();
    fn count(range: &str, sign: char) -> Option<usize> {
        let range = range.strip_prefix(sign)?;
        let (start, length) = range.split_once(',').unwrap_or((range, "1"));
        start.parse::<usize>().ok()?;
        length.parse().ok()
    }
    Some((count(parts.next()?, '-')?, count(parts.next()?, '+')?))
}

pub(super) fn patch(text: &str) -> Review {
    let end = text.floor_char_boundary(text.len().min(PATCH_LIMIT));
    let mut lines = text[..end].split_inclusive('\n').peekable();
    let mut files = Vec::new();
    let mut file = FileDiff::new("Patch");
    let mut has_file = false;
    let mut has_pair = false;
    let mut has_hunk = false;
    let mut remaining = (0usize, 0usize);
    for _ in 0..PATCH_LINES {
        let Some(line) = lines.next() else {
            break;
        };
        let git = line.strip_prefix("diff --git ");
        let pair = remaining == (0, 0)
            && line.starts_with("--- ")
            && lines.peek().is_some_and(|s| s.starts_with("+++ "));
        if git.is_some() || (pair && (has_pair || has_hunk)) {
            let name = git.unwrap_or("Patch").trim_end();
            if has_file {
                files.push(file);
                file = FileDiff::new(name);
            } else {
                file.name = name.into();
            }
            has_file = true;
            has_pair = false;
            has_hunk = false;
            remaining = (0, 0);
        }
        if pair {
            let old = line[4..].trim_end().split('\t').next().unwrap_or("");
            let new = lines.peek().unwrap()[4..]
                .trim_end()
                .split('\t')
                .next()
                .unwrap_or("");
            file.name = if new == "/dev/null" { old } else { new }.into();
            has_pair = true;
            has_file = true;
        }
        if let Some(lengths) = counts(line) {
            if has_hunk {
                file.hunks.push(String::new());
            }
            has_hunk = true;
            remaining = lengths;
        } else if remaining != (0, 0) {
            if line.starts_with([' ', '-']) {
                remaining.0 = remaining.0.saturating_sub(1);
            }
            if line.starts_with([' ', '+']) {
                remaining.1 = remaining.1.saturating_sub(1);
            }
        }
        file.hunks.last_mut().unwrap().push_str(line);
    }
    if files.is_empty() && file.hunks[0].is_empty() {
        file.hunks[0] = "Empty patch".into();
    }
    files.push(file);
    Review {
        files,
        truncated: end < text.len() || lines.peek().is_some(),
    }
}

impl Review {
    pub(super) fn view(&self, reader: &ThemeReader) -> RebuildOnChange<(usize, usize)> {
        let selection = sui::Signal::new((0usize, 0usize));
        let target = selection.clone();
        let review = self.clone();
        let reader = reader.clone();
        RebuildOnChange::key_from(selection, move |&(f, h)| {
            let file = &review.files[f];
            let nav = |label: &'static str, icon, next: Option<(usize, usize)>| {
                let target = target.clone();
                IconButton::new(icon, label)
                    .size(28.)
                    .theme_when(clone_reader(&reader))
                    .enabled(next.is_some())
                    .on_press(move || {
                        if let Some(pos) = next {
                            target.set(pos);
                        }
                    })
            };
            let mut tools = Flex::horizontal().gap(4.).align_items(Alignment::Center);
            let title = if review.files.len() > 1 {
                tools = tools
                    .with_child(nav(
                        "Previous file",
                        IconGlyph::ChevronLeft,
                        f.checked_sub(1).map(|i| (i, 0)),
                    ))
                    .with_child(nav(
                        "Next file",
                        IconGlyph::ChevronRight,
                        (f + 1 < review.files.len()).then_some((f + 1, 0)),
                    ));
                format!("{}/{}  {}", f + 1, review.files.len(), file.name)
            } else {
                file.name.clone()
            };
            tools = tools.with_item(
                Label::new(title)
                    .single_line(true)
                    .theme_when(clone_reader(&reader)),
                FlexItem::fill(),
            );
            if file.hunks.len() > 1 {
                tools = tools
                    .with_child(nav(
                        "Previous hunk",
                        IconGlyph::ChevronUp,
                        h.checked_sub(1).map(|i| (f, i)),
                    ))
                    .with_child(
                        Label::new(format!("Hunk {}/{}", h + 1, file.hunks.len()))
                            .theme_when(clone_reader(&reader)),
                    )
                    .with_child(nav(
                        "Next hunk",
                        IconGlyph::ChevronDown,
                        (h + 1 < file.hunks.len()).then_some((f, h + 1)),
                    ));
            }
            let theme = reader();
            let text = &file.hunks[h];
            let mut pos = 0;
            let mut spans = Vec::new();
            for line in text.split_inclusive('\n') {
                let start = pos;
                pos += line.len();
                let color = match line.as_bytes().first() {
                    Some(b'+') => theme.palette.success,
                    Some(b'-') => theme.palette.danger,
                    Some(b'@' | b'\\') => theme.palette.accent,
                    _ => continue,
                };
                spans.push(sui::TextSurfaceStyleSpan::new(
                    start..pos,
                    theme.mono_text_style(color),
                ));
            }
            let mut surface = TextSurface::new("Unified diff")
                .value(text.clone())
                .read_only(true)
                .wrap(TextWrap::NoWrap)
                .theme_when(clone_reader(&reader))
                .text_style_when(|t| t.mono_text_style(t.palette.text))
                .style_spans(spans);
            surface.set_selection(0, 0);
            let mut body = Dock::new(surface).top(32., tools);
            if review.truncated {
                body = body.bottom(
                    24.,
                    Label::new("Partial patch preview · Review limit reached")
                        .theme_when(clone_reader(&reader)),
                );
            }
            WidgetPod::new(body)
        })
    }
}

pub(super) fn compare(
    browser: &SifsBrowser,
    identity: &DfsFileVersionIdentity,
    saved_path: &str,
    hash: &str,
    size: u64,
    path: &str,
) -> Result<Review, String> {
    let head = browser.stat(path)?;
    if head.file_system_id != identity.file_system_id.to_string()
        || head.object_id != identity.object_id.to_string()
        || head.kind != "file"
    {
        return Err(
            "The file at this path changed identity. Refresh history to compare again.".into(),
        );
    }
    let current_size = head.size_bytes.ok_or("Current version has no size")?;
    let current_hash = head
        .content_hash
        .as_deref()
        .ok_or("Current version has no content hash")?;
    let metadata = format!(
        "Saved {} · {} bytes\n{}\nCurrent {} · {} bytes\n{}",
        identity.version, size, hash, head.version, current_size, current_hash
    );
    let summary = |reason: &str| Review {
        files: vec![FileDiff {
            name: path.into(),
            hunks: vec![format!("{reason}\n\n{metadata}")],
        }],
        truncated: false,
    };
    if size == current_size && hash == current_hash {
        return Ok(summary(if size == 0 {
            "Both versions are empty"
        } else {
            "Identical content"
        }));
    }
    if size.max(current_size) > INPUT_LIMIT as u64 {
        return Ok(summary("Content differs · Size and hash comparison"));
    }
    let read = |version: &str, hash: &str, size: u64| -> Result<Vec<u8>, String> {
        let mut bytes = Vec::with_capacity(size as usize);
        while (bytes.len() as u64) < size {
            let offset = bytes.len() as u64;
            let length = (size - offset).min(DFS_BROWSER_RANGE_LIMIT as u64) as u32;
            let chunk = browser.read_object_range(&Range {
                object_id: head.object_id.clone(),
                version: version.into(),
                content_hash: hash.into(),
                size_bytes: size,
                offset,
                length,
            })?;
            if chunk.len() != length as usize {
                return Err("Incomplete version read".into());
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    };
    let old = read(&identity.version.to_string(), hash, size)?;
    let new = read(&head.version, current_hash, current_size)?;
    if !preview_bytes_are_text(&old) || !preview_bytes_are_text(&new) {
        return Ok(summary("Binary content differs"));
    }
    let diff = similar::TextDiff::configure()
        .timeout(Duration::from_millis(300))
        .diff_lines(
            std::str::from_utf8(&old).unwrap(),
            std::str::from_utf8(&new).unwrap(),
        );
    // A legal filename can contain patch delimiters, including newlines.
    let quote_path = |p: &str| {
        if p.chars().any(char::is_control) {
            format!("{p:?}")
        } else {
            p.into()
        }
    };
    let saved_path = quote_path(saved_path);
    let path = quote_path(path);
    let old_label = format!(
        "{saved_path}\t{}{}",
        identity.version,
        if old.is_empty() { " (empty)" } else { "" }
    );
    let new_label = format!(
        "{path}\t{}{}",
        head.version,
        if new.is_empty() { " (empty)" } else { "" }
    );
    Ok(patch(
        &diff
            .unified_diff()
            .header(&old_label, &new_label)
            .to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn diff_review_patch_tab_navigates_files_and_hunks() -> sui::Result<()> {
        let source = "--- before\n+++ after\n@@ -1 +1 @@\n-old\n+new\n@@ -9 +9 @@\n-away\n+here\n--- /dev/null\n+++ added\n@@ -0,0 +1 @@\n+created\n\\ No newline at end of file\n";
        let root = sinomo_testdir::TestDir::new("diff-review-document");
        let path = root.join("example.patch");
        fs::write(&path, source).unwrap();
        let path = path.to_str().unwrap();
        let loaded = load_local_text_document(path, FILE_DOCUMENT_MAX_BYTES).unwrap();
        assert_eq!(loaded.format, DocumentFormat::Diff);
        let shared = Shared::new(crate::state::AppState::ready(Backend::local_fallback()));
        shared
            .open_document_tab(FilesSourceKind::Local, path.into(), "example.patch".into())
            .unwrap();
        let scope = shared.active_scope();
        {
            let mut files = scope.files_mut();
            files.pending_document_request_id = Some(1);
            assert!(files.apply_worker_result(FilesWorkerResult::Document {
                request_id: 1,
                source: FilesSourceKind::Local,
                path: path.into(),
                result: Ok(loaded),
            }));
        }
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Patch review")
                    .root(document_view(scope.clone())),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let win = app.main_window()?;
        for width in [920., 420.] {
            win.root()
                .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(width, 620.))))?;
            app.run_until_idle()?;
            let click = |name| {
                win.get_by_role(SemanticsRole::Button)
                    .with_name(name)
                    .click()
            };
            click("Next hunk")?;
            let displayed = || -> sui::Result<String> {
                let value = win
                    .snapshot()?
                    .accessibility
                    .nodes
                    .into_iter()
                    .find(|n| {
                        n.role == SemanticsRole::TextInput
                            && n.name.as_deref() == Some("Unified diff")
                    })
                    .unwrap()
                    .value;
                let Some(SemanticsValue::Text(text)) = value else {
                    panic!("Missing diff text")
                };
                Ok(text)
            };
            assert!(displayed()?.contains("+here"));
            click("Next file")?;
            assert!(displayed()?.contains("No newline"));
            win.capture_screenshot()?
                .write_png(std::env::temp_dir().join(format!("sinomo-diff-review-{width}.png")))?;
            click("Previous file")?;
        }
        assert_eq!(scope.files().document.as_ref().unwrap().text, source);
        Ok(())
    }
    #[test]
    fn headers_in_hunk_content_are_not_files() {
        let review = patch(
            "--- a\n+++ b\n@@ -1 +1 @@\n--- content\n+++ content\n@@ -8 +8 @@\n-x\n+y\n\\ No newline at end of file\n--- /dev/null\n+++ created\n@@ -0,0 +1 @@\n+new\n",
        );
        assert_eq!(review.files.len(), 2);
        assert_eq!(review.files[0].hunks.len(), 2);
        assert!(review.files[0].hunks[1].contains("No newline"));
        assert_eq!(review.files[1].name, "created");
    }
}
