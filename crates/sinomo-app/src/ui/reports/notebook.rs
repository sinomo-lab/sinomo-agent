//! Build the passive notebook model on the report worker. Cell layout and navigation
//! are implemented by the bundled script; the original editor text stays untouched.
use serde::Deserialize;

pub(super) const SCRIPT_NAME: &str = "sinomo-notebook.js";
pub(super) const SCRIPT: &str = include_str!("../../../assets/notebook/viewer.js");
const STYLE: &str = include_str!("../../../assets/notebook/viewer.css");
const MAX_SOURCE: usize = 32 * 1024 * 1024;
const MAX_CELLS: usize = 10_000;

#[derive(Deserialize)]
struct Notebook {
    nbformat: u32,
    cells: Option<Vec<Cell>>,
}
#[derive(Deserialize)]
struct Cell {
    cell_type: String,
    source: Lines,
}
#[derive(Deserialize)]
#[serde(untagged)]
enum Lines {
    One(String),
    Many(Vec<String>),
}
impl Lines {
    fn join(self) -> String {
        match self {
            Self::One(s) => s,
            Self::Many(lines) => lines.concat(),
        }
    }
}

fn render(source: &[u8]) -> Result<String, String> {
    if source.len() > MAX_SOURCE {
        return Err("The notebook exceeds the 32 MiB preview limit.".into());
    }
    let source = std::str::from_utf8(source)
        .map_err(|e| e.to_string())?
        .trim_start_matches('\u{feff}');
    let nb: Notebook = serde_json::from_str(source).map_err(|e| e.to_string())?;
    if nb.nbformat != 4 {
        return Err(format!(
            "Notebook format {} is unsupported. This viewer reads nbformat 4.",
            nb.nbformat
        ));
    }
    let cells = nb.cells.ok_or("Missing notebook cells")?;
    if cells.len() > MAX_CELLS {
        return Err("The notebook exceeds the 10,000-cell preview limit.".into());
    }
    let mut markdown = Vec::with_capacity(cells.len());
    for cell in cells {
        if cell.cell_type == "markdown" {
            let mut html = String::new();
            pulldown_cmark::html::push_html(
                &mut html,
                pulldown_cmark::Parser::new_ext(
                    &cell.source.join(),
                    pulldown_cmark::Options::ENABLE_TABLES
                        | pulldown_cmark::Options::ENABLE_STRIKETHROUGH,
                ),
            );
            markdown.push(Some(html));
        } else {
            markdown.push(None);
        }
    }
    // Script data is raw text in HTML. Escape '<' in *both* JSON blocks so even a
    // literal closing script tag remains data when the browser parses the page.
    Ok(format!(
        r#"<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Notebook preview</title><style>{STYLE}</style>
<header><strong>Notebook</strong><span id="summary"></span><button id="next" hidden>Next error ↓</button></header><main id="cells"></main>
<script type="application/json" id="notebook">{}</script><script type="application/json" id="markdown">{}</script><script src="{SCRIPT_NAME}"></script>"#,
        source.replace('<', "\\u003c"),
        serde_json::to_string(&markdown)
            .map_err(|e| e.to_string())?
            .replace('<', "\\u003c")
    ))
}

pub(super) fn page(source: &[u8]) -> String {
    render(source).unwrap_or_else(|error| {
        let error = error.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;");
        format!("<!doctype html><meta charset=utf-8><style>{STYLE}</style><main><h2>Notebook preview unavailable</h2><p>{error}</p><p>Open Source to inspect or repair the notebook.</p></main>")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn notebook_invalid_and_unsupported_sources_offer_recovery() {
        for (source, reason) in [
            (b"{".as_slice(), "EOF"),
            (br#"{"nbformat":3,"worksheets":[]}"#, "format 3"),
            (
                br#"{"nbformat":4,"cells":[{"cell_type":"code","source":17}]}"#,
                "type",
            ),
        ] {
            let html = page(source);
            assert!(
                html.contains("preview unavailable")
                    && html.contains(reason)
                    && html.contains("Open Source"),
                "{html}"
            );
        }
        assert!(render(br#"{"nbformat":4,"cells":[]}"#).is_ok());
    }
}
