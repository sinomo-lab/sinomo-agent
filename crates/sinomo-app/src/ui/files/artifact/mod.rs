//! Read-only artifact inspection. The document tab owns the draft; one background
//! worker owns the reader, its snapshot, and at most one open member per archive level.
use super::*;
use serde_json::Value as Json;
use sui::Signal;
mod archive;
mod data;
mod input;
#[cfg(test)]
mod tests;
mod ui;
use archive::Archive;
pub(crate) use archive::Assets;
use input::Backing;

#[cfg(test)]
pub(in crate::ui) fn test_archive_assets(bytes: Vec<u8>, directory: &str) -> Assets {
    Assets {
        archive: Archive::open(Backing::memory(bytes)).unwrap(),
        directory: directory.into(),
    }
}

const PAGE: usize = 64;
const MAX_COLS: usize = 24;
fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}
fn short(s: &str) -> String {
    middle_short(s, 256)
}
#[derive(Clone, Default, PartialEq)]
struct Query {
    members: Vec<String>,
    folder: String,
    table: Option<String>,
    offset: u64,
    columns: Vec<usize>,
    schema: bool,
    header: bool,
}
impl Query {
    fn reset(&mut self) {
        self.offset = 0;
        self.columns.clear();
        self.schema = false;
        self.table = None;
    }
    fn back(&mut self) {
        if self.schema {
            self.schema = false;
            self.offset = 0;
            return;
        }
        if self.table.is_some() {
            self.reset();
            return;
        }
        if !self.folder.is_empty() {
            self.folder = self.folder.rsplit_once('/').map_or("", |p| p.0).into();
            self.offset = 0;
            return;
        }
        if let Some(member) = self.members.pop() {
            self.folder = member.rsplit_once('/').map_or("", |p| p.0).into();
            self.reset();
        }
    }
}
#[derive(Clone)]
enum Action {
    Folder(String),
    Member(String),
    Table(String),
    Column(usize),
    None,
}
#[derive(Clone)]
struct Entry {
    name: String,
    detail: String,
    action: Action,
}
struct Grid {
    offset: u64,
    cols: Vec<String>,
    rows: Vec<Vec<Json>>,
    note: String,
    total: Option<u64>,
    more: bool,
}
enum Screen {
    Grid(Grid),
    List {
        directory: bool,
        title: String,
        items: Vec<Entry>,
        total: Option<u64>,
        more: bool,
    },
    Info(String),
    Member {
        format: DocumentFormat,
        text: String,
        bytes: Arc<[u8]>,
        path: String,
        assets: Assets,
    },
}
fn list(title: &str, mut items: Vec<Entry>, total: Option<u64>) -> Screen {
    let more = items.len() > PAGE;
    items.truncate(PAGE);
    Screen::List {
        directory: false,
        title: title.into(),
        items,
        total,
        more,
    }
}
fn schema_page(q: &Query, schema: &[(String, String)]) -> Screen {
    let rows = schema
        .iter()
        .enumerate()
        .skip(q.offset as usize)
        .take(PAGE + 1)
        .map(|(i, (name, ty))| Entry {
            name: name.clone(),
            detail: format!("{}{}", if q.columns.contains(&i) { "✓ " } else { "" }, ty),
            action: Action::Column(i),
        })
        .collect();
    list(
        "Schema · select up to 24 columns",
        rows,
        Some(schema.len() as u64),
    )
}
fn grid(
    q: &Query,
    schema: Vec<(String, String)>,
    cols: Vec<usize>,
    mut rows: Vec<Vec<Json>>,
    total: Option<u64>,
    note: String,
) -> Screen {
    if q.schema {
        return schema_page(q, &schema);
    }
    let more = rows.len() > PAGE;
    rows.truncate(PAGE);
    Screen::Grid(Grid {
        offset: q.offset,
        cols: cols.iter().map(|i| schema[*i].0.clone()).collect(),
        rows,
        total,
        more,
        note,
    })
}
enum Dataset {
    Tabular(data::Data),
    Archive(Archive),
    Member {
        format: DocumentFormat,
        text: String,
        bytes: Arc<[u8]>,
        assets: Assets,
        name: String,
    },
}
impl Dataset {
    fn page(&mut self, q: &Query) -> Result<Screen, String> {
        match self {
            Self::Tabular(d) => d.page(q),
            Self::Archive(a) => a.listing(q),
            Self::Member {
                format,
                text,
                bytes,
                assets,
                name,
            } => Ok(Screen::Member {
                format: *format,
                text: text.clone(),
                bytes: bytes.clone(),
                assets: assets.clone(),
                path: name.clone(),
            }),
        }
    }
}
struct Session {
    root: Dataset,
    stack: Vec<(String, Dataset)>,
}
impl Session {
    fn page(&mut self, q: &Query) -> Result<Screen, String> {
        let common = q
            .members
            .iter()
            .zip(&self.stack)
            .take_while(|(a, (b, _))| *a == b)
            .count();
        self.stack.truncate(common);
        if q.members.len() > 3 {
            return Err("Nested archive preview limit reached".into());
        }
        for name in &q.members[common..] {
            let Dataset::Archive(archive) = self.stack.last().map(|p| &p.1).unwrap_or(&self.root)
            else {
                return Err("Not an archive".into());
            };
            let input = archive.input(name)?;
            let mut prefix = [0; 16];
            let length = input.size().min(16) as usize;
            input
                .reader()
                .read_exact(&mut prefix[..length])
                .map_err(err)?;
            let kind = DocumentFormat::artifact(name, &prefix[..length]);
            let next = match kind {
                Some(DocumentFormat::Zip | DocumentFormat::Npz) => {
                    Dataset::Archive(Archive::open(input)?)
                }
                Some(format) => {
                    if format == DocumentFormat::Sqlite
                        && ["-wal", "-journal"]
                            .iter()
                            .any(|s| archive.has(&format!("{name}{s}")))
                    {
                        return Err("SQLite has a journal inside the archive. Open a checkpointed database snapshot.".into());
                    }
                    Dataset::Tabular(data::Data::open(format, input, None)?)
                }
                None => {
                    let format = formats::text_format(name);
                    if matches!(format, DocumentFormat::Csv | DocumentFormat::Tsv) {
                        Dataset::Tabular(data::Data::open(format, input, None)?)
                    } else {
                        let bytes = input.bytes(64 * 1024 * 1024)?;
                        let (format, text) = if bytes.starts_with(b"%PDF-") {
                            (DocumentFormat::Pdf, String::new())
                        } else if bytes.len() > FILE_DOCUMENT_MAX_BYTES as usize
                            && !preview_bytes_are_text(&bytes)
                        {
                            return Err(
                                "Binary member exceeds the 2 MiB inline preview limit".into()
                            );
                        } else {
                            formats::document_from_bytes(name, &bytes)
                        };
                        Dataset::Member {
                            format,
                            text,
                            bytes: bytes.into(),
                            name: name.clone(),
                            assets: Assets {
                                archive: archive.clone(),
                                directory: name.rsplit_once('/').map_or("", |p| p.0).into(),
                            },
                        }
                    }
                }
            };
            self.stack.push((name.clone(), next));
        }
        self.stack
            .last_mut()
            .map(|v| &mut v.1)
            .unwrap_or(&mut self.root)
            .page(q)
    }
}

pub(super) fn view(doc: &FilesDocument, shared: &Shared, reader: &ThemeReader) -> WidgetPod {
    let files = shared.files();
    let open = opener(doc, files.daemon_address.clone(), files.app_runtime.clone());
    ui::viewer(open, doc.format, doc.title.clone(), shared, reader)
}
fn opener(
    doc: &FilesDocument,
    address: Option<String>,
    runtime: tokio::runtime::Handle,
) -> impl FnOnce(Arc<AtomicBool>) -> Result<Session, String> + Send + 'static {
    let format = doc.format;
    let local = doc.local_fingerprint.clone();
    let remote = doc.sifs_fingerprint.clone();
    let path = doc.path.clone();
    let buffer = (!doc.deferred_csv() && !doc.streamed_artifact()).then(|| doc.text.clone());
    move |closed| -> Result<Session, String> {
        let local_path = local.as_ref().map(|f| f.canonical_path.clone());
        let source = if let Some(text) = buffer {
            let bytes = if format.hex_buffer() {
                formats::document_bytes(format, &text)?
            } else {
                text.into_bytes()
            };
            paged::Source::Memory(bytes)
        } else if let Some(f) = local {
            paged::Source::Local(fs::File::open(&f.canonical_path).map_err(err)?, f)
        } else {
            let browser = sinomo_client::SifsBrowser::open_daemon_on_runtime(
                address.as_deref().ok_or("SIFS unavailable")?,
                &runtime,
            )?;
            if format == DocumentFormat::Sqlite {
                for suffix in ["-wal", "-journal"] {
                    if browser.path_exists(&format!("{path}{suffix}"))? {
                        return Err("This SIFS database has a journal. Create a checkpointed SQLite snapshot before previewing it.".into());
                    }
                }
            }
            paged::Source::Sifs(browser, remote.ok_or("Missing SIFS version")?)
        };
        let mut source = Backing::new(source);
        source.closed = closed;
        let root = if matches!(format, DocumentFormat::Zip | DocumentFormat::Npz) {
            Dataset::Archive(Archive::open(source)?)
        } else {
            Dataset::Tabular(data::Data::open(format, source, local_path)?)
        };
        Ok(Session {
            root,
            stack: vec![],
        })
    }
}
