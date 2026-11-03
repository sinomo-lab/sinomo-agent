//! Bounded, version-pinned browsing of logs and newline-delimited JSON.
use super::*;
use std::io::{Seek, SeekFrom};
use sui::Signal;

const BLOCK: usize = sinomo_client::api::DFS_BROWSER_RANGE_LIMIT as usize;
const PAGE_BYTES: usize = 16 * 1024;
const PAGE_ROWS: usize = 64;
const RECORD_BYTES: usize = 64 * 1024;

enum Source {
    Memory(Vec<u8>),
    Local(fs::File, LocalFileFingerprint),
    Sifs(sinomo_client::SifsBrowser, SifsFileFingerprint),
}
impl Source {
    fn len(&self) -> u64 {
        match self {
            Self::Memory(bytes) => bytes.len() as u64,
            Self::Local(_, f) => f.len,
            Self::Sifs(_, f) => f.size_bytes.unwrap_or(0),
        }
    }
    fn read(&mut self, offset: u64, length: usize) -> Result<Vec<u8>, String> {
        match self {
            Self::Memory(bytes) => Ok(bytes[offset as usize..offset as usize + length].to_vec()),
            Self::Local(file, f) => {
                let verify = |file: &fs::File| -> Result<(), String> {
                    let meta = file.metadata().map_err(|e| e.to_string())?;
                    if meta.len() != f.len
                        || meta.modified().ok().and_then(system_time_unix_nanos)
                            != f.modified_at_unix_nanos
                    {
                        return Err("Local file changed. Reopen it to read a new snapshot.".into());
                    }
                    Ok(())
                };
                verify(file)?;
                file.seek(SeekFrom::Start(offset))
                    .map_err(|e| e.to_string())?;
                let mut bytes = vec![0; length];
                file.read_exact(&mut bytes).map_err(|e| e.to_string())?;
                verify(file)?;
                Ok(bytes)
            }
            Self::Sifs(browser, f) => browser.read_object_range(
                &sinomo_client::api::DaemonDfsBrowserReadObjectRangeRequest {
                    object_id: f.object_id.clone(),
                    version: f.version.clone(),
                    content_hash: f.content_hash.clone().ok_or("Missing content hash")?,
                    size_bytes: f.size_bytes.ok_or("Missing file size")?,
                    offset,
                    length: length as u32,
                },
            ),
        }
    }
}
struct Reader {
    source: Source,
    cache: Vec<u8>,
    base: Option<u64>,
    // Sparse newline counts; coarsen the index for exceptionally large scans.
    anchors: BTreeMap<u64, u64>,
    stride: u64,
    frontier: u64,
    counted: u64,
}
impl Reader {
    fn new(source: Source) -> Self {
        Self {
            source,
            cache: vec![],
            base: None,
            anchors: BTreeMap::from([(0, 0)]),
            stride: 1024 * 1024,
            frontier: 0,
            counted: 0,
        }
    }
    fn chunk(&mut self, pos: u64, cancel: &dyn Fn() -> bool) -> Result<&[u8], String> {
        if cancel() {
            return Err("Cancelled".into());
        }
        let size = self.source.len();
        if pos >= size {
            return Ok(&[]);
        }
        let base = pos / BLOCK as u64 * BLOCK as u64;
        if self.base != Some(base) {
            let len = (size - base).min(BLOCK as u64) as usize;
            let bytes = self.source.read(base, len)?;
            if bytes.len() != len {
                return Err("Short snapshot range".into());
            }
            if base == self.frontier {
                self.frontier += len as u64;
                self.counted += bytes.iter().filter(|&&b| b == b'\n').count() as u64;
                if self.frontier % self.stride == 0 {
                    self.anchors.insert(self.frontier, self.counted);
                    if self.anchors.len() > 4096 {
                        self.stride *= 2;
                        self.anchors.retain(|offset, _| offset % self.stride == 0);
                    }
                }
            }
            self.base = Some(base);
            self.cache = bytes;
        }
        Ok(&self.cache[(pos - base) as usize..])
    }
    fn line(&mut self, at: u64, cancel: &dyn Fn() -> bool) -> Result<u64, String> {
        let (mut pos, mut count) = if at >= self.frontier {
            (self.frontier, self.counted)
        } else {
            let (&p, &n) = self.anchors.range(..=at).next_back().unwrap();
            (p, n)
        };
        while pos < at {
            let block = self.chunk(pos, cancel)?;
            let n = (at - pos).min(block.len() as u64) as usize;
            count += block[..n].iter().filter(|&&b| b == b'\n').count() as u64;
            pos += n as u64;
        }
        Ok(count + 1)
    }
    fn bytes(
        &mut self,
        start: u64,
        end: u64,
        cancel: &dyn Fn() -> bool,
    ) -> Result<Vec<u8>, String> {
        let mut out = Vec::with_capacity((end - start) as usize);
        let mut pos = start;
        while pos < end {
            let data = self.chunk(pos, cancel)?;
            let n = (end - pos).min(data.len() as u64) as usize;
            out.extend_from_slice(&data[..n]);
            pos += n as u64;
        }
        Ok(out)
    }
    fn before(&mut self, end: u64, cancel: &dyn Fn() -> bool) -> Result<u64, String> {
        let start = end.saturating_sub(PAGE_BYTES as u64);
        let data = self.bytes(start, end, cancel)?;
        let body = data.strip_suffix(b"\n").unwrap_or(&data);
        let breaks: Vec<_> = body
            .iter()
            .enumerate()
            .filter_map(|(i, &b)| (b == b'\n').then_some(i + 1))
            .collect();
        if breaks.len() >= PAGE_ROWS {
            return Ok(start + breaks[breaks.len() - PAGE_ROWS] as u64);
        }
        if start == 0 || self.chunk(start - 1, cancel)?[0] == b'\n' {
            return Ok(start);
        }
        if let Some(&i) = breaks.first() {
            return Ok(start + i as u64);
        }
        Ok(start
            + data
                .iter()
                .take(3)
                .take_while(|&&b| b & 0xc0 == 0x80)
                .count() as u64)
    }
    fn page(
        &mut self,
        start: u64,
        end: u64,
        json: bool,
        cancel: &dyn Fn() -> bool,
    ) -> Result<Page, String> {
        let mut pos = start;
        let mut rows = Vec::new();
        let size = self.source.len();
        while pos < end && rows.len() < PAGE_ROWS && pos - start < PAGE_BYTES as u64 {
            let offset = pos;
            let continued = pos > 0 && self.chunk(pos - 1, cancel)?[0] != b'\n';
            let mut bytes = Vec::new();
            while pos < end && bytes.len() < RECORD_BYTES {
                let block = self.chunk(pos, cancel)?;
                let limit = block
                    .len()
                    .min(RECORD_BYTES - bytes.len())
                    .min((end - pos).min(BLOCK as u64) as usize);
                let part = &block[..limit];
                let n = part
                    .iter()
                    .position(|&b| b == b'\n')
                    .map_or(limit, |i| i + 1);
                bytes.extend_from_slice(&part[..n]);
                pos += n as u64;
                if bytes.last() == Some(&b'\n') {
                    break;
                }
            }
            let newline = bytes.last() == Some(&b'\n');
            let fragment = continued || (!newline && pos < size);
            if !newline && pos < size && self.chunk(pos, cancel)?[0] & 0xc0 == 0x80 {
                if let Err(e) = std::str::from_utf8(&bytes)
                    && e.error_len().is_none()
                    && e.valid_up_to() > 0
                {
                    pos -= (bytes.len() - e.valid_up_to()) as u64;
                    bytes.truncate(e.valid_up_to());
                }
            }
            if newline {
                bytes.pop();
                if bytes.last() == Some(&b'\r') {
                    bytes.pop();
                }
            }
            let utf = String::from_utf8_lossy(&bytes);
            let issue = if matches!(utf, std::borrow::Cow::Owned(_)) {
                Some("Invalid UTF-8".into())
            } else if fragment {
                Some("Long line · fragment".into())
            } else if json {
                serde_json::from_slice::<serde::de::IgnoredAny>(&bytes)
                    .err()
                    .map(|e| {
                        if e.is_eof() && pos == size && !newline {
                            format!("Incomplete JSON · {e}")
                        } else {
                            format!("Invalid JSON · {e}")
                        }
                    })
            } else {
                None
            };
            rows.push(Row {
                offset,
                text: utf.into_owned(),
                issue,
                newline,
            });
        }
        Ok(Page {
            start,
            end: pos,
            size,
            first_line: None,
            rows,
            hit: None,
        })
    }
    fn find(
        &mut self,
        mut pos: u64,
        query: &str,
        cancel: &dyn Fn() -> bool,
    ) -> Result<Option<u64>, String> {
        let needle = query.as_bytes();
        if needle.is_empty() || needle.len() > 4096 {
            return Err("Enter a search term of 1–4096 bytes".into());
        }
        // Carry the matched prefix across RPC boundaries.
        let mut prefix = vec![0; needle.len()];
        let mut matched = 0;
        for i in 1..needle.len() {
            while matched > 0 && needle[i] != needle[matched] {
                matched = prefix[matched - 1];
            }
            if needle[i] == needle[matched] {
                matched += 1;
            }
            prefix[i] = matched;
        }
        matched = 0;
        while pos < self.source.len() {
            let block = self.chunk(pos, cancel)?;
            for (i, &byte) in block.iter().enumerate() {
                while matched > 0 && byte != needle[matched] {
                    matched = prefix[matched - 1];
                }
                if byte == needle[matched] {
                    matched += 1;
                }
                if matched == needle.len() {
                    return Ok(Some(pos + i as u64 + 1 - matched as u64));
                }
            }
            pos += block.len() as u64;
        }
        Ok(None)
    }
    fn record_start(&mut self, at: u64, cancel: &dyn Fn() -> bool) -> Result<u64, String> {
        let start = at.saturating_sub(RECORD_BYTES as u64 - 4);
        let data = self.bytes(start, at, cancel)?;
        Ok(data.iter().rposition(|&b| b == b'\n').map_or_else(
            || {
                start
                    + data
                        .iter()
                        .take(3)
                        .take_while(|&&b| b & 0xc0 == 0x80)
                        .count() as u64
            },
            |i| start + i as u64 + 1,
        ))
    }
}

#[derive(Clone, Debug, PartialEq)]
struct Row {
    offset: u64,
    text: String,
    issue: Option<String>,
    newline: bool,
}
#[derive(Clone, Debug, PartialEq)]
struct Page {
    start: u64,
    end: u64,
    size: u64,
    first_line: Option<u64>,
    rows: Vec<Row>,
    hit: Option<(u64, String)>,
}
#[derive(Clone, Default, PartialEq)]
struct View {
    page: Option<Arc<Page>>,
    message: String,
    busy: bool,
}
enum Action {
    First,
    Previous,
    Next,
    End,
    Search(String),
    Cancel,
}
type Request = (u64, Action, Option<Arc<Page>>);
struct Client {
    sender: mpsc::Sender<Request>,
    epoch: Arc<AtomicU64>,
    gate: Arc<Mutex<()>>,
    state: Signal<View>,
}
impl Client {
    fn send(&self, action: Action) {
        let _guard = self.gate.lock().unwrap();
        let epoch = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;
        let page = self.state.get().page;
        let (message, busy) = match &action {
            Action::Cancel => ("Cancelled", false),
            Action::Search(_) => ("Searching…", true),
            _ => ("Reading page…", true),
        };
        self.state.set(View {
            page: page.clone(),
            message: message.into(),
            busy,
        });
        if self.sender.send((epoch, action, page.clone())).is_err() {
            self.state.set(View {
                page,
                message: "Page worker unavailable".into(),
                busy: false,
            });
        }
    }
}
impl Drop for Client {
    fn drop(&mut self) {
        self.epoch.fetch_add(1, Ordering::SeqCst);
    }
}
fn worker(
    rx: mpsc::Receiver<Request>,
    epoch: Arc<AtomicU64>,
    gate: Arc<Mutex<()>>,
    state: Signal<View>,
    mut open: impl FnMut() -> Result<Source, String>,
    json: bool,
) {
    let mut reader = None;
    for (id, action, previous) in rx {
        let cancel = || epoch.load(Ordering::SeqCst) != id;
        if cancel() || matches!(action, Action::Cancel) {
            continue;
        }
        let publish = |page: Option<Arc<Page>>, message: String, busy| {
            let _guard = gate.lock().unwrap();
            if !cancel() {
                state.set(View {
                    page,
                    message,
                    busy,
                });
            }
        };
        let mut visible = previous.clone();
        let result = (|| -> Result<(), String> {
            if reader.is_none() {
                reader = Some(Reader::new(open()?));
            }
            let r = reader.as_mut().unwrap();
            let size = r.source.len();
            let mut hit = None;
            let mut end = size;
            let start = match action {
                Action::First => 0,
                Action::Next => previous.as_ref().map_or(0, |p| p.end),
                Action::Previous => {
                    end = previous.as_ref().map_or(0, |p| p.start);
                    r.before(end, &cancel)?
                }
                Action::End => r.before(end, &cancel)?,
                Action::Search(query) => {
                    let at = previous.as_ref().map_or(0, |p| {
                        p.hit
                            .as_ref()
                            .filter(|(_, q)| q == &query)
                            .map_or(p.start, |(at, _)| at + 1)
                    });
                    let Some(found) = r.find(at, &query, &cancel)? else {
                        publish(previous.clone(), "No further match".into(), false);
                        return Ok(());
                    };
                    hit = Some((found, query));
                    r.record_start(found, &cancel)?
                }
                Action::Cancel => unreachable!(),
            };
            let mut page = r.page(start, end, json, &cancel)?;
            page.hit = hit;
            if start > r.frontier {
                visible = Some(Arc::new(page.clone()));
                publish(visible.clone(), "Counting preceding lines…".into(), true);
            }
            page.first_line = Some(r.line(start, &cancel)?);
            publish(Some(Arc::new(page)), String::new(), false);
            Ok(())
        })();
        if let Err(error) = result {
            publish(visible, error, false);
        }
    }
}

pub(super) fn view(
    document: &FilesDocument,
    shared: &Shared,
    reader: &ThemeReader,
    active: &source_editor::ActiveSearch,
) -> WidgetPod {
    let local = document.local_fingerprint.clone();
    let sifs = document.sifs_fingerprint.clone();
    let address = shared.files().daemon_address.clone();
    let runtime = shared.files().app_runtime.clone();
    let large = local
        .as_ref()
        .map(|f| f.len)
        .or_else(|| sifs.as_ref().and_then(|f| f.size_bytes))
        .unwrap_or(0)
        > FILE_DOCUMENT_MAX_BYTES;
    let mut memory = (!large || document.dirty()).then(|| document.text.as_bytes().to_vec());
    let json = document.format == DocumentFormat::JsonLines;
    let epoch = Arc::new(AtomicU64::new(0));
    let gate = Arc::new(Mutex::new(()));
    let (sender, rx) = mpsc::channel();
    let state = Signal::new(View::default());
    let args = (epoch.clone(), gate.clone(), state.clone());
    let _ = thread::Builder::new()
        .name("sinomo-file-pages".into())
        .spawn(move || {
            worker(
                rx,
                args.0,
                args.1,
                args.2,
                move || {
                    if let Some(bytes) = memory.take() {
                        return Ok(Source::Memory(bytes));
                    }
                    if let Some(f) = &local {
                        return Ok(Source::Local(
                            fs::File::open(&f.canonical_path).map_err(|e| e.to_string())?,
                            f.clone(),
                        ));
                    }
                    let f = sifs.as_ref().ok_or("Missing SIFS snapshot")?;
                    let browser = sinomo_client::SifsBrowser::open_daemon_on_runtime(
                        address.as_ref().ok_or("SIFS connection unavailable")?,
                        &runtime,
                    )?;
                    Ok(Source::Sifs(browser, f.clone()))
                },
                json,
            )
        });
    let client = Rc::new(Client {
        sender,
        epoch,
        gate,
        state: state.clone(),
    });
    client.send(Action::First);
    let button = |label: &'static str, action: fn() -> Action, enabled: fn(&View) -> bool| {
        let client = client.clone();
        let observe = state.clone();
        sui::Button::new(label)
            .theme_when(clone_reader(reader))
            .enabled_from(observe.select_named(label, enabled))
            .on_press(move || client.send(action()))
    };
    let reload = shared.clone();
    let clean = shared.clone();
    let refresh = IconButton::new(IconGlyph::Redo, "Reload file snapshot")
        .theme_when(clone_reader(reader))
        .enabled_when(move || {
            clean
                .files()
                .document
                .as_ref()
                .is_some_and(|d| !d.dirty() && !d.saving)
        })
        .on_press_with_ctx(move |ctx| {
            let waker = reload.borrow().waker.clone();
            reload.files_mut().retry_document(waker);
            request_files_overlay_rebuild(ctx);
        });
    let nav = Flex::horizontal()
        .gap(4.)
        .align_items(Alignment::Center)
        .with_child(button("Start", || Action::First, |_| true))
        .with_child(button(
            "Previous",
            || Action::Previous,
            |v| v.page.as_ref().is_some_and(|p| p.start > 0),
        ))
        .with_child(button(
            "Next",
            || Action::Next,
            |v| v.page.as_ref().is_some_and(|p| p.end < p.size),
        ))
        .with_child(button("End", || Action::End, |_| true))
        .with_child(refresh);
    let query = Rc::new(RefCell::new(String::new()));
    let edit = query.clone();
    let submit = client.clone();
    let go = client.clone();
    let field = widgets::Boxed::new(
        sui::TextInput::new("Search file")
            .min_width(0.)
            .placeholder("Search file")
            .theme_when(clone_reader(reader))
            .on_change(move |v| *edit.borrow_mut() = v)
            .on_submit_with_ctx(move |_, q| submit.send(Action::Search(q.into()))),
    );
    let search = Flex::horizontal()
        .gap(4.)
        .align_items(Alignment::Center)
        .with_item(field, FlexItem::fill())
        .with_child(
            sui::Button::new("Find next")
                .theme_when(clone_reader(reader))
                .on_press(move || go.send(Action::Search(query.borrow().clone()))),
        )
        .with_child(button("Cancel", || Action::Cancel, |v| v.busy));
    let expanded = Signal::new(None::<u64>);
    let toggle = expanded.clone();
    let r = reader.clone();
    let body = RebuildOnChange::key_from(
        sui::combine_named("log rows", (state.clone(), expanded), |(v, e)| {
            (v.clone(), *e)
        }),
        move |(v, expanded)| {
            let mut body = Stack::vertical().alignment(Alignment::Stretch).gap(3.);
            if let Some(page) = &v.page {
                let mut number = page.first_line;
                for (i, row) in page.rows.iter().enumerate() {
                    let end = page.rows.get(i + 1).map_or(page.end, |next| next.offset);
                    let hit = page
                        .hit
                        .as_ref()
                        .is_some_and(|(at, _)| row.offset <= *at && *at < end);
                    let prefix =
                        number.map_or_else(|| format!("@{}", row.offset), |n| format!("{n}"));
                    let summary = if hit {
                        let at = (page.hit.as_ref().unwrap().0 - row.offset) as usize;
                        let start = row
                            .text
                            .floor_char_boundary(at.saturating_sub(32).min(row.text.len()));
                        middle_short(&row.text[start..], 160)
                    } else {
                        middle_short(&row.text, 160)
                    };
                    let text = format!(
                        "{}{}  {}{}",
                        if hit { "Match · " } else { "" },
                        prefix,
                        row.issue.as_ref().map_or(String::new(), |s| format!(
                            "{} · ",
                            s.split(" · ").next().unwrap_or(s)
                        )),
                        summary
                    );
                    let open = *expanded == Some(row.offset);
                    let offset = row.offset;
                    let toggle = toggle.clone();
                    let expand = IconButton::new(
                        if open {
                            IconGlyph::ChevronDown
                        } else {
                            IconGlyph::ChevronRight
                        },
                        format!("{} line {prefix}", if open { "Collapse" } else { "Expand" }),
                    )
                    .theme_when(clone_reader(&r))
                    .size(24.)
                    .on_press(move || {
                        toggle.set((!open).then_some(offset));
                    });
                    body = body.with_child(
                        TrailingSlotRow::new(
                            Label::new(text)
                                .single_line(true)
                                .text_style_when(live_text_style(&r, |theme| {
                                    theme.mono_text_style(theme.palette.text)
                                })),
                            expand,
                        )
                        .trailing_width(28.)
                        .trailing_height(24.),
                    );
                    if open {
                        if let Some(issue) = &row.issue {
                            body = body
                                .with_child(Label::new(issue.clone()).theme_when(clone_reader(&r)));
                        }
                        let content = if json && row.issue.is_none() {
                            formats::json_preview_content(&row.text)
                                .map(|v| v.0)
                                .unwrap_or_else(|_| row.text.clone())
                        } else {
                            row.text.clone()
                        };
                        body = body.with_child(
                            SizedBox::new().height(160.).child(
                                TextSurface::new("Expanded record")
                                    .value(content)
                                    .read_only(true)
                                    .wrap(TextWrap::Word)
                                    .theme_when(clone_reader(&r)),
                            ),
                        );
                    }
                    if row.newline {
                        number = number.map(|n| n + 1);
                    }
                }
                if page.rows.is_empty() {
                    body = body.with_child(Label::new("Empty file").theme_when(clone_reader(&r)));
                }
            }
            WidgetPod::new(
                ScrollView::vertical(body)
                    .name("Paged file records")
                    .viewport_width_hint(true)
                    .theme_when(clone_gray_scrollbar_reader(&r)),
            )
        },
    );
    let summary = Label::new("Page status")
        .text_from(state.select_named("paged file status", |v| v.message.clone()))
        .theme_when(clone_reader(reader));
    source_editor::SearchHost::editor(
        active,
        Dock::new(body).top(36., nav).bottom(20., summary),
        widgets::Boxed::new(search),
        None,
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn paged_log_preserves_utf8_crlf_and_records_across_range_boundaries() {
        let mut text = "a".repeat(BLOCK - 1);
        text.push_str("λ end\r\n{\"ok\":true}\n{bad}\n{\"unfinished\":");
        let mut r = Reader::new(Source::Memory(text.as_bytes().to_vec()));
        let size = r.source.len();
        let first = r.page(0, size, true, &|| false).unwrap();
        assert_eq!(first.end, (BLOCK - 1) as u64);
        assert!(!first.rows[0].text.contains('\u{fffd}'));
        let second = r.page(first.end, size, true, &|| false).unwrap();
        assert_eq!(r.line(second.start, &|| false).unwrap(), 1);
        assert_eq!(second.rows[0].text, "λ end");
        assert!(second.rows[0].newline);
        assert!(second.rows[1].issue.is_none());
        assert!(
            second.rows[2]
                .issue
                .as_deref()
                .unwrap()
                .contains("Invalid JSON")
        );
        assert!(
            second.rows[3]
                .issue
                .as_deref()
                .unwrap()
                .contains("Incomplete JSON")
        );
        assert_eq!(
            r.find(0, "aλ", &|| false).unwrap(),
            Some((BLOCK - 2) as u64)
        );
        assert_eq!(r.line(second.rows[3].offset, &|| false).unwrap(), 4);
        let cancelled = Cell::new(0);
        assert!(
            r.find(0, "missing", &|| {
                cancelled.set(cancelled.get() + 1);
                cancelled.get() > 1
            })
            .is_err()
        );
    }
    #[test]
    fn paged_log_stale_search_completion_cannot_replace_a_cancelled_page() {
        let (sender, rx) = mpsc::channel();
        let epoch = Arc::new(AtomicU64::new(0));
        let gate = Arc::new(Mutex::new(()));
        let state = Signal::new(View::default());
        let client = Client {
            sender,
            epoch: epoch.clone(),
            gate: gate.clone(),
            state: state.clone(),
        };
        let (entered, waiting) = mpsc::channel();
        let (release, blocked) = mpsc::channel();
        let output = state.clone();
        let worker = thread::spawn(move || {
            worker(
                rx,
                epoch,
                gate,
                output,
                || {
                    entered.send(()).unwrap();
                    blocked.recv().unwrap();
                    Ok(Source::Memory(b"found".to_vec()))
                },
                false,
            )
        });
        client.send(Action::Search("found".into()));
        waiting.recv_timeout(Duration::from_secs(3)).unwrap();
        client.send(Action::Cancel);
        assert!(!state.get().busy);
        release.send(()).unwrap();
        drop(client);
        worker.join().unwrap();
        assert_eq!(state.get().message, "Cancelled");
        assert!(state.get().page.is_none());
    }

    #[test]
    fn paged_log_json_record_across_two_reads_is_one_expandable_record() {
        let start = BLOCK - 8;
        let mut data = vec![b'\n'; start];
        data.extend_from_slice("{\"key\":\"λ\"}\r\n".as_bytes());
        let mut reader = Reader::new(Source::Memory(data));
        let page = reader
            .page(start as u64, reader.source.len(), true, &|| false)
            .unwrap();
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0].text, "{\"key\":\"λ\"}");
        assert!(page.rows[0].issue.is_none());
        assert_eq!(
            reader.line(page.start, &|| false).unwrap(),
            start as u64 + 1
        );
    }

    #[test]
    fn paged_log_tail_reads_bounded_window_before_counting_lines() {
        let mut r = Reader::new(Source::Memory(b"hello\n".repeat(600_000)));
        let size = r.source.len();
        let start = r.before(size, &|| false).unwrap();
        let page = r.page(start, size, false, &|| false).unwrap();
        assert_eq!(page.rows.len(), PAGE_ROWS);
        assert_eq!(page.end, size);
        assert_eq!(
            r.frontier, 0,
            "tailing must not scan the prefix to display the final page"
        );
        assert_eq!(
            r.line(start, &|| false).unwrap(),
            600_001 - PAGE_ROWS as u64
        );
        let prev = r.before(start, &|| false).unwrap();
        assert_eq!(r.page(prev, start, false, &|| false).unwrap().end, start);
    }
    #[test]
    fn paged_log_large_local_source_saves_all_text_and_rejects_external_changes() {
        let dir = tests_dir();
        let path = dir.join("large.log");
        fs::write(&path, b"line\n".repeat(450_000)).unwrap();
        let loaded =
            load_local_text_document(path.to_str().unwrap(), FILE_DOCUMENT_MAX_BYTES).unwrap();
        assert_eq!(loaded.format, DocumentFormat::Log);
        assert!(loaded.read_only_reason.is_none());
        let edited = format!("{}final edit", loaded.text);
        save_local_text_document(
            path.to_str().unwrap(),
            loaded.local_fingerprint.as_ref().unwrap(),
            edited.as_bytes(),
        )
        .unwrap();
        assert_eq!(fs::read(&path).unwrap(), edited.as_bytes());
        let loaded =
            load_local_text_document(path.to_str().unwrap(), FILE_DOCUMENT_MAX_BYTES).unwrap();
        let f = loaded.local_fingerprint.unwrap();
        let mut source = Source::Local(fs::File::open(&path).unwrap(), f.clone());
        assert_eq!(source.read(5, 4).unwrap(), b"line");
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"changed")
            .unwrap();
        assert!(source.read(10, 4).is_err());
        let mut doc = FilesDocument::loading(
            FilesSourceKind::Local,
            path.to_string_lossy().into(),
            "large.log".into(),
        );
        doc.loading = false;
        doc.local_fingerprint = Some(f);
        doc.format = DocumentFormat::Log;
        assert!(doc.editable());
        assert!(
            save_local_text_document(
                path.to_str().unwrap(),
                doc.local_fingerprint.as_ref().unwrap(),
                b"stale"
            )
            .is_err()
        );
        drop(source);
        fs::remove_dir_all(dir).unwrap();
    }
    fn tests_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "sinomo-paged-{}",
            sinomo_runtime::core::Id::generate()
        ));
        fs::create_dir(&dir).unwrap();
        dir
    }

    #[test]
    fn paged_log_rendered_view_navigates_searches_and_expands_json() -> sui::Result<()> {
        use crate::state::AppState;
        let dir = tests_dir();
        let path = dir.join("events.jsonl");
        let mut data = b"{\"event\":\"running\"}\n".repeat(110_000);
        data.extend_from_slice(b"{\"result\":\"final result\"}\n{broken");
        fs::write(&path, data).unwrap();
        let loaded =
            load_local_text_document(path.to_str().unwrap(), FILE_DOCUMENT_MAX_BYTES).unwrap();
        let shared = Shared::new(AppState::ready(Backend::local_fallback()));
        let mut doc = FilesDocument::loading(loaded.source, loaded.path, loaded.title);
        doc.loading = false;
        doc.format = loaded.format;
        doc.local_fingerprint = loaded.local_fingerprint;
        doc.read_only_reason = loaded.read_only_reason;
        doc.preview_open = true;
        shared.files_mut().document = Some(doc);
        let reader: ThemeReader = {
            let shared = shared.clone();
            Rc::new(move || shared.theme())
        };
        let runtime = sui::Application::new()
            .window(
                sui::WindowBuilder::new()
                    .title("Paged JSONL")
                    .root(FilesDialogHost::new(
                        SizedBox::new(),
                        shared.clone(),
                        reader,
                    )),
            )
            .build()?;
        let app = sui_testing::TestApp::from_runtime(runtime)?;
        let window = app.main_window()?;
        window
            .root()
            .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(560., 720.))))?;
        app.run_until_idle()?;
        window.advance_time(1.)?;
        let wait_row = |name: &str| -> sui::Result<()> {
            let until = std::time::Instant::now() + Duration::from_secs(6);
            loop {
                app.run_until_idle()?;
                if window
                    .get_by_role(SemanticsRole::Button)
                    .with_name(name)
                    .count()?
                    > 0
                {
                    return Ok(());
                }
                assert!(std::time::Instant::now() < until, "missing {name}");
                thread::sleep(Duration::from_millis(15));
            }
        };
        wait_row("Expand line 1")?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Next")
            .click()?;
        wait_row("Expand line 65")?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Previous")
            .click()?;
        wait_row("Expand line 1")?;
        assert_eq!(
            window
                .get_by_role(SemanticsRole::TextInput)
                .with_name("Search file")
                .count()?,
            0
        );
        let mut key = sui::KeyboardEvent::new("f", KeyState::Pressed);
        key.modifiers.control = true;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Next")
            .dispatch_event(Event::Keyboard(key))?;
        app.run_until_idle()?;
        window
            .get_by_role(SemanticsRole::TextInput)
            .with_name("Search file")
            .fill("final result")?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Find next")
            .click()?;
        wait_row("Expand line 110001")?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("Expand line 110001")
            .click()?;
        app.run_until_idle()?;
        assert!(
            window
                .get_by_role(SemanticsRole::TextInput)
                .with_name("Expanded record")
                .count()?
                > 0
        );
        window
            .capture_screenshot()?
            .write_png(std::env::temp_dir().join("sinomo-paged-log-viewer.png"))?;
        window
            .get_by_role(SemanticsRole::Button)
            .with_name("End")
            .click()?;
        wait_row("Expand line 110002")?;
        drop(window);
        drop(app);
        drop(shared);
        fs::remove_dir_all(dir).unwrap();
        Ok(())
    }
}
