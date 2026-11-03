use super::*;

#[derive(Clone)]
struct Frame {
    seq: u64,
    q: Query,
    result: Option<Arc<Result<Screen, String>>>,
}
impl PartialEq for Frame {
    fn eq(&self, other: &Self) -> bool {
        self.seq == other.seq && self.result.is_some() == other.result.is_some()
    }
}
struct WorkerLease(Arc<AtomicBool>);
impl Drop for WorkerLease {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
#[derive(Clone)]
struct Client {
    _lease: Rc<WorkerLease>,
    tx: Sender<(u64, Query)>,
    state: Signal<Frame>,
}
impl Client {
    fn send(&self, q: Query) {
        let seq = self.state.get().seq.wrapping_add(1);
        self.state.set(Frame {
            seq,
            q: q.clone(),
            result: None,
        });
        if self.tx.send((seq, q.clone())).is_err() {
            self.state.set(Frame {
                seq,
                q,
                result: Some(Arc::new(Err("Artifact worker stopped".into()))),
            });
        }
    }
    fn action(&self, q: &Query, action: &Action) {
        let mut q = q.clone();
        match action {
            Action::None => return,
            Action::Folder(path) => {
                q.folder = path.clone();
                q.offset = 0;
            }
            Action::Member(name) => {
                q.members.push(name.clone());
                q.folder.clear();
                q.reset();
            }
            Action::Table(n) => {
                q.reset();
                q.table = Some(n.clone());
            }
            Action::Column(i) => {
                if let Some(pos) = q.columns.iter().position(|v| v == i) {
                    q.columns.remove(pos);
                } else if q.columns.len() < MAX_COLS {
                    q.columns.push(*i);
                    q.columns.sort_unstable();
                }
            }
        }
        self.send(q);
    }
}
fn control(label: &str, r: &ThemeReader, enabled: bool, f: impl FnMut() + 'static) -> sui::Button {
    sui::Button::new(label)
        .enabled(enabled)
        .appearance(ButtonAppearance::Ghost)
        .theme_when(clone_reader(r))
        .on_press(f)
}
pub(super) fn viewer(
    open: impl FnOnce(Arc<AtomicBool>) -> Result<Session, String> + Send + 'static,
    format: DocumentFormat,
    title: String,
    shared: &Shared,
    r: &ThemeReader,
) -> WidgetPod {
    let q = Query {
        header: true,
        ..Default::default()
    };
    let state = Signal::new(Frame {
        seq: 0,
        q: q.clone(),
        result: None,
    });
    let (tx, rx) = mpsc::channel::<(u64, Query)>();
    let closed = Arc::new(AtomicBool::new(false));
    let client = Client {
        _lease: Rc::new(WorkerLease(closed.clone())),
        tx,
        state: state.clone(),
    };
    let publish = state.clone();
    let _ = thread::Builder::new()
        .name("sinomo-artifact-reader".into())
        .spawn(move || {
            let mut session =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| open(closed)))
                    .unwrap_or_else(|_| Err("The file reader rejected this artifact.".into()));
            while let Ok(mut request) = rx.recv() {
                while let Ok(next) = rx.try_recv() {
                    request = next;
                }
                let (seq, q) = request;
                let result = match &mut session {
                    Ok(s) => {
                        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| s.page(&q)))
                        {
                            Ok(r) => r,
                            Err(_) => {
                                let message =
                                    "The file reader rejected the artifact contents.".to_owned();
                                session = Err(message.clone());
                                Err(message)
                            }
                        }
                    }
                    Err(e) => Err(e.clone()),
                };
                publish.update(|frame| {
                    if frame.seq == seq {
                        frame.result = Some(Arc::new(result));
                    }
                });
            }
        });
    client.send(q);
    let shared = shared.clone();
    let r = r.clone();
    WidgetPod::new(RebuildOnChange::key_from(state, move |frame| {
        let q = &frame.q;
        let nav = |label: &str, enabled: bool, query: Query| {
            let c = client.clone();
            control(label, &r, enabled, move || c.send(query.clone()))
        };
        let mut tools = Flex::horizontal()
            .wrap(FlexWrap::Wrap)
            .gap(4.)
            .align_items(Alignment::Center);
        let nested = q.table.is_some() || q.schema || !q.members.is_empty() || !q.folder.is_empty();
        if nested {
            let mut back = q.clone();
            back.back();
            tools = tools.with_child(nav("Back", true, back));
        }
        let screen = frame.result.as_ref().and_then(|v| v.as_ref().as_ref().ok());
        let (more, total, rows) = match screen {
            Some(Screen::Grid(g)) => (g.more, g.total, g.rows.len()),
            Some(Screen::List {
                more, total, items, ..
            }) => (*more, *total, items.len()),
            _ => (false, None, 0),
        };
        if (more || q.offset > 0) && matches!(screen, Some(Screen::Grid(_) | Screen::List { .. })) {
            let mut prev = q.clone();
            prev.offset = prev.offset.saturating_sub(PAGE as u64);
            let mut next = q.clone();
            next.offset = next.offset.saturating_add(PAGE as u64);
            tools = tools
                .with_child(nav("Previous page", q.offset > 0, prev))
                .with_child(nav("Next page", more, next));
            if rows > 0 {
                tools = tools.with_child(
                    Label::new(format!(
                        "{}–{}{}",
                        q.offset + 1,
                        q.offset + rows as u64,
                        total.map_or(String::new(), |n| format!(" of {n}"))
                    ))
                    .theme_when(clone_reader(&r)),
                );
            }
            let c = client.clone();
            let jump = q.clone();
            tools = tools.with_child(
                SizedBox::new().width(84.).child(
                    sui::TextInput::new("Go to row")
                        .placeholder("Go to row")
                        .min_width(0.)
                        .theme_when(clone_reader(&r))
                        .on_submit_with_ctx(move |_, v| {
                            if let Ok(row) = v.trim().parse::<u64>() {
                                let mut query = jump.clone();
                                query.offset = row
                                    .saturating_sub(1)
                                    .min(total.unwrap_or(i64::MAX as u64).saturating_sub(1));
                                c.send(query);
                            }
                        }),
                ),
            );
        }
        let parquet = q
            .members
            .last()
            .map_or(format == DocumentFormat::Parquet, |name| {
                DocumentFormat::artifact(name, &[]) == Some(DocumentFormat::Parquet)
            });
        if matches!(screen, Some(Screen::Grid(_))) || q.schema || parquet || q.table.is_some() {
            let mut toggle = q.clone();
            toggle.schema = !q.schema;
            toggle.offset = 0;
            tools = tools.with_child(nav(
                if q.schema { "Show rows" } else { "Schema" },
                true,
                toggle,
            ));
            if q.schema && !q.columns.is_empty() {
                let mut reset = q.clone();
                reset.columns.clear();
                tools = tools.with_child(nav("Reset columns", true, reset));
            }
        }
        let member_csv = q.members.last().is_some_and(|m| {
            matches!(
                formats::text_format(m),
                DocumentFormat::Csv | DocumentFormat::Tsv
            )
        });
        if matches!(format, DocumentFormat::Csv | DocumentFormat::Tsv) || member_csv {
            let mut toggle = q.clone();
            toggle.offset = 0;
            toggle.header = !q.header;
            toggle.columns.clear();
            tools = tools.with_child(nav(
                if q.header {
                    "Header: first row"
                } else {
                    "Header: none"
                },
                true,
                toggle,
            ));
        }
        let content = match frame.result.as_deref() {
            None => WidgetPod::new(Label::new("Reading artifact…").theme_when(clone_reader(&r))),
            Some(Err(e)) => WidgetPod::new(
                Label::new(format!("Preview unavailable · {e}")).theme_when(clone_reader(&r)),
            ),
            Some(Ok(Screen::Info(text))) => {
                WidgetPod::new(Label::new(text).theme_when(clone_reader(&r)))
            }
            Some(Ok(Screen::List {
                directory: true,
                title,
                items,
                ..
            })) => archive_listing(title, items, q, &client, &r),
            Some(Ok(Screen::List { title, items, .. })) => {
                let items = items.clone();
                let actions = items.iter().map(|e| e.action.clone()).collect::<Vec<_>>();
                let c = client.clone();
                let query = q.clone();
                let list = ListView::new("Artifact entries")
                    .items(items.into_iter().map(|e| {
                        let icon = match e.action {
                            Action::Folder(_) => IconGlyph::Folder,
                            Action::Table(_) => IconGlyph::Storage,
                            _ => IconGlyph::File,
                        };
                        ListItem::new(&e.name)
                            .semantic_name(e.name)
                            .trailing(e.detail)
                            .leading_icon(icon)
                            .enabled(!matches!(e.action, Action::None))
                    }))
                    .theme_when(clone_reader(&r))
                    .on_change_with_ctx(move |_, i, _| c.action(&query, &actions[i]));
                WidgetPod::new(
                    Dock::new(
                        ScrollView::vertical(list)
                            .viewport_width_hint(true)
                            .theme_when(clone_gray_scrollbar_reader(&r)),
                    )
                    .top(28., Label::new(title).theme_when(clone_reader(&r))),
                )
            }
            Some(Ok(Screen::Grid(g))) => grid_view(g, &r),
            Some(Ok(Screen::Member {
                format,
                text,
                bytes,
                path,
                assets,
            })) => member_view(*format, text, bytes, path, assets, &shared, &r),
        };
        let caption = q
            .members
            .last()
            .map(|s| format!("{title} / {s}"))
            .unwrap_or_default();
        let mut layout = Flex::vertical().align_items(Alignment::Stretch);
        if !caption.is_empty() {
            layout = layout.with_child(
                Label::new(caption)
                    .single_line(true)
                    .theme_when(clone_reader(&r)),
            );
        }
        WidgetPod::new(
            Surface::panel(
                layout
                    .with_child(tools)
                    .with_item(formats::PreviewWidget(content), FlexItem::fill()),
            )
            .theme_when(clone_reader(&r))
            .fill(),
        )
    }))
}

fn archive_listing(
    title: &str,
    entries: &[Entry],
    q: &Query,
    client: &Client,
    r: &ThemeReader,
) -> WidgetPod {
    let entries = Rc::new(entries.to_vec());
    let names = entries.clone();
    let paint = entries.clone();
    let reader = r.clone();
    let client = client.clone();
    let q = q.clone();
    let selected = Signal::new(None::<usize>);
    let current = selected.clone();
    let table = VirtualTable::new("Archive files")
        .columns([
            VirtualTableColumn::new("Name").min_width(160.),
            VirtualTableColumn::new("Details").width(185.),
        ])
        .row_count(entries.len())
        .row_name(move |i| names[i].name.clone())
        .theme_when(clone_gray_data_scrollbar_reader(r))
        .selected_when(move || current.get())
        .row_painter(move |ctx, row| {
            let entry = &paint[row.row_index];
            let theme = reader();
            let color = theme.palette.text;
            if let Some(cell) = row.column_rects.first() {
                let glyph = if matches!(entry.action, Action::Folder(_)) {
                    IconGlyph::Folder
                } else {
                    IconGlyph::File
                };
                paint_file_name(
                    ctx,
                    &theme,
                    *cell,
                    &entry.name,
                    Some(glyph),
                    ROW_THUMBNAIL_SIDE,
                    theme.palette.text_muted,
                    color,
                );
            }
            if let Some(cell) = row.column_rects.get(1) {
                paint_text_cell(
                    ctx,
                    &theme,
                    *cell,
                    &entry.detail,
                    TextCellPaint::new()
                        .text_token(theme.text.xs)
                        .color(theme.palette.text_muted)
                        .padding(8., 8.),
                );
            }
        })
        .on_row_activate(move |i, activation| {
            selected.set(Some(i));
            if activation == VirtualTableRowActivationKind::Double || cfg!(target_os = "android") {
                client.action(&q, &entries[i].action);
            }
        });
    WidgetPod::new(Dock::new(table).top(26., Label::new(title).theme_when(clone_reader(r))))
}
fn brief(v: &Json) -> String {
    match v {
        Json::Null => "NULL".into(),
        Json::String(s) if s.is_empty() || s == "NULL" => format!("{s:?}"),
        Json::String(s) => short(s),
        Json::Array(a) => format!("[{} items]", a.len()),
        Json::Object(o) => format!("{{{} fields}}", o.len()),
        v => v.to_string(),
    }
}
fn grid_view(grid: &Grid, r: &ThemeReader) -> WidgetPod {
    let selection = Signal::new(None::<usize>);
    let target = selection.clone();
    let rows = Arc::new(grid.rows.clone());
    let cols = grid.cols.clone();
    let note = grid.note.clone();
    let first = grid.offset;
    let viewport = VirtualTableState::new();
    let r = r.clone();
    WidgetPod::new(RebuildOnChange::key_from(selection, move |selected| {
        if let Some(i) = *selected {
            let target = target.clone();
            return WidgetPod::new(Dock::new(row_tree(&cols, &rows[i], &r)).top(
                32.,
                control("Back to rows", &r, true, move || {
                    target.set(None);
                }),
            ));
        }
        if rows.is_empty() {
            return WidgetPod::new(
                Label::new(format!("{note} · No rows on this page")).theme_when(clone_reader(&r)),
            );
        }
        let target = target.clone();
        let cells = rows.clone();
        let painter = r.clone();
        let table = VirtualTable::new("Artifact rows")
            .state(viewport.clone())
            .row_count(rows.len())
            .columns(
                std::iter::once(
                    VirtualTableColumn::new("#")
                        .min_width(48.)
                        .width(62.)
                        .pinned(true),
                )
                .chain(
                    cols.iter()
                        .map(|n| VirtualTableColumn::new(n).min_width(132.)),
                ),
            )
            .theme_when(clone_gray_data_scrollbar_reader(&r))
            .row_name({
                let rows = rows.clone();
                move |i| rows[i].iter().map(brief).collect::<Vec<_>>().join(" · ")
            })
            .row_painter(move |ctx, row| {
                let theme = painter();
                if let Some(cell) = row.column_rects.first() {
                    paint_text_cell(
                        ctx,
                        &theme,
                        *cell,
                        &(first + row.row_index as u64 + 1).to_string(),
                        TextCellPaint::new()
                            .text_token(theme.text.xs)
                            .color(theme.palette.text_muted)
                            .padding(8., 8.),
                    );
                }
                for (cell, value) in row.column_rects.iter().skip(1).zip(&cells[row.row_index]) {
                    paint_text_cell(
                        ctx,
                        &theme,
                        *cell,
                        &brief(value),
                        TextCellPaint::new()
                            .text_token(theme.text.sm)
                            .color(theme.palette.text)
                            .padding(8., 8.),
                    );
                }
            })
            .on_row_activate(move |i, _| {
                target.set(Some(i));
            });
        WidgetPod::new(
            Dock::new(table).top(
                24.,
                Label::new(&note)
                    .single_line(true)
                    .theme_when(clone_reader(&r)),
            ),
        )
    }))
}
fn row_tree(
    names: &[String],
    row: &[Json],
    r: &ThemeReader,
) -> RebuildOnChange<BTreeSet<Vec<usize>>> {
    let fields = names
        .iter()
        .cloned()
        .zip(row.iter().cloned())
        .collect::<Vec<_>>();
    let open = Signal::new(BTreeSet::<Vec<usize>>::new());
    let target = open.clone();
    let r = r.clone();
    RebuildOnChange::key_from(open, move |expanded| {
        fn add(
            key: &str,
            v: &Json,
            path: &mut Vec<usize>,
            exp: &BTreeSet<Vec<usize>>,
            items: &mut Vec<ListItem>,
            paths: &mut Vec<Vec<usize>>,
            r: &ThemeReader,
        ) {
            let branch = v.is_object() || v.is_array();
            let glyph = if branch {
                if exp.contains(path) { "▾" } else { "▸" }
            } else {
                "·"
            };
            items.push(sobj::field_row(
                r,
                r().metrics.tree_indent * path.len().saturating_sub(1) as f32,
                key,
                glyph,
                Some(brief(v)),
                match v {
                    Json::Null => "Null",
                    Json::Bool(_) => "Boolean",
                    Json::Number(_) => "Number",
                    Json::String(_) => "Text",
                    Json::Array(_) => "List",
                    Json::Object(_) => "Object",
                }
                .into(),
            ));
            paths.push(path.clone());
            if branch && exp.contains(path) && path.len() < 32 {
                let children: Vec<_> = match v {
                    Json::Object(o) => o.iter().take(256).map(|(k, v)| (k.to_owned(), v)).collect(),
                    Json::Array(a) => a
                        .iter()
                        .take(256)
                        .enumerate()
                        .map(|(i, v)| (format!("[{i}]"), v))
                        .collect(),
                    _ => vec![],
                };
                for (i, (k, child)) in children.into_iter().enumerate() {
                    path.push(i);
                    add(&k, child, path, exp, items, paths, r);
                    path.pop();
                }
            }
        }
        let mut items = vec![];
        let mut paths = vec![];
        for (i, (k, v)) in fields.iter().enumerate() {
            add(k, v, &mut vec![i], expanded, &mut items, &mut paths, &r);
        }
        let target = target.clone();
        let list = ListView::new("Row fields")
            .items(items)
            .theme_when(clone_reader(&r))
            .on_change_with_ctx(move |_, i, _| {
                target.update(|s| {
                    if !s.remove(&paths[i]) {
                        s.insert(paths[i].clone());
                    }
                });
            });
        WidgetPod::new(
            ScrollView::vertical(list)
                .viewport_width_hint(true)
                .theme_when(clone_gray_scrollbar_reader(&r)),
        )
    })
}
fn member_view(
    format: DocumentFormat,
    text: &str,
    bytes: &Arc<[u8]>,
    path: &str,
    assets: &Assets,
    shared: &Shared,
    r: &ThemeReader,
) -> WidgetPod {
    #[cfg(all(
        feature = "desktop",
        any(target_os = "windows", target_os = "macos", target_os = "linux")
    ))]
    if matches!(
        format,
        DocumentFormat::Html | DocumentFormat::Svg | DocumentFormat::Pdf | DocumentFormat::Notebook
    ) {
        use crate::ui::reports::{Content, NOTEBOOK_MIME, Report};
        let mime = match format {
            DocumentFormat::Svg => "image/svg+xml",
            DocumentFormat::Pdf => "application/pdf",
            DocumentFormat::Notebook => NOTEBOOK_MIME,
            _ => "text/html",
        };
        return shared.borrow().reports.widget(Report {
            source: FilesSourceKind::Local,
            base: String::new(),
            name: path.rsplit('/').next().unwrap_or(path).into(),
            mime,
            content: Content::Archive(bytes.clone(), assets.clone()),
            address: None,
            runtime: shared.files().app_runtime.clone(),
        });
    }
    if let Some(preview) = formats::preview_widget(format, text, r) {
        if matches!(format, DocumentFormat::Json | DocumentFormat::Diff) {
            return WidgetPod::new(preview);
        }
        return WidgetPod::new(
            ScrollView::vertical(preview)
                .viewport_width_hint(true)
                .theme_when(clone_gray_scrollbar_reader(r)),
        );
    }
    let mut surface = TextSurface::new("Archive member contents")
        .value(text)
        .read_only(true)
        .wrap(TextWrap::NoWrap)
        .theme_when(clone_reader(r))
        .text_style_when(|theme| source_editor::mono(&theme));
    surface.set_selection(0, 0);
    WidgetPod::new(editor_surface::EditorSurface::new(
        surface,
        r,
        Signal::new(Default::default()),
        Insets::all(8.),
    ))
}
