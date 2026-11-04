use super::*;
use std::io::{Cursor, Write};
use zip::{ZipWriter, write::SimpleFileOptions};
fn q() -> Query {
    Query {
        header: true,
        ..Default::default()
    }
}
fn grid_of(d: &mut data::Data, q: &Query) -> Grid {
    let Screen::Grid(g) = d.page(q).unwrap() else {
        panic!("expected a grid")
    };
    g
}

#[test]
fn artifact_csv_jump_quoted_rows_and_projection() {
    let mut s = String::from("id,description,cost\r\n");
    for i in 0..50030 {
        s.push_str(&format!("{i},\"quoted, line\nsecond\",{i}\r\n"));
    }
    let mut data =
        data::Data::open(DocumentFormat::Csv, Backing::memory(s.into_bytes()), None).unwrap();
    let mut query = q();
    query.offset = 50000;
    query.columns = vec![0, 1];
    let page = grid_of(&mut data, &query);
    assert_eq!(page.rows.len(), 30);
    assert_eq!(
        page.rows[0],
        [Json::from("50000"), Json::from("quoted, line\nsecond")]
    );
    query.offset = 0;
    query.header = false;
    assert_eq!(grid_of(&mut data, &query).rows[0][0], "id");
}
fn npy(dtype: &str, shape: &str, fortran: bool, data: &[u8]) -> Vec<u8> {
    let mut h = format!(
        "{{'descr': '{dtype}', 'fortran_order': {}, 'shape': {shape}, }}",
        if fortran { "True" } else { "False" }
    );
    while (11 + h.len()) % 64 != 0 {
        h.push(' ');
    }
    h.push('\n');
    let mut b = b"\x93NUMPY\x01\x00".to_vec();
    b.extend((h.len() as u16).to_le_bytes());
    b.extend(h.bytes());
    b.extend(data);
    b
}
#[test]
fn artifact_npy_big_endian_fortran_and_object_arrays() {
    let values = (0i32..80).flat_map(i32::to_be_bytes).collect::<Vec<_>>();
    let mut d = data::Data::open(
        DocumentFormat::Npy,
        Backing::memory(npy(">i4", "(2, 40)", true, &values)),
        None,
    )
    .unwrap();
    let mut query = q();
    query.offset = 64;
    let g = grid_of(&mut d, &query);
    assert_eq!(g.rows[1], vec![Json::from("[1, 32]"), Json::from("65")]);
    let mut object = data::Data::open(
        DocumentFormat::Npy,
        Backing::memory(npy("|O", "(1,)", false, b"invalid pickle")),
        None,
    )
    .unwrap();
    assert!(matches!(object.page(&q()).unwrap(), Screen::Info(_)));
}
fn zipfile(files: &[(&str, &[u8])]) -> Vec<u8> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    for (n, b) in files {
        writer
            .start_file(
                *n,
                SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated),
            )
            .unwrap();
        writer.write_all(b).unwrap();
    }
    writer.finish().unwrap().into_inner()
}
#[test]
fn artifact_zip_implicit_folders_relative_assets_and_npz_arrays() {
    let arr = npy("<i2", "(2,)", false, &[21, 0, 42, 0]);
    let bytes = zipfile(&[
        ("reports/run.html", b"report"),
        ("reports/img/chart.svg", b"<svg/>"),
        ("outside.txt", b"private"),
        ("../escape", b"no"),
        ("weights.npy", &arr),
    ]);
    let archive = Archive::open(Backing::memory(bytes)).unwrap();
    assert!(archive.read("../escape").is_err());
    let assets = Assets {
        archive: archive.clone(),
        directory: "reports".into(),
    };
    assert_eq!(assets.read("img/chart.svg").unwrap(), b"<svg/>");
    assert!(assets.read("../outside.txt").is_err());
    let mut session = Session {
        root: Dataset::Archive(archive),
        stack: vec![],
    };
    let mut query = q();
    query.folder = "reports".into();
    let Screen::List { items, .. } = session.page(&query).unwrap() else {
        panic!()
    };
    assert!(matches!(items[0].action, Action::Folder(_)));
    query.members.push("weights.npy".into());
    query.folder.clear();
    let Screen::Grid(g) = session.page(&query).unwrap() else {
        panic!()
    };
    assert_eq!(g.rows[1][1], "42");
    query.back();
    assert!(query.members.is_empty());
}
#[test]
fn artifact_sqlite_pages_types_primary_keys_and_live_wal() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("run.db");
    let writer = rusqlite::Connection::open(&path).unwrap();
    writer.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE \"run data\"(id INTEGER PRIMARY KEY, note TEXT, payload BLOB); WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<70) INSERT INTO \"run data\" SELECT x, CASE WHEN x=65 THEN NULL ELSE '' END,x'0001' FROM n;").unwrap();
    let mut db =
        data::Data::open(DocumentFormat::Sqlite, Backing::memory(vec![]), Some(path)).unwrap();
    let Screen::List { items, .. } = db.page(&q()).unwrap() else {
        panic!()
    };
    assert_eq!(items[0].name, "run data");
    let mut query = q();
    query.table = Some("run data".into());
    query.offset = 64;
    let page = grid_of(&mut db, &query);
    assert_eq!(page.rows.len(), 6);
    assert_eq!(page.rows[0][0], 65);
    assert_eq!(page.rows[0][1], Json::Null);
    assert_eq!(page.rows[1][1], "");
    assert!(page.rows[0][2].as_str().unwrap().contains("2 bytes"));
    query.schema = true;
    query.offset = 0;
    let Screen::List { items, .. } = db.page(&query).unwrap() else {
        panic!()
    };
    assert!(items[0].detail.contains("primary key"));
    writer
        .execute("INSERT INTO \"run data\" VALUES(71,'new',NULL)", [])
        .unwrap();
    query.schema = false;
    query.offset = 64;
    assert_eq!(grid_of(&mut db, &query).rows.len(), 6);
}
fn parquet_fixture() -> Vec<u8> {
    use parquet::{
        data_type::Int64Type, file::writer::SerializedFileWriter,
        schema::parser::parse_message_type,
    };
    let buf = Vec::new();
    let schema = parse_message_type(
        "message test { REQUIRED INT64 id; REQUIRED group details { REQUIRED INT64 score; } }",
    )
    .unwrap();
    let mut w = SerializedFileWriter::new(buf, Arc::new(schema), Default::default()).unwrap();
    for k in 0..2 {
        let mut g = w.next_row_group().unwrap();
        for mul in [1, 2] {
            let mut col = g.next_column().unwrap().unwrap();
            let values = (0..40).map(|i| (k * 40 + i) * mul).collect::<Vec<i64>>();
            col.typed::<Int64Type>()
                .write_batch(&values, None, None)
                .unwrap();
            col.close().unwrap();
        }
        g.close().unwrap();
    }
    w.into_inner().unwrap()
}
#[test]
fn artifact_parquet_row_groups_project_nested_fields() {
    let file = parquet_fixture();
    let mut d = data::Data::open(DocumentFormat::Parquet, Backing::memory(file), None).unwrap();
    let mut query = q();
    query.offset = 35;
    query.columns = vec![1];
    let g = grid_of(&mut d, &query);
    assert_eq!(g.cols, vec!["details"]);
    assert_eq!(g.total, Some(80));
    assert_eq!(g.rows.len(), 45);
    assert_eq!(g.rows[5][0]["score"], 80);
}

#[test]
fn artifact_document_tab_pages_rows_and_opens_schema() -> sui::Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("analysis.csv");
    let mut text = String::from("run,score,category\n");
    for n in 0..154 {
        text.push_str(&format!("run-{n:03},{},validation\n", n * 2));
    }
    fs::write(&path, &text).unwrap();
    let path = path.to_str().unwrap();
    let loaded = load_local_text_document(path, FILE_DOCUMENT_MAX_BYTES).unwrap();
    let (_app, win, scope) = artifact_tab(path, loaded, 720.)?;
    let wait = |role, name: &str| wait_for(&win, role, name);
    wait(SemanticsRole::Table, "Artifact rows")?;
    win.get_by_role(SemanticsRole::Button)
        .with_name("Next page")
        .click()?;
    wait(SemanticsRole::Text, "65–128")?;
    win.capture_screenshot()?
        .write_png(std::env::temp_dir().join("sinomo-artifact-grid.png"))?;
    win.get_by_role(SemanticsRole::Button)
        .with_name("Schema")
        .click()?;
    wait(SemanticsRole::ListItem, "score")?;
    win.get_by_role(SemanticsRole::ListItem)
        .with_name("score")
        .click()?;
    wait(SemanticsRole::Button, "Show rows")?;
    win.get_by_role(SemanticsRole::Button)
        .with_name("Show rows")
        .click()?;
    wait(SemanticsRole::Table, "Artifact rows")?;
    assert_eq!(scope.files().document.as_ref().unwrap().text, text);
    Ok(())
}

fn artifact_tab(
    path: &str,
    loaded: FilesDocumentLoad,
    width: f32,
) -> sui::Result<(sui_testing::TestApp, sui_testing::TestWindow, Shared)> {
    let shared = Shared::new(crate::state::AppState::ready(Backend::local_fallback()));
    shared
        .open_document_tab(FilesSourceKind::Local, path.into(), loaded.title.clone())
        .unwrap();
    let scope = shared.active_scope();
    {
        let mut files = scope.files_mut();
        files.pending_document_request_id = Some(7);
        assert!(files.apply_worker_result(FilesWorkerResult::Document {
            request_id: 7,
            source: FilesSourceKind::Local,
            path: path.into(),
            result: Ok(loaded)
        }));
    }
    let runtime = sui::Application::new()
        .window(
            sui::WindowBuilder::new()
                .title("Artifact inspector")
                .root(document_view(scope.clone())),
        )
        .build()?;
    let app = sui_testing::TestApp::from_runtime(runtime)?;
    let win = app.main_window()?;
    win.root()
        .dispatch_event(Event::Window(WindowEvent::Resized(Size::new(width, 600.))))?;
    Ok((app, win, scope))
}
fn wait_for(win: &sui_testing::TestWindow, role: SemanticsRole, name: &str) -> sui::Result<()> {
    let end = Instant::now() + Duration::from_secs(8);
    loop {
        win.run_until_idle()?;
        if win.get_by_role(role.clone()).with_name(name).count()? > 0 {
            return Ok(());
        }
        assert!(Instant::now() < end, "missing {name}");
        thread::sleep(Duration::from_millis(10));
    }
}
fn as_document(load: FilesDocumentLoad) -> FilesDocument {
    let mut doc = FilesDocument::loading(load.source, load.path, load.title);
    doc.loading = false;
    doc.format = load.format;
    doc.preview_open = load.preview_open;
    doc.saved_text = load.text.clone();
    doc.text = load.text;
    doc.sifs_fingerprint = load.sifs_fingerprint;
    doc.local_fingerprint = load.local_fingerprint;
    doc.read_only_reason = load.read_only_reason;
    doc
}
fn sqlite_fixture() -> Vec<u8> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fixture.db");
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("CREATE TABLE sample(id INTEGER PRIMARY KEY,score REAL); INSERT INTO sample VALUES(7,0.75);").unwrap();
    drop(db);
    fs::read(path).unwrap()
}
#[test]
fn artifact_zip_opens_parquet_with_nested_row_tree() -> sui::Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("results.zip");
    let file = parquet_fixture();
    fs::write(
        &path,
        zipfile(&[("metrics/results.parquet", &file), ("manifest.json", b"{}")]),
    )
    .unwrap();
    let path = path.to_str().unwrap();
    let (_app, win, _scope) = artifact_tab(
        path,
        load_local_text_document(path, FILE_DOCUMENT_MAX_BYTES).unwrap(),
        480.,
    )?;
    let open = |name: &str| -> sui::Result<()> {
        wait_for(&win, SemanticsRole::ListItem, name)?;
        let item = win.get_by_role(SemanticsRole::ListItem).with_name(name);
        item.click()?;
        item.click()
    };
    wait_for(&win, SemanticsRole::ListItem, "metrics")?;
    win.capture_screenshot()?
        .write_png(std::env::temp_dir().join("sinomo-archive-browser.png"))?;
    open("metrics")?;
    open("results.parquet")?;
    wait_for(&win, SemanticsRole::Table, "Artifact rows")?;
    win.get_by_role(SemanticsRole::ListItem)
        .with_name("0 · {1 fields}")
        .click()?;
    wait_for(&win, SemanticsRole::ListItem, "details")?;
    win.get_by_role(SemanticsRole::ListItem)
        .with_name("details")
        .click()?;
    wait_for(&win, SemanticsRole::ListItem, "score")?;
    win.capture_screenshot()?
        .write_png(std::env::temp_dir().join("sinomo-parquet-row.png"))?;
    Ok(())
}

#[test]
fn artifact_large_tsv_defers_source_and_keeps_editable_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("large.tsv");
    let text = format!("id\tnote\r\n{}", "1\tlong enough\r\n".repeat(155000));
    assert!(text.len() > FILE_DOCUMENT_MAX_BYTES as usize);
    fs::write(&path, text.as_bytes()).unwrap();
    let path = path.to_str().unwrap();
    let doc = as_document(load_local_text_document(path, FILE_DOCUMENT_MAX_BYTES).unwrap());
    assert!(doc.deferred_csv());
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut session = opener(&doc, None, rt.handle().clone())(Default::default()).unwrap();
    let mut query = q();
    query.offset = 50000;
    let Screen::Grid(g) = session.page(&query).unwrap() else {
        panic!()
    };
    assert_eq!(g.rows[0][1], "long enough");
    let loaded = load_local_document_mode(path, FILE_DOCUMENT_MAX_BYTES, true).unwrap();
    assert_eq!(loaded.text, text);
}
#[test]
fn artifact_stored_npz_half_and_complex_samples() {
    let h = npy(">f2", "(1,)", false, &0x3c00u16.to_be_bytes());
    let c = npy(
        ">c8",
        "(1,)",
        false,
        &[1.5f32, -2.0]
            .into_iter()
            .flat_map(f32::to_be_bytes)
            .collect::<Vec<_>>(),
    );
    let mut z = ZipWriter::new(Cursor::new(Vec::new()));
    for (name, data) in [("half.npy", h), ("complex.npy", c)] {
        z.start_file(
            name,
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored),
        )
        .unwrap();
        z.write_all(&data).unwrap();
    }
    let archive = Archive::open(Backing::memory(z.finish().unwrap().into_inner())).unwrap();
    for (name, expected) in [("half.npy", "1"), ("complex.npy", "(1.5, -2j)")] {
        let mut d =
            data::Data::open(DocumentFormat::Npy, archive.input(name).unwrap(), None).unwrap();
        assert_eq!(grid_of(&mut d, &q()).rows[0][1], expected);
    }
}

#[test]
fn artifact_db_signature_and_non_database_db_fallback() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("opaque.bin");
    fs::write(&path, sqlite_fixture()).unwrap();
    assert_eq!(
        load_local_text_document(path.to_str().unwrap(), FILE_DOCUMENT_MAX_BYTES)
            .unwrap()
            .format,
        DocumentFormat::Sqlite
    );
    assert_eq!(
        formats::document_from_bytes("other.db", b"\x00example").0,
        DocumentFormat::Hex
    );
}
#[test]
#[ignore = "requires a disposable SIFS daemon"]
fn artifact_live_sifs_files_and_journal_detection() {
    let addr = std::env::var("SINOMO_FILES_HISTORY_E2E_ADDRESS").unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let browser = SifsBrowser::open_daemon_on_runtime(&addr, runtime.handle()).unwrap();
    let base = format!("/analysis-{}", sinomo_runtime::core::Id::generate());
    browser.create_dir_all(&base).unwrap();
    let arr = npy("<i4", "(2,)", false, &[13, 0, 0, 0, 26, 0, 0, 0]);
    let bundle = zipfile(&[
        ("metrics/score.tsv", b"id\tvalue\n1\t42\n"),
        (
            "reports/run.html",
            b"<link rel=stylesheet href=css/site.css><h1>Analysis report</h1>",
        ),
        ("reports/css/site.css", b"h1{color:teal}"),
    ]);
    for (name, bytes, format) in [
        ("database.bin", sqlite_fixture(), DocumentFormat::Sqlite),
        (
            "results.parquet",
            parquet_fixture(),
            DocumentFormat::Parquet,
        ),
        ("samples.npy", arr.clone(), DocumentFormat::Npy),
        (
            "arrays.npz",
            zipfile(&[("weight.npy", &arr)]),
            DocumentFormat::Npz,
        ),
        ("deliverables.zip", bundle, DocumentFormat::Zip),
        (
            "large.csv",
            format!("id,v\n{}", "1,validation\n".repeat(180000)).into_bytes(),
            DocumentFormat::Csv,
        ),
    ] {
        let path = format!("{base}/{name}");
        browser.write_file(&path, &bytes).unwrap();
        let doc =
            as_document(load_sifs_text_document(&browser, &path, FILE_DOCUMENT_MAX_BYTES).unwrap());
        assert_eq!(doc.format, format);
        let open =
            || opener(&doc, Some(addr.clone()), runtime.handle().clone())(Default::default());
        let mut session = open().unwrap();
        let mut query = q();
        match format {
            DocumentFormat::Sqlite => query.table = Some("sample".into()),
            DocumentFormat::Parquet => query.offset = 64,
            DocumentFormat::Zip => query.members.push("metrics/score.tsv".into()),
            DocumentFormat::Npz => query.members.push("weight.npy".into()),
            DocumentFormat::Csv => {
                assert!(doc.deferred_csv());
                query.offset = 50000;
            }
            _ => {}
        }
        let Screen::Grid(page) = session.page(&query).unwrap() else {
            panic!("no grid for {name}")
        };
        assert!(!page.rows.is_empty());
        if format == DocumentFormat::Sqlite {
            browser
                .write_file(&format!("{path}-wal"), b"sidecar")
                .unwrap();
            assert!(open().err().unwrap().contains("journal"));
        }
    }
    println!("SIFS analysis fixtures: {base}");
}
