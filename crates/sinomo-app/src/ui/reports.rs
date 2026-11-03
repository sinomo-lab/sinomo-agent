//! Native desktop report views and their read-only, directory-scoped resource origin.
mod notebook;
pub(super) const NOTEBOOK_MIME: &str = "application/x-ipynb+json";
use super::files::FilesSourceKind;
use cap_std::{ambient_authority, fs::Dir};
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use sinomo_client::{
    SifsBrowser,
    api::{DFS_BROWSER_RANGE_LIMIT, DaemonDfsBrowserReadObjectRangeRequest as RangeRead},
};
use std::{
    borrow::Cow,
    cell::RefCell,
    io::{self, Cursor, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
use sui::{DesktopExtension, DesktopExtensionContext, Label, RebuildOnChange, Signal, WidgetPod};
use sui_webview::{WebViewHost, wry::NewWindowResponse};
use tiny_http::{Header, Method, Request, Response, Server};

#[derive(Clone, Debug)]
pub(crate) struct ReportHost {
    native: Rc<RefCell<WebViewHost>>,
    failure: Signal<Option<String>>,
    views: Rc<RefCell<Vec<(sui_webview::WebViewHandle, bool)>>>,
}
impl ReportHost {
    pub(crate) fn new(_data: &sinomo_runtime::local::SinomoDataDir) -> Self {
        let native = WebViewHost::new();
        #[cfg(windows)]
        let native = native.with_web_context(sui_webview::wry::WebContext::new(Some(
            _data.root().join("desktop-webview"),
        )));
        Self {
            native: Rc::new(RefCell::new(native)),
            failure: Signal::new(None),
            views: Default::default(),
        }
    }
}
impl DesktopExtension for ReportHost {
    fn update(&mut self, ctx: DesktopExtensionContext<'_>) -> sui::Result<()> {
        // Native children sit above the GPU surface. Let SUI modals and menus receive
        // input while they cover a report, including dialogs opened outside Files.
        let mut views = self.views.borrow_mut();
        views.retain(|(view, _)| view.is_open());
        for (view, was_visible) in views.iter_mut() {
            let (Some(window), Some(widget)) = (view.window_id(), view.widget_id()) else {
                continue;
            };
            let overlays = ctx.runtime().overlay_snapshot(window)?;
            let top = overlays.overlays.iter().rev().find(|o| {
                matches!(
                    o.options.kind,
                    sui::OverlayKind::Dialog
                        | sui::OverlayKind::Sheet
                        | sui::OverlayKind::Menu
                        | sui::OverlayKind::Popover
                        | sui::OverlayKind::CommandPalette
                )
            });
            let visible = if let Some(overlay) = top {
                let graph = ctx.runtime().widget_graph(window)?;
                let mut parent = Some(widget);
                let mut inside = false;
                while let Some(id) = parent {
                    if id == overlay.owner || overlay.surfaces.contains(&id) {
                        inside = true;
                        break;
                    }
                    parent = graph
                        .nodes
                        .iter()
                        .find(|n| n.id == id)
                        .and_then(|n| n.parent);
                }
                inside
            } else {
                true
            };
            if visible != *was_visible {
                let _ = view.set_visible(visible);
                *was_visible = visible;
            }
        }
        drop(views);
        if self.failure.get().is_none() {
            let result = self.native.borrow_mut().update(ctx);
            if let Err(e) = result {
                self.failure
                    .set(Some(format!("Browser preview unavailable: {e}")));
                self.native.borrow_mut().suspended()?;
            }
        }
        Ok(())
    }
    fn suspended(&mut self) -> sui::Result<()> {
        self.native.borrow_mut().suspended()
    }
    fn poll_interval(&self) -> Option<Duration> {
        if self.failure.get().is_some() {
            None
        } else {
            self.native.borrow().poll_interval()
        }
    }
}
pub(super) enum Content {
    Bytes(Vec<u8>),
    LocalPdf(PathBuf, u64, Option<u64>),
    SifsPdf(String, String, String, u64),
}
pub(super) struct Report {
    pub source: FilesSourceKind,
    pub base: String,
    pub name: String,
    pub mime: &'static str,
    pub content: Content,
    pub address: Option<String>,
    pub runtime: tokio::runtime::Handle,
}

struct Origin {
    url: String,
    server: Arc<Server>,
    closed: Arc<AtomicBool>,
}
impl Drop for Origin {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        self.server.unblock();
    }
}
impl ReportHost {
    pub(super) fn widget(&self, report: Report) -> WidgetPod {
        let notebook = report.mime == NOTEBOOK_MIME;
        let origin = match Origin::start(report) {
            Ok(v) => Rc::new(v),
            Err(e) => return WidgetPod::new(Label::new(format!("Cannot open report: {e}"))),
        };
        let host = self.native.clone();
        let views = self.views.clone();
        WidgetPod::new(RebuildOnChange::key_from(
            self.failure.clone(),
            move |error| {
                if let Some(e) = error {
                    return WidgetPod::new(Label::new(e));
                }
                let origin = origin.clone();
                let view = host
                    .borrow()
                    .url(&origin.url)
                    .accessible_name(if notebook {
                        "Notebook preview"
                    } else {
                        "Report preview"
                    })
                    .configure(move |builder| {
                        let root = origin.url.rsplit_once('/').unwrap().0.to_owned() + "/";
                        let builder = if notebook {
                            builder
                        } else {
                            builder.with_javascript_disabled()
                        };
                        let entry = origin.url.clone();
                        builder
                            .with_clipboard(false)
                            .with_devtools(false)
                            .with_navigation_handler(move |url| {
                                if notebook {
                                    url.split('#').next() == Some(entry.as_str())
                                } else {
                                    url.starts_with(&root)
                                }
                            })
                            .with_new_window_req_handler(|_, _| NewWindowResponse::Deny)
                            .with_download_started_handler(|_, _| false)
                    });
                views.borrow_mut().push((view.handle(), true));
                WidgetPod::new(view)
            },
        ))
    }
}
impl Origin {
    fn start(mut report: Report) -> Result<Self, String> {
        let server = Arc::new(Server::http("127.0.0.1:0").map_err(|e| e.to_string())?);
        let host = server.server_addr().to_string();
        let mut secret = [0u8; 24];
        getrandom::fill(&mut secret).map_err(|e| e.to_string())?;
        let prefix = format!(
            "/{}/",
            secret
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        let url = format!(
            "http://{host}{prefix}{}",
            if report.mime == NOTEBOOK_MIME {
                "notebook.html".into()
            } else {
                utf8_percent_encode(&report.name, NON_ALPHANUMERIC).to_string()
            }
        );
        let closed = Arc::new(AtomicBool::new(false));
        let stop = closed.clone();
        let listen = server.clone();
        thread::Builder::new()
            .name("sinomo-report-assets".into())
            .spawn(move || {
                let local = if report.source == FilesSourceKind::Local
                    && Path::new(&report.base).is_absolute()
                {
                    Dir::open_ambient_dir(&report.base, ambient_authority()).ok()
                } else {
                    None
                };
                if report.mime == NOTEBOOK_MIME
                    && let Content::Bytes(data) = &mut report.content
                {
                    *data = notebook::page(data).into_bytes();
                }
                let mut browser = None;
                while !stop.load(Ordering::Acquire) {
                    let Ok(req) = listen.recv() else { break };
                    let reply = serve(&req, &host, &prefix, &report, local.as_ref(), &mut browser)
                        .unwrap_or_else(|(status, reason)| {
                            let msg = reason.into_bytes();
                            let len = msg.len();
                            Response::new(
                                status.into(),
                                vec![header("Content-Type", "text/plain; charset=utf-8")],
                                Box::new(Cursor::new(msg)) as Box<dyn Read + Send>,
                                Some(len),
                                None,
                            )
                        });
                    let _ = req.respond(reply);
                }
            })
            .map_err(|e| e.to_string())?;
        Ok(Self {
            url,
            server,
            closed,
        })
    }
}

fn header(name: &str, value: &str) -> Header {
    Header::from_bytes(name, value).expect("valid report response header")
}
type Failure = (u16, String);
type Body = Box<dyn Read + Send>;
fn asset_path(url: &str, prefix: &str) -> Option<String> {
    let raw = url.split('?').next()?.strip_prefix(prefix)?;
    let mut components = Vec::new();
    for segment in raw.split('/') {
        let value = percent_decode_str(segment).decode_utf8().ok()?;
        if value.is_empty()
            || matches!(&*value, "." | "..")
            || value.contains(['/', '\\', ':'])
            || value.chars().any(char::is_control)
            || value.ends_with(['.', ' '])
        {
            return None;
        }
        let stem = value.split('.').next()?.to_ascii_uppercase();
        if matches!(&*stem, "NUL" | "CON" | "PRN" | "AUX" | "CONIN$" | "CONOUT$")
            || (stem.starts_with("COM") || stem.starts_with("LPT"))
                && stem[3..].chars().count() == 1
        {
            return None;
        }
        components.push(value);
    }
    Some(components.join("/"))
}

fn byte_range(value: Option<&str>, size: u64) -> Result<(u64, u64), Failure> {
    let Some(value) = value else {
        return Ok((0, size));
    };
    let bad = || (416, "Invalid byte range".into());
    let (start, end) = value
        .strip_prefix("bytes=")
        .and_then(|v| v.split_once('-'))
        .ok_or_else(bad)?;
    if size == 0 {
        return Err(bad());
    }
    if start.is_empty() {
        let n: u64 = end.parse().map_err(|_| bad())?;
        if n == 0 {
            return Err(bad());
        }
        return Ok((size.saturating_sub(n), size));
    }
    let start: u64 = start.parse().map_err(|_| bad())?;
    let end = if end.is_empty() {
        size
    } else {
        end.parse::<u64>()
            .map_err(|_| bad())?
            .saturating_add(1)
            .min(size)
    };
    if start >= end {
        return Err(bad());
    }
    Ok((start, end))
}

struct SifsReader {
    client: Arc<SifsBrowser>,
    request: RangeRead,
    end: u64,
    pending: Cursor<Vec<u8>>,
}
impl Read for SifsReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        if self.pending.position() == self.pending.get_ref().len() as u64 {
            if self.request.offset >= self.end {
                return Ok(0);
            }
            self.request.length =
                (self.end - self.request.offset).min(DFS_BROWSER_RANGE_LIMIT as u64) as u32;
            let bytes = self
                .client
                .read_object_range(&self.request)
                .map_err(io::Error::other)?;
            self.request.offset += bytes.len() as u64;
            self.pending = Cursor::new(bytes);
        }
        self.pending.read(out)
    }
}
enum Resource<'a> {
    Memory(&'a [u8]),
    Local(std::fs::File, u64),
    Remote(Arc<SifsBrowser>, RangeRead),
}
impl Resource<'_> {
    fn len(&self) -> u64 {
        match self {
            Self::Memory(b) => b.len() as u64,
            Self::Local(_, n) => *n,
            Self::Remote(_, r) => r.size_bytes,
        }
    }
    fn body(self, start: u64, end: u64) -> io::Result<Body> {
        Ok(match self {
            Self::Memory(b) => Box::new(Cursor::new(b[start as usize..end as usize].to_vec())),
            Self::Local(mut f, _) => {
                f.seek(SeekFrom::Start(start))?;
                Box::new(f.take(end - start))
            }
            Self::Remote(client, mut request) => {
                request.offset = start;
                Box::new(SifsReader {
                    client,
                    request,
                    end,
                    pending: Cursor::new(vec![]),
                })
            }
        })
    }
}

fn serve(
    req: &Request,
    host: &str,
    prefix: &str,
    report: &Report,
    local: Option<&Dir>,
    cached: &mut Option<Arc<SifsBrowser>>,
) -> Result<Response<Body>, Failure> {
    if !matches!(req.method(), Method::Get | Method::Head) {
        return Err((405, "Read-only report".into()));
    }
    let field = |name: &'static str| {
        req.headers()
            .iter()
            .find(|h| h.field.equiv(name))
            .map(|h| h.value.as_str())
    };
    let missing = || (404, "Resource outside this report or unavailable".into());
    if field("Host") != Some(host) {
        return Err(missing());
    }
    let path = asset_path(req.url(), prefix).ok_or_else(missing)?;
    let mut browser = || -> Result<Arc<SifsBrowser>, Failure> {
        if cached.is_none() {
            let address = report.address.as_deref().ok_or_else(missing)?;
            *cached = Some(Arc::new(
                SifsBrowser::open_daemon_on_runtime(address, &report.runtime)
                    .map_err(|e| (503, e))?,
            ));
        }
        Ok(cached.as_ref().unwrap().clone())
    };
    let remote = |client, id: String, version: String, hash: String, size| {
        Resource::Remote(
            client,
            RangeRead {
                object_id: id,
                version,
                content_hash: hash,
                size_bytes: size,
                offset: 0,
                length: 0,
            },
        )
    };
    let notebook = report.mime == NOTEBOOK_MIME;
    let primary = if notebook {
        path == "notebook.html"
    } else {
        path == report.name
    };
    let renderer = notebook && path == notebook::SCRIPT_NAME;
    let mime = if renderer {
        Cow::Borrowed("text/javascript")
    } else if primary {
        Cow::Borrowed(if notebook { "text/html" } else { report.mime })
    } else {
        Cow::Owned(
            mime_guess::from_path(&path)
                .first_or_octet_stream()
                .essence_str()
                .to_owned(),
        )
    };
    let resource = if renderer {
        Resource::Memory(notebook::SCRIPT.as_bytes())
    } else if primary {
        match &report.content {
            Content::Bytes(data) => Resource::Memory(data),
            Content::LocalPdf(path, len, modified) => {
                let f = std::fs::File::open(path).map_err(|_| missing())?;
                let m = f.metadata().map_err(|_| missing())?;
                let stamp = m
                    .modified()
                    .ok()
                    .and_then(|v| v.duration_since(std::time::UNIX_EPOCH).ok())
                    .and_then(|d| u64::try_from(d.as_nanos()).ok());
                if m.len() != *len || stamp != *modified {
                    return Err((
                        409,
                        "PDF changed. Reopen it to view the current version.".into(),
                    ));
                }
                Resource::Local(f, *len)
            }
            Content::SifsPdf(id, version, hash, size) => {
                remote(browser()?, id.clone(), version.clone(), hash.clone(), *size)
            }
        }
    } else if report.mime == "application/pdf" {
        return Err(missing());
    } else {
        match report.source {
            FilesSourceKind::Local => {
                let f = local
                    .ok_or_else(missing)?
                    .open(&path)
                    .map_err(|_| missing())?
                    .into_std();
                let m = f.metadata().map_err(|_| missing())?;
                if !m.is_file() {
                    return Err(missing());
                }
                Resource::Local(f, m.len())
            }
            FilesSourceKind::Sifs => {
                let client = browser()?;
                let base = report.base.trim_end_matches('/');
                if !report.base.starts_with('/') || report.base.split('/').any(|p| p == "..") {
                    return Err(missing());
                }
                let details = client
                    .stat(&format!("{base}/{path}"))
                    .map_err(|_| missing())?;
                // stat returns the resolved path; pin the read to the object that passed containment.
                if details.kind != "file" || !details.path.starts_with(&format!("{base}/")) {
                    return Err(missing());
                }
                remote(
                    client,
                    details.object_id,
                    details.version,
                    details.content_hash.ok_or_else(missing)?,
                    details.size_bytes.ok_or_else(missing)?,
                )
            }
        }
    };
    let size = resource.len();
    let range = field("Range");
    let (start, end) = byte_range(range, size)?;
    // The app's editable report buffers are UTF-8, including fileless chat HTML.
    let content_type = if primary && mime != "application/pdf" {
        format!("{mime}; charset=utf-8")
    } else {
        mime.to_string()
    };
    let mut headers = vec![
        header("Content-Type", &content_type),
        header("Accept-Ranges", "bytes"),
        header("Cache-Control", "no-store"),
        header("X-Content-Type-Options", "nosniff"),
        header("Referrer-Policy", "no-referrer"),
        header("X-DNS-Prefetch-Control", "off"),
    ];
    if notebook && primary {
        headers.push(header("Content-Security-Policy", &format!("default-src 'none'; script-src http://{host}{prefix}{}; img-src 'self' data:; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'; sandbox allow-scripts allow-same-origin", notebook::SCRIPT_NAME)));
    } else if mime != "application/pdf" {
        headers.push(header("Content-Security-Policy","default-src 'none'; script-src 'none'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; font-src 'self' data:; media-src 'self'; base-uri 'self'; form-action 'none'; frame-ancestors 'none'; sandbox allow-same-origin"));
    }
    if range.is_some() {
        headers.push(header(
            "Content-Range",
            &format!("bytes {start}-{}/{size}", end - 1),
        ));
    }
    let body: Body = if req.method() == &Method::Head {
        Box::new(io::empty())
    } else {
        resource
            .body(start, end)
            .map_err(|e| (500, e.to_string()))?
    };
    Ok(Response::new(
        if range.is_some() { 206 } else { 200 }.into(),
        headers,
        body,
        usize::try_from(end - start).ok(),
        None,
    )
    .with_chunked_threshold(usize::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    fn get(origin: &Origin, relative: Option<&str>, range: Option<&str>) -> (u16, Vec<u8>, String) {
        let url = origin.url.strip_prefix("http://").unwrap();
        let (host, path) = url.split_once('/').unwrap();
        let base = path.rsplit_once('/').unwrap().0;
        let path = relative
            .map(|p| format!("{base}/{p}"))
            .unwrap_or_else(|| path.into());
        let mut conn = std::net::TcpStream::connect(host).unwrap();
        let range = range.map(|r| format!("Range: {r}\r\n")).unwrap_or_default();
        write!(
            conn,
            "GET /{path} HTTP/1.1\r\nHost: {host}\r\n{range}Connection: close\r\n\r\n"
        )
        .unwrap();
        let mut data = vec![];
        conn.read_to_end(&mut data).unwrap();
        let end = data.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
        let head = String::from_utf8_lossy(&data[..end]).into_owned();
        (
            head.split_whitespace().nth(1).unwrap().parse().unwrap(),
            data[end + 4..].to_vec(),
            head,
        )
    }

    #[test]
    fn report_origin_serves_nested_assets_and_rejects_escapes() {
        let dir = sinomo_testdir::TestDir::new("report-origin");
        let base = dir.join("report");
        std::fs::create_dir_all(base.join("assets/images")).unwrap();
        std::fs::write(
            base.join("assets/site.css"),
            b"body{background:url(images/chart.svg)}",
        )
        .unwrap();
        std::fs::write(base.join("assets/images/chart.svg"), b"<svg/>").unwrap();
        std::fs::write(dir.join("outside.txt"), b"private").unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let origin = Origin::start(Report {
            source: FilesSourceKind::Local,
            base: base.to_string_lossy().into_owned(),
            name: "report.html".into(),
            mime: "text/html",
            content: Content::Bytes(b"<link rel=stylesheet href=assets/site.css>".to_vec()),
            runtime: runtime.handle().clone(),
            address: None,
        })
        .unwrap();
        let (status, _, headers) = get(&origin, None, None);
        assert_eq!(status, 200);
        assert!(headers.contains("script-src 'none'"));
        assert_eq!(
            get(&origin, Some("assets/site.css?version=1"), None).1,
            b"body{background:url(images/chart.svg)}"
        );
        assert_eq!(
            get(&origin, Some("assets/images/chart.svg"), None).1,
            b"<svg/>"
        );
        for path in [
            "../outside.txt",
            "%2e%2e/outside.txt",
            "C%3a/windows",
            "assets%5c..%5coutside.txt",
            "/outside.txt",
        ] {
            assert_eq!(get(&origin, Some(path), None).0, 404, "{path}");
        }
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(dir.join("outside.txt"), base.join("link")).unwrap();
            assert_eq!(get(&origin, Some("link"), None).0, 404);
        }
    }

    #[test]
    #[ignore = "requires a disposable SIFS daemon"]
    fn report_sifs_assets_and_version_pinned_pdf() {
        let address = std::env::var("SINOMO_FILES_HISTORY_E2E_ADDRESS").unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let client = SifsBrowser::open_daemon_on_runtime(&address, rt.handle()).unwrap();
        let root = format!("/report-test-{}", sinomo_runtime::core::Id::generate());
        client.create_dir_all(&format!("{root}/figures")).unwrap();
        let svg = b"<svg xmlns='http://www.w3.org/2000/svg'/>";
        client
            .write_file(&format!("{root}/figures/test.svg"), svg)
            .unwrap();
        let report = |name: &str, mime, content| Report {
            source: FilesSourceKind::Sifs,
            base: root.clone(),
            name: name.into(),
            mime,
            content,
            address: Some(address.clone()),
            runtime: rt.handle().clone(),
        };
        let html = Origin::start(report(
            "report.html",
            "text/html",
            Content::Bytes(b"<img src='figures/test.svg'>".to_vec()),
        ))
        .unwrap();
        assert_eq!(get(&html, Some("figures/test.svg"), None).1, svg);
        assert_eq!(get(&html, Some("../outside.svg"), None).0, 404);
        let path = format!("{root}/report.pdf");
        let mut bytes = b"%PDF-1.7".to_vec();
        bytes.resize(3 * 1024 * 1024, 42);
        client.write_file(&path, &bytes).unwrap();
        let info = client.stat(&path).unwrap();
        let pdf = Origin::start(report(
            "report.pdf",
            "application/pdf",
            Content::SifsPdf(
                info.object_id,
                info.version,
                info.content_hash.unwrap(),
                info.size_bytes.unwrap(),
            ),
        ))
        .unwrap();
        let (status, body, _) = get(&pdf, None, Some("bytes=65000-135000"));
        assert_eq!(status, 206);
        assert_eq!(body, bytes[65000..135001]);
    }

    #[test]
    fn notebook_origin_serves_only_the_bundled_script_as_executable_content() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let dir = sinomo_testdir::TestDir::new("notebook-preview");
        std::fs::write(dir.join(notebook::SCRIPT_NAME), "untrusted sibling script").unwrap();
        let origin = Origin::start(Report {
            source: FilesSourceKind::Local,
            base: dir.to_string_lossy().into_owned(),
            name: "failed-run.ipynb".into(),
            mime: NOTEBOOK_MIME,
            content: Content::Bytes(
                include_bytes!("../../assets/notebook/testdata/failed-run.ipynb").to_vec(),
            ),
            runtime: runtime.handle().clone(),
            address: None,
        })
        .unwrap();
        let (status, body, headers) = get(&origin, None, None);
        let page = String::from_utf8(body).unwrap();
        assert_eq!(status, 200);
        assert!(headers.contains("script-src http://127.0.0.1:"));
        assert!(!headers.contains("script-src 'unsafe-inline'"));
        assert!(headers.contains("default-src 'none'"));
        assert!(!page.contains("</script><script>window.injectionRan"));
        assert!(page.contains(r"\u003ch1>Evaluation run"));
        let (status, script, _) = get(&origin, Some(notebook::SCRIPT_NAME), None);
        assert_eq!(status, 200);
        assert_eq!(script, notebook::SCRIPT.as_bytes());
    }
    #[test]
    fn report_pdf_serves_full_and_partial_responses() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let data = b"%PDF-1.7 fixture";
        let origin = Origin::start(Report {
            source: FilesSourceKind::Local,
            base: "".into(),
            name: "example.pdf".into(),
            mime: "application/pdf",
            content: Content::Bytes(data.to_vec()),
            runtime: runtime.handle().clone(),
            address: None,
        })
        .unwrap();
        assert_eq!(get(&origin, None, None).1, data);
        let (status, body, headers) = get(&origin, None, Some("bytes=0-4"));
        assert_eq!(status, 206);
        assert_eq!(body, b"%PDF-");
        assert!(headers.contains("bytes 0-4/16"));
        assert_eq!(get(&origin, None, Some("bytes=-7")).1, b"fixture");
        assert_eq!(get(&origin, None, Some("bytes=32-")).0, 416);
    }
}
