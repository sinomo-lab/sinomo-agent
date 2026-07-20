use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread;
use std::time::Duration;

pub(crate) const PROMETHEUS_DEFAULT_LISTEN: &str = "127.0.0.1:9464";
pub(crate) const PROMETHEUS_DEFAULT_REFRESH_SECONDS: u64 = 15;
pub(crate) const PROMETHEUS_MIN_REFRESH_SECONDS: u64 = 1;
pub(crate) const PROMETHEUS_MAX_REFRESH_SECONDS: u64 = 3_600;

const HEALTH_CONTENT_TYPE: &str = "text/plain; charset=utf-8";
const MAX_HTTP_REQUEST_BYTES: usize = 8 * 1024;
const HTTP_IO_TIMEOUT: Duration = Duration::from_secs(2);
const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(20);
const COLLECTION_ERROR_BODY: &str = "# HELP sinomo_observe_prometheus_collection_success Whether the most recent Sinomo observation collection succeeded.\n\
# TYPE sinomo_observe_prometheus_collection_success gauge\n\
sinomo_observe_prometheus_collection_success 0\n\
# EOF\n";

pub(crate) type PrometheusCollector =
    Arc<dyn Fn() -> Result<String, String> + Send + Sync + 'static>;

pub(crate) struct PrometheusSignalGuard {
    registrations: Vec<signal_hook::SigId>,
}

impl Drop for PrometheusSignalGuard {
    fn drop(&mut self) {
        unregister_signal_handlers(&mut self.registrations);
    }
}

type PrometheusSignalState = (Arc<AtomicBool>, PrometheusSignalGuard);

pub(crate) fn install_prometheus_shutdown_signals() -> Result<PrometheusSignalState, String> {
    let shutdown = Arc::new(AtomicBool::new(false));
    let mut registrations = Vec::with_capacity(signal_hook::consts::TERM_SIGNALS.len() * 2);
    for signal in signal_hook::consts::TERM_SIGNALS {
        let result = (|| {
            registrations.push(signal_hook::flag::register_conditional_default(
                *signal,
                Arc::clone(&shutdown),
            )?);
            registrations.push(signal_hook::flag::register(*signal, Arc::clone(&shutdown))?);
            Ok::<(), std::io::Error>(())
        })();
        if let Err(source) = result {
            unregister_signal_handlers(&mut registrations);
            return Err(format!(
                "failed to install Prometheus exporter shutdown signal: {source}"
            ));
        }
    }
    Ok((shutdown, PrometheusSignalGuard { registrations }))
}

fn unregister_signal_handlers(registrations: &mut Vec<signal_hook::SigId>) {
    for registration in registrations.drain(..) {
        let _ = signal_hook::low_level::unregister(registration);
    }
}

#[derive(Clone, Default)]
pub(crate) struct PrometheusShutdown {
    inner: Arc<PrometheusShutdownInner>,
}

#[derive(Default)]
struct PrometheusShutdownInner {
    cancelled: AtomicBool,
    wait_lock: Mutex<()>,
    wait_condition: Condvar,
}

impl PrometheusShutdown {
    pub(crate) fn cancel(&self) {
        self.inner.cancelled.store(true, Ordering::Release);
        self.inner.wait_condition.notify_all();
    }

    fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    fn wait_timeout(&self, timeout: Duration) -> bool {
        if self.is_cancelled() {
            return true;
        }
        let Ok(guard) = self.inner.wait_lock.lock() else {
            return true;
        };
        let _ = self
            .inner
            .wait_condition
            .wait_timeout_while(guard, timeout, |_| !self.is_cancelled());
        self.is_cancelled()
    }
}

#[derive(Clone)]
struct PrometheusCache {
    body: Arc<RwLock<Arc<str>>>,
}

impl Default for PrometheusCache {
    fn default() -> Self {
        Self {
            body: Arc::new(RwLock::new(Arc::from(COLLECTION_ERROR_BODY))),
        }
    }
}

impl PrometheusCache {
    fn replace(&self, result: Result<String, String>) {
        let body: Arc<str> = match result {
            Ok(body) => Arc::from(body),
            Err(_) => Arc::from(COLLECTION_ERROR_BODY),
        };
        if let Ok(mut cached) = self.body.write() {
            *cached = body;
        }
    }

    fn body(&self) -> Arc<str> {
        self.body
            .read()
            .map(|body| Arc::clone(&body))
            .unwrap_or_else(|_| Arc::from(COLLECTION_ERROR_BODY))
    }
}

pub(crate) struct PrometheusHttpServer {
    listener: TcpListener,
}

impl PrometheusHttpServer {
    pub(crate) fn bind(address: SocketAddr) -> Result<Self, String> {
        if !address.ip().is_loopback() {
            return Err(format!(
                "Prometheus exporter listen address `{address}` is not loopback; expose it through an authenticated reverse proxy"
            ));
        }
        let listener = TcpListener::bind(address).map_err(|source| {
            format!("failed to bind Prometheus exporter at {address}: {source}")
        })?;
        listener.set_nonblocking(true).map_err(|source| {
            format!("failed to configure Prometheus exporter listener at {address}: {source}")
        })?;
        Ok(Self { listener })
    }

    pub(crate) fn local_addr(&self) -> Result<SocketAddr, String> {
        self.listener.local_addr().map_err(|source| {
            format!("failed to read Prometheus exporter listen address: {source}")
        })
    }

    pub(crate) fn serve(
        self,
        refresh_interval: Duration,
        collector: PrometheusCollector,
        shutdown: PrometheusShutdown,
        external_shutdown: Arc<AtomicBool>,
    ) -> Result<(), String> {
        let cache = PrometheusCache::default();
        let collector_cache = cache.clone();
        let collector_shutdown = shutdown.clone();
        let collector_thread = thread::Builder::new()
            .name("sinomo-prometheus-collector".to_string())
            .spawn(move || {
                while !collector_shutdown.is_cancelled() {
                    let result =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| collector()))
                            .unwrap_or_else(|_| Err("Prometheus collector panicked".to_string()));
                    collector_cache.replace(result);
                    if collector_shutdown.wait_timeout(refresh_interval) {
                        break;
                    }
                }
            })
            .map_err(|source| format!("failed to start Prometheus collector: {source}"))?;

        let serve_result = self.serve_connections(&cache, &shutdown, &external_shutdown);
        external_shutdown.store(true, Ordering::Release);
        shutdown.cancel();
        let join_result = collector_thread
            .join()
            .map_err(|_| "Prometheus collector thread panicked".to_string());
        serve_result.and(join_result)
    }

    fn serve_connections(
        &self,
        cache: &PrometheusCache,
        shutdown: &PrometheusShutdown,
        external_shutdown: &AtomicBool,
    ) -> Result<(), String> {
        while !shutdown.is_cancelled() && !external_shutdown.load(Ordering::Acquire) {
            match self.listener.accept() {
                Ok((stream, _peer)) => handle_http_connection(stream, cache),
                Err(source) if source.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(ACCEPT_POLL_INTERVAL);
                }
                Err(source) => {
                    return Err(format!("Prometheus exporter accept failed: {source}"));
                }
            }
        }
        Ok(())
    }
}

fn handle_http_connection(mut stream: TcpStream, cache: &PrometheusCache) {
    let _ = stream.set_read_timeout(Some(HTTP_IO_TIMEOUT));
    let _ = stream.set_write_timeout(Some(HTTP_IO_TIMEOUT));
    let request = match read_http_request(&mut stream) {
        Ok(request) => request,
        Err(HttpReadError::Timeout) => {
            let _ = write_http_response(
                &mut stream,
                "408 Request Timeout",
                HEALTH_CONTENT_TYPE,
                "request timeout\n",
                true,
                None,
            );
            return;
        }
        Err(HttpReadError::TooLarge) => {
            let _ = write_http_response(
                &mut stream,
                "431 Request Header Fields Too Large",
                HEALTH_CONTENT_TYPE,
                "request headers too large\n",
                true,
                None,
            );
            return;
        }
        Err(HttpReadError::Malformed) => {
            let _ = write_http_response(
                &mut stream,
                "400 Bad Request",
                HEALTH_CONTENT_TYPE,
                "bad request\n",
                true,
                None,
            );
            return;
        }
    };

    let Some((method, path)) = parse_http_request_line(&request) else {
        let _ = write_http_response(
            &mut stream,
            "400 Bad Request",
            HEALTH_CONTENT_TYPE,
            "bad request\n",
            true,
            None,
        );
        return;
    };
    let include_body = method == "GET";
    if !matches!(method, "GET" | "HEAD") {
        let _ = write_http_response(
            &mut stream,
            "405 Method Not Allowed",
            HEALTH_CONTENT_TYPE,
            "method not allowed\n",
            true,
            Some("Allow: GET, HEAD\r\n"),
        );
        return;
    }
    match path {
        "/metrics" => {
            let body = cache.body();
            let _ = write_http_response(
                &mut stream,
                "200 OK",
                crate::OPENMETRICS_CONTENT_TYPE,
                &body,
                include_body,
                None,
            );
        }
        "/-/healthy" => {
            let _ = write_http_response(
                &mut stream,
                "200 OK",
                HEALTH_CONTENT_TYPE,
                "ok\n",
                include_body,
                None,
            );
        }
        _ => {
            let _ = write_http_response(
                &mut stream,
                "404 Not Found",
                HEALTH_CONTENT_TYPE,
                "not found\n",
                include_body,
                None,
            );
        }
    }
}

enum HttpReadError {
    Timeout,
    TooLarge,
    Malformed,
}

fn read_http_request(stream: &mut TcpStream) -> Result<Vec<u8>, HttpReadError> {
    let mut request = Vec::with_capacity(1024);
    let mut chunk = [0_u8; 1024];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => return Err(HttpReadError::Malformed),
            Ok(count) => {
                if request.len().saturating_add(count) > MAX_HTTP_REQUEST_BYTES {
                    return Err(HttpReadError::TooLarge);
                }
                request.extend_from_slice(&chunk[..count]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    return Ok(request);
                }
                if request.len() == MAX_HTTP_REQUEST_BYTES {
                    return Err(HttpReadError::TooLarge);
                }
            }
            Err(source)
                if matches!(
                    source.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                return Err(HttpReadError::Timeout);
            }
            Err(_) => return Err(HttpReadError::Malformed),
        }
    }
}

fn parse_http_request_line(request: &[u8]) -> Option<(&str, &str)> {
    let line_end = request.windows(2).position(|window| window == b"\r\n")?;
    let line = std::str::from_utf8(&request[..line_end]).ok()?;
    let mut parts = line.split_ascii_whitespace();
    let method = parts.next()?;
    let path = parts.next()?;
    let version = parts.next()?;
    if parts.next().is_some() || !matches!(version, "HTTP/1.0" | "HTTP/1.1") {
        return None;
    }
    Some((method, path))
}

fn write_http_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &str,
    include_body: bool,
    extra_headers: Option<&str>,
) -> std::io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n",
        body.len(),
        extra_headers.unwrap_or_default()
    )?;
    if include_body {
        stream.write_all(body.as_bytes())?;
    }
    stream.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Instant;

    fn test_body() -> String {
        "# TYPE sinomo_test gauge\nsinomo_test 7\n# EOF\n".to_string()
    }

    fn send_request(address: SocketAddr, request: &str) -> String {
        let mut stream = TcpStream::connect(address).expect("connect exporter");
        stream.write_all(request.as_bytes()).expect("write request");
        let mut response = String::new();
        stream.read_to_string(&mut response).expect("read response");
        response
    }

    fn wait_for_collection(collections: &AtomicUsize) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while collections.load(Ordering::Acquire) == 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(collections.load(Ordering::Acquire), 1);
    }

    #[test]
    fn exporter_rejects_non_loopback_listeners() {
        let address: SocketAddr = "0.0.0.0:9464".parse().expect("address");
        assert!(PrometheusHttpServer::bind(address).is_err());
    }

    #[test]
    fn metrics_scrapes_read_the_cache_without_recollecting() {
        let server = PrometheusHttpServer::bind("127.0.0.1:0".parse().expect("address"))
            .expect("bind exporter");
        let address = server.local_addr().expect("listen address");
        let shutdown = PrometheusShutdown::default();
        let server_shutdown = shutdown.clone();
        let collections = Arc::new(AtomicUsize::new(0));
        let collector: PrometheusCollector = {
            let collections = Arc::clone(&collections);
            Arc::new(move || {
                collections.fetch_add(1, Ordering::AcqRel);
                Ok(test_body())
            })
        };
        let server_thread = thread::spawn(move || {
            server.serve(
                Duration::from_secs(60),
                collector,
                server_shutdown,
                Arc::new(AtomicBool::new(false)),
            )
        });
        wait_for_collection(&collections);

        for _ in 0..2 {
            let response =
                send_request(address, "GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n");
            assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
            assert!(response.contains(crate::OPENMETRICS_CONTENT_TYPE));
            assert!(response.ends_with(&test_body()));
        }
        assert_eq!(collections.load(Ordering::Acquire), 1);

        shutdown.cancel();
        server_thread
            .join()
            .expect("server thread")
            .expect("serve exporter");
    }

    #[test]
    fn exporter_supports_head_health_and_bounded_error_responses() {
        let server = PrometheusHttpServer::bind("127.0.0.1:0".parse().expect("address"))
            .expect("bind exporter");
        let address = server.local_addr().expect("listen address");
        let shutdown = PrometheusShutdown::default();
        let server_shutdown = shutdown.clone();
        let collections = Arc::new(AtomicUsize::new(0));
        let collector: PrometheusCollector = {
            let collections = Arc::clone(&collections);
            Arc::new(move || {
                collections.fetch_add(1, Ordering::AcqRel);
                Ok(test_body())
            })
        };
        let server_thread = thread::spawn(move || {
            server.serve(
                Duration::from_secs(60),
                collector,
                server_shutdown,
                Arc::new(AtomicBool::new(false)),
            )
        });
        wait_for_collection(&collections);

        let head = send_request(address, "HEAD /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n");
        assert!(head.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(head.ends_with("\r\n\r\n"));
        assert!(!head.contains("sinomo_test 7"));

        let health = send_request(
            address,
            "GET /-/healthy HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );
        assert!(health.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(health.ends_with("ok\n"));

        let missing = send_request(address, "GET /missing HTTP/1.1\r\nHost: localhost\r\n\r\n");
        assert!(missing.starts_with("HTTP/1.1 404 Not Found\r\n"));

        let method = send_request(address, "POST /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n");
        assert!(method.starts_with("HTTP/1.1 405 Method Not Allowed\r\n"));
        assert!(method.contains("Allow: GET, HEAD\r\n"));

        shutdown.cancel();
        server_thread
            .join()
            .expect("server thread")
            .expect("serve exporter");
    }
}
