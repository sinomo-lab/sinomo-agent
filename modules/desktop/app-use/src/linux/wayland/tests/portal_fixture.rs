//! Private-bus portal fixture. Signals use the real public D-Bus signatures.
use super::*;
use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use zbus::zvariant::{OwnedObjectPath, OwnedValue, Value};
use zbus::{Connection, message::Header};
pub type Dict = HashMap<String, OwnedValue>;
fn value(v: impl Into<Value<'static>>) -> OwnedValue {
    v.into().try_to_owned().unwrap()
}

#[derive(Default)]
pub struct Fixture {
    pub code: AtomicU32,
    pub fail_input: AtomicBool,
    pub devices: AtomicU32,
    pub closes: AtomicUsize,
    pub requests_closed: AtomicUsize,
    pub starts: AtomicUsize,
    pub inputs: Mutex<Vec<String>>,
    pub sessions: Mutex<Vec<String>>,
    pub node: AtomicU32,
    pub serial: AtomicUsize,
}
struct Request(Arc<Fixture>);
#[zbus::interface(name = "org.freedesktop.portal.Request")]
impl Request {
    fn close(&self) {
        self.0.requests_closed.fetch_add(1, Ordering::AcqRel);
    }
}
struct Session(Arc<Fixture>);
#[zbus::interface(name = "org.freedesktop.portal.Session")]
impl Session {
    fn close(&self) {
        self.0.closes.fetch_add(1, Ordering::AcqRel);
    }
}
fn sender(header: &Header<'_>) -> String {
    header
        .sender()
        .unwrap()
        .as_str()
        .trim_start_matches(':')
        .replace('.', "_")
}
async fn respond(
    conn: &Connection,
    header: &Header<'_>,
    options: Dict,
    result: Dict,
    state: &Arc<Fixture>,
    code: u32,
) -> OwnedObjectPath {
    let token = <&str>::try_from(options.get("handle_token").unwrap()).unwrap();
    let path = OwnedObjectPath::try_from(format!(
        "/org/freedesktop/portal/desktop/request/{}/{token}",
        sender(header)
    ))
    .unwrap();
    conn.object_server()
        .at(path.clone(), Request(state.clone()))
        .await
        .unwrap();
    if code != u32::MAX {
        // Intentionally emit before the method response, exercising registration order.
        conn.emit_signal(
            header.sender().map(|s| s.as_str()),
            &path,
            "org.freedesktop.portal.Request",
            "Response",
            &(code, result),
        )
        .await
        .unwrap();
    }
    path
}
async fn create(
    conn: &Connection,
    header: &Header<'_>,
    options: Dict,
    state: &Arc<Fixture>,
) -> OwnedObjectPath {
    let token = <&str>::try_from(options.get("session_handle_token").unwrap()).unwrap();
    let path = format!(
        "/org/freedesktop/portal/desktop/session/{}/{token}",
        sender(header)
    );
    conn.object_server()
        .at(path.clone(), Session(state.clone()))
        .await
        .unwrap();
    state.sessions.lock().unwrap().push(path.clone());
    respond(
        conn,
        header,
        options,
        HashMap::from([("session_handle".into(), value(path))]),
        state,
        0,
    )
    .await
}
async fn start(
    conn: &Connection,
    header: &Header<'_>,
    options: Dict,
    state: &Arc<Fixture>,
) -> OwnedObjectPath {
    state.starts.fetch_add(1, Ordering::AcqRel);
    let props: Dict = HashMap::from([
        ("size".into(), value((640i32, 480i32))),
        ("source_type".into(), value(1u32)),
        (
            "pipewire-serial".into(),
            value(state.serial.load(Ordering::Acquire) as u64),
        ),
    ]);
    let streams = vec![(state.node.load(Ordering::Acquire), props)];
    let values = HashMap::from([
        ("streams".into(), value(streams)),
        (
            "devices".into(),
            value(state.devices.load(Ordering::Acquire)),
        ),
    ]);
    respond(
        conn,
        header,
        options,
        values,
        state,
        state.code.load(Ordering::Acquire),
    )
    .await
}
pub struct Remote(pub Arc<Fixture>);
#[zbus::interface(name = "org.freedesktop.portal.RemoteDesktop")]
impl Remote {
    #[zbus(property)]
    fn available_device_types(&self) -> u32 {
        self.0.devices.load(Ordering::Acquire)
    }
    #[zbus(property, name = "version")]
    fn version(&self) -> u32 {
        2
    }
    async fn create_session(
        &self,
        options: Dict,
        #[zbus(connection)] conn: &Connection,
        #[zbus(header)] header: Header<'_>,
    ) -> OwnedObjectPath {
        create(conn, &header, options, &self.0).await
    }
    async fn select_devices(
        &self,
        _session: OwnedObjectPath,
        options: Dict,
        #[zbus(connection)] conn: &Connection,
        #[zbus(header)] header: Header<'_>,
    ) -> OwnedObjectPath {
        assert!(!options.contains_key("persist_mode") && !options.contains_key("restore_token"));
        respond(conn, &header, options, Dict::new(), &self.0, 0).await
    }
    async fn start(
        &self,
        _session: OwnedObjectPath,
        _parent: &str,
        options: Dict,
        #[zbus(connection)] conn: &Connection,
        #[zbus(header)] header: Header<'_>,
    ) -> OwnedObjectPath {
        start(conn, &header, options, &self.0).await
    }
    fn notify_pointer_motion_absolute(
        &self,
        _session: OwnedObjectPath,
        _options: Dict,
        node: u32,
        x: f64,
        y: f64,
    ) {
        self.0
            .inputs
            .lock()
            .unwrap()
            .push(format!("absolute:{node}:{x}:{y}"));
    }
    fn notify_pointer_motion(&self, _session: OwnedObjectPath, _options: Dict, x: f64, y: f64) {
        self.0
            .inputs
            .lock()
            .unwrap()
            .push(format!("relative:{x}:{y}"));
    }
    fn notify_pointer_button(
        &self,
        _session: OwnedObjectPath,
        _options: Dict,
        button: i32,
        state: u32,
    ) {
        self.0
            .inputs
            .lock()
            .unwrap()
            .push(format!("button:{button}:{state}"));
    }
    fn notify_pointer_axis_discrete(
        &self,
        _session: OwnedObjectPath,
        _options: Dict,
        axis: u32,
        steps: i32,
    ) {
        self.0
            .inputs
            .lock()
            .unwrap()
            .push(format!("wheel:{axis}:{steps}"));
    }
    fn notify_keyboard_keycode(
        &self,
        _session: OwnedObjectPath,
        _options: Dict,
        code: i32,
        state: u32,
    ) -> zbus::fdo::Result<()> {
        self.0
            .inputs
            .lock()
            .unwrap()
            .push(format!("key:{code}:{state}"));
        if self.0.fail_input.load(Ordering::Acquire) {
            Err(zbus::fdo::Error::Failed(
                "input outcome is uncertain".into(),
            ))
        } else {
            Ok(())
        }
    }
    fn notify_keyboard_keysym(
        &self,
        _session: OwnedObjectPath,
        _options: Dict,
        symbol: i32,
        state: u32,
    ) {
        self.0
            .inputs
            .lock()
            .unwrap()
            .push(format!("symbol:{symbol}:{state}"));
    }
}
pub struct Screen(pub Arc<Fixture>);
#[zbus::interface(name = "org.freedesktop.portal.ScreenCast")]
impl Screen {
    #[zbus(property)]
    fn available_source_types(&self) -> u32 {
        1
    }
    #[zbus(property, name = "version")]
    fn version(&self) -> u32 {
        6
    }
    async fn create_session(
        &self,
        options: Dict,
        #[zbus(connection)] conn: &Connection,
        #[zbus(header)] header: Header<'_>,
    ) -> OwnedObjectPath {
        create(conn, &header, options, &self.0).await
    }
    async fn select_sources(
        &self,
        _session: OwnedObjectPath,
        options: Dict,
        #[zbus(connection)] conn: &Connection,
        #[zbus(header)] header: Header<'_>,
    ) -> OwnedObjectPath {
        assert_eq!(u32::try_from(options.get("types").unwrap()).unwrap(), 1);
        assert!(!bool::try_from(options.get("multiple").unwrap()).unwrap());
        assert!(!options.contains_key("restore_token") && !options.contains_key("persist_mode"));
        respond(conn, &header, options, Dict::new(), &self.0, 0).await
    }
    async fn start(
        &self,
        _session: OwnedObjectPath,
        _parent: &str,
        options: Dict,
        #[zbus(connection)] conn: &Connection,
        #[zbus(header)] header: Header<'_>,
    ) -> OwnedObjectPath {
        start(conn, &header, options, &self.0).await
    }
    fn open_pipe_wire_remote(
        &self,
        _session: OwnedObjectPath,
        _options: Dict,
    ) -> zbus::fdo::Result<zbus::zvariant::OwnedFd> {
        let path = std::env::var("SINOMO_TEST_PIPEWIRE_SOCKET")
            .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))?;
        let socket = std::os::unix::net::UnixStream::connect(path)
            .map_err(|e| zbus::fdo::Error::Failed(e.to_string()))?;
        Ok(OwnedFd::from(socket).into())
    }
}
