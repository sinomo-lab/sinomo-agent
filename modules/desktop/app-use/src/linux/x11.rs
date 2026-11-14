//! X11 backend. Every worker owns its connection; no process-global Xlib state,
//! focus changes, keyboard-map edits, shell helpers, or implicit raw fallback.
use super::*;
use sinomo_device::{NativeVideoPlane, NativeVideoSampleFormat};
use std::os::fd::AsRawFd;
use std::sync::Arc;
use x11rb::connection::Connection;
use x11rb::protocol::{Event, composite, damage, res, xfixes, xinput, xproto::*, xtest};
use x11rb::rust_connection::RustConnection;

#[path = "x11_readback.rs"]
mod readback;

fn platform(error: impl std::fmt::Display) -> DesktopError {
    DesktopError::Platform(error.to_string())
}
fn unavailable(error: impl std::fmt::Display) -> DesktopError {
    DesktopError::TargetUnavailable(error.to_string())
}
struct X11 {
    conn: RustConnection,
    screen: usize,
}
impl X11 {
    fn open() -> DesktopResult<Self> {
        // Xwayland is not a whole-desktop capture/input authority. Do not use
        // it to advertise raw control over a native Wayland desktop.
        if std::env::var_os("WAYLAND_DISPLAY").is_some()
            || std::env::var("XDG_SESSION_TYPE").is_ok_and(|s| s == "wayland")
        {
            return Err(DesktopError::Unsupported(
                "the X11 backend cannot operate a Wayland session".into(),
            ));
        }
        if !std::env::var("DISPLAY").is_ok_and(|display| display.starts_with(':')) {
            return Err(DesktopError::Unsupported(
                "App Use requires a local Unix-socket X11 DISPLAY (for example :0)".into(),
            ));
        }
        let (conn, screen) = x11rb::connect(None).map_err(|e| {
            DesktopError::Unsupported(format!("an accessible local X11 DISPLAY is required: {e}"))
        })?;
        Ok(Self { conn, screen })
    }
    fn root(&self) -> Window {
        self.conn.setup().roots[self.screen].root
    }
    fn display_name(&self) -> String {
        format!(
            "x11:{}:{}",
            std::env::var("DISPLAY").unwrap_or_default(),
            self.root()
        )
    }
    fn atom(&self, name: &[u8]) -> DesktopResult<Atom> {
        Ok(self
            .conn
            .intern_atom(false, name)
            .map_err(platform)?
            .reply()
            .map_err(platform)?
            .atom)
    }
    fn property(&self, window: Window, name: &[u8], ty: Atom) -> DesktopResult<GetPropertyReply> {
        self.conn
            .get_property(false, window, self.atom(name)?, ty, 0, 4096)
            .map_err(platform)?
            .reply()
            .map_err(unavailable)
    }
    fn identity(&self, window: Window) -> DesktopResult<DesktopTarget> {
        // _NET_WM_PID is client-supplied. XRes identifies the actual local
        // connection owner, including child windows without EWMH properties.
        let ids = res::query_client_ids(
            &self.conn,
            &[res::ClientIdSpec {
                client: window,
                mask: res::ClientIdMask::LOCAL_CLIENT_PID,
            }],
        )
        .map_err(platform)?
        .reply()
        .map_err(unavailable)?;
        let pid = ids
            .ids
            .iter()
            .find_map(|id| {
                (id.spec.mask & res::ClientIdMask::LOCAL_CLIENT_PID != res::ClientIdMask::default())
                    .then(|| id.value.first().copied())
                    .flatten()
            })
            .filter(|pid| *pid > 0)
            .ok_or_else(|| unavailable("XRes cannot identify a local window owner"))?;
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).map_err(unavailable)?;
        let started = process_start(&stat)?;
        self.conn
            .get_window_attributes(window)
            .map_err(platform)?
            .reply()
            .map_err(unavailable)?;
        Ok(DesktopTarget::Window {
            handle: window.into(),
            process_id: pid,
            thread_id: 0,
            process_started: started,
        })
    }
    fn resolve(&self, target: &DesktopTarget) -> DesktopResult<Window> {
        match target {
            DesktopTarget::Monitor { device_name } if *device_name == self.display_name() => {
                Ok(self.root())
            }
            DesktopTarget::Window { handle, .. } => {
                let window = u32::try_from(*handle).map_err(unavailable)?;
                if self.identity(window)? != *target {
                    return Err(unavailable("X11 window owner changed; enumerate again"));
                }
                Ok(window)
            }
            _ => Err(unavailable("X11 display identity changed; enumerate again")),
        }
    }
    fn bounds(&self, window: Window) -> DesktopResult<[i32; 4]> {
        let geo = self
            .conn
            .get_geometry(window)
            .map_err(platform)?
            .reply()
            .map_err(unavailable)?;
        let pos = self
            .conn
            .translate_coordinates(window, self.root(), 0, 0)
            .map_err(platform)?
            .reply()
            .map_err(unavailable)?;
        Ok([
            pos.dst_x.into(),
            pos.dst_y.into(),
            i32::from(pos.dst_x) + i32::from(geo.width),
            i32::from(pos.dst_y) + i32::from(geo.height),
        ])
    }
    fn info(&self, window: Window) -> DesktopResult<DesktopTargetInfo> {
        let target = self.identity(window)?;
        let title = self
            .property(window, b"_NET_WM_NAME", self.atom(b"UTF8_STRING")?)
            .ok()
            .filter(|p| !p.value.is_empty())
            .or_else(|| {
                self.property(window, b"WM_NAME", AtomEnum::STRING.into())
                    .ok()
            })
            .map(|p| String::from_utf8_lossy(&p.value).into_owned())
            .unwrap_or_default();
        let bounds = self.bounds(window)?;
        let attr = self
            .conn
            .get_window_attributes(window)
            .map_err(platform)?
            .reply()
            .map_err(unavailable)?;
        Ok(DesktopTargetInfo {
            resource_id: target.resource_id(),
            target,
            title,
            bounds,
            client_bounds: Some(bounds),
            minimized: attr.map_state != MapState::VIEWABLE,
        })
    }
}
fn process_start(stat: &str) -> DesktopResult<u64> {
    // comm may contain spaces and ')'; field 22 follows the final closing ')'.
    stat.rsplit_once(')')
        .and_then(|(_, tail)| tail.split_whitespace().nth(19))
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .ok_or_else(|| unavailable("invalid local process start time"))
}

pub fn capabilities() -> DesktopCapabilities {
    let mut caps = DesktopCapabilities { frame_rate_ceiling: true, accessibility: false, capture_formats: vec![sinomo_device::NativeVideoSampleFormat::Bgra8],
        platform: "linux".into(), capture: false, keyboard_mouse: false,
        background_window_input: false, gamepad: "unsupported".into(),
        limitations: vec![
            "X11 backend selected; Wayland sessions use the portal backend".into(),
            "Cursor capture requires XInput2 motion and XFixes cursor notifications".into(),
            "Background events require application support for synthetic X11 events; minimized windows cannot be captured".into(),
            "Text is limited to characters in the current X keyboard map; gamepad injection is unavailable".into(),
        ],
    };
    match X11::open() {
        Ok(x) => {
            let has = |name: &[u8]| {
                x.conn
                    .query_extension(name)
                    .ok()
                    .and_then(|c| c.reply().ok())
                    .is_some_and(|r| r.present)
            };
            caps.background_window_input = has(b"X-Resource");
            caps.keyboard_mouse = has(b"XTEST");
            caps.capture = has(b"Composite")
                && has(b"DAMAGE")
                && has(b"XFIXES")
                && caps.background_window_input;
        }
        Err(error) => caps.limitations.push(error.to_string()),
    }
    caps
}
pub fn enumerate_targets(windows: bool) -> DesktopResult<Vec<DesktopTargetInfo>> {
    let x = X11::open()?;
    if !windows {
        let target = DesktopTarget::Monitor {
            device_name: x.display_name(),
        };
        return Ok(vec![DesktopTargetInfo {
            resource_id: target.resource_id(),
            target,
            title: "X11 virtual desktop".into(),
            bounds: x.bounds(x.root())?,
            client_bounds: None,
            minimized: false,
        }]);
    }
    let clients = x.property(x.root(), b"_NET_CLIENT_LIST", AtomEnum::WINDOW.into())?;
    let windows = if clients.type_ == u32::from(AtomEnum::WINDOW) {
        clients
            .value32()
            .into_iter()
            .flatten()
            .take(MAX_TARGETS)
            .collect()
    } else {
        x.conn
            .query_tree(x.root())
            .map_err(platform)?
            .reply()
            .map_err(platform)?
            .children
    };
    Ok(windows
        .into_iter()
        .take(MAX_TARGETS)
        .filter_map(|w| x.info(w).ok())
        .collect())
}
pub fn enumerate_child_windows(target: &DesktopTarget) -> DesktopResult<Vec<DesktopTargetInfo>> {
    let x = X11::open()?;
    let window = x.resolve(target)?;
    if !target.is_window() {
        return Err(DesktopError::Invalid("children requires a window".into()));
    }
    Ok(x.conn
        .query_tree(window)
        .map_err(platform)?
        .reply()
        .map_err(unavailable)?
        .children
        .into_iter()
        .take(MAX_TARGETS)
        .filter_map(|w| x.info(w).ok())
        .collect())
}
pub fn validate_target(target: &DesktopTarget) -> DesktopResult<()> {
    X11::open()?.resolve(target).map(|_| ())
}
pub struct Input {
    x: X11,
    target: DesktopTarget,
    window: Window,
    position: (i16, i16),
    modifiers: u16,
    // Pin native codes for held keys so a layout change cannot redirect key-up.
    keycodes: std::collections::BTreeMap<(u16, bool), u8>,
}
impl Input {
    pub fn new(target: &DesktopTarget) -> DesktopResult<Self> {
        let x = X11::open()?;
        let window = x.resolve(target)?;
        if !target.is_window() {
            xtest::get_version(&x.conn, 2, 2)
                .map_err(platform)?
                .reply()
                .map_err(platform)?;
        }
        Ok(Self {
            x,
            target: target.clone(),
            window,
            position: (0, 0),
            modifiers: 0,
            keycodes: std::collections::BTreeMap::new(),
        })
    }
    fn keycode(&self, keysym: u32) -> DesktopResult<(u8, bool)> {
        let setup = self.x.conn.setup();
        let map = self
            .x
            .conn
            .get_keyboard_mapping(setup.min_keycode, setup.max_keycode - setup.min_keycode + 1)
            .map_err(platform)?
            .reply()
            .map_err(platform)?;
        if map.keysyms_per_keycode == 0 {
            return Err(platform("empty X keyboard map"));
        }
        for (index, symbols) in map
            .keysyms
            .chunks(map.keysyms_per_keycode.into())
            .enumerate()
        {
            for (level, symbol) in symbols.iter().take(2).enumerate() {
                if *symbol == keysym {
                    return Ok((setup.min_keycode + index as u8, level == 1));
                }
            }
        }
        Err(DesktopError::Unsupported(format!(
            "keysym {keysym:#x} is absent from the current X11 keyboard map"
        )))
    }
    fn event(&self, ty: u8, detail: u8) -> DesktopResult<()> {
        if !self.target.is_window() {
            return xtest::fake_input(
                &self.x.conn,
                ty,
                detail,
                x11rb::CURRENT_TIME,
                self.x.root(),
                self.position.0,
                self.position.1,
                0,
            )
            .map_err(platform)?
            .check()
            .map_err(platform);
        }
        let bounds = self.x.bounds(self.window)?;
        let event = KeyPressEvent {
            response_type: ty,
            detail,
            sequence: 0,
            time: x11rb::CURRENT_TIME,
            root: self.x.root(),
            event: self.window,
            child: x11rb::NONE,
            root_x: (bounds[0] + i32::from(self.position.0)) as i16,
            root_y: (bounds[1] + i32::from(self.position.1)) as i16,
            event_x: self.position.0,
            event_y: self.position.1,
            state: self.modifiers.into(),
            same_screen: true,
        };
        let mask = match ty {
            KEY_PRESS_EVENT => EventMask::KEY_PRESS,
            KEY_RELEASE_EVENT => EventMask::KEY_RELEASE,
            BUTTON_PRESS_EVENT => EventMask::BUTTON_PRESS,
            BUTTON_RELEASE_EVENT => EventMask::BUTTON_RELEASE,
            _ => EventMask::POINTER_MOTION,
        };
        self.x
            .conn
            .send_event(false, self.window, mask, event)
            .map_err(platform)?
            .check()
            .map_err(platform)
    }
    fn move_to(&mut self, x: i32, y: i32) -> DesktopResult<()> {
        let bounds = self.x.bounds(self.window)?;
        let (left, top, right, bottom) = if self.target.is_window() {
            (0, 0, bounds[2] - bounds[0], bounds[3] - bounds[1])
        } else {
            (bounds[0], bounds[1], bounds[2], bounds[3])
        };
        if x < left || y < top || x >= right || y >= bottom {
            return Err(DesktopError::Invalid(
                "pointer is outside the selected target".into(),
            ));
        }
        self.position = (
            i16::try_from(x).map_err(platform)?,
            i16::try_from(y).map_err(platform)?,
        );
        self.event(MOTION_NOTIFY_EVENT, 0)
    }
    pub fn send(&mut self, event: &DesktopInput) -> DesktopResult<()> {
        // Validate against the connection that performs the effect too.
        self.x.resolve(&self.target)?;
        match event {
            DesktopInput::Move { x, y } => self.move_to(*x, *y)?,
            DesktopInput::MoveInFrame {
                x,
                y,
                source_width,
                source_height,
            } => {
                let bounds = self.x.bounds(self.window)?;
                let (x, y) =
                    frame_point_to_client(*x, *y, [*source_width, *source_height], bounds, bounds)?;
                self.move_to(x, y)?;
            }
            DesktopInput::RelativeMove { dx, dy } => {
                let pointer = self
                    .x
                    .conn
                    .query_pointer(self.x.root())
                    .map_err(platform)?
                    .reply()
                    .map_err(platform)?;
                self.move_to(
                    i32::from(pointer.root_x).saturating_add(*dx),
                    i32::from(pointer.root_y).saturating_add(*dy),
                )?;
            }
            DesktopInput::Key {
                code,
                extended,
                down,
            } => {
                let key = match self.keycodes.get(&(*code, *extended)) {
                    Some(key) => *key,
                    None if self.target.is_window() => self.keycode(virtual_keysym(*code)?)?.0,
                    None => scan_keycode(*code, *extended)?,
                };
                self.event(
                    if *down {
                        KEY_PRESS_EVENT
                    } else {
                        KEY_RELEASE_EVENT
                    },
                    key,
                )?;
                if *down {
                    self.keycodes.insert((*code, *extended), key);
                } else {
                    self.keycodes.remove(&(*code, *extended));
                }
                if self.target.is_window() {
                    self.modifiers &= !0x4d;
                    for (code, _) in self.keycodes.keys() {
                        self.modifiers |= match code {
                            0x10 | 0xa0 | 0xa1 => 1,
                            0x11 | 0xa2 | 0xa3 => 4,
                            0x12 | 0xa4 | 0xa5 => 8,
                            0x5b | 0x5c => 64,
                            _ => 0,
                        };
                    }
                }
            }
            DesktopInput::Text { text } => {
                // Resolve all characters before the first effect. Never change
                // the user's keyboard map to make a character available.
                let keys = text
                    .chars()
                    .map(|c| {
                        self.keycode(match c {
                            '\n' => 0xff0d,
                            '\t' => 0xff09,
                            c if u32::from(c) <= 255 => c.into(),
                            c => 0x01000000 | u32::from(c),
                        })
                    })
                    .collect::<DesktopResult<Vec<_>>>()?;
                let shift = self.keycode(0xffe1)?.0;
                for (key, shifted) in keys {
                    let before = self.modifiers;
                    if shifted {
                        self.event(KEY_PRESS_EVENT, shift)?;
                        self.modifiers |= 1;
                    }
                    let result = self
                        .event(KEY_PRESS_EVENT, key)
                        .and_then(|_| self.event(KEY_RELEASE_EVENT, key));
                    if shifted {
                        let released = self.event(KEY_RELEASE_EVENT, shift);
                        self.modifiers = before;
                        released?;
                    }
                    result?;
                }
            }
            DesktopInput::Button { button, down } => {
                let detail = match button {
                    MouseButton::Left => 1,
                    MouseButton::Middle => 2,
                    MouseButton::Right => 3,
                    MouseButton::X1 => 8,
                    MouseButton::X2 => 9,
                };
                self.event(
                    if *down {
                        BUTTON_PRESS_EVENT
                    } else {
                        BUTTON_RELEASE_EVENT
                    },
                    detail,
                )?;
                if detail <= 3 {
                    let mask = 1 << (7 + detail);
                    if *down {
                        self.modifiers |= mask;
                    } else {
                        self.modifiers &= !mask;
                    }
                }
            }
            DesktopInput::Wheel { delta, horizontal } => {
                if *delta % 120 != 0 {
                    return Err(DesktopError::Unsupported(
                        "X11 wheel delta must be a multiple of 120".into(),
                    ));
                }
                let button = match (*horizontal, *delta > 0) {
                    (false, true) => 4,
                    (false, false) => 5,
                    (true, false) => 6,
                    (true, true) => 7,
                };
                for _ in 0..(i32::from(*delta).unsigned_abs() / 120) {
                    self.event(BUTTON_PRESS_EVENT, button)?;
                    self.event(BUTTON_RELEASE_EVENT, button)?;
                }
            }
            DesktopInput::Gamepad { .. } => {
                return Err(DesktopError::Unsupported(
                    "X11 gamepad injection is unavailable".into(),
                ));
            }
            DesktopInput::ReleaseAll => {}
        }
        self.x.conn.flush().map_err(platform)
    }
    pub fn release_gamepad(&mut self) {}
}

fn virtual_keysym(key: u16) -> DesktopResult<u32> {
    Ok(match key {
        0x30..=0x39 => key.into(),
        0x41..=0x5a => u32::from(key + 32),
        0x70..=0x87 => 0xffbe + u32::from(key - 0x70),
        0x08 => 0xff08,
        0x09 => 0xff09,
        0x0d => 0xff0d,
        0x1b => 0xff1b,
        0x20 => 0x20,
        0x10 | 0xa0 => 0xffe1,
        0xa1 => 0xffe2,
        0x11 | 0xa2 => 0xffe3,
        0xa3 => 0xffe4,
        0x12 | 0xa4 => 0xffe9,
        0xa5 => 0xffea,
        0x14 => 0xffe5,
        0x21 => 0xff55,
        0x22 => 0xff56,
        0x23 => 0xff57,
        0x24 => 0xff50,
        0x25 => 0xff51,
        0x26 => 0xff52,
        0x27 => 0xff53,
        0x28 => 0xff54,
        0x2d => 0xff63,
        0x2e => 0xffff,
        0x5b => 0xffeb,
        0x5c => 0xffec,
        0xba => b';'.into(),
        0xbb => b'='.into(),
        0xbc => b','.into(),
        0xbd => b'-'.into(),
        0xbe => b'.'.into(),
        0xbf => b'/'.into(),
        0xc0 => b'`'.into(),
        0xdb => b'['.into(),
        0xdc => b'\\'.into(),
        0xdd => b']'.into(),
        0xde => b'\''.into(),
        _ => {
            return Err(DesktopError::Unsupported(format!(
                "virtual key {key:#x} has no X11 mapping"
            )));
        }
    })
}
pub(super) fn scan_keycode(code: u16, extended: bool) -> DesktopResult<u8> {
    let evdev = if extended {
        match code {
            0x1c => 96,
            0x1d => 97,
            0x35 => 98,
            0x37 => 99,
            0x38 => 100,
            0x47 => 102,
            0x48 => 103,
            0x49 => 104,
            0x4b => 105,
            0x4d => 106,
            0x4f => 107,
            0x50 => 108,
            0x51 => 109,
            0x52 => 110,
            0x53 => 111,
            0x5b => 125,
            0x5c => 126,
            0x5d => 127,
            _ => {
                return Err(DesktopError::Unsupported(
                    "unsupported extended Set-1 key".into(),
                ));
            }
        }
    } else if (1..=0x58).contains(&code) {
        code
    } else {
        return Err(DesktopError::Unsupported("unsupported Set-1 key".into()));
    };
    Ok((evdev + 8) as u8)
}

pub fn capture(
    config: DesktopCaptureConfig,
    permissions: LocalNodePermissionStore,
    mailbox: &CaptureMailbox,
    stop: mpsc::Receiver<()>,
    ready: mpsc::SyncSender<DesktopResult<()>>,
    wake: &std::os::unix::net::UnixStream,
) -> DesktopResult<()> {
    let result = Capture::new(&config);
    let mut capture = match result {
        Ok(value) => value,
        Err(error) => {
            let _ = ready.send(Err(error.clone()));
            return Err(error);
        }
    };
    let _ = ready.send(Ok(()));
    let started = Instant::now();
    let cadence = Duration::from_secs_f64(1.0 / f64::from(config.frames_per_second));
    let deadline = started + Duration::from_millis(config.lease_ms.min(300_000));
    let mut sequence = 0;
    let mut dirty = true;
    let mut ceiling = FrameRateCeiling::new(Some(config.frames_per_second));
    loop {
        if Instant::now() >= deadline || stop.try_recv() != Err(mpsc::TryRecvError::Empty) {
            return Ok(());
        }
        authorize(&permissions, &config.target, "capture")?;
        let mut drained = 0;
        while let Some(event) = capture.x.conn.poll_for_event().map_err(platform)? {
            drained += 1;
            match event {
                Event::DamageNotify(_) | Event::ConfigureNotify(_) => dirty = true,
                Event::XfixesCursorNotify(_)
                | Event::XinputRawMotion(_)
                | Event::MotionNotify(_)
                | Event::EnterNotify(_)
                | Event::LeaveNotify(_)
                    if config.include_cursor =>
                {
                    dirty = true
                }
                Event::DestroyNotify(event) => {
                    if event.window == capture.window {
                        return Err(unavailable("captured window was destroyed"));
                    }
                    capture.cursor_windows.remove(&event.window);
                    dirty = true;
                }
                Event::UnmapNotify(event) => {
                    if event.window == capture.window {
                        return Err(unavailable("captured window is no longer visible"));
                    }
                    dirty = true;
                }
                Event::CreateNotify(event)
                    if config.include_cursor && capture.cursor_windows.contains(&event.parent) =>
                {
                    capture.watch_cursor_tree(event.window)?;
                    dirty = true;
                }
                Event::ReparentNotify(event) if config.include_cursor => {
                    if event.window != capture.window {
                        if capture.cursor_windows.contains(&event.parent) {
                            capture.watch_cursor_tree(event.window)?;
                        } else {
                            capture.unwatch_cursor_tree(event.window)?;
                        }
                    }
                    dirty = true;
                }
                _ => {}
            }
            if drained == 256 {
                break;
            }
        }
        let delay = ceiling.delay(Instant::now());
        if dirty && delay.is_zero() {
            capture.x.resolve(&config.target)?;
            damage::subtract(&capture.x.conn, capture.damage, x11rb::NONE, x11rb::NONE)
                .map_err(platform)?
                .check()
                .map_err(platform)?;
            sequence += 1;
            let timestamp = started.elapsed().as_nanos() as u64;
            ceiling.admit(Instant::now());
            let mut sample = capture.frame(config.include_cursor, sequence)?;
            sample.timestamp_ns = timestamp;
            sample.duration_ns = cadence.as_nanos() as u64;
            authorize(&permissions, &config.target, "capture")?;
            if Instant::now() >= deadline || stop.try_recv() != Err(mpsc::TryRecvError::Empty) {
                return Ok(());
            }
            mailbox.publish(sample);
            dirty = false;
            // Replies during capture may have queued Damage/cursor events in
            // x11rb already. Drain them before waiting on the socket again.
            continue;
        }
        if drained == 256 {
            continue;
        }
        let timeout = deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(100));
        let timeout = if dirty { timeout.min(delay) } else { timeout };
        if !wait_for_x_event(&capture.x.conn, wake, timeout)? {
            return Ok(());
        }
    }
}

fn wait_for_x_event(
    connection: &RustConnection,
    wake: &std::os::unix::net::UnixStream,
    timeout: Duration,
) -> DesktopResult<bool> {
    let mut fds = [
        libc::pollfd {
            fd: connection.stream().as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: wake.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        },
    ];
    // Readable X events and cancellation both wake immediately. The timeout
    // only bounds authority/lease checks; it never paces available frames.
    let millis = timeout.as_nanos().div_ceil(1_000_000).min(i32::MAX as u128) as i32;
    let result = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, millis) };
    if result < 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            return Err(platform(error));
        }
    }
    if fds[1].revents != 0 {
        return Ok(false);
    }
    if fds[0].revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
        return Err(unavailable("X11 capture connection closed"));
    }
    Ok(true)
}

struct Capture {
    x: X11,
    window: Window,
    damage: u32,
    redirected: bool,
    resolution: Option<CaptureResolution>,
    readback: readback::Readback,
    cursor_windows: std::collections::BTreeSet<Window>,
}
impl Capture {
    fn new(config: &DesktopCaptureConfig) -> DesktopResult<Self> {
        let x = X11::open()?;
        let window = x.resolve(&config.target)?;
        composite::query_version(&x.conn, 0, 4)
            .map_err(platform)?
            .reply()
            .map_err(platform)?;
        damage::query_version(&x.conn, 1, 1)
            .map_err(platform)?
            .reply()
            .map_err(platform)?;
        xfixes::query_version(&x.conn, 5, 0)
            .map_err(platform)?
            .reply()
            .map_err(platform)?;
        let damage = x.conn.generate_id().map_err(platform)?;
        let readback = readback::Readback::new(&x.conn);
        let mut capture = Self {
            x,
            window,
            damage,
            redirected: false,
            resolution: config.resolution,
            readback,
            cursor_windows: std::collections::BTreeSet::new(),
        };
        if config.target.is_window() {
            composite::redirect_window(&capture.x.conn, window, composite::Redirect::AUTOMATIC)
                .map_err(platform)?
                .check()
                .map_err(platform)?;
            capture.redirected = true;
        }
        capture
            .x
            .conn
            .change_window_attributes(
                window,
                &ChangeWindowAttributesAux::new().event_mask(EventMask::STRUCTURE_NOTIFY),
            )
            .map_err(platform)?
            .check()
            .map_err(platform)?;
        damage::create(
            &capture.x.conn,
            damage,
            window,
            damage::ReportLevel::NON_EMPTY,
        )
        .map_err(platform)?
        .check()
        .map_err(platform)?;
        if config.include_cursor {
            capture.watch_cursor_tree(window)?;
            // XFixes reports shape changes; XI2 raw motion covers pointer moves
            // over child windows even when another client consumes core events.
            xinput::xi_query_version(&capture.x.conn, 2, 0)
                .map_err(platform)?
                .reply()
                .map_err(|e| {
                    DesktopError::Unsupported(format!("cursor notifications require XInput2: {e}"))
                })?;
            xinput::xi_select_events(
                &capture.x.conn,
                capture.x.root(),
                &[xinput::EventMask {
                    deviceid: xinput::Device::ALL_MASTER.into(),
                    mask: vec![xinput::XIEventMask::RAW_MOTION],
                }],
            )
            .map_err(platform)?
            .check()
            .map_err(platform)?;
            xfixes::select_cursor_input(
                &capture.x.conn,
                capture.x.root(),
                xfixes::CursorNotifyMask::DISPLAY_CURSOR,
            )
            .map_err(platform)?
            .check()
            .map_err(platform)?;
        }
        // Fail opening instead of claiming capture support before a first image.
        capture.frame(config.include_cursor, 0)?;
        Ok(capture)
    }
    fn watch_cursor_tree(&mut self, window: Window) -> DesktopResult<()> {
        let mut pending = vec![window];
        while let Some(window) = pending.pop() {
            if self.cursor_windows.contains(&window) {
                continue;
            }
            if self.cursor_windows.len() >= 16_384 {
                return Err(DesktopError::Unsupported(
                    "X11 cursor notification tree exceeds 16384 windows".into(),
                ));
            }
            // Core events cover warps, which do not generate XI2 RawMotion.
            // Watch descendants too: another client's selection on a child can
            // stop propagation to our target. Masks affect this connection only.
            let mask = EventMask::STRUCTURE_NOTIFY
                | EventMask::SUBSTRUCTURE_NOTIFY
                | EventMask::POINTER_MOTION
                | EventMask::ENTER_WINDOW
                | EventMask::LEAVE_WINDOW;
            match self
                .x
                .conn
                .change_window_attributes(
                    window,
                    &ChangeWindowAttributesAux::new().event_mask(mask),
                )
                .map_err(platform)?
                .check()
            {
                Ok(()) => {}
                Err(error) if vanished_window(&error) && window != self.window => continue,
                Err(error) => return Err(unavailable(error)),
            }
            self.cursor_windows.insert(window);
            match self.x.conn.query_tree(window).map_err(platform)?.reply() {
                Ok(tree) => {
                    if pending.len() + tree.children.len() + self.cursor_windows.len() > 16_384 {
                        return Err(DesktopError::Unsupported(
                            "X11 cursor notification tree exceeds 16384 windows".into(),
                        ));
                    }
                    pending.extend(tree.children);
                }
                Err(error) if vanished_window(&error) && window != self.window => {
                    self.cursor_windows.remove(&window);
                }
                Err(error) => return Err(unavailable(error)),
            }
        }
        Ok(())
    }
    fn unwatch_cursor_tree(&mut self, window: Window) -> DesktopResult<()> {
        let mut pending = vec![window];
        while let Some(window) = pending.pop() {
            if !self.cursor_windows.remove(&window) {
                continue;
            }
            match self.x.conn.query_tree(window).map_err(platform)?.reply() {
                Ok(tree) => pending.extend(
                    tree.children
                        .into_iter()
                        .filter(|child| self.cursor_windows.contains(child)),
                ),
                Err(error) if vanished_window(&error) => {}
                Err(error) => return Err(unavailable(error)),
            }
            match self
                .x
                .conn
                .change_window_attributes(
                    window,
                    &ChangeWindowAttributesAux::new().event_mask(EventMask::NO_EVENT),
                )
                .map_err(platform)?
                .check()
            {
                Ok(()) => {}
                Err(error) if vanished_window(&error) => {}
                Err(error) => return Err(unavailable(error)),
            }
        }
        Ok(())
    }
    fn frame(&mut self, cursor: bool, sequence: u64) -> DesktopResult<NativeVideoSample> {
        let start = Instant::now();
        let captured_at_unix_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let attr = self
            .x
            .conn
            .get_window_attributes(self.window)
            .map_err(platform)?
            .reply()
            .map_err(unavailable)?;
        if attr.map_state != MapState::VIEWABLE {
            return Err(unavailable("window is not mapped"));
        }
        let geo = self
            .x
            .conn
            .get_geometry(self.window)
            .map_err(platform)?
            .reply()
            .map_err(unavailable)?;
        let width = u32::from(geo.width);
        let height = u32::from(geo.height);
        if width == 0
            || height == 0
            || u64::from(width) * u64::from(height) * 4 > MAX_FRAME_BYTES as u64
        {
            return Err(DesktopError::Invalid(
                "capture dimensions exceed frame limit".into(),
            ));
        }
        let pixmap = if self.redirected {
            let id = self.x.conn.generate_id().map_err(platform)?;
            composite::name_window_pixmap(&self.x.conn, self.window, id)
                .map_err(platform)?
                .check()
                .map_err(unavailable)?;
            Some(id)
        } else {
            None
        };
        let border = if pixmap.is_some() {
            i16::try_from(geo.border_width).map_err(platform)?
        } else {
            0
        };
        let geometry_us = start.elapsed().as_micros();
        let readback = Instant::now();
        let image = self
            .readback
            .image(&self.x.conn, pixmap.unwrap_or(self.window), border, &geo);
        if let Some(id) = pixmap {
            let _ = self.x.conn.free_pixmap(id);
        }
        let (depth, storage) = image?;
        let readback_backend = if matches!(storage, readback::Pixels::Shared(_)) {
            "mit-shm"
        } else {
            "get-image"
        };
        let readback_us = readback.elapsed().as_micros();
        let normalize = Instant::now();
        let setup = self.x.conn.setup();
        let format = setup
            .pixmap_formats
            .iter()
            .find(|f| f.depth == depth)
            .ok_or_else(|| platform("unknown X image depth"))?;
        let visual = setup
            .roots
            .iter()
            .flat_map(|s| &s.allowed_depths)
            .flat_map(|d| &d.visuals)
            .find(|v| v.visual_id == attr.visual)
            .ok_or_else(|| platform("unknown X visual"))?;
        let masks = [visual.red_mask, visual.green_mask, visual.blue_mask];
        let little = setup.image_byte_order == ImageOrder::LSB_FIRST;
        let mut pixels = match storage {
            readback::Pixels::Shared(data)
                if format.bits_per_pixel == 32 && little && masks == [0xff0000, 0xff00, 0xff] =>
            {
                if data.len() != width as usize * height as usize * 4 {
                    return Err(platform("invalid shared XRGB image length"));
                }
                for pixel in data.chunks_exact_mut(4) {
                    pixel[3] = 255;
                }
                readback::Pixels::Shared(data)
            }
            readback::Pixels::Shared(data) => readback::Pixels::Owned(decode_pixels(
                data,
                width,
                height,
                format.bits_per_pixel,
                format.scanline_pad,
                little,
                masks,
            )?),
            readback::Pixels::Owned(data) => readback::Pixels::Owned(decode_pixels_owned(
                data,
                width,
                height,
                format.bits_per_pixel,
                format.scanline_pad,
                little,
                masks,
            )?),
        };
        let normalize_us = normalize.elapsed().as_micros();
        let cursor_started = Instant::now();
        if cursor {
            let cursor = xfixes::get_cursor_image(&self.x.conn)
                .map_err(platform)?
                .reply()
                .map_err(platform)?;
            let bounds = self.x.bounds(self.window)?;
            for cy in 0..u32::from(cursor.height) {
                for cx in 0..u32::from(cursor.width) {
                    let x = i32::from(cursor.x) - i32::from(cursor.xhot) - bounds[0] + cx as i32;
                    let y = i32::from(cursor.y) - i32::from(cursor.yhot) - bounds[1] + cy as i32;
                    if x < 0 || y < 0 || x >= width as i32 || y >= height as i32 {
                        continue;
                    }
                    let argb = cursor.cursor_image[(cy * u32::from(cursor.width) + cx) as usize];
                    let alpha = argb >> 24;
                    let offset = ((y as u32 * width + x as u32) * 4) as usize;
                    for channel in 0..3 {
                        let source = (argb >> (channel * 8)) & 255;
                        pixels.as_mut()[offset + channel] = (source
                            + (u32::from(pixels.as_ref()[offset + channel]) * (255 - alpha) + 127)
                                / 255)
                            .min(255)
                            as u8;
                    }
                }
            }
        }
        let cursor_us = cursor_started.elapsed().as_micros();
        let resize = Instant::now();
        let [output_width, output_height] = self
            .resolution
            .map(|bounds| bounds.fit(width, height, 1))
            .transpose()?
            .unwrap_or([width, height]);
        let scaled = [output_width, output_height] != [width, height];
        let data: Arc<[u8]> = if scaled {
            // Resample directly from the X reply buffer into final shared output;
            // no full-resolution Arc and no temporary resized Vec are needed.
            resize_capture_pixels(
                pixels.as_ref(),
                width,
                height,
                width as usize * 4,
                output_width,
                output_height,
            )?
        } else {
            pixels.into_arc()
        };
        let resize_us = resize.elapsed().as_micros();
        if super::frame_trace_enabled() {
            super::trace_frame(serde_json::json!({"kind":"x11","sequence":sequence,
            "captured_at_unix_ms":captured_at_unix_ms,"geometry_us":geometry_us,"readback_us":readback_us,
            "readback_backend":readback_backend,"normalize_us":normalize_us,"cursor_us":cursor_us,"resize_us":resize_us,"total_us":start.elapsed().as_micros(),
            "source_width":width,"source_height":height,"width":output_width,"height":output_height}));
        }
        Ok(NativeVideoSample {
            native_dimensions: scaled.then_some([width, height]),
            sequence,
            format: NativeVideoSampleFormat::Bgra8,
            width: output_width,
            height: output_height,
            timestamp_ns: 0,
            duration_ns: 0,
            captured_at_unix_ms,
            planes: Arc::from([NativeVideoPlane {
                offset: 0,
                len: data.len(),
                width: output_width,
                height: output_height,
                stride: output_width * 4,
            }]),
            data,
        })
    }
}
impl Drop for Capture {
    fn drop(&mut self) {
        self.readback.detach(&self.x.conn);
        let _ = damage::destroy(&self.x.conn, self.damage);
        if self.redirected {
            let _ = composite::unredirect_window(
                &self.x.conn,
                self.window,
                composite::Redirect::AUTOMATIC,
            );
        }
        let _ = self.x.conn.flush();
    }
}
fn vanished_window(error: &x11rb::errors::ReplyError) -> bool {
    matches!(error,x11rb::errors::ReplyError::X11Error(e) if e.error_kind==x11rb::protocol::ErrorKind::Window)
}
fn decode_pixels_owned(
    mut data: Vec<u8>,
    width: u32,
    height: u32,
    bpp: u8,
    pad: u8,
    little: bool,
    masks: [u32; 3],
) -> DesktopResult<Vec<u8>> {
    if bpp == 32 && [8, 16, 32].contains(&pad) && little && masks == [0xff0000, 0xff00, 0xff] {
        let len = u64::from(width) * u64::from(height) * 4;
        if len > MAX_FRAME_BYTES as u64 || data.len() != len as usize {
            return Err(platform("invalid X11 image length"));
        }
        // XRGB's unused byte is not alpha. Normalize it while reusing the
        // connection's reply allocation rather than decoding/copying every channel.
        for pixel in data.chunks_exact_mut(4) {
            pixel[3] = 255;
        }
        return Ok(data);
    }
    decode_pixels(&data, width, height, bpp, pad, little, masks)
}
fn decode_pixels(
    data: &[u8],
    width: u32,
    height: u32,
    bpp: u8,
    pad: u8,
    little: bool,
    masks: [u32; 3],
) -> DesktopResult<Vec<u8>> {
    if ![16, 24, 32].contains(&bpp) || ![8, 16, 32].contains(&pad) || masks.contains(&0) {
        return Err(DesktopError::Unsupported(
            "X11 capture requires a TrueColor 16/24/32-bit visual".into(),
        ));
    }
    let stride =
        (width as usize * usize::from(bpp)).div_ceil(usize::from(pad)) * usize::from(pad) / 8;
    let len = u64::from(width) * u64::from(height) * 4;
    if len > MAX_FRAME_BYTES as u64 || data.len() != stride * height as usize {
        return Err(platform("invalid X11 image length"));
    }
    let mut result = vec![0; len as usize];
    for y in 0..height as usize {
        for x in 0..width as usize {
            let start = y * stride + x * usize::from(bpp / 8);
            let source = &data[start..start + usize::from(bpp / 8)];
            let mut pixel = 0u32;
            for (i, byte) in source.iter().enumerate() {
                pixel |= u32::from(*byte) << (if little { i } else { source.len() - 1 - i } * 8);
            }
            let offset = (y * width as usize + x) * 4;
            for (channel, mask) in masks.iter().rev().enumerate() {
                let max = mask >> mask.trailing_zeros();
                result[offset + channel] = ((u64::from((pixel & mask) >> mask.trailing_zeros())
                    * 255
                    + u64::from(max) / 2)
                    / u64::from(max)) as u8;
            }
            result[offset + 3] = 255;
        }
    }
    Ok(result)
}

#[cfg(test)]
#[path = "../linux_tests.rs"]
mod tests;
