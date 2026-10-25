use super::*;
use futures_util::StreamExt;
use serde::Serialize;
use std::collections::HashMap;
use zbus::zvariant::{DynamicType, OwnedObjectPath, OwnedValue, Value};
use zbus::{Connection, Proxy};

const DEST: &str = "org.freedesktop.portal.Desktop";
const ROOT: &str = "/org/freedesktop/portal/desktop";
const REMOTE: &str = "org.freedesktop.portal.RemoteDesktop";
const SCREEN: &str = "org.freedesktop.portal.ScreenCast";
type Dict = HashMap<String, OwnedValue>;

pub(super) fn runtime() -> DesktopResult<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(platform)
}
async fn connect() -> DesktopResult<Connection> {
    tokio::time::timeout(Duration::from_secs(3), async {
        zbus::connection::Builder::session()?
            .method_timeout(Duration::from_secs(2))
            .build()
            .await
    })
    .await
    .map_err(platform)?
    .map_err(|e| {
        unsupported(&format!(
            "Wayland session bus/desktop portal unavailable: {e}"
        ))
    })
}
pub(super) async fn capabilities() -> DesktopResult<(bool, bool)> {
    let conn = connect().await?;
    let rd = Proxy::new(&conn, DEST, ROOT, REMOTE)
        .await
        .map_err(platform)?;
    let sc = Proxy::new(&conn, DEST, ROOT, SCREEN)
        .await
        .map_err(platform)?;
    let devices: u32 = rd.get_property("AvailableDeviceTypes").await.unwrap_or(0);
    let sources: u32 = sc
        .get_property("AvailableSourceTypes")
        .await
        .map_err(platform)?;
    Ok((sources & 1 != 0, devices & 3 != 0))
}
fn token() -> String {
    format!("sinomo{}", sinomo_core::Id::generate())
}
fn sender(conn: &Connection) -> DesktopResult<String> {
    conn.unique_name()
        .map(|n| n.as_str().trim_start_matches(':').replace('.', "_"))
        .ok_or_else(|| platform("portal connection has no unique bus name"))
}
async fn request<B: Serialize + DynamicType>(
    proxy: &Proxy<'_>,
    method: &str,
    body: &B,
    token: &str,
    shared: &Shared,
) -> DesktopResult<Dict> {
    let path = format!("{ROOT}/request/{}/{token}", sender(proxy.connection())?);
    let request = Proxy::new(
        proxy.connection(),
        proxy.destination().as_str(),
        path.as_str(),
        "org.freedesktop.portal.Request",
    )
    .await
    .map_err(platform)?;
    // Subscribe before calling: fast denial/success must not race registration.
    let mut responses = request.receive_signal("Response").await.map_err(platform)?;
    let handle: OwnedObjectPath = proxy.call(method, body).await.map_err(platform)?;
    if handle.as_str() != path {
        if let Ok(other) = Proxy::new(
            proxy.connection(),
            proxy.destination().as_str(),
            handle.as_str(),
            "org.freedesktop.portal.Request",
        )
        .await
        {
            let _ = other.call::<_, _, ()>("Close", &()).await;
        }
        return Err(platform("portal returned an unexpected request identity"));
    }
    let outcome = loop {
        tokio::select! {
            response = responses.next() => {
                if Instant::now() >= shared.preparing_until { break Err(DesktopError::Closed("Wayland chooser expired".into())); }
                let Some(response) = response else { break Err(platform("portal request channel closed")); };
                let (code, values): (u32, Dict) = response.body().deserialize().map_err(platform)?;
                break match code {
                    0 => Ok(values),
                    1 => Err(DesktopError::PermissionDenied("local Wayland portal selection was cancelled".into())),
                    _ => Err(DesktopError::PermissionDenied("local Wayland portal rejected the request".into())),
                };
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                if shared.stopped.load(Ordering::Acquire) || Instant::now() >= shared.preparing_until {
                    break Err(DesktopError::Closed("Wayland chooser cancelled or expired".into()));
                }
                if let Err(error) = authorize_host(&shared.permissions) { break Err(error); }
            }
        }
    };
    if outcome.is_err() {
        let _ = request.call::<_, _, ()>("Close", &()).await;
    }
    outcome
}

pub(super) fn parse_target(
    mut values: Dict,
    generation: &str,
    screen_version: u32,
) -> DesktopResult<Target> {
    let devices = values
        .remove("devices")
        .and_then(|v| u32::try_from(v).ok())
        .unwrap_or(0)
        & 3;
    let mut streams: Vec<(u32, Dict)> = values
        .remove("streams")
        .ok_or_else(|| unsupported("portal granted no capture stream"))?
        .try_into()
        .map_err(platform)?;
    if streams.len() != 1 {
        return Err(unsupported(
            "Wayland raw mode requires exactly one approved monitor stream",
        ));
    }
    let (node, mut properties) = streams.remove(0);
    if node == 0 || node == u32::MAX {
        return Err(platform(
            "portal returned an invalid PipeWire node identity",
        ));
    }
    if properties
        .remove("source_type")
        .is_some_and(|v| u32::try_from(v).ok() != Some(1))
    {
        return Err(unsupported(
            "portal did not grant a monitor; app-only capture cannot authorize raw display input",
        ));
    }
    let (width, height): (i32, i32) = properties
        .remove("logical_size")
        .or_else(|| properties.remove("size"))
        .ok_or_else(|| {
            unsupported(
                "portal omitted stream coordinate bounds; safe pointer mapping is unavailable",
            )
        })?
        .try_into()
        .map_err(platform)?;
    if width <= 0
        || height <= 0
        || u64::from(width as u32) * u64::from(height as u32) > (MAX_FRAME_BYTES / 4) as u64
    {
        return Err(platform("invalid portal monitor dimensions"));
    }
    let serial = properties
        .remove("pipewire-serial")
        .map(u64::try_from)
        .transpose()
        .map_err(platform)?;
    if serial == Some(0) {
        return Err(platform("portal returned an invalid PipeWire serial"));
    }
    if screen_version >= 6 && serial.is_none() {
        return Err(unsupported(
            "ScreenCast v6 omitted the stable PipeWire stream serial",
        ));
    }
    let target = DesktopTarget::Monitor {
        device_name: format!("wayland:{generation}:{node}"),
    };
    Ok(Target {
        node,
        serial,
        devices,
        info: DesktopTargetInfo {
            resource_id: target.resource_id(),
            target,
            title: "Wayland portal monitor".into(),
            bounds: [0, 0, width, height],
            client_bounds: None,
            minimized: false,
        },
    })
}

pub(super) async fn run(
    shared: &Arc<Shared>,
    mut commands: tokio::sync::mpsc::Receiver<Command>,
) -> DesktopResult<()> {
    let conn = connect().await?;
    let discovery = Proxy::new(&conn, DEST, ROOT, REMOTE)
        .await
        .map_err(platform)?;
    let bus = zbus::fdo::DBusProxy::new(&conn).await.map_err(platform)?;
    if !bus
        .name_has_owner(DEST.try_into().map_err(platform)?)
        .await
        .map_err(platform)?
    {
        bus.start_service_by_name(DEST.try_into().map_err(platform)?, 0)
            .await
            .map_err(platform)?;
    }
    // Initial D-Bus activation is not a revocation. Subscribe after it finishes,
    // then resolve the owner so every later replacement is observed.
    let mut owner_changes = discovery.receive_owner_changed().await.map_err(platform)?;
    let unique = bus
        .get_name_owner(DEST.try_into().map_err(platform)?)
        .await
        .map_err(platform)?;
    // Pin the granting service owner. A portal restart cannot inherit a target.
    let rd = Proxy::new(&conn, unique.as_str(), ROOT, REMOTE)
        .await
        .map_err(platform)?;
    let sc = Proxy::new(&conn, unique.as_str(), ROOT, SCREEN)
        .await
        .map_err(platform)?;
    let sources: u32 = sc
        .get_property("AvailableSourceTypes")
        .await
        .map_err(platform)?;
    let devices: u32 = rd.get_property("AvailableDeviceTypes").await.unwrap_or(0);
    let screen_version: u32 = sc.get_property("version").await.map_err(platform)?;
    if sources & 1 == 0 {
        return Err(unsupported("portal does not support monitor capture"));
    }
    let session_token = token();
    let create_token = token();
    let create_options = HashMap::from([
        ("handle_token", Value::from(create_token.as_str())),
        ("session_handle_token", Value::from(session_token.as_str())),
    ]);
    let controlling = devices & 3 != 0;
    let session_portal = if controlling { &rd } else { &sc };
    let created = request(
        session_portal,
        "CreateSession",
        &(create_options,),
        &create_token,
        shared,
    )
    .await?;
    let session: String = created
        .get("session_handle")
        .ok_or_else(|| platform("portal omitted session handle"))?
        .try_clone()
        .map_err(platform)?
        .try_into()
        .map_err(platform)?;
    let expected = format!("{ROOT}/session/{}/{session_token}", sender(&conn)?);
    if session != expected {
        return Err(platform("portal returned an unexpected session identity"));
    }
    let path = OwnedObjectPath::try_from(session).map_err(platform)?;
    let session = Proxy::new(
        &conn,
        unique.as_str(),
        path.as_str(),
        "org.freedesktop.portal.Session",
    )
    .await
    .map_err(platform)?;
    let mut closed = session.receive_signal("Closed").await.map_err(platform)?;
    let work = async {
        let select_token = token();
        let options = HashMap::from([
            ("handle_token", Value::from(select_token.as_str())),
            ("types", Value::from(devices & 3)),
        ]);
        if controlling {
            request(
                &rd,
                "SelectDevices",
                &(&path, options),
                &select_token,
                shared,
            )
            .await?;
        }
        let sources_token = token();
        let options = HashMap::from([
            ("handle_token", Value::from(sources_token.as_str())),
            ("types", Value::from(1u32)),
            ("multiple", Value::from(false)),
        ]); // Default hidden cursor; never persist or restore portal consent.
        request(
            &sc,
            "SelectSources",
            &(&path, options),
            &sources_token,
            shared,
        )
        .await?;
        let start_token = token();
        let options = HashMap::from([("handle_token", Value::from(start_token.as_str()))]);
        let result = request(
            session_portal,
            "Start",
            &(&path, "", options),
            &start_token,
            shared,
        )
        .await?;
        let target = parse_target(result, &session_token, screen_version)?;
        authorize_host(&shared.permissions)?;
        *shared.status.lock().unwrap_or_else(|e| e.into_inner()) = Ok(Some(target.clone()));
        let prepared = Instant::now();
        loop {
            tokio::select! {
                _ = closed.next() => return Err(DesktopError::Closed("Wayland sharing was stopped by the portal".into())),
                _ = owner_changes.next() => return Err(DesktopError::Closed("Wayland portal owner changed".into())),
                command = commands.recv() => match command {
                    Some(Command::Input(event, reply)) => {
                        if shared.stopped.load(Ordering::Acquire) { return Ok(()); }
                        let result = input(&rd, &path, &target, shared, &event).await;
                        let uncertain = matches!(result, Err(DesktopError::Platform(_)));
                        let _ = reply.send(result);
                        // A failed D-Bus acknowledgement may follow an applied
                        // key-down. Retire the OS session, including its held
                        // device state, instead of retaining it for capture.
                        if uncertain { return Err(DesktopError::Closed("portal input failed; session retired, do not replay the operation".into())); }
                    }
                    Some(Command::Remote(reply)) => {
                        let result = async {
                            authorize(&shared.permissions, &target.info.target, "capture")?;
                            let fd: zbus::zvariant::OwnedFd = sc.call("OpenPipeWireRemote", &(&path, HashMap::<String, Value>::new())).await.map_err(platform)?;
                            Ok(fd.into())
                        }.await;
                        let _ = reply.send(result);
                    }
                    None => return Ok(()),
                },
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    authorize_host(&shared.permissions)?;
                    if shared.stopped.load(Ordering::Acquire)
                        || (shared.users.load(Ordering::Acquire) == 0 && prepared.elapsed() >= Duration::from_secs(120)) { return Ok(()); }
                }
            }
        }
    };
    let result = work.await;
    shared.stop();
    let _ = session.call::<_, _, ()>("Close", &()).await;
    result
}

async fn input(
    rd: &Proxy<'_>,
    path: &OwnedObjectPath,
    target: &Target,
    shared: &Shared,
    event: &DesktopInput,
) -> DesktopResult<()> {
    let release = matches!(
        event,
        DesktopInput::Key { down: false, .. } | DesktopInput::Button { down: false, .. }
    );
    if !release {
        authorize(&shared.permissions, &target.info.target, "input")?;
    }
    let keyboard = matches!(event, DesktopInput::Key { .. } | DesktopInput::Text { .. });
    if target.devices & (if keyboard { 1 } else { 2 }) == 0 {
        return Err(unsupported(
            "the selected portal session did not grant this input device",
        ));
    }
    let options = HashMap::<String, Value>::new;
    match event {
        DesktopInput::Move { x, y } => {
            if *x < 0 || *y < 0 || *x >= target.info.bounds[2] || *y >= target.info.bounds[3] {
                return Err(DesktopError::Invalid(
                    "pointer is outside the portal monitor's coordinate bounds".into(),
                ));
            }
            rd.call::<_, _, ()>(
                "NotifyPointerMotionAbsolute",
                &(path, options(), target.node, f64::from(*x), f64::from(*y)),
            )
            .await
            .map_err(platform)?;
        }
        DesktopInput::RelativeMove { dx, dy } => {
            rd.call::<_, _, ()>(
                "NotifyPointerMotion",
                &(path, options(), f64::from(*dx), f64::from(*dy)),
            )
            .await
            .map_err(platform)?;
        }
        DesktopInput::Button { button, down } => {
            let button: i32 = match button {
                MouseButton::Left => 272,
                MouseButton::Right => 273,
                MouseButton::Middle => 274,
                MouseButton::X1 => 275,
                MouseButton::X2 => 276,
            };
            rd.call::<_, _, ()>(
                "NotifyPointerButton",
                &(path, options(), button, u32::from(*down)),
            )
            .await
            .map_err(platform)?;
        }
        DesktopInput::Wheel { delta, horizontal } => {
            if delta % 120 != 0 {
                return Err(unsupported(
                    "Wayland discrete wheel deltas must be multiples of 120",
                ));
            }
            rd.call::<_, _, ()>(
                "NotifyPointerAxisDiscrete",
                &(
                    path,
                    options(),
                    u32::from(*horizontal),
                    -i32::from(*delta) / 120,
                ),
            )
            .await
            .map_err(platform)?;
        }
        DesktopInput::Key {
            code,
            extended,
            down,
        } => {
            let code = i32::from(super::super::x11::scan_keycode(*code, *extended)?) - 8;
            rd.call::<_, _, ()>(
                "NotifyKeyboardKeycode",
                &(path, options(), code, u32::from(*down)),
            )
            .await
            .map_err(platform)?;
        }
        DesktopInput::Text { text } => {
            for ch in text.chars() {
                authorize(&shared.permissions, &target.info.target, "input")?;
                if shared.stopped.load(Ordering::Acquire) {
                    return Err(DesktopError::Closed("portal input cancelled".into()));
                }
                let sym = keysym(ch);
                rd.call::<_, _, ()>("NotifyKeyboardKeysym", &(path, options(), sym, 1u32))
                    .await
                    .map_err(platform)?;
                // Always pair a key-down even if consent was revoked meanwhile.
                rd.call::<_, _, ()>("NotifyKeyboardKeysym", &(path, options(), sym, 0u32))
                    .await
                    .map_err(platform)?;
            }
        }
        DesktopInput::ReleaseAll => {}
        _ => {
            return Err(unsupported(
                "Wayland raw input has no background frame coordinates or gamepad endpoint",
            ));
        }
    }
    Ok(())
}
fn keysym(ch: char) -> i32 {
    match ch {
        '\n' | '\r' => 0xff0d,
        '\t' => 0xff09,
        '\u{8}' => 0xff08,
        c if u32::from(c) <= 255 => c as i32,
        c => (0x0100_0000 | u32::from(c)) as i32,
    }
}
