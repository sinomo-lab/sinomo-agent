use super::*;
use pipewire::spa::param::video::VideoFormat;
#[path = "tests/portal_fixture.rs"]
mod fixture;

#[test]
fn pipewire_pixels_respect_chunk_offset_padding_and_format() {
    assert!(!video::valid_dimensions(u32::MAX, u32::MAX));
    assert!(!video::valid_dimensions(0, 1));
    assert!(video::valid_dimensions(1920, 1080));
    let bytes = [99, 99, 1, 2, 3, 4, 88, 88, 5, 6, 7, 8];
    assert_eq!(
        video::copy_pixels(&bytes, 2, 10, 6, 1, 2, VideoFormat::RGBA).unwrap(),
        [3, 2, 1, 255, 7, 6, 5, 255]
    );
    assert_eq!(
        video::copy_pixels(&bytes[2..6], 0, 4, 4, 1, 1, VideoFormat::BGRx).unwrap(),
        [1, 2, 3, 255]
    );
    assert_eq!(
        video::copy_pixels(&[99, 1, 2, 3, 99, 4, 5, 6], 1, 7, 4, 1, 2, VideoFormat::RGB).unwrap(),
        [3, 2, 1, 255, 6, 5, 4, 255]
    );
    for (offset, size, stride, width, height) in [
        (2, 9, 6, 1, 2),
        (2, 11, 6, 1, 2),
        (0, 12, -4, 1, 2),
        (0, 12, 2, 1, 2),
        (0, 12, 4, 0, 2),
        (0, 12, 4, u32::MAX, u32::MAX),
    ] {
        assert!(
            video::copy_pixels(
                &bytes,
                offset,
                size,
                stride,
                width,
                height,
                VideoFormat::RGBA
            )
            .is_err()
        );
    }
    assert!(video::copy_pixels(&bytes, 0, 4, 4, 1, 1, VideoFormat::I420).is_err());
}

async fn wait_target(store: &LocalNodePermissionStore) -> DesktopTargetInfo {
    let end = Instant::now() + Duration::from_secs(5);
    loop {
        let found = targets(store, false).unwrap();
        if let Some(info) = found.into_iter().next() {
            return info;
        }
        assert!(
            Instant::now() < end,
            "portal never published a ready target"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a private dbus-run-session; never run against the user's desktop portal"]
async fn wayland_portal_consent_input_revocation_and_shutdown() {
    assert_eq!(
        std::env::var("SINOMO_TEST_PRIVATE_PORTAL_BUS").as_deref(),
        Ok("1")
    );
    let fixture = Arc::new(fixture::Fixture::default());
    fixture.devices.store(3, Ordering::Release);
    fixture.node.store(42, Ordering::Release);
    fixture.serial.store(1234, Ordering::Release);
    let connection = zbus::connection::Builder::session()
        .unwrap()
        .name("org.freedesktop.portal.Desktop")
        .unwrap()
        .serve_at(
            "/org/freedesktop/portal/desktop",
            fixture::Remote(fixture.clone()),
        )
        .unwrap()
        .serve_at(
            "/org/freedesktop/portal/desktop",
            fixture::Screen(fixture.clone()),
        )
        .unwrap()
        .build()
        .await
        .unwrap();
    let root = std::env::temp_dir().join(format!("sinomo-wayland-{}", sinomo_core::Id::generate()));
    let store = LocalNodePermissionStore::new(&root);
    assert!(matches!(
        targets(&store, false),
        Err(DesktopError::PermissionDenied(_))
    ));
    assert_eq!(fixture.starts.load(Ordering::Acquire), 0);
    store
        .mutate(|p| Ok(p.set_remote_management_enabled(true)))
        .unwrap();
    let caps = capabilities();
    assert!(caps.capture && caps.keyboard_mouse && !caps.background_window_input);
    assert!(matches!(
        targets(&store, true),
        Err(DesktopError::Unsupported(_))
    ));
    let started = Instant::now();
    assert!(targets(&store, false).unwrap().is_empty());
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "chooser must not block enumeration"
    );
    let target = wait_target(&store).await;
    assert_eq!(target.bounds, [0, 0, 640, 480]);
    assert!(matches!(
        DesktopInputSession::open_with_lease(
            target.target.clone(),
            store.clone(),
            Duration::from_secs(10),
            Arc::new(AtomicBool::new(false))
        ),
        Err(DesktopError::PermissionDenied(_))
    ));
    store
        .mutate(|p| p.expose_resource_operation(&target.resource_id, "input"))
        .unwrap();
    let foreign = LocalNodePermissionStore::new(root.join("foreign"));
    assert!(matches!(
        Lease::acquire(&target.target, &foreign, true),
        Err(DesktopError::PermissionDenied(_))
    ));
    let capture_hold = Lease::acquire(&target.target, &store, false).unwrap();
    let mut input = DesktopInputSession::open_with_lease(
        target.target.clone(),
        store.clone(),
        Duration::from_secs(10),
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();
    input
        .send(&[
            DesktopInput::Move { x: 320, y: 240 },
            DesktopInput::Key {
                code: 0x1e,
                extended: false,
                down: true,
            },
            DesktopInput::Button {
                button: MouseButton::Left,
                down: true,
            },
            DesktopInput::Text {
                text: "a🦀".into()
            },
            DesktopInput::Wheel {
                delta: 120,
                horizontal: false,
            },
        ])
        .unwrap();
    let observed = fixture.inputs.lock().unwrap().clone();
    assert!(observed.contains(&"absolute:42:320:240".into()));
    assert!(observed.contains(&"key:30:1".into()));
    assert!(observed.contains(&"button:272:1".into()));
    assert!(observed.contains(&format!("symbol:{}:1", 0x0101f980)));
    assert!(observed.contains(&"wheel:0:-1".into()));
    store
        .mutate(|p| p.hide_resource_operation(&target.resource_id, "input"))
        .unwrap();
    assert!(matches!(
        input.send(&[DesktopInput::Move { x: 10, y: 10 }]),
        Err(DesktopError::PermissionDenied(_))
    ));
    drop(input);
    let observed = fixture.inputs.lock().unwrap().clone();
    assert!(observed.contains(&"key:30:0".into()));
    assert!(observed.contains(&"button:272:0".into()));
    assert!(
        validate_target(&target.target).is_ok(),
        "capture lease must retain the shared session"
    );
    drop(capture_hold);
    shutdown();
    assert!(fixture.closes.load(Ordering::Acquire) > 0);
    assert!(validate_target(&target.target).is_err());

    // Capture-only portal backends do not pretend to grant input.
    fixture.devices.store(0, Ordering::Release);
    let capture_only = wait_target(&store).await;
    assert!(matches!(
        Input::new(&capture_only.target, &store),
        Err(DesktopError::Unsupported(_))
    ));
    assert!(validate_target(&capture_only.target).is_ok());
    let path = fixture.sessions.lock().unwrap().last().unwrap().clone();
    connection
        .emit_signal(
            None::<&str>,
            path,
            "org.freedesktop.portal.Session",
            "Closed",
            &std::collections::HashMap::<String, zbus::zvariant::OwnedValue>::new(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(validate_target(&capture_only.target).is_err());
    shutdown();

    fixture.devices.store(3, Ordering::Release);
    let owner_target = wait_target(&store).await;
    connection
        .release_name("org.freedesktop.portal.Desktop")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        validate_target(&owner_target.target).is_err(),
        "portal owner loss must retire old targets"
    );
    shutdown();
    connection
        .request_name("org.freedesktop.portal.Desktop")
        .await
        .unwrap();

    let uncertain = wait_target(&store).await;
    store
        .mutate(|p| p.expose_resource_operation(&uncertain.resource_id, "input"))
        .unwrap();
    let capture_hold = Lease::acquire(&uncertain.target, &store, false).unwrap();
    let mut input = DesktopInputSession::open_with_lease(
        uncertain.target.clone(),
        store.clone(),
        Duration::from_secs(10),
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();
    let before = fixture.inputs.lock().unwrap().len();
    fixture.fail_input.store(true, Ordering::Release);
    assert!(matches!(
        input.send(&[DesktopInput::Key {
            code: 0x1e,
            extended: false,
            down: true
        }]),
        Err(DesktopError::Platform(_))
    ));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        validate_target(&uncertain.target).is_err(),
        "uncertain key-down must retire the OS session even with capture retained"
    );
    assert_eq!(
        fixture.inputs.lock().unwrap().len(),
        before + 1,
        "never replay an uncertain input"
    );
    drop(input);
    drop(capture_hold);
    shutdown();
    fixture.fail_input.store(false, Ordering::Release);

    fixture.code.store(1, Ordering::Release);
    assert!(targets(&store, false).unwrap().is_empty());
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(matches!(
        targets(&store, false),
        Err(DesktopError::PermissionDenied(_))
    ));
    shutdown();
    fixture.code.store(u32::MAX, Ordering::Release);
    let starts = fixture.starts.load(Ordering::Acquire);
    assert!(targets(&store, false).unwrap().is_empty());
    let deadline = Instant::now() + Duration::from_secs(3);
    while fixture.starts.load(Ordering::Acquire) == starts {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let started = Instant::now();
    shutdown();
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(
        fixture.requests_closed.load(Ordering::Acquire) > 0,
        "unload must cancel pending chooser requests"
    );
    let starts = fixture.starts.load(Ordering::Acquire);
    assert!(targets(&store, false).unwrap().is_empty());
    let deadline = Instant::now() + Duration::from_secs(3);
    while fixture.starts.load(Ordering::Acquire) == starts {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    store
        .mutate(|p| Ok(p.set_remote_management_enabled(false)))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        owner()
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .shared
            .stopped
            .load(Ordering::Acquire)
    );
    shutdown();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn portal_metadata_rejects_ambiguous_or_recycled_stream_identity() {
    use std::collections::HashMap;
    use zbus::zvariant::{OwnedValue, Value};
    fn v(value: impl Into<Value<'static>>) -> OwnedValue {
        value.into().try_to_owned().unwrap()
    }
    let values = |node, size, source, serial| {
        let mut props: HashMap<String, OwnedValue> =
            HashMap::from([("size".into(), v(size)), ("source_type".into(), v(source))]);
        if let Some(serial) = serial {
            props.insert("pipewire-serial".into(), v(serial));
        }
        HashMap::from([
            ("devices".into(), v(3u32)),
            ("streams".into(), v(vec![(node, props)])),
        ])
    };
    let target = portal::parse_target(
        values(7u32, (800i32, 600i32), 1u32, Some(99u64)),
        "generation",
        6,
    )
    .unwrap();
    assert_eq!(target.info.bounds, [0, 0, 800, 600]);
    assert_eq!(target.serial, Some(99));
    for input in [
        values(7, (800, 600), 1, None),
        values(u32::MAX, (800, 600), 1, Some(99)),
        values(7, (800, 600), 2, Some(99)),
        values(7, (0, 600), 1, Some(99)),
        values(7, (800, 600), 1, Some(0)),
    ] {
        assert!(portal::parse_target(input, "generation", 6).is_err());
    }
    assert!(portal::parse_target(values(7, (800, 600), 1, None), "legacy", 5).is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires private D-Bus plus the native PipeWire video fixture/linker; see the Wayland fixture runner"]
async fn wayland_pipewire_frames_and_revocation() {
    assert_eq!(
        std::env::var("SINOMO_TEST_PRIVATE_PORTAL_BUS").as_deref(),
        Ok("1")
    );
    let fixture = Arc::new(fixture::Fixture::default());
    fixture.devices.store(3, Ordering::Release);
    fixture.node.store(
        std::env::var("SINOMO_TEST_PIPEWIRE_NODE")
            .unwrap()
            .parse()
            .unwrap(),
        Ordering::Release,
    );
    fixture.serial.store(
        std::env::var("SINOMO_TEST_PIPEWIRE_SERIAL")
            .unwrap()
            .parse()
            .unwrap(),
        Ordering::Release,
    );
    let connection = zbus::connection::Builder::session()
        .unwrap()
        .name("org.freedesktop.portal.Desktop")
        .unwrap()
        .serve_at(
            "/org/freedesktop/portal/desktop",
            fixture::Remote(fixture.clone()),
        )
        .unwrap()
        .serve_at(
            "/org/freedesktop/portal/desktop",
            fixture::Screen(fixture.clone()),
        )
        .unwrap()
        .build()
        .await
        .unwrap();
    let root = std::env::temp_dir().join(format!(
        "sinomo-wayland-video-{}",
        sinomo_core::Id::generate()
    ));
    let store = LocalNodePermissionStore::new(&root);
    store
        .mutate(|p| Ok(p.set_remote_management_enabled(true)))
        .unwrap();
    let info = wait_target(&store).await;
    store
        .mutate(|p| p.expose_resource_operation(&info.resource_id, "capture"))
        .unwrap();
    let config = DesktopCaptureConfig {
        target: info.target.clone(),
        frames_per_second: 10,
        include_cursor: false,
        lease_ms: 10000,
    };
    let authority = store.clone();
    let capture = tokio::task::spawn_blocking(move || DesktopCapture::start(config, authority))
        .await
        .unwrap()
        .unwrap();
    let first = capture.next(0, Duration::from_secs(2)).unwrap().unwrap();
    first.validate().unwrap();
    assert_eq!((first.width, first.height), (64, 48));
    assert_eq!(
        info.bounds,
        [0, 0, 640, 480],
        "portal logical geometry is independent of source pixels"
    );
    assert_eq!(
        &first.data[1..4],
        &[0, 170, 255],
        "native BGRx producer pixels must become BGRA unchanged"
    );
    assert!(
        first
            .data
            .as_chunks::<4>()
            .0
            .iter()
            .any(|p| p[..3] != [0, 0, 0]),
        "actual native pixels must reach the mailbox"
    );
    let second = capture
        .next(first.sequence, Duration::from_secs(2))
        .unwrap()
        .unwrap();
    assert!(second.sequence > first.sequence && second.timestamp_ns >= first.timestamp_ns);
    assert_ne!(
        first.data[0], second.data[0],
        "native producer must deliver fresh frames"
    );
    let path = fixture.sessions.lock().unwrap().last().unwrap().clone();
    connection
        .emit_signal(
            None::<&str>,
            path,
            "org.freedesktop.portal.Session",
            "Closed",
            &std::collections::HashMap::<String, zbus::zvariant::OwnedValue>::new(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        matches!(
            capture.next(0, Duration::ZERO),
            Err(DesktopError::TargetUnavailable(_)) | Err(DesktopError::Closed(_))
        ),
        "cached pixels must not survive portal revocation"
    );
    store
        .mutate(|p| p.hide_resource_operation(&info.resource_id, "capture"))
        .unwrap();
    assert!(matches!(
        capture.next(0, Duration::ZERO),
        Err(DesktopError::PermissionDenied(_))
    ));
    drop(capture);
    shutdown();
    assert!(fixture.closes.load(Ordering::Acquire) > 0);
    assert!(validate_target(&info.target).is_err());
    std::fs::remove_dir_all(root).unwrap();
}
