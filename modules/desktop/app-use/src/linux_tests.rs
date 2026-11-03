use super::*;

#[test]
fn image_conversion_handles_padding_byte_order_and_rgb565() {
    assert_eq!(
        decode_pixels(
            &[0, 0, 255, 0],
            1,
            1,
            24,
            32,
            true,
            [0xff0000, 0xff00, 0xff]
        )
        .unwrap(),
        [0, 0, 255, 255]
    );
    assert_eq!(
        decode_pixels(&[0xf8, 0, 0, 0], 1, 1, 16, 32, false, [0xf800, 0x7e0, 0x1f]).unwrap(),
        [0, 0, 255, 255]
    );
    assert!(decode_pixels(&[0; 3], 1, 1, 24, 32, true, [0xff0000, 0xff00, 0xff]).is_err());
    assert!(decode_pixels(&[0; 4], 1, 1, 32, 32, true, [0, 0, 0]).is_err());
}

#[test]
fn identities_parse_process_names_with_spaces_and_parentheses() {
    assert_eq!(
        process_start(&format!(
            "42 (a ) tricky name) S {} 123 0",
            vec!["0"; 18].join(" ")
        ))
        .unwrap(),
        123
    );
    assert!(process_start("42 (a) S").is_err());
    assert_eq!(scan_keycode(0x1e, false).unwrap(), 38);
    assert_eq!(scan_keycode(0x1d, true).unwrap(), 105);
    assert!(scan_keycode(255, true).is_err());
}

#[test]
#[ignore = "requires an isolated X11 server with Composite, Damage, XRes and XTest (for example Xvfb)"]
fn x11_background_pixels_input_revocation_and_cleanup() {
    use sinomo_core::node::LocalNodePermissions;
    let x = X11::open().unwrap();
    let window = x.conn.generate_id().unwrap();
    x.conn
        .create_window(
            x11rb::COPY_DEPTH_FROM_PARENT,
            window,
            x.root(),
            20,
            30,
            120,
            80,
            0,
            WindowClass::INPUT_OUTPUT,
            0,
            &CreateWindowAux::new()
                .background_pixel(0xff0000)
                .event_mask(
                    EventMask::KEY_PRESS
                        | EventMask::KEY_RELEASE
                        | EventMask::BUTTON_PRESS
                        | EventMask::BUTTON_RELEASE
                        | EventMask::POINTER_MOTION,
                ),
        )
        .unwrap()
        .check()
        .unwrap();
    x.conn.map_window(window).unwrap().check().unwrap();
    x.conn.flush().unwrap();
    let target = x.identity(window).unwrap();
    use x11rb::wrapper::ConnectionExt as _;
    x.conn
        .change_property32(
            PropMode::REPLACE,
            window,
            x.atom(b"_NET_WM_PID").unwrap(),
            AtomEnum::CARDINAL,
            &[1],
        )
        .unwrap()
        .check()
        .unwrap();
    assert_eq!(
        x.identity(window).unwrap(),
        target,
        "client PID properties cannot change authority"
    );
    let mut stale = target.clone();
    if let DesktopTarget::Window {
        process_started, ..
    } = &mut stale
    {
        *process_started += 1;
    }
    assert!(validate_target(&stale).is_err());
    assert_eq!(
        enumerate_targets(true)
            .unwrap()
            .iter()
            .find(|i| i.target == target)
            .unwrap()
            .bounds,
        [20, 30, 140, 110]
    );
    let root =
        std::env::temp_dir().join(format!("sinomo-x11-test-{}", sinomo_core::Id::generate()));
    let store = LocalNodePermissionStore::new(&root);
    let mut grants = LocalNodePermissions::new();
    grants.set_remote_management_enabled(true);
    grants
        .expose_resource_operation(target.resource_id(), "capture")
        .unwrap();
    grants
        .expose_resource_operation(target.resource_id(), "input")
        .unwrap();
    store.save(&grants).unwrap();
    let before = x.conn.query_pointer(x.root()).unwrap().reply().unwrap();
    let focus = x.conn.get_input_focus().unwrap().reply().unwrap().focus;
    let mut input = DesktopInputSession::open_with_lease(
        target.clone(),
        store.clone(),
        Duration::from_secs(10),
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();
    input
        .send(&[
            DesktopInput::Move { x: 10, y: 12 },
            DesktopInput::Key {
                code: 0x41,
                extended: false,
                down: true,
            },
        ])
        .unwrap();
    let a_key = Input::new(&target)
        .unwrap()
        .keycode(virtual_keysym(0x41).unwrap())
        .unwrap()
        .0;
    let mapping = x
        .conn
        .get_keyboard_mapping(a_key, 1)
        .unwrap()
        .reply()
        .unwrap();
    x.conn
        .change_keyboard_mapping(
            1,
            a_key,
            mapping.keysyms_per_keycode,
            &vec![0; mapping.keysyms.len()],
        )
        .unwrap()
        .check()
        .unwrap();
    drop(input);
    x.conn
        .change_keyboard_mapping(1, a_key, mapping.keysyms_per_keycode, &mapping.keysyms)
        .unwrap()
        .check()
        .unwrap();
    let mut got_press = false;
    let mut got_release = false;
    while let Some(event) = x.conn.poll_for_event().unwrap() {
        match event {
            Event::KeyPress(e) => {
                assert_eq!(e.event, window);
                got_press = true;
            }
            Event::KeyRelease(e) => {
                assert_eq!(
                    e.detail, a_key,
                    "cleanup uses the injected code even after a keymap change"
                );
                got_release = true;
            }
            _ => {}
        }
    }
    assert!(got_press && got_release);
    let after = x.conn.query_pointer(x.root()).unwrap().reply().unwrap();
    assert_eq!((before.root_x, before.root_y), (after.root_x, after.root_y));
    assert_eq!(
        focus,
        x.conn.get_input_focus().unwrap().reply().unwrap().focus
    );
    let capture = DesktopCapture::start(
        DesktopCaptureConfig {
            resolution: None,
            pixel_format: None,
            target: target.clone(),
            frames_per_second: 30,
            include_cursor: false,
            lease_ms: 5000,
        },
        store.clone(),
    )
    .unwrap();
    let frame = capture.next(0, Duration::from_secs(2)).unwrap().unwrap();
    assert_eq!((frame.width, frame.height), (120, 80));
    assert_eq!(&frame.data[..4], &[0, 0, 255, 255]);
    let cover = x.conn.generate_id().unwrap();
    x.conn
        .create_window(
            x11rb::COPY_DEPTH_FROM_PARENT,
            cover,
            x.root(),
            0,
            0,
            400,
            300,
            0,
            WindowClass::INPUT_OUTPUT,
            0,
            &CreateWindowAux::new().background_pixel(0x0000ff),
        )
        .unwrap()
        .check()
        .unwrap();
    x.conn.map_window(cover).unwrap().check().unwrap();
    // A redraw while fully occluded still comes from the app's redirected pixmap.
    x.conn
        .clear_area(true, window, 0, 0, 0, 0)
        .unwrap()
        .check()
        .unwrap();
    let occluded = capture
        .next(frame.sequence, Duration::from_secs(2))
        .unwrap()
        .unwrap();
    assert_eq!(&occluded.data[..4], &[0, 0, 255, 255]);
    x.conn
        .configure_window(window, &ConfigureWindowAux::new().width(160).height(100))
        .unwrap()
        .check()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut sequence = occluded.sequence;
    loop {
        let fresh = capture.next(sequence, Duration::from_millis(200)).unwrap();
        if let Some(fresh) = fresh {
            if (fresh.width, fresh.height) == (160, 100) {
                break;
            }
            sequence = fresh.sequence;
        }
        assert!(Instant::now() < deadline, "capture did not follow resize");
    }
    let mut input = Input::new(&target).unwrap();
    assert!(matches!(
        input.send(&DesktopInput::MoveInFrame {
            x: 0.5,
            y: 0.5,
            source_width: 120,
            source_height: 80
        }),
        Err(DesktopError::TargetUnavailable(_))
    ));
    grants.set_remote_management_enabled(false);
    store.save(&grants).unwrap();
    assert!(matches!(
        capture.next(0, Duration::ZERO),
        Err(DesktopError::PermissionDenied(_))
    ));
    drop(capture);
    x.conn.destroy_window(cover).unwrap().check().unwrap();
    x.conn.destroy_window(window).unwrap().check().unwrap();
    assert!(validate_target(&target).is_err());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
#[ignore = "requires an isolated X11 server; deliberately moves its pointer using XTest"]
fn x11_raw_input_and_lease_cleanup() {
    use sinomo_core::node::LocalNodePermissions;
    let x = X11::open().unwrap();
    let target = enumerate_targets(false).unwrap().remove(0).target;
    let root = std::env::temp_dir().join(format!("sinomo-x11-raw-{}", sinomo_core::Id::generate()));
    let store = LocalNodePermissionStore::new(&root);
    let mut grants = LocalNodePermissions::new();
    grants.set_remote_management_enabled(true);
    grants
        .expose_resource_operation(target.resource_id(), "input")
        .unwrap();
    store.save(&grants).unwrap();
    let mut input = DesktopInputSession::open_with_lease(
        target,
        store,
        Duration::from_secs(3),
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();
    input
        .send(&[
            DesktopInput::Move { x: 250, y: 200 },
            DesktopInput::Key {
                code: 0x1e,
                down: true,
                extended: false,
            },
        ])
        .unwrap();
    let pointer = x.conn.query_pointer(x.root()).unwrap().reply().unwrap();
    assert_eq!((pointer.root_x, pointer.root_y), (250, 200));
    let keys = x.conn.query_keymap().unwrap().reply().unwrap();
    assert_ne!(keys.keys[38 / 8] & (1 << (38 % 8)), 0);
    drop(input);
    let keys = x.conn.query_keymap().unwrap().reply().unwrap();
    assert_eq!(keys.keys[38 / 8] & (1 << (38 % 8)), 0);
    std::fs::remove_dir_all(root).unwrap();
}
