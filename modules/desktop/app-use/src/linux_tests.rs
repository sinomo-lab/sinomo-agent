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

#[test]
#[ignore = "requires an isolated 1280x720 X11 server; emits capture latency samples"]
fn x11_capture_change_latency() {
    let x = X11::open().unwrap();
    let window = x.conn.generate_id().unwrap();
    x.conn
        .create_window(
            x11rb::COPY_DEPTH_FROM_PARENT,
            window,
            x.root(),
            0,
            0,
            1280,
            720,
            0,
            WindowClass::INPUT_OUTPUT,
            0,
            &CreateWindowAux::new().background_pixel(0),
        )
        .unwrap()
        .check()
        .unwrap();
    x.conn.map_window(window).unwrap().check().unwrap();
    let target = x.identity(window).unwrap();
    let root = std::env::temp_dir().join(format!(
        "sinomo-x11-latency-{}",
        sinomo_core::Id::generate()
    ));
    let store = LocalNodePermissionStore::new(&root);
    store
        .mutate(|p| {
            p.set_remote_management_enabled(true);
            p.expose_resource_operation(target.resource_id(), "capture")
        })
        .unwrap();
    let capture = DesktopCapture::start(
        DesktopCaptureConfig {
            target,
            frames_per_second: 10,
            include_cursor: false,
            lease_ms: 60000,
            resolution: Some(CaptureResolution {
                width: 640,
                height: 640,
            }),
            pixel_format: None,
        },
        store,
    )
    .unwrap();
    let first = capture.next(0, Duration::from_secs(2)).unwrap().unwrap();
    let mut sequence = first.sequence;
    let mut rows = Vec::new();
    for index in 0..55u32 {
        std::thread::sleep(Duration::from_millis(
            [137, 181, 223, 157, 199][index as usize % 5],
        ));
        let red = (index * 3 + 20) as u8;
        let started = Instant::now();
        x.conn
            .change_window_attributes(
                window,
                &ChangeWindowAttributesAux::new().background_pixel(u32::from(red) << 16),
            )
            .unwrap();
        x.conn.clear_area(false, window, 0, 0, 0, 0).unwrap();
        x.conn.flush().unwrap();
        loop {
            let frame = capture.next(sequence, Duration::from_millis(100)).unwrap();
            if let Some(frame) = frame {
                sequence = frame.sequence;
                if frame.data[..4] == [0, 0, red, 255] {
                    assert_eq!((frame.width, frame.height), (640, 360));
                    assert_eq!(frame.native_dimensions(), [1280, 720]);
                    rows.push(serde_json::json!({"sample":index,"warmup":index<5,"change_to_frame_us":started.elapsed().as_micros(),"sequence":frame.sequence,"captured_at_unix_ms":frame.captured_at_unix_ms}));
                    break;
                }
            }
            assert!(
                started.elapsed() < Duration::from_secs(2),
                "final static change was not delivered"
            );
        }
    }
    drop(capture);
    x.conn.destroy_window(window).unwrap().check().unwrap();
    std::fs::remove_dir_all(root).unwrap();
    if let Ok(path) = std::env::var("SINOMO_X11_CAPTURE_RESULTS") {
        std::fs::write(path, serde_json::to_vec_pretty(&rows).unwrap()).unwrap();
    }
}

#[test]
fn standard_xrgb_reuses_reply_allocation_and_normalizes_alpha() {
    let input = vec![3, 2, 1, 0, 6, 5, 4, 77];
    let pointer = input.as_ptr();
    let result = decode_pixels_owned(input, 2, 1, 32, 32, true, [0xff0000, 0xff00, 0xff]).unwrap();
    assert_eq!(result.as_ptr(), pointer);
    assert_eq!(result, [3, 2, 1, 255, 6, 5, 4, 255]);
    let input = vec![255, 0, 0, 0];
    assert_eq!(
        decode_pixels_owned(input.clone(), 1, 1, 24, 32, false, [0xff0000, 0xff00, 0xff]).unwrap(),
        decode_pixels(&input, 1, 1, 24, 32, false, [0xff0000, 0xff00, 0xff]).unwrap()
    );
}

struct CaptureFixture {
    x: X11,
    window: Window,
    capture: Option<DesktopCapture>,
    store: LocalNodePermissionStore,
    root: std::path::PathBuf,
}
impl CaptureFixture {
    fn new(fps: u32, cursor: bool) -> Self {
        Self::new_target(fps, cursor, false)
    }
    fn new_target(fps: u32, cursor: bool, monitor: bool) -> Self {
        let x = X11::open().unwrap();
        let window = x.conn.generate_id().unwrap();
        x.conn
            .create_window(
                x11rb::COPY_DEPTH_FROM_PARENT,
                window,
                x.root(),
                0,
                0,
                256,
                128,
                0,
                WindowClass::INPUT_OUTPUT,
                0,
                &CreateWindowAux::new().background_pixel(0x0000ff),
            )
            .unwrap()
            .check()
            .unwrap();
        x.conn.map_window(window).unwrap().check().unwrap();
        let target = if monitor {
            DesktopTarget::Monitor {
                device_name: x.display_name(),
            }
        } else {
            x.identity(window).unwrap()
        };
        let root =
            std::env::temp_dir().join(format!("sinomo-x11-events-{}", sinomo_core::Id::generate()));
        let store = LocalNodePermissionStore::new(&root);
        store
            .mutate(|p| {
                p.set_remote_management_enabled(true);
                p.expose_resource_operation(target.resource_id(), "capture")
            })
            .unwrap();
        let capture = DesktopCapture::start(
            DesktopCaptureConfig {
                target,
                frames_per_second: fps,
                include_cursor: cursor,
                lease_ms: 10000,
                resolution: None,
                pixel_format: None,
            },
            store.clone(),
        )
        .unwrap();
        Self {
            x,
            window,
            capture: Some(capture),
            store,
            root,
        }
    }
    fn next(&self, after: u64, timeout: Duration) -> Option<NativeVideoSample> {
        self.capture.as_ref().unwrap().next(after, timeout).unwrap()
    }
    fn paint(&self, color: u32) {
        self.x
            .conn
            .change_window_attributes(
                self.window,
                &ChangeWindowAttributesAux::new().background_pixel(color),
            )
            .unwrap();
        self.x
            .conn
            .clear_area(false, self.window, 0, 0, 0, 0)
            .unwrap();
        self.x.conn.flush().unwrap();
    }
}
impl Drop for CaptureFixture {
    fn drop(&mut self) {
        drop(self.capture.take());
        let _ = self.x.conn.destroy_window(self.window);
        let _ = self.x.conn.flush();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
#[test]
#[ignore = "requires isolated X11 with XInput2/XFixes and XTest"]
fn x11_cursor_notifications_idle_and_revocation() {
    let f = CaptureFixture::new(30, true);
    let source = f.x.conn.generate_id().unwrap();
    let gc = f.x.conn.generate_id().unwrap();
    let cursor = f.x.conn.generate_id().unwrap();
    f.x.conn
        .create_pixmap(1, source, f.window, 8, 8)
        .unwrap()
        .check()
        .unwrap();
    f.x.conn
        .create_gc(gc, source, &CreateGCAux::new().foreground(1))
        .unwrap()
        .check()
        .unwrap();
    f.x.conn
        .poly_fill_rectangle(
            source,
            gc,
            &[Rectangle {
                x: 0,
                y: 0,
                width: 8,
                height: 8,
            }],
        )
        .unwrap()
        .check()
        .unwrap();
    f.x.conn
        .create_cursor(cursor, source, source, 65535, 0, 0, 0, 0, 0, 0, 0)
        .unwrap()
        .check()
        .unwrap();
    let mut sequence = f.next(0, Duration::from_secs(2)).unwrap().sequence;
    f.x.conn
        .change_window_attributes(f.window, &ChangeWindowAttributesAux::new().cursor(cursor))
        .unwrap()
        .check()
        .unwrap();
    xtest::fake_input(&f.x.conn, MOTION_NOTIFY_EVENT, 0, 0, f.x.root(), 20, 20, 0)
        .unwrap()
        .check()
        .unwrap();
    let until = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(frame) = f.next(sequence, Duration::from_millis(100)) {
            sequence = frame.sequence;
            if frame.data[(20 * 256 + 20) * 4..(20 * 256 + 20) * 4 + 4] == [0, 0, 255, 255] {
                break;
            }
        }
        assert!(
            Instant::now() < until,
            "cursor motion/shape did not wake capture"
        );
    }
    // Drain only pending startup notifications, then prove no periodic screenshots.
    while let Some(frame) = f.next(sequence, Duration::from_millis(100)) {
        sequence = frame.sequence;
        assert!(Instant::now() < until, "idle cursor keeps producing frames");
    }
    assert!(f.next(sequence, Duration::from_millis(350)).is_none());
    xtest::fake_input(&f.x.conn, MOTION_NOTIFY_EVENT, 0, 0, f.x.root(), 40, 30, 0)
        .unwrap()
        .check()
        .unwrap();
    let frame = f.next(sequence, Duration::from_secs(1)).unwrap();
    assert_eq!(
        &frame.data[(30 * 256 + 40) * 4..(30 * 256 + 40) * 4 + 4],
        &[0, 0, 255, 255]
    );
    // Programmatic warps also change visible cursor position without Damage.
    f.x.conn
        .warp_pointer(x11rb::NONE, f.x.root(), 0, 0, 0, 0, 60, 40)
        .unwrap()
        .check()
        .unwrap();
    let warped = f
        .next(frame.sequence, Duration::from_secs(1))
        .expect("cursor warp was not observed");
    assert_eq!(
        &warped.data[(40 * 256 + 60) * 4..(40 * 256 + 60) * 4 + 4],
        &[0, 0, 255, 255]
    );
    let changed_cursor = f.x.conn.generate_id().unwrap();
    f.x.conn
        .create_cursor(changed_cursor, source, source, 0, 65535, 0, 0, 0, 0, 0, 0)
        .unwrap()
        .check()
        .unwrap();
    f.x.conn
        .change_window_attributes(
            f.window,
            &ChangeWindowAttributesAux::new().cursor(changed_cursor),
        )
        .unwrap()
        .check()
        .unwrap();
    let changed = f
        .next(warped.sequence, Duration::from_secs(1))
        .expect("cursor shape was not observed");
    assert_eq!(
        &changed.data[(40 * 256 + 60) * 4..(40 * 256 + 60) * 4 + 4],
        &[0, 255, 0, 255]
    );
    // A child may consume core motion, preventing propagation to its parent.
    let child = f.x.conn.generate_id().unwrap();
    f.x.conn
        .create_window(
            x11rb::COPY_DEPTH_FROM_PARENT,
            child,
            f.window,
            100,
            0,
            100,
            100,
            0,
            WindowClass::INPUT_OUTPUT,
            0,
            &CreateWindowAux::new()
                .background_pixel(0x0000ff)
                .event_mask(EventMask::POINTER_MOTION),
        )
        .unwrap()
        .check()
        .unwrap();
    f.x.conn.map_window(child).unwrap().check().unwrap();
    let mut seq = changed.sequence;
    let until = Instant::now() + Duration::from_secs(1);
    while let Some(frame) = f.next(seq, Duration::from_millis(50)) {
        seq = frame.sequence;
        assert!(Instant::now() < until);
    }
    f.x.conn
        .warp_pointer(x11rb::NONE, f.x.root(), 0, 0, 0, 0, 120, 20)
        .unwrap()
        .check()
        .unwrap();
    let in_child = f
        .next(seq, Duration::from_secs(1))
        .expect("child consumed warp notification");
    assert_eq!(
        &in_child.data[(20 * 256 + 120) * 4..(20 * 256 + 120) * 4 + 4],
        &[0, 255, 0, 255]
    );
    f.x.conn
        .reparent_window(child, f.x.root(), 300, 0)
        .unwrap()
        .check()
        .unwrap();
    let until = Instant::now() + Duration::from_secs(1);
    while f
        .x
        .conn
        .get_window_attributes(child)
        .unwrap()
        .reply()
        .unwrap()
        .all_event_masks
        .contains(EventMask::STRUCTURE_NOTIFY)
    {
        assert!(
            Instant::now() < until,
            "cursor watcher survived reparent outside target"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        f.x.conn
            .get_window_attributes(child)
            .unwrap()
            .reply()
            .unwrap()
            .all_event_masks
            .contains(EventMask::POINTER_MOTION),
        "another client's event mask was changed"
    );
    f.x.conn.destroy_window(child).unwrap().check().unwrap();
    f.paint(0x101010);
    let until = Instant::now() + Duration::from_secs(1);
    let mut seq = in_child.sequence;
    loop {
        if let Some(frame) = f.next(seq, Duration::from_millis(100)) {
            seq = frame.sequence;
            if frame.data[..4] == [16, 16, 16, 255] {
                break;
            }
        }
        assert!(
            Instant::now() < until,
            "child removal stopped parent capture"
        );
    }
    f.x.conn.free_cursor(changed_cursor).unwrap();
    let target = f.x.identity(f.window).unwrap();
    f.store
        .mutate(|p| p.hide_resource_operation(&target.resource_id(), "capture"))
        .unwrap();
    assert!(matches!(
        f.capture
            .as_ref()
            .unwrap()
            .next(frame.sequence, Duration::ZERO),
        Err(DesktopError::PermissionDenied(_))
    ));
    f.x.conn.free_cursor(cursor).unwrap();
    f.x.conn.free_gc(gc).unwrap();
    f.x.conn.free_pixmap(source).unwrap();
}
#[test]
#[ignore = "requires isolated X11 server"]
fn x11_ceiling_keeps_final_static_update_and_stop_is_bounded() {
    let mut f = CaptureFixture::new(2, false);
    let first = f.next(0, Duration::from_secs(1)).unwrap();
    f.paint(0xff0000);
    let second = f.next(first.sequence, Duration::from_secs(1)).unwrap();
    f.paint(0x00ff00);
    std::thread::sleep(Duration::from_millis(30));
    f.paint(0xffffff);
    assert!(
        f.next(second.sequence, Duration::from_millis(150))
            .is_none(),
        "exhausted ceiling admitted an early frame"
    );
    let final_frame = f.next(second.sequence, Duration::from_secs(2)).unwrap();
    assert_eq!(&final_frame.data[..4], &[255, 255, 255, 255]);
    assert!(
        f.next(final_frame.sequence, Duration::from_millis(200))
            .is_none()
    );
    let stop = Instant::now();
    drop(f.capture.take());
    assert!(stop.elapsed() < Duration::from_millis(500));
}

#[test]
#[ignore = "synthetic resize timing; run with android-debug so both implementations are optimized"]
fn capture_resize_cost() {
    use std::hint::black_box;
    #[inline(never)]
    fn floating(source: &[u8], sw: usize, sh: usize, w: usize, h: usize) -> Arc<[u8]> {
        let mut pixels = vec![0u8; w * h * 4];
        for y in 0..h {
            let sy = ((y as f64 + 0.5) * sh as f64 / h as f64 - 0.5).max(0.0);
            let y0 = sy.floor() as usize;
            let y1 = (y0 + 1).min(sh - 1);
            let fy = sy - y0 as f64;
            for x in 0..w {
                let sx = ((x as f64 + 0.5) * sw as f64 / w as f64 - 0.5).max(0.0);
                let x0 = sx.floor() as usize;
                let x1 = (x0 + 1).min(sw - 1);
                let fx = sx - x0 as f64;
                for c in 0..4 {
                    let top = f64::from(source[(y0 * sw + x0) * 4 + c]) * (1.0 - fx)
                        + f64::from(source[(y0 * sw + x1) * 4 + c]) * fx;
                    let bottom = f64::from(source[(y1 * sw + x0) * 4 + c]) * (1.0 - fx)
                        + f64::from(source[(y1 * sw + x1) * 4 + c]) * fx;
                    pixels[(y * w + x) * 4 + c] = (top * (1.0 - fy) + bottom * fy).round() as u8;
                }
            }
        }
        pixels.into()
    }
    let source: Vec<u8> = (0..1280 * 720 * 4).map(|i| (i * 37 % 256) as u8).collect();
    let mut rows = Vec::new();
    for sample in 0..55 {
        let (sw, sh, w, h) = black_box((1280, 720, 640, 360));
        let start = Instant::now();
        let reference = floating(black_box(&source), sw, sh, w, h);
        let reference_us = start.elapsed().as_micros();
        let start = Instant::now();
        let resized = resize_capture_pixels(
            black_box(&source),
            sw as u32,
            sh as u32,
            sw * 4,
            w as u32,
            h as u32,
        )
        .unwrap();
        let fixed_us = start.elapsed().as_micros();
        assert!(
            reference
                .iter()
                .zip(resized.iter())
                .all(|(a, b)| a.abs_diff(*b) <= 1)
        );
        rows.push(serde_json::json!({"sample":sample,"warmup":sample<5,"floating_reference_us":reference_us,"fixed_point_us":fixed_us}));
    }
    if let Ok(path) = std::env::var("SINOMO_X11_RESIZE_RESULTS") {
        std::fs::write(path, serde_json::to_vec_pretty(&rows).unwrap()).unwrap();
    }
}

#[test]
#[ignore = "requires isolated X11 server"]
fn x11_socket_wait_is_woken_by_cancellation() {
    use std::io::Write;
    let x = X11::open().unwrap();
    let (mut sender, receiver) = std::os::unix::net::UnixStream::pair().unwrap();
    let started = Instant::now();
    let worker = std::thread::spawn(move || {
        wait_for_x_event(&x.conn, &receiver, Duration::from_secs(5)).unwrap()
    });
    sender.write_all(&[1]).unwrap();
    assert!(!worker.join().unwrap());
    assert!(started.elapsed() < Duration::from_millis(500));
}

#[test]
#[ignore = "requires isolated X11 server"]
fn x11_monitor_child_damage_and_idle() {
    let f = CaptureFixture::new_target(10, false, true);
    let first = f.next(0, Duration::from_secs(1)).unwrap();
    let mut sequence = first.sequence;
    f.paint(0xabcdef);
    let until = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(frame) = f.next(sequence, Duration::from_millis(100)) {
            sequence = frame.sequence;
            if frame.data[..4] == [0xef, 0xcd, 0xab, 255] {
                break;
            }
        }
        assert!(
            Instant::now() < until,
            "child redraw did not notify monitor capture"
        );
    }
    while let Some(frame) = f.next(sequence, Duration::from_millis(100)) {
        sequence = frame.sequence;
        assert!(Instant::now() < until);
    }
    assert!(f.next(sequence, Duration::from_millis(250)).is_none());
}
