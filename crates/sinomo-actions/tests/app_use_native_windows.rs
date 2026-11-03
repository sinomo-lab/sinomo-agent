//! Opt-in interactive-desktop acceptance. Sends messages only to an owned test
//! window, never raw keyboard/mouse input to the user's active application.
#![cfg(target_os = "windows")]
use sinomo_actions::desktop::provider::DesktopProviderClient;
use sinomo_runtime::device::desktop::*;
use sinomo_runtime::node::{LocalNodePermissionStore, LocalNodePermissions};
#[path = "support/app_use.rs"]
mod support;
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};
use windows::Win32::Foundation::*;
use windows::Win32::Graphics::Gdi::*;
use windows::Win32::UI::HiDpi::*;
use windows::Win32::UI::WindowsAndMessaging::*;
use windows::core::w;

type Log = Arc<Mutex<Vec<(u32, usize, isize)>>>;
unsafe extern "system" fn wndproc(window: HWND, message: u32, wp: WPARAM, lp: LPARAM) -> LRESULT {
    if message == WM_NCCREATE {
        let create = unsafe { &*(lp.0 as *const CREATESTRUCTW) };
        unsafe {
            SetWindowLongPtrW(window, GWLP_USERDATA, create.lpCreateParams as isize);
        }
    }
    let state = unsafe { GetWindowLongPtrW(window, GWLP_USERDATA) } as *const Log;
    if !state.is_null()
        && matches!(
            message,
            WM_CHAR
                | WM_KEYDOWN
                | WM_KEYUP
                | WM_MOUSEMOVE
                | WM_LBUTTONDOWN
                | WM_LBUTTONUP
                | WM_MOUSEWHEEL
        )
    {
        unsafe { &*state }
            .lock()
            .unwrap()
            .push((message, wp.0, lp.0));
    }
    if message == WM_PAINT {
        let mut paint = PAINTSTRUCT::default();
        let dc = unsafe { BeginPaint(window, &mut paint) };
        let color = if !state.is_null()
            && unsafe { &*state }
                .lock()
                .unwrap()
                .iter()
                .any(|m| m.0 == WM_CHAR)
        {
            COLORREF(0x0000ff)
        } else {
            COLORREF(0x00ff00)
        };
        let brush = unsafe { CreateSolidBrush(color) };
        unsafe {
            FillRect(dc, &paint.rcPaint, brush);
            let _ = DeleteObject(brush.into());
            let _ = EndPaint(window, &paint);
        }
        return LRESULT(0);
    }
    unsafe { DefWindowProcW(window, message, wp, lp) }
}

#[test]
#[ignore = "requires an interactive Windows desktop; creates one no-activate test window"]
fn background_window_messages_and_continuous_capture_preserve_user_input() {
    background_window_acceptance(false);
}

#[test]
#[ignore = "requires an interactive Windows desktop; creates one no-activate DPI-unaware window"]
fn background_dpi_unaware_window_maps_frame_coordinates() {
    background_window_acceptance(true);
}

fn background_window_acceptance(dpi_unaware: bool) {
    let root = sinomo_testdir::TestDir::new("desktop-windows-acceptance");
    support::install(&root);
    let provider = DesktopProviderClient::resolve(&root).unwrap();
    let store = LocalNodePermissionStore::new(&root);
    let mut grants = LocalNodePermissions::new();
    grants.set_remote_management_enabled(true);
    store.save(&grants).unwrap();
    let log: Log = Arc::new(Mutex::new(Vec::new()));
    let thread_log = log.clone();
    let (ready, receive) = mpsc::sync_channel(1);
    let (stop, stopping) = mpsc::channel();
    let before_focus = unsafe { GetForegroundWindow() };
    let mut before_cursor = POINT::default();
    unsafe {
        GetCursorPos(&mut before_cursor).unwrap();
    }
    let thread = std::thread::spawn(move || unsafe {
        let old_dpi = SetThreadDpiAwarenessContext(if dpi_unaware {
            DPI_AWARENESS_CONTEXT_UNAWARE
        } else {
            DPI_AWARENESS_CONTEXT_PER_MONITOR_AWARE_V2
        });
        let class = WNDCLASSW {
            lpfnWndProc: Some(wndproc),
            lpszClassName: w!("SinomoDesktopAcceptance"),
            ..Default::default()
        };
        assert_ne!(RegisterClassW(&class), 0);
        let window = CreateWindowExW(
            WS_EX_NOACTIVATE | WS_EX_TOOLWINDOW,
            class.lpszClassName,
            w!("Sinomo desktop acceptance"),
            WS_OVERLAPPEDWINDOW,
            80,
            80,
            220,
            180,
            None,
            None,
            None,
            Some(&thread_log as *const _ as *const _),
        )
        .unwrap();
        let _ = ShowWindow(window, SW_SHOWNOACTIVATE);
        let _ = SetWindowPos(
            window,
            Some(HWND_BOTTOM),
            0,
            0,
            0,
            0,
            SWP_NOMOVE | SWP_NOSIZE | SWP_NOACTIVATE,
        );
        // Derive the physical point from the application's own logical space,
        // independently of the backend which must invert this conversion.
        let mut origin = POINT::default();
        let mut point = POINT { x: 31, y: 29 };
        ClientToScreen(window, &mut origin).ok().unwrap();
        ClientToScreen(window, &mut point).ok().unwrap();
        LogicalToPhysicalPointForPerMonitorDPI(Some(window), &mut origin)
            .ok()
            .unwrap();
        LogicalToPhysicalPointForPerMonitorDPI(Some(window), &mut point)
            .ok()
            .unwrap();
        ready
            .send((
                window.0 as usize as u64,
                [point.x - origin.x, point.y - origin.y],
            ))
            .unwrap();
        let mut message = MSG::default();
        while stopping.try_recv().is_err() {
            let _ = InvalidateRect(Some(window), None, false);
            while PeekMessageW(&mut message, None, 0, 0, PM_REMOVE).as_bool() {
                // Do not TranslateMessage: this fixture records exactly what
                // the app-use backend queued, including explicit WM_CHAR.
                DispatchMessageW(&message);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        DestroyWindow(window).unwrap();
        UnregisterClassW(class.lpszClassName, None).unwrap();
        SetThreadDpiAwarenessContext(old_dpi);
    });
    let (handle, physical) = receive.recv_timeout(Duration::from_secs(3)).unwrap();
    // Always stop the fixture even if a validation below panics.
    struct Cleanup(
        Option<mpsc::Sender<()>>,
        Option<std::thread::JoinHandle<()>>,
    );
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = self.0.take().unwrap().send(());
            let _ = self.1.take().unwrap().join();
        }
    }
    let _cleanup = Cleanup(Some(stop), Some(thread));
    let info = provider.targets(DesktopControlMode::Background).unwrap().into_iter().find(|info| matches!(info.target, DesktopTarget::Window { handle: found, .. } if found == handle)).unwrap();
    let target = info.target.clone();
    for operation in ["input", "capture"] {
        grants
            .expose_resource_operation(target.resource_id(), operation)
            .unwrap();
    }
    store.save(&grants).unwrap();
    let capture = provider
        .capture(DesktopCaptureConfig {
            resolution: None,
            pixel_format: None,
            target: target.clone(),
            frames_per_second: 30,
            include_cursor: false,
            lease_ms: 30_000,
        })
        .unwrap();
    let first = capture
        .next(0, Duration::from_secs(5))
        .unwrap()
        .expect("first compositor frame");
    first.validate().unwrap();
    let mut input = provider
        .open_input(target.clone(), DesktopControlMode::Background, 30_000)
        .unwrap();
    input
        .send(&[
            DesktopInput::Move {
                x: physical[0],
                y: physical[1],
            },
            DesktopInput::Button {
                button: MouseButton::Left,
                down: true,
            },
            DesktopInput::Button {
                button: MouseButton::Left,
                down: false,
            },
            DesktopInput::Key {
                code: 65,
                down: true,
                extended: false,
            },
            DesktopInput::Key {
                code: 65,
                down: false,
                extended: false,
            },
            DesktopInput::Text { text: "Z".into() },
        ])
        .unwrap();
    let until = Instant::now() + Duration::from_secs(3);
    while Instant::now() < until && !log.lock().unwrap().iter().any(|m| m.0 == WM_CHAR) {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        log.lock()
            .unwrap()
            .iter()
            .any(|m| m.0 == WM_CHAR && m.1 == 'Z' as usize)
    );
    assert!(
        log.lock()
            .unwrap()
            .iter()
            .any(|m| m.0 == WM_LBUTTONDOWN && m.2 as u32 == (31 | (29 << 16))),
        "physical={physical:?}, messages={:?}",
        log.lock().unwrap()
    );
    assert!(
        log.lock()
            .unwrap()
            .iter()
            .any(|m| m.0 == WM_KEYUP && m.2 as u32 & 0xc0000000 == 0xc0000000)
    );
    let client = info.client_bounds.unwrap();
    let normalized = DesktopInput::MoveInFrame {
        x: f64::from(client[0] + physical[0] - info.bounds[0]) / f64::from(first.width - 1),
        y: f64::from(client[1] + physical[1] - info.bounds[1]) / f64::from(first.height - 1),
        source_width: first.width,
        source_height: first.height,
    };
    log.lock().unwrap().clear();
    input
        .send(&[
            normalized,
            DesktopInput::Button {
                button: MouseButton::Left,
                down: true,
            },
            DesktopInput::Button {
                button: MouseButton::Left,
                down: false,
            },
            DesktopInput::Wheel {
                delta: 120,
                horizontal: false,
            },
            DesktopInput::Text { text: "Z".into() },
        ])
        .unwrap();
    let until = Instant::now() + Duration::from_secs(3);
    while Instant::now() < until && !log.lock().unwrap().iter().any(|m| m.0 == WM_CHAR) {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        log.lock()
            .unwrap()
            .iter()
            .any(|m| m.0 == WM_LBUTTONDOWN && m.2 as u32 == (31 | (29 << 16))),
        "normalized point must reach the same logical client pixel, including DPI virtualization"
    );
    assert!(
        log.lock()
            .unwrap()
            .iter()
            .any(|m| m.0 == WM_MOUSEWHEEL && (m.1 >> 16) == 120)
    );
    assert!(matches!(
        input.send(&[
            DesktopInput::MoveInFrame {
                x: 0.5,
                y: 0.5,
                source_width: first.width + 100,
                source_height: first.height
            },
            DesktopInput::Text { text: "!".into() },
        ]),
        Err(DesktopError::TargetUnavailable(_))
    ));
    std::thread::sleep(Duration::from_millis(60));
    assert!(
        !log.lock()
            .unwrap()
            .iter()
            .any(|m| m.0 == WM_CHAR && m.1 == '!' as usize),
        "stale geometry must stop the batch before text input"
    );
    let mut sequence = first.sequence;
    let changed = loop {
        let frame = capture
            .next(sequence, Duration::from_secs(3))
            .unwrap()
            .expect("subsequent video frame");
        sequence = frame.sequence;
        if frame.data != first.data {
            break frame;
        }
        assert!(Instant::now() < until, "window pixels did not update");
    };
    assert!(changed.timestamp_ns > first.timestamp_ns);
    assert_eq!(unsafe { GetForegroundWindow() }, before_focus);
    let mut after_cursor = POINT::default();
    unsafe {
        GetCursorPos(&mut after_cursor).unwrap();
    }
    assert_eq!(
        (after_cursor.x, after_cursor.y),
        (before_cursor.x, before_cursor.y)
    );
    // Revoke while a key is held: the worker must release it without another
    // caller request, and cached video must no longer be readable.
    input
        .send(&[DesktopInput::Key {
            code: 66,
            down: true,
            extended: false,
        }])
        .unwrap();
    grants.set_remote_management_enabled(false);
    store.save(&grants).unwrap();
    let until = Instant::now() + Duration::from_secs(2);
    while Instant::now() < until
        && !log
            .lock()
            .unwrap()
            .iter()
            .any(|m| m.0 == WM_KEYUP && m.1 == 66)
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        log.lock()
            .unwrap()
            .iter()
            .any(|m| m.0 == WM_KEYUP && m.1 == 66)
    );
    assert!(matches!(
        capture.next(0, Duration::ZERO),
        Err(DesktopError::PermissionDenied(_))
    ));
    drop(capture);
    drop(input);
    let report = Arc::try_unwrap(provider)
        .ok()
        .expect("all native handles released")
        .unload();
    assert!(
        report.shutdown_ok && report.library_handle_released,
        "{report:?}"
    );
}
