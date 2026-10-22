//! Owned Win32 test application. The model can change state only through real
//! keyboard/mouse messages; the verifier reads this independent application state.
use serde::Serialize;
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};
use windows::Gaming::Input::{Gamepad, GamepadButtons};
use windows::Win32::Foundation::*;
use windows::Win32::Graphics::Gdi::*;
use windows::Win32::System::StationsAndDesktops::*;
use windows::Win32::System::Threading::GetCurrentThreadId;
use windows::Win32::System::WinRT::{RO_INIT_MULTITHREADED, RoInitialize, RoUninitialize};
use windows::Win32::UI::HiDpi::*;
use windows::Win32::UI::Input::KeyboardAndMouse::*;
use windows::Win32::UI::Input::XboxController::*;
use windows::Win32::UI::WindowsAndMessaging::*;
use windows::core::w;

pub fn desktop_name(input: bool) -> Result<String, String> {
    unsafe {
        let desktop = if input {
            OpenInputDesktop(DESKTOP_CONTROL_FLAGS(0), false, DESKTOP_READOBJECTS)
                .map_err(|e| e.to_string())?
        } else {
            GetThreadDesktop(GetCurrentThreadId()).map_err(|e| e.to_string())?
        };
        let mut name = [0u16; 256];
        let mut needed = 0;
        let result = GetUserObjectInformationW(
            HANDLE(desktop.0),
            UOI_NAME,
            Some(name.as_mut_ptr().cast()),
            (name.len() * 2) as u32,
            Some(&mut needed),
        );
        if input {
            let _ = CloseDesktop(desktop);
        }
        result.map_err(|e| e.to_string())?;
        Ok(String::from_utf16_lossy(
            &name[..name.iter().position(|c| *c == 0).unwrap_or(name.len())],
        ))
    }
}
pub fn require_test_desktop(expected: &str, interactive: bool) -> Result<(), String> {
    if (!interactive && !expected.starts_with("SinomoE2E-")) || desktop_name(false)? != expected {
        return Err(
            "desktop tests require the launcher or explicit interactive-fixture consent".into(),
        );
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize)]
pub struct State {
    pub case: String,
    pub expected_code: String,
    pub typed: String,
    pub submitted: String,
    pub counter: u32,
    pub enabled: bool,
    pub expanded: bool,
    pub dragged: bool,
    pub wheel_ticks: i32,
    pub input_events: u64,
    pub token: [f64; 2],
    #[serde(skip)]
    text_active: bool,
    #[serde(skip)]
    dragging: bool,
    #[serde(skip)]
    mouse_down: Option<[f64; 2]>,
    #[serde(skip)]
    control_down: bool,
    pub video_tick: u64,
    pub gamepad_pattern_seen: bool,
    pub gamepad_neutral_seen: bool,
    pub gamepad_slot: Option<u32>,
    pub gamepad_last_reading: Option<[i32; 7]>,
    pub setup_activation_click: bool,
    pub gamepad_wgi_count: u32,
    pub gamepad_wgi_reading: Option<[f64; 7]>,
    pub gamepad_wgi_error: Option<String>,
    pub gamepad_observer: Option<String>,
    #[serde(skip)]
    wgi_selected: Option<Gamepad>,
}
impl State {
    fn new(case: &str, code: &str) -> Self {
        Self {
            case: case.into(),
            expected_code: code.into(),
            typed: String::new(),
            submitted: String::new(),
            counter: 0,
            enabled: false,
            expanded: false,
            dragged: false,
            wheel_ticks: 0,
            input_events: 0,
            token: [150.0, 490.0],
            text_active: false,
            dragging: false,
            mouse_down: None,
            control_down: false,
            video_tick: 0,
            gamepad_pattern_seen: false,
            gamepad_neutral_seen: false,
            gamepad_slot: None,
            gamepad_last_reading: None,
            setup_activation_click: false,
            gamepad_wgi_count: 0,
            gamepad_wgi_reading: None,
            gamepad_wgi_error: None,
            gamepad_observer: None,
            wgi_selected: None,
        }
    }
    fn poll_gamepad(&mut self) {
        if self.case != "agent.desktop.gamepad-state" {
            return;
        }
        for slot in 0..4 {
            if self.gamepad_slot.is_some_and(|expected| expected != slot) {
                continue;
            }
            let mut reading = XINPUT_STATE::default();
            if unsafe { XInputGetState(slot, &mut reading) } != 0 {
                continue;
            }
            let g = reading.Gamepad;
            let values = [
                i32::from(g.wButtons.0),
                i32::from(g.sThumbLX),
                i32::from(g.sThumbLY),
                i32::from(g.sThumbRX),
                i32::from(g.sThumbRY),
                i32::from(g.bLeftTrigger),
                i32::from(g.bRightTrigger),
            ];
            self.gamepad_last_reading = Some(values);
            if g.wButtons == XINPUT_GAMEPAD_A
                && (f64::from(g.sThumbLX) / 32767.0 - 0.65).abs() < 0.03
                && (f64::from(g.sThumbLY) / 32767.0 + 0.4).abs() < 0.03
                && (f64::from(g.bRightTrigger) / 255.0 - 0.75).abs() < 0.03
                && g.sThumbRX == 0
                && g.sThumbRY == 0
                && g.bLeftTrigger == 0
            {
                self.gamepad_pattern_seen = true;
                self.gamepad_slot = Some(slot);
                self.gamepad_observer = Some("XInput".into());
            }
            if self.gamepad_slot == Some(slot) && values.iter().all(|value| *value == 0) {
                self.gamepad_neutral_seen = true;
            }
        }
        let gamepads = match Gamepad::Gamepads() {
            Ok(gamepads) => gamepads,
            Err(error) => {
                self.gamepad_wgi_error = Some(error.to_string());
                return;
            }
        };
        self.gamepad_wgi_count = gamepads.Size().unwrap_or_default();
        for index in 0..self.gamepad_wgi_count {
            let Ok(gamepad) = gamepads.GetAt(index) else {
                continue;
            };
            if self
                .wgi_selected
                .as_ref()
                .is_some_and(|selected| selected != &gamepad)
            {
                continue;
            }
            let g = match gamepad.GetCurrentReading() {
                Ok(g) => g,
                Err(error) => {
                    self.gamepad_wgi_error = Some(error.to_string());
                    continue;
                }
            };
            let values = [
                f64::from(g.Buttons.0),
                g.LeftThumbstickX,
                g.LeftThumbstickY,
                g.RightThumbstickX,
                g.RightThumbstickY,
                g.LeftTrigger,
                g.RightTrigger,
            ];
            self.gamepad_wgi_reading = Some(values);
            if g.Buttons == GamepadButtons::A
                && (g.LeftThumbstickX - 0.65).abs() < 0.03
                && (g.LeftThumbstickY + 0.4).abs() < 0.03
                && (g.RightTrigger - 0.75).abs() < 0.03
                && g.RightThumbstickX == 0.0
                && g.RightThumbstickY == 0.0
                && g.LeftTrigger == 0.0
            {
                self.gamepad_pattern_seen = true;
                self.wgi_selected = Some(gamepad.clone());
                self.gamepad_observer = Some("Windows.Gaming.Input".into());
            }
            if self.wgi_selected.as_ref() == Some(&gamepad)
                && values.iter().all(|value| *value == 0.0)
            {
                self.gamepad_neutral_seen = true;
            }
        }
    }
}
pub struct TestWindow {
    pub handle: u64,
    pub foreground_handle: u64,
    state: Arc<Mutex<State>>,
    stop: mpsc::Sender<()>,
    worker: Option<std::thread::JoinHandle<()>>,
}
struct WindowResources {
    windows: Vec<HWND>,
    registered: bool,
    old_dpi: DPI_AWARENESS_CONTEXT,
}
impl Drop for WindowResources {
    fn drop(&mut self) {
        unsafe {
            for window in self.windows.iter().rev() {
                let _ = KillTimer(Some(*window), 1);
                let _ = DestroyWindow(*window);
            }
            if self.registered {
                let _ = UnregisterClassW(w!("SinomoAgentDesktopFixture"), None);
            }
            SetThreadDpiAwarenessContext(self.old_dpi);
            RoUninitialize();
        }
    }
}
impl TestWindow {
    pub fn start(
        case: &str,
        code: &str,
        raw: bool,
        ready_file: &std::path::Path,
        expected_desktop: &str,
        interactive: bool,
    ) -> Result<Self, String> {
        require_test_desktop(expected_desktop, interactive)?;
        let state = Arc::new(Mutex::new(State::new(case, code)));
        let shared = state.clone();
        let (ready, started) = mpsc::sync_channel(1);
        let (stop, stopped) = mpsc::channel();
        let marker = ready_file.to_path_buf();
        let desktop = expected_desktop.to_string();
        let worker = std::thread::spawn(move || {
            let result = (|| -> Result<(), String> {
                unsafe {
                    RoInitialize(RO_INIT_MULTITHREADED).map_err(|e| e.to_string())?;
                    let old_dpi =
                        SetThreadDpiAwarenessContext(DPI_AWARENESS_CONTEXT_PER_MONITOR_AWARE_V2);
                    let mut resources = WindowResources {
                        windows: Vec::new(),
                        registered: false,
                        old_dpi,
                    };
                    let class = WNDCLASSW {
                        lpfnWndProc: Some(window_proc),
                        lpszClassName: w!("SinomoAgentDesktopFixture"),
                        hbrBackground: GetSysColorBrush(COLOR_WINDOW),
                        ..Default::default()
                    };
                    if RegisterClassW(&class) == 0 {
                        return Err("register fixture window class".into());
                    }
                    resources.registered = true;
                    let (style, x, y, width, height) = if raw {
                        (
                            WS_POPUP,
                            0,
                            0,
                            GetSystemMetrics(SM_CXSCREEN),
                            GetSystemMetrics(SM_CYSCREEN),
                        )
                    } else {
                        (WS_OVERLAPPEDWINDOW, 220, 110, 930, 690)
                    };
                    let window = CreateWindowExW(
                        if raw {
                            WS_EX_TOPMOST
                        } else {
                            WINDOW_EX_STYLE(0)
                        },
                        class.lpszClassName,
                        w!("Sinomo agent test - owned fixture"),
                        style,
                        x,
                        y,
                        width,
                        height,
                        None,
                        None,
                        None,
                        Some(Arc::as_ptr(&shared) as *const _),
                    )
                    .map_err(|e| e.to_string())?;
                    resources.windows.push(window);
                    let _ = ShowWindow(window, SW_SHOWNOACTIVATE);
                    let guard = if raw {
                        None
                    } else {
                        Some(
                            CreateWindowExW(
                                WS_EX_TOPMOST,
                                class.lpszClassName,
                                w!("Foreground guard: agent must leave focus and pointer here"),
                                WS_OVERLAPPEDWINDOW,
                                230,
                                120,
                                430,
                                140,
                                None,
                                None,
                                None,
                                None,
                            )
                            .map_err(|e| e.to_string())?,
                        )
                    };
                    if let Some(guard) = guard {
                        resources.windows.push(guard);
                    }
                    std::fs::write(marker, "ready").map_err(|e| e.to_string())?;
                    let deadline = Instant::now() + Duration::from_secs(30);
                    while desktop_name(true).ok().as_deref() != Some(&desktop) {
                        if Instant::now() > deadline
                            || !matches!(stopped.try_recv(), Err(mpsc::TryRecvError::Empty))
                        {
                            return Err("launcher did not activate the test desktop".into());
                        }
                        std::thread::sleep(Duration::from_millis(25));
                    }
                    let foreground = guard.unwrap_or(window);
                    let _ = ShowWindow(foreground, SW_SHOW);
                    let _ = SetForegroundWindow(foreground);
                    if GetForegroundWindow() != foreground {
                        // After a prior case exits, Windows may deny foreground
                        // activation. Establish it with a bounded real click on
                        // this owned, hit-tested window before recording begins.
                        activate_owned_fixture(foreground)?;
                        shared.lock().unwrap().setup_activation_click = true;
                    }
                    SetTimer(Some(window), 1, 100, None);
                    let _ = UpdateWindow(window);
                    let settle = Instant::now() + Duration::from_millis(350);
                    let mut initial_message = MSG::default();
                    while Instant::now() < settle {
                        while PeekMessageW(&mut initial_message, None, 0, 0, PM_REMOVE).as_bool() {
                            let _ = TranslateMessage(&initial_message);
                            DispatchMessageW(&initial_message);
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    ready
                        .send(Ok((window.0 as usize as u64, foreground.0 as usize as u64)))
                        .map_err(|e| e.to_string())?;
                    let mut message = MSG::default();
                    let mut quit = false;
                    while !quit && matches!(stopped.try_recv(), Err(mpsc::TryRecvError::Empty)) {
                        while PeekMessageW(&mut message, None, 0, 0, PM_REMOVE).as_bool() {
                            if message.message == WM_QUIT {
                                quit = true;
                                break;
                            }
                            let _ = TranslateMessage(&message);
                            DispatchMessageW(&message);
                        }
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Ok(())
                }
            })();
            if let Err(error) = result {
                let _ = ready.send(Err(error));
            }
        });
        let result = match started.recv_timeout(Duration::from_secs(40)) {
            Ok(result) => result,
            Err(error) => {
                let _ = stop.send(());
                drop(started);
                let _ = worker.join();
                return Err(error.to_string());
            }
        };
        match result {
            Ok((handle, foreground_handle)) => Ok(Self {
                handle,
                foreground_handle,
                state,
                stop,
                worker: Some(worker),
            }),
            Err(error) => {
                let _ = stop.send(());
                let _ = worker.join();
                Err(error)
            }
        }
    }
    pub fn state(&self) -> State {
        self.state.lock().unwrap().clone()
    }
    pub fn foreground(&self) -> u64 {
        unsafe { GetForegroundWindow().0 as usize as u64 }
    }
    pub fn pointer(&self) -> Result<[i32; 2], String> {
        let mut point = POINT::default();
        unsafe {
            GetCursorPos(&mut point).map_err(|e| e.to_string())?;
        }
        Ok([point.x, point.y])
    }
}
impl Drop for TestWindow {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn activate_owned_fixture(window: HWND) -> Result<(), String> {
    unsafe {
        let mut original = POINT::default();
        GetCursorPos(&mut original).map_err(|e| e.to_string())?;
        let mut target = POINT { x: 8, y: 8 };
        ClientToScreen(window, &mut target)
            .ok()
            .map_err(|e| e.to_string())?;
        if WindowFromPoint(target) != window {
            return Err("owned fixture activation point is occluded; setup stopped".into());
        }
        let left = GetSystemMetrics(SM_XVIRTUALSCREEN);
        let top = GetSystemMetrics(SM_YVIRTUALSCREEN);
        let width = GetSystemMetrics(SM_CXVIRTUALSCREEN);
        let height = GetSystemMetrics(SM_CYVIRTUALSCREEN);
        if width <= 1 || height <= 1 {
            return Err("invalid fixture desktop geometry".into());
        }
        let movement = |p: POINT| INPUT {
            r#type: INPUT_MOUSE,
            Anonymous: INPUT_0 {
                mi: MOUSEINPUT {
                    dx: ((i64::from(p.x - left) * 65535) / i64::from(width - 1)) as i32,
                    dy: ((i64::from(p.y - top) * 65535) / i64::from(height - 1)) as i32,
                    dwFlags: MOUSEEVENTF_MOVE | MOUSEEVENTF_ABSOLUTE | MOUSEEVENTF_VIRTUALDESK,
                    ..Default::default()
                },
            },
        };
        let button = |flags| INPUT {
            r#type: INPUT_MOUSE,
            Anonymous: INPUT_0 {
                mi: MOUSEINPUT {
                    dwFlags: flags,
                    ..Default::default()
                },
            },
        };
        let inputs = [
            movement(target),
            button(MOUSEEVENTF_LEFTDOWN),
            button(MOUSEEVENTF_LEFTUP),
            movement(original),
        ];
        if SendInput(&inputs, std::mem::size_of::<INPUT>() as i32) != inputs.len() as u32 {
            let _ = SendInput(
                &[button(MOUSEEVENTF_LEFTUP), movement(original)],
                std::mem::size_of::<INPUT>() as i32,
            );
            return Err("owned fixture activation input was rejected".into());
        }
        Ok(())
    }
}

fn layout(window: HWND) -> (f64, f64, f64) {
    let mut rect = RECT::default();
    unsafe {
        let _ = GetClientRect(window, &mut rect);
    }
    let scale = (f64::from(rect.right) / 900.0)
        .min(f64::from(rect.bottom) / 600.0)
        .max(0.2);
    (
        (f64::from(rect.right) - 900.0 * scale) / 2.0,
        (f64::from(rect.bottom) - 600.0 * scale) / 2.0,
        scale,
    )
}
fn point(window: HWND, lp: LPARAM) -> [f64; 2] {
    let (x, y, s) = layout(window);
    [
        (f64::from(lp.0 as i16) - x) / s,
        (f64::from((lp.0 >> 16) as i16) - y) / s,
    ]
}
fn inside(p: [f64; 2], rect: [f64; 4]) -> bool {
    p[0] >= rect[0] && p[0] <= rect[2] && p[1] >= rect[1] && p[1] <= rect[3]
}
const FIELD: [f64; 4] = [45., 150., 500., 220.];
const SUBMIT: [f64; 4] = [540., 150., 820., 220.];
const PLUS: [f64; 4] = [45., 260., 270., 330.];
const TOGGLE: [f64; 4] = [320., 260., 560., 330.];
const EXPAND: [f64; 4] = [620., 260., 855., 330.];
const GOAL: [f64; 4] = [550., 430., 820., 555.];
unsafe extern "system" fn window_proc(
    window: HWND,
    message: u32,
    wp: WPARAM,
    lp: LPARAM,
) -> LRESULT {
    if message == WM_NCCREATE {
        let create = unsafe { &*(lp.0 as *const CREATESTRUCTW) };
        unsafe {
            SetWindowLongPtrW(window, GWLP_USERDATA, create.lpCreateParams as isize);
        }
    }
    let pointer = unsafe { GetWindowLongPtrW(window, GWLP_USERDATA) } as *const Mutex<State>;
    if pointer.is_null() {
        return unsafe { DefWindowProcW(window, message, wp, lp) };
    }
    let mut state = unsafe { &*pointer }
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    match message {
        WM_TIMER => {
            state.video_tick += 1;
            state.poll_gamepad();
            unsafe {
                let _ = InvalidateRect(Some(window), None, false);
            }
            return LRESULT(0);
        }
        WM_PAINT => {
            paint(window, &state);
            return LRESULT(0);
        }
        WM_KEYDOWN => {
            state.input_events += 1;
            if wp.0 == 0x11 {
                state.control_down = true;
            }
            if wp.0 == 0x41 && state.control_down {
                state.typed.clear();
            }
        }
        WM_KEYUP => {
            state.input_events += 1;
            if wp.0 == 0x11 {
                state.control_down = false;
            }
        }
        WM_CHAR => {
            state.input_events += 1;
            if state.text_active {
                match wp.0 {
                    8 => {
                        state.typed.pop();
                    }
                    13 => {
                        state.submitted = state.typed.clone();
                    }
                    n if n >= 32 && state.typed.len() < 128 => {
                        if let Some(ch) = char::from_u32(n as u32) {
                            state.typed.push(ch);
                        }
                    }
                    _ => {}
                }
            }
        }
        WM_LBUTTONDOWN => {
            state.input_events += 1;
            let p = point(window, lp);
            state.mouse_down = Some(p);
            state.text_active = inside(p, FIELD);
            state.dragging =
                (p[0] - state.token[0]).abs() < 28. && (p[1] - state.token[1]).abs() < 28.;
        }
        WM_MOUSEMOVE => {
            if state.dragging {
                state.token = point(window, lp);
            }
        }
        WM_LBUTTONUP => {
            state.input_events += 1;
            let p = point(window, lp);
            if state.dragging {
                state.token = p;
                state.dragged = inside(p, GOAL);
                state.dragging = false;
                state.mouse_down = None;
            } else if let Some(start) = state.mouse_down.take() {
                if inside(start, SUBMIT) && inside(p, SUBMIT) {
                    state.submitted = state.typed.clone();
                }
                if inside(start, PLUS) && inside(p, PLUS) {
                    state.counter += 1;
                }
                if inside(start, TOGGLE) && inside(p, TOGGLE) {
                    state.enabled = !state.enabled;
                }
                if inside(start, EXPAND) && inside(p, EXPAND) {
                    state.expanded = true;
                    drop(state);
                    unsafe {
                        let _ = SetWindowPos(
                            window,
                            None,
                            0,
                            0,
                            1150,
                            790,
                            SWP_NOMOVE | SWP_NOZORDER | SWP_NOACTIVATE,
                        );
                    }
                    return LRESULT(0);
                }
            }
        }
        WM_MOUSEWHEEL => {
            state.input_events += 1;
            state.wheel_ticks += ((wp.0 >> 16) as i16 as i32) / 120;
        }
        WM_CLOSE => {
            unsafe {
                PostQuitMessage(0);
            }
            return LRESULT(0);
        }
        _ => {
            drop(state);
            return unsafe { DefWindowProcW(window, message, wp, lp) };
        }
    }
    drop(state);
    unsafe {
        let _ = InvalidateRect(Some(window), None, false);
    }
    LRESULT(0)
}
fn paint(window: HWND, state: &State) {
    unsafe {
        let mut paint = PAINTSTRUCT::default();
        let dc = BeginPaint(window, &mut paint);
        fill(dc, paint.rcPaint, COLORREF(0x21180e));
        let (ox, oy, s) = layout(window);
        let rect = |r: [f64; 4]| RECT {
            left: (ox + r[0] * s) as i32,
            top: (oy + r[1] * s) as i32,
            right: (ox + r[2] * s) as i32,
            bottom: (oy + r[3] * s) as i32,
        };
        let font = CreateFontW(
            (25.0 * s) as i32,
            0,
            0,
            0,
            500,
            0,
            0,
            0,
            DEFAULT_CHARSET,
            OUT_DEFAULT_PRECIS,
            CLIP_DEFAULT_PRECIS,
            CLEARTYPE_QUALITY,
            0,
            w!("Segoe UI"),
        );
        let old = SelectObject(dc, font.into());
        SetBkMode(dc, TRANSPARENT);
        SetTextColor(dc, COLORREF(0xf4f2eb));
        let text = |x: f64, y: f64, value: &str| {
            let wide: Vec<u16> = value.encode_utf16().collect();
            let _ = TextOutW(dc, (ox + x * s) as i32, (oy + y * s) as i32, &wide);
        };
        text(40., 25., &format!("SINOMO  |  {}", state.case));
        text(
            40.,
            82.,
            &format!("Type and submit: {}", state.expected_code),
        );
        fill(
            dc,
            rect(FIELD),
            COLORREF(if state.text_active {
                0x90763b
            } else {
                0x594737
            }),
        );
        text(58., 170., &state.typed);
        for (r, label) in [
            (SUBMIT, "SUBMIT"),
            (PLUS, "+1"),
            (TOGGLE, if state.enabled { "ON" } else { "OFF" }),
            (EXPAND, if state.expanded { "EXPANDED" } else { "EXPAND" }),
        ] {
            fill(dc, rect(r), COLORREF(0x71532e));
            text(r[0] + 18., r[1] + 19., label);
        }
        text(45., 350., &format!("Counter: {}", state.counter));
        text(450., 350., &format!("Scroll ticks: {}", state.wheel_ticks));
        text(45., 390., &format!("Submitted: {}", state.submitted));
        if state.case == "agent.desktop.gamepad-state" {
            text(
                45.,
                430.,
                &format!(
                    "Gamepad pattern: {} | Returned neutral: {}",
                    state.gamepad_pattern_seen, state.gamepad_neutral_seen
                ),
            );
            text(
                45.,
                475.,
                &format!(
                    "XInput: {:?} | WGI pads: {}",
                    state.gamepad_last_reading, state.gamepad_wgi_count
                ),
            );
            text(
                45.,
                520.,
                "Target: A + left stick (0.65, -0.4) + right trigger 0.75",
            );
        } else {
            fill(dc, rect([45., 430., 330., 555.]), COLORREF(0x43382b));
            fill(dc, rect(GOAL), COLORREF(0x405c26));
            text(
                575.,
                450.,
                if state.dragged {
                    "DROP COMPLETE"
                } else {
                    "DROP HERE"
                },
            );
            fill(
                dc,
                rect([
                    state.token[0] - 24.,
                    state.token[1] - 24.,
                    state.token[0] + 24.,
                    state.token[1] + 24.,
                ]),
                COLORREF(0x50c4f5),
            );
        }
        text(
            45.,
            566.,
            &format!(
                "Live video frame {}   |   all controls belong to this test",
                state.video_tick
            ),
        );
        SelectObject(dc, old);
        let _ = DeleteObject(font.into());
        let _ = EndPaint(window, &paint);
    }
}
unsafe fn fill(dc: HDC, rect: RECT, color: COLORREF) {
    let brush = unsafe { CreateSolidBrush(color) };
    unsafe {
        FillRect(dc, &rect, brush);
        let _ = DeleteObject(brush.into());
    }
}
