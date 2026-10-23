use super::*;
use sinomo_device::{NativeVideoPlane, NativeVideoSampleFormat};
use std::ffi::c_void;
use std::mem::size_of;
use windows::Foundation::{EventHandler, TypedEventHandler};
use windows::Gaming::Input::{Gamepad, GamepadButtons};
use windows::Graphics::Capture::{
    Direct3D11CaptureFramePool, GraphicsCaptureItem, GraphicsCaptureSession,
    IDirect3D11CaptureFramePoolStatics2, IGraphicsCaptureSessionStatics,
};
use windows::Graphics::DirectX::{Direct3D11::IDirect3DDevice, DirectXPixelFormat};
use windows::UI::Input::Preview::Injection::{InjectedInputGamepadInfo, InputInjector};
use windows::Win32::Foundation::*;
use windows::Win32::Graphics::Direct3D::*;
use windows::Win32::Graphics::Direct3D11::*;
use windows::Win32::Graphics::Dwm::{DWMWA_EXTENDED_FRAME_BOUNDS, DwmGetWindowAttribute};
use windows::Win32::Graphics::Dxgi::IDXGIDevice;
use windows::Win32::Graphics::Gdi::*;
use windows::Win32::System::Threading::*;
use windows::Win32::System::WinRT::Direct3D11::*;
use windows::Win32::System::WinRT::Graphics::Capture::IGraphicsCaptureItemInterop;
use windows::Win32::System::WinRT::{RO_INIT_MULTITHREADED, RoInitialize, RoUninitialize};
use windows::Win32::UI::HiDpi::*;
use windows::Win32::UI::Input::KeyboardAndMouse::*;
use windows::Win32::UI::WindowsAndMessaging::*;
use windows::core::{Interface, factory};

/// An unpackaged Windows host can receive S_OK from injection initialization
/// without a virtual controller becoming available. Do not report input applied
/// until the endpoint promised by InitializeGamepadInjection is observable.
fn create_gamepad_injector() -> DesktopResult<InputInjector> {
    struct Observation(i64);
    impl Drop for Observation {
        fn drop(&mut self) {
            let _ = Gamepad::RemoveGamepadAdded(self.0);
        }
    }
    fn gamepads() -> windows::core::Result<Vec<Gamepad>> {
        let pads = Gamepad::Gamepads()?;
        (0..pads.Size()?).map(|index| pads.GetAt(index)).collect()
    }
    let _observation = Observation(
        Gamepad::GamepadAdded(&EventHandler::<Gamepad>::new(|_, _| Ok(()))).map_err(|e| {
            DesktopError::Unsupported(format!("Windows gamepad discovery unavailable: {e}"))
        })?,
    );
    let before = gamepads().map_err(|e| {
        DesktopError::Unsupported(format!("Windows gamepad discovery unavailable: {e}"))
    })?;
    let injector = InputInjector::TryCreate().map_err(|e| {
        DesktopError::Unsupported(format!(
            "Windows gamepad inputInjectionBrokered unavailable: {e}"
        ))
    })?;
    injector.InitializeGamepadInjection().map_err(|e| {
        DesktopError::Unsupported(format!("Windows gamepad injection unavailable: {e}"))
    })?;
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match gamepads() {
            Ok(pads) if pads.iter().any(|pad| !before.contains(pad)) => return Ok(injector),
            Err(error) => {
                let _ = injector.UninitializeGamepadInjection();
                return Err(DesktopError::Unsupported(format!(
                    "Cannot verify native gamepad endpoint: {error}"
                )));
            }
            _ => {}
        }
        if Instant::now() >= deadline {
            let _ = injector.UninitializeGamepadInjection();
            return Err(DesktopError::Unsupported("Windows accepted gamepad initialization but no virtual controller became visible; this host cannot verify native gamepad support".into()));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn error(e: impl std::fmt::Display) -> DesktopError {
    DesktopError::Platform(e.to_string())
}
fn hwnd(handle: u64) -> HWND {
    HWND(handle as usize as *mut c_void)
}

pub(super) struct Apartment(bool);
impl Apartment {
    fn new() -> DesktopResult<Self> {
        let result = unsafe { RoInitialize(RO_INIT_MULTITHREADED) };
        if result
            .as_ref()
            .is_err_and(|error| error.code() == RPC_E_CHANGED_MODE)
        {
            return Ok(Self(false));
        }
        result.map_err(error)?;
        Ok(Self(true))
    }
}
impl Drop for Apartment {
    fn drop(&mut self) {
        if self.0 {
            unsafe {
                RoUninitialize();
            }
        }
    }
}
struct DpiScope(DPI_AWARENESS_CONTEXT);
impl DpiScope {
    fn new() -> DesktopResult<Self> {
        let previous =
            unsafe { SetThreadDpiAwarenessContext(DPI_AWARENESS_CONTEXT_PER_MONITOR_AWARE_V2) };
        if previous.0.is_null() {
            return Err(error("cannot establish physical-pixel DPI context"));
        }
        Ok(Self(previous))
    }
}
impl Drop for DpiScope {
    fn drop(&mut self) {
        unsafe {
            SetThreadDpiAwarenessContext(self.0);
        }
    }
}
pub(super) struct InputThread {
    _apartment: Apartment,
    _dpi: DpiScope,
}
pub(super) fn initialize_input_thread() -> DesktopResult<InputThread> {
    Ok(InputThread {
        _apartment: Apartment::new()?,
        _dpi: DpiScope::new()?,
    })
}

pub fn capabilities() -> DesktopCapabilities {
    let _apartment = Apartment::new();
    DesktopCapabilities { platform: "windows".into(), capture: capture_supported().unwrap_or(false), keyboard_mouse: true, background_window_input: true,
        gamepad: "conditional: Windows InputInjector requires inputInjectionBrokered; availability is checked on first gamepad input".into(),
        limitations: vec!["Input is subject to UIPI, interactive-session and secure-desktop boundaries.".into(), "Window messages are best effort: raw-input/DirectInput apps, focus checks and held modifier polling may ignore them.".into(), "Occluded windows can be captured; minimized, protected or suspended windows may stop producing frames. Capture never restores or focuses a window.".into()] }
}

fn window_target(window: HWND) -> DesktopResult<DesktopTarget> {
    unsafe {
        if window.0.is_null() || window.0 as usize == 0xffff || !IsWindow(Some(window)).as_bool() {
            return Err(DesktopError::TargetUnavailable("invalid window".into()));
        }
        let mut process_id = 0;
        let thread_id = GetWindowThreadProcessId(window, Some(&mut process_id));
        let process =
            OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, false, process_id).map_err(error)?;
        let (mut created, mut exit, mut kernel, mut user) = (
            FILETIME::default(),
            FILETIME::default(),
            FILETIME::default(),
            FILETIME::default(),
        );
        let result = GetProcessTimes(process, &mut created, &mut exit, &mut kernel, &mut user);
        let _ = CloseHandle(process);
        result.map_err(error)?;
        Ok(DesktopTarget::Window {
            handle: window.0 as usize as u64,
            process_id,
            thread_id,
            process_started: ((created.dwHighDateTime as u64) << 32) | created.dwLowDateTime as u64,
        })
    }
}

unsafe extern "system" fn collect_monitor(
    monitor: HMONITOR,
    _: HDC,
    _: *mut RECT,
    data: LPARAM,
) -> windows::core::BOOL {
    let results = unsafe { &mut *(data.0 as *mut Vec<(HMONITOR, DesktopTargetInfo)>) };
    if results.len() >= MAX_TARGETS {
        return true.into();
    }
    let mut info = MONITORINFOEXW::default();
    info.monitorInfo.cbSize = size_of::<MONITORINFOEXW>() as u32;
    if unsafe { GetMonitorInfoW(monitor, &mut info.monitorInfo) }.as_bool() {
        let name = String::from_utf16_lossy(
            &info.szDevice[..info
                .szDevice
                .iter()
                .position(|c| *c == 0)
                .unwrap_or(info.szDevice.len())],
        );
        let target = DesktopTarget::Monitor {
            device_name: name.clone(),
        };
        let r = info.monitorInfo.rcMonitor;
        results.push((
            monitor,
            DesktopTargetInfo {
                resource_id: target.resource_id(),
                target,
                title: name,
                bounds: [r.left, r.top, r.right, r.bottom],
                client_bounds: None,
                minimized: false,
            },
        ));
    }
    true.into()
}
fn monitors() -> DesktopResult<Vec<(HMONITOR, DesktopTargetInfo)>> {
    let mut result = Vec::new();
    unsafe {
        EnumDisplayMonitors(
            None,
            None,
            Some(collect_monitor),
            LPARAM(&mut result as *mut _ as isize),
        )
        .ok()
        .map_err(error)?;
    }
    Ok(result)
}
fn monitor_handle(name: &str) -> DesktopResult<HMONITOR> {
    monitors()?
        .into_iter()
        .find_map(|(h, item)| (item.title == name).then_some(h))
        .ok_or_else(|| DesktopError::TargetUnavailable("monitor disconnected".into()))
}
unsafe extern "system" fn collect_window(window: HWND, data: LPARAM) -> windows::core::BOOL {
    let results = unsafe { &mut *(data.0 as *mut Vec<DesktopTargetInfo>) };
    if results.len() >= MAX_TARGETS {
        return true.into();
    }
    if let Ok(target) = window_target(window) {
        let mut text = [0u16; 1024];
        let length = unsafe { GetWindowTextW(window, &mut text) }.max(0) as usize;
        let mut rect = RECT::default();
        if unsafe { GetWindowRect(window, &mut rect) }.is_ok() {
            // WGC includes visible non-client chrome, but not invisible resize
            // borders included by GetWindowRect. DWM reports that exact frame.
            unsafe {
                let _ = DwmGetWindowAttribute(
                    window,
                    DWMWA_EXTENDED_FRAME_BOUNDS,
                    &mut rect as *mut _ as *mut c_void,
                    size_of::<RECT>() as u32,
                );
            }
            let mut client = RECT::default();
            let mut origin = POINT::default();
            let client_bounds = if unsafe { GetClientRect(window, &mut client) }.is_ok()
                && unsafe { ClientToScreen(window, &mut origin) }.as_bool()
            {
                Some([
                    origin.x,
                    origin.y,
                    origin.x + client.right - client.left,
                    origin.y + client.bottom - client.top,
                ])
            } else {
                None
            };
            results.push(DesktopTargetInfo {
                resource_id: target.resource_id(),
                target,
                title: String::from_utf16_lossy(&text[..length]),
                bounds: [rect.left, rect.top, rect.right, rect.bottom],
                client_bounds,
                minimized: unsafe { IsIconic(window) }.as_bool(),
            });
        }
    }
    true.into()
}
pub fn enumerate_targets(windows: bool) -> DesktopResult<Vec<DesktopTargetInfo>> {
    let _dpi = DpiScope::new()?;
    if !windows {
        return Ok(monitors()?.into_iter().map(|(_, item)| item).collect());
    }
    let mut result = Vec::new();
    unsafe {
        EnumWindows(Some(collect_window), LPARAM(&mut result as *mut _ as isize)).map_err(error)?;
    }
    Ok(result)
}
pub fn enumerate_child_windows(parent: &DesktopTarget) -> DesktopResult<Vec<DesktopTargetInfo>> {
    let _dpi = DpiScope::new()?;
    let DesktopTarget::Window { handle, .. } = parent else {
        return Err(DesktopError::Invalid(
            "child enumeration requires a window".into(),
        ));
    };
    let mut result = Vec::new();
    // EnumChildWindows does not report a meaningful last-error on an empty list.
    unsafe {
        let _ = EnumChildWindows(
            Some(hwnd(*handle)),
            Some(collect_window),
            LPARAM(&mut result as *mut _ as isize),
        );
    }
    Ok(result)
}
pub fn validate_target(target: &DesktopTarget) -> DesktopResult<()> {
    match target {
        DesktopTarget::Monitor { device_name } => {
            monitor_handle(device_name)?;
        }
        DesktopTarget::Window { handle, .. } => {
            if &window_target(hwnd(*handle))? != target {
                return Err(DesktopError::TargetUnavailable(
                    "window/process identity changed; enumerate again".into(),
                ));
            }
        }
    }
    Ok(())
}

pub struct Input {
    target: DesktopTarget,
    position: (i32, i32),
    mouse_flags: u16,
    keys: BTreeSet<u16>,
    gamepad: Option<InputInjector>,
}
impl Input {
    pub fn new(target: &DesktopTarget) -> DesktopResult<Self> {
        Ok(Self {
            target: target.clone(),
            position: (0, 0),
            mouse_flags: 0,
            keys: BTreeSet::new(),
            gamepad: None,
        })
    }
    pub fn send(&mut self, event: &DesktopInput) -> DesktopResult<()> {
        if let DesktopTarget::Window {
            handle, thread_id, ..
        } = self.target
        {
            return self.window_input(hwnd(handle), thread_id, event);
        }
        match event {
            DesktopInput::Key {
                code,
                down,
                extended,
            } => send_input(&[key_input(*code, *down, *extended, false)]),
            DesktopInput::Text { text } => {
                let events: Vec<_> = text
                    .encode_utf16()
                    .flat_map(|c| {
                        [
                            key_input(c, true, false, true),
                            key_input(c, false, false, true),
                        ]
                    })
                    .collect();
                send_input(&events)
            }
            DesktopInput::Move { x, y } => unsafe {
                let (left, top, width, height) = (
                    GetSystemMetrics(SM_XVIRTUALSCREEN),
                    GetSystemMetrics(SM_YVIRTUALSCREEN),
                    GetSystemMetrics(SM_CXVIRTUALSCREEN),
                    GetSystemMetrics(SM_CYVIRTUALSCREEN),
                );
                if width <= 1
                    || height <= 1
                    || *x < left
                    || *y < top
                    || i64::from(*x) >= i64::from(left) + i64::from(width)
                    || i64::from(*y) >= i64::from(top) + i64::from(height)
                {
                    return Err(DesktopError::Invalid(
                        "pointer is outside the virtual desktop".into(),
                    ));
                }
                let dx = ((i64::from(*x) - i64::from(left)) * 65535 / i64::from(width - 1)) as i32;
                let dy = ((i64::from(*y) - i64::from(top)) * 65535 / i64::from(height - 1)) as i32;
                send_input(&[mouse_input(
                    dx,
                    dy,
                    0,
                    MOUSEEVENTF_MOVE | MOUSEEVENTF_ABSOLUTE | MOUSEEVENTF_VIRTUALDESK,
                )])
            },
            DesktopInput::RelativeMove { dx, dy } => {
                send_input(&[mouse_input(*dx, *dy, 0, MOUSEEVENTF_MOVE)])
            }
            DesktopInput::MoveInFrame { .. } => Err(DesktopError::Unsupported(
                "frame-relative input requires an app window".into(),
            )),
            DesktopInput::Button { button, down } => {
                let (flags, data) = match (button, down) {
                    (MouseButton::Left, true) => (MOUSEEVENTF_LEFTDOWN, 0),
                    (MouseButton::Left, false) => (MOUSEEVENTF_LEFTUP, 0),
                    (MouseButton::Right, true) => (MOUSEEVENTF_RIGHTDOWN, 0),
                    (MouseButton::Right, false) => (MOUSEEVENTF_RIGHTUP, 0),
                    (MouseButton::Middle, true) => (MOUSEEVENTF_MIDDLEDOWN, 0),
                    (MouseButton::Middle, false) => (MOUSEEVENTF_MIDDLEUP, 0),
                    (MouseButton::X1, down) => (
                        if *down {
                            MOUSEEVENTF_XDOWN
                        } else {
                            MOUSEEVENTF_XUP
                        },
                        1,
                    ),
                    (MouseButton::X2, down) => (
                        if *down {
                            MOUSEEVENTF_XDOWN
                        } else {
                            MOUSEEVENTF_XUP
                        },
                        2,
                    ),
                };
                send_input(&[mouse_input(0, 0, data, flags)])
            }
            DesktopInput::Wheel { delta, horizontal } => send_input(&[mouse_input(
                0,
                0,
                i32::from(*delta) as u32,
                if *horizontal {
                    MOUSEEVENTF_HWHEEL
                } else {
                    MOUSEEVENTF_WHEEL
                },
            )]),
            DesktopInput::Gamepad {
                buttons,
                left_x,
                left_y,
                right_x,
                right_y,
                left_trigger,
                right_trigger,
            } => {
                if self.gamepad.is_none() {
                    self.gamepad = Some(create_gamepad_injector()?);
                }
                let info = InjectedInputGamepadInfo::new().map_err(error)?;
                info.SetButtons(GamepadButtons(*buttons)).map_err(error)?;
                info.SetLeftThumbstickX(*left_x).map_err(error)?;
                info.SetLeftThumbstickY(*left_y).map_err(error)?;
                info.SetRightThumbstickX(*right_x).map_err(error)?;
                info.SetRightThumbstickY(*right_y).map_err(error)?;
                info.SetLeftTrigger(*left_trigger).map_err(error)?;
                info.SetRightTrigger(*right_trigger).map_err(error)?;
                self.gamepad
                    .as_ref()
                    .unwrap()
                    .InjectGamepadInput(&info)
                    .map_err(error)
            }
            DesktopInput::ReleaseAll => Ok(()),
        }
    }
    pub fn release_gamepad(&mut self) {
        if let Some(injector) = self.gamepad.take() {
            if let Ok(neutral) = InjectedInputGamepadInfo::new() {
                let _ = injector.InjectGamepadInput(&neutral);
            }
            let _ = injector.UninitializeGamepadInjection();
        }
    }
    fn window_input(
        &mut self,
        window: HWND,
        thread_id: u32,
        event: &DesktopInput,
    ) -> DesktopResult<()> {
        // Never activate a window, attach input queues, alter global key state,
        // move the user's pointer or fall back to SendInput on this path.
        let post = |message, wp, lp| unsafe {
            PostMessageW(Some(window), message, WPARAM(wp), LPARAM(lp)).map_err(error)
        };
        match event {
            DesktopInput::Key {
                code,
                down,
                extended,
            } => {
                let was_down = self.keys.contains(code);
                let alt = self.keys.contains(&VK_MENU.0) || *code == VK_MENU.0;
                let scan = unsafe {
                    MapVirtualKeyExW(
                        u32::from(*code),
                        MAPVK_VK_TO_VSC,
                        Some(GetKeyboardLayout(thread_id)),
                    )
                } & 0xff;
                let bits = 1u32
                    | (scan << 16)
                    | (u32::from(*extended) << 24)
                    | (u32::from(alt) << 29)
                    | (u32::from(was_down || !down) << 30)
                    | (u32::from(!down) << 31);
                post(
                    match (alt, down) {
                        (true, true) => WM_SYSKEYDOWN,
                        (true, false) => WM_SYSKEYUP,
                        (false, true) => WM_KEYDOWN,
                        (false, false) => WM_KEYUP,
                    },
                    usize::from(*code),
                    bits as isize,
                )?;
                if *down {
                    self.keys.insert(*code);
                } else {
                    self.keys.remove(code);
                }
                Ok(())
            }
            DesktopInput::Text { text } => {
                for unit in text.encode_utf16() {
                    post(WM_CHAR, usize::from(unit), 1)?;
                }
                Ok(())
            }
            DesktopInput::Move { x, y } => {
                // This worker posts physical pixels from a per-monitor-aware
                // context. Windows translates these mouse messages for a
                // DPI-unaware receiver; pre-scaling would translate twice.
                post(
                    WM_MOUSEMOVE,
                    self.window_mouse_flags() as usize,
                    packed_position(*x, *y),
                )?;
                self.position = (*x, *y);
                Ok(())
            }
            DesktopInput::MoveInFrame {
                x,
                y,
                source_width,
                source_height,
            } => {
                let mut bounds = RECT::default();
                let mut client = RECT::default();
                let mut origin = POINT::default();
                unsafe {
                    DwmGetWindowAttribute(
                        window,
                        DWMWA_EXTENDED_FRAME_BOUNDS,
                        &mut bounds as *mut _ as *mut c_void,
                        size_of::<RECT>() as u32,
                    )
                    .map_err(error)?;
                    GetClientRect(window, &mut client).map_err(error)?;
                    ClientToScreen(window, &mut origin).ok().map_err(error)?;
                }
                let (x, y) = frame_point_to_client(
                    *x,
                    *y,
                    [*source_width, *source_height],
                    [bounds.left, bounds.top, bounds.right, bounds.bottom],
                    [
                        origin.x,
                        origin.y,
                        origin.x + client.right - client.left,
                        origin.y + client.bottom - client.top,
                    ],
                )?;
                self.window_input(window, thread_id, &DesktopInput::Move { x, y })
            }
            DesktopInput::Button { button, down } => {
                let (flag, pressed, released, xbutton) = match button {
                    MouseButton::Left => (1, WM_LBUTTONDOWN, WM_LBUTTONUP, 0),
                    MouseButton::Right => (2, WM_RBUTTONDOWN, WM_RBUTTONUP, 0),
                    MouseButton::Middle => (16, WM_MBUTTONDOWN, WM_MBUTTONUP, 0),
                    MouseButton::X1 => (32, WM_XBUTTONDOWN, WM_XBUTTONUP, 1),
                    MouseButton::X2 => (64, WM_XBUTTONDOWN, WM_XBUTTONUP, 2),
                };
                let flags = if *down {
                    self.mouse_flags | flag
                } else {
                    self.mouse_flags & !flag
                };
                let modifiers = self.window_mouse_flags() & !self.mouse_flags;
                post(
                    if *down { pressed } else { released },
                    usize::from(flags | modifiers) | (xbutton << 16),
                    packed_position(self.position.0, self.position.1),
                )?;
                self.mouse_flags = flags;
                Ok(())
            }
            DesktopInput::Wheel { delta, horizontal } => {
                let mut point = POINT {
                    x: self.position.0,
                    y: self.position.1,
                };
                unsafe {
                    ClientToScreen(window, &mut point).ok().map_err(error)?;
                }
                if i16::try_from(point.x).is_err() || i16::try_from(point.y).is_err() {
                    return Err(DesktopError::Invalid(
                        "wheel screen coordinates overflow window message".into(),
                    ));
                }
                post(
                    if *horizontal {
                        WM_MOUSEHWHEEL
                    } else {
                        WM_MOUSEWHEEL
                    },
                    usize::from(self.window_mouse_flags()) | ((*delta as u16 as usize) << 16),
                    packed_position(point.x, point.y),
                )
            }
            DesktopInput::ReleaseAll => Ok(()),
            _ => Err(DesktopError::Unsupported(
                "hardware events cannot target a background window".into(),
            )),
        }
    }
    fn window_mouse_flags(&self) -> u16 {
        self.mouse_flags
            | if self.keys.contains(&VK_SHIFT.0) {
                4
            } else {
                0
            }
            | if self.keys.contains(&VK_CONTROL.0) {
                8
            } else {
                0
            }
    }
}
impl Drop for Input {
    fn drop(&mut self) {
        self.release_gamepad();
    }
}
fn packed_position(x: i32, y: i32) -> isize {
    ((x as u16 as u32) | ((y as u16 as u32) << 16)) as isize
}

fn key_input(code: u16, down: bool, extended: bool, unicode: bool) -> INPUT {
    let mut flags = if unicode {
        KEYEVENTF_UNICODE
    } else {
        KEYEVENTF_SCANCODE
    };
    if !down {
        flags |= KEYEVENTF_KEYUP;
    }
    if extended {
        flags |= KEYEVENTF_EXTENDEDKEY;
    }
    INPUT {
        r#type: INPUT_KEYBOARD,
        Anonymous: INPUT_0 {
            ki: KEYBDINPUT {
                wScan: code,
                dwFlags: flags,
                ..Default::default()
            },
        },
    }
}
fn mouse_input(dx: i32, dy: i32, data: u32, flags: MOUSE_EVENT_FLAGS) -> INPUT {
    INPUT {
        r#type: INPUT_MOUSE,
        Anonymous: INPUT_0 {
            mi: MOUSEINPUT {
                dx,
                dy,
                mouseData: data,
                dwFlags: flags,
                ..Default::default()
            },
        },
    }
}
fn send_input(events: &[INPUT]) -> DesktopResult<()> {
    let count = unsafe { SendInput(events, size_of::<INPUT>() as i32) };
    if count as usize != events.len() {
        // Unicode text is submitted as down/up pairs. A partially accepted
        // batch can end on a down event; compensate that exact accepted key.
        if let Some(last) = events
            .get(count.saturating_sub(1) as usize)
            .filter(|_| count != 0)
            && last.r#type == INPUT_KEYBOARD
        {
            let mut key = unsafe { last.Anonymous.ki };
            if !key.dwFlags.contains(KEYEVENTF_KEYUP) {
                key.dwFlags |= KEYEVENTF_KEYUP;
                let release = INPUT {
                    r#type: INPUT_KEYBOARD,
                    Anonymous: INPUT_0 { ki: key },
                };
                unsafe {
                    SendInput(&[release], size_of::<INPUT>() as i32);
                }
            }
        }
        return Err(DesktopError::Platform(format!(
            "SendInput inserted {count}/{} records (UIPI may block injection)",
            events.len()
        )));
    }
    Ok(())
}

pub fn capture(
    config: DesktopCaptureConfig,
    permissions: LocalNodePermissionStore,
    output: &CaptureMailbox,
    stop: mpsc::Receiver<()>,
    ready: mpsc::SyncSender<DesktopResult<()>>,
) -> DesktopResult<()> {
    let _apartment = Apartment::new()?;
    let mut capture = match Capture::new(&config) {
        Ok(capture) => capture,
        Err(error) => {
            let _ = ready.send(Err(error.clone()));
            return Err(error);
        }
    };
    let _ = ready.send(Ok(()));
    let interval = Duration::from_nanos(1_000_000_000 / u64::from(config.frames_per_second));
    let mut next_frame = Instant::now();
    let mut pending_frame = false;
    let mut sequence = 0;
    let deadline = Instant::now() + Duration::from_millis(config.lease_ms.min(300_000));
    loop {
        if Instant::now() >= deadline {
            break;
        }
        match stop.try_recv() {
            Ok(()) | Err(mpsc::TryRecvError::Disconnected) => break,
            Err(mpsc::TryRecvError::Empty) => {}
        }
        authorize(&permissions, &config.target, "capture")?;
        validate_target(&config.target)?;
        if pending_frame && Instant::now() >= next_frame {
            pending_frame = false;
            if let Some(mut sample) = capture.next_sample()? {
                sequence += 1;
                sample.sequence = sequence;
                sample.duration_ns = interval.as_nanos() as u64;
                sample.captured_at_unix_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis();
                output.publish(sample);
                next_frame = Instant::now() + interval;
            }
        }
        let timeout = if pending_frame {
            next_frame
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(100))
        } else {
            Duration::from_millis(100)
        };
        match capture.arrivals.recv_timeout(timeout) {
            Ok(()) => pending_frame = true,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(DesktopError::Closed("frame callback disconnected".into()));
            }
        }
    }
    Ok(())
}

struct Capture {
    pool: Direct3D11CaptureFramePool,
    session: GraphicsCaptureSession,
    device: ID3D11Device,
    context: ID3D11DeviceContext,
    winrt_device: IDirect3DDevice,
    size: windows::Graphics::SizeInt32,
    arrivals: mpsc::Receiver<()>,
    token: i64,
    staging: Option<ID3D11Texture2D>,
    first_timestamp: Option<i64>,
    item: GraphicsCaptureItem,
    include_cursor: bool,
}

// The generated static methods keep process-global factory pointers. WGC can
// invalidate those when the last WinRT apartment shuts down, crashing the next
// capture in a small host. Acquire scoped factories so they are released before
// our apartment, rather than retaining COM state across module lifetimes.
fn capture_supported() -> windows::core::Result<bool> {
    let factory = factory::<GraphicsCaptureSession, IGraphicsCaptureSessionStatics>()?;
    let mut supported = false;
    unsafe {
        (factory.vtable().IsSupported)(factory.as_raw(), &mut supported).ok()?;
    }
    Ok(supported)
}

fn create_frame_pool(
    device: &IDirect3DDevice,
    size: windows::Graphics::SizeInt32,
) -> windows::core::Result<Direct3D11CaptureFramePool> {
    let factory = factory::<Direct3D11CaptureFramePool, IDirect3D11CaptureFramePoolStatics2>()?;
    let mut pool = std::ptr::null_mut();
    unsafe {
        (factory.vtable().CreateFreeThreaded)(
            factory.as_raw(),
            device.as_raw(),
            DirectXPixelFormat::B8G8R8A8UIntNormalized,
            2,
            size,
            &mut pool,
        )
        .ok()?;
        if pool.is_null() {
            Err(windows::core::Error::empty())
        } else {
            Ok(Direct3D11CaptureFramePool::from_raw(pool))
        }
    }
}
struct CapturedFrame(windows::Graphics::Capture::Direct3D11CaptureFrame);
impl std::ops::Deref for CapturedFrame {
    type Target = windows::Graphics::Capture::Direct3D11CaptureFrame;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl Drop for CapturedFrame {
    fn drop(&mut self) {
        let _ = self.0.Close();
    }
}
impl Capture {
    fn new(config: &DesktopCaptureConfig) -> DesktopResult<Self> {
        if !capture_supported().map_err(error)? {
            return Err(DesktopError::Unsupported(
                "Windows Graphics Capture is unavailable".into(),
            ));
        }
        let interop =
            factory::<GraphicsCaptureItem, IGraphicsCaptureItemInterop>().map_err(error)?;
        let item: GraphicsCaptureItem = match &config.target {
            DesktopTarget::Window { handle, .. } => unsafe {
                interop.CreateForWindow(hwnd(*handle))
            },
            DesktopTarget::Monitor { device_name } => unsafe {
                interop.CreateForMonitor(monitor_handle(device_name)?)
            },
        }
        .map_err(error)?;
        let (mut device, mut context) = (None, None);
        unsafe {
            D3D11CreateDevice(
                None,
                D3D_DRIVER_TYPE_HARDWARE,
                HMODULE::default(),
                D3D11_CREATE_DEVICE_BGRA_SUPPORT,
                None,
                D3D11_SDK_VERSION,
                Some(&mut device),
                None,
                Some(&mut context),
            )
            .map_err(error)?;
        }
        let device = device.ok_or_else(|| error("no D3D11 device"))?;
        let context = context.ok_or_else(|| error("no D3D11 context"))?;
        let dxgi: IDXGIDevice = device.cast().map_err(error)?;
        let winrt_device: IDirect3DDevice = unsafe { CreateDirect3D11DeviceFromDXGIDevice(&dxgi) }
            .map_err(error)?
            .cast()
            .map_err(error)?;
        let size = item.Size().map_err(error)?;
        validate_size(size.Width, size.Height)?;
        let pool = create_frame_pool(&winrt_device, size).map_err(error)?;
        let session = pool.CreateCaptureSession(&item).map_err(error)?;
        session
            .SetIsCursorCaptureEnabled(config.include_cursor)
            .map_err(error)?;
        let (notify, arrivals) = mpsc::sync_channel(1);
        let token = pool
            .FrameArrived(&TypedEventHandler::new(move |_, _| {
                let _ = notify.try_send(());
                Ok(())
            }))
            .map_err(error)?;
        let capture = Self {
            pool,
            session,
            device,
            context,
            winrt_device,
            size,
            arrivals,
            token,
            staging: None,
            first_timestamp: None,
            item,
            include_cursor: config.include_cursor,
        };
        capture.session.StartCapture().map_err(error)?;
        Ok(capture)
    }
    fn next_sample(&mut self) -> DesktopResult<Option<NativeVideoSample>> {
        let mut latest = None::<CapturedFrame>;
        // Drain the two-buffer pool to the newest frame at our delivery edge.
        // Never consume-and-discard the last change merely because it arrived
        // before the cadence deadline: a static window may not emit another.
        for _ in 0..2 {
            match self.pool.TryGetNextFrame() {
                Ok(frame) => {
                    drop(latest.replace(CapturedFrame(frame)));
                }
                // WinRT returns a null frame when a coalesced notification has no
                // remaining buffer. windows-rs represents this successful null
                // as Error::empty (S_OK); older projections use E_POINTER.
                Err(error) if error.code().is_ok() || error.code() == E_POINTER => break,
                Err(source) => return Err(error(source)),
            }
        }
        let Some(frame) = latest else {
            return Ok(None);
        };
        let native_timestamp = frame.SystemRelativeTime().map_err(error)?.Duration;
        let first = *self.first_timestamp.get_or_insert(native_timestamp);
        let timestamp_ns = native_timestamp.saturating_sub(first).max(0) as u64 * 100;
        let size = frame.ContentSize().map_err(error)?;
        if size.Width <= 0 || size.Height <= 0 {
            return Ok(None);
        }
        validate_size(size.Width, size.Height)?;
        if size != self.size {
            drop(frame);
            self.resize(size)?;
            return Ok(None);
        }
        let access: IDirect3DDxgiInterfaceAccess =
            frame.Surface().map_err(error)?.cast().map_err(error)?;
        let texture: ID3D11Texture2D = unsafe { access.GetInterface() }.map_err(error)?;
        if self.staging.is_none() {
            let mut desc = D3D11_TEXTURE2D_DESC::default();
            unsafe {
                texture.GetDesc(&mut desc);
            }
            validate_size(desc.Width as i32, desc.Height as i32)?;
            desc.Usage = D3D11_USAGE_STAGING;
            desc.BindFlags = 0;
            desc.CPUAccessFlags = D3D11_CPU_ACCESS_READ.0 as u32;
            desc.MiscFlags = 0;
            unsafe {
                self.device
                    .CreateTexture2D(&desc, None, Some(&mut self.staging))
                    .map_err(error)?;
            }
        }
        let staging = self
            .staging
            .as_ref()
            .ok_or_else(|| error("missing staging texture"))?;
        unsafe {
            self.context.CopyResource(staging, &texture);
        }
        let mut mapped = D3D11_MAPPED_SUBRESOURCE::default();
        unsafe {
            self.context
                .Map(staging, 0, D3D11_MAP_READ, 0, Some(&mut mapped))
                .map_err(error)?;
        }
        let (width, height) = (size.Width as u32, size.Height as u32);
        let stride = width * 4;
        let mut bytes = vec![0; (stride * height) as usize];
        if mapped.RowPitch < stride || mapped.pData.is_null() {
            unsafe {
                self.context.Unmap(staging, 0);
            }
            return Err(error("invalid D3D11 mapped texture"));
        }
        for row in 0..height as usize {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    (mapped.pData as *const u8).add(row * mapped.RowPitch as usize),
                    bytes.as_mut_ptr().add(row * stride as usize),
                    stride as usize,
                );
            }
        }
        unsafe {
            self.context.Unmap(staging, 0);
        }
        drop(frame);
        let len = bytes.len();
        Ok(Some(NativeVideoSample {
            sequence: 0,
            format: NativeVideoSampleFormat::Bgra8,
            width,
            height,
            timestamp_ns,
            duration_ns: 0,
            captured_at_unix_ms: 0,
            data: bytes.into(),
            planes: Arc::from([NativeVideoPlane {
                offset: 0,
                len,
                width,
                height,
                stride,
            }]),
        }))
    }
    fn resize(&mut self, size: windows::Graphics::SizeInt32) -> DesktopResult<()> {
        // Recreate alone can discard the resize frame and then wait forever
        // for a window that has stopped changing. A fresh capture session asks
        // the compositor for a complete initial frame at the new size. Reuse
        // the D3D device/context, retain the native PTS origin, and never focus
        // or repaint the target application.
        let pool = create_frame_pool(&self.winrt_device, size).map_err(error)?;
        let session = pool.CreateCaptureSession(&self.item).map_err(error)?;
        session
            .SetIsCursorCaptureEnabled(self.include_cursor)
            .map_err(error)?;
        let (notify, arrivals) = mpsc::sync_channel(1);
        let token = pool
            .FrameArrived(&TypedEventHandler::new(move |_, _| {
                let _ = notify.try_send(());
                Ok(())
            }))
            .map_err(error)?;
        let mut replacement = Self {
            pool,
            session,
            device: self.device.clone(),
            context: self.context.clone(),
            winrt_device: self.winrt_device.clone(),
            size,
            arrivals,
            token,
            staging: None,
            first_timestamp: self.first_timestamp,
            item: self.item.clone(),
            include_cursor: self.include_cursor,
        };
        replacement.session.StartCapture().map_err(error)?;
        std::mem::swap(self, &mut replacement);
        // Drop closes the old pool/session and disconnects its callback before
        // the owning worker consumes the replacement's first frame.
        Ok(())
    }
}
impl Drop for Capture {
    fn drop(&mut self) {
        let _ = self.pool.RemoveFrameArrived(self.token);
        let _ = self.session.Close();
        let _ = self.pool.Close();
    }
}
fn validate_size(width: i32, height: i32) -> DesktopResult<()> {
    if width <= 0
        || height <= 0
        || (width as usize)
            .checked_mul(height as usize)
            .and_then(|pixels| pixels.checked_mul(4))
            .is_none_or(|bytes| bytes > MAX_FRAME_BYTES)
    {
        return Err(DesktopError::Invalid(format!(
            "capture size {width}x{height} is unavailable or exceeds the 128 MiB frame limit"
        )));
    }
    Ok(())
}
