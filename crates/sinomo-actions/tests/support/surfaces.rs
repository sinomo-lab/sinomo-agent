//! Owned, no-activate Windows application with independent windows and a tray icon.
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
    mpsc,
};
use std::time::Duration;
use windows::Win32::UI::Input::KeyboardAndMouse::EnableWindow;
use windows::Win32::{
    Foundation::*,
    Graphics::Gdi::*,
    UI::{Shell::*, WindowsAndMessaging::*},
};
use windows::core::w;

pub const SPAWN: usize = 1;
pub const MODAL: usize = 2;
pub const CLOSE_SECOND: usize = 3;
pub const CLOSE_MAIN: usize = 4;
pub const TRAY: usize = 5;
pub const STOP: usize = 6;
pub const CLOSE_MODAL: usize = 7;
pub const MENU: usize = 8;
pub const LATE_CREATE: usize = 9;
const COMMAND: u32 = WM_APP + 101;
const TRAY_EVENT: u32 = WM_APP + 102;
pub struct Fixture {
    pub control: u64,
    pub main: u64,
    pub name: String,
    pub log: Arc<Mutex<Vec<(u64, u16)>>>,
    pub activations: Arc<AtomicU64>,
    worker: Option<std::thread::JoinHandle<()>>,
}
struct Ui {
    main: AtomicU64,
    second: AtomicU64,
    modal: AtomicU64,
    control: AtomicU64,
    stopping: AtomicBool,
    tray: AtomicBool,
    guid: windows::core::GUID,
    name: String,
    log: Arc<Mutex<Vec<(u64, u16)>>>,
    activations: Arc<AtomicU64>,
}
fn hwnd(value: u64) -> HWND {
    HWND(value as usize as *mut _)
}
unsafe fn create(ui: &Ui, title: &str, owner: Option<HWND>, x: i32, y: i32) -> HWND {
    let title: Vec<u16> = title.encode_utf16().chain([0]).collect();
    let window = unsafe {
        CreateWindowExW(
            WS_EX_NOACTIVATE | WS_EX_TOOLWINDOW,
            w!("SinomoApplicationSurfaceFixture"),
            windows::core::PCWSTR(title.as_ptr()),
            WS_OVERLAPPEDWINDOW,
            x,
            y,
            480,
            330,
            owner,
            None,
            None,
            Some(ui as *const _ as *const _),
        )
    }
    .unwrap();
    unsafe {
        let _ = ShowWindow(window, SW_SHOWNOACTIVATE);
        let _ = SetWindowPos(
            window,
            Some(HWND_BOTTOM),
            0,
            0,
            0,
            0,
            SWP_NOACTIVATE | SWP_NOMOVE | SWP_NOSIZE,
        );
    }
    window
}
unsafe fn second(ui: &Ui) {
    let old = hwnd(ui.second.load(Ordering::Acquire));
    if unsafe { IsWindow(Some(old)) }.as_bool() {
        return;
    }
    let main = hwnd(ui.main.load(Ordering::Acquire));
    let owner = unsafe { IsWindow(Some(main)) }.as_bool().then_some(main);
    let window = unsafe { create(ui, &format!("{} secondary", ui.name), owner, 720, 190) };
    ui.second.store(window.0 as usize as u64, Ordering::Release);
}
unsafe fn tray(ui: &Ui, command: NOTIFY_ICON_MESSAGE) {
    let mut data = NOTIFYICONDATAW {
        cbSize: std::mem::size_of::<NOTIFYICONDATAW>() as u32,
        hWnd: hwnd(ui.control.load(Ordering::Acquire)),
        uID: 1,
        uFlags: NIF_MESSAGE | NIF_ICON | NIF_TIP | NIF_GUID,
        uCallbackMessage: TRAY_EVENT,
        guidItem: ui.guid,
        hIcon: unsafe { LoadIconW(None, IDI_INFORMATION) }.unwrap(),
        ..Default::default()
    };
    let tip: Vec<u16> = ui.name.encode_utf16().collect();
    data.szTip[..tip.len().min(127)].copy_from_slice(&tip[..tip.len().min(127)]);
    assert!(unsafe { Shell_NotifyIconW(command, &data) }.as_bool());
    if command == NIM_ADD {
        data.Anonymous.uVersion = NOTIFYICON_VERSION_4;
        assert!(unsafe { Shell_NotifyIconW(NIM_SETVERSION, &data) }.as_bool());
    }
}
unsafe extern "system" fn proc(window: HWND, message: u32, wp: WPARAM, lp: LPARAM) -> LRESULT {
    if message == WM_NCCREATE {
        let create = unsafe { &*(lp.0 as *const CREATESTRUCTW) };
        unsafe {
            SetWindowLongPtrW(window, GWLP_USERDATA, create.lpCreateParams as isize);
        }
    }
    let pointer = unsafe { GetWindowLongPtrW(window, GWLP_USERDATA) } as *const Ui;
    if pointer.is_null() {
        return unsafe { DefWindowProcW(window, message, wp, lp) };
    }
    let ui = unsafe { &*pointer };
    if (message == WM_ACTIVATE && wp.0 & 0xffff != WA_INACTIVE as usize) || message == WM_SETFOCUS {
        ui.activations.fetch_add(1, Ordering::Relaxed);
    }
    if message == COMMAND {
        match wp.0 {
            LATE_CREATE => unsafe {
                windows::Win32::UI::Accessibility::NotifyWinEvent(
                    EVENT_OBJECT_CREATE,
                    hwnd(ui.main.load(Ordering::Acquire)),
                    0,
                    0,
                );
            },
            SPAWN => unsafe {
                second(ui);
            },
            MODAL => unsafe {
                let main = hwnd(ui.main.load(Ordering::Acquire));
                let _ = EnableWindow(main, false);
                let modal = create(ui, &format!("{} dialog", ui.name), Some(main), 430, 240);
                ui.modal.store(modal.0 as usize as u64, Ordering::Release);
            },
            CLOSE_SECOND => unsafe {
                let old = hwnd(ui.second.swap(0, Ordering::AcqRel));
                if IsWindow(Some(old)).as_bool() {
                    let _ = DestroyWindow(old);
                }
            },
            CLOSE_MODAL => unsafe {
                let old = hwnd(ui.modal.swap(0, Ordering::AcqRel));
                if IsWindow(Some(old)).as_bool() {
                    let _ = DestroyWindow(old);
                }
                let _ = EnableWindow(hwnd(ui.main.load(Ordering::Acquire)), true);
            },
            CLOSE_MAIN => unsafe {
                let old = hwnd(ui.main.swap(0, Ordering::AcqRel));
                if IsWindow(Some(old)).as_bool() {
                    let _ = DestroyWindow(old);
                }
            },
            TRAY => {
                if !ui.tray.swap(true, Ordering::AcqRel) {
                    unsafe {
                        tray(ui, NIM_ADD);
                    }
                }
            }
            MENU => unsafe {
                let menu = CreatePopupMenu().unwrap();
                AppendMenuW(menu, MF_STRING, 100, w!("Open fixture window")).unwrap();
                let selected = TrackPopupMenu(
                    menu,
                    TPM_RETURNCMD | TPM_NONOTIFY,
                    1000,
                    650,
                    None,
                    window,
                    None,
                );
                if selected.0 == 100 {
                    second(ui);
                }
                let _ = DestroyMenu(menu);
            },
            STOP => {
                ui.stopping.store(true, Ordering::Release);
                unsafe {
                    let _ = EndMenu();
                }
            }
            _ => {}
        }
        return LRESULT(0);
    }
    if message == TRAY_EVENT {
        let event = lp.0 as u32 & 0xffff;
        if event == NIN_SELECT || event == (NIN_SELECT | 1) || event == WM_LBUTTONUP {
            unsafe {
                let _ = PostMessageW(Some(window), COMMAND, WPARAM(MENU), LPARAM(0));
            }
        } else if event == WM_CONTEXTMENU {
            unsafe {
                let menu = CreatePopupMenu().unwrap();
                AppendMenuW(menu, MF_STRING, 100, w!("Open fixture window")).unwrap();
                let selected = TrackPopupMenu(
                    menu,
                    TPM_RETURNCMD | TPM_NONOTIFY,
                    1000,
                    650,
                    None,
                    window,
                    None,
                );
                if selected.0 == 100 {
                    second(ui);
                }
                let _ = DestroyMenu(menu);
            }
        }
        return LRESULT(0);
    }
    if message == WM_CHAR {
        ui.log
            .lock()
            .unwrap()
            .push((window.0 as usize as u64, wp.0 as u16));
        unsafe {
            let _ = InvalidateRect(Some(window), None, false);
        }
        return LRESULT(0);
    }
    if message == WM_PAINT {
        let mut paint = PAINTSTRUCT::default();
        let dc = unsafe { BeginPaint(window, &mut paint) };
        let brush = unsafe { CreateSolidBrush(COLORREF(0x604030)) };
        unsafe {
            FillRect(dc, &paint.rcPaint, brush);
            let _ = DeleteObject(brush.into());
            let text: Vec<u16> = format!("{}  messages: {}", ui.name, ui.log.lock().unwrap().len())
                .encode_utf16()
                .collect();
            SetTextColor(dc, COLORREF(0xffffff));
            SetBkMode(dc, TRANSPARENT);
            let _ = TextOutW(dc, 20, 40, &text);
            let _ = EndPaint(window, &paint);
        }
        return LRESULT(0);
    }
    if message == WM_NCDESTROY {
        unsafe {
            SetWindowLongPtrW(window, GWLP_USERDATA, 0);
        }
    }
    unsafe { DefWindowProcW(window, message, wp, lp) }
}
impl Fixture {
    pub fn new() -> Self {
        let name = format!(
            "Sinomo surfaces {}-{}",
            std::process::id(),
            sinomo_runtime::core::Id::generate()
        );
        let title = name.clone();
        let log = Arc::new(Mutex::new(Vec::new()));
        let output = log.clone();
        let activations = Arc::new(AtomicU64::new(0));
        let activation_output = activations.clone();
        let (ready, receive) = mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || unsafe {
            let class = WNDCLASSW {
                lpfnWndProc: Some(proc),
                lpszClassName: w!("SinomoApplicationSurfaceFixture"),
                ..Default::default()
            };
            assert_ne!(RegisterClassW(&class), 0);
            let ui = Box::new(Ui {
                main: AtomicU64::new(0),
                second: AtomicU64::new(0),
                modal: AtomicU64::new(0),
                control: AtomicU64::new(0),
                stopping: AtomicBool::new(false),
                tray: AtomicBool::new(false),
                guid: windows::core::GUID::from_u128(
                    std::process::id() as u128
                        + (std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_nanos()
                            << 32),
                ),
                name: title,
                log: output,
                activations: activation_output,
            });
            let control = CreateWindowExW(
                WINDOW_EX_STYLE(0),
                class.lpszClassName,
                w!("controller"),
                WINDOW_STYLE(0),
                0,
                0,
                0,
                0,
                Some(HWND_MESSAGE),
                None,
                None,
                Some(&*ui as *const _ as *const _),
            )
            .unwrap();
            ui.control
                .store(control.0 as usize as u64, Ordering::Release);
            let main = create(&ui, &format!("{} main", ui.name), None, 180, 130);
            ui.main.store(main.0 as usize as u64, Ordering::Release);
            ready
                .send((control.0 as usize as u64, main.0 as usize as u64))
                .unwrap();
            let mut message = MSG::default();
            while !ui.stopping.load(Ordering::Acquire) {
                while PeekMessageW(&mut message, None, 0, 0, PM_REMOVE).as_bool() {
                    DispatchMessageW(&message);
                }
                for value in [&ui.main, &ui.second, &ui.modal] {
                    let window = hwnd(value.load(Ordering::Acquire));
                    if IsWindow(Some(window)).as_bool() {
                        let _ = InvalidateRect(Some(window), None, false);
                    }
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            if ui.tray.load(Ordering::Acquire) {
                tray(&ui, NIM_DELETE);
            }
            for value in [&ui.modal, &ui.second, &ui.main] {
                let window = hwnd(value.load(Ordering::Acquire));
                if IsWindow(Some(window)).as_bool() {
                    let _ = DestroyWindow(window);
                }
            }
            DestroyWindow(control).unwrap();
            UnregisterClassW(class.lpszClassName, None).unwrap();
        });
        let (control, main) = receive.recv_timeout(Duration::from_secs(5)).unwrap();
        Self {
            control,
            main,
            name,
            log,
            activations,
            worker: Some(worker),
        }
    }
    pub fn command(&self, command: usize) {
        unsafe {
            PostMessageW(
                Some(hwnd(self.control)),
                COMMAND,
                WPARAM(command),
                LPARAM(0),
            )
            .unwrap();
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.command(STOP);
        let _ = self.worker.take().unwrap().join();
    }
}
