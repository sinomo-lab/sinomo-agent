//! Windows application identity, owned-window discovery and lifetime invalidation.
//! WinEvent callbacks only mark observations dirty; effects revalidate identity.
use super::*;
use sinomo_device::desktop::surfaces::*;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, Weak};
use windows::Win32::System::Diagnostics::ToolHelp::*;
use windows::Win32::System::RemoteDesktop::ProcessIdToSessionId;
use windows::Win32::UI::Accessibility::*;
#[path = "tray.rs"]
mod tray;
pub(crate) use tray::{invoke_menu_item, invoke_tray, menu_items, tray_items};

pub fn surface_capabilities() -> DesktopSurfaceCapabilities {
    DesktopSurfaceCapabilities { application_sessions: true, window_events: true, tray_items: tray::available(), menu_items: tray::available(),
        limitations: vec!["Tray discovery includes visible UI Automation elements only; overflow is never opened automatically.".into(), "Semantic invocation may activate app or shell UI and requires explicit desktop interaction consent.".into()] }
}
pub fn shutdown_surfaces() -> bool {
    tray::shutdown()
}

pub(super) fn process_info(pid: u32) -> DesktopResult<DesktopApplicationInfo> {
    unsafe {
        let process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, false, pid).map_err(error)?;
        let result = (|| {
            let (mut created, mut exit, mut kernel, mut user) = (
                FILETIME::default(),
                FILETIME::default(),
                FILETIME::default(),
                FILETIME::default(),
            );
            GetProcessTimes(process, &mut created, &mut exit, &mut kernel, &mut user)
                .map_err(error)?;
            let mut code = 0;
            GetExitCodeProcess(process, &mut code).map_err(error)?;
            if code != 259 {
                return Err(DesktopError::TargetUnavailable(
                    "application process exited".into(),
                ));
            }
            let mut path = vec![0u16; 32768];
            let mut length = path.len() as u32;
            QueryFullProcessImageNameW(
                process,
                PROCESS_NAME_WIN32,
                windows::core::PWSTR(path.as_mut_ptr()),
                &mut length,
            )
            .map_err(error)?;
            let executable = String::from_utf16_lossy(&path[..length as usize]);
            let name = std::path::Path::new(&executable)
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned();
            let application = DesktopApplication {
                process_id: pid,
                process_started: (u64::from(created.dwHighDateTime) << 32)
                    | u64::from(created.dwLowDateTime),
            };
            Ok(DesktopApplicationInfo {
                resource_id: application.resource_id(),
                application,
                name,
                executable,
            })
        })();
        let _ = CloseHandle(process);
        result
    }
}
fn validate_application(application: &DesktopApplication) -> DesktopResult<()> {
    if &process_info(application.process_id)?.application != application {
        return Err(DesktopError::TargetUnavailable(
            "application process identity changed".into(),
        ));
    }
    Ok(())
}
pub fn applications(
    permissions: &LocalNodePermissionStore,
) -> DesktopResult<Vec<DesktopApplicationInfo>> {
    authorize_host(permissions)?;
    unsafe {
        let mut current_session = 0;
        ProcessIdToSessionId(GetCurrentProcessId(), &mut current_session).map_err(error)?;
        let snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0).map_err(error)?;
        let mut entry = PROCESSENTRY32W {
            dwSize: size_of::<PROCESSENTRY32W>() as u32,
            ..Default::default()
        };
        let mut result = Vec::new();
        if Process32FirstW(snapshot, &mut entry).is_ok() {
            loop {
                let mut session = u32::MAX;
                if ProcessIdToSessionId(entry.th32ProcessID, &mut session).is_ok()
                    && session == current_session
                    && let Ok(info) = process_info(entry.th32ProcessID)
                {
                    result.push(info);
                }
                if result.len() >= MAX_TARGETS || Process32NextW(snapshot, &mut entry).is_err() {
                    break;
                }
            }
        }
        let _ = CloseHandle(snapshot);
        Ok(result)
    }
}

#[derive(Default)]
struct WindowIndex {
    revision: u64,
    windows: BTreeMap<u64, DesktopWindowSurface>,
}
struct Observations {
    application: DesktopApplication,
    dirty: AtomicBool,
    alive: AtomicBool,
    index: Mutex<WindowIndex>,
}
thread_local! { static OBSERVER: RefCell<Option<Weak<Observations>>> = const { RefCell::new(None) }; }
unsafe extern "system" fn window_event(
    _: HWINEVENTHOOK,
    event: u32,
    window: HWND,
    object: i32,
    child: i32,
    _: u32,
    _: u32,
) {
    if object != 0 || child != 0 || window.0.is_null() {
        return;
    }
    OBSERVER.with(|slot| {
        let Some(observer) = slot.borrow().as_ref().and_then(Weak::upgrade) else {
            return;
        };
        let key = window.0 as usize as u64;
        let mut index = observer.index.lock().unwrap_or_else(|e| e.into_inner());
        if index.windows.contains_key(&key)
            || related_window(window, observer.application.process_id)
        {
            // A queued create notification can arrive after enumeration has
            // already returned this HWND. Only destruction retires that known
            // reference; enumeration also replaces changed native identities.
            if event == EVENT_OBJECT_DESTROY && index.windows.remove(&key).is_some() {
                index.revision += 1;
            }
            observer.dirty.store(true, Ordering::Release);
        }
    });
}
fn related_window(mut window: HWND, pid: u32) -> bool {
    let mut visited = BTreeSet::new();
    for _ in 0..64 {
        if window.0.is_null() || !visited.insert(window.0 as usize) {
            return false;
        }
        let mut actual = 0;
        unsafe {
            GetWindowThreadProcessId(window, Some(&mut actual));
        }
        if actual == pid {
            return true;
        }
        window = if unsafe { GetWindowLongPtrW(window, GWL_STYLE) } as u32 & WS_CHILD.0 != 0 {
            unsafe { GetParent(window) }.unwrap_or_default()
        } else {
            unsafe { GetWindow(window, GW_OWNER) }.unwrap_or_default()
        };
    }
    false
}
fn enabled_in_hierarchy(mut window: HWND) -> bool {
    for _ in 0..64 {
        if !unsafe { IsWindowEnabled(window) }.as_bool() {
            return false;
        }
        if unsafe { GetWindowLongPtrW(window, GWL_STYLE) } as u32 & WS_CHILD.0 == 0 {
            return true;
        }
        window = unsafe { GetParent(window) }.unwrap_or_default();
        if window.0.is_null() {
            return false;
        }
    }
    false
}
fn application_windows(pid: u32) -> DesktopResult<Vec<DesktopTargetInfo>> {
    struct Query {
        pid: u32,
        windows: Vec<DesktopTargetInfo>,
    }
    unsafe extern "system" fn collect(window: HWND, value: LPARAM) -> windows::core::BOOL {
        let query = unsafe { &mut *(value.0 as *mut Query) };
        if related_window(window, query.pid) {
            unsafe {
                let _ = collect_window(window, LPARAM(&mut query.windows as *mut _ as isize));
            }
        }
        true.into()
    }
    let mut query = Query {
        pid,
        windows: Vec::new(),
    };
    unsafe {
        EnumWindows(Some(collect), LPARAM(&mut query as *mut _ as isize)).map_err(error)?;
    }
    let roots: Vec<_> = query
        .windows
        .iter()
        .map(|window| window.target.clone())
        .collect();
    for root in roots {
        if query.windows.len() >= MAX_TARGETS {
            break;
        }
        let remaining = MAX_TARGETS - query.windows.len();
        query
            .windows
            .extend(enumerate_child_windows(&root)?.into_iter().take(remaining));
    }
    Ok(query.windows)
}
struct HookWorker {
    thread_id: u32,
    worker: std::thread::JoinHandle<()>,
}
pub struct ApplicationTracker {
    application: DesktopApplication,
    permissions: LocalNodePermissionStore,
    expires: Instant,
    closed: AtomicBool,
    observations: Arc<Observations>,
    worker: Mutex<Option<HookWorker>>,
}
impl ApplicationTracker {
    pub fn new(
        application: DesktopApplication,
        permissions: LocalNodePermissionStore,
        duration: Duration,
    ) -> DesktopResult<Arc<Self>> {
        authorize_resource(&permissions, &application.resource_id(), "observe")?;
        validate_application(&application)?;
        let observations = Arc::new(Observations {
            application: application.clone(),
            dirty: AtomicBool::new(true),
            alive: AtomicBool::new(true),
            index: Mutex::new(WindowIndex::default()),
        });
        let observed = Arc::downgrade(&observations);
        let (ready, receive) = mpsc::sync_channel(1);
        let worker = std::thread::Builder::new()
            .name("app-use-window-events".into())
            .spawn(move || unsafe {
                let mut message = MSG::default();
                let _ = PeekMessageW(&mut message, None, 0, 0, PM_NOREMOVE);
                OBSERVER.with(|slot| *slot.borrow_mut() = Some(observed));
                let hook = SetWinEventHook(
                    EVENT_OBJECT_CREATE,
                    EVENT_OBJECT_PARENTCHANGE,
                    None,
                    Some(window_event),
                    0,
                    0,
                    WINEVENT_OUTOFCONTEXT,
                );
                if hook.0.is_null() {
                    let _ = ready.send(Err(error("cannot install window event observer")));
                    return;
                }
                let _ = ready.send(Ok(GetCurrentThreadId()));
                while GetMessageW(&mut message, None, 0, 0).0 > 0 {
                    if message.message == WM_APP + 73 {
                        break;
                    }
                    DispatchMessageW(&message);
                }
                let _ = UnhookWinEvent(hook);
                OBSERVER.with(|slot| {
                    if let Some(observer) = slot.borrow().as_ref().and_then(Weak::upgrade) {
                        observer.alive.store(false, Ordering::Release);
                    }
                });
                OBSERVER.with(|slot| *slot.borrow_mut() = None);
            })
            .map_err(error)?;
        let thread_id = match receive.recv() {
            Ok(Ok(id)) => id,
            Ok(Err(error)) => {
                let _ = worker.join();
                return Err(error);
            }
            Err(failure) => {
                let _ = worker.join();
                return Err(error(failure));
            }
        };
        Ok(Arc::new(Self {
            application,
            permissions,
            expires: Instant::now() + duration,
            closed: AtomicBool::new(false),
            observations,
            worker: Mutex::new(Some(HookWorker { thread_id, worker })),
        }))
    }
    pub fn application(&self) -> DesktopApplication {
        self.application.clone()
    }
    pub fn remaining(&self) -> Duration {
        self.expires.saturating_duration_since(Instant::now())
    }
    fn validate(&self) -> DesktopResult<()> {
        if self.closed.load(Ordering::Acquire)
            || !self.observations.alive.load(Ordering::Acquire)
            || Instant::now() >= self.expires
        {
            return Err(DesktopError::Closed("application session ended".into()));
        }
        authorize_resource(
            &self.permissions,
            &self.application.resource_id(),
            "observe",
        )?;
        validate_application(&self.application)
    }
    pub fn snapshot(&self) -> DesktopResult<DesktopApplicationSnapshot> {
        self.validate()?;
        if self.observations.dirty.swap(false, Ordering::AcqRel) {
            let _dpi = DpiScope::new()?;
            let windows = application_windows(self.application.process_id)?;
            let mut index = self
                .observations
                .index
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let mut next = BTreeMap::new();
            for window in windows {
                let DesktopTarget::Window {
                    handle, process_id, ..
                } = window.target
                else {
                    continue;
                };
                if !related_window(hwnd(handle), self.application.process_id) {
                    continue;
                }
                let parent = unsafe { GetWindow(hwnd(handle), GW_OWNER) }.unwrap_or_default();
                let owner = window_target(parent).ok();
                let top_level =
                    unsafe { GetWindowLongPtrW(hwnd(handle), GWL_STYLE) } as u32 & WS_CHILD.0 == 0;
                let child_parent = if top_level {
                    None
                } else {
                    window_target(unsafe { GetParent(hwnd(handle)) }.unwrap_or_default()).ok()
                };
                let mut class = [0u16; 256];
                let length = unsafe { GetClassNameW(hwnd(handle), &mut class) }.max(0) as usize;
                let surface_id = index
                    .windows
                    .get(&handle)
                    .filter(|old| old.window.target == window.target)
                    .map(|old| old.surface_id.clone())
                    .unwrap_or_else(|| sinomo_core::Id::generate().to_string());
                next.insert(
                    handle,
                    DesktopWindowSurface {
                        resource_id: format!(
                            "window-surface:{}:{}",
                            self.application.resource_id(),
                            surface_id
                        ),
                        surface_id,
                        window,
                        owner,
                        parent: child_parent,
                        top_level,
                        class_name: String::from_utf16_lossy(&class[..length]),
                        visible: unsafe { IsWindowVisible(hwnd(handle)) }.as_bool(),
                        enabled: enabled_in_hierarchy(hwnd(handle)),
                        blocks_owner: !parent.0.is_null()
                            && unsafe { IsWindowEnabled(hwnd(handle)) }.as_bool()
                            && !unsafe { IsWindowEnabled(parent) }.as_bool(),
                        application_grant_applies: process_id == self.application.process_id,
                    },
                );
            }
            if next != index.windows {
                index.revision += 1;
                index.windows = next;
            }
        }
        let index = self
            .observations
            .index
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        Ok(DesktopApplicationSnapshot {
            application: self.application.clone(),
            revision: index.revision,
            windows: index.windows.values().cloned().collect(),
        })
    }
    pub fn resolve(
        &self,
        surface_id: &str,
        operation: &str,
    ) -> DesktopResult<DesktopWindowSurface> {
        let surface = self
            .snapshot()?
            .windows
            .into_iter()
            .find(|window| window.surface_id == surface_id)
            .ok_or_else(|| {
                DesktopError::TargetUnavailable(
                    "window reference expired; refresh application surfaces".into(),
                )
            })?;
        validate_target(&surface.window.target)?;
        let DesktopTarget::Window { handle, .. } = surface.window.target else {
            unreachable!()
        };
        if !related_window(hwnd(handle), self.application.process_id) {
            return Err(DesktopError::TargetUnavailable(
                "window no longer belongs to application instance".into(),
            ));
        }
        if operation == "input" && !enabled_in_hierarchy(hwnd(handle)) {
            return Err(DesktopError::TargetUnavailable(
                "window is disabled; select its enabled dialog explicitly".into(),
            ));
        }
        if operation != "observe"
            && (!surface.application_grant_applies
                || authorize_resource(
                    &self.permissions,
                    &self.application.resource_id(),
                    operation,
                )
                .is_err())
        {
            authorize_resource(&self.permissions, &surface.resource_id, operation)?;
        }
        Ok(surface)
    }
    pub fn authority(self: &Arc<Self>, surface_id: String) -> DesktopResult<DeviceAuthority> {
        Ok(DeviceAuthority {
            store: self.permissions.clone(),
            application: Some(Arc::new(WindowAuthority {
                tracker: self.clone(),
                surface_id,
            })),
        })
    }
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        if let Some(worker) = self.worker.lock().unwrap_or_else(|e| e.into_inner()).take() {
            unsafe {
                let _ = PostThreadMessageW(worker.thread_id, WM_APP + 73, WPARAM(0), LPARAM(0));
            }
            let _ = worker.worker.join();
        }
    }
}
impl Drop for ApplicationTracker {
    fn drop(&mut self) {
        self.close();
    }
}
struct WindowAuthority {
    tracker: Arc<ApplicationTracker>,
    surface_id: String,
}
impl TargetAuthority for WindowAuthority {
    fn check(&self, target: &DesktopTarget, operation: &str) -> DesktopResult<()> {
        let current = self.tracker.resolve(&self.surface_id, operation)?;
        if &current.window.target != target {
            return Err(DesktopError::TargetUnavailable(
                "window reference does not match target".into(),
            ));
        }
        Ok(())
    }
}
