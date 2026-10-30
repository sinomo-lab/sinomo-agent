//! Semantic shell/menu actions on a dedicated, bounded UIA worker. No pointer
//! injection, automatic overflow expansion, Explorer memory reads or DLL injection.
use super::*;
use std::sync::{
    OnceLock,
    mpsc::{Receiver, SyncSender},
};
use windows::Win32::System::Com::{CLSCTX_INPROC_SERVER, CoCreateInstance};

enum Command {
    Tray(LocalNodePermissionStore),
    InvokeTray(
        LocalNodePermissionStore,
        String,
        DesktopSemanticAction,
        bool,
    ),
    Menus(Arc<ApplicationTracker>, String),
    InvokeMenu(Arc<ApplicationTracker>, String, String, bool),
    Stop,
}
enum Reply {
    Snapshot(DesktopSemanticSnapshot),
    Invoked,
}
struct Request {
    command: Command,
    deadline: Instant,
    cancelled: Arc<AtomicBool>,
    reply: SyncSender<DesktopResult<Reply>>,
}
struct Worker {
    sender: SyncSender<Request>,
    thread: std::thread::JoinHandle<()>,
}
static WORKER: OnceLock<Mutex<Option<Worker>>> = OnceLock::new();
static UNAVAILABLE: AtomicBool = AtomicBool::new(false);
pub(super) fn available() -> bool {
    !UNAVAILABLE.load(Ordering::Acquire)
}
fn call(command: Command) -> DesktopResult<Reply> {
    if UNAVAILABLE.load(Ordering::Acquire) {
        return Err(DesktopError::Unsupported(
            "UI Automation worker unavailable; an earlier invocation may still be completing"
                .into(),
        ));
    }
    let mut worker = WORKER
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if worker.is_none() {
        let (sender, receiver) = mpsc::sync_channel(8);
        let thread = std::thread::Builder::new()
            .name("app-use-uia".into())
            .spawn(move || run(receiver))
            .map_err(error)?;
        *worker = Some(Worker { sender, thread });
    }
    let (reply, receive) = mpsc::sync_channel(1);
    let cancelled = Arc::new(AtomicBool::new(false));
    worker
        .as_ref()
        .unwrap()
        .sender
        .try_send(Request {
            command,
            deadline: Instant::now() + Duration::from_secs(3),
            cancelled: cancelled.clone(),
            reply,
        })
        .map_err(|_| DesktopError::Closed("UI Automation worker busy or stopped".into()))?;
    drop(worker);
    match receive.recv_timeout(Duration::from_secs(3)) {
        Ok(result) => result,
        Err(_) => {
            cancelled.store(true, Ordering::Release);
            UNAVAILABLE.store(true, Ordering::Release);
            Err(DesktopError::Platform("UI Automation timed out; action outcome may be unknown. Do not replay automatically.".into()))
        }
    }
}
pub fn shutdown() -> bool {
    let Some(slot) = WORKER.get() else {
        return true;
    };
    let mut slot = slot.lock().unwrap_or_else(|e| e.into_inner());
    let Some(worker) = slot.as_ref() else {
        return true;
    };
    let (reply, _) = mpsc::sync_channel(1);
    let _ = worker.sender.try_send(Request {
        command: Command::Stop,
        deadline: Instant::now() + Duration::from_secs(2),
        cancelled: Arc::new(AtomicBool::new(false)),
        reply,
    });
    let until = Instant::now() + Duration::from_secs(2);
    while !worker.thread.is_finished() && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(10));
    }
    if !worker.thread.is_finished() {
        return false;
    } // Host quarantines the DLL mapping.
    let worker = slot.take().unwrap();
    let _ = worker.thread.join();
    UNAVAILABLE.store(false, Ordering::Release);
    true
}
pub fn tray_items(
    permissions: &LocalNodePermissionStore,
) -> DesktopResult<DesktopSemanticSnapshot> {
    authorize_host(permissions)?;
    match call(Command::Tray(permissions.clone()))? {
        Reply::Snapshot(snapshot) => Ok(snapshot),
        _ => Err(error("invalid UIA response")),
    }
}
pub fn invoke_tray(
    permissions: &LocalNodePermissionStore,
    id: String,
    action: DesktopSemanticAction,
    allow: bool,
) -> DesktopResult<()> {
    if !allow {
        return Err(DesktopError::InteractionRequired(
            "tray actions may open shell/app UI; explicitly allow desktop interaction".into(),
        ));
    }
    match call(Command::InvokeTray(permissions.clone(), id, action, allow))? {
        Reply::Invoked => Ok(()),
        _ => Err(error("invalid UIA response")),
    }
}
pub fn menu_items(
    tracker: Arc<ApplicationTracker>,
    surface: String,
) -> DesktopResult<DesktopSemanticSnapshot> {
    match call(Command::Menus(tracker, surface))? {
        Reply::Snapshot(snapshot) => Ok(snapshot),
        _ => Err(error("invalid UIA response")),
    }
}
pub fn invoke_menu_item(
    tracker: Arc<ApplicationTracker>,
    surface: String,
    item: String,
    allow: bool,
) -> DesktopResult<()> {
    if !allow {
        return Err(DesktopError::InteractionRequired(
            "menu invocation may activate app UI; explicitly allow desktop interaction".into(),
        ));
    }
    match call(Command::InvokeMenu(tracker, surface, item, allow))? {
        Reply::Invoked => Ok(()),
        _ => Err(error("invalid UIA response")),
    }
}

enum Owner {
    Tray(DesktopApplication),
    Menu(Arc<ApplicationTracker>, String, DesktopTarget),
}
struct Item {
    element: IUIAutomationElement,
    info: DesktopSemanticItem,
    owner: Owner,
    expires: Instant,
}
struct Automation {
    uia: IUIAutomation2,
    items: BTreeMap<String, Item>,
}
fn run(receiver: Receiver<Request>) {
    // UIA proxies must share the native provider's physical-pixel context,
    // including when the host process is DPI-unaware. Keep it for worker life.
    let context = initialize_input_thread();
    let mut automation = context
        .as_ref()
        .map_err(Clone::clone)
        .and_then(|_| unsafe {
            CoCreateInstance::<_, IUIAutomation2>(&CUIAutomation8, None, CLSCTX_INPROC_SERVER)
                .map_err(error)
        })
        .and_then(|uia| {
            unsafe {
                uia.SetAutoSetFocus(false).map_err(error)?;
                uia.SetConnectionTimeout(750).map_err(error)?;
                uia.SetTransactionTimeout(1000).map_err(error)?;
            }
            Ok(Automation {
                uia,
                items: BTreeMap::new(),
            })
        });
    while let Ok(request) = receiver.recv() {
        if matches!(request.command, Command::Stop) {
            break;
        }
        let result =
            if request.cancelled.load(Ordering::Acquire) || Instant::now() >= request.deadline {
                Err(DesktopError::Closed(
                    "UIA request expired before execution".into(),
                ))
            } else {
                match automation.as_mut() {
                    Ok(automation) => automation.execute(&request),
                    Err(error) => {
                        UNAVAILABLE.store(true, Ordering::Release);
                        Err(error.clone())
                    }
                }
            };
        let _ = request.reply.send(result);
    }
}
impl Automation {
    fn execute(&mut self, request: &Request) -> DesktopResult<Reply> {
        self.items.retain(|_, item| item.expires > Instant::now());
        match &request.command {
            Command::Tray(permissions) => {
                authorize_host(permissions)?;
                let shell = window_target(unsafe { GetShellWindow() })?;
                let DesktopTarget::Window {
                    process_id,
                    process_started,
                    ..
                } = shell
                else {
                    unreachable!()
                };
                let identity = DesktopApplication {
                    process_id,
                    process_started,
                };
                let mut candidates = Vec::new();
                for window in enumerate_targets(true)? {
                    let DesktopTarget::Window {
                        handle,
                        process_id: pid,
                        ..
                    } = window.target
                    else {
                        continue;
                    };
                    if pid != process_id || !unsafe { IsWindowVisible(hwnd(handle)) }.as_bool() {
                        continue;
                    }
                    let mut class = [0u16; 256];
                    let length = unsafe { GetClassNameW(hwnd(handle), &mut class) }.max(0) as usize;
                    let class = String::from_utf16_lossy(&class[..length]);
                    if ![
                        "Shell_TrayWnd",
                        "Shell_SecondaryTrayWnd",
                        "NotifyIconOverflowWindow",
                        "TopLevelWindowForOverflowXamlIsland",
                    ]
                    .contains(&class.as_str())
                    {
                        continue;
                    }
                    let root =
                        unsafe { self.uia.ElementFromHandle(hwnd(handle)) }.map_err(error)?;
                    self.walk(
                        root,
                        class.contains("Overflow"),
                        false,
                        request,
                        &mut candidates,
                    )?;
                }
                let mut result = Vec::new();
                let mut retained = BTreeSet::new();
                for element in candidates.into_iter().take(128) {
                    let name = unsafe { element.CurrentName() }.map_err(error)?.to_string();
                    let existing = self
                        .items
                        .iter()
                        .find(|(_, item)| {
                            matches!(&item.owner, Owner::Tray(owner) if owner == &identity)
                                && item.info.name == name
                                && unsafe { self.uia.CompareElements(&item.element, &element) }
                                    .is_ok_and(|same| same.as_bool())
                        })
                        .map(|(id, _)| id.clone());
                    let id = existing.unwrap_or_else(|| sinomo_core::Id::generate().to_string());
                    let info = self.describe(
                        &element,
                        id.clone(),
                        format!(
                            "tray:{}:{}:{id}",
                            identity.process_id, identity.process_started
                        ),
                        "tray",
                        None,
                    )?;
                    self.items.insert(
                        id.clone(),
                        Item {
                            element,
                            info: info.clone(),
                            owner: Owner::Tray(identity.clone()),
                            expires: Instant::now() + Duration::from_secs(60),
                        },
                    );
                    retained.insert(id);
                    result.push(info);
                }
                self.items.retain(|id, item| {
                    !matches!(item.owner, Owner::Tray(_)) || retained.contains(id)
                });
                Ok(Reply::Snapshot(DesktopSemanticSnapshot { items: result, limitations: vec!["Only visible shell UIA controls are listed. Overflow is not opened automatically; item ownership is the shell, not a verified application.".into(), "References expire after 60 seconds or disappearance; rediscover after shell/icon recreation.".into()] }))
            }
            Command::Menus(tracker, surface) => {
                let window = tracker.resolve(surface, "observe")?;
                let DesktopTarget::Window {
                    handle,
                    process_id,
                    process_started,
                    ..
                } = window.window.target
                else {
                    unreachable!()
                };
                let root = unsafe { self.uia.ElementFromHandle(hwnd(handle)) }.map_err(error)?;
                let mut candidates = Vec::new();
                self.walk(root, false, true, request, &mut candidates)?;
                self.items.retain(|_, item| !matches!(&item.owner, Owner::Menu(owner, id, _) if Arc::ptr_eq(owner, tracker) && id == surface));
                let mut result = Vec::new();
                for element in candidates.into_iter().take(128) {
                    let id = sinomo_core::Id::generate().to_string();
                    let resource = if window.application_grant_applies {
                        tracker.application().resource_id()
                    } else {
                        window.resource_id.clone()
                    };
                    let info = self.describe(
                        &element,
                        id.clone(),
                        resource,
                        "menu_item",
                        Some(DesktopApplication {
                            process_id,
                            process_started,
                        }),
                    )?;
                    self.items.insert(
                        id,
                        Item {
                            element,
                            info: info.clone(),
                            owner: Owner::Menu(
                                tracker.clone(),
                                surface.clone(),
                                window.window.target.clone(),
                            ),
                            expires: Instant::now() + Duration::from_secs(30),
                        },
                    );
                    result.push(info);
                }
                Ok(Reply::Snapshot(DesktopSemanticSnapshot { items: result, limitations: vec!["Only exposed UIA menu items are available; invocation may open or activate app UI.".into()] }))
            }
            Command::InvokeTray(permissions, id, action, allow) => {
                if !allow {
                    return Err(DesktopError::InteractionRequired(
                        "desktop interaction not allowed".into(),
                    ));
                }
                let item = self.items.get(id).ok_or_else(|| {
                    DesktopError::TargetUnavailable("tray reference expired; rediscover".into())
                })?;
                let Owner::Tray(shell) = &item.owner else {
                    return Err(error("reference is not a tray item"));
                };
                validate_application(shell)?;
                let actual = window_target(unsafe { GetShellWindow() })?;
                if !matches!(actual, DesktopTarget::Window { process_id, process_started, .. } if process_id == shell.process_id && process_started == shell.process_started)
                {
                    return Err(DesktopError::TargetUnavailable("shell changed".into()));
                }
                authorize_resource(permissions, &item.info.resource_id, "invoke")?;
                authorize_resource(permissions, &item.info.resource_id, "desktop-interaction")?;
                self.invoke(item, *action, request)?;
                Ok(Reply::Invoked)
            }
            Command::InvokeMenu(tracker, surface, id, allow) => {
                if !allow {
                    return Err(DesktopError::InteractionRequired(
                        "desktop interaction not allowed".into(),
                    ));
                }
                let item = self.items.get(id).ok_or_else(|| {
                    DesktopError::TargetUnavailable("menu reference expired; rediscover".into())
                })?;
                let Owner::Menu(owner, bound, target) = &item.owner else {
                    return Err(error("reference is not a menu item"));
                };
                if !Arc::ptr_eq(owner, tracker) || bound != surface {
                    return Err(DesktopError::PermissionDenied(
                        "menu item belongs to another window/session".into(),
                    ));
                }
                if tracker.resolve(surface, "input")?.window.target != *target {
                    return Err(DesktopError::TargetUnavailable("menu owner changed".into()));
                }
                tracker.resolve(surface, "desktop-interaction")?;
                self.invoke(item, DesktopSemanticAction::Invoke, request)?;
                Ok(Reply::Invoked)
            }
            Command::Stop => unreachable!(),
        }
    }
    fn describe(
        &self,
        element: &IUIAutomationElement,
        id: String,
        resource: String,
        kind: &str,
        owner: Option<DesktopApplication>,
    ) -> DesktopResult<DesktopSemanticItem> {
        unsafe {
            let rect = element.CurrentBoundingRectangle().map_err(error)?;
            let invoke = element
                .GetCurrentPatternAs::<IUIAutomationInvokePattern>(UIA_InvokePatternId)
                .is_ok()
                || element
                    .GetCurrentPatternAs::<IUIAutomationLegacyIAccessiblePattern>(
                        UIA_LegacyIAccessiblePatternId,
                    )
                    .is_ok_and(|p| p.CurrentDefaultAction().is_ok_and(|a| !a.is_empty()));
            Ok(DesktopSemanticItem {
                item_id: id,
                resource_id: resource,
                name: element.CurrentName().map_err(error)?.to_string(),
                kind: kind.into(),
                owner_application: owner,
                bounds: [rect.left, rect.top, rect.right, rect.bottom],
                invoke,
                context_menu: if element.cast::<IUIAutomationElement3>().is_ok() {
                    None
                } else {
                    Some(false)
                },
                may_activate_window: true,
            })
        }
    }
    fn walk(
        &self,
        root: IUIAutomationElement,
        tray: bool,
        menu: bool,
        request: &Request,
        result: &mut Vec<IUIAutomationElement>,
    ) -> DesktopResult<()> {
        let walker = unsafe { self.uia.RawViewWalker() }.map_err(error)?;
        let mut pending = vec![(root, tray, 0usize)];
        let mut visited = 0;
        while let Some((element, in_tray, depth)) = pending.pop() {
            if request.cancelled.load(Ordering::Acquire) || Instant::now() >= request.deadline {
                return Err(DesktopError::Closed(
                    "UIA discovery deadline exceeded".into(),
                ));
            }
            visited += 1;
            if visited > 2048 || result.len() >= 128 {
                break;
            }
            unsafe {
                let class = element
                    .CurrentClassName()
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                let id = element
                    .CurrentAutomationId()
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                let in_tray = in_tray
                    || class == "TrayNotifyWnd"
                    || class == "NotifyIconOverflowWindow"
                    || class.contains("NotificationArea")
                    || id.contains("NotificationArea");
                let control = element.CurrentControlType().map_err(error)?;
                let candidate = if menu {
                    control == UIA_MenuItemControlTypeId
                } else {
                    id == "SystemTrayIcon"
                        || class.contains("NotifyIconView")
                        || (in_tray && control == UIA_ButtonControlTypeId)
                };
                if candidate
                    && !element.CurrentIsOffscreen().map_err(error)?.as_bool()
                    && element.CurrentIsEnabled().map_err(error)?.as_bool()
                {
                    result.push(element.clone());
                }
                if depth < 32
                    && let Ok(mut child) = walker.GetFirstChildElement(&element)
                {
                    for _ in 0..256 {
                        pending.push((child.clone(), in_tray, depth + 1));
                        match walker.GetNextSiblingElement(&child) {
                            Ok(next) => child = next,
                            Err(_) => break,
                        }
                    }
                }
            }
        }
        Ok(())
    }
    fn invoke(
        &self,
        item: &Item,
        action: DesktopSemanticAction,
        request: &Request,
    ) -> DesktopResult<()> {
        if request.cancelled.load(Ordering::Acquire) || Instant::now() >= request.deadline {
            return Err(DesktopError::Closed(
                "semantic action expired before execution".into(),
            ));
        }
        unsafe {
            if item
                .element
                .CurrentIsOffscreen()
                .map_err(|e| error(format!("UIA visibility check: {e}")))?
                .as_bool()
                || !item
                    .element
                    .CurrentIsEnabled()
                    .map_err(|e| error(format!("UIA enabled check: {e}")))?
                    .as_bool()
                || item
                    .element
                    .CurrentName()
                    .map_err(|e| error(format!("UIA name check: {e}")))?
                    != item.info.name
            {
                return Err(DesktopError::TargetUnavailable(
                    "semantic element changed or is unavailable; rediscover".into(),
                ));
            }
            self.uia.SetAutoSetFocus(false).map_err(error)?;
            match action {
                DesktopSemanticAction::ContextMenu => item
                    .element
                    .cast::<IUIAutomationElement3>()
                    .map_err(error)?
                    .ShowContextMenu()
                    .map_err(|e| {
                        DesktopError::Unsupported(format!("UIA context menu is not supported: {e}"))
                    }),
                DesktopSemanticAction::Invoke => {
                    if let Ok(pattern) = item
                        .element
                        .GetCurrentPatternAs::<IUIAutomationInvokePattern>(UIA_InvokePatternId)
                    {
                        pattern.Invoke().map_err(|e| error(format!("UIA Invoke failed: {e}; action outcome may be unknown. Observe the application before deciding whether to retry.")))
                    } else if let Ok(pattern) = item
                        .element
                        .GetCurrentPatternAs::<IUIAutomationLegacyIAccessiblePattern>(
                            UIA_LegacyIAccessiblePatternId,
                        )
                    {
                        pattern.DoDefaultAction().map_err(|e| error(format!("UIA default action failed: {e}; action outcome may be unknown. Observe the application before deciding whether to retry.")))
                    } else {
                        Err(DesktopError::Unsupported("no semantic invocation pattern; select raw interaction explicitly if needed".into()))
                    }
                }
            }
        }
    }
}
