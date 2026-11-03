#![cfg(windows)]
#[path = "support/surfaces.rs"]
mod fixture;
#[path = "support/app_use.rs"]
mod support;
#[path = "support/surface_host.rs"]
mod surface_host;
use sinomo_actions::desktop::provider::{DesktopProviderClient, NativeApplication};
use sinomo_runtime::device::desktop::{surfaces::*, *};
use sinomo_runtime::node::{LocalNodePermissionStore, LocalNodePermissions};
use std::sync::Arc;
use std::time::{Duration, Instant};
use windows::Win32::{
    Foundation::POINT,
    UI::WindowsAndMessaging::{GetCursorPos, GetForegroundWindow},
};

fn wait_window(app: &NativeApplication, title: &str) -> DesktopWindowSurface {
    let until = Instant::now() + Duration::from_secs(4);
    loop {
        if let Some(window) = app
            .snapshot()
            .unwrap()
            .windows
            .into_iter()
            .find(|w| w.window.title.ends_with(title))
        {
            return window;
        }
        assert!(Instant::now() < until, "window {title} not observed");
        std::thread::sleep(Duration::from_millis(25));
    }
}
fn wait_removed(app: &NativeApplication, id: &str) {
    let until = Instant::now() + Duration::from_secs(4);
    while app
        .snapshot()
        .unwrap()
        .windows
        .iter()
        .any(|w| w.surface_id == id)
    {
        assert!(Instant::now() < until, "destroyed window remained observed");
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[test]
#[ignore = "creates owned no-activate windows; run with keyboard and pointer idle"]
fn application_grants_follow_new_windows_and_reject_stale_references() {
    let root = sinomo_testdir::TestDir::new("app-surface-windows");
    support::install(&root);
    let store = LocalNodePermissionStore::new(&root);
    let mut grants = LocalNodePermissions::new();
    grants.set_remote_management_enabled(true);
    store.save(&grants).unwrap();
    let foreground = unsafe { GetForegroundWindow() };
    let mut pointer = POINT::default();
    unsafe {
        GetCursorPos(&mut pointer).unwrap();
    }
    let fixture = fixture::Fixture::new();
    let provider = DesktopProviderClient::resolve(&root).unwrap();
    let info = provider
        .applications()
        .unwrap()
        .into_iter()
        .find(|app| app.application.process_id == std::process::id())
        .unwrap();
    assert!(matches!(
        provider.open_application(info.application.clone(), 30_000),
        Err(DesktopError::PermissionDenied(_))
    ));
    grants
        .expose_resource_operation(info.resource_id.clone(), "observe")
        .unwrap();
    store.save(&grants).unwrap();
    let (app, _) = provider.open_application(info.application, 30_000).unwrap();
    let main = wait_window(&app, " main");
    fixture.command(fixture::LATE_CREATE);
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        app.resolve(&main.surface_id, "observe").unwrap().surface_id,
        main.surface_id,
        "late create must not retire an already observed window"
    );
    assert_eq!(
        match main.window.target {
            DesktopTarget::Window { handle, .. } => handle,
            _ => 0,
        },
        fixture.main
    );
    grants
        .expose_resource_operation(main.window.resource_id.clone(), "input")
        .unwrap();
    store.save(&grants).unwrap();
    assert!(
        provider
            .open_input(
                main.window.target.clone(),
                DesktopControlMode::Background,
                1000
            )
            .is_ok(),
        "legacy window grant stays valid for its original API"
    );
    assert!(
        matches!(
            app.open_input(&main.surface_id, 20_000),
            Err(DesktopError::PermissionDenied(_))
        ),
        "legacy grant cannot become a new surface-lifetime grant"
    );
    grants
        .expose_resource_operation(main.resource_id.clone(), "input")
        .unwrap();
    store.save(&grants).unwrap();
    let mut first = app.open_input(&main.surface_id, 20_000).unwrap();
    fixture.command(fixture::SPAWN);
    let second = wait_window(&app, " secondary");
    assert!(second.owner.is_some());
    assert!(second.application_grant_applies);
    assert!(
        matches!(
            app.open_input(&second.surface_id, 20_000),
            Err(DesktopError::PermissionDenied(_))
        ),
        "single-window consent must not broaden"
    );
    for operation in ["input", "capture"] {
        grants
            .expose_resource_operation(info.resource_id.clone(), operation)
            .unwrap();
    }
    store.save(&grants).unwrap();
    let mut second_input = app.open_input(&second.surface_id, 20_000).unwrap();
    second_input
        .send(&[DesktopInput::Text {
            text: "SECOND".into(),
        }])
        .unwrap();
    let capture = app
        .capture(
            &second.surface_id,
            DesktopCaptureConfig {
                resolution: None,
                pixel_format: None,
                target: second.window.target.clone(),
                frames_per_second: 20,
                include_cursor: false,
                lease_ms: 20_000,
            },
        )
        .unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    let sample = loop {
        if let Some(sample) = capture.next(0, Duration::from_millis(100)).unwrap() {
            break sample;
        }
        assert!(Instant::now() < until);
    };
    sample.validate().unwrap();
    assert!(
        matches!(capture.identity(),Some(DesktopCaptureIdentity::Target{application:Some(binding),..})if binding.surface_id==second.surface_id)
    );
    fixture.command(fixture::MODAL);
    let dialog = wait_window(&app, " dialog");
    assert!(dialog.blocks_owner);
    assert!(
        first
            .send(&[DesktopInput::Text {
                text: "BLOCKED".into()
            }])
            .is_err()
    );
    let mut dialog_input = app.open_input(&dialog.surface_id, 10_000).unwrap();
    dialog_input
        .send(&[DesktopInput::Text {
            text: "DIALOG".into(),
        }])
        .unwrap();
    drop(dialog_input);
    fixture.command(fixture::CLOSE_MODAL);
    wait_removed(&app, &dialog.surface_id);
    fixture.command(fixture::CLOSE_SECOND);
    wait_removed(&app, &second.surface_id);
    assert!(
        second_input
            .send(&[DesktopInput::Text {
                text: "STALE".into()
            }])
            .is_err()
    );
    assert!(capture.next(sample.sequence, Duration::ZERO).is_err());
    drop(capture);
    drop(second_input);
    drop(first);
    fixture.command(fixture::SPAWN);
    let replacement = wait_window(&app, " secondary");
    assert_ne!(replacement.surface_id, second.surface_id);
    fixture.command(fixture::CLOSE_MAIN);
    wait_removed(&app, &main.surface_id);
    assert!(
        app.snapshot().is_ok(),
        "app lifetime is independent of main window"
    );
    fixture.command(fixture::CLOSE_SECOND);
    wait_removed(&app, &replacement.surface_id);
    assert!(
        app.snapshot()
            .unwrap()
            .windows
            .iter()
            .all(|w| !w.window.title.starts_with(&fixture.name)),
        "tray-only/no-window application remains observable"
    );
    fixture.command(fixture::SPAWN);
    let restored = wait_window(&app, " secondary");
    assert!(app.open_input(&restored.surface_id, 1000).is_ok());
    app.close();
    assert!(app.snapshot().is_err());
    drop(app);
    let report = Arc::try_unwrap(provider).ok().unwrap().unload();
    assert!(
        report.shutdown_ok && report.library_handle_released,
        "{report:?}"
    );
    assert_eq!(
        fixture
            .activations
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "background window input/capture must never activate or focus a controlled window"
    );
    println!(
        "uncontrolled foreground before={foreground:?} after={:?}",
        unsafe { GetForegroundWindow() }
    );
    let mut after = POINT::default();
    unsafe {
        GetCursorPos(&mut after).unwrap();
    }
    assert_eq!((pointer.x, pointer.y), (after.x, after.y));
    let text: String = fixture
        .log
        .lock()
        .unwrap()
        .iter()
        .map(|(_, c)| char::from_u32(*c as u32).unwrap())
        .collect();
    assert!(text.contains("SECOND") && text.contains("DIALOG"));
    assert!(!text.contains("BLOCKED") && !text.contains("STALE"));
}

#[test]
#[ignore = "read-only inspection of exposed shell UI Automation tray items"]
fn inspect_visible_tray_surfaces() {
    let root = sinomo_testdir::TestDir::new("app-tray-inspection");
    support::install(&root);
    let store = LocalNodePermissionStore::new(&root);
    let mut grants = LocalNodePermissions::new();
    grants.set_remote_management_enabled(true);
    store.save(&grants).unwrap();
    let provider = DesktopProviderClient::resolve(&root).unwrap();
    let snapshot = provider.tray_items().unwrap();
    println!("{}", serde_json::to_string_pretty(&snapshot).unwrap());
    assert!(
        !snapshot.items.is_empty(),
        "this desktop exposes no supported tray UIA elements"
    );
    let report = Arc::try_unwrap(provider).ok().unwrap().unload();
    assert!(report.shutdown_ok && report.library_handle_released);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "opens only the owned fixture tray/menu; requires an idle interactive desktop and ffmpeg"]
async fn tray_invocation_and_application_menu_restore_a_window() {
    use sinomo_actions::desktop::*;
    use surface_host::{Host, Recording};
    let root = sinomo_testdir::TestDir::new("app-surface-tray");
    support::install(&root);
    let store = LocalNodePermissionStore::new(&root);
    let mut grants = LocalNodePermissions::new();
    grants.set_remote_management_enabled(true);
    store.save(&grants).unwrap();
    let fixture = fixture::Fixture::new();
    let mut host = Host::new(&root);
    let DesktopControlResponse::Applications { applications } =
        host.control(DesktopControlRequest::Applications).await
    else {
        panic!("applications")
    };
    let app = applications
        .into_iter()
        .find(|app| app.application.process_id == std::process::id())
        .unwrap();
    for operation in ["observe", "input", "capture", "desktop-interaction"] {
        grants
            .expose_resource_operation(app.resource_id.clone(), operation)
            .unwrap();
    }
    store.save(&grants).unwrap();
    let DesktopControlResponse::ApplicationOpened {
        session_id: application,
        ..
    } = host
        .control(DesktopControlRequest::OpenApplication {
            application: app.application,
            lease_ms: 120_000,
        })
        .await
    else {
        panic!("open application")
    };
    let DesktopControlResponse::Opened {
        session_id: view, ..
    } = host
        .control(DesktopControlRequest::OpenOverview {
            application_session_id: application.clone(),
            max_windows: 4,
        })
        .await
    else {
        panic!("overview")
    };
    let artifacts = std::env::var_os("SINOMO_APP_SURFACES_ARTIFACTS")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| root.join("artifacts"));
    let recording = Recording::start(&host, &view, &artifacts).await;
    fixture.command(fixture::TRAY);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let overflow_name =
        std::env::var("SINOMO_TRAY_OVERFLOW_NAME").unwrap_or_else(|_| "Show Hidden Icons".into());
    let mut opened_overflow = false;
    let until = Instant::now() + Duration::from_secs(6);
    let item = loop {
        let DesktopControlResponse::SemanticItems { snapshot } =
            host.control(DesktopControlRequest::TrayItems).await
        else {
            panic!("tray snapshot")
        };
        if let Some(item) = snapshot
            .items
            .iter()
            .find(|item| item.name.contains(&fixture.name))
        {
            break item.clone();
        }
        if !opened_overflow {
            let toggle=snapshot.items.iter().find(|item|item.name.eq_ignore_ascii_case(&overflow_name)).expect("overflow toggle not exposed; set SINOMO_TRAY_OVERFLOW_NAME for this shell language");
            for operation in ["invoke", "desktop-interaction"] {
                grants
                    .expose_resource_operation(toggle.resource_id.clone(), operation)
                    .unwrap();
            }
            store.save(&grants).unwrap();
            assert!(matches!(
                host.control(DesktopControlRequest::InvokeTray {
                    item_id: toggle.item_id.clone(),
                    operation: DesktopSemanticAction::Invoke,
                    allow_desktop_interaction: true
                })
                .await,
                DesktopControlResponse::SemanticInvoked { .. }
            ));
            opened_overflow = true;
        }
        assert!(Instant::now() < until, "owned tray icon not exposed");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(
        item.owner_application.is_none(),
        "shell identity must not masquerade as app ownership"
    );
    assert!(matches!(
        host.control(DesktopControlRequest::InvokeTray {
            item_id: item.item_id.clone(),
            operation: DesktopSemanticAction::Invoke,
            allow_desktop_interaction: false
        })
        .await,
        DesktopControlResponse::Failed {
            error: DesktopError::InteractionRequired(_)
        }
    ));
    assert!(matches!(
        host.control(DesktopControlRequest::InvokeTray {
            item_id: item.item_id.clone(),
            operation: DesktopSemanticAction::Invoke,
            allow_desktop_interaction: true
        })
        .await,
        DesktopControlResponse::Failed {
            error: DesktopError::PermissionDenied(_)
        }
    ));
    for operation in ["invoke", "desktop-interaction"] {
        grants
            .expose_resource_operation(item.resource_id.clone(), operation)
            .unwrap();
    }
    store.save(&grants).unwrap();
    fixture.command(fixture::CLOSE_MAIN);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        host.snapshot(&application)
            .await
            .windows
            .iter()
            .all(|w| !w.window.title.ends_with(" main"))
    );
    assert!(matches!(
        host.control(DesktopControlRequest::InvokeTray {
            item_id: item.item_id.clone(),
            operation: DesktopSemanticAction::Invoke,
            allow_desktop_interaction: true
        })
        .await,
        DesktopControlResponse::SemanticInvoked { .. }
    ));
    let until = Instant::now() + Duration::from_secs(5);
    let menu = loop {
        if let Some(menu) = host
            .snapshot(&application)
            .await
            .windows
            .into_iter()
            .find(|w| w.class_name == "#32768" && w.visible)
        {
            break menu;
        }
        assert!(
            Instant::now() < until,
            "tray default action did not open its menu"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    let DesktopControlResponse::SemanticItems { snapshot } = host
        .control(DesktopControlRequest::MenuItems {
            application_session_id: application.clone(),
            surface_id: menu.surface_id.clone(),
        })
        .await
    else {
        panic!("menu items")
    };
    let command = snapshot
        .items
        .iter()
        .find(|item| item.name.contains("Open fixture window"))
        .unwrap();
    let bounds = menu.window.bounds;
    assert!(
        command.bounds[0] >= bounds[0]
            && command.bounds[1] >= bounds[1]
            && command.bounds[2] <= bounds[2]
            && command.bounds[3] <= bounds[3],
        "UIA menu bounds must use the window inventory's physical pixels: {:?} vs {:?}",
        command.bounds,
        bounds
    );
    assert!(matches!(
        host.control(DesktopControlRequest::InvokeMenuItem {
            application_session_id: application.clone(),
            surface_id: menu.surface_id.clone(),
            item_id: command.item_id.clone(),
            allow_desktop_interaction: true
        })
        .await,
        DesktopControlResponse::SemanticInvoked { .. }
    ));
    let until = Instant::now() + Duration::from_secs(5);
    let restored = loop {
        if let Some(window) = host
            .snapshot(&application)
            .await
            .windows
            .into_iter()
            .find(|w| w.window.title.ends_with(" secondary"))
        {
            break window;
        }
        assert!(
            Instant::now() < until,
            "menu action did not restore fixture window"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(matches!(
        host.control(DesktopControlRequest::SelectWindow {
            overview_session_id: view.clone(),
            surface_id: Some(restored.surface_id.clone())
        })
        .await,
        DesktopControlResponse::ViewSelected { .. }
    ));
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        matches!(
            host.control(DesktopControlRequest::InvokeMenuItem {
                application_session_id: application.clone(),
                surface_id: menu.surface_id,
                item_id: command.item_id.clone(),
                allow_desktop_interaction: true
            })
            .await,
            DesktopControlResponse::Failed { .. }
        ),
        "closed menu references cannot be replayed"
    );
    if opened_overflow
        && let DesktopControlResponse::SemanticItems { snapshot } =
            host.control(DesktopControlRequest::TrayItems).await
        && snapshot
            .items
            .iter()
            .any(|item| item.name.contains(&fixture.name))
        && let Some(toggle) = snapshot
            .items
            .iter()
            .find(|item| item.name.eq_ignore_ascii_case(&overflow_name))
    {
        for operation in ["invoke", "desktop-interaction"] {
            grants
                .expose_resource_operation(toggle.resource_id.clone(), operation)
                .unwrap();
        }
        store.save(&grants).unwrap();
        host.control(DesktopControlRequest::InvokeTray {
            item_id: toggle.item_id.clone(),
            operation: DesktopSemanticAction::Invoke,
            allow_desktop_interaction: true,
        })
        .await;
    }
    let artifact = recording.finish(&host.calls).await;
    std::fs::write(artifact.join("assertions.json"),serde_json::to_vec_pretty(&serde_json::json!({"tray_requires_explicit_interaction":true,"tray_grant_does_not_follow_app_grant":true,"tray_invocation_opened_menu":true,"menu_invocation_restored_window":true,"stale_menu_rejected":true,"provider":"actual native dylib","restored_surface":restored})).unwrap()).unwrap();
    host.control(DesktopControlRequest::Close { session_id: view })
        .await;
    host.control(DesktopControlRequest::Close {
        session_id: application,
    })
    .await;
    host.host
        .shutdown_and_wait(Duration::from_secs(5))
        .await
        .unwrap();
    println!(
        "video_artifact={}",
        artifact.join("recording.mp4").display()
    );
}
