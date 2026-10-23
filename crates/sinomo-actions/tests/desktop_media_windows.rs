//! Exercise the real component/device/media boundary without external model
//! credentials or input to any user-owned application.
#![cfg(target_os = "windows")]
#[path = "support/app_use.rs"]
mod support;
use sinomo_actions::desktop::*;
use sinomo_runtime::device::desktop::*;
use sinomo_runtime::module::*;
use sinomo_runtime::network::*;
use sinomo_runtime::node::{LocalNodePermissionStore, LocalNodePermissions};
use sinomo_runtime::service::*;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
use windows::Win32::UI::HiDpi::*;
use windows::Win32::UI::WindowsAndMessaging::*;
use windows::core::w;

struct WindowFixture {
    handle: u64,
    commands: mpsc::Sender<bool>,
    worker: Option<std::thread::JoinHandle<()>>,
}
impl WindowFixture {
    fn new() -> Self {
        let (ready, started) = mpsc::sync_channel(1);
        let (commands, incoming) = mpsc::channel();
        let worker = std::thread::spawn(move || unsafe {
            let old_dpi = SetThreadDpiAwarenessContext(DPI_AWARENESS_CONTEXT_PER_MONITOR_AWARE_V2);
            let window = CreateWindowExW(
                WS_EX_NOACTIVATE | WS_EX_TOOLWINDOW,
                w!("STATIC"),
                w!("Sinomo media frame one"),
                WS_OVERLAPPEDWINDOW,
                60,
                60,
                260,
                180,
                None,
                None,
                None,
                None,
            )
            .unwrap();
            let _ = ShowWindow(window, SW_SHOWNOACTIVATE);
            SetWindowPos(
                window,
                Some(HWND_BOTTOM),
                0,
                0,
                0,
                0,
                SWP_NOMOVE | SWP_NOSIZE | SWP_NOACTIVATE,
            )
            .unwrap();
            ready.send(window.0 as usize as u64).unwrap();
            let mut message = MSG::default();
            loop {
                match incoming.try_recv() {
                    Ok(true) => {
                        SetWindowTextW(window, w!("Sinomo media frame two")).unwrap();
                        SetWindowPos(
                            window,
                            None,
                            0,
                            0,
                            280,
                            200,
                            SWP_NOMOVE | SWP_NOZORDER | SWP_NOACTIVATE,
                        )
                        .unwrap();
                    }
                    Ok(false) | Err(mpsc::TryRecvError::Disconnected) => break,
                    Err(mpsc::TryRecvError::Empty) => {}
                }
                while PeekMessageW(&mut message, None, 0, 0, PM_REMOVE).as_bool() {
                    DispatchMessageW(&message);
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            DestroyWindow(window).unwrap();
            SetThreadDpiAwarenessContext(old_dpi);
        });
        Self {
            handle: started.recv_timeout(Duration::from_secs(3)).unwrap(),
            commands,
            worker: Some(worker),
        }
    }
}
impl Drop for WindowFixture {
    fn drop(&mut self) {
        let _ = self.commands.send(false);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
struct Sink(Arc<Mutex<Option<ObjectRpcOutgoingResponse>>>);
impl ResponseSink for Sink {
    fn send(self: Box<Self>, response: ObjectRpcOutgoingResponse) -> Result<(), ServiceError> {
        *self.0.lock().unwrap() = Some(response);
        Ok(())
    }
}
async fn invoke<T: serde::Serialize>(
    registry: &ServiceRegistry,
    data_dir: &Arc<sinomo_runtime::local::SinomoDataDir>,
    route: &str,
    category: &str,
    request: &T,
) -> ObjectRpcOutgoingResponse {
    let output = Arc::new(Mutex::new(None));
    let ctx = ServiceContext::new(
        data_dir.clone(),
        None,
        ServiceSigner::Local,
        ObjectRpcIncomingBody::default(),
        Box::new(Sink(output.clone())),
    );
    registry
        .dispatch(
            ctx,
            ObjectRpcRequest::encode_body(route, category, request).unwrap(),
        )
        .await
        .unwrap();
    output.lock().unwrap().take().unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires an interactive Windows desktop; creates an owned background window"]
async fn app_component_streams_live_video_over_the_shared_rpc_media_path() {
    let root = sinomo_testdir::TestDir::new("desktop-component-media");
    support::install(&root);
    let data_dir = Arc::new(sinomo_runtime::local::SinomoDataDir::new(&root));
    let store = LocalNodePermissionStore::new(&root);
    let mut grants = LocalNodePermissions::new();
    grants.set_remote_management_enabled(true);
    store.save(&grants).unwrap();
    let foreground = unsafe { GetForegroundWindow() };
    let fixture = WindowFixture::new();
    let context =
        ComponentContext::new(data_dir.clone()).with_runtime(tokio::runtime::Handle::current());
    let mut host = ComponentReconciler::new(context, ServiceRegistry::default());
    host.reconcile(&[Arc::new(component_factory(DesktopModule::AppUse).unwrap())])
        .unwrap();
    let snapshot = host.service_registry().active_snapshot();
    let registry = snapshot.registry();
    let targets = invoke(
        registry,
        &data_dir,
        APP_CONTROL_ROUTE,
        "app-use",
        &DesktopControlRequest::Targets {
            mode: DesktopControlMode::Background,
        },
    )
    .await
    .into_materialized_value_response(1024 * 1024)
    .await
    .unwrap()
    .decode_body::<DesktopControlResponse>()
    .unwrap();
    let DesktopControlResponse::Targets { targets } = targets else {
        panic!("targets response");
    };
    let target = targets.into_iter().find(|item| matches!(item.target, DesktopTarget::Window { handle, .. } if handle == fixture.handle)).unwrap().target;
    grants
        .expose_resource_operation(target.resource_id(), "capture")
        .unwrap();
    store.save(&grants).unwrap();
    let opened = invoke(
        registry,
        &data_dir,
        APP_CONTROL_ROUTE,
        "app-use",
        &DesktopControlRequest::Open {
            target,
            mode: DesktopControlMode::Background,
            input: false,
            capture: true,
            lease_ms: 30_000,
        },
    )
    .await
    .into_materialized_value_response(16 * 1024)
    .await
    .unwrap()
    .decode_body::<DesktopControlResponse>()
    .unwrap();
    let DesktopControlResponse::Opened {
        session_id,
        stream: Some(stream),
        ..
    } = opened
    else {
        panic!("open response");
    };
    assert_eq!(stream.route, APP_CAPTURE_ROUTE);
    let request = DesktopStreamRequest {
        session_id: session_id.clone(),
        frames_per_second: 30,
        include_cursor: false,
        encoding: sinomo_lm::RealtimeVideoInputConfig {
            max_frame_rate_hz: Some(2),
            ..Default::default()
        },
    };
    let outgoing = invoke(
        registry,
        &data_dir,
        APP_CAPTURE_ROUTE,
        MEDIA_CATEGORY,
        &request,
    )
    .await;
    let (result, producer) = outgoing
        .into_test_result(&tokio::runtime::Handle::current())
        .unwrap();
    let header: serde_json::Value = result.response.decode_body().unwrap();
    assert_eq!(
        header["version"],
        sinomo_video::stream::VIDEO_STREAM_VERSION
    );
    let mut body = result.body.cancel_on_drop();
    let (first, first_frame) = tokio::time::timeout(
        Duration::from_secs(5),
        sinomo_video::stream::read_frame(&mut body),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert!(first_frame.image.len() > 100);
    fixture.commands.send(true).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let (next, frame) =
            tokio::time::timeout_at(deadline, sinomo_video::stream::read_frame(&mut body))
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        assert!(next.sequence > first.sequence);
        if next.source_width != first.source_width {
            assert!(next.timestamp_ns > first.timestamp_ns);
            assert_ne!(frame.image, first_frame.image);
            break;
        }
    }
    drop(body);
    // The loopback fixture has no mux carrier reset guard. Aborting its owned
    // producer models the reset performed by cancel_on_drop on a routed RPC,
    // including when the source is static and no further frame would wake it.
    producer.abort();
    let _ = tokio::time::timeout(Duration::from_secs(3), producer)
        .await
        .unwrap();
    invoke(
        registry,
        &data_dir,
        APP_CONTROL_ROUTE,
        "app-use",
        &DesktopControlRequest::Close { session_id },
    )
    .await
    .into_materialized_value_response(4096)
    .await
    .unwrap();
    host.shutdown_and_wait(Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(
        host.service_registry()
            .active_snapshot()
            .registry()
            .route_count(),
        0
    );
    assert_eq!(unsafe { GetForegroundWindow() }, foreground);
}
