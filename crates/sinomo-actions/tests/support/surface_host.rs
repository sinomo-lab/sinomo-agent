use sinomo_actions::desktop::*;
use sinomo_runtime::{local::SinomoDataDir, module::*, network::*, service::*};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};
pub struct Host {
    pub data: Arc<SinomoDataDir>,
    pub host: ComponentReconciler,
    pub calls: Vec<serde_json::Value>,
}
struct Sink(Arc<Mutex<Option<ObjectRpcOutgoingResponse>>>);
impl ResponseSink for Sink {
    fn send(self: Box<Self>, value: ObjectRpcOutgoingResponse) -> Result<(), ServiceError> {
        *self.0.lock().unwrap() = Some(value);
        Ok(())
    }
}
impl Host {
    pub fn new(root: &Path) -> Self {
        let data = Arc::new(SinomoDataDir::new(root));
        let context =
            ComponentContext::new(data.clone()).with_runtime(tokio::runtime::Handle::current());
        let mut host = ComponentReconciler::new(context, ServiceRegistry::default());
        host.reconcile(&[Arc::new(component_factory(DesktopModule::AppUse).unwrap())])
            .unwrap();
        Self {
            data,
            host,
            calls: Vec::new(),
        }
    }
    pub async fn request<T: serde::Serialize>(
        &self,
        route: &str,
        category: &str,
        input: &T,
        signer: ServiceSigner,
    ) -> ObjectRpcOutgoingResponse {
        let output = Arc::new(Mutex::new(None));
        self.host
            .service_registry()
            .active_snapshot()
            .registry()
            .dispatch(
                ServiceContext::new(
                    self.data.clone(),
                    None,
                    signer,
                    ObjectRpcIncomingBody::default(),
                    Box::new(Sink(output.clone())),
                ),
                ObjectRpcRequest::encode_body(route, category, input).unwrap(),
            )
            .await
            .unwrap();
        output.lock().unwrap().take().unwrap()
    }
    pub async fn control(&mut self, input: DesktopControlRequest) -> DesktopControlResponse {
        let response = self
            .request(APP_CONTROL_ROUTE, "app-use", &input, ServiceSigner::Local)
            .await
            .into_materialized_value_response(4 * 1024 * 1024)
            .await
            .unwrap()
            .decode_body::<DesktopControlResponse>()
            .unwrap();
        self.calls
            .push(serde_json::json!({"input":input,"response":response}));
        std::fs::write(
            self.data.root().join("surface-tool-calls.json"),
            serde_json::to_vec_pretty(&self.calls).unwrap(),
        )
        .unwrap();
        response
    }
    pub async fn snapshot(
        &mut self,
        id: &str,
    ) -> sinomo_runtime::device::desktop::surfaces::DesktopApplicationSnapshot {
        let DesktopControlResponse::Surfaces { snapshot } = self
            .control(DesktopControlRequest::Surfaces {
                session_id: id.into(),
            })
            .await
        else {
            panic!("snapshot")
        };
        snapshot
    }
}
pub struct Recording {
    stop: Arc<AtomicBool>,
    reader: tokio::task::JoinHandle<()>,
    producer: tokio::task::JoinHandle<Result<(), NetworkError>>,
    writer: Option<std::thread::JoinHandle<()>>,
    pub directory: PathBuf,
}
impl Recording {
    pub async fn start(host: &Host, view: &str, root: &Path) -> Self {
        let directory = root.join(format!("tray-{}", sinomo_runtime::core::Id::generate()));
        std::fs::create_dir_all(&directory).unwrap();
        let response = host
            .request(
                APP_CAPTURE_ROUTE,
                MEDIA_CATEGORY,
                &DesktopStreamRequest {
        transport: sinomo_video::codec::VideoTransport::Image,
                    session_id: view.into(),
                    frames_per_second: 20,
                    include_cursor: false,
                    encoding: sinomo_lm::RealtimeVideoInputConfig {
                        max_dimension: Some(1024),
                        max_frame_rate_hz: Some(10),
                        ..Default::default()
                    },
                },
                ServiceSigner::Local,
            )
            .await;
        let (result, producer) = response
            .into_test_result(&tokio::runtime::Handle::current())
            .unwrap();
        let latest = Arc::new(Mutex::new(None::<Vec<u8>>));
        let output = latest.clone();
        let path = directory.join("frames.jsonl");
        let reader = tokio::spawn(async move {
            let mut body = result.body.cancel_on_drop();
            let mut log = std::fs::File::create(path).unwrap();
            while let Ok(Some((metadata, frame))) =
                sinomo_video::stream::read_frame(&mut body).await
            {
                writeln!(log, "{}", serde_json::to_string(&metadata).unwrap()).unwrap();
                *output.lock().unwrap() = Some(frame.image.to_vec());
            }
        });
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = stop.clone();
        let movie = directory.join("recording.mp4");
        let stderr = std::fs::File::create(directory.join("encoder.log")).unwrap();
        let writer = std::thread::spawn(move || {
            use std::os::windows::process::CommandExt;
            let mut child=std::process::Command::new("ffmpeg").creation_flags(0x08000000).args(["-v","error","-y","-f","image2pipe","-vcodec","mjpeg","-framerate","10","-i","pipe:0","-vf","scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2","-c:v","libx264","-pix_fmt","yuv420p"]).arg(&movie).stdin(std::process::Stdio::piped()).stdout(std::process::Stdio::null()).stderr(stderr).spawn().expect("ffmpeg required for video evidence");
            let mut input = child.stdin.take().unwrap();
            let mut next = Instant::now();
            while !stopped.load(Ordering::Acquire) {
                if let Some(frame) = latest.lock().unwrap().clone()
                    && input.write_all(&frame).is_err()
                {
                    break;
                }
                next += Duration::from_millis(100);
                std::thread::sleep(next.saturating_duration_since(Instant::now()));
            }
            drop(input);
            let until = Instant::now() + Duration::from_secs(10);
            loop {
                if let Some(status) = child.try_wait().unwrap() {
                    assert!(status.success());
                    break;
                }
                if Instant::now() >= until {
                    let _ = child.kill();
                    panic!("recording finalization deadline");
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        });
        Self {
            stop,
            reader,
            producer,
            writer: Some(writer),
            directory,
        }
    }
    pub async fn finish(mut self, calls: &[serde_json::Value]) -> PathBuf {
        tokio::time::sleep(Duration::from_millis(300)).await;
        self.stop.store(true, Ordering::Release);
        self.reader.abort();
        self.producer.abort();
        let writer = self.writer.take().unwrap();
        tokio::task::spawn_blocking(move || writer.join().unwrap())
            .await
            .unwrap();
        std::fs::write(
            self.directory.join("tool-calls.json"),
            serde_json::to_vec_pretty(calls).unwrap(),
        )
        .unwrap();
        self.directory.clone()
    }
}
impl Drop for Recording {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        self.reader.abort();
        self.producer.abort();
        if let Some(writer) = self.writer.take() {
            let _ = writer.join();
        }
    }
}
