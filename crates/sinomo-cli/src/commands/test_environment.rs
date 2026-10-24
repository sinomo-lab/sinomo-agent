//! Minimal real agent/component host for opt-in live desktop tests. It never
//! constructs a daemon, opens cluster state, or resolves an operator workspace.
use base64::Engine;
use serde::Serialize;
use sinomo_agent::*;
use sinomo_lm::*;
use sinomo_runtime::{local::SinomoDataDir, module::*, network::*, service::*};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Runtime;

pub fn write_json(path: &Path, value: &impl Serialize) -> Result<(), String> {
    std::fs::write(
        path,
        serde_json::to_vec_pretty(value).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())
}
fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

struct Sink(Arc<Mutex<Option<ObjectRpcOutgoingResponse>>>);
impl ResponseSink for Sink {
    fn send(self: Box<Self>, response: ObjectRpcOutgoingResponse) -> Result<(), ServiceError> {
        *self.0.lock().unwrap() = Some(response);
        Ok(())
    }
}
pub type CallGuard = dyn Fn(&ObjectRpcRequest) -> Result<(), String> + Send + Sync;
pub struct FixtureTransport {
    data: Arc<SinomoDataDir>,
    registry: ServiceRegistrySnapshotStore,
    runtime: tokio::runtime::Handle,
    pub receipts: Mutex<Vec<serde_json::Value>>,
    workers: Mutex<Vec<tokio::task::JoinHandle<Result<(), NetworkError>>>>,
    guard: Arc<CallGuard>,
    pub last_effect_unix_ms: AtomicU64,
}
impl AgentRemoteObjectRpcTransport for FixtureTransport {
    fn call<'a>(&'a self, call: ObjectRpcCall) -> AgentRemoteObjectRpcFuture<'a> {
        Box::pin(async move {
            let started = Instant::now();
            let (mut incoming, request_worker) = call
                .into_test_incoming(&self.runtime)
                .map_err(|e| e.to_string())?;
            incoming
                .materialize_value_body(128 * 1024)
                .await
                .map_err(|e| e.to_string())?;
            (self.guard)(&incoming.request)?;
            let route = incoming.request.route.clone();
            let payload = incoming
                .request
                .decode_body::<serde_json::Value>()
                .unwrap_or_default();
            let operation = incoming.request.operation_id().map(str::to_string);
            let output = Arc::new(Mutex::new(None));
            let context = ServiceContext::new(
                self.data.clone(),
                None,
                ServiceSigner::Local,
                incoming.body,
                Box::new(Sink(output.clone())),
            );
            let dispatched = self
                .registry
                .active_snapshot()
                .registry()
                .dispatch(context, incoming.request)
                .await;
            request_worker
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())?;
            dispatched.map_err(|e| e.to_string())?;
            let response = output
                .lock()
                .unwrap()
                .take()
                .ok_or("component returned no response")?;
            let (mut result, worker) = response
                .into_test_result(&self.runtime)
                .map_err(|e| e.to_string())?;
            result
                .materialize_value_body(4 * 1024 * 1024)
                .await
                .map_err(|e| e.to_string())?;
            let response = result
                .response
                .decode_body::<serde_json::Value>()
                .unwrap_or_default();
            let completed_at = unix_ms();
            self.receipts.lock().unwrap().push(serde_json::json!({"route":route,"operation_id":operation,"input":payload,"response":response,"elapsed_ms":started.elapsed().as_millis(),"completed_at_unix_ms":completed_at}));
            self.last_effect_unix_ms
                .store(completed_at, Ordering::Release);
            let mut workers = self.workers.lock().unwrap();
            workers.retain(|task| !task.is_finished());
            workers.push(worker);
            Ok(result)
        })
    }
}
impl FixtureTransport {
    pub async fn invoke<T: Serialize>(
        &self,
        route: &str,
        category: &str,
        request: &T,
    ) -> Result<ObjectRpcResult, String> {
        self.call(ObjectRpcCall::value(
            ObjectRpcRequest::encode_body(route, category, request).map_err(|e| e.to_string())?,
        ))
        .await
    }
}

pub struct ComponentTestEnvironment {
    pub node_id: sinomo_runtime::core::Id,
    pub runtime: Runtime,
    pub transport: Arc<FixtureTransport>,
    pub projection: AgentModuleToolProjection,
    host: ComponentReconciler,
}
impl ComponentTestEnvironment {
    pub fn start(
        data: &SinomoDataDir,
        catalog: ComponentCatalog,
        guard: Arc<CallGuard>,
    ) -> Result<Self, String> {
        if sinomo_runtime::network::ClusterPaths::new(data)
            .state
            .exists()
        {
            return Err("test environment must not contain real cluster state".into());
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let node_id = sinomo_runtime::core::Id::generate();
        let context = ComponentContext::new(Arc::new(data.clone()))
            .with_runtime(runtime.handle().clone())
            .with_event_bus(ComponentEventBus::default().with_origin_node_id(node_id.clone()));
        let mut host = ComponentReconciler::new(context, ServiceRegistry::default());
        host.reconcile(&catalog.factories())
            .map_err(|e| e.to_string())?;
        let transport = Arc::new(FixtureTransport {
            data: Arc::new(data.clone()),
            registry: host.service_registry().clone(),
            runtime: runtime.handle().clone(),
            receipts: Mutex::new(Vec::new()),
            workers: Mutex::new(Vec::new()),
            guard,
            last_effect_unix_ms: AtomicU64::new(unix_ms()),
        });
        Ok(Self {
            node_id,
            projection: AgentModuleToolProjection::from_registry(catalog.modules()),
            runtime,
            transport,
            host,
        })
    }
    pub fn tools(&self, data: &SinomoDataDir) -> AgentToolRegistry {
        let registry = AgentToolRegistry::new().with_local_permission_data_dir(data.root());
        register_module_agent_tools(&registry, &self.projection, self.transport.clone());
        registry
    }
    pub fn close(&mut self) -> Result<(), String> {
        let workers = std::mem::take(&mut *self.transport.workers.lock().unwrap());
        self.runtime.block_on(async {
            // The fixture owns the local RPC producer tasks. Abort models the
            // carrier reset used by cancel_on_drop in a routed connection.
            for worker in workers {
                worker.abort();
                let _ = worker.await;
            }
            self.host
                .shutdown_and_wait(Duration::from_secs(5))
                .await
                .map_err(|e| e.to_string())
        })
    }
}
impl Drop for ComponentTestEnvironment {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

pub type VideoFrame = (
    sinomo_video::stream::VideoFrameMetadata,
    RealtimeEncodedVideoFrameInput,
);
pub type FrameGuard = dyn Fn() -> Result<(), String> + Send + Sync;
pub struct LiveFrames {
    latest: Mutex<Option<VideoFrame>>,
    changed: Condvar,
    pub failure: Mutex<Option<String>>,
    guard: Arc<FrameGuard>,
}
impl LiveFrames {
    pub fn latest(&self, wait: Duration) -> Result<VideoFrame, String> {
        self.after(0, wait)
    }
    pub fn after(&self, capture_time: u64, wait: Duration) -> Result<VideoFrame, String> {
        (self.guard)()?;
        let guard = self.latest.lock().unwrap();
        let (guard, _) = self
            .changed
            .wait_timeout_while(guard, wait, |v| {
                self.failure.lock().unwrap().is_none()
                    && v.as_ref().is_none_or(|(metadata, _)| {
                        metadata.captured_at_unix_ms < u128::from(capture_time)
                    })
            })
            .unwrap();
        if let Some(error) = self.failure.lock().unwrap().as_ref() {
            return Err(error.clone());
        }
        (self.guard)()?;
        guard
            .clone()
            .filter(|(metadata, _)| metadata.captured_at_unix_ms >= u128::from(capture_time))
            .ok_or_else(|| {
                "no sufficiently fresh video frame arrived after the preceding action".into()
            })
    }
}
pub struct VideoRecording {
    pub started: Instant,
    pub frames: Arc<LiveFrames>,
    stop: Arc<AtomicBool>,
    pump: tokio::task::JoinHandle<()>,
    encoder: Option<std::thread::JoinHandle<Result<serde_json::Value, String>>>,
}
impl VideoRecording {
    pub fn start(
        environment: &ComponentTestEnvironment,
        body: ObjectRpcIncomingBody,
        directory: &Path,
        ffmpeg: &Path,
        guard: Arc<FrameGuard>,
    ) -> Result<Self, String> {
        use std::io::Write;
        use std::os::windows::process::CommandExt;
        use std::process::{Command, Stdio};
        let started = Instant::now();
        let encoder_log =
            std::fs::File::create(directory.join("encoder.log")).map_err(|e| e.to_string())?;
        let timeline = std::fs::File::create(directory.join("video-frames.jsonl"))
            .map_err(|e| e.to_string())?;
        let mut child = Command::new(ffmpeg).args(["-hide_banner", "-loglevel", "warning", "-y", "-f", "image2pipe", "-framerate", "10", "-i", "pipe:0", "-vf", "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2", "-c:v", "libx264", "-preset", "veryfast", "-crf", "20", "-pix_fmt", "yuv420p", "-movflags", "+faststart"])
            .arg(directory.join("recording.mp4")).stdin(Stdio::piped()).stdout(Stdio::null()).stderr(Stdio::from(encoder_log)).creation_flags(0x08000000).spawn().map_err(|e| e.to_string())?;
        let Some(mut stdin) = child.stdin.take() else {
            let _ = child.kill();
            let _ = child.wait();
            return Err("missing ffmpeg input".into());
        };
        let frames = Arc::new(LiveFrames {
            latest: Mutex::new(None),
            changed: Condvar::new(),
            failure: Mutex::new(None),
            guard,
        });
        let output = frames.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let pump = environment.runtime.spawn(async move {
            let mut body = body.cancel_on_drop();
            loop {
                match sinomo_video::stream::read_frame(&mut body).await {
                    Ok(Some(frame)) => {
                        if let Err(error) = (output.guard)() {
                            *output.failure.lock().unwrap() = Some(error);
                            *output.latest.lock().unwrap() = None;
                            output.changed.notify_all();
                            break;
                        }
                        *output.latest.lock().unwrap() = Some(frame);
                        output.changed.notify_all();
                    }
                    Ok(None) => {
                        *output.failure.lock().unwrap() = Some("capture stream ended".into());
                        output.changed.notify_all();
                        break;
                    }
                    Err(error) => {
                        *output.failure.lock().unwrap() = Some(error.to_string());
                        output.changed.notify_all();
                        break;
                    }
                }
            }
        });
        let encoder_frames = frames.clone();
        let encoder_stop = stop.clone();
        let encoder = std::thread::spawn(move || {
            let mut log = timeline;
            let mut count = 0u64;
            let result = (|| -> Result<(), String> {
                encoder_frames.latest(Duration::from_secs(10))?;
                while !encoder_stop.load(Ordering::Acquire) {
                    let (metadata, frame) = encoder_frames.latest(Duration::ZERO)?;
                    stdin.write_all(&frame.image).map_err(|e| e.to_string())?;
                    writeln!(log, "{}", serde_json::json!({"recording_frame":count,"wall_ms":started.elapsed().as_millis(),"source":metadata})).map_err(|e| e.to_string())?;
                    count += 1;
                    let due = started + Duration::from_millis(count * 100);
                    std::thread::sleep(due.saturating_duration_since(Instant::now()));
                }
                Ok(())
            })();
            drop(stdin);
            let deadline = Instant::now() + Duration::from_secs(15);
            let status = loop {
                if let Some(status) = child.try_wait().map_err(|e| e.to_string())? {
                    break status;
                }
                if Instant::now() > deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err("video encoder did not finish".into());
                }
                std::thread::sleep(Duration::from_millis(50));
            };
            result?;
            if !status.success() || count == 0 {
                return Err("video recording failed; inspect encoder.log".into());
            }
            Ok(
                serde_json::json!({"frames":count,"frames_per_second":10,"duration_seconds":count as f64/10.0,"codec":"h264","container":"mp4","repeated_latest_frames":true}),
            )
        });
        Ok(Self {
            started,
            frames,
            stop,
            pump,
            encoder: Some(encoder),
        })
    }
    pub fn finish(&mut self) -> Result<serde_json::Value, String> {
        self.stop.store(true, Ordering::Release);
        let result = self
            .encoder
            .take()
            .ok_or("recording already finalized")?
            .join()
            .map_err(|_| "encoder worker panicked")?;
        self.pump.abort();
        result
    }
}

pub struct RealtimeCaseEvidence {
    directory: PathBuf,
    started: Instant,
    log: Mutex<std::fs::File>,
    audio: Mutex<(std::fs::File, u64)>,
    failure: Mutex<Option<String>>,
}
impl RealtimeCaseEvidence {
    fn observe(&self, event: &serde_json::Value) {
        use std::io::Write;
        let result = (|| -> Result<(), String> {
            let mut safe = event.clone();
            if event["type"] == "response.output_audio.delta"
                && let Some(encoded) = event["delta"].as_str()
            {
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .map_err(|e| e.to_string())?;
                let mut audio = self.audio.lock().unwrap();
                let target = (self.started.elapsed().as_secs_f64() * 24_000.0) as u64 * 2;
                if target > audio.1 {
                    let padding = (target - audio.1) as usize;
                    audio
                        .0
                        .write_all(&vec![0; padding])
                        .map_err(|e| e.to_string())?;
                    audio.1 = target;
                }
                audio.0.write_all(&decoded).map_err(|e| e.to_string())?;
                audio.1 += decoded.len() as u64;
                safe["delta"] = serde_json::json!({"pcm_bytes":decoded.len()});
            }
            redact_realtime_event(&mut safe);
            writeln!(
                self.log.lock().unwrap(),
                "{}",
                serde_json::json!({"elapsed_ms":self.started.elapsed().as_millis(),"event":safe})
            )
            .map_err(|e| e.to_string())
        })();
        if let Err(error) = result {
            *self.failure.lock().unwrap() = Some(error);
        }
    }
    pub fn audio_tail(&self) -> Duration {
        Duration::from_secs_f64(self.audio.lock().unwrap().1 as f64 / 48_000.0)
            .saturating_sub(self.started.elapsed())
    }
    pub fn mux(&self, ffmpeg: &Path) -> Result<(), String> {
        use std::io::Write;
        use std::os::windows::process::CommandExt;
        if let Some(error) = self.failure.lock().unwrap().clone() {
            return Err(error);
        }
        let mut audio = self.audio.lock().unwrap();
        audio.0.flush().map_err(|e| e.to_string())?;
        if audio.1 == 0 {
            return Ok(());
        }
        let duration = self.started.elapsed().as_secs_f64().to_string();
        let log =
            std::fs::File::create(self.directory.join("mux.log")).map_err(|e| e.to_string())?;
        let mut child = std::process::Command::new(ffmpeg)
            .args(["-hide_banner", "-loglevel", "error", "-y", "-i"])
            .arg(self.directory.join("recording.mp4"))
            .args(["-f", "s16le", "-ar", "24000", "-ac", "1", "-i"])
            .arg(self.directory.join("realtime-output.pcm"))
            .args([
                "-c:v",
                "copy",
                "-c:a",
                "aac",
                "-af",
                "apad",
                "-movflags",
                "+faststart",
            ])
            .args(["-t", duration.as_str()])
            .arg(self.directory.join("recording-with-audio.mp4"))
            .creation_flags(0x08000000)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(log)
            .spawn()
            .map_err(|e| e.to_string())?;
        let deadline = Instant::now() + Duration::from_secs(15);
        let status = loop {
            if let Some(status) = child.try_wait().map_err(|e| e.to_string())? {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err("recording mux timed out; inspect mux.log".into());
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        if !status.success() {
            return Err("recording audio mux failed; inspect mux.log".into());
        }
        Ok(())
    }
}
fn redact_realtime_event(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(fields) => {
            for (key, value) in fields {
                if matches!(
                    key.as_str(),
                    "access_token" | "refresh_token" | "api_key" | "client_secret"
                ) {
                    *value = serde_json::Value::String("[redacted]".into());
                } else {
                    redact_realtime_event(value);
                }
            }
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(redact_realtime_event),
        serde_json::Value::String(text) if text.starts_with("data:") => {
            *value = serde_json::Value::String("[media retained separately]".into())
        }
        _ => {}
    }
}
struct RealtimeFixtureSource {
    frames: Arc<LiveFrames>,
    transport: Arc<FixtureTransport>,
    directory: PathBuf,
    model: String,
    sequence: AtomicUsize,
    evidence: Arc<RealtimeCaseEvidence>,
    desktop_origin: Option<[i32; 2]>,
}
impl RealtimeModelVideoSource for RealtimeFixtureSource {
    fn observe(
        &self,
        before_response: bool,
    ) -> Result<Option<RealtimeModelVideoObservation>, String> {
        if let Some(error) = self.evidence.failure.lock().unwrap().clone() {
            return Err(error);
        }
        let after = if before_response {
            self.transport
                .last_effect_unix_ms
                .load(Ordering::Acquire)
                .saturating_add(150)
        } else {
            0
        };
        let (metadata, frame) = self.frames.after(
            after,
            if before_response {
                Duration::from_secs(5)
            } else {
                Duration::ZERO
            },
        )?;
        let index = self.sequence.fetch_add(1, Ordering::Relaxed);
        let path = self
            .directory
            .join("observations")
            .join(format!("video-{index:06}.jpg"));
        std::fs::write(&path, &frame.image).map_err(|e| e.to_string())?;
        write_json(
            &path.with_extension("json"),
            &serde_json::json!({"model":self.model,"source":metadata,"before_response":before_response,"observation_after_unix_ms":after,"image_blake3":blake3::hash(&frame.image).to_hex().to_string()}),
        )?;
        let coordinates = if let Some(origin) = self.desktop_origin {
            format!(
                "Convert an image pixel (x,y) to desktop input with input_x=round(x*{})+({}), input_y=round(y*{})+({}).",
                f64::from(metadata.source_width) / f64::from(metadata.width),
                origin[0],
                f64::from(metadata.source_height) / f64::from(metadata.height),
                origin[1],
            )
        } else {
            format!(
                "Use move_in_frame for app input. Locate control centers as fractions of the entire visible image (x=0 left, x=1 right, y=0 top, y=1 bottom). Do not subtract the title bar or convert to client pixels. For this frame supply source_width={} and source_height={}. The device performs coordinate conversion. After resizing, use fresh dimensions from a new frame. Observe each action's result before continuing.",
                metadata.source_width, metadata.source_height,
            )
        };
        let coordinates = if metadata.coordinate_grid {
            format!(
                "{coordinates} The image has coordinate rulers: labels along the TOP are x fractions and labels along the LEFT are y fractions. Thin grid lines mark every 0.1. Read those labels and interpolate the control center between the lines; do not estimate a different image size."
            )
        } else {
            coordinates
        };
        Ok(Some(RealtimeModelVideoObservation {
            frame,
            context: Some(format!(
                "{coordinates} Current frame: {}",
                serde_json::to_string(&metadata).unwrap()
            )),
        }))
    }
}
pub struct VisualRealtime {
    pub provider: Arc<RealtimeModelProvider>,
    pub evidence: Arc<RealtimeCaseEvidence>,
    credentials: OpenAiResponsesCodexClient,
    pub auth_failed: Arc<AtomicBool>,
    directory: PathBuf,
    sequence: AtomicUsize,
}
impl VisualRealtime {
    fn new(
        model: &str,
        credentials: &Path,
        frames: Arc<LiveFrames>,
        transport: Arc<FixtureTransport>,
        directory: &Path,
        started: Instant,
        desktop_origin: Option<[i32; 2]>,
    ) -> Result<Self, String> {
        let credentials = OpenAiResponsesCodexClient::new(OpenAiResponsesCodexConfig {
            credentials_path: Some(credentials.into()),
            default_model: model.into(),
            allow_credential_refresh: false,
            ..Default::default()
        });
        credential_preflight(&credentials)?;
        let mut config = RealtimeAudioSessionConfig::new(RealtimeAudioProvider::Codex, model)
            .with_video_input(RealtimeVideoInputConfig {
                max_frame_rate_hz: Some(2),
                detail: RealtimeVideoFrameDetail::High,
                ..Default::default()
            });
        credentials
            .authorize_realtime_session(&mut config)
            .map_err(|e| format!("AUTH_STOP: {e}"))?;
        config.connect_timeout_ms = Some(15_000);
        config.receive_timeout_ms = Some(45_000);
        std::fs::create_dir_all(directory.join("observations")).map_err(|e| e.to_string())?;
        let evidence = Arc::new(RealtimeCaseEvidence {
            directory: directory.into(),
            started,
            log: Mutex::new(
                std::fs::File::create(directory.join("realtime-events.jsonl"))
                    .map_err(|e| e.to_string())?,
            ),
            audio: Mutex::new((
                std::fs::File::create(directory.join("realtime-output.pcm"))
                    .map_err(|e| e.to_string())?,
                0,
            )),
            failure: Mutex::new(None),
        });
        let source = Arc::new(RealtimeFixtureSource {
            frames,
            transport,
            directory: directory.into(),
            model: model.into(),
            sequence: AtomicUsize::new(0),
            evidence: evidence.clone(),
            desktop_origin,
        });
        let observed = evidence.clone();
        let provider = RealtimeModelProvider::new(config)
            .map_err(|e| e.to_string())?
            .with_video_source(source)
            .with_observer(move |event| observed.observe(event));
        Ok(Self {
            provider: Arc::new(provider),
            evidence,
            credentials,
            auth_failed: Arc::new(AtomicBool::new(false)),
            directory: directory.into(),
            sequence: AtomicUsize::new(0),
        })
    }
}
pub enum DesktopVisualModel {
    Responses(VisualCodex),
    Realtime(VisualRealtime),
}
impl DesktopVisualModel {
    pub fn new(
        realtime: bool,
        model: &str,
        credentials: &Path,
        recording: &VideoRecording,
        transport: Arc<FixtureTransport>,
        directory: &Path,
        desktop_origin: Option<[i32; 2]>,
    ) -> Result<Self, String> {
        if realtime {
            VisualRealtime::new(
                model,
                credentials,
                recording.frames.clone(),
                transport,
                directory,
                recording.started,
                desktop_origin,
            )
            .map(Self::Realtime)
        } else {
            VisualCodex::new(
                model,
                credentials,
                recording.frames.clone(),
                transport,
                directory,
            )
            .map(Self::Responses)
        }
    }
    pub fn auth_failed(&self) -> Arc<AtomicBool> {
        match self {
            Self::Responses(p) => p.auth_failed.clone(),
            Self::Realtime(p) => p.auth_failed.clone(),
        }
    }
    pub fn realtime_handles(
        &self,
    ) -> Option<(Arc<RealtimeModelProvider>, Arc<RealtimeCaseEvidence>)> {
        match self {
            Self::Realtime(p) => Some((p.provider.clone(), p.evidence.clone())),
            _ => None,
        }
    }
}
impl LanguageModelProvider for DesktopVisualModel {
    fn info(&self) -> LanguageModelProviderInfo {
        match self {
            Self::Responses(p) => LanguageModelProvider::info(p),
            Self::Realtime(p) => LanguageModelProvider::info(p.provider.as_ref()),
        }
    }
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        LanguageModelProvider::complete_streaming_events_with_cancellation(
            self,
            request,
            &LanguageModelCancellation::default(),
            &mut |_| {},
        )
    }
    fn complete_streaming_events_with_cancellation(
        &self,
        request: &LanguageModelRequest,
        cancellation: &LanguageModelCancellation,
        event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        match self {
            Self::Responses(p) => {
                LanguageModelProvider::complete_streaming_events_with_cancellation(
                    p,
                    request,
                    cancellation,
                    event,
                )
            }
            Self::Realtime(p) => {
                if let Err(detail) = credential_preflight(&p.credentials) {
                    p.auth_failed.store(true, Ordering::Release);
                    return Err(LanguageModelError::AuthenticationRequired {
                        provider: "codex".into(),
                        detail,
                    });
                }
                let index = p.sequence.fetch_add(1, Ordering::Relaxed);
                write_json(
                    &p.directory
                        .join("observations")
                        .join(format!("{index:03}.request.json")),
                    request,
                )
                .map_err(|detail| {
                    LanguageModelError::ProviderInvocationFailed {
                        provider: "codex".into(),
                        model: request.model.clone(),
                        detail,
                    }
                })?;
                eprintln!("realtime agent model turn {index}: {}", request.model);
                let result = LanguageModelProvider::complete_streaming_events_with_cancellation(
                    p.provider.as_ref(),
                    request,
                    cancellation,
                    event,
                );
                if matches!(
                    result,
                    Err(LanguageModelError::AuthenticationRequired { .. })
                ) {
                    p.auth_failed.store(true, Ordering::Release);
                }
                result
            }
        }
    }
}
impl Drop for VideoRecording {
    fn drop(&mut self) {
        if self.encoder.is_some() {
            let _ = self.finish();
        }
        self.pump.abort();
    }
}

pub fn credential_preflight(client: &OpenAiResponsesCodexClient) -> Result<(), String> {
    let tokens = client
        .chatgpt_tokens()
        .map_err(|_| "AUTH_STOP: stored Codex credential is unavailable".to_string())?;
    let payload = tokens
        .access_token
        .split('.')
        .nth(1)
        .ok_or("AUTH_STOP: credential expiry cannot be verified")?;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .map_err(|_| "AUTH_STOP: credential expiry cannot be verified")?;
    let claims: serde_json::Value =
        serde_json::from_slice(&decoded).map_err(|_| "AUTH_STOP: invalid credential metadata")?;
    let expires = claims["exp"]
        .as_u64()
        .ok_or("AUTH_STOP: credential has no expiry")?;
    if expires
        <= SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            + 30
    {
        return Err("AUTH_STOP: stored Codex access token expired; no refresh attempted".into());
    }
    Ok(())
}

pub struct VisualCodex {
    model: String,
    credential_check: OpenAiResponsesCodexClient,
    frames: Arc<LiveFrames>,
    transport: Arc<FixtureTransport>,
    directory: PathBuf,
    sequence: AtomicUsize,
    pub auth_failed: Arc<AtomicBool>,
}
impl VisualCodex {
    pub fn new(
        model: &str,
        credentials: &Path,
        frames: Arc<LiveFrames>,
        transport: Arc<FixtureTransport>,
        directory: &Path,
    ) -> Result<Self, String> {
        let config = OpenAiResponsesCodexConfig {
            credentials_path: Some(credentials.into()),
            default_model: model.into(),
            allow_credential_refresh: false,
            transport_health_timeout: Duration::from_secs(60),
            ..Default::default()
        };
        let check = OpenAiResponsesCodexClient::new(config.clone());
        credential_preflight(&check)?;
        std::fs::create_dir_all(directory.join("observations")).map_err(|e| e.to_string())?;
        Ok(Self {
            model: model.into(),
            credential_check: check,
            frames,
            transport,
            directory: directory.into(),
            sequence: AtomicUsize::new(0),
            auth_failed: Arc::new(AtomicBool::new(false)),
        })
    }
    fn provider(&self) -> CodexProvider {
        CodexProvider::new(
            CodexConfig {
                default_model: self.model.clone(),
                ..Default::default()
            },
            self.credential_check.clone(),
            CodexToolRegistry::new(),
            DefaultCodexPromptBuilder::default(),
            DefaultCodexContextManager::default(),
        )
    }
}
impl LanguageModelProvider for VisualCodex {
    fn info(&self) -> LanguageModelProviderInfo {
        LanguageModelProvider::info(&self.provider())
    }
    fn complete(
        &self,
        request: &LanguageModelRequest,
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        LanguageModelProvider::complete_streaming_events_with_cancellation(
            self,
            request,
            &LanguageModelCancellation::default(),
            &mut |_| {},
        )
    }
    fn complete_streaming_events(
        &self,
        request: &LanguageModelRequest,
        event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        LanguageModelProvider::complete_streaming_events_with_cancellation(
            self,
            request,
            &LanguageModelCancellation::default(),
            event,
        )
    }
    fn complete_streaming_events_with_cancellation(
        &self,
        request: &LanguageModelRequest,
        cancellation: &LanguageModelCancellation,
        event: &mut dyn FnMut(LanguageModelStreamEvent),
    ) -> Result<LanguageModelResponse, LanguageModelError> {
        if let Err(detail) = credential_preflight(&self.credential_check) {
            self.auth_failed.store(true, Ordering::Release);
            return Err(LanguageModelError::AuthenticationRequired {
                provider: "codex".into(),
                detail,
            });
        }
        let after = self
            .transport
            .last_effect_unix_ms
            .load(Ordering::Acquire)
            .saturating_add(150);
        let (metadata, frame) =
            self.frames
                .after(after, Duration::from_secs(5))
                .map_err(|detail| LanguageModelError::ProviderInvocationFailed {
                    provider: "codex".into(),
                    model: request.model.clone(),
                    detail,
                })?;
        let index = self.sequence.fetch_add(1, Ordering::Relaxed);
        let image_path = self
            .directory
            .join("observations")
            .join(format!("{index:03}.jpg"));
        std::fs::write(&image_path, &frame.image).map_err(|e| {
            LanguageModelError::ProviderInvocationFailed {
                provider: "codex".into(),
                model: request.model.clone(),
                detail: e.to_string(),
            }
        })?;
        let _ = write_json(
            &image_path.with_extension("json"),
            &serde_json::json!({"model":request.model,"source":metadata,"observation_after_unix_ms":after,"image_blake3":blake3::hash(&frame.image).to_hex().to_string()}),
        );
        let _ = write_json(&image_path.with_extension("request.json"), request);
        let mut request = request.clone();
        if request.items.is_empty() {
            request.items = request
                .messages
                .iter()
                .map(|m| LanguageModelRequestItem::message(m.role.clone(), m.content.clone()))
                .collect();
        }
        request.items.push(LanguageModelRequestItem::Context {
            label: "Current live video observation".into(),
            content: serde_json::to_string(&metadata).unwrap(),
        });
        request.items.push(LanguageModelRequestItem::Image {
            media_type: "image/jpeg".into(),
            data_base64: base64::engine::general_purpose::STANDARD.encode(&frame.image),
        });
        request.parallel_tool_calls = Some(false);
        eprintln!(
            "live agent model turn {index}: {} (video frame {})",
            request.model, metadata.sequence
        );
        let result = LanguageModelProvider::complete_streaming_events_with_cancellation(
            &self.provider(),
            &request,
            cancellation,
            event,
        );
        if matches!(
            result,
            Err(LanguageModelError::AuthenticationRequired { .. })
        ) {
            self.auth_failed.store(true, Ordering::Release);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(time: u128) -> VideoFrame {
        (
            sinomo_video::stream::VideoFrameMetadata {
                sequence: time as u64,
                timestamp_ns: 0,
                duration_ns: 100_000_000,
                captured_at_unix_ms: time,
                width: 1,
                height: 1,
                source_width: 1,
                source_height: 1,
                format: RealtimeVideoFrameFormat::Jpeg,
                coordinate_grid: false,
            },
            RealtimeEncodedVideoFrameInput::new(
                Vec::<u8>::new(),
                RealtimeVideoFrameFormat::Jpeg,
                time as u64,
            ),
        )
    }

    #[test]
    fn visual_observation_waits_for_a_post_action_frame() {
        let frames = Arc::new(LiveFrames {
            latest: Mutex::new(Some(sample(10))),
            changed: Condvar::new(),
            failure: Mutex::new(None),
            guard: Arc::new(|| Ok(())),
        });
        assert!(
            frames.after(20, Duration::ZERO).is_err(),
            "stale observations must never be supplied"
        );
        let producer = frames.clone();
        let worker = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            *producer.latest.lock().unwrap() = Some(sample(30));
            producer.changed.notify_all();
        });
        assert_eq!(
            frames
                .after(20, Duration::from_secs(2))
                .unwrap()
                .0
                .captured_at_unix_ms,
            30
        );
        worker.join().unwrap();
    }

    #[test]
    fn visual_observation_never_exposes_frames_after_ownership_loss() {
        let frames = LiveFrames {
            latest: Mutex::new(Some(sample(30))),
            changed: Condvar::new(),
            failure: Mutex::new(None),
            guard: Arc::new(|| Err("fixture lost ownership".into())),
        };
        assert_eq!(
            frames.latest(Duration::ZERO).unwrap_err(),
            "fixture lost ownership"
        );
    }

    #[test]
    fn expired_credentials_stop_without_refreshing_or_rewriting() {
        let dir = sinomo_testdir::TestDir::new("desktop-expired-auth");
        let path = dir.join("codex.json");
        let client = OpenAiResponsesCodexClient::new(OpenAiResponsesCodexConfig {
            credentials_path: Some(path.clone()),
            auth_issuer: "http://127.0.0.1:1".into(),
            allow_credential_refresh: false,
            ..Default::default()
        });
        client
            .save_chatgpt_tokens(CodexChatGptTokens {
                id_token: "test".into(),
                access_token: format!(
                    "header.{}.signature",
                    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(br#"{"exp":1}"#)
                ),
                refresh_token: "must-not-be-used".into(),
                account_id: Some("fixture".into()),
                chatgpt_account_is_fedramp: false,
            })
            .unwrap();
        let before = std::fs::read(&path).unwrap();
        assert!(
            credential_preflight(&client)
                .unwrap_err()
                .starts_with("AUTH_STOP:")
        );
        assert!(matches!(
            client.refresh_chatgpt_tokens(),
            Err(LanguageModelError::AuthenticationRequired { .. })
        ));
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }
}
