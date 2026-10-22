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
            let started = Instant::now();
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
