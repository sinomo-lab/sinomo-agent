//! Opt-in, live model-driven desktop acceptance. These tests own their data,
//! windows, consent, component host and agent runtime. They never join a cluster.
mod fixture;
use super::test_environment::*;
use sinomo_actions::desktop::*;
use sinomo_agent::*;
use sinomo_lm::*;
use sinomo_runtime::{
    device::desktop::*, events::SessionId, local::SinomoDataDir, module::*, node::*, test::*,
};
use std::path::{Path, PathBuf};
use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
enum Case {
    AppForm,
    AppDragResize,
    ComputerForm,
    GamepadCapability,
    GamepadState,
}
impl Case {
    fn id(self) -> &'static str {
        match self {
            Self::AppForm => "agent.desktop.app-form",
            Self::AppDragResize => "agent.desktop.app-drag-resize",
            Self::ComputerForm => "agent.desktop.computer-form",
            Self::GamepadCapability => "agent.desktop.gamepad-capability",
            Self::GamepadState => "agent.desktop.gamepad-state",
        }
    }
    fn raw(self) -> bool {
        matches!(
            self,
            Self::ComputerForm | Self::GamepadCapability | Self::GamepadState
        )
    }
}
pub fn register(builder: SinomoTestRegistryBuilder) -> SinomoTestRegistryBuilder {
    let requirements = || {
        [
            SinomoTestRequirement::LiveNetwork,
            SinomoTestRequirement::provider_account("codex"),
        ]
    };
    builder.register(SinomoTestCase::new(Case::AppForm.id(),"Live agent fills a background form and uses message-based controls",["agent-desktop","live","video"],requirements(),|ctx|run(ctx,Case::AppForm)))
    .register(SinomoTestCase::new(Case::AppDragResize.id(),"Live agent drags, scrolls, and continues after a background window resize",["agent-desktop","live","video"],requirements(),|ctx|run(ctx,Case::AppDragResize)))
    .register(SinomoTestCase::new(Case::ComputerForm.id(),"Live agent operates an owned foreground app through raw desktop input",["agent-desktop","live","video"],requirements(),|ctx|run(ctx,Case::ComputerForm)))
    .register(SinomoTestCase::new(Case::GamepadCapability.id(),"Live agent probes native gamepad injection and reports unsupported capability honestly",["agent-desktop","live","video"],requirements(),|ctx|run(ctx,Case::GamepadCapability)))
    .register(SinomoTestCase::new(Case::GamepadState.id(),"Live agent drives native gamepad buttons, axes and trigger observed through native gamepad APIs",["agent-desktop","live","video"],requirements(),|ctx|run(ctx,Case::GamepadState)))
}

fn run(ctx: &mut SinomoTestContext, case: Case) -> SinomoTestResult {
    let model = ctx.required_parameter("model")?.to_string();
    if ctx.parameter("provider").unwrap_or("codex") != "codex" {
        return Err(SinomoTestError::failed(
            "desktop live fixture currently uses the Codex provider",
        ));
    }
    let root = PathBuf::from(ctx.required_parameter("artifacts-dir")?);
    let directory = root.join(case.id());
    std::fs::create_dir(&directory)
        .map_err(|e| SinomoTestError::failed(format!("create fresh case directory: {e}")))?;
    let credentials = codex_credentials_file_in_data_dir(&SinomoDataDir::new(ctx.data_dir()));
    let client = OpenAiResponsesCodexClient::new(OpenAiResponsesCodexConfig {
        credentials_path: Some(credentials.clone()),
        default_model: model.clone(),
        transport_health_timeout: Duration::from_secs(30),
        ..Default::default()
    });
    if let Err(error) = credential_preflight(&client) {
        ctx.request_stop(&error);
        return Err(SinomoTestError::failed(error));
    }
    // Catalog visibility is advisory. An explicitly requested model may be
    // callable while absent from the menu. The real agent request determines
    // availability; never substitute another model or spend a probe turn.
    let result = run_live(ctx, case, &model, &credentials, &directory, &root);
    if let Err(error) = &result
        && error.contains("AUTH_STOP")
    {
        ctx.request_stop(error);
    }
    let summary = match &result {
        Ok(outcome) => serde_json::to_value(outcome).unwrap(),
        Err(error) => serde_json::json!({"status":"failed","error":error}),
    };
    write_json(&directory.join("case-result.json"),&serde_json::json!({"case":case.id(),"model":model,"provider":"codex","result":summary,"environment":if ctx.parameter("interactive-fixture")==Some("true"){"owned windows on interactive desktop; isolated data and real agent/component runtimes"}else{"isolated Win32 desktop; real agent/component runtimes"},"transport":"local object-RPC production body ownership and framing"})).map_err(SinomoTestError::failed)?;
    for (file, mime, label) in [
        (
            "recording-with-audio.mp4",
            "video/mp4",
            "Live Realtime agent screen and returned speech",
        ),
        (
            "realtime-events.jsonl",
            "application/x-ndjson",
            "Realtime session, tool and usage events",
        ),
        ("recording.mp4", "video/mp4", "Live agent screen recording"),
        ("agent-run.json", "application/json", "Real agent execution"),
        (
            "tool-calls.json",
            "application/json",
            "Component RPC receipts",
        ),
        (
            "case-result.json",
            "application/json",
            "Independent case verdict",
        ),
        ("video-info.json", "application/json", "Video validation"),
        (
            "assertions.json",
            "application/json",
            "Independent application-state assertions",
        ),
        ("task.json", "application/json", "Task and assigned target"),
        (
            "agent-trace.jsonl",
            "application/x-ndjson",
            "Agent execution trace",
        ),
        (
            "video-frames.jsonl",
            "application/x-ndjson",
            "Video source frame timeline",
        ),
    ] {
        let path = directory.join(file);
        if path.is_file() {
            ctx.record_artifact(&path, mime, label)?;
        }
    }
    ctx.record("model", model);
    ctx.record("case_directory", directory);
    ctx.record("cluster_used", false);
    result.map_err(SinomoTestError::failed)
}

fn run_live(
    ctx: &mut SinomoTestContext,
    case: Case,
    model: &str,
    credentials: &Path,
    directory: &Path,
    root: &Path,
) -> Result<SinomoTestOutcome, String> {
    let desktop = ctx
        .required_parameter("desktop-name")
        .map_err(|e| e.to_string())?
        .to_string();
    let interactive = ctx.parameter("interactive-fixture") == Some("true");
    fixture::require_test_desktop(&desktop, interactive)?;
    let ffmpeg = PathBuf::from(
        ctx.required_parameter("ffmpeg")
            .map_err(|e| e.to_string())?,
    );
    let ffprobe = PathBuf::from(
        ctx.required_parameter("ffprobe")
            .map_err(|e| e.to_string())?,
    );
    let data = SinomoDataDir::new(directory.join("data"));
    std::fs::create_dir_all(data.root()).map_err(|e| e.to_string())?;
    let permissions = LocalNodePermissionStore::new(data.root());
    let mut grants = LocalNodePermissions::new();
    grants.set_remote_management_enabled(true);
    permissions.save(&grants).map_err(|e| e.to_string())?;
    let code = format!(
        "ORBIT-{}",
        sinomo_runtime::core::Id::generate()
            .to_string()
            .chars()
            .rev()
            .take(5)
            .collect::<String>()
            .to_ascii_uppercase()
    );
    let window = fixture::TestWindow::start(
        case.id(),
        &code,
        case.raw(),
        &root.join("desktop-ready"),
        &desktop,
        interactive,
    )?;
    let initial_pointer = window.pointer()?;
    let initial_foreground = window.foreground();
    if initial_foreground != window.foreground_handle
        || (!case.raw() && initial_foreground == window.handle)
    {
        return Err("fixture did not establish the required foreground ownership".into());
    }
    let kind = DesktopModule::AppUse;
    let mode = if case.raw() {
        DesktopControlMode::Raw
    } else {
        DesktopControlMode::Background
    };
    let artifact = PathBuf::from(
        ctx.required_parameter("app-use-module")
            .map_err(|e| e.to_string())?,
    );
    sinomo_runtime::module::install_native_dylib_module_from_file(
        &data.root(),
        &artifact,
        sinomo_runtime::module::NativeDylibModuleInstallRequest {
            expected_module_id: Some(
                sinomo_runtime::module::ModuleId::from_legacy_named(
                    sinomo_runtime::device::desktop::protocol::APP_USE_OS_MODULE_ID,
                )
                .unwrap(),
            ),
            artifact_id: None,
            expected_interface_version: Some("sinomo.app-use.os.v1".into()),
            cache_key: Some(sinomo_runtime::device::desktop::protocol::APP_USE_OS_CACHE_KEY.into()),
            library_name: None,
            source: "desktop-e2e verified local artifact".into(),
        },
    )
    .map_err(|e| e.to_string())?;
    let targets = {
        let provider =
            sinomo_actions::desktop::provider::DesktopProviderClient::resolve(&data.root())
                .map_err(|e| e.to_string())?;
        provider.targets(mode).map_err(|e| e.to_string())?
    };
    let target_info = targets
        .into_iter()
        .find(|item| match &item.target {
            DesktopTarget::Window { handle, .. } => *handle == window.handle,
            DesktopTarget::Monitor { .. } => {
                item.bounds[0] <= 0
                    && item.bounds[1] <= 0
                    && item.bounds[2] > 0
                    && item.bounds[3] > 0
            }
        })
        .ok_or("owned target not enumerated")?;
    for operation in ["capture", "input"] {
        grants
            .expose_resource_operation(target_info.resource_id.clone(), operation)
            .map_err(|e| e.to_string())?;
    }
    permissions.save(&grants).map_err(|e| e.to_string())?;
    let allowed = target_info.target.clone();
    let allowed_bounds = target_info.bounds;
    let fixture_handle = window.handle;
    let raw = case.raw();
    let expected_desktop = desktop.to_string();
    ctx.record("target", &target_info);
    let guard: Arc<CallGuard> = Arc::new(move |request| {
        if fixture::desktop_name(true)?.as_str() != expected_desktop {
            return Err("isolated desktop lost input ownership; test stopped".into());
        }
        if request.route != kind.control_route() && request.route != kind.capture_route() {
            return Err("route is outside the isolated fixture".into());
        }
        if request.route == kind.control_route() {
            let command: DesktopControlRequest =
                request.decode_body().map_err(|e| e.to_string())?;
            match command {
                DesktopControlRequest::Targets{..}=>return Err("fixture discovery is scoped to the assigned target; use the supplied target identity and current video dimensions".into()),
                DesktopControlRequest::Open{target,..}|DesktopControlRequest::Children{target} if target!=allowed=>return Err("target is outside the owned test resource".into()),
                DesktopControlRequest::Input{events,..}=>{
                    if raw&&unsafe{windows::Win32::UI::WindowsAndMessaging::GetForegroundWindow().0 as usize as u64}!=fixture_handle{return Err("raw input stopped because the owned fixture lost foreground".into());}
                    if raw && events.iter().any(|event| match event {
                        DesktopInput::Move { x, y } => *x < allowed_bounds[0] || *x >= allowed_bounds[2] || *y < allowed_bounds[1] || *y >= allowed_bounds[3],
                        DesktopInput::RelativeMove { .. } => true,
                        _ => false,
                    }) { return Err("raw test motion must stay inside the owned fixture; relative motion is not part of these cases".into()); }
                    if events.iter().any(|event|matches!(event,DesktopInput::Key{code,..} if [0x5b,0x5c,0x5d].contains(code)||(raw&&*code==0x38))){return Err("system hotkeys are outside the test task".into());}
                }
                _=>{}
            }
        }
        Ok(())
    });
    let registration = module_registration(kind);
    let skill = registration
        .surfaces
        .declarative_resources
        .iter()
        .filter_map(|r| r.embedded_text.as_deref())
        .collect::<Vec<_>>()
        .join("\n\n");
    let catalog = ComponentCatalog::builder()
        .register_module_factory(
            registration,
            Arc::new(component_factory(kind).map_err(|e| e.to_string())?),
        )
        .map_err(|e| e.to_string())?
        .build()
        .map_err(|e| e.to_string())?;
    let mut environment = ComponentTestEnvironment::start(&data, catalog, guard)?;
    let open = environment.runtime.block_on(environment.transport.invoke(
        kind.control_route(),
        kind.name(),
        &DesktopControlRequest::Open {
            target: target_info.target.clone(),
            mode,
            input: false,
            capture: true,
            lease_ms: 300_000,
        },
    ))?;
    let DesktopControlResponse::Opened {
        session_id: capture_id,
        ..
    } = open.response.decode_body().map_err(|e| e.to_string())?
    else {
        return Err("capture lease failed".into());
    };
    let max_dimension: u32 = ctx
        .parameter("capture-max-dimension")
        .unwrap_or("1440")
        .parse()
        .map_err(|_| "capture-max-dimension must be an integer")?;
    if !(128..=4096).contains(&max_dimension) {
        return Err("capture-max-dimension must be 128..4096".into());
    }
    let stream = environment.runtime.block_on(environment.transport.invoke(
        kind.capture_route(),
        MEDIA_CATEGORY,
        &DesktopStreamRequest {
            session_id: capture_id.clone(),
            frames_per_second: 20,
            include_cursor: case.raw(),
            encoding: RealtimeVideoInputConfig {
                max_dimension: Some(max_dimension),
                max_frame_rate_hz: Some(10),
                coordinate_grid: ctx.parameter("coordinate-grid") == Some("true"),
                ..Default::default()
            },
        },
    ))?;
    let frame_desktop = desktop.to_string();
    let frame_guard: Arc<FrameGuard> = Arc::new(move || {
        if fixture::desktop_name(true)?.as_str() == frame_desktop
            && (!raw
                || unsafe {
                    windows::Win32::UI::WindowsAndMessaging::GetForegroundWindow().0 as usize as u64
                } == fixture_handle)
        {
            Ok(())
        } else {
            Err("owned fixture lost capture ownership; no frame forwarded".into())
        }
    });
    let mut recording =
        VideoRecording::start(&environment, stream.body, directory, &ffmpeg, frame_guard)?;
    recording.frames.latest(Duration::from_secs(10))?;
    if ctx.parameter("fixture-only") == Some("true") {
        std::thread::sleep(Duration::from_secs(2));
        let video = recording.finish()?;
        let _ = environment.runtime.block_on(environment.transport.invoke(
            kind.control_route(),
            kind.name(),
            &DesktopControlRequest::Close {
                session_id: capture_id,
            },
        ));
        environment.close()?;
        write_json(&directory.join("video-info.json"), &video)?;
        return Ok(SinomoTestOutcome::skipped(
            "fixture-only diagnostic; no model or agent task ran",
        )
        .with_diagnostics(serde_json::json!({"fixture_ready":true,"model_invoked":false})));
    }
    let realtime = match ctx.parameter("agent-runtime") {
        Some("realtime") => true,
        Some("responses") | None => false,
        _ => return Err("unknown agent-runtime; use responses or realtime".into()),
    };
    ctx.record(
        "agent_runtime",
        if realtime {
            "AgentSessionRuntime + persistent RealtimeModelProvider"
        } else {
            "AgentSessionRuntime + Responses"
        },
    );
    let desktop_origin = raw.then_some([target_info.bounds[0], target_info.bounds[1]]);
    let visual = DesktopVisualModel::new(
        realtime,
        model,
        credentials,
        &recording,
        environment.transport.clone(),
        directory,
        desktop_origin,
    )?;
    let realtime_handles = visual.realtime_handles();
    let auth_failed = visual.auth_failed();
    let tools = environment.tools(&data);
    let mut config = AgentConfig::new("codex", model)
        .with_workspace_root(directory)
        .with_prompt_reference_expansion(false)
        .with_max_steps(20)
        .with_max_tool_calls_per_step(1)
        .with_max_model_repair_attempts(1)
        .with_module_tool_projection(environment.projection.clone());
    config.visible_toolsets = vec!["module".into(), kind.name().into()];
    config.max_output_tokens = Some(4096);
    if realtime {
        config.definition.output_format = AgentOutputFormat::NativeTools;
        config = config
            .with_max_tool_calls_per_step(8)
            .with_max_model_repair_attempts(0);
    }
    let agent = Agent::new(visual, "codex", model)
        .with_config(config)
        .with_tool_registry(tools)
        .with_trace_store(FileAgentTraceStore::new(
            directory.join("agent-trace.jsonl"),
        ))
        .with_session_store(FileAgentSessionStore::for_data_dir(&data));
    let mut session = AgentSession::new(SessionId::generate(), AgentRole::Main);
    let interaction = InteractionSession::new(
        session.session_id.clone(),
        environment.node_id.clone(),
        InteractionSurface::new("desktop-e2e", InteractionSurfaceKind::CommandLine),
    );
    let mut task=match case{
        Case::AppForm|Case::ComputerForm=>format!("Fill the visible input with {code}, submit it using Enter or the Submit button, make Counter exactly 3, and turn the OFF button ON. Use the provided desktop control tool. Verify the final screen, release all held inputs and close your input session."),
        Case::AppDragResize=>format!("Click EXPAND to resize the window, then use the updated visual frame to drag the yellow token into the green DROP HERE area. Scroll upward three wheel ticks so Scroll ticks reads 3. Type {code} into the input and submit. Verify the final screen and close your input session."),
        Case::GamepadCapability=>"Open an input session and attempt one neutral native gamepad state (all buttons, axes and triggers zero). If the OS rejects the restricted capability, report that honestly and do not substitute keyboard input. Close your session. This is a capability probe, not a game-play claim.".into(),
        Case::GamepadState=>"Use native gamepad injection to hold A (Windows GamepadButtons mask 4), left_x=0.65, left_y=-0.4, right_trigger=0.75, all other axes/triggers zero. Wait for the next visual observation and verify Gamepad pattern reads true. Then send a separate completely neutral gamepad state, verify Returned neutral reads true, and close your input session. Never send the held and neutral states in the same batch. If Windows reports the native capability unsupported, report that honestly without substituting keyboard input.".into(),
    };
    if realtime {
        if !raw {
            task.push_str(" Ground app clicks and drags with move_in_frame using fractions of the entire image and its source dimensions. Separate resizing from subsequent actions and observe the new frame first. After each meaningful action, inspect the result and correct a miss before continuing.");
        }
        task.push_str(" Do not copy example coordinates from the skill. Capture is already streaming to you: open input=true, capture=false with lease_ms=180000, then use only input and close. Keep speech brief and perform the task with native function calls.");
    }
    task.push_str(&format!(" Use mode={} in the open request for this case. This is an explicit test of that input mode.", if raw { "raw" } else { "background" }));
    let system = format!(
        "You are an actual Sinomo agent performing a bounded acceptance task in an owned Windows test window with isolated runtime state. Use only the offered module control tool; do not delegate or invent other tools. A fresh video frame is supplied before each model request. Coordinate metadata accompanies it. The test window belongs to this task. Use the exact target below when opening an input=true, capture=false, lease_ms=180000 session. All observation recording is already running independently. Do not open a second capture. Query capabilities if useful. Global target discovery is intentionally denied; use the assigned target. The window origin stays fixed if it resizes, and current video metadata supplies the updated dimensions. Use one tool invocation at a time, but batch related input events. Always pair button/key down and up. For background app input send move before clicks, use VK_RETURN 13 for Enter; computer use uses scan-code 28 for Enter. Only claim completion after observing the resulting state.\nTARGET INFO:\n{}\nMODULE SKILL:\n{}",
        serde_json::to_string(&target_info).unwrap(),
        skill
    );
    write_json(
        &directory.join("task.json"),
        &serde_json::json!({"task":task,"model":model,"target":target_info,"skill_blake3":blake3::hash(skill.as_bytes()).to_hex().to_string(),"isolated_desktop":desktop}),
    )?;
    let input = AgentLoopInput {
        session: interaction.clone(),
        latest_event: user_interaction_input_event(&interaction, &task),
        transient_system_context: Some(system),
        async_update: None,
        context_attachments: Vec::new(),
        tool_specs: Vec::new(),
    };
    // A surface supplying transient system context owns the stable user
    // transcript, just like the daemon chat host. Seed both projections.
    let user_item = AgentItem::UserMessage {
        item_id: TaskId::generate(),
        step_index: 0,
        content: task.clone(),
        event_id: Some(input.latest_event.event_id.clone()),
    };
    session.transcript.push(user_item.clone());
    session.raw_transcript.push(user_item);
    let agent_runtime = AgentSessionRuntime::new(agent, session);
    let op = AgentOp::start_turn(input).with_timeout_ms(180_000);
    let op_id = op.op_id.clone();
    let result = (|| -> Result<AgentRun, String> {
        let accepted = agent_runtime.submit_op(op).map_err(|e| e.to_string())?;
        if !accepted.accepted {
            return Err("agent rejected the task".into());
        }
        agent_runtime
            .join_active_turn()
            .map_err(|e| e.to_string())?;
        agent_runtime
            .turn_result(&op_id)
            .map_err(|e| e.to_string())?
            .and_then(|r| r.run)
            .ok_or("agent returned no run".into())
    })();
    if auth_failed.load(Ordering::Acquire) {
        ctx.request_stop("Codex authentication expired or was rejected");
    }
    if let Ok(run) = &result {
        write_json(&directory.join("agent-run.json"), run)?;
    } else {
        write_json(
            &directory.join("agent-run.json"),
            &serde_json::json!({"error":result.as_ref().err()}),
        )?;
    }
    if let Some((provider, _)) = &realtime_handles {
        provider.close()?;
    }
    let tail = realtime_handles
        .as_ref()
        .map(|(_, e)| e.audio_tail())
        .unwrap_or_default();
    std::thread::sleep(Duration::from_secs(1).max(tail));
    let actual = window.state();
    let pointer = window.pointer()?;
    let foreground = window.foreground();
    let initial_request = std::fs::read(directory.join("observations/000.request.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<LanguageModelRequest>(&bytes).ok());
    let task_delivered=initial_request.as_ref().is_some_and(|request|request.model==model&&request.items.iter().any(|item|matches!(item,LanguageModelRequestItem::Message{role:LanguageModelRole::User,content} if content.contains(&task))));
    let receipts = environment.transport.receipts.lock().unwrap().clone();
    write_json(&directory.join("tool-calls.json"), &receipts)?;
    let recording_result = recording.finish();
    let _ = environment.runtime.block_on(environment.transport.invoke(
        kind.control_route(),
        kind.name(),
        &DesktopControlRequest::Close {
            session_id: capture_id,
        },
    ));
    environment.close()?;
    let video = recording_result?;
    if let Some((_, evidence)) = &realtime_handles {
        evidence.mux(&ffmpeg)?;
    }
    let probe = std::process::Command::new(ffprobe)
        .args([
            "-v",
            "error",
            "-show_streams",
            "-show_format",
            "-of",
            "json",
        ])
        .arg(directory.join("recording.mp4"))
        .output()
        .map_err(|e| e.to_string())?;
    if !probe.status.success() {
        return Err("recorded video failed ffprobe validation".into());
    }
    let probe: serde_json::Value =
        serde_json::from_slice(&probe.stdout).map_err(|e| e.to_string())?;
    write_json(
        &directory.join("video-info.json"),
        &serde_json::json!({"recording":video,"ffprobe":probe}),
    )?;
    let calls: Vec<_> = receipts
        .iter()
        .filter(|r| r["operation_id"].is_string())
        .collect();
    let gamepad_attempted = |r: &&serde_json::Value| {
        r["input"]["action"] == "input"
            && r["input"]["events"]
                .as_array()
                .is_some_and(|events| events.iter().any(|event| event["kind"] == "gamepad"))
    };
    let unsupported =
        calls.iter().copied().filter(gamepad_attempted).any(|r| {
            r.pointer("/response/error/code").and_then(|v| v.as_str()) == Some("unsupported")
        });
    let applied = calls
        .iter()
        .any(|r| r["response"]["status"] == "input_applied");
    let opened_inputs: Vec<_> = calls
        .iter()
        .filter(|r| {
            r["input"]["action"] == "open"
                && r["input"]["input"] == true
                && r["response"]["status"] == "opened"
        })
        .filter_map(|r| r["response"]["session_id"].as_str())
        .collect();
    let agent_closed = !opened_inputs.is_empty()
        && opened_inputs.iter().all(|id| {
            calls.iter().any(|r| {
                r["input"]["action"] == "close"
                    && r["input"]["session_id"] == *id
                    && r["response"]["status"] == "closed"
            })
        });
    let task_passed = match case {
        Case::AppForm | Case::ComputerForm => {
            actual.submitted == code && actual.counter == 3 && actual.enabled
        }
        Case::AppDragResize => {
            actual.submitted == code && actual.dragged && actual.expanded && actual.wheel_ticks == 3
        }
        Case::GamepadCapability => {
            calls
                .iter()
                .copied()
                .filter(gamepad_attempted)
                .any(|r| r["response"]["status"] == "input_applied")
                || unsupported
        }
        Case::GamepadState => actual.gamepad_pattern_seen && actual.gamepad_neutral_seen,
    };
    let focus_preserved = if case.raw() {
        foreground == window.handle
    } else {
        foreground == initial_foreground && pointer == initial_pointer
    };
    let diagnostics = serde_json::json!({"actual_app_state":actual,"goal_reached":task_passed,"task_delivered_to_model":task_delivered,"agent_closed_input_session":agent_closed,"input_isolation_verified":focus_preserved,"background_focus_and_pointer_preserved":if raw{None}else{Some(focus_preserved)},"raw_fixture_retained_foreground":if raw{Some(focus_preserved)}else{None},"initial_foreground":initial_foreground,"final_foreground":foreground,"initial_pointer":initial_pointer,"final_pointer":pointer,"agent_tool_calls":calls.len(),"input_applied":applied,"gamepad_unsupported":unsupported,"real_cluster_used":false});
    write_json(&directory.join("assertions.json"), &diagnostics)?;
    if auth_failed.load(Ordering::Acquire) {
        return Err(
            "AUTH_STOP: Codex authentication expired or was rejected; stopped live verification"
                .into(),
        );
    }
    result?;
    if matches!(case, Case::GamepadCapability | Case::GamepadState)
        && unsupported
        && task_delivered
        && agent_closed
        && focus_preserved
    {
        return Ok(SinomoTestOutcome::skipped("Windows native gamepad capability unavailable; agent attempted it and reported the limitation").with_diagnostics(diagnostics));
    }
    if !task_passed
        || !task_delivered
        || !agent_closed
        || !focus_preserved
        || !applied
        || calls.is_empty()
    {
        return Ok(SinomoTestOutcome::failed("independent application-state, task-delivery, cleanup or input-isolation assertion failed").with_diagnostics(diagnostics));
    }
    let summary = match case {
        Case::GamepadCapability => {
            "real agent native gamepad API probe succeeded; receiver behavior is tested separately"
        }
        Case::GamepadState => {
            "real agent gamepad button, axes and trigger observed through native gamepad APIs, then returned neutral"
        }
        _ => {
            "real agent completed the visible task through module tools; video and RPC evidence retained"
        }
    };
    Ok(SinomoTestOutcome::passed(summary).with_diagnostics(diagnostics))
}
