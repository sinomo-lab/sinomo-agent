//! Timeline interpretation and layout live entirely in this native provider.
use serde::{Deserialize, Serialize};
use sinomo_module_api::{host::InvocationHost, panels::*, sessions::*, *};
use std::collections::BTreeMap;
const MODULE: &str = "module.agent-timeline";
const SERVICE: &str = "agent-timeline-panel";
static LIFECYCLE: NativeModuleLifecycle = NativeModuleLifecycle::new();

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Span {
    pub session_id: String,
    pub call_id: String,
    pub label: String,
    pub start: i64,
    pub known_start: bool,
    pub end: Option<i64>,
    pub status: String,
    pub detail: String,
    pub targets: Vec<String>,
}
pub fn project(records: &[SessionActivity]) -> Vec<Span> {
    let mut calls = BTreeMap::<(String, String), Span>::new();
    let mut ordered: Vec<_> = records.iter().collect();
    ordered.sort_by_key(|r| (r.session_id.as_str(), r.sequence));
    for r in ordered {
        let kind = r.item["kind"].as_str().unwrap_or("");
        let id = r
            .tool_call_id
            .clone()
            .unwrap_or_else(|| format!("event-{}", r.sequence));
        let key = (r.session_id.clone(), id.clone());
        match kind {
            "tool_call" => {
                let call = &r.item["call"];
                calls.insert(
                    key,
                    Span {
                        session_id: r.session_id.clone(),
                        call_id: id,
                        label: call["tool"].as_str().unwrap_or("tool").into(),
                        start: r.recorded_at_unix_ms,
                        known_start: true,
                        end: None,
                        status: "running".into(),
                        detail: format!(
                            "{}{}",
                            serde_json::to_string(&call["input"]).unwrap_or_default(),
                            if r.item["projection_truncated"] == true {
                                " [record preview truncated]"
                            } else {
                                ""
                            }
                        ),
                        targets: if call["input"]["action"] == "message" {
                            call["input"]["id"]
                                .as_str()
                                .map(str::to_string)
                                .into_iter()
                                .collect()
                        } else {
                            vec![]
                        },
                    },
                );
            }
            "tool_result" => {
                if let Some(span) = calls.get_mut(&key) {
                    if let Some(target) = r.item["output"]["session_id"].as_str()
                        && target != span.session_id
                    {
                        span.targets = vec![target.to_string()];
                    }
                    if r.item["output"]["detached"] == true {
                        span.status = "detached".into();
                    } else {
                        span.end = Some(r.recorded_at_unix_ms);
                        span.status = r.item["status"].as_str().unwrap_or("unknown").into();
                    }
                } else {
                    calls.insert(
                        key,
                        Span {
                            session_id: r.session_id.clone(),
                            call_id: id,
                            label: r.item["tool_name"].as_str().unwrap_or("tool result").into(),
                            start: r.recorded_at_unix_ms,
                            known_start: false,
                            end: Some(r.recorded_at_unix_ms),
                            status: r.item["status"].as_str().unwrap_or("unknown").into(),
                            detail: "Call start is outside the loaded history window".into(),
                            targets: vec![],
                        },
                    );
                }
            }
            "async_task_update" => {
                if let Some(span) = calls.get_mut(&key) {
                    let status = r.item["update"]["status"].as_str().unwrap_or("unknown");
                    if !matches!(status, "running" | "progress") {
                        span.end = Some(r.recorded_at_unix_ms);
                    }
                    span.status = status.into();
                }
            }
            "conversation_message" => {
                calls.insert(
                    key,
                    Span {
                        session_id: r.session_id.clone(),
                        call_id: id,
                        label: "Channel message".into(),
                        start: r.recorded_at_unix_ms,
                        known_start: true,
                        end: Some(r.recorded_at_unix_ms),
                        status: "message".into(),
                        detail: r.item["content"].as_str().unwrap_or("").into(),
                        targets: r.item["recipients"]
                            .as_array()
                            .map(|ids| {
                                ids.iter()
                                    .filter_map(|id| id.as_str().map(str::to_string))
                                    .collect()
                            })
                            .unwrap_or_default(),
                    },
                );
            }
            "user_message" | "assistant_text" => {
                calls.insert(
                    key,
                    Span {
                        session_id: r.session_id.clone(),
                        call_id: id,
                        label: if kind == "user_message" {
                            "Message"
                        } else {
                            "Response"
                        }
                        .into(),
                        start: r.recorded_at_unix_ms,
                        known_start: true,
                        end: Some(r.recorded_at_unix_ms),
                        status: "message".into(),
                        detail: r.item["content"]
                            .as_str()
                            .unwrap_or("")
                            .chars()
                            .take(1000)
                            .collect(),
                        targets: vec![],
                    },
                );
            }
            _ => {}
        }
    }
    let mut spans: Vec<_> = calls.into_values().collect();
    spans.sort_by_key(|s| (s.start, s.session_id.clone(), s.call_id.clone()));
    spans
}
fn rect(x: f32, y: f32, width: f32, height: f32) -> UiRect {
    UiRect {
        x,
        y,
        width: width.max(0.),
        height: height.max(0.),
    }
}
fn text(frame: &mut UiWidgetFrame, r: UiRect, s: impl Into<String>, color: UiColor, size: f32) {
    frame.commands.push(UiPaintCommand::DrawText {
        rect: r,
        text: s.into(),
        style: UiTextStyle {
            color,
            font_size: size,
            font_weight: 400,
            monospace: false,
        },
    });
}
#[derive(Default, Serialize, Deserialize)]
struct ViewState {
    zoom: Option<f64>,
    pan: f64,
    archived: bool,
    selected: Option<String>,
    row_offset: usize,
}

pub fn draw(
    request: &PanelRenderRequest,
    sessions: &[SessionInfo],
    records: &[SessionActivity],
    partial: bool,
) -> PanelRenderResponse {
    let mut state: ViewState = serde_json::from_value(request.state.clone()).unwrap_or_default();
    let mut zoom = state.zoom.unwrap_or(1.).clamp(1., 64.);
    match &request.event {
        Some(UiInputEvent::Scroll { delta_x, delta_y }) => {
            zoom = (zoom * (1. + (-*delta_y as f64 / 500.).clamp(-0.5, 0.5))).clamp(1., 64.);
            state.pan = (state.pan + *delta_x as f64 / 500.).clamp(0., 1.);
        }
        Some(UiInputEvent::Key {
            key, pressed: true, ..
        }) if key.eq_ignore_ascii_case("r") => {
            zoom = 1.;
            state.pan = 0.;
            state.row_offset = 0;
        }
        Some(UiInputEvent::Key {
            key, pressed: true, ..
        }) if key.eq_ignore_ascii_case("a") => state.archived = !state.archived,
        Some(UiInputEvent::Key {
            key, pressed: true, ..
        }) if key == "ArrowLeft" => state.pan = (state.pan - 0.1).max(0.),
        Some(UiInputEvent::Key {
            key, pressed: true, ..
        }) if key == "ArrowRight" => state.pan = (state.pan + 0.1).min(1.),
        Some(UiInputEvent::Key {
            key, pressed: true, ..
        }) if key == "ArrowDown" => state.row_offset = state.row_offset.saturating_add(1),
        Some(UiInputEvent::Key {
            key, pressed: true, ..
        }) if key == "ArrowUp" => state.row_offset = state.row_offset.saturating_sub(1),
        Some(UiInputEvent::SemanticsAction { node_id, .. }) => {
            state.selected = Some(node_id.clone())
        }
        _ => {}
    }
    state.zoom = Some(zoom);
    let spans = project(records);
    let width = request.viewport.width;
    let height = request.viewport.height;
    let label_width = (width * 0.24).clamp(80., 190.);
    let plot_width = (width - label_width - 16.).max(1.);
    let minimum = spans.iter().map(|s| s.start).min().unwrap_or(0);
    let maximum = spans
        .iter()
        .map(|s| s.end.unwrap_or(s.start))
        .max()
        .unwrap_or(minimum + 1000)
        .max(minimum + 1000);
    let total = (maximum - minimum) as f64;
    let visible = total / zoom;
    let start = minimum as f64 + state.pan * (total - visible);
    let x = |millis: i64| label_width + ((millis as f64 - start) / visible) as f32 * plot_width;
    let mut frame = UiWidgetFrame::default();
    frame.commands.push(UiPaintCommand::Clear {
        color: request.theme.background,
    });
    text(
        &mut frame,
        rect(12., 6., width - 24., 24.),
        "Agent activity",
        request.theme.foreground,
        16.,
    );
    text(
        &mut frame,
        rect(12., 32., width - 24., 20.),
        format!(
            "{} agents · {} recorded events{} · wheel: zoom · ←/→ pan · ↑/↓ lanes · A: archives · R: reset",
            sessions
                .iter()
                .filter(|s| s.session_id != "conversation-events")
                .count(),
            spans.len(),
            if partial { " · partial history" } else { "" }
        ),
        request.theme.muted_foreground,
        11.,
    );
    for tick in 0..=4 {
        let offset = visible * tick as f64 / 4.;
        text(
            &mut frame,
            rect(
                (label_width + plot_width * tick as f32 / 4.).min(width - 75.),
                55.,
                75.,
                18.,
            ),
            format!("+{:.2}s", (start - minimum as f64 + offset) / 1000.),
            request.theme.muted_foreground,
            10.,
        );
    }
    let visible_sessions: Vec<_> = sessions
        .iter()
        .filter(|s| state.archived || !s.archived)
        .collect();
    // Pack overlapping recorded calls into separate tracks in each agent lane.
    // Layout uses screen intervals so even zero-duration markers remain selectable.
    let mut layout = Vec::new();
    let mut total_height = 0f32;
    for session in &visible_sessions {
        let mut ends = Vec::<f32>::new();
        let mut placed = Vec::new();
        for span in spans.iter().filter(|s| s.session_id == session.session_id) {
            let left = x(span.start);
            let right = x(span.end.unwrap_or(maximum)).max(left + 5.);
            if right < label_width || left > width - 16. {
                continue;
            }
            let track = ends
                .iter()
                .position(|end| *end + 3. <= left)
                .unwrap_or(ends.len());
            if track == ends.len() {
                ends.push(right);
            } else {
                ends[track] = right;
            }
            placed.push((span, track));
        }
        let lane_height = (ends.len().max(1) as f32 * 29. + 8.).max(42.);
        layout.push((*session, total_height, placed, lane_height));
        total_height += lane_height;
    }
    let plot_bottom = (height - 68.).max(90.);
    let max_offset = ((total_height - (plot_bottom - 80.)).max(0.) / 42.).ceil() as usize;
    state.row_offset = state.row_offset.min(max_offset);
    let scroll = state.row_offset as f32 * 42.;
    let mut hits = Vec::new();
    frame.commands.push(UiPaintCommand::PushClipRect {
        rect: rect(0., 78., width, plot_bottom - 78.),
    });
    for (session, offset, placed, lane_height) in &layout {
        let y = 80. + offset - scroll;
        if y + lane_height < 78. || y > plot_bottom {
            continue;
        }
        text(
            &mut frame,
            rect(12., y, label_width - 18., 18.),
            format!(
                "{}{}",
                if session.relationship.is_some() {
                    "↳ "
                } else {
                    ""
                },
                session.label
            ),
            request.theme.foreground,
            12.,
        );
        text(
            &mut frame,
            rect(12., y + 18., label_width - 18., 15.),
            if session.session_id == "conversation-events" {
                "Human and system messages"
            } else if session.archived {
                "Archived"
            } else if session.relationship.is_some() {
                "Delegated session"
            } else {
                "Channel participant"
            },
            request.theme.muted_foreground,
            10.,
        );
        frame.commands.push(UiPaintCommand::FillRect {
            rect: rect(label_width, y + 30., plot_width, 1.),
            color: request.theme.surface,
        });
        for (span, track) in placed {
            let y = y + *track as f32 * 29.;
            if y < 78. || y + 27. > plot_bottom {
                continue;
            }
            let left = x(span.start).max(label_width);
            let right = x(span.end.unwrap_or(maximum)).min(width - 16.);
            if right < label_width || left > width - 16. {
                continue;
            }
            let r = rect(left, y + 2., (right - left).max(5.), 25.);
            let color = if matches!(span.status.as_str(), "failed" | "blocked" | "cancelled") {
                request.theme.danger
            } else if span.status == "message" {
                request.theme.muted_foreground
            } else {
                request.theme.accent
            };
            frame
                .commands
                .push(UiPaintCommand::FillRect { rect: r, color });
            let id = format!("call-{}-{}", span.session_id, span.call_id);
            frame.semantics.push(UiSemanticsNode {
                id: id.clone(),
                parent_id: None,
                role: UiSemanticsRole::Button,
                bounds: r,
                name: Some(format!(
                    "{}: {} ({})",
                    session.label, span.label, span.status
                )),
                value: None,
                actions: vec![UiSemanticsAction::Activate],
            });
            hits.push((id.clone(), r));
            if r.width > 45. {
                text(
                    &mut frame,
                    rect(left + 4., y + 5., r.width - 8., 18.),
                    span.label.clone(),
                    request.theme.background,
                    10.,
                );
            }
            for target in &span.targets {
                let target = if target == "parent" {
                    session
                        .relationship
                        .as_ref()
                        .map(|r| r.parent_session_id.as_str())
                } else {
                    Some(target.as_str())
                };
                if let Some((_, target_y, _, _)) = layout.iter().find(|(s, _, _, _)| {
                    Some(s.session_id.as_str()) == target
                        || s.aliases.iter().any(|alias| Some(alias.as_str()) == target)
                }) {
                    frame.commands.push(UiPaintCommand::StrokePath {
                        path: UiPath {
                            elements: vec![
                                UiPathElement::MoveTo {
                                    point: UiPoint {
                                        x: left,
                                        y: y + 27.,
                                    },
                                },
                                UiPathElement::LineTo {
                                    point: UiPoint {
                                        x: left,
                                        y: (80. + target_y - scroll).clamp(78., plot_bottom),
                                    },
                                },
                            ],
                        },
                        color: request.theme.accent,
                        width: 1.,
                    });
                }
            }
        }
    }
    frame.commands.push(UiPaintCommand::PopClip);
    if let Some(UiInputEvent::Pointer {
        phase: UiPointerPhase::Up,
        position,
        ..
    }) = &request.event
    {
        state.selected = hits
            .iter()
            .rev()
            .find(|(_, r)| {
                position.x >= r.x
                    && position.x <= r.x + r.width
                    && position.y >= r.y
                    && position.y <= r.y + r.height
            })
            .map(|(id, _)| id.clone());
    }
    let detail = state
        .selected
        .as_ref()
        .and_then(|id| {
            spans
                .iter()
                .find(|s| format!("call-{}-{}", s.session_id, s.call_id) == *id)
        })
        .map(|s| {
            format!(
                "{} · {} · {}\n{}",
                s.label,
                s.status,
                if !s.known_start {
                    "Start outside loaded history".into()
                } else {
                    s.end
                        .map(|end| {
                            if end >= s.start {
                                format!("{} ms", end - s.start)
                            } else {
                                "Clock changed; duration unavailable".into()
                            }
                        })
                        .unwrap_or_else(|| {
                            if partial {
                                "End outside loaded history or still active".into()
                            } else {
                                "In progress".into()
                            }
                        })
                },
                s.detail
            )
        })
        .unwrap_or_else(|| {
            if spans.is_empty() {
                "No recorded interactions in this conversation.".into()
            } else {
                "Select an interaction to inspect its status and arguments.".into()
            }
        });
    text(
        &mut frame,
        rect(12., (height - 58.).max(80.), width - 24., 54.),
        detail,
        request.theme.foreground,
        12.,
    );
    PanelRenderResponse {
        frame,
        state: serde_json::to_value(state).unwrap(),
    }
}

fn render(
    request: PanelRenderRequest,
    host: &InvocationHost,
) -> Result<PanelRenderResponse, String> {
    let client = unsafe { SessionClient::new(host) };
    let mut sessions: Vec<SessionInfo> = client
        .request(&SessionRequest::List {
            include_archived: true,
            include_related: true,
        })
        .map_err(|e| e.to_string())?;
    let mut records = vec![];
    let mut partial = sessions.len() > 64;
    for session in sessions.iter().take(64) {
        let page: SessionActivityPage = client
            .request(&SessionRequest::Activity {
                session_id: session.session_id.clone(),
                after_sequence: None,
                tail: true,
                limit: 128,
            })
            .map_err(|e| e.to_string())?;
        partial |= page.has_more;
        records.extend(page.records);
    }
    let messages: Vec<ConversationMessage> = client
        .request(&SessionRequest::Messages {
            after_sequence: None,
            limit: 256,
        })
        .map_err(|e| e.to_string())?;
    partial |= messages.len() == 256;
    if messages
        .iter()
        .any(|message| message.sender_session_id.is_none())
    {
        sessions.insert(
            0,
            SessionInfo {
                session_id: "conversation-events".into(),
                label: "Conversation".into(),
                aliases: vec![],
                relationship: None,
                archived: false,
            },
        );
    }
    for message in messages {
        let sender = message
            .sender_session_id
            .unwrap_or_else(|| "conversation-events".into());
        {
            records.push(SessionActivity{session_id:sender,sequence:message.sequence,recorded_at_unix_ms:message.recorded_at_unix_ms,operation_id:message.operation_id,tool_call_id:Some(format!("message-{}",message.message_id)),item:serde_json::json!({"kind":"conversation_message","content":message.content,"recipients":message.recipients,"reply_to":message.reply_to})});
        }
    }
    Ok(draw(&request, &sessions, &records, partial))
}
pub fn descriptor() -> NativeModuleDescriptor {
    let contribution = UiExtensionContribution {
        contribution_id: "module.agent-timeline.conversation".into(),
        extension_point_id: CONVERSATION_PANELS.into(),
        extension_point_version: "1.0.0".into(),
        group: "activity".into(),
        order: 0,
        before: vec![],
        after: vec![],
        when: vec![],
        payload: UiContributionPayload::Declarative {
            schema_id: PANEL_SCHEMA.into(),
            payload_sobj: encode_sobj(&SessionPanelDefinition {
                label: "Agent timeline".into(),
                service_id: SERVICE.into(),
            })
            .unwrap(),
        },
    };
    NativeModuleDescriptor {
        module_id: MODULE.into(),
        module_version: env!("CARGO_PKG_VERSION").into(),
        owner: MODULE.into(),
        native_abi_version: SINOMO_NATIVE_MODULE_ABI_VERSION,
        backend: NativeModuleBackend::NativeDylib,
        interface_version: "sinomo.ui.session-panel.v1".into(),
        platform_triple: format!(
            "{}-{}-{}",
            std::env::consts::ARCH,
            std::env::consts::OS,
            std::env::consts::FAMILY
        ),
        supported_operations: vec![PANEL_RENDER.into()],
        capabilities: vec![PANEL_RENDER.into()],
        surfaces: NativeModuleSurfaces {
            ui_contributions: vec![contribution],
            capability_exports: vec![PANEL_RENDER.into()],
            ..Default::default()
        },
        component: ComponentDescriptor::new(MODULE, env!("CARGO_PKG_VERSION"))
            .with_display_name("Agent timeline")
            .with_offer(
                ServiceOffer::new(SERVICE, PANEL_INTERFACE, "1.0.0")
                    .with_operation(ServiceOperationOffer::new(PANEL_RENDER)),
            )
            .require(
                ServiceRequirement::new(SESSION_INTERFACE, SESSION_VERSION)
                    .with_operation(SESSION_ACCESS),
            ),
    }
}
#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_api_version() -> u32 {
    SINOMO_NATIVE_MODULE_ABI_VERSION
}
#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_descriptor(
    out: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    write(out, &descriptor())
}
#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_init(host: SinomoModuleHostApiV2) -> SinomoModuleStatusCode {
    if host.abi_version != SINOMO_NATIVE_MODULE_ABI_VERSION {
        return SINOMO_MODULE_STATUS_UNSUPPORTED_ABI;
    }
    if LIFECYCLE.initialize() {
        SINOMO_MODULE_STATUS_OK
    } else {
        SINOMO_MODULE_STATUS_RUNTIME_ERROR
    }
}
#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_shutdown() -> SinomoModuleStatusCode {
    if LIFECYCLE.begin_shutdown() {
        SINOMO_MODULE_STATUS_OK
    } else {
        SINOMO_MODULE_STATUS_RUNTIME_ERROR
    }
}
#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_free_buffer(buffer: SinomoModuleByteBuffer) {
    unsafe {
        drop(SinomoModuleByteBuffer::into_vec(buffer));
    }
}
#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_invoke(
    request: SinomoModuleByteSlice,
    out: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    invoke(request, None, out)
}
#[unsafe(no_mangle)]
pub extern "C" fn sinomo_module_invoke_with_host_v1(
    request: SinomoModuleByteSlice,
    host: InvocationHost,
    out: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    invoke(request, Some(&host), out)
}
fn invoke(
    request: SinomoModuleByteSlice,
    host: Option<&InvocationHost>,
    out: *mut SinomoModuleByteBuffer,
) -> SinomoModuleStatusCode {
    let Some(_lease) = LIFECYCLE.begin_invocation() else {
        return SINOMO_MODULE_STATUS_RUNTIME_ERROR;
    };
    let response = std::panic::catch_unwind(|| -> Result<NativeModuleResponse, String> {
        let request: NativeModuleRequest =
            decode_sobj(unsafe { request.as_slice() }).map_err(|e| e.to_string())?;
        match request {
            NativeModuleRequest::ProbeCapabilities(_) => Ok(NativeModuleResponse::Capabilities(
                NativeModuleCapabilitiesResponse {
                    descriptor: descriptor(),
                },
            )),
            NativeModuleRequest::Invoke(request) => {
                if request.operation != PANEL_RENDER {
                    return Err("unsupported timeline operation".into());
                }
                let payload = decode_sobj(&request.payload_sobj).map_err(|e| e.to_string())?;
                let response = render(payload, host.ok_or("session host unavailable")?)?;
                validate_response(&response)?;
                Ok(NativeModuleResponse::Invocation(
                    NativeModuleInvocationResponse {
                        operation: request.operation,
                        payload_sobj: encode_sobj(&response).map_err(|e| e.to_string())?,
                        response_body: None,
                    },
                ))
            }
        }
    })
    .unwrap_or_else(|_| Err("timeline provider panicked".into()));
    write(
        out,
        &response.unwrap_or_else(|detail| {
            NativeModuleResponse::Error(NativeModuleErrorResponse {
                code: "timeline".into(),
                detail,
            })
        }),
    )
}
fn write<T: Serialize>(out: *mut SinomoModuleByteBuffer, value: &T) -> SinomoModuleStatusCode {
    if out.is_null() {
        return SINOMO_MODULE_STATUS_INVALID_ARGUMENT;
    }
    match encode_sobj(value) {
        Ok(bytes) => {
            unsafe {
                *out = SinomoModuleByteBuffer::from_vec(bytes);
            }
            SINOMO_MODULE_STATUS_OK
        }
        Err(_) => SINOMO_MODULE_STATUS_RUNTIME_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record(
        session: &str,
        seq: u64,
        time: i64,
        call: &str,
        item: serde_json::Value,
    ) -> SessionActivity {
        SessionActivity {
            session_id: session.into(),
            sequence: seq,
            recorded_at_unix_ms: time,
            operation_id: Some(format!("op-{seq}")),
            tool_call_id: Some(call.into()),
            item,
        }
    }
    fn request() -> PanelRenderRequest {
        let color = UiColor {
            red: 120,
            green: 150,
            blue: 180,
            alpha: 255,
        };
        PanelRenderRequest {
            viewport: UiViewport {
                width: 960.,
                height: 360.,
                scale_factor: 1.,
            },
            theme: UiThemeSnapshot {
                mode: UiThemeMode::Dark,
                accent: color,
                foreground: color,
                muted_foreground: color,
                background: UiColor::default(),
                surface: color,
                danger: color,
            },
            event: None,
            state: serde_json::Value::Null,
        }
    }
    fn session(id: &str) -> SessionInfo {
        SessionInfo {
            session_id: id.into(),
            label: id.into(),
            aliases: vec![],
            relationship: None,
            archived: false,
        }
    }
    #[test]
    fn joins_by_session_and_call_identity_across_operations() {
        let records = vec![
            record(
                "one",
                1,
                100,
                "same",
                serde_json::json!({"kind":"tool_call","call":{"tool":"read_file","input":{}}}),
            ),
            record(
                "two",
                1,
                110,
                "same",
                serde_json::json!({"kind":"tool_call","call":{"tool":"exec","input":{}}}),
            ),
            record(
                "one",
                2,
                120,
                "same",
                serde_json::json!({"kind":"tool_result","status":"succeeded","output":{"detached":true,"session_id":"two"}}),
            ),
            record(
                "two",
                2,
                130,
                "same",
                serde_json::json!({"kind":"tool_result","status":"failed","output":{}}),
            ),
            record(
                "one",
                3,
                150,
                "same",
                serde_json::json!({"kind":"async_task_update","update":{"status":"completed"}}),
            ),
        ];
        let spans = project(&records);
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].end, Some(150));
        assert_eq!(spans[0].targets, vec!["two"]);
        assert_eq!(spans[1].status, "failed");
        assert_eq!(spans[1].end, Some(130));
    }
    #[test]
    fn concurrent_calls_use_distinct_tracks_and_pointer_selection() {
        let records = vec![
            record(
                "one",
                1,
                100,
                "a",
                serde_json::json!({"kind":"tool_call","call":{"tool":"first","input":{}}}),
            ),
            record(
                "one",
                2,
                100,
                "b",
                serde_json::json!({"kind":"tool_call","call":{"tool":"second","input":{}}}),
            ),
        ];
        let sessions = vec![session("one")];
        let first = draw(&request(), &sessions, &records, false);
        validate_response(&first).unwrap();
        assert_eq!(first.frame.semantics.len(), 2);
        assert_ne!(
            first.frame.semantics[0].bounds.y,
            first.frame.semantics[1].bounds.y
        );
        let bounds = first.frame.semantics[1].bounds;
        let mut req = request();
        req.event = Some(UiInputEvent::Pointer {
            phase: UiPointerPhase::Up,
            pointer_id: 1,
            position: UiPoint {
                x: bounds.x + 1.,
                y: bounds.y + 1.,
            },
            buttons: 0,
        });
        let selected = draw(&req, &sessions, &records, false);
        assert_eq!(selected.state["selected"], "call-one-b");
    }
    #[test]
    fn journal_sequence_preserves_correlation_when_the_wall_clock_moves_backwards() {
        let records = vec![
            record(
                "one",
                1,
                100,
                "a",
                serde_json::json!({"kind":"tool_call","call":{"tool":"first","input":{}}}),
            ),
            record(
                "one",
                2,
                90,
                "a",
                serde_json::json!({"kind":"tool_result","status":"succeeded","output":{}}),
            ),
        ];
        let spans = project(&records);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].start, 100);
        assert_eq!(spans[0].end, Some(90));
        assert_eq!(spans[0].status, "succeeded");
    }

    #[test]
    fn incomplete_windows_do_not_invent_call_start_or_duration() {
        let spans = project(&[record(
            "one",
            900,
            5000,
            "old",
            serde_json::json!({"kind":"tool_result","tool_name":"old tool","status":"succeeded"}),
        )]);
        assert!(!spans[0].known_start);
    }
    #[test]
    fn empty_small_and_archived_views_are_valid() {
        let mut req = request();
        req.viewport.width = 160.;
        req.viewport.height = 180.;
        let mut archived = session("archive");
        archived.archived = true;
        let response = draw(&req, &[archived], &[], false);
        validate_response(&response).unwrap();
        assert!(response.frame.semantics.is_empty());
        descriptor().component.validate().unwrap();
    }
}
