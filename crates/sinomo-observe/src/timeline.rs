use crate::ObserveError;
use crate::runtime::observe_runtime_process_identity;
use serde::{Deserialize, Serialize};
use sinomo_core::network::{
    NetworkTimelineEvent, NetworkTimelineSnapshot, network_timeline_capture_is_active,
    network_timeline_stage_name, start_network_timeline_capture, stop_network_timeline_capture,
};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use time::OffsetDateTime;

pub const OBSERVE_NETWORK_TIMELINE_DEFAULT_DURATION_SECONDS: u64 = 30;
pub const OBSERVE_NETWORK_TIMELINE_MIN_DURATION_SECONDS: u64 = 1;
pub const OBSERVE_NETWORK_TIMELINE_MAX_DURATION_SECONDS: u64 = 120;
pub const OBSERVE_NETWORK_TIMELINE_DEFAULT_MAX_EVENTS: usize = 4 * 1024;
pub const OBSERVE_NETWORK_TIMELINE_MAX_EVENTS: usize = 32 * 1024;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ObserveNetworkTimelineAction {
    Start,
    Stop,
    #[default]
    Status,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ObserveNetworkTimelineRequest {
    #[serde(default)]
    pub action: ObserveNetworkTimelineAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_events: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ObserveNetworkTimelineEvent {
    pub sequence: u64,
    pub monotonic_ns: u64,
    pub duration_ns: u64,
    pub trace_id: String,
    pub span_id: String,
    pub connection_id: u64,
    pub call_id: u64,
    pub frame_id: u64,
    pub stage: u16,
    pub stage_name: String,
    pub flags: u16,
    pub bytes: u32,
    pub aux: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ObserveNetworkTimelineResponse {
    pub active: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capture_id: Option<String>,
    pub process_role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    pub process_id: u32,
    #[serde(with = "time::serde::rfc3339")]
    pub captured_at: OffsetDateTime,
    pub started_monotonic_ns: u64,
    pub captured_monotonic_ns: u64,
    pub max_events: usize,
    pub event_count: usize,
    pub dropped_events: u64,
    pub overflowed: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<ObserveNetworkTimelineEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

struct ManagedTimelineCapture {
    capture_id: String,
    max_events: usize,
    snapshot: Option<NetworkTimelineSnapshot>,
    captured_at: Option<OffsetDateTime>,
    error: Option<String>,
}

static NETWORK_TIMELINE_CAPTURE: OnceLock<Mutex<Option<ManagedTimelineCapture>>> = OnceLock::new();

fn managed_capture() -> &'static Mutex<Option<ManagedTimelineCapture>> {
    NETWORK_TIMELINE_CAPTURE.get_or_init(|| Mutex::new(None))
}

pub fn handle_observe_network_timeline(
    request: ObserveNetworkTimelineRequest,
) -> Result<ObserveNetworkTimelineResponse, ObserveError> {
    validate_request(&request)?;
    match request.action {
        ObserveNetworkTimelineAction::Start => start_timeline(request),
        ObserveNetworkTimelineAction::Stop => stop_timeline(request.capture_id.as_deref()),
        ObserveNetworkTimelineAction::Status => timeline_status(),
    }
}

fn start_timeline(
    request: ObserveNetworkTimelineRequest,
) -> Result<ObserveNetworkTimelineResponse, ObserveError> {
    let duration_seconds = request
        .duration_seconds
        .unwrap_or(OBSERVE_NETWORK_TIMELINE_DEFAULT_DURATION_SECONDS);
    let max_events = request
        .max_events
        .unwrap_or(OBSERVE_NETWORK_TIMELINE_DEFAULT_MAX_EVENTS);
    let capture_id = request.capture_id.unwrap_or_else(|| {
        format!(
            "network-{}-{}",
            OffsetDateTime::now_utc().unix_timestamp_nanos(),
            std::process::id()
        )
    });
    validate_capture_id(&capture_id)?;

    let mut managed = managed_capture()
        .lock()
        .map_err(|_| timeline_lock_error())?;
    if let Some(active) = managed.as_ref() {
        return Err(ObserveError::Runtime {
            detail: format!(
                "network timeline capture `{}` has not been collected",
                active.capture_id
            ),
        });
    }
    start_network_timeline_capture(max_events).map_err(timeline_runtime_error)?;
    *managed = Some(ManagedTimelineCapture {
        capture_id: capture_id.clone(),
        max_events,
        snapshot: None,
        captured_at: None,
        error: None,
    });
    drop(managed);

    if let Err(source) = std::thread::Builder::new()
        .name("sinomo-network-timeline-watchdog".to_string())
        .stack_size(256 * 1024)
        .spawn(move || {
            std::thread::sleep(Duration::from_secs(duration_seconds));
            finish_timeline_from_watchdog(&capture_id);
        })
    {
        let _ = stop_network_timeline_capture();
        if let Ok(mut managed) = managed_capture().lock() {
            *managed = None;
        }
        return Err(ObserveError::Runtime {
            detail: format!("failed to spawn network timeline watchdog: {source}"),
        });
    }
    timeline_status()
}

fn finish_timeline_from_watchdog(capture_id: &str) {
    let Ok(mut managed) = managed_capture().lock() else {
        return;
    };
    let Some(capture) = managed
        .as_mut()
        .filter(|capture| capture.capture_id == capture_id && capture.snapshot.is_none())
    else {
        return;
    };
    match stop_network_timeline_capture() {
        Ok(snapshot) => {
            capture.snapshot = Some(snapshot);
            capture.captured_at = Some(OffsetDateTime::now_utc());
        }
        Err(error) => capture.error = Some(error.to_string()),
    }
}

fn stop_timeline(
    expected_capture_id: Option<&str>,
) -> Result<ObserveNetworkTimelineResponse, ObserveError> {
    let mut managed = managed_capture()
        .lock()
        .map_err(|_| timeline_lock_error())?;
    let Some(capture) = managed.as_mut() else {
        return Err(ObserveError::Runtime {
            detail: "network timeline capture is not active".to_string(),
        });
    };
    if let Some(expected_capture_id) = expected_capture_id
        && capture.capture_id != expected_capture_id
    {
        return Err(ObserveError::Runtime {
            detail: format!(
                "network timeline capture mismatch: active is `{}`",
                capture.capture_id
            ),
        });
    }
    if capture.snapshot.is_none() && capture.error.is_none() {
        match stop_network_timeline_capture() {
            Ok(snapshot) => {
                capture.snapshot = Some(snapshot);
                capture.captured_at = Some(OffsetDateTime::now_utc());
            }
            Err(error) => capture.error = Some(error.to_string()),
        }
    }
    let capture = managed.take().expect("managed capture exists");
    Ok(response_from_capture(capture, true))
}

fn timeline_status() -> Result<ObserveNetworkTimelineResponse, ObserveError> {
    let managed = managed_capture()
        .lock()
        .map_err(|_| timeline_lock_error())?;
    let Some(capture) = managed.as_ref() else {
        return Ok(empty_response());
    };
    let (started_monotonic_ns, captured_monotonic_ns, event_count, dropped_events, overflowed) =
        capture
            .snapshot
            .as_ref()
            .map(|snapshot| {
                (
                    snapshot.started_monotonic_ns,
                    snapshot.captured_monotonic_ns,
                    snapshot.events.len(),
                    snapshot.dropped_events,
                    snapshot.overflowed,
                )
            })
            .unwrap_or((0, 0, 0, 0, false));
    let (process_role, node_id) = observe_runtime_process_identity();
    Ok(ObserveNetworkTimelineResponse {
        active: capture.snapshot.is_none()
            && capture.error.is_none()
            && network_timeline_capture_is_active(),
        capture_id: Some(capture.capture_id.clone()),
        process_role,
        node_id,
        process_id: std::process::id(),
        captured_at: capture.captured_at.unwrap_or_else(OffsetDateTime::now_utc),
        started_monotonic_ns,
        captured_monotonic_ns,
        max_events: capture.max_events,
        event_count,
        dropped_events,
        overflowed,
        events: Vec::new(),
        error: capture.error.clone(),
    })
}

fn response_from_capture(
    capture: ManagedTimelineCapture,
    include_events: bool,
) -> ObserveNetworkTimelineResponse {
    let (process_role, node_id) = observe_runtime_process_identity();
    let captured_at = capture.captured_at.unwrap_or_else(OffsetDateTime::now_utc);
    let mut response = ObserveNetworkTimelineResponse {
        active: false,
        capture_id: Some(capture.capture_id),
        process_role,
        node_id,
        process_id: std::process::id(),
        captured_at,
        started_monotonic_ns: 0,
        captured_monotonic_ns: 0,
        max_events: capture.max_events,
        event_count: 0,
        dropped_events: 0,
        overflowed: false,
        events: Vec::new(),
        error: capture.error,
    };
    if let Some(snapshot) = capture.snapshot {
        let mut captured = observe_network_timeline_response_from_snapshot_at(
            response.capture_id.clone(),
            response.process_role.clone(),
            response.node_id.clone(),
            captured_at,
            snapshot,
        );
        if !include_events {
            captured.events.clear();
        }
        response = captured;
    }
    response
}

/// Converts a completed process-local capture after the hot path has stopped.
/// This is also used by short-lived diagnostic clients that cannot be reached
/// through the long-lived observe service while their request is in flight.
pub fn observe_network_timeline_response_from_snapshot(
    capture_id: Option<String>,
    process_role: impl Into<String>,
    node_id: Option<String>,
    snapshot: NetworkTimelineSnapshot,
) -> ObserveNetworkTimelineResponse {
    observe_network_timeline_response_from_snapshot_at(
        capture_id,
        process_role,
        node_id,
        OffsetDateTime::now_utc(),
        snapshot,
    )
}

fn observe_network_timeline_response_from_snapshot_at(
    capture_id: Option<String>,
    process_role: impl Into<String>,
    node_id: Option<String>,
    captured_at: OffsetDateTime,
    snapshot: NetworkTimelineSnapshot,
) -> ObserveNetworkTimelineResponse {
    ObserveNetworkTimelineResponse {
        active: false,
        capture_id,
        process_role: process_role.into(),
        node_id,
        process_id: std::process::id(),
        captured_at,
        started_monotonic_ns: snapshot.started_monotonic_ns,
        captured_monotonic_ns: snapshot.captured_monotonic_ns,
        max_events: snapshot.max_events,
        event_count: snapshot.events.len(),
        dropped_events: snapshot.dropped_events,
        overflowed: snapshot.overflowed,
        events: snapshot.events.into_iter().map(event_record).collect(),
        error: None,
    }
}

fn empty_response() -> ObserveNetworkTimelineResponse {
    let (process_role, node_id) = observe_runtime_process_identity();
    ObserveNetworkTimelineResponse {
        active: false,
        capture_id: None,
        process_role,
        node_id,
        process_id: std::process::id(),
        captured_at: OffsetDateTime::now_utc(),
        started_monotonic_ns: 0,
        captured_monotonic_ns: 0,
        max_events: 0,
        event_count: 0,
        dropped_events: 0,
        overflowed: false,
        events: Vec::new(),
        error: None,
    }
}

fn event_record(event: NetworkTimelineEvent) -> ObserveNetworkTimelineEvent {
    ObserveNetworkTimelineEvent {
        sequence: event.sequence,
        monotonic_ns: event.monotonic_ns,
        duration_ns: event.duration_ns,
        trace_id: encode_hex(&event.trace_id),
        span_id: encode_hex(&event.span_id),
        connection_id: event.connection_id,
        call_id: event.call_id,
        frame_id: event.frame_id,
        stage: event.stage,
        stage_name: network_timeline_stage_name(event.stage).to_string(),
        flags: event.flags,
        bytes: event.bytes,
        aux: event.aux,
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn validate_request(request: &ObserveNetworkTimelineRequest) -> Result<(), ObserveError> {
    if let Some(capture_id) = request.capture_id.as_deref() {
        validate_capture_id(capture_id)?;
    }
    if request.action != ObserveNetworkTimelineAction::Start
        && (request.duration_seconds.is_some() || request.max_events.is_some())
    {
        return Err(ObserveError::Parse {
            detail: "network timeline duration and max-events are only valid for start".to_string(),
        });
    }
    if let Some(duration) = request.duration_seconds
        && !(OBSERVE_NETWORK_TIMELINE_MIN_DURATION_SECONDS
            ..=OBSERVE_NETWORK_TIMELINE_MAX_DURATION_SECONDS)
            .contains(&duration)
    {
        return Err(ObserveError::Parse {
            detail: format!(
                "network timeline duration must be {}-{} seconds",
                OBSERVE_NETWORK_TIMELINE_MIN_DURATION_SECONDS,
                OBSERVE_NETWORK_TIMELINE_MAX_DURATION_SECONDS
            ),
        });
    }
    if let Some(max_events) = request.max_events
        && !(1..=OBSERVE_NETWORK_TIMELINE_MAX_EVENTS).contains(&max_events)
    {
        return Err(ObserveError::Parse {
            detail: format!(
                "network timeline max-events must be 1-{}",
                OBSERVE_NETWORK_TIMELINE_MAX_EVENTS
            ),
        });
    }
    Ok(())
}

fn validate_capture_id(capture_id: &str) -> Result<(), ObserveError> {
    let valid = !capture_id.is_empty()
        && capture_id.len() <= 64
        && !capture_id.starts_with('.')
        && capture_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if valid {
        Ok(())
    } else {
        Err(ObserveError::Parse {
            detail: "network timeline capture id must be 1-64 path-safe ASCII characters"
                .to_string(),
        })
    }
}

fn timeline_lock_error() -> ObserveError {
    ObserveError::Runtime {
        detail: "network timeline state lock is poisoned".to_string(),
    }
}

fn timeline_runtime_error(error: impl ToString) -> ObserveError {
    ObserveError::Runtime {
        detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeline_event_conversion_names_stages_and_hex_ids() {
        let event = NetworkTimelineEvent {
            trace_id: [0xab; 16],
            span_id: [0xcd; 8],
            stage: sinomo_core::network::network_timeline_stage::TX_QUEUE_WAIT,
            ..NetworkTimelineEvent::default()
        };
        let record = event_record(event);
        assert_eq!(record.trace_id, "abababababababababababababababab");
        assert_eq!(record.span_id, "cdcdcdcdcdcdcdcd");
        assert_eq!(record.stage_name, "channel.tx-queue-wait");
    }

    #[test]
    fn timeline_request_bounds_are_validated_without_allocating_capture() {
        let error = validate_request(&ObserveNetworkTimelineRequest {
            action: ObserveNetworkTimelineAction::Start,
            duration_seconds: Some(OBSERVE_NETWORK_TIMELINE_MAX_DURATION_SECONDS + 1),
            ..ObserveNetworkTimelineRequest::default()
        })
        .expect_err("unbounded duration must fail");
        assert!(matches!(error, ObserveError::Parse { .. }));

        let error = validate_request(&ObserveNetworkTimelineRequest {
            action: ObserveNetworkTimelineAction::Status,
            max_events: Some(1),
            ..ObserveNetworkTimelineRequest::default()
        })
        .expect_err("status max-events must fail");
        assert!(matches!(error, ObserveError::Parse { .. }));
    }

    #[test]
    fn completed_capture_keeps_the_wall_anchor_from_snapshot_time() {
        let captured_at = OffsetDateTime::from_unix_timestamp(1_234).expect("timestamp");
        let response = response_from_capture(
            ManagedTimelineCapture {
                capture_id: "watchdog-capture".to_string(),
                max_events: 4,
                snapshot: Some(NetworkTimelineSnapshot {
                    started_monotonic_ns: 10,
                    captured_monotonic_ns: 20,
                    max_events: 4,
                    events: Vec::new(),
                    dropped_events: 0,
                    overflowed: false,
                    active: false,
                }),
                captured_at: Some(captured_at),
                error: None,
            },
            true,
        );

        assert_eq!(response.captured_at, captured_at);
        assert_eq!(response.captured_monotonic_ns, 20);
    }
}
