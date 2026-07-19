#!/usr/bin/env python3
"""Summarize bounded Sinomo network timeline captures.

Inputs may be raw ``ObserveNetworkTimelineResponse`` JSON documents or test
reports containing ``diagnostics.timeline_client_capture``. Multiple captures
are aligned onto wall time using ``captured_at`` and
``captured_monotonic_ns``. The script retains only compact normalized events,
and enforces hard file, aggregate-input, and event-count bounds.
"""

from __future__ import annotations

import argparse
import calendar
import io
import json
import math
import re
import sys
import tempfile
from collections import defaultdict
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO, TextIO


MAX_INPUT_FILE_BYTES = 8 * 1024 * 1024
MAX_TOTAL_INPUT_BYTES = 32 * 1024 * 1024
MAX_TIMELINE_EVENTS = 100_000
MAX_TIMELINE_PRINT_EVENTS = 1_000
LEGACY_READINESS_PAIR_MAX_NS = 100_000_000

RFC3339_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})T"
    r"(?P<time>\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<fraction>\d+))?"
    r"(?P<offset>Z|[+-]\d{2}:\d{2})$"
)
TRACE_ID_RE = re.compile(r"^[0-9a-fA-F]{32}$")

FLAG_REQUEST = 1 << 0
FLAG_RESPONSE = 1 << 1
FLAG_SUCCESS = 1 << 4
FLAG_DIRECTION_MASK = FLAG_REQUEST | FLAG_RESPONSE
STAGE_FRAME_WRITE = 4
STAGE_FRAME_INGRESS = 5
STAGE_HANDLER_DISPATCH = 9
STAGE_PENDING_RESOLVE = 11
STAGE_CALL_COMPLETE = 12
STAGE_CHANNEL_LOOKUP = 14
STAGE_ENDPOINT_OPEN = 21
STAGE_CHANNEL_CLOSE = 25
STAGE_FRAME_FIRST_READ = 26
STAGE_FRAME_READ = 27
STAGE_FRAME_ENCODE = 28
STAGE_FRAME_SOCKET_WRITE = 29
STAGE_FRAME_DECODE = 30
STAGE_FRAME_READY_TO_POLL = 31

CHANNEL_LOOKUP_OUTCOMES = {
    0: "incompatible",
    1: "available",
    2: "absent",
    3: "closed",
    4: "error",
}

DIRECT_CARRIER_GAP_LABELS = (
    (
        "client_to_caller",
        "client socket-write -> caller-daemon readiness wake",
    ),
    (
        "caller_to_target",
        "caller inner socket-write -> target-daemon readiness wake",
    ),
    (
        "target_to_caller",
        "target socket-write -> caller-daemon readiness wake",
    ),
    (
        "caller_to_client",
        "caller outer socket-write -> test-client readiness wake",
    ),
)

FORWARDED_CARRIER_GAP_LABELS = (
    (
        "client_to_caller",
        "client socket-write -> caller-daemon readiness wake",
    ),
    (
        "caller_to_relay",
        "caller socket-write -> relay-daemon readiness wake",
    ),
    (
        "relay_to_target",
        "relay socket-write -> target-daemon readiness wake",
    ),
    (
        "target_to_relay",
        "target socket-write -> relay-daemon readiness wake",
    ),
    (
        "relay_to_caller",
        "relay socket-write -> caller-daemon readiness wake",
    ),
    (
        "caller_to_client",
        "caller socket-write -> test-client readiness wake",
    ),
)

DIRECT_CARRIER_TOTAL_LABELS = (
    (
        "client_caller_round_trip",
        "client <-> caller hop round-trip gap total",
    ),
    (
        "caller_target_round_trip",
        "caller <-> target hop round-trip gap total",
    ),
    (
        "carrier_chain_total",
        "whole carrier-chain gap total (same trace)",
    ),
)

FORWARDED_CARRIER_TOTAL_LABELS = (
    (
        "client_caller_round_trip",
        "client <-> caller hop round-trip gap total",
    ),
    (
        "caller_relay_round_trip",
        "caller <-> relay hop round-trip gap total",
    ),
    (
        "relay_target_round_trip",
        "relay <-> target hop round-trip gap total",
    ),
    (
        "carrier_chain_total",
        "whole carrier-chain gap total (same trace)",
    ),
)


class SummaryError(ValueError):
    """Raised for invalid input or a violated resource bound."""


@dataclass(frozen=True, slots=True)
class TimelineEvent:
    process: str
    process_role: str
    node_id: str
    process_id: int
    capture_id: str
    sequence: int
    start_wall_ns: int
    end_wall_ns: int
    duration_ns: int
    trace_id: str
    span_id: str
    stage: int
    stage_name: str
    connection_id: int
    call_id: int
    frame_id: int
    byte_count: int
    flags: int
    aux: int


@dataclass(frozen=True, slots=True)
class CaptureInfo:
    source: str
    capture_id: str
    process: str
    event_count: int
    declared_event_count: int
    dropped_events: int
    overflowed: bool


@dataclass(slots=True)
class LoadedTimeline:
    events: list[TimelineEvent]
    captures: list[CaptureInfo]
    input_bytes: int = 0


@dataclass(frozen=True, slots=True)
class TraceAnalysis:
    trace_id: str
    client: TimelineEvent
    handler: TimelineEvent | None
    inner_call: TimelineEvent | None
    outbound_gap_ns: int | None
    return_gap_ns: int | None
    client_minus_handler_ns: int | None
    client_minus_inner_ns: int | None
    score_ns: int


@dataclass(frozen=True, slots=True)
class CarrierHop:
    source_process: str
    destination_process: str
    call_id: int
    source_connection_id: int
    destination_connection_id: int
    request_gap_ns: int
    response_gap_ns: int | None


@dataclass(slots=True)
class CarrierGapCollection:
    direct_samples: dict[str, list[int]]
    direct_total_samples: dict[str, list[int]]
    direct_count: int
    forwarded_samples: dict[str, list[int]]
    forwarded_total_samples: dict[str, list[int]]
    forwarded_count: int
    unclassified_count: int


@dataclass(slots=True)
class ReceiverReadinessProcess:
    wake_to_poll_samples_ns: list[int] = field(default_factory=list)
    poll_to_first_read_samples_ns: list[int] = field(default_factory=list)
    ready_count: int = 0
    first_read_count: int = 0
    exact_pair_count: int = 0
    legacy_pair_count: int = 0
    unmatched_ready_count: int = 0
    unmatched_first_read_count: int = 0


class InputBudget:
    def __init__(self) -> None:
        self.total_bytes = 0

    def load_json(self, path: str) -> Any:
        remaining = MAX_TOTAL_INPUT_BYTES - self.total_bytes
        if remaining <= 0:
            raise SummaryError(
                f"aggregate input exceeds {MAX_TOTAL_INPUT_BYTES} bytes"
            )
        limit = min(MAX_INPUT_FILE_BYTES, remaining)
        if path == "-":
            raw = self._read_bounded(sys.stdin.buffer, limit, "stdin")
        else:
            with Path(path).open("rb") as source:
                raw = self._read_bounded(source, limit, path)
        self.total_bytes += len(raw)
        try:
            return json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise SummaryError(f"{path}: invalid JSON: {error}") from error

    def _read_bounded(self, source: BinaryIO, limit: int, label: str) -> bytes:
        raw = source.read(limit + 1)
        if len(raw) > limit:
            if limit < MAX_INPUT_FILE_BYTES:
                raise SummaryError(
                    f"{label}: aggregate input exceeds {MAX_TOTAL_INPUT_BYTES} bytes"
                )
            raise SummaryError(
                f"{label}: input exceeds per-file limit of "
                f"{MAX_INPUT_FILE_BYTES} bytes"
            )
        return raw


def parse_rfc3339_ns(value: str) -> int:
    match = RFC3339_RE.fullmatch(value)
    if match is None:
        raise SummaryError(f"invalid RFC3339 timestamp: {value!r}")
    date_time = datetime.strptime(
        f"{match.group('date')}T{match.group('time')}",
        "%Y-%m-%dT%H:%M:%S",
    )
    seconds = calendar.timegm(date_time.timetuple())
    offset = match.group("offset")
    if offset != "Z":
        sign = 1 if offset[0] == "+" else -1
        hours, minutes = (int(part) for part in offset[1:].split(":"))
        seconds -= sign * (hours * 3600 + minutes * 60)
    fraction = (match.group("fraction") or "")[:9].ljust(9, "0")
    return seconds * 1_000_000_000 + int(fraction or "0")


def require_object(value: Any, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SummaryError(f"{context} must be a JSON object")
    return value


def require_array(value: Any, context: str) -> list[Any]:
    if not isinstance(value, list):
        raise SummaryError(f"{context} must be a JSON array")
    return value


def require_string(value: dict[str, Any], key: str, context: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result:
        raise SummaryError(f"{context} `{key}` must be a non-empty string")
    return result


def optional_string(value: Any, fallback: str) -> str:
    return value if isinstance(value, str) and value else fallback


def require_integer(value: dict[str, Any], key: str, context: str) -> int:
    result = value.get(key)
    if isinstance(result, bool) or not isinstance(result, int) or result < 0:
        raise SummaryError(f"{context} `{key}` must be a non-negative integer")
    return result


def integer_or(value: Any, fallback: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return fallback
    return value


def is_timeline_response(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and "captured_at" in value
        and "captured_monotonic_ns" in value
        and "events" in value
    )


def extract_captures(document: Any, source: str) -> list[tuple[str, dict[str, Any]]]:
    root = require_object(document, source)
    captures: list[tuple[str, dict[str, Any]]] = []
    if is_timeline_response(root):
        captures.append((source, root))

    diagnostics = root.get("diagnostics")
    if isinstance(diagnostics, dict):
        client_capture = diagnostics.get("timeline_client_capture")
        if client_capture is not None:
            capture = require_object(
                client_capture,
                f"{source} diagnostics.timeline_client_capture",
            )
            if not is_timeline_response(capture):
                raise SummaryError(
                    f"{source}: diagnostics.timeline_client_capture is not a "
                    "network timeline response"
                )
            captures.append((f"{source}#timeline_client_capture", capture))

    targets = root.get("targets")
    if isinstance(targets, list):
        for index, raw_target in enumerate(targets):
            target = require_object(raw_target, f"{source} targets[{index}]")
            response = target.get("response")
            if is_timeline_response(response):
                captures.append((f"{source}#targets[{index}]", response))

    if not captures:
        raise SummaryError(
            f"{source}: expected a network timeline response or "
            "diagnostics.timeline_client_capture"
        )
    return captures


def append_capture(
    loaded: LoadedTimeline,
    source: str,
    response: dict[str, Any],
) -> None:
    context = f"{source} timeline response"
    captured_at = require_string(response, "captured_at", context)
    captured_wall_ns = parse_rfc3339_ns(captured_at)
    captured_monotonic_ns = require_integer(
        response, "captured_monotonic_ns", context
    )
    process_role = require_string(response, "process_role", context)
    node_id = optional_string(response.get("node_id"), Path(source.split("#", 1)[0]).stem)
    process_id = require_integer(response, "process_id", context)
    capture_id = optional_string(response.get("capture_id"), source)
    process = f"{process_role}@{node_id}/pid{process_id}"
    raw_events = require_array(response.get("events"), f"{context} events")
    if len(loaded.events) + len(raw_events) > MAX_TIMELINE_EVENTS:
        raise SummaryError(
            f"timeline event count exceeds hard limit of {MAX_TIMELINE_EVENTS}"
        )

    max_events = integer_or(response.get("max_events"), len(raw_events))
    if max_events and len(raw_events) > max_events:
        raise SummaryError(
            f"{source}: capture has {len(raw_events)} events but max_events is {max_events}"
        )

    for index, raw_event in enumerate(raw_events):
        event = require_object(raw_event, f"{source} events[{index}]")
        event_context = f"{source} events[{index}]"
        monotonic_ns = require_integer(event, "monotonic_ns", event_context)
        duration_ns = require_integer(event, "duration_ns", event_context)
        if monotonic_ns > captured_monotonic_ns:
            raise SummaryError(
                f"{event_context} monotonic_ns is after captured_monotonic_ns"
            )
        trace_id = require_string(event, "trace_id", event_context).lower()
        if TRACE_ID_RE.fullmatch(trace_id) is None:
            raise SummaryError(f"{event_context} trace_id must be 32 hexadecimal digits")
        span_id = require_string(event, "span_id", event_context).lower()
        stage = require_integer(event, "stage", event_context)
        stage_name = optional_string(event.get("stage_name"), f"stage.{stage}")
        end_wall_ns = captured_wall_ns - (captured_monotonic_ns - monotonic_ns)
        loaded.events.append(
            TimelineEvent(
                process=process,
                process_role=process_role,
                node_id=node_id,
                process_id=process_id,
                capture_id=capture_id,
                sequence=integer_or(event.get("sequence")),
                start_wall_ns=end_wall_ns - duration_ns,
                end_wall_ns=end_wall_ns,
                duration_ns=duration_ns,
                trace_id=trace_id,
                span_id=span_id,
                stage=stage,
                stage_name=stage_name,
                connection_id=integer_or(event.get("connection_id")),
                call_id=integer_or(event.get("call_id")),
                frame_id=integer_or(event.get("frame_id")),
                byte_count=integer_or(event.get("bytes")),
                flags=integer_or(event.get("flags")),
                aux=integer_or(event.get("aux")),
            )
        )

    declared_event_count = integer_or(response.get("event_count"), len(raw_events))
    loaded.captures.append(
        CaptureInfo(
            source=source,
            capture_id=capture_id,
            process=process,
            event_count=len(raw_events),
            declared_event_count=declared_event_count,
            dropped_events=integer_or(response.get("dropped_events")),
            overflowed=response.get("overflowed") is True,
        )
    )


def load_inputs(paths: list[str]) -> LoadedTimeline:
    budget = InputBudget()
    loaded = LoadedTimeline(events=[], captures=[])
    for path in paths:
        document = budget.load_json(path)
        for source, response in extract_captures(document, path):
            append_capture(loaded, source, response)
        del document
    loaded.input_bytes = budget.total_bytes
    return loaded


def nearest_rank(sorted_values: list[int], percentile: float) -> int:
    rank = max(1, math.ceil(percentile * len(sorted_values)))
    return sorted_values[rank - 1]


def format_us(value_ns: int | None) -> str:
    if value_ns is None:
        return "-"
    return f"{value_ns / 1_000:.3f}"


def format_signed_us(value_ns: int | None) -> str:
    if value_ns is None:
        return "-"
    return f"{value_ns / 1_000:+.3f}"


def print_capture_health(loaded: LoadedTimeline, output: TextIO) -> None:
    print(
        f"Loaded {len(loaded.captures)} captures, {len(loaded.events)} events, "
        f"{loaded.input_bytes / (1024 * 1024):.2f} MiB input",
        file=output,
    )
    print("", file=output)
    print("Capture health", file=output)
    for capture in loaded.captures:
        mismatch = ""
        if capture.event_count != capture.declared_event_count:
            mismatch = f" declared={capture.declared_event_count}"
        print(
            f"  {capture.process} capture={capture.capture_id} "
            f"events={capture.event_count}{mismatch} "
            f"dropped={capture.dropped_events} overflowed={str(capture.overflowed).lower()}",
            file=output,
        )


def print_stage_summary(events: list[TimelineEvent], output: TextIO) -> None:
    groups: dict[tuple[str, int, str], list[int]] = defaultdict(list)
    for event in events:
        groups[(event.process, event.stage, event.stage_name)].append(event.duration_ns)

    print("", file=output)
    print("Duration summary by process/stage (microseconds)", file=output)
    print(
        f"  {'process':<52} {'stage':<28} {'count':>7} {'min':>10} "
        f"{'p50':>10} {'p95':>10} {'max':>10}",
        file=output,
    )
    for (process, stage, stage_name), values in sorted(groups.items()):
        values.sort()
        label = f"{stage_name}({stage})"
        print(
            f"  {process:<52} {label:<28} {len(values):>7} "
            f"{format_us(values[0]):>10} "
            f"{format_us(nearest_rank(values, 0.50)):>10} "
            f"{format_us(nearest_rank(values, 0.95)):>10} "
            f"{format_us(values[-1]):>10}",
            file=output,
        )


def receiver_readiness_key(
    event: TimelineEvent,
    *,
    include_frame_id: bool,
) -> tuple[str, str, str, int, int, int, int] | tuple[str, str, str, int, int, int]:
    base = (
        event.capture_id,
        event.process,
        event.trace_id,
        event.connection_id,
        event.call_id,
        event.flags & FLAG_DIRECTION_MASK,
    )
    if include_frame_id:
        return (*base, event.frame_id)
    return base


def pair_receiver_readiness_events(
    ready_events: list[TimelineEvent],
    first_read_events: list[TimelineEvent],
    *,
    max_gap_ns: int | None,
) -> tuple[list[int], int, int]:
    """Pairs receiver-local events without an event-count-squared scan."""
    ready = sorted(ready_events, key=lambda event: event.end_wall_ns)
    first_reads = sorted(first_read_events, key=lambda event: event.start_wall_ns)
    gaps: list[int] = []
    ready_index = 0
    first_read_index = 0
    unmatched_ready = 0
    unmatched_first_read = 0
    while ready_index < len(ready) and first_read_index < len(first_reads):
        ready_event = ready[ready_index]
        first_read_event = first_reads[first_read_index]
        gap_ns = first_read_event.start_wall_ns - ready_event.end_wall_ns
        if gap_ns < 0:
            unmatched_first_read += 1
            first_read_index += 1
            continue
        if max_gap_ns is not None and gap_ns > max_gap_ns:
            unmatched_ready += 1
            ready_index += 1
            continue
        gaps.append(gap_ns)
        ready_index += 1
        first_read_index += 1
    unmatched_ready += len(ready) - ready_index
    unmatched_first_read += len(first_reads) - first_read_index
    return gaps, unmatched_ready, unmatched_first_read


def collect_receiver_readiness(
    events: list[TimelineEvent],
) -> dict[str, ReceiverReadinessProcess]:
    processes: dict[str, ReceiverReadinessProcess] = {}
    exact_ready: dict[tuple[str, str, str, int, int, int, int], list[TimelineEvent]] = (
        defaultdict(list)
    )
    exact_first_read: dict[
        tuple[str, str, str, int, int, int, int], list[TimelineEvent]
    ] = defaultdict(list)
    legacy_ready: dict[tuple[str, str, str, int, int, int], list[TimelineEvent]] = (
        defaultdict(list)
    )
    legacy_first_read: dict[
        tuple[str, str, str, int, int, int], list[TimelineEvent]
    ] = defaultdict(list)

    for event in events:
        if event.stage not in (STAGE_FRAME_READY_TO_POLL, STAGE_FRAME_FIRST_READ):
            continue
        process = processes.setdefault(event.process, ReceiverReadinessProcess())
        if event.stage == STAGE_FRAME_READY_TO_POLL:
            process.ready_count += 1
            process.wake_to_poll_samples_ns.append(event.duration_ns)
            target = exact_ready if event.frame_id else legacy_ready
        else:
            process.first_read_count += 1
            target = exact_first_read if event.frame_id else legacy_first_read
        target[
            receiver_readiness_key(event, include_frame_id=event.frame_id != 0)
        ].append(event)

    for ready_by_key, first_read_by_key, max_gap_ns, exact in (
        (exact_ready, exact_first_read, None, True),
        (
            legacy_ready,
            legacy_first_read,
            LEGACY_READINESS_PAIR_MAX_NS,
            False,
        ),
    ):
        for key in ready_by_key.keys() | first_read_by_key.keys():
            process_name = key[1]
            gaps, unmatched_ready, unmatched_first_read = (
                pair_receiver_readiness_events(
                    ready_by_key.get(key, []),
                    first_read_by_key.get(key, []),
                    max_gap_ns=max_gap_ns,
                )
            )
            process = processes[process_name]
            process.poll_to_first_read_samples_ns.extend(gaps)
            if exact:
                process.exact_pair_count += len(gaps)
            else:
                process.legacy_pair_count += len(gaps)
            process.unmatched_ready_count += unmatched_ready
            process.unmatched_first_read_count += unmatched_first_read
    return processes


def print_receiver_readiness_metric(
    process: str,
    segment: str,
    values: list[int],
    missing: int,
    output: TextIO,
) -> None:
    ordered = sorted(values)
    if ordered:
        p50 = format_us(nearest_rank(ordered, 0.50))
        p95 = format_us(nearest_rank(ordered, 0.95))
        maximum = format_us(ordered[-1])
    else:
        p50 = p95 = maximum = "-"
    print(
        f"  {process:<52} {segment:<36} {len(ordered):>7} {missing:>8} "
        f"{p50:>12} {p95:>12} {maximum:>12}",
        file=output,
    )


def print_receiver_readiness_summary(
    events: list[TimelineEvent],
    output: TextIO,
) -> None:
    processes = collect_receiver_readiness(events)
    print("", file=output)
    print("Receiver-local readiness split by process (microseconds)", file=output)
    print(
        "  Nonzero frame ids pair within capture/process/connection/call/direction; "
        "legacy zero-id events use an ordered 100 ms timestamp bound.",
        file=output,
    )
    print(
        "  wake -> task poll missing counts first-read markers without readiness; "
        "poll -> successful read missing counts unmatched events on either side.",
        file=output,
    )
    if not processes:
        print("  no frame.ready-to-poll or frame.first-read events", file=output)
        return
    print(
        f"  {'process':<52} {'segment':<36} {'count':>7} {'missing':>8} "
        f"{'p50':>12} {'p95':>12} {'max':>12}",
        file=output,
    )
    for process_name, process in sorted(processes.items()):
        print_receiver_readiness_metric(
            process_name,
            "wake -> task poll",
            process.wake_to_poll_samples_ns,
            process.unmatched_first_read_count,
            output,
        )
        print_receiver_readiness_metric(
            process_name,
            "task poll -> successful first read",
            process.poll_to_first_read_samples_ns,
            process.unmatched_ready_count + process.unmatched_first_read_count,
            output,
        )
        print(
            f"    pairs exact={process.exact_pair_count} "
            f"legacy-zero-id={process.legacy_pair_count} "
            f"unmatched-ready={process.unmatched_ready_count} "
            f"unmatched-first-read={process.unmatched_first_read_count}",
            file=output,
        )


def channel_lookup_outcome(event: TimelineEvent) -> str:
    # New captures carry an explicit outcome and the logical connection id.
    # Older captures used aux as a call-site counter, so preserve an honest
    # legacy label when the schema cannot be distinguished.
    if event.aux in CHANNEL_LOOKUP_OUTCOMES and (
        event.connection_id > 0 or event.aux >= 2
    ):
        return CHANNEL_LOOKUP_OUTCOMES[event.aux]
    return "available-legacy" if event.flags & FLAG_SUCCESS else "unavailable-legacy"


def print_channel_lookup_summary(events: list[TimelineEvent], output: TextIO) -> None:
    grouped: dict[tuple[str, str], list[TimelineEvent]] = defaultdict(list)
    for event in events:
        if event.stage == STAGE_CHANNEL_LOOKUP:
            grouped[(event.process, channel_lookup_outcome(event))].append(event)
    print("", file=output)
    print("Channel lookup outcomes", file=output)
    if not grouped:
        print("  no channel lookup events", file=output)
        return
    print(
        f"  {'process':<52} {'outcome':<20} {'count':>7} {'connections':>12}",
        file=output,
    )
    for (process, outcome), samples in sorted(grouped.items()):
        connections = {event.connection_id for event in samples if event.connection_id > 0}
        connection_text = str(len(connections)) if connections else "-"
        print(
            f"  {process:<52} {outcome:<20} {len(samples):>7} "
            f"{connection_text:>12}",
            file=output,
        )


def is_client(event: TimelineEvent) -> bool:
    return "client" in event.process_role.lower()


def analyze_traces(
    traces: dict[str, list[TimelineEvent]],
) -> list[TraceAnalysis]:
    analyses: list[TraceAnalysis] = []
    for trace_id, events in traces.items():
        processes = {event.process for event in events}
        if len(processes) < 2:
            continue
        client_completes = [
            event
            for event in events
            if is_client(event) and event.stage_name == "call.complete"
        ]
        if not client_completes:
            continue
        client = max(client_completes, key=lambda event: event.duration_ns)
        daemon_events = [event for event in events if event.process != client.process]
        handlers = [
            event for event in daemon_events if event.stage_name == "handler.dispatch"
        ]
        inner_calls = [
            event for event in daemon_events if event.stage_name == "call.complete"
        ]
        handler = max(handlers, key=lambda event: event.duration_ns, default=None)
        inner_call = max(inner_calls, key=lambda event: event.duration_ns, default=None)
        first_daemon_start = min(
            (event.start_wall_ns for event in daemon_events), default=None
        )
        last_daemon_end = max(
            (event.end_wall_ns for event in daemon_events), default=None
        )
        outbound_gap = (
            first_daemon_start - client.start_wall_ns
            if first_daemon_start is not None
            else None
        )
        return_gap = (
            client.end_wall_ns - last_daemon_end
            if last_daemon_end is not None
            else None
        )
        client_minus_handler = (
            client.duration_ns - handler.duration_ns if handler is not None else None
        )
        client_minus_inner = (
            client.duration_ns - inner_call.duration_ns
            if inner_call is not None
            else None
        )
        score = max(
            [
                max(0, value)
                for value in (
                    outbound_gap,
                    return_gap,
                    client_minus_handler,
                    client_minus_inner,
                )
                if value is not None
            ],
            default=0,
        )
        analyses.append(
            TraceAnalysis(
                trace_id=trace_id,
                client=client,
                handler=handler,
                inner_call=inner_call,
                outbound_gap_ns=outbound_gap,
                return_gap_ns=return_gap,
                client_minus_handler_ns=client_minus_handler,
                client_minus_inner_ns=client_minus_inner,
                score_ns=score,
            )
        )
    analyses.sort(key=lambda analysis: analysis.score_ns, reverse=True)
    return analyses


def directional_stage_events(
    events: list[TimelineEvent],
    *,
    stage: int,
    call_id: int,
    direction: int,
    process: str | None = None,
    connection_id: int | None = None,
) -> list[TimelineEvent]:
    candidates = [
        event
        for event in events
        if event.stage == stage
        and event.call_id == call_id
        and (process is None or event.process == process)
        and (connection_id is None or event.connection_id == connection_id)
    ]
    directed = [
        event
        for event in candidates
        if event.flags & FLAG_DIRECTION_MASK == direction
    ]
    # Old captures may not have carried direction flags. Call id, stage, and
    # process still provide a useful bounded fallback.
    return directed or candidates


def preferred_directional_stage_events(
    events: list[TimelineEvent],
    *,
    stage: int,
    fallback_stage: int | tuple[int, ...],
    call_id: int,
    direction: int,
    process: str | None = None,
    connection_id: int | None = None,
) -> list[TimelineEvent]:
    fallback_stages = (
        fallback_stage if isinstance(fallback_stage, tuple) else (fallback_stage,)
    )
    for candidate_stage in (stage, *fallback_stages):
        candidates = directional_stage_events(
            events,
            stage=candidate_stage,
            call_id=call_id,
            direction=direction,
            process=process,
            connection_id=connection_id,
        )
        if candidates:
            return candidates
    return []


def closest_carrier_pair(
    writes: list[TimelineEvent],
    receives: list[TimelineEvent],
) -> tuple[TimelineEvent, TimelineEvent] | None:
    if not writes or not receives:
        return None
    exact = [
        (write, receive)
        for write in writes
        for receive in receives
        if write.frame_id != 0 and write.frame_id == receive.frame_id
    ]
    if exact:
        return min(
            exact,
            key=lambda pair: abs(pair[1].start_wall_ns - pair[0].end_wall_ns),
        )
    return min(
        ((write, receive) for write in writes for receive in receives),
        key=lambda pair: abs(pair[1].start_wall_ns - pair[0].end_wall_ns),
    )


def infer_carrier_hop(
    events: list[TimelineEvent],
    *,
    source_process: str,
    call_id: int,
    source_connection_id: int,
    excluded_destinations: set[str],
) -> CarrierHop | None:
    request_writes = preferred_directional_stage_events(
        events,
        stage=STAGE_FRAME_SOCKET_WRITE,
        fallback_stage=STAGE_FRAME_WRITE,
        call_id=call_id,
        direction=FLAG_REQUEST,
        process=source_process,
        connection_id=source_connection_id,
    )
    request_receives = [
        event
        for event in preferred_directional_stage_events(
            events,
            stage=STAGE_FRAME_READY_TO_POLL,
            fallback_stage=(STAGE_FRAME_FIRST_READ, STAGE_FRAME_INGRESS),
            call_id=call_id,
            direction=FLAG_REQUEST,
        )
        if event.process != source_process
        and event.process not in excluded_destinations
    ]
    request_pair = closest_carrier_pair(request_writes, request_receives)
    if request_pair is None:
        return None
    request_write, request_receive = request_pair
    destination_process = request_receive.process

    response_writes = preferred_directional_stage_events(
        events,
        stage=STAGE_FRAME_SOCKET_WRITE,
        fallback_stage=STAGE_FRAME_WRITE,
        call_id=call_id,
        direction=FLAG_RESPONSE,
        process=destination_process,
        connection_id=request_receive.connection_id,
    )
    response_receives = preferred_directional_stage_events(
        events,
        stage=STAGE_FRAME_READY_TO_POLL,
        fallback_stage=(STAGE_FRAME_FIRST_READ, STAGE_PENDING_RESOLVE),
        call_id=call_id,
        direction=FLAG_RESPONSE,
        process=source_process,
        connection_id=request_write.connection_id,
    )
    response_pair = closest_carrier_pair(response_writes, response_receives)
    response_gap_ns = None
    if response_pair is not None:
        response_write, response_receive = response_pair
        response_gap_ns = response_receive.start_wall_ns - response_write.end_wall_ns

    return CarrierHop(
        source_process=source_process,
        destination_process=destination_process,
        call_id=call_id,
        source_connection_id=request_write.connection_id,
        destination_connection_id=request_receive.connection_id,
        request_gap_ns=request_receive.start_wall_ns - request_write.end_wall_ns,
        response_gap_ns=response_gap_ns,
    )


def nested_carrier_hop(
    events: list[TimelineEvent],
    inbound_hop: CarrierHop,
    visited_processes: set[str],
) -> CarrierHop | None:
    source_process = inbound_hop.destination_process
    candidates = [
        event
        for event in events
        if event.process == source_process
        and event.stage == STAGE_CALL_COMPLETE
        and event.call_id > 0
    ]
    directed = [
        event
        for event in candidates
        if event.flags & FLAG_DIRECTION_MASK == FLAG_RESPONSE
    ]
    candidates = directed or candidates

    handlers = [
        event
        for event in events
        if event.process == source_process
        and event.stage == STAGE_HANDLER_DISPATCH
        and event.call_id == inbound_hop.call_id
        and event.connection_id == inbound_hop.destination_connection_id
    ]
    viable: list[tuple[int, int, CarrierHop]] = []
    for candidate in candidates:
        hop = infer_carrier_hop(
            events,
            source_process=source_process,
            call_id=candidate.call_id,
            source_connection_id=candidate.connection_id,
            excluded_destinations=visited_processes,
        )
        if hop is None:
            continue
        outside_handler_ns = 0
        if handlers:
            outside_handler_ns = min(
                max(0, handler.start_wall_ns - candidate.start_wall_ns)
                + max(0, candidate.end_wall_ns - handler.end_wall_ns)
                for handler in handlers
            )
        viable.append((outside_handler_ns, -candidate.duration_ns, hop))
    if not viable:
        return None
    viable.sort(key=lambda entry: (entry[0], entry[1], entry[2].call_id))
    return viable[0][2]


def infer_carrier_path(events: list[TimelineEvent]) -> list[CarrierHop]:
    client_completes = [
        event
        for event in events
        if is_client(event)
        and event.stage == STAGE_CALL_COMPLETE
        and event.call_id > 0
    ]
    if not client_completes:
        return []
    client_complete = max(client_completes, key=lambda event: event.duration_ns)
    first_hop = infer_carrier_hop(
        events,
        source_process=client_complete.process,
        call_id=client_complete.call_id,
        source_connection_id=client_complete.connection_id,
        excluded_destinations={client_complete.process},
    )
    if first_hop is None:
        return []

    hops = [first_hop]
    visited_processes = {
        first_hop.source_process,
        first_hop.destination_process,
    }
    while len(hops) < 8:
        next_hop = nested_carrier_hop(events, hops[-1], visited_processes)
        if next_hop is None:
            break
        hops.append(next_hop)
        visited_processes.add(next_hop.destination_process)
    return hops


def direct_gaps_for_path(hops: list[CarrierHop]) -> dict[str, int]:
    if len(hops) != 2:
        return {}
    gaps = {
        "client_to_caller": hops[0].request_gap_ns,
        "caller_to_target": hops[1].request_gap_ns,
    }
    if hops[1].response_gap_ns is not None:
        gaps["target_to_caller"] = hops[1].response_gap_ns
    if hops[0].response_gap_ns is not None:
        gaps["caller_to_client"] = hops[0].response_gap_ns
    return gaps


def forwarded_gaps_for_path(hops: list[CarrierHop]) -> dict[str, int]:
    if len(hops) != 3:
        return {}
    gaps = {
        "client_to_caller": hops[0].request_gap_ns,
        "caller_to_relay": hops[1].request_gap_ns,
        "relay_to_target": hops[2].request_gap_ns,
    }
    if hops[2].response_gap_ns is not None:
        gaps["target_to_relay"] = hops[2].response_gap_ns
    if hops[1].response_gap_ns is not None:
        gaps["relay_to_caller"] = hops[1].response_gap_ns
    if hops[0].response_gap_ns is not None:
        gaps["caller_to_client"] = hops[0].response_gap_ns
    return gaps


def carrier_round_trip_totals_for_path(
    hops: list[CarrierHop],
    hop_keys: tuple[str, ...],
) -> dict[str, int]:
    if len(hops) != len(hop_keys):
        return {}
    totals: dict[str, int] = {}
    for key, hop in zip(hop_keys, hops, strict=True):
        if hop.response_gap_ns is None:
            continue
        totals[key] = hop.request_gap_ns + hop.response_gap_ns
    # The chain total is emitted only when every request/response pair came
    # from this same trace. This keeps percentile rows coherent instead of
    # adding independently selected marginal percentiles.
    if len(totals) == len(hops):
        totals["carrier_chain_total"] = sum(totals.values())
    return totals


def direct_round_trip_totals_for_path(hops: list[CarrierHop]) -> dict[str, int]:
    return carrier_round_trip_totals_for_path(
        hops,
        (
            "client_caller_round_trip",
            "caller_target_round_trip",
        ),
    )


def forwarded_round_trip_totals_for_path(hops: list[CarrierHop]) -> dict[str, int]:
    return carrier_round_trip_totals_for_path(
        hops,
        (
            "client_caller_round_trip",
            "caller_relay_round_trip",
            "relay_target_round_trip",
        ),
    )


def collect_carrier_gaps(
    traces: dict[str, list[TimelineEvent]],
) -> CarrierGapCollection:
    direct_samples = {key: [] for key, _label in DIRECT_CARRIER_GAP_LABELS}
    direct_total_samples = {
        key: [] for key, _label in DIRECT_CARRIER_TOTAL_LABELS
    }
    forwarded_samples = {
        key: [] for key, _label in FORWARDED_CARRIER_GAP_LABELS
    }
    forwarded_total_samples = {
        key: [] for key, _label in FORWARDED_CARRIER_TOTAL_LABELS
    }
    direct_count = 0
    forwarded_count = 0
    unclassified_count = 0
    for events in traces.values():
        if not any(
            is_client(event)
            and event.stage == STAGE_CALL_COMPLETE
            and event.call_id > 0
            for event in events
        ):
            continue
        # A canonical endpoint-open fallback omits the target-side pooled
        # carrier spans. Treating its remaining two hops as a healthy direct
        # path hides precisely the pool failure this report is meant to find.
        if any(event.stage == STAGE_ENDPOINT_OPEN for event in events):
            unclassified_count += 1
            continue
        path = infer_carrier_path(events)
        if len(path) == 2:
            direct_count += 1
            for key, gap_ns in direct_gaps_for_path(path).items():
                direct_samples[key].append(gap_ns)
            for key, total_ns in direct_round_trip_totals_for_path(path).items():
                direct_total_samples[key].append(total_ns)
        elif len(path) == 3:
            forwarded_count += 1
            for key, gap_ns in forwarded_gaps_for_path(path).items():
                forwarded_samples[key].append(gap_ns)
            for key, total_ns in forwarded_round_trip_totals_for_path(path).items():
                forwarded_total_samples[key].append(total_ns)
        else:
            unclassified_count += 1
    return CarrierGapCollection(
        direct_samples=direct_samples,
        direct_total_samples=direct_total_samples,
        direct_count=direct_count,
        forwarded_samples=forwarded_samples,
        forwarded_total_samples=forwarded_total_samples,
        forwarded_count=forwarded_count,
        unclassified_count=unclassified_count,
    )


def print_carrier_table(
    labels: tuple[tuple[str, str], ...],
    samples: dict[str, list[int]],
    candidate_count: int,
    output: TextIO,
) -> None:
    print(
        f"  {'boundary':<62} {'count':>7} {'missing':>8} "
        f"{'p50':>12} {'p95':>12} {'max':>12}",
        file=output,
    )
    for key, label in labels:
        values = sorted(samples[key])
        missing = max(0, candidate_count - len(values))
        if values:
            p50 = format_signed_us(nearest_rank(values, 0.50))
            p95 = format_signed_us(nearest_rank(values, 0.95))
            maximum = format_signed_us(values[-1])
        else:
            p50 = p95 = maximum = "-"
        print(
            f"  {label:<62} {len(values):>7} {missing:>8} "
            f"{p50:>12} {p95:>12} {maximum:>12}",
            file=output,
        )


def print_carrier_gap_summary(
    traces: dict[str, list[TimelineEvent]],
    output: TextIO,
) -> None:
    collected = collect_carrier_gaps(traces)
    print("", file=output)
    print("Socket delivery/reactor readiness gaps by aligned trace (microseconds)", file=output)
    print(
        "  New captures pair frame.socket-write completion with the start of "
        "frame.ready-to-poll; immediate-ready frames fall back to frame.first-read, "
        "and legacy captures fall back to frame.write and ingress/resolve.",
        file=output,
    )
    print(
        "  Individual one-way gaps may be slightly negative from cross-process "
        "clock-anchor skew; same-trace round trips cancel a fixed offset.",
        file=output,
    )
    print(
        "  frame.ready-to-poll duration is the receiver-local Tokio runnable delay "
        "after each reported gap; poll-to-first-read is receiver read dispatch.",
        file=output,
    )
    print(
        f"Direct path: client -> caller -> target ({collected.direct_count} traces)",
        file=output,
    )
    print_carrier_table(
        DIRECT_CARRIER_GAP_LABELS,
        collected.direct_samples,
        collected.direct_count,
        output,
    )
    print("  Coherent same-trace round-trip and carrier-chain totals", file=output)
    print_carrier_table(
        DIRECT_CARRIER_TOTAL_LABELS,
        collected.direct_total_samples,
        collected.direct_count,
        output,
    )
    print(
        f"One-hop forward: client -> caller -> relay -> target "
        f"({collected.forwarded_count} traces)",
        file=output,
    )
    print_carrier_table(
        FORWARDED_CARRIER_GAP_LABELS,
        collected.forwarded_samples,
        collected.forwarded_count,
        output,
    )
    print("  Coherent same-trace round-trip and carrier-chain totals", file=output)
    print_carrier_table(
        FORWARDED_CARRIER_TOTAL_LABELS,
        collected.forwarded_total_samples,
        collected.forwarded_count,
        output,
    )
    if collected.unclassified_count:
        print(
            f"  unclassified client traces (incomplete or deeper path): "
            f"{collected.unclassified_count}",
            file=output,
        )


def print_trace_gaps(
    analyses: list[TraceAnalysis],
    threshold_ns: int,
    top: int,
    output: TextIO,
) -> list[TraceAnalysis]:
    major = [analysis for analysis in analyses if analysis.score_ns >= threshold_ns]
    selected = major[:top]
    print("", file=output)
    print(
        "Cross-process trace residuals "
        f"(major threshold {threshold_ns / 1_000:.3f} us)",
        file=output,
    )
    if not analyses:
        print("  no trace has both a client call.complete and daemon events", file=output)
        return []
    if not selected:
        selected = analyses[: min(top, 3)]
        print("  no residual crossed the threshold; showing the largest", file=output)
    for analysis in selected:
        handler_process = analysis.handler.process if analysis.handler else "-"
        inner_process = analysis.inner_call.process if analysis.inner_call else "-"
        print(
            f"  trace={analysis.trace_id} client_total="
            f"{format_us(analysis.client.duration_ns)} us score="
            f"{format_us(analysis.score_ns)} us",
            file=output,
        )
        print(
            f"    handler={format_us(analysis.handler.duration_ns if analysis.handler else None)} "
            f"us client-handler={format_signed_us(analysis.client_minus_handler_ns)} us "
            f"process={handler_process}",
            file=output,
        )
        print(
            f"    inner-call={format_us(analysis.inner_call.duration_ns if analysis.inner_call else None)} "
            f"us client-inner={format_signed_us(analysis.client_minus_inner_ns)} us "
            f"process={inner_process}",
            file=output,
        )
        print(
            f"    aligned outbound-gap={format_signed_us(analysis.outbound_gap_ns)} us "
            f"return-gap={format_signed_us(analysis.return_gap_ns)} us",
            file=output,
        )
    return selected


def resolve_trace(
    requested: str,
    traces: dict[str, list[TimelineEvent]],
) -> str:
    normalized = requested.lower()
    if normalized.startswith("00-"):
        parts = normalized.split("-")
        if len(parts) >= 4:
            normalized = parts[1]
    if normalized in traces:
        return normalized
    matches = [trace_id for trace_id in traces if trace_id.startswith(normalized)]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise SummaryError(f"trace {requested!r} was not found")
    raise SummaryError(f"trace prefix {requested!r} matches {len(matches)} traces")


def print_critical_timeline(
    trace_id: str,
    events: list[TimelineEvent],
    limit: int,
    output: TextIO,
) -> None:
    ordered = sorted(
        events,
        key=lambda event: (
            event.start_wall_ns,
            event.end_wall_ns,
            event.process,
            event.sequence,
        ),
    )
    base_ns = min(event.start_wall_ns for event in ordered)
    print("", file=output)
    print(f"Critical timeline for trace {trace_id} (relative microseconds)", file=output)
    print(
        f"  {'start':>12} {'end':>12} {'duration':>12} "
        f"{'process':<52} {'stage':<28} {'conn':>6} {'call':>8} "
        f"{'frame':>18} {'flags':>6} {'aux':>5} {'bytes':>8} {'detail'}",
        file=output,
    )
    for event in ordered[:limit]:
        detail = ""
        if event.stage == STAGE_CHANNEL_LOOKUP:
            detail = f"lookup={channel_lookup_outcome(event)}"
        elif event.stage == STAGE_CHANNEL_CLOSE:
            detail = f"active_calls={event.aux} queued_bytes={event.byte_count}"
        elif event.stage in (
            STAGE_FRAME_FIRST_READ,
            STAGE_FRAME_READ,
            STAGE_FRAME_DECODE,
            STAGE_FRAME_READY_TO_POLL,
        ):
            detail = f"first_read_bytes={event.aux}"
        print(
            f"  {(event.start_wall_ns - base_ns) / 1_000:>12.3f} "
            f"{(event.end_wall_ns - base_ns) / 1_000:>12.3f} "
            f"{event.duration_ns / 1_000:>12.3f} "
            f"{event.process:<52} {event.stage_name:<28} "
            f"{event.connection_id:>6} {event.call_id:>8} "
            f"{event.frame_id:>18} 0x{event.flags:04x} "
            f"{event.aux:>5} {event.byte_count:>8} {detail}",
            file=output,
        )
    if len(ordered) > limit:
        print(f"  ... {len(ordered) - limit} events omitted by --timeline-limit", file=output)


def summarize(
    loaded: LoadedTimeline,
    output: TextIO,
    *,
    gap_threshold_us: float,
    top_traces: int,
    requested_trace: str | None,
    timeline_limit: int,
    show_critical_timeline: bool,
) -> None:
    print_capture_health(loaded, output)
    print_stage_summary(loaded.events, output)
    print_receiver_readiness_summary(loaded.events, output)
    print_channel_lookup_summary(loaded.events, output)
    traces: dict[str, list[TimelineEvent]] = defaultdict(list)
    for event in loaded.events:
        if event.trace_id.strip("0"):
            traces[event.trace_id].append(event)
    print_carrier_gap_summary(traces, output)
    analyses = analyze_traces(traces)
    selected = print_trace_gaps(
        analyses,
        int(gap_threshold_us * 1_000),
        top_traces,
        output,
    )
    critical_trace: str | None = None
    if requested_trace is not None:
        critical_trace = resolve_trace(requested_trace, traces)
    elif show_critical_timeline and selected:
        critical_trace = selected[0].trace_id
    if critical_trace is not None:
        print_critical_timeline(
            critical_trace,
            traces[critical_trace],
            timeline_limit,
            output,
        )


def self_test() -> None:
    trace_id = "0123456789abcdef0123456789abcdef"
    client_capture = {
        "active": False,
        "capture_id": "client",
        "process_role": "test-client",
        "node_id": "node-a",
        "process_id": 10,
        "captured_at": "2026-07-18T12:00:01.000000000Z",
        "captured_monotonic_ns": 1_000_000_000,
        "max_events": 8,
        "event_count": 1,
        "dropped_events": 0,
        "overflowed": False,
        "events": [
            {
                "sequence": 1,
                "monotonic_ns": 900_000_000,
                "duration_ns": 600_000,
                "trace_id": trace_id,
                "span_id": "0123456789abcdef",
                "stage": 12,
                "stage_name": "call.complete",
                "connection_id": 1,
                "call_id": 2,
                "frame_id": 0,
                "flags": 1,
                "bytes": 0,
                "aux": 0,
            }
        ],
    }
    daemon_capture = {
        "active": False,
        "capture_id": "daemon",
        "process_role": "daemon",
        "node_id": "node-b",
        "process_id": 20,
        "captured_at": "2026-07-18T12:00:02.000000000Z",
        "captured_monotonic_ns": 2_000_000_000,
        "max_events": 8,
        "event_count": 2,
        "dropped_events": 0,
        "overflowed": False,
        "events": [
            {
                "sequence": 1,
                "monotonic_ns": 1_899_900_000,
                "duration_ns": 400_000,
                "trace_id": trace_id,
                "span_id": "0123456789abcdef",
                "stage": 9,
                "stage_name": "handler.dispatch",
                "connection_id": 3,
                "call_id": 4,
                "frame_id": 0,
                "flags": 1,
                "bytes": 0,
                "aux": 0,
            },
            {
                "sequence": 2,
                "monotonic_ns": 1_899_850_000,
                "duration_ns": 300_000,
                "trace_id": trace_id,
                "span_id": "0123456789abcdef",
                "stage": 12,
                "stage_name": "call.complete",
                "connection_id": 3,
                "call_id": 5,
                "frame_id": 0,
                "flags": 1,
                "bytes": 0,
                "aux": 0,
            },
        ],
    }
    report = {"diagnostics": {"timeline_client_capture": client_capture}}
    with tempfile.TemporaryDirectory(prefix="sinomo-network-summary-test-") as temp:
        report_path = Path(temp) / "report.json"
        daemon_path = Path(temp) / "daemon.json"
        report_path.write_text(json.dumps(report), encoding="utf-8")
        daemon_path.write_text(json.dumps(daemon_capture), encoding="utf-8")
        loaded = load_inputs([str(report_path), str(daemon_path)])
    assert len(loaded.captures) == 2
    assert len(loaded.events) == 3
    assert loaded.events[0].end_wall_ns == parse_rfc3339_ns(
        "2026-07-18T12:00:00.900000000Z"
    )
    output = io.StringIO()
    summarize(
        loaded,
        output,
        gap_threshold_us=1.0,
        top_traces=4,
        requested_trace=None,
        timeline_limit=16,
        show_critical_timeline=True,
    )
    rendered = output.getvalue()
    assert "Duration summary by process/stage" in rendered
    assert "client-handler=+200.000 us" in rendered
    assert "client-inner=+300.000 us" in rendered
    assert f"Critical timeline for trace {trace_id}" in rendered

    def carrier_event(
        process: str,
        process_role: str,
        node_id: str,
        stage: int,
        stage_name: str,
        call_id: int,
        start_ns: int,
        end_ns: int,
        flags: int,
    ) -> TimelineEvent:
        return TimelineEvent(
            process=process,
            process_role=process_role,
            node_id=node_id,
            process_id=1,
            capture_id="carrier-fixture",
            sequence=1,
            start_wall_ns=start_ns,
            end_wall_ns=end_ns,
            duration_ns=end_ns - start_ns,
            trace_id=trace_id,
            span_id="0123456789abcdef",
            stage=stage,
            stage_name=stage_name,
            connection_id=1,
            call_id=call_id,
            frame_id=0,
            byte_count=1,
            flags=flags,
            aux=0,
        )

    client_process = "test-client@node-a/pid10"
    caller_process = "daemon@node-a/pid20"
    target_process = "daemon@node-b/pid30"
    carrier_events = [
        carrier_event(
            client_process,
            "test-client",
            "node-a",
            STAGE_CALL_COMPLETE,
            "call.complete",
            11,
            0,
            2_500_000,
            FLAG_RESPONSE,
        ),
        carrier_event(
            client_process,
            "test-client",
            "node-a",
            STAGE_FRAME_WRITE,
            "frame.write",
            11,
            150_000,
            200_000,
            FLAG_REQUEST,
        ),
        carrier_event(
            caller_process,
            "daemon",
            "node-a",
            STAGE_FRAME_INGRESS,
            "frame.ingress",
            11,
            300_000,
            320_000,
            FLAG_REQUEST,
        ),
        carrier_event(
            caller_process,
            "daemon",
            "node-a",
            STAGE_CALL_COMPLETE,
            "call.complete",
            22,
            400_000,
            1_400_000,
            FLAG_RESPONSE,
        ),
        carrier_event(
            caller_process,
            "daemon",
            "node-a",
            STAGE_FRAME_WRITE,
            "frame.write",
            22,
            550_000,
            600_000,
            FLAG_REQUEST,
        ),
        carrier_event(
            target_process,
            "daemon",
            "node-b",
            STAGE_FRAME_INGRESS,
            "frame.ingress",
            22,
            700_000,
            720_000,
            FLAG_REQUEST,
        ),
        carrier_event(
            target_process,
            "daemon",
            "node-b",
            STAGE_FRAME_WRITE,
            "frame.write",
            22,
            950_000,
            1_000_000,
            FLAG_RESPONSE,
        ),
        carrier_event(
            caller_process,
            "daemon",
            "node-a",
            STAGE_PENDING_RESOLVE,
            "pending.resolve",
            22,
            1_100_000,
            1_120_000,
            FLAG_RESPONSE,
        ),
        carrier_event(
            caller_process,
            "daemon",
            "node-a",
            STAGE_FRAME_WRITE,
            "frame.write",
            11,
            1_450_000,
            1_500_000,
            FLAG_RESPONSE,
        ),
        carrier_event(
            client_process,
            "test-client",
            "node-a",
            STAGE_PENDING_RESOLVE,
            "pending.resolve",
            11,
            1_700_000,
            1_720_000,
            FLAG_RESPONSE,
        ),
    ]
    legacy_direct = collect_carrier_gaps({trace_id: carrier_events})
    assert legacy_direct.direct_count == 1
    assert legacy_direct.forwarded_count == 0
    assert legacy_direct.unclassified_count == 0
    assert legacy_direct.direct_samples == {
        "client_to_caller": [100_000],
        "caller_to_target": [100_000],
        "target_to_caller": [100_000],
        "caller_to_client": [200_000],
    }
    assert legacy_direct.direct_total_samples == {
        "client_caller_round_trip": [300_000],
        "caller_target_round_trip": [200_000],
        "carrier_chain_total": [500_000],
    }
    assert all(not values for values in legacy_direct.forwarded_samples.values())
    assert all(not values for values in legacy_direct.forwarded_total_samples.values())

    def with_split_carrier_stages(events: list[TimelineEvent]) -> list[TimelineEvent]:
        split: list[TimelineEvent] = []
        for event in events:
            split.append(event)
            if event.stage == STAGE_FRAME_WRITE:
                split.append(
                    replace(
                        event,
                        stage=STAGE_FRAME_SOCKET_WRITE,
                        stage_name="frame.socket-write",
                    )
                )
            if event.stage in (STAGE_FRAME_INGRESS, STAGE_PENDING_RESOLVE):
                split.append(
                    replace(
                        event,
                        stage=STAGE_FRAME_FIRST_READ,
                        stage_name="frame.first-read",
                        end_wall_ns=event.start_wall_ns,
                        duration_ns=0,
                        aux=12,
                    )
                )
                split.append(
                    replace(
                        event,
                        stage=STAGE_FRAME_READY_TO_POLL,
                        stage_name="frame.ready-to-poll",
                        start_wall_ns=event.start_wall_ns - 10_000,
                        end_wall_ns=event.start_wall_ns,
                        duration_ns=10_000,
                        aux=12,
                    )
                )
            if event.stage == STAGE_FRAME_INGRESS:
                split.append(
                    replace(
                        event,
                        stage=STAGE_FRAME_READ,
                        stage_name="frame.read",
                        aux=12,
                    )
                )
                split.append(
                    replace(
                        event,
                        stage=STAGE_FRAME_DECODE,
                        stage_name="frame.decode",
                        start_wall_ns=event.end_wall_ns,
                        duration_ns=0,
                        aux=12,
                    )
                )
        return split

    split_events = with_split_carrier_stages(carrier_events)
    readiness_split_events = [
        replace(
            event,
            start_wall_ns=event.start_wall_ns - 4_000,
            end_wall_ns=event.end_wall_ns - 4_000,
        )
        if event.stage == STAGE_FRAME_READY_TO_POLL
        else event
        for event in split_events
    ]
    legacy_readiness = collect_receiver_readiness(readiness_split_events)
    caller_readiness = legacy_readiness[caller_process]
    assert caller_readiness.wake_to_poll_samples_ns == [10_000, 10_000]
    assert caller_readiness.poll_to_first_read_samples_ns == [4_000, 4_000]
    assert caller_readiness.exact_pair_count == 0
    assert caller_readiness.legacy_pair_count == 2
    assert caller_readiness.unmatched_ready_count == 0
    assert caller_readiness.unmatched_first_read_count == 0

    exact_readiness_events = [
        replace(
            event,
            frame_id=event.call_id * 10 + (event.flags & FLAG_DIRECTION_MASK),
        )
        if event.stage in (STAGE_FRAME_READY_TO_POLL, STAGE_FRAME_FIRST_READ)
        else event
        for event in readiness_split_events
    ]
    exact_readiness = collect_receiver_readiness(exact_readiness_events)
    assert exact_readiness[caller_process].exact_pair_count == 2
    assert exact_readiness[caller_process].legacy_pair_count == 0
    readiness_output = io.StringIO()
    print_receiver_readiness_summary(exact_readiness_events, readiness_output)
    readiness_rendered = readiness_output.getvalue()
    assert "Receiver-local readiness split by process" in readiness_rendered
    assert re.search(
        rf"{re.escape(caller_process)}\s+wake -> task poll\s+2\s+0"
        r"\s+10\.000\s+10\.000\s+10\.000",
        readiness_rendered,
    )
    assert re.search(
        rf"{re.escape(caller_process)}\s+task poll -> successful first read\s+2\s+0"
        r"\s+4\.000\s+4\.000\s+4\.000",
        readiness_rendered,
    )
    assert "pairs exact=2 legacy-zero-id=0" in readiness_rendered

    first_target_ready = next(
        event
        for event in exact_readiness_events
        if event.process == target_process
        and event.stage == STAGE_FRAME_READY_TO_POLL
    )
    missing_ready = collect_receiver_readiness(
        [event for event in exact_readiness_events if event is not first_target_ready]
    )[target_process]
    assert missing_ready.ready_count == 0
    assert missing_ready.first_read_count == 1
    assert missing_ready.unmatched_ready_count == 0
    assert missing_ready.unmatched_first_read_count == 1

    split_direct = collect_carrier_gaps({trace_id: split_events})
    assert split_direct.direct_count == 1
    assert split_direct.forwarded_count == 0
    assert split_direct.unclassified_count == 0
    # Readiness starts 10us before first-read in the fixture. Preferring stage
    # 31 therefore removes receiver scheduling from every directional gap.
    assert split_direct.direct_samples == {
        "client_to_caller": [90_000],
        "caller_to_target": [90_000],
        "target_to_caller": [90_000],
        "caller_to_client": [190_000],
    }
    assert split_direct.direct_total_samples == {
        "client_caller_round_trip": [280_000],
        "caller_target_round_trip": [180_000],
        "carrier_chain_total": [460_000],
    }
    first_read_fallback = collect_carrier_gaps(
        {trace_id: [event for event in split_events if event.stage != STAGE_FRAME_READY_TO_POLL]}
    )
    assert first_read_fallback.direct_samples == legacy_direct.direct_samples
    legacy_stage_fallback = collect_carrier_gaps(
        {
            trace_id: [
                event
                for event in split_events
                if event.stage not in (STAGE_FRAME_READY_TO_POLL, STAGE_FRAME_FIRST_READ)
            ]
        }
    )
    assert legacy_stage_fallback.direct_samples == legacy_direct.direct_samples
    exact_write = replace(
        split_events[1],
        frame_id=101,
        start_wall_ns=0,
        end_wall_ns=100,
        duration_ns=100,
    )
    closer_wrong_write = replace(
        exact_write,
        frame_id=202,
        start_wall_ns=250,
        end_wall_ns=290,
        duration_ns=40,
    )
    exact_receive = replace(
        next(event for event in split_events if event.stage == STAGE_FRAME_READY_TO_POLL),
        frame_id=101,
        start_wall_ns=300,
        end_wall_ns=300,
        duration_ns=0,
    )
    assert closest_carrier_pair(
        [exact_write, closer_wrong_write], [exact_receive]
    ) == (exact_write, exact_receive)
    split_stage_output = io.StringIO()
    print_stage_summary(split_events, split_stage_output)
    assert "frame.read" in split_stage_output.getvalue()
    assert "frame.decode" in split_stage_output.getvalue()
    assert "frame.ready-to-poll" in split_stage_output.getvalue()
    split_total_output = io.StringIO()
    print_carrier_gap_summary({trace_id: split_events}, split_total_output)
    split_total_rendered = split_total_output.getvalue()
    assert "Coherent same-trace round-trip and carrier-chain totals" in split_total_rendered
    assert "whole carrier-chain gap total (same trace)" in split_total_rendered
    assert re.search(
        r"whole carrier-chain gap total \(same trace\)\s+1\s+0"
        r"\s+\+460\.000\s+\+460\.000\s+\+460\.000",
        split_total_rendered,
    )

    lookup_event = replace(
        carrier_events[1],
        stage=STAGE_CHANNEL_LOOKUP,
        stage_name="route.channel-lookup",
        connection_id=42,
        flags=FLAG_REQUEST | FLAG_SUCCESS,
        aux=1,
    )
    lookup_output = io.StringIO()
    print_channel_lookup_summary([lookup_event], lookup_output)
    assert "available" in lookup_output.getvalue()

    endpoint_open = replace(
        carrier_events[1],
        stage=STAGE_ENDPOINT_OPEN,
        stage_name="route.endpoint-open",
    )
    fallback = collect_carrier_gaps({trace_id: [*carrier_events, endpoint_open]})
    assert fallback.direct_count == 0
    assert fallback.forwarded_count == 0
    assert fallback.unclassified_count == 1
    assert all(not values for values in fallback.direct_samples.values())
    assert all(not values for values in fallback.direct_total_samples.values())

    relay_process = "daemon@relay-node/pid30"
    forwarded_target_process = "daemon@target-node/pid40"
    forwarded_events = [
        carrier_event(
            client_process,
            "test-client",
            "client-node",
            STAGE_CALL_COMPLETE,
            "call.complete",
            11,
            0,
            2_100_000,
            FLAG_RESPONSE,
        ),
        carrier_event(
            client_process,
            "test-client",
            "client-node",
            STAGE_FRAME_WRITE,
            "frame.write",
            11,
            150_000,
            200_000,
            FLAG_REQUEST,
        ),
        carrier_event(
            caller_process,
            "daemon",
            "caller-node",
            STAGE_FRAME_INGRESS,
            "frame.ingress",
            11,
            300_000,
            320_000,
            FLAG_REQUEST,
        ),
        carrier_event(
            caller_process,
            "daemon",
            "caller-node",
            STAGE_HANDLER_DISPATCH,
            "handler.dispatch",
            11,
            330_000,
            1_750_000,
            FLAG_REQUEST,
        ),
        carrier_event(
            caller_process,
            "daemon",
            "caller-node",
            STAGE_CALL_COMPLETE,
            "call.complete",
            22,
            350_000,
            1_600_000,
            FLAG_RESPONSE,
        ),
        carrier_event(
            caller_process,
            "daemon",
            "caller-node",
            STAGE_FRAME_WRITE,
            "frame.write",
            22,
            450_000,
            500_000,
            FLAG_REQUEST,
        ),
        carrier_event(
            relay_process,
            "daemon",
            "relay-node",
            STAGE_FRAME_INGRESS,
            "frame.ingress",
            22,
            610_000,
            630_000,
            FLAG_REQUEST,
        ),
        carrier_event(
            relay_process,
            "daemon",
            "relay-node",
            STAGE_HANDLER_DISPATCH,
            "handler.dispatch",
            22,
            640_000,
            1_450_000,
            FLAG_REQUEST,
        ),
        carrier_event(
            relay_process,
            "daemon",
            "relay-node",
            STAGE_CALL_COMPLETE,
            "call.complete",
            33,
            650_000,
            1_300_000,
            FLAG_RESPONSE,
        ),
        carrier_event(
            relay_process,
            "daemon",
            "relay-node",
            STAGE_FRAME_WRITE,
            "frame.write",
            33,
            750_000,
            800_000,
            FLAG_REQUEST,
        ),
        carrier_event(
            forwarded_target_process,
            "daemon",
            "target-node",
            STAGE_FRAME_INGRESS,
            "frame.ingress",
            33,
            920_000,
            940_000,
            FLAG_REQUEST,
        ),
        carrier_event(
            forwarded_target_process,
            "daemon",
            "target-node",
            STAGE_FRAME_WRITE,
            "frame.write",
            33,
            1_050_000,
            1_100_000,
            FLAG_RESPONSE,
        ),
        carrier_event(
            relay_process,
            "daemon",
            "relay-node",
            STAGE_PENDING_RESOLVE,
            "pending.resolve",
            33,
            1_230_000,
            1_250_000,
            FLAG_RESPONSE,
        ),
        carrier_event(
            relay_process,
            "daemon",
            "relay-node",
            STAGE_FRAME_WRITE,
            "frame.write",
            22,
            1_350_000,
            1_400_000,
            FLAG_RESPONSE,
        ),
        carrier_event(
            caller_process,
            "daemon",
            "caller-node",
            STAGE_PENDING_RESOLVE,
            "pending.resolve",
            22,
            1_540_000,
            1_560_000,
            FLAG_RESPONSE,
        ),
        carrier_event(
            caller_process,
            "daemon",
            "caller-node",
            STAGE_FRAME_WRITE,
            "frame.write",
            11,
            1_650_000,
            1_700_000,
            FLAG_RESPONSE,
        ),
        carrier_event(
            client_process,
            "test-client",
            "client-node",
            STAGE_PENDING_RESOLVE,
            "pending.resolve",
            11,
            1_850_000,
            1_870_000,
            FLAG_RESPONSE,
        ),
    ]
    legacy_forwarded = collect_carrier_gaps({trace_id: forwarded_events})
    assert legacy_forwarded.direct_count == 0
    assert legacy_forwarded.forwarded_count == 1
    assert legacy_forwarded.unclassified_count == 0
    assert all(not values for values in legacy_forwarded.direct_samples.values())
    assert all(not values for values in legacy_forwarded.direct_total_samples.values())
    assert legacy_forwarded.forwarded_samples == {
        "client_to_caller": [100_000],
        "caller_to_relay": [110_000],
        "relay_to_target": [120_000],
        "target_to_relay": [130_000],
        "relay_to_caller": [140_000],
        "caller_to_client": [150_000],
    }
    assert legacy_forwarded.forwarded_total_samples == {
        "client_caller_round_trip": [250_000],
        "caller_relay_round_trip": [250_000],
        "relay_target_round_trip": [250_000],
        "carrier_chain_total": [750_000],
    }
    split_forwarded = collect_carrier_gaps(
        {trace_id: with_split_carrier_stages(forwarded_events)}
    )
    assert split_forwarded.forwarded_count == 1
    assert split_forwarded.unclassified_count == 0
    assert split_forwarded.forwarded_samples == {
        "client_to_caller": [90_000],
        "caller_to_relay": [100_000],
        "relay_to_target": [110_000],
        "target_to_relay": [120_000],
        "relay_to_caller": [130_000],
        "caller_to_client": [140_000],
    }
    assert split_forwarded.forwarded_total_samples == {
        "client_caller_round_trip": [230_000],
        "caller_relay_round_trip": [230_000],
        "relay_target_round_trip": [230_000],
        "carrier_chain_total": [690_000],
    }
    forwarded_total_output = io.StringIO()
    print_carrier_gap_summary(
        {trace_id: with_split_carrier_stages(forwarded_events)},
        forwarded_total_output,
    )
    assert re.search(
        r"whole carrier-chain gap total \(same trace\)\s+1\s+0"
        r"\s+\+690\.000\s+\+690\.000\s+\+690\.000",
        forwarded_total_output.getvalue(),
    )
    try:
        InputBudget()._read_bounded(io.BytesIO(b"12345"), 4, "bounded-fixture")
    except SummaryError:
        pass
    else:
        raise AssertionError("bounded input reader accepted data beyond its limit")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="*", help="timeline JSON files; use - for stdin")
    parser.add_argument(
        "--gap-threshold-us",
        type=float,
        default=50.0,
        help="minimum residual reported as major (default: 50 us)",
    )
    parser.add_argument(
        "--top-traces",
        type=int,
        default=8,
        help="maximum major traces to report (default: 8)",
    )
    parser.add_argument(
        "--trace",
        help="print the critical timeline for this full trace id or unique prefix",
    )
    parser.add_argument(
        "--timeline-limit",
        type=int,
        default=80,
        help="maximum events in the critical timeline (default: 80)",
    )
    parser.add_argument(
        "--no-critical-timeline",
        action="store_true",
        help="do not print the worst trace timeline unless --trace is supplied",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="run embedded raw-response and test-report fixtures",
    )
    args = parser.parse_args(argv)
    if not args.self_test and not args.inputs:
        parser.error("at least one input file or - is required")
    if args.inputs.count("-") > 1:
        parser.error("stdin may only be used once")
    if (
        not math.isfinite(args.gap_threshold_us)
        or args.gap_threshold_us < 0
        or args.gap_threshold_us > 1_000_000_000
    ):
        parser.error("--gap-threshold-us must be finite and non-negative")
    if args.top_traces < 1 or args.top_traces > 100:
        parser.error("--top-traces must be between 1 and 100")
    if args.timeline_limit < 1 or args.timeline_limit > MAX_TIMELINE_PRINT_EVENTS:
        parser.error(
            f"--timeline-limit must be between 1 and {MAX_TIMELINE_PRINT_EVENTS}"
        )
    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if args.self_test:
        self_test()
        print("self-test passed")
        return 0
    try:
        loaded = load_inputs(args.inputs)
        summarize(
            loaded,
            sys.stdout,
            gap_threshold_us=args.gap_threshold_us,
            top_traces=args.top_traces,
            requested_trace=args.trace,
            timeline_limit=args.timeline_limit,
            show_critical_timeline=not args.no_critical_timeline,
        )
    except (OSError, SummaryError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
