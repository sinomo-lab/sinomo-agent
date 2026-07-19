#!/usr/bin/env python3
"""Convert bounded Sinomo observe captures to Chrome/Perfetto trace JSON.

Each input may be any of the JSON shapes emitted by:

    sinomo observe traces --json
    sinomo observe traces --all --json
    sinomo observe timeline network stop --json
    sinomo test run ... --json  (via diagnostics.timeline_client_capture)

Events with a numeric ``fields.elapsed_us`` become complete duration events
whose end is the observe timestamp. Other events become thread-scoped instant
events. Network timeline durations are converted from nanoseconds into complete
events and anchored to wall time with the response's monotonic capture cursor.
The output can be opened directly in https://ui.perfetto.dev/.

Keep captures bounded at their source, for example with ``--limit 256``. This
converter supports multiple input files and loads one JSON document at a time,
but streams converted events to the output instead of building a second
in-memory event list.
"""

from __future__ import annotations

import argparse
import calendar
import io
import json
import re
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, TextIO


RFC3339_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})T"
    r"(?P<time>\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<fraction>\d+))?"
    r"(?P<offset>Z|[+-]\d{2}:\d{2})$"
)


class ConversionError(ValueError):
    """Raised when an observe trace document does not match its wire shape."""


@dataclass
class TraceWriter:
    output: TextIO
    first_event: bool = True
    process_ids: dict[str, int] = field(default_factory=dict)
    thread_ids: dict[tuple[int, str], int] = field(default_factory=dict)
    next_process_id: int = 1
    next_thread_id: dict[int, int] = field(default_factory=dict)
    record_count: int = 0
    input_errors: list[dict[str, str]] = field(default_factory=list)

    def start(self) -> None:
        self.output.write('{"displayTimeUnit":"us","traceEvents":[')

    def finish(self) -> None:
        self.output.write('],"otherData":')
        json.dump(
            {
                "converter": "scripts/debug/observe-traces-to-perfetto.py",
                "converted_records": self.record_count,
                "input_errors": self.input_errors,
            },
            self.output,
            separators=(",", ":"),
        )
        self.output.write("}\n")

    def emit(self, event: dict[str, Any]) -> None:
        if not self.first_event:
            self.output.write(",")
        self.first_event = False
        json.dump(event, self.output, separators=(",", ":"), ensure_ascii=False)

    def process_id(self, node: str) -> int:
        existing = self.process_ids.get(node)
        if existing is not None:
            return existing
        process_id = self.next_process_id
        self.next_process_id += 1
        self.process_ids[node] = process_id
        self.next_thread_id[process_id] = 1
        self.emit(
            {
                "name": "process_name",
                "ph": "M",
                "pid": process_id,
                "tid": 0,
                "args": {"name": node},
            }
        )
        return process_id

    def thread_id(self, process_id: int, lane: str) -> int:
        key = (process_id, lane)
        existing = self.thread_ids.get(key)
        if existing is not None:
            return existing
        thread_id = self.next_thread_id[process_id]
        self.next_thread_id[process_id] = thread_id + 1
        self.thread_ids[key] = thread_id
        self.emit(
            {
                "name": "thread_name",
                "ph": "M",
                "pid": process_id,
                "tid": thread_id,
                "args": {"name": lane},
            }
        )
        return thread_id

    def add_record(self, node: str, record: dict[str, Any]) -> None:
        timestamp = require_string(record, "timestamp")
        timestamp_us = parse_rfc3339_us(timestamp)
        target = string_or(record.get("target"), "sinomo.observe")
        fields = record.get("fields")
        if fields is None:
            fields = {}
        if not isinstance(fields, dict):
            raise ConversionError("trace record `fields` must be a JSON object")

        traceparent = first_nonempty_string(record.get("traceparent"), fields.get("traceparent"))
        lane = record_lane(target, traceparent, fields)
        process_id = self.process_id(node)
        thread_id = self.thread_id(process_id, lane)
        name = first_nonempty_string(fields.get("stage"), record.get("name")) or "event"

        arguments = dict(fields)
        arguments.update(
            {
                "node": node,
                "level": string_or(record.get("level"), ""),
                "observe_name": string_or(record.get("name"), ""),
                "observe_timestamp_end": timestamp,
            }
        )
        if traceparent:
            arguments["traceparent"] = traceparent
        capture_id = first_nonempty_string(record.get("capture_id"))
        if capture_id:
            arguments["capture_id"] = capture_id

        duration_us = nonnegative_integer(fields.get("elapsed_us"))
        event: dict[str, Any] = {
            "name": name,
            "cat": target,
            "pid": process_id,
            "tid": thread_id,
            "args": arguments,
        }
        if duration_us is None:
            event.update({"ph": "i", "s": "t", "ts": timestamp_us})
        else:
            event.update(
                {
                    "ph": "X",
                    "ts": max(0, timestamp_us - duration_us),
                    "dur": duration_us,
                }
            )
        self.emit(event)
        self.record_count += 1

    def add_timeline_response(
        self,
        response: dict[str, Any],
        default_node: str,
    ) -> None:
        captured_at = require_string(response, "captured_at")
        captured_at_us = parse_rfc3339_us(captured_at)
        captured_monotonic_ns = require_nonnegative_integer(
            response,
            "captured_monotonic_ns",
            "network timeline response",
        )
        source_process_id = require_nonnegative_integer(
            response,
            "process_id",
            "network timeline response",
        )
        process_role = require_string(response, "process_role")
        node = first_nonempty_string(response.get("node_id")) or default_node
        process_label = f"{node} ({process_role}, pid {source_process_id})"
        process_id = self.process_id(process_label)
        capture_id = first_nonempty_string(response.get("capture_id"))

        events = response.get("events")
        if not isinstance(events, list):
            raise ConversionError("network timeline response `events` must be an array")
        response_error = response.get("error")
        if response_error is not None:
            if not isinstance(response_error, str):
                raise ConversionError("network timeline response `error` must be a string")
            if response_error:
                self.input_errors.append(
                    {"target_node_id": node, "error": response_error}
                )

        for raw_event in events:
            if not isinstance(raw_event, dict):
                raise ConversionError("network timeline event must be an object")
            self.add_timeline_event(
                node=node,
                source_process_id=source_process_id,
                process_role=process_role,
                process_id=process_id,
                capture_id=capture_id,
                captured_at=captured_at,
                captured_at_us=captured_at_us,
                captured_monotonic_ns=captured_monotonic_ns,
                record=raw_event,
            )

    def add_timeline_event(
        self,
        *,
        node: str,
        source_process_id: int,
        process_role: str,
        process_id: int,
        capture_id: str | None,
        captured_at: str,
        captured_at_us: int,
        captured_monotonic_ns: int,
        record: dict[str, Any],
    ) -> None:
        event_monotonic_ns = require_nonnegative_integer(
            record,
            "monotonic_ns",
            "network timeline event",
        )
        duration_ns = require_nonnegative_integer(
            record,
            "duration_ns",
            "network timeline event",
        )
        if event_monotonic_ns > captured_monotonic_ns:
            raise ConversionError(
                "network timeline event `monotonic_ns` is after "
                "response `captured_monotonic_ns`"
            )

        stage_name = require_string(record, "stage_name")
        trace_id = require_string(record, "trace_id")
        span_id = require_string(record, "span_id")
        thread_id = self.thread_id(
            process_id,
            timeline_lane(trace_id, record),
        )
        delta_from_capture_ns = captured_monotonic_ns - event_monotonic_ns
        event_end_us = captured_at_us - delta_from_capture_ns / 1_000.0
        duration_us = duration_ns / 1_000.0

        arguments = dict(record)
        arguments.update(
            {
                "node_id": node,
                "process_id": source_process_id,
                "process_role": process_role,
                "captured_at": captured_at,
                "captured_monotonic_ns": captured_monotonic_ns,
                "event_end_us": event_end_us,
                "trace_id": trace_id,
                "span_id": span_id,
            }
        )
        if capture_id:
            arguments["capture_id"] = capture_id

        self.emit(
            {
                "name": stage_name,
                "cat": "sinomo.network.timeline",
                "ph": "X",
                "ts": max(0.0, event_end_us - duration_us),
                "dur": duration_us,
                "pid": process_id,
                "tid": thread_id,
                "args": arguments,
            }
        )
        self.record_count += 1


def parse_rfc3339_us(value: str) -> int:
    match = RFC3339_RE.fullmatch(value)
    if match is None:
        raise ConversionError(f"invalid RFC3339 observe timestamp: {value!r}")
    date_time = datetime.strptime(
        f"{match.group('date')}T{match.group('time')}",
        "%Y-%m-%dT%H:%M:%S",
    )
    offset = match.group("offset")
    if offset == "Z":
        utc = date_time.replace(tzinfo=timezone.utc)
    else:
        sign = 1 if offset[0] == "+" else -1
        hours, minutes = (int(part) for part in offset[1:].split(":"))
        utc = date_time.replace(
            tzinfo=timezone(sign * timedelta(hours=hours, minutes=minutes))
        ).astimezone(timezone.utc)
    seconds = calendar.timegm(utc.utctimetuple())
    fraction = (match.group("fraction") or "")[:6].ljust(6, "0")
    return seconds * 1_000_000 + int(fraction or "0")


def require_string(value: dict[str, Any], key: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result:
        raise ConversionError(f"trace record `{key}` must be a non-empty string")
    return result


def string_or(value: Any, fallback: str) -> str:
    return value if isinstance(value, str) else fallback


def first_nonempty_string(*values: Any) -> str | None:
    for value in values:
        if isinstance(value, str) and value:
            return value
    return None


def nonnegative_integer(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ConversionError("trace `elapsed_us` must be an integer, not a boolean")
    if isinstance(value, int):
        result = value
    elif isinstance(value, str):
        try:
            result = int(value)
        except ValueError as error:
            raise ConversionError(f"trace `elapsed_us` is not an integer: {value!r}") from error
    else:
        raise ConversionError(f"trace `elapsed_us` is not an integer: {value!r}")
    if result < 0:
        raise ConversionError(f"trace `elapsed_us` is negative: {result}")
    return result


def require_nonnegative_integer(
    value: dict[str, Any],
    key: str,
    context: str,
) -> int:
    result = value.get(key)
    if isinstance(result, bool) or not isinstance(result, int):
        raise ConversionError(f"{context} `{key}` must be a non-negative integer")
    if result < 0:
        raise ConversionError(f"{context} `{key}` is negative: {result}")
    return result


def record_lane(target: str, traceparent: str | None, fields: dict[str, Any]) -> str:
    if traceparent:
        return f"trace {short_trace_id(traceparent)}"
    for key in ("session_id", "operation_id", "request_id", "nonce"):
        value = first_nonempty_string(fields.get(key))
        if value:
            return f"{key} {value}"
    route = first_nonempty_string(fields.get("route"))
    if route:
        return f"route {route}"
    return target


def timeline_lane(trace_id: str, record: dict[str, Any]) -> str:
    if trace_id.strip("0"):
        return f"trace {trace_id[-16:]}"
    call_id = record.get("call_id")
    if isinstance(call_id, int) and not isinstance(call_id, bool) and call_id > 0:
        return f"call {call_id}"
    connection_id = record.get("connection_id")
    if (
        isinstance(connection_id, int)
        and not isinstance(connection_id, bool)
        and connection_id > 0
    ):
        return f"connection {connection_id}"
    return "network timeline"


def short_trace_id(traceparent: str) -> str:
    parts = traceparent.split("-")
    if len(parts) >= 4 and len(parts[1]) >= 8:
        return parts[1][-16:]
    return traceparent


def iter_document_records(
    document: Any,
    default_node: str,
) -> tuple[Iterable[tuple[str, dict[str, Any]]], list[dict[str, str]]]:
    if not isinstance(document, dict):
        raise ConversionError("observe trace input must be a JSON object")
    if "targets" in document:
        targets = document.get("targets")
        errors = document.get("errors", [])
        if not isinstance(targets, list) or not isinstance(errors, list):
            raise ConversionError("observe fanout `targets` and `errors` must be arrays")

        def fanout_records() -> Iterable[tuple[str, dict[str, Any]]]:
            for target in targets:
                if not isinstance(target, dict):
                    raise ConversionError("observe fanout target must be an object")
                node = require_string(target, "target_node_id")
                response = target.get("response")
                yield from response_records(response, node)

        normalized_errors = []
        for error in errors:
            if not isinstance(error, dict):
                raise ConversionError("observe fanout error must be an object")
            normalized_errors.append(
                {
                    "target_node_id": string_or(error.get("target_node_id"), "<unknown>"),
                    "error": string_or(error.get("error"), "<unknown>"),
                }
            )
        return fanout_records(), normalized_errors
    return response_records(document, default_node), []


def response_records(response: Any, node: str) -> Iterable[tuple[str, dict[str, Any]]]:
    if not isinstance(response, dict):
        raise ConversionError("observe trace response must be an object")
    records = response.get("records")
    if not isinstance(records, list):
        raise ConversionError("observe trace response `records` must be an array")
    for record in records:
        if not isinstance(record, dict):
            raise ConversionError("observe trace record must be an object")
        yield node, record


def load_document(path: str) -> Any:
    if path == "-":
        return json.load(sys.stdin)
    with Path(path).open("r", encoding="utf-8") as source:
        return json.load(source)


def is_network_timeline_response(document: Any) -> bool:
    return isinstance(document, dict) and "captured_monotonic_ns" in document


def network_timeline_response(document: Any) -> dict[str, Any] | None:
    if is_network_timeline_response(document):
        return document
    if not isinstance(document, dict):
        return None
    diagnostics = document.get("diagnostics")
    if not isinstance(diagnostics, dict):
        return None
    capture = diagnostics.get("timeline_client_capture")
    return capture if is_network_timeline_response(capture) else None


def network_timeline_fanout(
    document: Any,
) -> tuple[list[tuple[str, dict[str, Any]]], list[dict[str, str]]] | None:
    if not isinstance(document, dict) or "targets" not in document:
        return None
    targets = document.get("targets")
    errors = document.get("errors", [])
    if not isinstance(targets, list) or not isinstance(errors, list):
        raise ConversionError("observe fanout `targets` and `errors` must be arrays")
    captures: list[tuple[str, dict[str, Any]]] = []
    for target in targets:
        if not isinstance(target, dict):
            raise ConversionError("observe fanout target must be an object")
        response = target.get("response")
        if not is_network_timeline_response(response):
            return None
        captures.append((require_string(target, "target_node_id"), response))
    if not captures:
        return None
    normalized_errors: list[dict[str, str]] = []
    for error in errors:
        if not isinstance(error, dict):
            raise ConversionError("observe fanout error must be an object")
        normalized_errors.append(
            {
                "target_node_id": string_or(error.get("target_node_id"), "<unknown>"),
                "error": string_or(error.get("error"), "<unknown>"),
            }
        )
    return captures, normalized_errors


def convert(inputs: list[str], output: TextIO, default_node: str) -> int:
    writer = TraceWriter(output)
    writer.start()
    for input_path in inputs:
        document = load_document(input_path)
        fallback_node = default_node
        if fallback_node == "local" and input_path != "-":
            fallback_node = Path(input_path).stem
        timeline_fanout = network_timeline_fanout(document)
        if timeline_fanout is not None:
            captures, errors = timeline_fanout
            writer.input_errors.extend(errors)
            for node, capture in captures:
                writer.add_timeline_response(capture, node)
            continue
        timeline_response = network_timeline_response(document)
        if timeline_response is not None:
            writer.add_timeline_response(timeline_response, fallback_node)
            continue
        records, errors = iter_document_records(document, fallback_node)
        writer.input_errors.extend(errors)
        for node, record in records:
            writer.add_record(node, record)
    writer.finish()
    return writer.record_count


def self_test() -> None:
    legacy_fixture = {
        "targets": [
            {
                "target_node_id": "node-a",
                "response": {
                    "records": [
                        {
                            "timestamp": "2026-07-18T12:00:00.001500Z",
                            "level": "INFO",
                            "target": "sinomo.network.perf",
                            "name": "event crates/sinomo-network/src/route.rs:1",
                            "traceparent": "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
                            "capture_id": "capture-a",
                            "fields": {
                                "stage": "multiplexed_channel_total",
                                "route": "/cluster/network/ping",
                                "elapsed_us": 500,
                            },
                        },
                        {
                            "timestamp": "2026-07-18T12:00:00.001700+00:00",
                            "level": "INFO",
                            "target": "sinomo.network.perf",
                            "name": "instant-marker",
                            "fields": {"stage": "marker", "session_id": "session-a"},
                        },
                    ]
                },
            },
            {
                "target_node_id": "node-b",
                "response": {
                    "records": [
                        {
                            "timestamp": "2026-07-18T12:00:00.001300Z",
                            "level": "INFO",
                            "target": "sinomo.network.perf",
                            "name": "server-event",
                            "traceparent": "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
                            "fields": {"stage": "server_dispatch", "elapsed_us": "100"},
                        }
                    ]
                },
            },
        ],
        "errors": [{"target_node_id": "node-c", "error": "unavailable"}],
    }
    timeline_fixture = {
        "active": False,
        "capture_id": "timeline-a",
        "process_role": "main-daemon",
        "node_id": "node-timeline",
        "process_id": 4242,
        "captured_at": "2026-07-18T12:00:00.010000Z",
        "started_monotonic_ns": 1_000_000,
        "captured_monotonic_ns": 2_000_000,
        "max_events": 16,
        "event_count": 4,
        "dropped_events": 0,
        "overflowed": False,
        "events": [
            {
                "sequence": 7,
                "monotonic_ns": 1_500_000,
                "duration_ns": 250_000,
                "trace_id": "0123456789abcdef0123456789abcdef",
                "span_id": "0123456789abcdef",
                "connection_id": 11,
                "call_id": 13,
                "frame_id": 17,
                "stage": 4,
                "stage_name": "channel.frame-write",
                "flags": 1,
                "bytes": 128,
                "aux": 0,
            },
            {
                "sequence": 8,
                "monotonic_ns": 1_500_000,
                "duration_ns": 80_000,
                "trace_id": "0123456789abcdef0123456789abcdef",
                "span_id": "0123456789abcdef",
                "connection_id": 11,
                "call_id": 13,
                "frame_id": 17,
                "stage": 31,
                "stage_name": "frame.ready-to-poll",
                "flags": 17,
                "bytes": 128,
                "aux": 12,
            },
            {
                "sequence": 9,
                "monotonic_ns": 1_650_000,
                "duration_ns": 150_000,
                "trace_id": "0123456789abcdef0123456789abcdef",
                "span_id": "0123456789abcdef",
                "connection_id": 11,
                "call_id": 13,
                "frame_id": 17,
                "stage": 27,
                "stage_name": "frame.read",
                "flags": 17,
                "bytes": 128,
                "aux": 12,
            },
            {
                "sequence": 10,
                "monotonic_ns": 1_700_000,
                "duration_ns": 50_000,
                "trace_id": "0123456789abcdef0123456789abcdef",
                "span_id": "0123456789abcdef",
                "connection_id": 11,
                "call_id": 13,
                "frame_id": 17,
                "stage": 30,
                "stage_name": "frame.decode",
                "flags": 17,
                "bytes": 128,
                "aux": 12,
            },
        ],
    }

    original_stdin = sys.stdin
    sys.stdin = io.StringIO(json.dumps(legacy_fixture))
    try:
        with tempfile.TemporaryDirectory(prefix="sinomo-perfetto-converter-test-") as temp:
            timeline_path = Path(temp) / "timeline-node.json"
            timeline_path.write_text(json.dumps(timeline_fixture), encoding="utf-8")
            client_path = Path(temp) / "timeline-client-report.json"
            client_fixture = json.loads(json.dumps(timeline_fixture))
            client_fixture["capture_id"] = "timeline-client"
            client_fixture["process_role"] = "test-client"
            client_path.write_text(
                json.dumps({"diagnostics": {"timeline_client_capture": client_fixture}}),
                encoding="utf-8",
            )
            fanout_path = Path(temp) / "timeline-fanout.json"
            fanout_fixture = json.loads(json.dumps(timeline_fixture))
            fanout_fixture["capture_id"] = "timeline-fanout"
            fanout_fixture["node_id"] = "node-fanout"
            fanout_path.write_text(
                json.dumps(
                    {
                        "targets": [
                            {
                                "target_node_id": "node-fanout",
                                "response": fanout_fixture,
                            }
                        ],
                        "errors": [],
                    }
                ),
                encoding="utf-8",
            )
            output = io.StringIO()
            count = convert(
                ["-", str(timeline_path), str(client_path), str(fanout_path)],
                output,
                "local",
            )
    finally:
        sys.stdin = original_stdin
    assert count == 15
    converted = json.loads(output.getvalue())
    events = converted["traceEvents"]
    complete = next(event for event in events if event.get("name") == "multiplexed_channel_total")
    server = next(event for event in events if event.get("name") == "server_dispatch")
    instant = next(event for event in events if event.get("name") == "marker")
    timeline = next(event for event in events if event.get("name") == "channel.frame-write")
    frame_ready = next(event for event in events if event.get("name") == "frame.ready-to-poll")
    frame_read = next(event for event in events if event.get("name") == "frame.read")
    frame_decode = next(event for event in events if event.get("name") == "frame.decode")
    client_timeline = next(
        event
        for event in events
        if event.get("name") == "channel.frame-write"
        and event.get("args", {}).get("process_role") == "test-client"
    )
    fanout_timeline = next(
        event
        for event in events
        if event.get("name") == "channel.frame-write"
        and event.get("args", {}).get("capture_id") == "timeline-fanout"
    )
    assert complete["ph"] == "X" and complete["dur"] == 500
    assert complete["ts"] == parse_rfc3339_us("2026-07-18T12:00:00.001000Z")
    assert server["ph"] == "X" and server["dur"] == 100
    assert instant["ph"] == "i" and instant["s"] == "t"
    assert complete["args"]["capture_id"] == "capture-a"
    assert complete["pid"] != server["pid"]
    assert timeline["ph"] == "X" and timeline["dur"] == 250.0
    expected_end_us = parse_rfc3339_us("2026-07-18T12:00:00.009500Z")
    expected_start_us = parse_rfc3339_us("2026-07-18T12:00:00.009250Z")
    assert timeline["args"]["event_end_us"] == expected_end_us
    assert timeline["ts"] == expected_start_us
    assert timeline["args"]["process_id"] == 4242
    assert timeline["args"]["trace_id"] == timeline_fixture["events"][0]["trace_id"]
    assert frame_ready["ph"] == "X" and frame_ready["dur"] == 80.0
    assert frame_read["ph"] == "X" and frame_read["dur"] == 150.0
    assert frame_decode["ph"] == "X" and frame_decode["dur"] == 50.0
    assert client_timeline["args"]["capture_id"] == "timeline-client"
    assert fanout_timeline["args"]["node_id"] == "node-fanout"
    assert converted["otherData"]["converted_records"] == 15
    assert converted["otherData"]["input_errors"] == legacy_fixture["errors"]


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "inputs",
        nargs="*",
        help="observe trace JSON files; use - for stdin",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="-",
        help="Chrome/Perfetto JSON output path (default: stdout)",
    )
    parser.add_argument(
        "--node",
        default="local",
        help="node name for a non-fanout response (default: input filename)",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="run the embedded conversion fixture and exit",
    )
    args = parser.parse_args(argv)
    if not args.self_test and not args.inputs:
        parser.error("at least one input file or - is required")
    if args.inputs.count("-") > 1:
        parser.error("stdin may only be used once")
    return args


def output_stream(path: str) -> tuple[TextIO, bool]:
    if path == "-":
        return sys.stdout, False
    return Path(path).open("w", encoding="utf-8"), True


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.self_test:
        self_test()
        print("observe trace Perfetto converter self-test passed")
        return 0
    input_paths = {
        Path(path).resolve() for path in args.inputs if path != "-"
    }
    if args.output != "-" and Path(args.output).resolve() in input_paths:
        raise ConversionError("output path must not overwrite an input trace")
    output, should_close = output_stream(args.output)
    try:
        convert(args.inputs, output, args.node)
    finally:
        if should_close:
            output.close()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ConversionError, json.JSONDecodeError, OSError) as error:
        print(f"observe trace conversion failed: {error}", file=sys.stderr)
        raise SystemExit(2) from error
