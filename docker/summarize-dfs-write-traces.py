#!/usr/bin/env python3
"""Summarize ordinary DFS write phases from bounded workload daemon logs.

The default `cargo build -p sinomo` does not enable in-process CPU profiling.
For its bounded CPU sampler, build a diagnostic binary explicitly:
  cargo build --locked --offline -p sinomo -p sinomo-observe --features sinomo-observe/cpu-profiler
Recreate diagnostic containers with that executable and verify process hashes.
Alternatively use docker/docker-compose.profile.yml's opt-in perf sidecar to
sample existing default-feature daemon processes; trace control needs no feature.

After cluster formation, run these on each node with one unique capture ID:
  sinomo observe control --capture ID --filter sinomo.dfs.perf=trace --clear-traces --json
  sinomo observe profile cpu start --capture ID --duration-seconds 30 --frequency-hz 99 --json
Record byte offsets of /data/daemon/main.{stdout,stderr}.log before the workload.
Afterwards save the appended bytes as main-daemon-nodeN.workload.{stdout,stderr}.log
for all six nodes in ARTIFACT_DIR. Preserve the full logs separately if a daemon
restarted or a log shrank; such a capture is not a bounded workload sample.
Stop the CPU capture (or let its watchdog expire), then:
  sinomo observe control --clear-capture --expected-capture ID --json
  sinomo observe traces --capture ID --limit 0 --json
  sinomo observe profile cpu status --json
An unsupported CPU state is not a capture. When the CPU state is complete, copy
/data/observe/profiles/ID/ from the container.
Render its cpu.pprof.pb with scripts/debug/render-pprof-flamegraph.py and inspect
`go tool pprof -top -nodecount=40 cpu.pprof.pb` for CPU attribution.

Usage: python3 docker/summarize-dfs-write-traces.py ARTIFACT_DIR
       python3 docker/summarize-dfs-write-traces.py --self-test

The summary uses nearest-rank quantiles, keeps missing values absent, and retains
per-event identities in JSONL. Nested/parallel phase times and phase quantiles
are never added. These diagnostic timings do not replace the client latency
acceptance measurement; trace logging and CPU sampling perturb the workload.
"""

import argparse
from collections import Counter, defaultdict
import csv
import json
import math
from pathlib import Path
import re
import statistics
import sys
import unittest


ANSI = re.compile(r"\x1b\[[0-9;]*[mK]")
TARGET = re.compile(r"\bsinomo\.dfs\.perf:\s*")
FIELD = re.compile(r'\b([A-Za-z][\w-]*)=("(?:\\.|[^"\\])*"|[^\s]+)')
NUMBER = re.compile(r"-?\d+(?:\.\d+)?")
TIMESTAMP = re.compile(r"\d{4}-\d\d-\d\dT[^\s]+")
TRACEPARENT = re.compile(r"[0-9a-f]{2}-([0-9a-f]{32})-[0-9a-f]{16}-[0-9a-f]{2}")
LOG_NAME = re.compile(r"main-daemon-(.+)\.workload\.(stdout|stderr)\.log")
UNITS = {"_ns": 0.000001, "_us": 0.001, "_ms": 1.0}
GROUP_FIELDS = ("kind", "mode", "phase", "outcome", "create")


def parse_line(line, node, source, line_number):
    """Accept tracing's plain/ANSI text format, including %args detail fields."""
    line = ANSI.sub("", line)
    target = TARGET.search(line)
    if target is None:
        return None
    body = line[target.end():].strip()
    fields = {}
    for match in FIELD.finditer(body):
        key, value = match.groups()
        if value.startswith('"'):
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                pass
        fields[key] = value
    detail = re.search(r"\bdetail=", body)
    if detail:
        payload = body[detail.end():].strip()
        # tracing normally emits Display fields unquoted. Also accept a quoted
        # detail, as exported text can quote it and contain escaped paths.
        if payload.startswith('"'):
            try:
                payload, _ = json.JSONDecoder().raw_decode(payload)
            except json.JSONDecodeError:
                pass
        for match in FIELD.finditer(payload):
            fields[match.group(1)] = match.group(2).strip('"')
    else:
        payload = body
    record = payload.split(maxsplit=1)[0] if payload else ""
    if not re.fullmatch(r"[a-z][a-z0-9-]+", record):
        return None
    fields.pop("detail", None)
    metrics = {}
    for key, value in fields.items():
        for suffix, scale in UNITS.items():
            if key.endswith(suffix) and NUMBER.fullmatch(str(value)):
                metrics[key] = float(value) * scale
                break
    timestamp = TIMESTAMP.match(line)
    traceparent = fields.get("traceparent", "")
    trace = TRACEPARENT.fullmatch(traceparent)
    correlation = f"trace:{trace.group(1)}" if trace else ""
    return {
        "node": node,
        "source": source,
        "line": line_number,
        "timestamp": timestamp.group(0) if timestamp else "",
        "record": record,
        "correlation": correlation,
        "fields": fields,
        "metrics_ms": metrics,
    }


def quantiles(values):
    ordered = sorted(values)
    return {
        "samples": len(ordered),
        "minimum_ms": ordered[0],
        "p50_ms": ordered[math.ceil(len(ordered) * 0.50) - 1],
        "p95_ms": ordered[math.ceil(len(ordered) * 0.95) - 1],
        "maximum_ms": ordered[-1],
        "mean_ms": statistics.mean(ordered),
    }


def attach_request_kinds(events):
    # Transport request counters are peer-local and can repeat on one daemon.
    # Only a propagated traceparent plus the request identifies this handler.
    # Untraced send/dispatch records stay unclassified instead of borrowing the
    # kind of an unrelated incoming request with the same counter.
    kinds = {
        (event["node"], event["fields"].get("traceparent"), event["fields"].get("request")):
        event["fields"]["kind"]
        for event in events
        if event["record"] == "dfs-message-handler-start"
        and "kind" in event["fields"] and event["fields"].get("traceparent")
    }
    for event in events:
        kind = kinds.get((event["node"], event["fields"].get("traceparent"),
                          event["fields"].get("request")))
        if kind:
            event["fields"].setdefault("kind", kind)


def summarize(events):
    groups = defaultdict(list)
    for event in events:
        fields = event["fields"]
        dimensions = tuple(fields.get(key, "") for key in GROUP_FIELDS)
        for metric, value in event["metrics_ms"].items():
            for node in ("all", event["node"]):
                groups[(node, event["record"], *dimensions, metric)].append(value)
    rows = []
    for key, values in sorted(groups.items()):
        row = dict(zip(("node", "record", *GROUP_FIELDS, "metric"), key))
        row.update(quantiles(values))
        rows.append(row)
    return rows


def correlated_operations(events):
    groups = defaultdict(list)
    for event in events:
        if event["correlation"]:
            groups[event["correlation"]].append(event)
    rows = []
    for correlation, records in groups.items():
        totals = [value for record in records for key, value in record["metrics_ms"].items()
                  if key in ("total_ns", "total_us", "total_ms")]
        if not totals:
            continue
        rows.append({
            "correlation": correlation,
            "max_observed_total_ms": max(totals),
            "nodes": sorted({record["node"] for record in records}),
            "records": records,
        })
    return sorted(rows, key=lambda row: row["max_observed_total_ms"], reverse=True)


def write_jsonl(path, records):
    with path.open("w") as output:
        for record in records:
            output.write(json.dumps(record, sort_keys=True) + "\n")


def run(artifacts):
    inputs = sorted(artifacts.glob("main-daemon-*.workload.*.log"))
    events = []
    coverage = []
    for path in inputs:
        match = LOG_NAME.fullmatch(path.name)
        if match is None:
            continue
        node, source = match.groups()
        count = Counter()
        with path.open(errors="replace") as lines:
            for line_number, line in enumerate(lines, 1):
                count["lines"] += 1
                event = parse_line(line, node, source, line_number)
                if event is not None:
                    events.append(event)
                    count["events"] += 1
                    count["timed_events"] += bool(event["metrics_ms"])
                elif "sinomo.dfs.perf" in line:
                    count["unparsed_perf_lines"] += 1
        coverage.append({"file": path.name, **count})
    attach_request_kinds(events)
    rows = summarize(events)
    if not rows:
        raise ValueError("no DFS phase timing records found in workload logs")
    write_jsonl(artifacts / "dfs-write-trace-events.jsonl", events)
    write_jsonl(artifacts / "dfs-write-trace-operations.jsonl", correlated_operations(events))
    with (artifacts / "dfs-write-trace-summary.tsv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]), delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    present = {entry["file"] for entry in coverage}
    missing = [f"main-daemon-node{node}.workload.{source}.log"
               for node in range(1, 7) for source in ("stdout", "stderr")
               if f"main-daemon-node{node}.workload.{source}.log" not in present]
    status = {
        "quantile_method": "nearest rank",
        "metric_unit": "milliseconds (including converted _us and _ns fields)",
        "inputs": coverage,
        "missing_logs": missing,
        "events": len(events),
        "timed_events": sum(bool(event["metrics_ms"]) for event in events),
        "correlated_events": sum(bool(event["correlation"]) for event in events),
        "limits": [
            "Nested and parallel phases overlap; do not sum phase values or quantiles.",
            "max_observed_total_ms ranks observed events, not end-to-end request latency.",
            "Missing metrics are absent, not zero. Quantiles are diagnostic samples only.",
            "Log bounds, process continuity, and workload purity must be verified by the collector.",
            "Untraced request IDs are not correlated: peer-local request counters can repeat.",
        ],
    }
    (artifacts / "dfs-write-trace-coverage.json").write_text(json.dumps(status, indent=2) + "\n")
    report = ["# DFS write phase diagnostics", "", f"Parsed {len(events)} events from {len(coverage)} logs.",
              "Timings are milliseconds; p95 uses nearest rank. Nested phases overlap.", "",
              "| Record | Kind / mode / phase | Metric | Samples | p50 | p95 | Max |",
              "|---|---|---|---:|---:|---:|---:|"]
    leaders = sorted((row for row in rows if row["node"] == "all"),
                     key=lambda row: row["p95_ms"], reverse=True)
    for row in leaders[:50]:
        dimensions = "/".join(str(row[key]) for key in GROUP_FIELDS if row[key])
        report.append(f"| {row['record']} | {dimensions} | {row['metric']} | {row['samples']} | "
                      f"{row['p50_ms']:.3f} | {row['p95_ms']:.3f} | {row['maximum_ms']:.3f} |")
    report.extend(["", "See the TSV for all phases and per-node samples; JSONL retains source lines and identities."])
    if missing:
        report.extend(["", "Missing logs: " + ", ".join(missing)])
    (artifacts / "dfs-write-trace-report.md").write_text("\n".join(report) + "\n")
    print(json.dumps({"events": len(events), "summary_rows": len(rows), "missing_logs": missing}))


class TraceTests(unittest.TestCase):
    def test_detail_ansi_correlation_and_submillisecond_values(self):
        event = parse_line(
            "\x1b[2m2026-09-27T01:02:03Z\x1b[0m TRACE sinomo.dfs.perf: dfs perf "
            "traceparent=00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01 "
            "detail=dfs-message-handler request=req admission_wait_us=250 total_ms=12", "node1", "stdout", 7)
        self.assertEqual(event["correlation"], "trace:" + "a" * 32)
        self.assertEqual(event["metrics_ms"], {"admission_wait_us": 0.25, "total_ms": 12})
        self.assertEqual(event["line"], 7)

    def test_structured_record_and_missing_metric_are_not_zero(self):
        event = parse_line("TRACE sinomo.dfs.perf: metadata-publish-plan outcomes=2 total_ms=5",
                           "node2", "stderr", 1)
        other = parse_line("TRACE sinomo.dfs.perf: metadata-publish-plan outcomes=0",
                           "node2", "stderr", 2)
        rows = summarize([event, other])
        self.assertEqual(rows[0]["samples"], 1)
        self.assertEqual(rows[0]["p95_ms"], 5)
        self.assertIsNone(parse_line("TRACE unrelated: total_ms=900", "node2", "stderr", 3))

    def test_phase_kind_and_node_separation(self):
        lines = ["dfs-message-handler-start request=r kind=distributed-write-request",
                 "dfs-message-handler request=r admission_wait_ms=7 total_ms=12",
                 "attachment-phase operation=r phase=read elapsed_us=1200",
                 "attachment-phase operation=r phase=stage elapsed_us=200"]
        events = [parse_line("TRACE sinomo.dfs.perf: dfs perf "
                             "traceparent=00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01 detail=" + line,
                             "node1", "stdout", index) for index, line in enumerate(lines)]
        attach_request_kinds(events)
        self.assertEqual(events[1]["fields"]["kind"], "distributed-write-request")
        phases = {row["phase"] for row in summarize(events) if row["record"] == "attachment-phase"}
        self.assertEqual(phases, {"read", "stage"})
        operations = correlated_operations(events)
        self.assertEqual(operations[0]["max_observed_total_ms"], 12)

    def test_reused_request_ids_require_the_same_traceparent(self):
        parent_a = "00-" + "a" * 32 + "-" + "1" * 16 + "-01"
        parent_b = "00-" + "b" * 32 + "-" + "2" * 16 + "-01"
        lines = [
            f"traceparent={parent_a} detail=dfs-message-handler-start request=dfs_message_1 kind=stage-blob",
            f"traceparent={parent_b} detail=dfs-message-handler-start request=dfs_message_1 kind=fetch-blob",
            f"traceparent={parent_a} detail=dfs-message-handler request=dfs_message_1 total_ms=100",
            f"traceparent={parent_b} detail=dfs-message-handler request=dfs_message_1 total_ms=2",
            "detail=dfs-message-send request=dfs_message_1 total_ms=500",
        ]
        events = [parse_line("TRACE sinomo.dfs.perf: dfs perf " + line,
                             "node5", "stdout", index) for index, line in enumerate(lines)]
        attach_request_kinds(events)
        self.assertEqual(events[2]["fields"]["kind"], "stage-blob")
        self.assertEqual(events[3]["fields"]["kind"], "fetch-blob")
        self.assertNotIn("kind", events[4]["fields"])
        self.assertEqual(events[4]["correlation"], "")
        operations = correlated_operations(events)
        self.assertEqual([row["max_observed_total_ms"] for row in operations], [100, 2])

    def test_stage_modes_do_not_pool_identically_named_phases(self):
        events = [parse_line(
            "TRACE sinomo.dfs.perf: dfs perf detail=stage-blob-phase "
            f"mode={mode} phase=content-persistence elapsed_us={elapsed}",
            "node5", "stdout", index,
        ) for index, (mode, elapsed) in enumerate((("inline", 1000), ("preingested", 9000)))]
        rows = [row for row in summarize(events) if row["node"] == "all"]
        self.assertEqual(len(rows), 2)
        self.assertEqual({row["mode"]: (row["samples"], row["p95_ms"]) for row in rows},
                         {"inline": (1, 1), "preingested": (1, 9)})

    def test_nearest_rank_p95_and_quoted_detail(self):
        self.assertEqual(quantiles(list(range(1, 21)))["p95_ms"], 19)
        event = parse_line('TRACE sinomo.dfs.perf: dfs perf detail="posix-byte-write total_ms=3"',
                           "node1", "stdout", 1)
        self.assertEqual(event["metrics_ms"]["total_ms"], 3)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("artifacts", type=Path, nargs="?")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        result = unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromTestCase(TraceTests))
        return 0 if result.wasSuccessful() else 1
    if args.artifacts is None:
        parser.error("ARTIFACT_DIR is required unless --self-test is used")
    try:
        run(args.artifacts)
    except (OSError, ValueError) as error:
        print(f"DFS trace summary failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
