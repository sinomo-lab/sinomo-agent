#!/usr/bin/env python3
"""Summarize Observe captures without summing overlapping stages or quantiles.

Input directory: manifest.json and <node>/traces-*.json snapshots from
capture-performance.py. Supports retained snapshots and process restarts.
"""
import argparse
import collections
import json
import math
from pathlib import Path

IO_KINDS = ("net.tx", "net.rx", "disk.read", "disk.write", "disk.sync_all", "disk.sync_data", "disk.sync_directory")


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


def process_key(process):
    return (process.get("node_id"), process.get("process_role"), process.get("process_id"), process.get("started_at"))


def counter_delta(before, after):
    if not isinstance(before, int) or not isinstance(after, int) or after < before:
        return None
    return after - before


def logical_node_io(spans):
    """Sum leaf-assigned I/O once; inclusive ancestor rows overlap."""
    totals = collections.defaultdict(collections.Counter)
    for span in spans:
        totals[(span["node"], tuple(span["process"]))].update(span["io_direct"])
    return [{"node": node, "process": list(process), "io": dict(io)}
            for (node, process), io in sorted(totals.items(), key=lambda item: str(item[0]))]


def rpc_activity(spans):
    """Group complete local RPC spans; client and server lifetimes overlap."""
    groups = collections.defaultdict(list)
    for span in spans:
        if span['stage'] in ('network.server', 'network.service_rpc'):
            groups[(span['node'], tuple(span['process']), span['stage'],
                    span.get('rpc_route') or 'unavailable')].append(span)
    rows = []
    for (node, process, stage, route), values in sorted(groups.items(), key=lambda item: str(item[0])):
        durations = [span['duration_ns'] / 1e6 for span in values]
        rows.append({'node': node, 'process': list(process), 'stage': stage, 'route': route,
                     'samples': len(values), 'p50_ms': percentile(durations, .5),
                     'p95_ms': percentile(durations, .95), 'max_ms': max(durations),
                     'outcomes': dict(collections.Counter(span['outcome'] for span in values))})
    return rows


def activity_counters(root, nodes):
    """Counter deltas require both complete snapshots and the same process."""
    def metric_delta(before, after):
        # Observe metric values use JSON floating-point numbers, unlike the
        # integer kernel counters used by counter_delta.
        if (any(type(value) not in (int, float) or not math.isfinite(value)
                for value in (before, after)) or after < before):
            return None
        return after - before
    rows = []
    for node in nodes:
        paths = [root / node / f"metrics-{index:06}.json" for index in (0, 1)]
        if not all(path.exists() for path in paths):
            continue
        before, after = [json.loads(path.read_text()) for path in paths]
        valid = (not before.get("partial", True) and not after.get("partial", True)
                 and before.get("process") and after.get("process")
                 and process_key(before["process"]) == process_key(after["process"]))
        def counters(snapshot):
            return {(m["name"], json.dumps(m.get("labels", {}), sort_keys=True)): m
                    for m in snapshot.get("metrics", []) if m.get("kind") in ("counter", "gauge")}
        first, last = counters(before), counters(after)
        for key in sorted(first.keys() | last.keys()):
            old, new = first.get(key), last.get(key)
            # A newly registered series has no measured starting value. Do not
            # turn absent, truncated or restarted counters into invented zeros.
            kind = (new or old)["kind"]
            delta = (metric_delta(old.get("value"), new.get("value"))
                     if valid and old and new and old['kind'] == new['kind'] == 'counter' else None)
            rows.append({"node": node, "name": key[0], "labels": json.loads(key[1]),
                         "kind": kind, "delta": delta, "before": old.get("value") if old else None,
                         "after": new.get("value") if new else None})
    return rows


def summarize(root):
    root = Path(root)
    manifest = json.loads((root / "manifest.json").read_text())
    issues = list(manifest.get("errors", []))
    if manifest.get("workload_exit_code") != 0:
        issues.append("Workload did not complete successfully")
    stages, process_rows, coverage, trace_rows = [], [], [], []
    for node in manifest["nodes"]:
        records, samples = {}, collections.defaultdict(list)
        node_issues = []
        last_sequence = {}
        ring_evictions = 0
        paths = sorted((root / node).glob("traces-*.json"))
        for path in paths:
            response = json.loads(path.read_text())
            process = response.get("process")
            if not process:
                node_issues.append("process identity unavailable (old binary)")
                continue
            key = process_key(process)
            if response.get("process_window_sample", True):
                samples[key].append(process)
            dropped = response.get("dropped_records", 0)
            ring_evictions = max(ring_evictions, dropped)
            sequences = [r["sequence"] for r in response.get("records", [])
                         if r.get("capture_id") == manifest["capture_id"] and r.get("sequence")]
            previous_sequence = last_sequence.get(key)
            if response.get("collection_snapshot_only"):
                # Counter-only window samples intentionally contain no records;
                # export, using the initial cursor, establishes trace coverage.
                continue
            if "last_sequence" in response:
                # New API reports loss relative to the requested cursor. Ring
                # eviction of already exported records is not missing evidence.
                if response.get("truncated"):
                    node_issues.append("trace ring/response truncated")
            else:
                if response.get("truncated") and not dropped:
                    node_issues.append("trace ring/response truncated")
                if dropped and (previous_sequence is None or not sequences or min(sequences) > previous_sequence + 1):
                    node_issues.append("trace ring/response truncated")
            if sequences:
                last_sequence[key] = max(max(sequences), previous_sequence or 0)
            for record in response.get("records", []):
                if record.get("capture_id") != manifest["capture_id"]:
                    continue
                if not record.get("sequence"):
                    node_issues.append("trace sequence unavailable")
                    continue
                identity = (key, record["sequence"])
                if identity in records and records[identity] != record:
                    node_issues.append("conflicting duplicate trace sequence")
                records[identity] = record
                if record.get("context_truncated"):
                    node_issues.append("span context truncated")
        if not paths or not samples:
            node_issues.append("node not observed")
        if len(samples) > 1:
            node_issues.append("process incarnation changed; intervals kept separate")
        # Observation RPCs appear in their own snapshots before they finish.
        # Their traffic remains in node counters, but is not workload latency.
        excluded = set()
        for (key, _), record in records.items():
            scope = record.get("spans", [])
            observed = next((i for i, span in enumerate(scope) if span.get("target") == "sinomo.perf" and span.get("fields", {}).get("observation") is True), None)
            if observed is None:
                continue
            start = next((i for i in range(observed, -1, -1) if scope[i].get("fields", {}).get("stage") == "network.server"), observed)
            excluded.update((key, span["span_id"]) for span in scope[start:])
        observed_records = len(records)
        records = {identity: record for identity, record in records.items()
                   if not any((identity[0], span["span_id"]) in excluded for span in record.get("spans", []))}
        observer_records = observed_records - len(records)
        opened, closed = {}, {}
        for (key, sequence), record in records.items():
            fields = record.get("fields") or {}
            identity = (key, fields.get("span_id"))
            kind = fields.get("record_kind")
            if kind == "span_started":
                opened[identity] = record
            elif kind == "span_closed":
                if identity in closed:
                    node_issues.append("duplicate span close")
                closed[identity] = record
        for identity in opened.keys() - closed.keys():
            node_issues.append("unfinished span(s)")
        valid = {}
        for identity, record in closed.items():
            fields = record["fields"]
            if identity not in opened or not fields.get("capture_complete"):
                node_issues.append("span crosses capture boundary or start is missing")
                continue
            duration = fields.get("duration_ns")
            if not isinstance(duration, int) or duration < 0:
                node_issues.append("invalid duration")
                continue
            valid[identity] = record
        # Allocate I/O once to its leaf span; an inclusive rollup is provided for
        # each ancestor instance, then summarized per stage. Never sum rows.
        direct = collections.defaultdict(lambda: collections.Counter())
        inclusive = collections.defaultdict(lambda: collections.Counter())
        unattributed = collections.Counter()
        peers = collections.defaultdict(set)
        for (key, _), record in records.items():
            fields = record.get("fields") or {}
            io_kind = fields.get("io_kind")
            if io_kind not in IO_KINDS or fields.get("record_kind") == "span_started":
                continue
            # Span I/O is emitted at close, while network records are events.
            if fields.get("record_kind") and fields["record_kind"] != "span_closed":
                continue
            chain = [(key, s["span_id"]) for s in record.get("spans", []) if (key, s["span_id"]) in valid]
            counters = collections.Counter({io_kind + ".ops": 1})
            if isinstance(fields.get("io_bytes"), int):
                counters[io_kind + ".bytes"] = fields["io_bytes"]
            else:
                counters[io_kind + ".unknown_bytes"] = 1
            if fields.get("outcome") != "ok":
                counters[io_kind + ".errors_or_unknown"] = 1
            if isinstance(fields.get("duration_ns"), int):
                counters[io_kind + ".ns"] = fields["duration_ns"]
            if not chain:
                unattributed.update(counters)
            else:
                direct[chain[-1]].update(counters)
                for span in chain:
                    inclusive[span].update(counters)
            peer = next((s.get("fields", {}).get("peer") for s in reversed(record.get("spans", [])) if s.get("fields", {}).get("peer")), None)
            if peer:
                for span in chain:
                    peers[span].add(peer)
        groups = collections.defaultdict(list)
        for identity, record in valid.items():
            key, span_id = identity
            fields = record["fields"]
            dimension = fields.get("phase") or fields.get("io_kind") or fields.get("owner")
            stage_name = fields["stage"] + ("/" + dimension if dimension else "")
            groups[(key, stage_name)].append((identity, record))
            traceparent = record.get("traceparent")
            if not traceparent:
                # A server decodes its trace context after reading the first
                # record. Use final ancestor context only within this process.
                for ancestor in reversed(record.get("spans", [])):
                    final = closed.get((key, ancestor["span_id"]), {})
                    traceparent = final.get("traceparent")
                    if traceparent:
                        break
            rpc_request_id = next((span.get("fields", {}).get("rpc_request_id") for span in reversed(record.get("spans", [])) if span.get("fields", {}).get("rpc_request_id")), None)
            trace_rows.append({"node": node, "process": list(key), "span_id": span_id,
                               "traceparent": traceparent, "rpc_request_id": rpc_request_id, "stage": stage_name,
                               "start_ns": fields["started_monotonic_ns"], "duration_ns": fields["duration_ns"],
                               "ancestors": [s["span_id"] for s in record.get("spans", [])[:-1]],
                               "io_direct": dict(direct[identity]), "io_inclusive": dict(inclusive[identity]),
                               "outcome": fields.get("outcome", "unknown"), "peers": sorted(peers[identity]),
                               "rpc_route": next((sp.get("fields", {}).get("rpc_route") for sp in reversed(record.get("spans", [])) if sp.get("fields", {}).get("rpc_route")), None)})
        for (key, stage), entries in sorted(groups.items(), key=lambda x: str(x[0])):
            durations = [r["fields"]["duration_ns"] / 1e6 for _, r in entries]
            direct_total, inclusive_total, peer_set = collections.Counter(), collections.Counter(), set()
            outcomes = collections.Counter()
            for identity, record in entries:
                direct_total.update(direct[identity]); inclusive_total.update(inclusive[identity])
                peer_set.update(peers[identity])
                outcome = record["fields"].get("outcome", "unknown")
                # A completed background pass may explicitly retain bounded
                # continuation work. Cancellation/defer/unfinished are unknown,
                # never omitted from the standard outcome accounting.
                outcome = "ok" if outcome in ("ok", "complete", "continuation") else "error" if outcome == "error" else "unknown"
                outcomes[outcome] += 1
            stages.append({"node": node, "process": list(key), "stage": stage, "samples": len(entries),
                           "p50_ms": percentile(durations, .5), "p95_ms": percentile(durations, .95),
                           "p99_ms": percentile(durations, .99), "max_ms": max(durations),
                           "outcomes": dict(outcomes), "io_direct": dict(direct_total),
                           "io_inclusive": dict(inclusive_total), "peers": sorted(peer_set)})
        for key, values in samples.items():
            values.sort(key=lambda s: s["observed_monotonic_ns"])
            before, after = values[0], values[-1]
            deltas = {field: counter_delta(before.get("io", {}).get(field), after.get("io", {}).get(field))
                      for field in ("read_bytes", "write_bytes", "read_chars", "write_chars", "read_syscalls", "write_syscalls", "cancelled_write_bytes", "framed_net_sent_bytes", "framed_net_received_bytes", "namespace_net_sent_bytes", "namespace_net_received_bytes")}
            window = after["observed_monotonic_ns"] - before["observed_monotonic_ns"]
            if len(values) < 2 or window <= 0:
                node_issues.append("process window missing")
                deltas = {field: None for field in deltas}
            process_rows.append({"node": node, "process": list(key), "window_ms": window / 1e6, "io_delta": deltas})
        if not valid and not (manifest.get("workload_kind") == "idle" and not opened and not closed):
            node_issues.append("no complete stage samples")
        if unattributed:
            node_issues.append("I/O without a complete captured stage")
        latest_context = {}
        for (process, _), record in records.items():
            for span in record.get("spans", []):
                latest_context[(process, span["span_id"])] = span
        window_ends = {process: max(s["observed_monotonic_ns"] for s in values) for process, values in samples.items()}
        censored_spans = []
        for identity in sorted((opened.keys() | closed.keys()) - valid.keys(), key=repr):
            record = closed.get(identity) or opened[identity]
            fields = latest_context.get(identity, {}).get("fields", record["fields"])
            started = opened.get(identity, {}).get("fields", {}).get("started_monotonic_ns")
            end = window_ends.get(identity[0])
            censored_spans.append({"node": node, "process": list(identity[0]), "span_id": identity[1],
                                   "ancestors": [sp["span_id"] for sp in record.get("spans", [])[:-1]],
                                   "stage": record["fields"].get("stage"),
                                   "rpc_request_id": fields.get("rpc_request_id"), "rpc_route": fields.get("rpc_route"),
                                   "peer": fields.get("peer"), "phase": fields.get("phase"),
                                   "elapsed_lower_bound_ms": max(0, end-started)/1e6 if started is not None and end is not None else None})
        coverage.append({"node": node, "records": len(records), "spans_started": len(opened), "spans_closed": len(closed),
                         "complete_spans": len(valid), "censored_spans": censored_spans, "observer_records_excluded": observer_records, "ring_evictions": ring_evictions, "unattributed_io": dict(unattributed), "issues": sorted(set(node_issues))})
        issues.extend(f"{node}: {issue}" for issue in sorted(set(node_issues)))
    return {"schema_version": 1, "capture_id": manifest["capture_id"], "classification": "diagnostic",
            "complete": not issues, "issues": issues, "stages": stages, "processes": process_rows,
            "coverage": coverage, "spans": trace_rows, "manifest": manifest,
            "node_io": logical_node_io(trace_rows), "rpc_activity": rpc_activity(trace_rows),
            "activity_counters": activity_counters(root, manifest["nodes"])}


def operation_view(report, root_stage, expected_roots=None, root_node=None, root_span_id=None):
    """Follow local ancestry and exact RPC request IDs; never join by wall time."""
    def key(row):
        return tuple(row["process"]) + (row["span_id"],)
    spans = {key(row): row for row in report["spans"]}
    children = collections.defaultdict(set)
    censored = set()
    censored_rows = {}
    for row in report["spans"]:
        for parent in row["ancestors"]:
            children[tuple(row["process"]) + (parent,)].add(key(row))
    for coverage in report["coverage"]:
        for row in coverage.get("censored_spans", []):
            censored.add(key(row))
            censored_rows[key(row)] = row
            for parent in row["ancestors"]:
                children[tuple(row["process"]) + (parent,)].add(key(row))
    servers, clients = collections.defaultdict(list), collections.defaultdict(list)
    for identity, row in spans.items():
        rpc_id = row.get("rpc_request_id")
        if rpc_id and row["stage"] == "network.server":
            servers[rpc_id].append(identity)
        if rpc_id and row["stage"] == "network.service_rpc":
            clients[rpc_id].append(identity)
    for identity, row in censored_rows.items():
        if row.get("rpc_request_id") and row.get("stage") == "network.server":
            servers[row["rpc_request_id"]].append(identity)
    unmatched, ambiguous, joined = set(), set(), set()
    for rpc_id, callers in clients.items():
        targets = servers[rpc_id]
        if len(callers) != 1 or len(targets) > 1:
            ambiguous.update(callers)
        elif targets:
            children[callers[0]].add(targets[0]); joined.add(callers[0])
        else:
            unmatched.add(callers[0])
    roots = [row for row in spans.values() if row["stage"] == root_stage]
    if (root_node is None) != (root_span_id is None):
        raise ValueError("root node and span ID must be selected together")
    if root_node is not None:
        roots = [row for row in roots if row["node"] == root_node and row["span_id"] == root_span_id]
    seeds = []
    for row in roots:
        # Include request admission before the browser method, selecting the
        # nearest RPC server envelope rather than a whole outer test invocation.
        ancestors = [tuple(row["process"]) + (parent,) for parent in row["ancestors"]]
        seeds.append(next((parent for parent in reversed(ancestors) if spans.get(parent, {}).get("stage") == "network.server"), key(row)))
    selected, pending = set(), list(seeds)
    while pending:
        identity = pending.pop()
        if identity in selected:
            continue
        selected.add(identity); pending.extend(children[identity] - selected)
    groups = collections.defaultdict(list)
    for identity in selected & spans.keys():
        row = spans[identity]
        groups[(tuple(row["process"]), row["stage"])].append(row)
    stages = []
    for (process, stage), rows in sorted(groups.items(), key=lambda item: str(item[0])):
        values = [row["duration_ns"] / 1e6 for row in rows]
        direct, inclusive, outcomes, peers = collections.Counter(), collections.Counter(), collections.Counter(), set()
        for row in rows:
            direct.update(row["io_direct"]); inclusive.update(row["io_inclusive"])
            outcomes[row["outcome"]] += 1; peers.update(row["peers"])
        stages.append({"node": rows[0]["node"], "process": list(process), "stage": stage, "samples": len(rows),
                       "p50_ms": percentile(values, .5), "p95_ms": percentile(values, .95), "p99_ms": percentile(values, .99),
                       "max_ms": max(values), "io_direct": dict(direct), "io_inclusive": dict(inclusive),
                       "outcomes": dict(outcomes), "peers": sorted(peers)})
    # Whole-capture loss prevents claiming that every selected descendant was
    # observed. Unrelated background lifetimes crossing the window are separate.
    issues = [issue for issue in report["issues"] if not any(reason in issue for reason in
              ("unfinished span(s)", "span crosses capture boundary or start is missing", "I/O without a complete captured stage", "no complete stage samples"))]
    if not roots or expected_roots is not None and len(roots) != expected_roots:
        issues.append(f"root-stage coverage: expected {expected_roots}, observed {len(roots)} {root_stage}")
    if selected & censored:
        issues.append(f"selected operation contains {len(selected & censored)} incomplete spans")
    if selected & unmatched:
        issues.append(f"selected operation has {len(selected & unmatched)} unmatched outgoing RPCs")
    if selected & ambiguous:
        issues.append(f"selected operation has {len(selected & ambiguous)} ambiguous RPC joins")
    selected_spans = [spans[k] for k in sorted(selected, key=repr) if k in spans]
    return {**report, "complete": not issues, "issues": issues, "capture_issues": report["issues"],
            "capture_complete": report["complete"], "root_stage": root_stage, "root_count": len(roots),
            "selected_root_node": root_node, "selected_root_span_id": root_span_id,
            "expected_roots": expected_roots, "joined_rpcs": len(selected & joined),
            "censored_workload_spans": [censored_rows[k] for k in sorted(selected & censored, key=repr)],
            "stages": stages, "spans": selected_spans, "node_io": logical_node_io(selected_spans)}


def number(value):
    return "n/a" if value is None else f"{value:.3f}" if isinstance(value, float) else str(value)


def io_value(io, kind, field):
    # No observation cannot prove zero for an incompletely instrumented domain.
    if not io.get(kind + ".ops") or io.get(kind + ".unknown_bytes") and field == "bytes":
        return "n/o"
    return number(io.get(kind + "." + field, 0))


def markdown(report):
    manifest = report["manifest"]
    lines = [f"# Performance capture {report['capture_id']}", "",
             "| Run identity | Value |", "|---|---|",
             f"| Source revision | {manifest.get('source_revision', 'unavailable')} |",
             f"| Binary SHA-256 | {manifest.get('binary_sha256', 'unavailable')} |",
             f"| Build profile | {manifest.get('profile', 'unavailable')} |",
             f"| Workload exit | {manifest.get('workload_exit_code', 'unavailable')} |",
             f"| Report coverage | {'complete at instrumented boundaries' if report['complete'] else 'partial'} |",
             f"| Scope | {report.get('root_stage', 'whole capture')} |", "",
             "| Acceptance lane | Samples | p50 / p95 / p99 / max | Throughput | Target | Result |",
             "|---|---:|---|---|---|---|",
             "| Verification gate results | unavailable | unavailable | unavailable | unchanged configured targets | see the generated verification report |", "",
             "Debugging, tracing and profiling remain enabled for Docker acceptance. Use observed timings directly; no overhead is subtracted or used to relax gates. This stage capture alone does not establish all gate results. Stage latencies measure captured span lifetimes, including unknown outcomes (which may be cancellations). Stage rows are inclusive and overlap; do not sum them. Percentiles use nearest-rank samples on each node. n/o means not observed, n/a unavailable.", "",
             "| Node / process | Stage | N | p50 ms | p95 ms | p99 ms | Max ms | Outcomes ok/error/unknown | TX / RX B (ops) | Read / write B (ops) | Sync ops / ms | Peers |",
             "|---|---|---:|---:|---:|---:|---:|---|---|---|---|---|"]
    for row in report["stages"]:
        io = row["io_inclusive"]
        syncs = sum(io.get(k + ".ops", 0) for k in IO_KINDS if k.startswith("disk.sync"))
        sync_ms = sum(io.get(k + ".ns", 0) for k in IO_KINDS if k.startswith("disk.sync")) / 1e6
        lines.append(f"| {row['node']} / {row['process'][1]}:{row['process'][2]} | {row['stage']} | {row['samples']} | {row['p50_ms']:.3f} | {row['p95_ms']:.3f} | {row['p99_ms']:.3f} | {row['max_ms']:.3f} | {row['outcomes'].get('ok', 0)}/{row['outcomes'].get('error', 0)}/{row['outcomes'].get('unknown', 0)} | {io_value(io, 'net.tx', 'bytes')} / {io_value(io, 'net.rx', 'bytes')} ({io_value(io, 'net.tx', 'ops')} / {io_value(io, 'net.rx', 'ops')}) | {io_value(io, 'disk.read', 'bytes')} / {io_value(io, 'disk.write', 'bytes')} ({io_value(io, 'disk.read', 'ops')} / {io_value(io, 'disk.write', 'ops')}) | {str(syncs) + ' / ' + number(sync_ms) if syncs else 'n/o'} | {', '.join(row['peers']) or 'n/o'} |")
    lines += ["", "Logical I/O below counts each leaf observation once. Sync time is total API work, "
              "including any overlap; it is not an additional critical-path duration.", "",
              "| Node / PID | TX / RX B (ops) | File read / write B (ops) | Sync ops / total ms |",
              "|---|---|---|---|"]
    for row in logical_node_io(report["spans"]):
        io = row["io"]
        syncs = sum(io.get(k + ".ops", 0) for k in IO_KINDS if k.startswith("disk.sync"))
        sync_ms = sum(io.get(k + ".ns", 0) for k in IO_KINDS if k.startswith("disk.sync")) / 1e6
        lines.append(f"| {row['node']} / {row['process'][2]} | {io_value(io, 'net.tx', 'bytes')} / {io_value(io, 'net.rx', 'bytes')} ({io_value(io, 'net.tx', 'ops')} / {io_value(io, 'net.rx', 'ops')}) | {io_value(io, 'disk.read', 'bytes')} / {io_value(io, 'disk.write', 'bytes')} ({io_value(io, 'disk.read', 'ops')} / {io_value(io, 'disk.write', 'ops')}) | {str(syncs) + ' / ' + number(sync_ms) if syncs else 'n/o'} |")
    lines += ["", "Process windows include all work, observation overhead, cache effects and deferred writeback. They are not per-request physical disk costs. Namespace traffic includes non-loopback interfaces, other processes and relay forwarding; it excludes loopback and may count a forwarded packet on multiple interfaces.", "",
              "| Node / PID | Window ms | Kernel read B | Kernel write B | Read / write syscalls | Namespace TX / RX B |", "|---|---:|---:|---:|---|---|"]
    for row in report["processes"]:
        io = row["io_delta"]
        lines.append(f"| {row['node']} / {row['process'][2]} | {row['window_ms']:.3f} | {number(io['read_bytes'])} | {number(io['write_bytes'])} | {number(io['read_syscalls'])} / {number(io['write_syscalls'])} | {number(io['namespace_net_sent_bytes'])} / {number(io['namespace_net_received_bytes'])} |")
    if report.get("root_stage"):
        lines += ["", f"Selected {report['root_count']} `{report['root_stage']}` roots and joined {report['joined_rpcs']} RPC exchanges by exact request ID. The following coverage table covers the entire capture, including unrelated background work."]
    if report.get('manifest', {}).get('workload_kind') == 'idle':
        lines += ["", "RPC activity uses complete captured spans. Incoming and outgoing rows overlap across "
                  "nodes; counts are observed calls, not logical operations. Censored calls remain in the coverage "
                  "table and are not completed latency samples.", "",
                  "| Node / PID | RPC stage / route | Calls | p50 / p95 / max ms | Outcomes ok/error/unknown |",
                  "|---|---|---:|---|---|"]
        for row in rpc_activity(report['spans']):
            outcomes = row['outcomes']
            route = row['route'].replace('|', '\\|')
            lines.append(f"| {row['node']} / {row['process'][2]} | {row['stage']} / {route} | {row['samples']} | {row['p50_ms']:.3f} / {row['p95_ms']:.3f} / {row['max_ms']:.3f} | {outcomes.get('ok', 0)}/{outcomes.get('error', 0)}/{outcomes.get('unknown', 0)} |")
    if report.get("activity_counters"):
        lines += ["", "Activity counters and pending-work gauges survive trace-ring eviction. Their endpoint window includes "
                  "observation calls and can differ slightly from trace/process windows. Missing or reset values "
                  "remain unavailable; newly registered counters retain their raw endpoints.", "",
                  "Gauge endpoints describe pending state; their delta is not a work count.", "",
                  "| Node | Metric / kind | Labels | Before | After | Counter delta |", "|---|---|---|---:|---:|---:|"]
        for row in report["activity_counters"]:
            labels = json.dumps(row["labels"], sort_keys=True).replace("|", "\\|")
            lines.append(f"| {row['node']} | {row['name']} / {row['kind']} | {labels} | {number(row['before'])} | {number(row['after'])} | {number(row['delta'])} |")
    lines += ["", "| Node | Records | Started / closed / complete spans | Coverage |", "|---|---:|---|---|"]
    for row in report["coverage"]:
        lines.append(f"| {row['node']} | {row['records']} | {row['spans_started']} / {row['spans_closed']} / {row['complete_spans']} | {'; '.join(row['issues']) or 'captured boundaries complete'} |")
    lines += ["", "Logical network bytes exclude transport framing, retransmissions and relay forwarding. Storage counts cover instrumented APIs; metadata opens, mmap, direct I/O outside the hooks and device-level attribution are not measured. Cross-node clocks are not synchronized; join by trace identity, never subtract remote monotonic timestamps."]
    if report.get("censored_workload_spans"):
        lines += ["", "Selected stages still open or crossing the observation window; bounds are not completed latency samples:", "",
                  "| Node | Stage / phase | Elapsed lower bound ms | RPC route |", "|---|---|---:|---|"]
        for row in report["censored_workload_spans"]:
            lines.append(f"| {row['node']} | {row['stage']} / {row.get('phase') or '-'} | {number(row.get('elapsed_lower_bound_ms'))} | {row.get('rpc_route') or '-'} |")
    if report["issues"]:
        lines += ["", "Capture limitations:", ""] + [f"- {issue}" for issue in report["issues"]]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("capture", type=Path)
    parser.add_argument("--root-stage", help="select operations and their local/remote descendants by exact RPC request ID")
    parser.add_argument("--expected-roots", type=int, help="expected foreground operation count, including warmup")
    parser.add_argument("--root-node", help="node of one selected foreground span")
    parser.add_argument("--root-span-id", type=int, help="process-local ID of one selected foreground span")
    parser.add_argument("--output-stem", choices=("performance", "workload", "create", "overwrite"))
    args = parser.parse_args()
    report = summarize(args.capture)
    stem = "performance"
    if args.root_stage:
        report = operation_view(report, args.root_stage, args.expected_roots, args.root_node, args.root_span_id)
        stem = "workload"
    elif args.root_node is not None or args.root_span_id is not None:
        parser.error("root selection requires --root-stage")
    stem = args.output_stem or stem
    (args.capture / (stem + ".json")).write_text(json.dumps(report, indent=2) + "\n")
    (args.capture / (stem.upper() + ".md")).write_text(markdown(report))
    print(args.capture / (stem.upper() + ".md"))
    return 0 if report["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
