#!/usr/bin/env python3
"""Render Docker verification evidence; never runs workloads or changes gates.

Drivers call write_report automatically. Re-render an existing run with
`python docker/verification_report.py ARTIFACTS`. Raw results stay authoritative.
"""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import subprocess


def cell(value):
    if value is None:
        return "unavailable"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value).replace("|", "\\|").replace("\n", " ")


def table(headers, rows):
    return ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)] + [
        "| " + " | ".join(cell(value) for value in row) + " |" for row in rows]


def latency_summary(values):
    values = sorted(values)
    return {"count": len(values),
            **{f"p{p}_us": values[(len(values) * p + 99) // 100 - 1] if values else None
               for p in (50, 95, 99)},
            "max_us": values[-1] if values else None,
            "serial_api_ops_per_second": len(values) * 1e6 / sum(values) if sum(values) else None}


def write_rows(result):
    """One fixed reduction of raw samples, including failed and unrun groups."""
    params = result["parameters"]
    groups = {(g["replication_scenario"], g["origin"], g["shard"]): g
              for g in result.get("groups", [])}
    rows = []
    for scenario in params["policies"]:
        w, n = {"w1-n2": (1, 2), "w2-n3": (2, 3)}[scenario]
        for origin in params["origins"]:
            for shard in params["shards"]:
                group = groups.get((scenario, origin, shard), {})
                for operation in ("create", "overwrite"):
                    samples = [s for s in group.get("samples", []) if s.get("operation") == operation]
                    measured = [s for s in samples if s.get("measured") is True]
                    successful = [s for s in measured if s.get("succeeded") is True and s.get("error") is None]
                    values = sorted(s["elapsed_us"] for s in successful
                                    if type(s.get("elapsed_us")) is int and s["elapsed_us"] >= 0)
                    summary = latency_summary(values)
                    milliseconds = lambda key: summary[key] / 1000 if summary[key] is not None else None
                    policy_verified = bool(samples) and all(
                        s.get("placement_verified") is True
                        and s.get("placement_policy") == {
                            "consistency": "replicated-eventual", "min_commit_replicas": w,
                            "desired_replicas": n}
                        and s.get("placement_source_path") == group.get("base")
                        and group.get("base") for s in samples)
                    rows.append({
                        "replication_scenario": scenario, "w": w, "n": n,
                        "origin": origin, "shard": shard, "operation": operation,
                        "payload_bytes": params["payload_bytes"], "concurrency": 1,
                        "warmup_observed": sum(s.get("measured") is False for s in samples),
                        "warmup_expected": params["warmup"], "measured_expected": params["iterations"],
                        "measured_observed": len(measured), "successful": len(successful),
                        "failed": sum(s.get("succeeded") is False for s in measured),
                        "command_timed_out": group.get("command_timed_out"),
                        "latency_samples": len(values), "policy_verified": policy_verified,
                        "p50_ms": milliseconds("p50_us"), "p95_ms": milliseconds("p95_us"),
                        "p99_ms": milliseconds("p99_us"), "max_ms": milliseconds("max_us"),
                        "serial_api_ops_per_second": summary["serial_api_ops_per_second"],
                        "target_p95_ms_exclusive": params["max_p95_us"] / 1000 or None,
                        "status": "passed" if group.get("passed") is True else "failed" if group else "not-run",
                        "error": group.get("error"),
                    })
    return rows


def unavailable_tables():
    nodes = [f"node{i}" for i in range(1, 7)]
    lines = ["", "Per-node stages and I/O are unavailable in this verification summary. "
             "Use the separately generated traced diagnostic reports; keep their timings separate.", ""]
    lines += table(["Node / process", "Stage", "Samples", "p50 / p95 / p99 / max ms",
                    "Outcomes ok/error/unknown", "Logical TX / RX B (ops)",
                    "File read / write B (ops)", "Sync count / time", "Peers"],
                   [(node, None, None, None, None, None, None, None, None) for node in nodes])
    lines += [""] + table(["Node / PID", "Window ms", "Kernel read / write B",
                             "Read / write syscalls", "Namespace TX / RX B"],
                            [(node, None, None, None, None) for node in nodes])
    return lines


def quietness_results(root):
    gates = []
    for path in sorted(root.glob("network-quiet-*.summary.txt")):
        summary = dict(line.split("=", 1) for line in path.read_text().splitlines() if "=" in line)
        samples = {f"node{i}": [] for i in range(1, int(summary.get("container_count", "0")) + 1)}
        sample_name = summary.get("samples_file", "")
        if sample_name and Path(sample_name).name == sample_name and (root / sample_name).is_file():
            for row in csv.DictReader((root / sample_name).read_text().splitlines(), delimiter="\t"):
                if row.get("window_state") == "stream-warmup":
                    continue
                try:
                    cpu = float(row["cpu_percent"].rstrip("%"))
                except (ValueError, KeyError):
                    continue
                if math.isfinite(cpu) and cpu >= 0:
                    samples.setdefault(row["node"], []).append(cpu)
        nodes = []
        for node, values in sorted(samples.items()):
            ordered = sorted(values)
            limit = float(summary["max_cpu_percent"])
            nodes.append({"node": node, "samples": len(values),
                          "mean_cpu_percent": sum(values) / len(values) if values else None,
                          "p95_cpu_percent": ordered[math.ceil(len(values) * .95) - 1] if values else None,
                          "max_cpu_percent": max(values) if values else None,
                          "over_limit_samples": sum(v > limit for v in values) if values else None})
        gates.append({"artifact": path.name, "summary": summary, "nodes": nodes})
    return gates


def write_report(root):
    root = Path(root)
    source = next((root / name for name in ("write-latency.json", "verification.json", "result.json")
                   if (root / name).is_file()), None)
    if source is None:
        raise ValueError(f"no supported verification result in {root}")
    raw = source.read_bytes()
    result = json.loads(raw)
    is_write = result.get("schema") == "sinomo.dfs-write-latency.v2"
    report = {"schema": "sinomo.docker-verification-report.v1", "source_artifact": source.name,
              "source_sha256": hashlib.sha256(raw).hexdigest(),
              "source_artifacts": {source.name: hashlib.sha256(raw).hexdigest()},
              "passed": result.get("passed") is True,
              "acceptance_scope": result.get("acceptance_scope") if is_write else None,
              "acceptance_rows": write_rows(result) if is_write else [],
              "stage_measurements": None, "node_window_measurements": None,
              "reporter_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    lines = ["# Docker verification report", "",
             f"Result: **{'PASS' if report['passed'] else 'FAIL / incomplete'}**. "
             f"Source: [{source.name}]({source.name}).", ""]
    if is_write:
        lines += ["Scope: " + ("standard acceptance matrix" if report["acceptance_scope"]
                              else "diagnostic; does not establish standard acceptance") + ".", ""]
    lines += table(["Run identity", "Value"], [
        ("Project", result.get("project")), ("Source revision", result.get("source_head")),
        ("Binary SHA-256", result.get("binary_sha256", result.get("sha256"))),
        ("Profile", result.get("profile", result.get("parameters", {}).get("profile"))),
        ("Timing scope", result.get("timing_scope")),
        ("Raw result SHA-256", report["source_sha256"]),
    ])
    if is_write:
        lines += ["", "Percentiles use nearest rank on successful measured operations, separated by policy, "
                  "origin, shard and operation. Warmups are excluded. Partial distributions remain visible; "
                  "a failed or incomplete group cannot pass. Throughput is reciprocal serial API time, "
                  "excluding setup and correctness checks; it is not sustained cluster throughput.", ""]
        lines += table(["W/N", "Origin / shard", "Operation", "Bytes / concurrency",
                        "Warmup observed/expected", "Measured observed/expected", "Success / failed",
                        "Command timeout", "Policy verified", "p50 ms", "p95 ms", "p99 ms", "Max ms",
                        "Serial API ops/s", "p95 target < ms", "Result"], [
            (f"{r['w']}/{r['n']}", f"{r['origin']} / {r['shard']}", r["operation"],
             f"{r['payload_bytes']} / {r['concurrency']}", f"{r['warmup_observed']}/{r['warmup_expected']}",
             f"{r['measured_observed']}/{r['measured_expected']}", f"{r['successful']} / {r['failed']}",
             r["command_timed_out"], r["policy_verified"], r["p50_ms"], r["p95_ms"], r["p99_ms"], r["max_ms"],
             r["serial_api_ops_per_second"], r["target_p95_ms_exclusive"], r["status"])
            for r in report["acceptance_rows"]])
    elif (root / "benchmark.json").exists():
        benchmark = json.loads((root / "benchmark.json").read_text())
        report["source_artifacts"]["benchmark.json"] = hashlib.sha256((root / "benchmark.json").read_bytes()).hexdigest()
        report["cli_benchmark_rows"] = benchmark.get("summary", [])
        lines += ["", "[CLI benchmark source](benchmark.json): host wall time includes Docker exec, CLI "
                  "startup, RPC and any read convergence polling. W/N was not recorded by this lane; "
                  "these rows do not substitute for the write-policy matrix.", ""]
        lines += table(["Profile", "Origin", "Operation", "W/N", "N", "p50 ms", "p95 ms", "Max ms", "Ops/s"], [
            (r["profile"], r["origin"], r["operation"], None, r["count"], r["p50_ms"], r["p95_ms"], r["max_ms"],
             r["operations_per_second"]) for r in report["cli_benchmark_rows"]])
    else:
        lines += [""] + table(["Acceptance lane", "Result", "Latency / throughput"],
                              [(result.get("lane", result.get("case", "selected verification")),
                                "passed" if report["passed"] else "failed / incomplete", None)])
    # Preserve native probe summaries and metrics verbatim. A whole probe's
    # duration includes setup and correctness work; it is not write latency.
    probes = []
    for path in sorted(root.glob("probe-*.json")):
        content = path.read_bytes()
        report["source_artifacts"][path.name] = hashlib.sha256(content).hexdigest()
        try:
            probe = json.loads(content)
            probes.append({"artifact": path.name, "case": probe["case"]["id"],
                           "status": probe.get("status"), "duration_ms": probe.get("duration_ms"),
                           "summary": probe.get("summary"), "metrics": probe.get("metrics")})
        except (ValueError, KeyError, TypeError):
            probes.append({"artifact": path.name, "case": None, "status": "invalid / incomplete",
                           "duration_ms": None, "summary": "see raw artifact", "metrics": None})
    report["native_probe_results"] = probes
    if probes:
        lines += ["", "Native probe results (elapsed time includes the entire probe, including setup "
                  "and verification; metrics below retain their original names and units):", ""]
        lines += table(["Raw artifact", "Case", "Result", "Probe elapsed ms", "Summary", "Metrics"], [
            (f"[{p['artifact']}]({p['artifact']})", p["case"], p["status"], p["duration_ms"], p["summary"],
             json.dumps(p["metrics"], sort_keys=True) if p["metrics"] is not None else None) for p in probes])
    report["quietness"] = quietness_results(root)
    for gate in report["quietness"]:
        for name in [gate["artifact"], gate["summary"].get("samples_file"), gate["summary"].get("generations_file")]:
            if name and Path(name).name == name and (root / name).is_file():
                report["source_artifacts"][name] = hashlib.sha256((root / name).read_bytes()).hexdigest()
    if report["quietness"]:
        lines += ["", "Quietness uses whole-container CPU (% of one CPU), including shadows, helpers and observation. "
                  "Rows summarize the continuous Docker stream, excluding its warmup row. Mean/p95/max describe "
                  "sampled intervals; the gate still requires an uninterrupted 30-second window on every node.", ""]
        lines += table(["Quiet gate", "Result", "Required seconds / CPU %", "Search elapsed seconds", "Reason"], [
            (f"[{g['summary']['label']}]({g['artifact']})", g["summary"]["status"],
             f"{g['summary']['required_quiet_duration_seconds']} / {g['summary']['max_cpu_percent']}",
             g["summary"]["elapsed_seconds"], g["summary"]["reason"]) for g in report["quietness"]])
        lines += [""] + table(["Quiet gate / node", "Intervals", "Mean CPU %", "p95 CPU %", "Max CPU %", "Over limit intervals"], [
            (f"{g['summary']['label']} / {n['node']}", n["samples"], n["mean_cpu_percent"],
             n["p95_cpu_percent"], n["max_cpu_percent"], n["over_limit_samples"])
            for g in report["quietness"] for n in g["nodes"]])
    lines += unavailable_tables()
    expected = len(report["acceptance_rows"])
    observed = sum(r["status"] != "not-run" for r in report["acceptance_rows"])
    report["coverage"] = {"expected_distributions": expected if is_write else None,
                          "observed_distributions": observed if is_write else None,
                          "trace_coverage": "not collected in this run",
                          "error": result.get("error"), "errors": result.get("errors", []),
                          "failed_scenarios": result.get("failed_scenarios", [])}
    lines += [""] + table(["Coverage", "Evidence"], [
        ("Expected / observed distributions", f"{expected} / {observed}" if is_write else None),
        ("Expected cluster nodes", 6),
        ("Initial / final process identity checks", f"{result.get('initial_identity_verified')} / {result.get('final_identity_verified')}" if is_write else None),
        ("Trace starts/closes, truncation, censored spans", "unavailable; not collected in this run"),
        ("Per-node I/O counters", "unavailable; not collected in this run"),
        ("Clock assumptions", "per-operation local monotonic time; no cross-node clock subtraction"),
        ("Error", result.get("error")), ("Failed scenarios", result.get("failed_scenarios")),
    ])
    if result.get("errors"):
        lines += ["", "Failures:", ""] + [f"- {cell(error)}" for error in result["errors"]]
    children = sorted(p.relative_to(root).as_posix() for name in ("REPORT.md", "PERFORMANCE.md", "WORKLOAD.md")
                      for p in root.rglob(name) if p.parent != root)
    report["child_reports"] = children
    if children:
        lines += ["", "Generated lane and diagnostic reports (keep their timing scopes separate):", ""]
        lines += [f"- [{path}]({path})" for path in children]
    lines += ["", "Reuse this report and its JSON directly. Re-render from saved evidence with "
              "`python docker/verification_report.py ARTIFACTS`; this performs no cluster operations."]
    for name, content in (("report.json", json.dumps(report, indent=2) + "\n"),
                          ("REPORT.md", "\n".join(lines) + "\n")):
        temp = root / (name + ".tmp")
        temp.write_text(content)
        temp.replace(root / name)
    (root / "verification_report.py").write_bytes(Path(__file__).read_bytes())
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifacts", type=Path)
    parser.add_argument("--project")
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--profile")
    parser.add_argument("--lane", default="selected verification")
    parser.add_argument("--exit-code", type=int)
    parser.add_argument("--failed-scenarios", default="")
    args = parser.parse_args()
    if args.exit_code is not None:
        result = {"schema": "sinomo.docker-verification.v1", "project": args.project,
                  "lane": args.lane, "exit_code": args.exit_code, "profile": args.profile,
                  "passed": args.exit_code == 0 and not args.failed_scenarios.strip(),
                  "failed_scenarios": args.failed_scenarios.split(),
                  "source_head": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                  "binary_sha256": None}
        if args.binary and args.binary.is_file():
            with args.binary.open("rb") as binary:
                result["binary_sha256"] = hashlib.file_digest(binary, "sha256").hexdigest()
        result["binary_source_note"] = "Binary hash identifies the tested executable; checkout revision and patch identify the verifier source, not a build attestation."
        (args.artifacts / "source.patch").write_bytes(subprocess.check_output(["git", "diff", "HEAD", "--binary"]))
        (args.artifacts / "verification.json").write_text(json.dumps(result, indent=2) + "\n")
    write_report(args.artifacts)
    print(f"Report: {args.artifacts / 'REPORT.md'}")


if __name__ == "__main__":
    main()
