#!/usr/bin/env python3
"""Trace prepared Docker write fixtures using the standard Observe tools.

Requires retained write-latency.json fixtures. Captures only correctness-verified
cases and records other selected cases as skipped with their original failures.
Does not start, compile, restart, reset or stop containers. Each capture uses one pair of operations; policy setup stays outside it. Generated reports retain partial
coverage and separate create/overwrite and both W/N policies.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import shutil


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ROOT = Path(__file__).resolve().parents[1]
latency = load(ROOT / "docker/measure-dfs-write-latency.py", "write_latency")
reporter = load(ROOT / "scripts/debug/report-performance.py", "performance_report")
presentation = load(ROOT / "scripts/debug/render-pprof-flamegraph.py", "pprof_presentation")


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def container_clients(project, nodes):
    """Use the verified container mount, independently of the host binary path."""
    return {node: ["docker", "exec", f"{project}-{alias}-1", "/usr/local/bin/sinomo"]
            for alias, node in nodes.items()}


def record_capture_manifest(capture, case):
    path = capture / "manifest.json"
    if path.exists():
        manifest = json.loads(path.read_text())
        case["capture_errors"] = manifest.get("errors", [])
        case["workload_started"] = "workload_started_unix_ns" in manifest
        case["workload_exit_code"] = manifest.get("workload_exit_code")


def record_capture_failure(case, error):
    detail = f"{type(error).__name__}: {error}"
    if case.get("capture_errors"):
        # A missing workload/output is a consequence of capture setup failure.
        # Preserve the original control/export errors as the visible cause.
        case["analysis_error"] = detail
        case["error"] = "capture failed: " + "; ".join(case["capture_errors"])
    else:
        case["error"] = detail


def select_roots(report, node):
    roots = [r for r in report["spans"] if r["stage"] == "dfs.browser.write"]
    if len(roots) != 2 or any(r["node"] != node for r in roots) or len({tuple(r["process"]) for r in roots}) != 1:
        raise ValueError("expected exactly two sequential write roots on the requested caller incarnation")
    roots.sort(key=lambda row: row["start_ns"])
    if roots[0]["start_ns"] == roots[1]["start_ns"]:
        raise ValueError("write root start order is ambiguous")
    return dict(zip(("create", "overwrite"), roots))


def render_index(root, result):
    lines = ["# DFS write stage diagnostics", "",
             "One-pair stage captures (N=1 per operation); these do not establish a full p95 gate. Debugging, tracing and profiling are normal acceptance settings, and no overhead is subtracted. "
             "Each case uses one sequential 4 KiB create/overwrite pair on a preconfigured fixture. "
             "The ordinary write probe and its timer are unchanged. Policy checks and correctness "
             "reads remain outside that timer. Capture node windows include those checks and "
             "background work; stage views select the write request by span ancestry and exact RPC IDs.", "",
             f"Binary SHA-256: `{result['binary_sha256']}`. Source: `{result['source_revision']}`. "
             f"Profile: `{result['profile']}`. Project: `{result['project']}`.", "",
             "| Node alias | Node ID |", "|---|---|"]
    lines += [f"| {alias} | {node} |" for alias, node in result.get("nodes", {}).items()]
    if result.get("gate_report"):
        lines += ["", f"Readiness/acceptance status: **{result.get('gate_status', 'unavailable')}**. "
                  f"[Generated gate report]({result['gate_report']})."]
    if result.get("storage_environment"):
        lines += ["", "Storage: " + result["storage_environment"]]
    if result.get("fixture_coverage"):
        coverage = result["fixture_coverage"]
        lines += ["", f"Verified fixtures: **{coverage['verified']}/{coverage['selected']}**. "
                  f"Skipped fixtures: **{coverage['skipped']}**. Unavailable cases retain their "
                  "original probe failures below; they are not replaced or counted as measured coverage."]
    lines += ["", "| Policy | Caller / shard | Operation | API write ms | Browser span ms | Selected coverage | Report |",
              "|---|---|---|---:|---:|---|---|"]
    for row in result["operations"]:
        lines.append(f"| {row['policy']} | {row['origin']} / {row['shard']} | {row['operation']} | {row['api_write_ms']:.3f} | {row['browser_span_ms']:.3f} | {'complete' if row['complete'] else 'partial'} | [{row['report']}]({row['report']}) |")
    names = {node: alias for alias, node in result.get("nodes", {}).items()}
    lines += ["", "Stage overview copied from the generated operation reports. Durations below are "
              "maximum captured span lifetimes within each individual operation; nested rows overlap.", "",
              "| Policy / caller / shard / operation | Caller queue ms | Metadata primary | Commit ms | Authority/probe ms | Primary sync ops / total ms |",
              "|---|---:|---|---:|---:|---|"]
    for row in result["operations"]:
        view = json.loads((root / row["json"]).read_text())
        commits = [s for s in view["stages"] if s["stage"] == "dfs.write.commit"]
        primary = max(commits, key=lambda s: s["max_ms"]) if commits else None
        queue = next((s["max_ms"] for s in view["stages"] if s["stage"] == "service.blocking.queue/dfs.browser"), None)
        authority = next((s["max_ms"] for s in view["stages"] if primary and s["node"] == primary["node"] and s["stage"] == "dfs.metadata.authority"), None)
        io = next((n["io"] for n in reporter.logical_node_io(view["spans"]) if primary and n["node"] == primary["node"]), {})
        sync_count = sum(io.get(kind + ".ops", 0) for kind in reporter.IO_KINDS if kind.startswith("disk.sync"))
        sync_ms = sum(io.get(kind + ".ns", 0) for kind in reporter.IO_KINDS if kind.startswith("disk.sync")) / 1e6
        label = f"{row['policy']} / {row['origin']} / {row['shard']} / {row['operation']}"
        lines.append(f"| {label} | {reporter.number(queue)} | {names.get(primary['node'], primary['node']) if primary else 'unavailable'} | {reporter.number(primary['max_ms'] if primary else None)} | {reporter.number(authority)} | {str(sync_count) + ' / ' + reporter.number(sync_ms) if sync_count else 'unavailable'} |")
    lines += ["", "The generated operation reports contain the standard acceptance, per-node stage/I/O, "
              "node-window and coverage tables. Inclusive stages overlap: do not add their durations "
              "or I/O. Each create/overwrite report shares its case's process windows; do not count "
              "those windows twice. Unfinished remote work is censored, not a completed latency sample.", "",
              "| Case | Capture exit | Workload verified | Selected coverage |", "|---|---:|---|---|"]
    lines += [f"| {c['name']} | {c.get('capture_exit')} | {c.get('workload_verified', False)} | "
              f"{('skipped: ' if c.get('skipped') else '') + c['error'] if c.get('error') else ('complete' if c.get('complete') else 'partial')} |"
              for c in result["cases"]]
    lines += ["", "CPU profiles are collected at 49 Hz while the workload runs, with a 30-second maximum. "
              "Sample counts are not wall time or per-request CPU time. Allocator, loader and other "
              "excluded system-library instruction ranges are recorded in each CPU manifest.", "",
              "| Case | Node | CPU samples | Artifact bytes | Profile |", "|---|---|---:|---:|---|"]
    for row in result.get("cpu_profiles", []):
        links = f"[pprof]({row['artifact']})"
        if row.get("flamegraph"):
            links += f" / [flamegraph]({row['flamegraph']}) / [top]({row['top']})"
        if row.get("presentation_status") == "deferred":
            links += f" / [presentation deferred]({row['presentation_receipt']})"
        lines.append(f"| {row['case']} | {row['node']} | {row.get('sample_count')} | {row.get('artifact_bytes')} | {links} |")
    if result.get("error"):
        lines += ["", "Run error: " + result["error"]]
    if result.get("workload_note"):
        lines += ["", "Workload scope: " + result["workload_note"]]
    (root / "REPORT.md").write_text("\n".join(lines) + "\n")
    save(root / "report.json", result)


def collect_cpu_profiles(run, capture, name, nodes):
    rows = []
    args = run.args
    for alias, node in nodes.items():
        profile_path = capture / node / "cpu-final.json"
        if not profile_path.exists():
            continue
        cpu = json.loads(profile_path.read_text())
        relative = cpu.get("artifact_relative_path")
        if relative and relative.startswith("observe/profiles/") and ".." not in Path(relative).parts:
            run.run(f"{name}-{alias}-cpu-artifact", ["docker", "cp",
                f"{args.project}-{alias}-1:/data/{relative}", str(capture / node / "cpu.pprof.pb")])
            manifest_relative = cpu.get("manifest_relative_path")
            if manifest_relative and manifest_relative.startswith("observe/profiles/") and ".." not in Path(manifest_relative).parts:
                run.run(f"{name}-{alias}-cpu-manifest", ["docker", "cp",
                    f"{args.project}-{alias}-1:/data/{manifest_relative}", str(capture / node / "cpu-manifest.json")])
            cpu_row = {"case": name, "node": alias,
                "sample_count": cpu.get("sample_count"), "artifact_bytes": cpu.get("artifact_bytes"),
                "artifact": f"{name}/{node}/cpu.pprof.pb"}
            if run.env.get("SINOMO_TEST_DEFER_PPROF_PRESENTATION") == "1":
                deferred = presentation.record_deferred_presentation(
                    capture / node / "cpu.pprof.pb", capture / node / "cpu-manifest.json",
                    capture / node / "pprof-presentation.json",
                    [profile_path, Path(__file__), Path(presentation.__file__)], include_top=True)
                cpu_row.update(presentation_status=deferred["status"],
                    presentation_receipt=f"{name}/{node}/pprof-presentation.json",
                    artifact_sha256=deferred["inputs"]["profile"]["sha256"],
                    manifest_sha256=deferred["inputs"]["manifest"]["sha256"])
            elif shutil.which("go"):
                with (capture / node / "cpu-top.txt").open("w") as top:
                    subprocess.run(["go", "tool", "pprof", "-top", "-nodecount=20",
                        str(capture / node / "cpu.pprof.pb")], stdout=top, stderr=subprocess.STDOUT, timeout=30)
                with (capture / node / "flamegraph.log").open("w") as log:
                    rendered = subprocess.run(["python3", str(ROOT / "scripts/debug/render-pprof-flamegraph.py"),
                        str(capture / node / "cpu.pprof.pb"), "-o", str(capture / node / "flamegraph.html")],
                        stdout=log, stderr=subprocess.STDOUT, timeout=30)
                if rendered.returncode == 0:
                    cpu_row.update(flamegraph=f"{name}/{node}/flamegraph.html", top=f"{name}/{node}/cpu-top.txt")
            rows.append(cpu_row)
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--project", required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--policies", nargs="+", choices=list(latency.POLICIES), default=list(latency.POLICIES))
    parser.add_argument("--origins", nargs="+", choices=list(latency.PEERS), default=list(latency.PEERS))
    parser.add_argument("--shards", nargs="+", choices=list(latency.BASES), default=list(latency.BASES))
    args = parser.parse_args()
    fixture_raw = args.fixtures.read_bytes()
    fixtures = json.loads(fixture_raw)
    selected = list(latency.product(args.policies, args.origins, args.shards))
    try:
        groups, skipped = latency.partition_verified_fixtures(fixtures, args.project, selected)
    except (ValueError, KeyError, TypeError) as error:
        parser.error(str(error))
    args.artifacts = args.artifacts.resolve()
    args.artifacts.mkdir(parents=True, exist_ok=False)
    run = latency.acceptance.Acceptance(args)
    result = {"schema": "sinomo.dfs-write-stage-diagnostics.v1", "classification": "diagnostic",
              "project": args.project, "binary_sha256": run.binary_hash, "profile": args.profile,
              "source_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip() + "+dirty",
              "fixtures_sha256": hashlib.sha256(fixture_raw).hexdigest(),
              "fixture_binary_sha256": fixtures["binary_sha256"], "cases": [], "operations": [], "complete": False,
              "fixture_coverage": {"selected": len(selected), "verified": len(groups), "skipped": len(skipped)}}
    (run.artifacts / "fixtures.json").write_bytes(fixture_raw)
    (run.artifacts / "source.patch").write_bytes(subprocess.check_output(["git", "diff", "HEAD", "--binary"]))
    for source in (Path(__file__), ROOT / "docker/measure-dfs-write-latency.py", ROOT / "scripts/debug/capture-performance.py",
                   ROOT / "scripts/debug/report-performance.py"):
        (run.artifacts / source.name).write_bytes(source.read_bytes())
    try:
        run.verify_processes("initial")
        run.verify_observe()
        nodes = {}
        for alias in (f"node{i}" for i in range(1, 7)):
            nodes[alias] = json.loads(run.probe(alias, "shard-layout"))["diagnostics"]["node"]
        result["nodes"] = nodes
        clients = container_clients(args.project, nodes)
        save(run.artifacts / "clients.json", clients)
        for policy, origin, shard in selected:
            name = f"{policy}-{origin}-{shard}"
            case = {"name": name, "policy": policy, "origin": origin, "shard": shard}
            result["cases"].append(case)
            key = (policy, origin, shard)
            if key in skipped:
                case.update(skipped[key], skipped=True, workload_verified=False, complete=False)
                print(name, "skipped:", case["error"], flush=True)
                render_index(run.artifacts, result)
                continue
            case["fixture_base"] = groups[key]["base"]
            capture = run.artifacts / name
            workload = clients[nodes[origin]] + ["test", "run", "cluster.dfs.write-latency", "--require-cluster", "--json"]
            params = {"prepared-base": groups[(policy, origin, shard)]["base"], "replication": policy,
                      "iterations": 1, "warmup-iterations": 0, "payload-bytes": 4096,
                      "max-p95-us": 0, "retain-fixture": "true"}
            for key, value in params.items():
                workload += ["--param", f"{key}={value}"]
            command = ["python3", str(ROOT / "scripts/debug/capture-performance.py"), "--buffered",
                       "--cpu-profile", "--minimum-capacity", "32768", "--nodes", *nodes.values(),
                       "--clients-file", str(run.artifacts / "clients.json"), "--output", str(capture),
                       "--source-revision", result["source_revision"], "--binary-sha256", run.binary_hash,
                       "--profile", args.profile, "--duration-seconds", "90", "--rpc-timeout", "30", "--", *workload]
            save(run.artifacts / (name + ".command.json"), command)
            try:
                with (run.artifacts / (name + ".log")).open("w") as log:
                    case["capture_exit"] = subprocess.run(command, env=run.env, stdout=log, stderr=subprocess.STDOUT).returncode
                record_capture_manifest(capture, case)
                whole = reporter.summarize(capture)
                save(capture / "performance.json", whole)
                (capture / "PERFORMANCE.md").write_text(reporter.markdown(whole))
                result.setdefault("cpu_profiles", []).extend(collect_cpu_profiles(run, capture, name, nodes))
                probe = json.loads((capture / "workload.stdout").read_text())
                latency.validate_report(probe, 1, 0, 0, policy)
                previous_seeds = {s["payload_seed"] for s in groups[(policy, origin, shard)]["samples"]}
                if any(s["payload_seed"] in previous_seeds for s in probe["diagnostics"]["samples"]):
                    raise ValueError("profile reused fixture payloads; a prepared-fixture probe with unique per-path payloads is required")
                case["workload_verified"] = True
                roots = select_roots(whole, nodes[origin])
                for sample in probe["diagnostics"]["samples"]:
                    operation = sample["operation"]
                    span = roots[operation]
                    view = reporter.operation_view(whole, "dfs.browser.write", 1, nodes[origin], span["span_id"])
                    view["write_sample"] = sample
                    view["write_policy"] = policy
                    save(capture / (operation + ".json"), view)
                    (capture / (operation.upper() + ".md")).write_text(reporter.markdown(view))
                    result["operations"].append({"policy": policy, "origin": origin, "shard": shard,
                        "operation": operation, "api_write_ms": sample["elapsed_us"] / 1000,
                        "browser_span_ms": span["duration_ns"] / 1e6, "complete": view["complete"],
                        "report": f"{name}/{operation.upper()}.md", "json": f"{name}/{operation}.json"})
                case["complete"] = case["capture_exit"] == 0 and all(row["complete"] for row in result["operations"][-2:])
                # Independent remote validation follows filter restoration.
                sample = probe["diagnostics"]["samples"][-1]
                run.probe(latency.PEERS[origin], "expect-file", path=sample["path"], payload_bytes=4096,
                          payload_seed=sample["payload_seed"], timeout_seconds=60)
                print(name, "complete" if case["complete"] else "partial", flush=True)
            except Exception as error:
                record_capture_failure(case, error)
                print(name, case["error"], flush=True)
            finally:
                render_index(run.artifacts, result)
        run.verify_processes("final")
        result["complete"] = len(result["cases"]) == len(selected) and all(c.get("complete") and not c.get("error") for c in result["cases"])
    except BaseException as error:
        result["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        render_index(run.artifacts, result)
    return 0 if result["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
