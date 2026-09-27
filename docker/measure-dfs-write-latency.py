#!/usr/bin/env python3
"""Gate ordinary committed DFS write latency on an existing six-node dev cluster.

Never starts, restarts or removes containers. The default checks readiness and
the standard 30-second container quiet gate, measures 100 creates and overwrites
per origin/shard, then validates every final
file from the opposite segment. Fixtures and the project are retained.
"""

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess


spec = importlib.util.spec_from_file_location(
    "dfs_acceptance", Path(__file__).with_name("test-file-history.py"))
acceptance = importlib.util.module_from_spec(spec)
spec.loader.exec_module(acceptance)

BASES = {"root": "/cluster-tests", "services": "/services/cluster-tests",
         "users": "/users/cluster-tests"}
PEERS = {"node1": "node5", "node5": "node1"}


def validate_report(report, iterations, warmup, max_p95_us):
    """Reject missing, censored, pooled, or incorrectly classified evidence."""
    diagnostics = report.get("diagnostics", {})
    samples = diagnostics.get("samples", [])
    if report.get("status") != "passed":
        raise RuntimeError(f"write probe did not pass: {report.get('summary', report.get('status'))}")
    if diagnostics.get("wait_for_commit") is not True:
        raise RuntimeError("write probe did not prove committed return")
    if diagnostics.get("correctness_passed") is not True:
        raise RuntimeError("write probe did not prove read-your-write correctness")
    if len(samples) != (iterations + warmup) * 2:
        raise RuntimeError("write probe omitted operations")
    summaries = []
    for operation in ("create", "overwrite"):
        group = [sample for sample in samples if sample.get("operation") == operation]
        if [sample.get("index") for sample in group] != list(range(iterations + warmup)):
            raise RuntimeError(f"{operation} sample indices are incomplete or duplicated")
        for sample in group:
            if sample.get("succeeded") is not True or sample.get("error") is not None:
                raise RuntimeError(f"{operation} includes a failed operation")
            if sample.get("measured") is not (sample["index"] >= warmup):
                raise RuntimeError(f"{operation} warmup classification is inconsistent")
            if not isinstance(sample.get("elapsed_us"), int) or sample["elapsed_us"] < 0:
                raise RuntimeError(f"{operation} has an invalid duration")
        values = sorted(sample["elapsed_us"] for sample in group if sample["measured"])
        percentile = lambda percent: values[(len(values) * percent + 99) // 100 - 1]
        summary = {"operation": operation, "count": len(values),
                   "p50_us": percentile(50), "p95_us": percentile(95), "max_us": max(values)}
        if max_p95_us and summary["p95_us"] >= max_p95_us:
            raise RuntimeError(f"{operation} p95 {summary['p95_us']}us is not below {max_p95_us}us")
        summaries.append(summary)
    return summaries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--warmup", type=int, default=12)
    parser.add_argument("--payload-bytes", type=int, default=4096)
    parser.add_argument("--max-p95-us", type=int, default=100_000,
                        help="exclusive limit for each origin/shard/operation; 0 is diagnostic only")
    parser.add_argument("--origins", nargs="+", choices=list(PEERS), default=list(PEERS))
    parser.add_argument("--shards", nargs="+", choices=list(BASES), default=list(BASES))
    parser.add_argument("--command-timeout", type=int, default=600)
    parser.add_argument("--skip-readiness", action="store_true",
                        help="diagnostic reuse only; omits standard readiness/mutation/convergence/quiet gates")
    args = parser.parse_args()
    if not __debug__:
        parser.error("omit -O/PYTHONOPTIMIZE: imported acceptance checks use assertions")
    if not 1 <= args.iterations <= 10_000 or not 0 <= args.warmup <= 1000:
        parser.error("iterations must be 1..10000 and warmup 0..1000")
    if not 1 <= args.payload_bytes <= 1024 * 1024 or args.max_p95_us < 0:
        parser.error("payload-bytes must be 1..1048576 and max-p95-us nonnegative")
    if args.command_timeout <= 0:
        parser.error("command-timeout must be positive")
    run = acceptance.Acceptance(args)
    run.env["SINOMO_TEST_DFS_READY_REQUIRE_QUIET"] = "1"
    result = {"schema": "sinomo.dfs-write-latency.v1", "passed": False,
              "project": args.project, "binary": str(run.binary), "binary_sha256": run.binary_hash,
              "parameters": {key: value for key, value in vars(args).items()
                             if key not in ("binary", "artifacts")},
              "source_head": subprocess.check_output(["git", "rev-parse", "HEAD"],
                                                       cwd=run.root, text=True).strip(),
              "timing_scope": "public daemon DFS write_file API through committed response; client RPC included",
              "fixtures_retained": True, "groups": [], "errors": []}
    result["acceptance_scope"] = (args.iterations >= 100 and args.payload_bytes == 4096
                                  and args.max_p95_us == 100_000
                                  and set(args.origins) == set(PEERS)
                                  and set(args.shards) == set(BASES) and not args.skip_readiness)
    for source in (Path(__file__), Path(__file__).with_name("test-file-history.py")):
        content = source.read_bytes()
        (run.artifacts / source.name).write_bytes(content)
        result[source.name + "_sha256"] = hashlib.sha256(content).hexdigest()
    (run.artifacts / "source.patch").write_bytes(
        subprocess.check_output(["git", "diff", "HEAD", "--binary"], cwd=run.root))
    try:
        run.run("docker-context", ["docker", "context", "show"])
        run.run("compose-config", [*run.compose, "config"])
        run.verify_processes("initial")
        if not args.skip_readiness:
            run.wait_ready("preflight")
        for origin in args.origins:
            run.probe(origin, "shard-layout")
            for shard in args.shards:
                label = f"{origin}-{shard}-write-latency"
                group = {"origin": origin, "shard": shard, "peer": PEERS[origin], "passed": False}
                result["groups"].append(group)
                argv = [*run.compose, "exec", "-T", origin, "sinomo", "test", "run",
                        "cluster.dfs.write-latency", "--require-cluster", "--json"]
                parameters = {"base": BASES[shard], "iterations": args.iterations,
                              "warmup-iterations": args.warmup, "payload-bytes": args.payload_bytes,
                              "max-p95-us": args.max_p95_us, "retain-fixture": "true"}
                for name, value in parameters.items():
                    argv += ["--param", f"{name}={value}"]
                probe_stem = run.artifacts / f"{run.index + 1:03d}-{label}"
                try:
                    raw = run.run(label, argv, timeout=args.command_timeout)
                    probe = json.loads(raw)
                    group["summary"] = validate_report(
                        probe, args.iterations, args.warmup, args.max_p95_us)
                    group["base"] = probe["diagnostics"]["base"]
                    group["samples"] = probe["diagnostics"]["samples"]
                    for sample in group["samples"]:
                        if sample["operation"] == "overwrite":
                            run.probe(PEERS[origin], "expect-file", path=sample["path"],
                                      payload_bytes=args.payload_bytes,
                                      payload_seed=sample["payload_seed"], timeout_seconds=60)
                    group["passed"] = True
                except Exception as error:
                    # Even a failed threshold exits nonzero. Keep its samples
                    # in the aggregate as well as the immutable raw stdout.
                    try:
                        failed = json.loads(probe_stem.with_suffix(".stdout").read_bytes())
                        group.setdefault("samples", failed.get("diagnostics", {}).get("samples", []))
                        group.setdefault("probe_diagnostics", failed.get("diagnostics", {}))
                    except (OSError, ValueError):
                        pass
                    group["error"] = f"{type(error).__name__}: {error}"
                    result["errors"].append(f"{label}: {group['error']}")
                finally:
                    (run.artifacts / "write-latency.json").write_text(json.dumps(result, indent=2) + "\n")
        for node in [f"node{i}" for i in range(1, 7)]:
            run.probe(node, "invariants", timeout_seconds=60)
        run.verify_processes("final")
        result["passed"] = not result["errors"] and all(group["passed"] for group in result["groups"])
        if not result["passed"]:
            raise RuntimeError("; ".join(result["errors"]))
    except Exception as error:
        result["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        (run.artifacts / "write-latency.json").write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
