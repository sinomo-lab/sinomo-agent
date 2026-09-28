#!/usr/bin/env python3
"""Gate ordinary committed DFS write latency on an existing six-node dev cluster.

Never starts, restarts or removes containers. The default checks readiness and
the standard 30-second container quiet gate, measures 100 creates and overwrites
per replication policy/origin/shard, then validates every final
file from the opposite segment. Fixtures and the project are retained.
"""

import argparse
import hashlib
import importlib.util
from itertools import product
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
POLICIES = {"w1-n2": {"consistency": "replicated-eventual", "min_commit_replicas": 1,
                      "desired_replicas": 2},
            "w2-n3": {"consistency": "replicated-eventual", "min_commit_replicas": 2,
                      "desired_replicas": 3}}


def verified_fixtures(fixtures, project, selected):
    """Reuse only retained fixtures with complete policy and correctness evidence."""
    if fixtures.get("project") != project or fixtures.get("fixtures_retained") is not True:
        raise ValueError("fixtures must be a retained matrix from this project")
    groups = {}
    for group in fixtures.get("groups", []):
        key = (group["replication_scenario"], group["origin"], group["shard"])
        if key in groups:
            raise ValueError(f"duplicate fixture: {key}")
        groups[key] = group
    for key in selected:
        group = groups.get(key) or {}
        evidence = group.get("probe_diagnostics", {})
        samples = evidence.get("samples", [])
        base = group.get("base")
        if (not isinstance(base, str) or not base.startswith(BASES[key[2]] + "/")
                or ".." in base.split("/") or evidence.get("base") != base
                or evidence.get("correctness_passed") is not True or not samples
                or not all(s.get("placement_verified") is True and s.get("succeeded") is True
                           and s.get("error") is None
                           and s.get("placement_policy") == POLICIES[key[0]]
                           and s.get("placement_source_path") == base for s in samples)):
            raise ValueError(f"missing verified fixture: {key}")
    return groups


def validate_report(report, iterations, warmup, max_p95_us, scenario):
    """Reject missing, censored, pooled, or incorrectly classified evidence."""
    diagnostics = report.get("diagnostics", {})
    samples = diagnostics.get("samples", [])
    if report.get("status") != "passed":
        raise RuntimeError(f"write probe did not pass: {report.get('summary', report.get('status'))}")
    if diagnostics.get("wait_for_commit") is not True:
        raise RuntimeError("write probe did not prove committed return")
    if diagnostics.get("correctness_passed") is not True:
        raise RuntimeError("write probe did not prove read-your-write correctness")
    expected = POLICIES[scenario]
    if (diagnostics.get("replication_scenario") != scenario
            or diagnostics.get("requested_placement_policy") != expected):
        raise RuntimeError("write probe omitted or changed the requested replication policy")
    if len(samples) != (iterations + warmup) * 2:
        raise RuntimeError("write probe omitted operations")
    summaries = []
    for operation in ("create", "overwrite"):
        group = [sample for sample in samples if sample.get("operation") == operation]
        if [sample.get("index") for sample in group] != list(range(iterations + warmup)):
            raise RuntimeError(f"{operation} sample indices are incomplete or duplicated")
        for sample in group:
            if (sample.get("placement_verified") is not True
                    or sample.get("placement_policy") != expected
                    or not diagnostics.get("base")
                    or sample.get("placement_source_path") != diagnostics["base"]):
                raise RuntimeError(f"{operation} has missing or mismatched placement policy evidence")
            if sample.get("succeeded") is not True or sample.get("error") is not None:
                raise RuntimeError(f"{operation} includes a failed operation")
            if sample.get("measured") is not (sample["index"] >= warmup):
                raise RuntimeError(f"{operation} warmup classification is inconsistent")
            if not isinstance(sample.get("elapsed_us"), int) or sample["elapsed_us"] < 0:
                raise RuntimeError(f"{operation} has an invalid duration")
        values = [sample["elapsed_us"] for sample in group if sample["measured"]]
        summary = {"operation": operation, "replication_scenario": scenario, **expected,
                   **acceptance.verification_report.latency_summary(values)}
        if max_p95_us and summary["p95_us"] >= max_p95_us:
            raise RuntimeError(f"{operation} p95 {summary['p95_us']}us is not below {max_p95_us}us")
        summaries.append(summary)
    return summaries


def record_probe_evidence(group, raw):
    """Keep reported evidence before command status or latency validation."""
    report = json.loads(raw)
    if not isinstance(report, dict) or not isinstance(report.get("diagnostics"), dict):
        raise ValueError("write probe report and diagnostics must be JSON objects")
    diagnostics = report["diagnostics"]
    group["probe_status"] = report.get("status")
    group["probe_summary"] = report.get("summary")
    group["probe_diagnostics"] = diagnostics
    group["base"] = diagnostics.get("base")
    group["samples"] = diagnostics.get("samples", [])
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--profile", default="unavailable", help="record the verified binary's build profile")
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--fixtures", type=Path,
                        help="reuse a retained verified write-latency.json; the probe still verifies each operation's effective W/N")
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--warmup", type=int, default=12)
    parser.add_argument("--payload-bytes", type=int, default=4096)
    parser.add_argument("--max-p95-us", type=int, default=100_000,
                        help="exclusive limit for each policy/origin/shard/operation; 0 is diagnostic only")
    parser.add_argument("--policies", nargs="+", choices=list(POLICIES), default=list(POLICIES),
                        help="replication scenarios; acceptance requires both w1-n2 and w2-n3")
    parser.add_argument("--origins", nargs="+", choices=list(PEERS), default=list(PEERS))
    parser.add_argument("--shards", nargs="+", choices=list(BASES), default=list(BASES))
    parser.add_argument("--command-timeout", type=int, default=600)
    parser.add_argument("--skip-readiness", action="store_true",
                        help="diagnostic reuse only; omits standard readiness/mutation/convergence/quiet gates")
    parser.add_argument("--stage-profile", action=argparse.BooleanOptionalAction, default=True,
                        help="generate per-stage cluster reports with Observe and CPU profiles (default enabled)")
    args = parser.parse_args()
    fixture_raw, fixture_groups = None, None
    if args.fixtures:
        fixture_raw = args.fixtures.read_bytes()
        try:
            fixture_groups = verified_fixtures(json.loads(fixture_raw), args.project,
                                              product(args.policies, args.origins, args.shards))
        except (ValueError, KeyError, TypeError) as error:
            parser.error(str(error))
    if not __debug__:
        parser.error("omit -O/PYTHONOPTIMIZE: imported acceptance checks use assertions")
    if not 1 <= args.iterations <= 10_000 or not 0 <= args.warmup <= 1000:
        parser.error("iterations must be 1..10000 and warmup 0..1000")
    if not 1 <= args.payload_bytes <= 1024 * 1024 or args.max_p95_us < 0:
        parser.error("payload-bytes must be 1..1048576 and max-p95-us nonnegative")
    if args.command_timeout <= 0:
        parser.error("command-timeout must be positive")
    for selection in (args.policies, args.origins, args.shards):
        if len(selection) != len(set(selection)):
            parser.error("policies, origins and shards must not contain duplicates")
    # Reserve the result before copying provenance or running commands. This
    # also excludes a concurrent invocation using the same artifact directory.
    args.artifacts = args.artifacts.resolve()
    args.artifacts.mkdir(parents=True, exist_ok=True)
    result_path = args.artifacts / "write-latency.json"
    try:
        with result_path.open("x") as initial:
            initial.write(json.dumps({"passed": False, "state": "initializing"}) + "\n")
    except FileExistsError:
        parser.error(f"artifact result already exists: {result_path}; use a new artifact directory")
    run = acceptance.Acceptance(args)
    run.env["SINOMO_TEST_DFS_READY_REQUIRE_QUIET"] = "1"
    result = {"schema": "sinomo.dfs-write-latency.v2", "passed": False,
              "project": args.project, "binary": str(run.binary), "binary_sha256": run.binary_hash,
              "parameters": {key: str(value) if isinstance(value, Path) else value
                             for key, value in vars(args).items()
                             if key not in ("binary", "artifacts")},
              "source_head": subprocess.check_output(["git", "rev-parse", "HEAD"],
                                                       cwd=run.root, text=True).strip(),
              "timing_scope": "public daemon DFS write_file API through committed response; client RPC included",
              "fixtures_retained": True, "groups": [], "errors": [],
              "initial_identity_verified": False, "final_identity_verified": False}
    result["acceptance_scope"] = (args.iterations >= 100 and args.payload_bytes == 4096
                                  and args.max_p95_us == 100_000
                                  and set(args.origins) == set(PEERS)
                                  and set(args.shards) == set(BASES)
                                  and set(args.policies) == set(POLICIES) and not args.skip_readiness
                                  and args.stage_profile)
    if fixture_raw is not None:
        (run.artifacts / "input-fixtures.json").write_bytes(fixture_raw)
        result["input_fixtures_sha256"] = hashlib.sha256(fixture_raw).hexdigest()
        result["fixture_reuse"] = True
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
        run.verify_observe()
        result["initial_identity_verified"] = True
        if not args.skip_readiness:
            run.wait_ready("preflight")
        for origin in args.origins:
            run.probe(origin, "shard-layout")
            for scenario, shard in product(args.policies, args.shards):
                label = f"{scenario}-{origin}-{shard}-write-latency"
                group = {"replication_scenario": scenario, **POLICIES[scenario],
                         "origin": origin, "shard": shard, "peer": PEERS[origin], "passed": False,
                         "command_timed_out": False}
                result["groups"].append(group)
                argv = [*run.compose, "exec", "-T", origin, "sinomo", "test", "run",
                        "cluster.dfs.write-latency", "--require-cluster", "--json"]
                parameters = {"base": BASES[shard], "replication": scenario,
                              "iterations": args.iterations,
                              "warmup-iterations": args.warmup, "payload-bytes": args.payload_bytes,
                              "max-p95-us": args.max_p95_us, "retain-fixture": "true"}
                if fixture_groups is not None:
                    parameters.pop("base")
                    parameters["prepared-base"] = fixture_groups[(scenario, origin, shard)]["base"]
                for name, value in parameters.items():
                    argv += ["--param", f"{name}={value}"]
                probe_stem = run.artifacts / f"{run.index + 1:03d}-{label}"
                try:
                    raw = run.run(label, argv, timeout=args.command_timeout)
                    probe = record_probe_evidence(group, raw)
                    group["summary"] = validate_report(
                        probe, args.iterations, args.warmup, args.max_p95_us, scenario)
                    for sample in group["samples"]:
                        if sample["operation"] == "overwrite":
                            run.probe(PEERS[origin], "expect-file", path=sample["path"],
                                      payload_bytes=args.payload_bytes,
                                      payload_seed=sample["payload_seed"], timeout_seconds=60)
                    group["passed"] = True
                except Exception as error:
                    group["command_timed_out"] = isinstance(error, subprocess.TimeoutExpired)
                    # Even a failed threshold exits nonzero. Keep its samples
                    # in the aggregate as well as the immutable raw stdout.
                    try:
                        record_probe_evidence(group, probe_stem.with_suffix(".stdout").read_bytes())
                    except (OSError, ValueError) as evidence_error:
                        group["probe_evidence_error"] = str(evidence_error)
                    group["error"] = f"{type(error).__name__}: {error}"
                    result["errors"].append(f"{label}: {group['error']}")
                finally:
                    (run.artifacts / "write-latency.json").write_text(json.dumps(result, indent=2) + "\n")
        for node in [f"node{i}" for i in range(1, 7)]:
            run.probe(node, "invariants", timeout_seconds=60)
        run.verify_processes("final")
        result["final_identity_verified"] = True
        result["passed"] = not result["errors"] and all(group["passed"] for group in result["groups"])
        (run.artifacts / "write-latency.json").write_text(json.dumps(result, indent=2) + "\n")
        if args.stage_profile:
            command = ["python3", str(Path(__file__).with_name("profile-dfs-writes.py")),
                       "--fixtures", str(result_path), "--project", args.project,
                       "--binary", str(run.binary), "--profile", args.profile,
                       "--artifacts", str(run.artifacts / "stages"),
                       "--policies", *args.policies, "--origins", *args.origins, "--shards", *args.shards]
            try:
                run.run("stage-profiles", command, timeout=len(result["groups"]) * 180 + 120)
                stage_report = json.loads((run.artifacts / "stages/report.json").read_text())
                result["stage_profile_passed"] = stage_report.get("complete") is True
                if not result["stage_profile_passed"]:
                    raise RuntimeError("stage profile coverage is incomplete")
            except Exception as error:
                result["stage_profile_passed"] = False
                result["errors"].append(f"stage profiling: {error}")
            result["passed"] = result["passed"] and result["stage_profile_passed"]
        if not result["passed"]:
            raise RuntimeError("; ".join(result["errors"]))
    except BaseException as error:
        result["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        (run.artifacts / "write-latency.json").write_text(json.dumps(result, indent=2) + "\n")
        acceptance.verification_report.write_report(run.artifacts)
        print(f"Report: {run.artifacts / 'REPORT.md'}", flush=True)


if __name__ == "__main__":
    main()
