#!/usr/bin/env python3
"""Verify a data-preserving DFS format upgrade on a disposable ready cluster.

Prepare fixtures using the older executable. Upgrade/recreate only that owned
project while keeping its volumes, then verify with the candidate executable.
The script never installs binaries or resets volumes itself.
"""

import argparse
import importlib.util
import json
import time
from pathlib import Path


def load_history_verifier():
    path = Path(__file__).with_name("test-file-history.py")
    spec = importlib.util.spec_from_file_location("storage_migration_history", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def snapshot_headers(run, expected):
    total = 0
    for number in range(1, 7):
        node = f"node{number}"
        width = len(expected)
        script = ("for snapshot in /data/dfs/metadata/*/records/tables/snapshot.sobj; do "
                  '[ ! -f "$snapshot" ] || head -c ' + str(width) + ' "$snapshot"; done')
        raw = run.run(f"{node}-physical-snapshot-headers",
                      [*run.compose, "exec", "-T", node, "sh", "-c", script])
        assert len(raw) % width == 0, (node, raw)
        assert all(raw[index:index + width] == expected
                   for index in range(0, len(raw), width)), (node, raw)
        total += len(raw) // width
    assert total > 0, "fixture has no materialized snapshots"
    return total


def prepare(run, operation_id):
    prefix = f"/services/cluster-tests/storage-migration-{operation_id()}"
    current = prefix + "/current.txt"
    retained = prefix + "/retained.txt"
    current_bytes = "current bytes survive storage migration"
    retained_bytes = "retained bytes survive storage migration"
    run.verify_processes("baseline")
    run.write("node1", current, current_bytes)
    run.expect("node6", current, current_bytes)
    current_identity = run.history("node3", "capture", current, "--pin", "before-upgrade")["identity"]
    run.write("node5", retained, retained_bytes)
    run.expect("node2", retained, retained_bytes)
    retained_identity = run.history("node4", "capture", retained, "--pin", "keep-across-upgrade")["identity"]
    run.probe("node6", "remove", path=retained)
    run.read("node2", retained_identity, retained_bytes)
    headers = snapshot_headers(run, b"SOBJ")
    fixture = {"project": run.args.project, "prefix": prefix, "current": current,
               "current_bytes": current_bytes, "current_identity": current_identity,
               "retained_bytes": retained_bytes, "retained_identity": retained_identity,
               "baseline_binary_sha256": run.binary_hash, "baseline_snapshot_count": headers}
    (run.artifacts / "fixture.json").write_text(json.dumps(fixture, indent=2) + "\n")
    return fixture


def wait_for_current_codec(run, timeout=360):
    """An upgraded process can be ready before all fenced codec handoffs finish."""
    deadline = time.monotonic() + timeout
    pending = {f"node{number}" for number in range(1, 7)}
    while pending:
        for node in sorted(pending):
            try:
                raw = run.probe(node, "shard-layout")
            except RuntimeError:
                # Startup routing can still be converging. Every failed probe
                # retains its evidence; none admits a metadata write.
                continue
            shards = json.loads(raw)["diagnostics"]["shards"]
            if ({shard["shard_id"] for shard in shards} == {"root", "services", "users"}
                    and all(shard["namespace_prepare_codec"] == "packed-v3" for shard in shards)):
                pending.remove(node)
        if pending:
            if time.monotonic() >= deadline:
                raise RuntimeError(f"codec transition did not settle on {sorted(pending)} within {timeout}s")
            time.sleep(1)


def verify(run, fixture_path, operation_id):
    fixture = json.loads(fixture_path.read_text())
    assert fixture["project"] == run.args.project
    assert fixture["prefix"].startswith("/services/cluster-tests/storage-migration-")
    assert fixture["current"] == fixture["prefix"] + "/current.txt"
    run.verify_processes("candidate")
    wait_for_current_codec(run)
    run.wait_ready("upgraded")
    for number in range(1, 7):
        run.probe(f"node{number}", "shard-layout", expected_namespace_prepare_codec="packed-v3")
    run.expect("node6", fixture["current"], fixture["current_bytes"])
    observed = run.history("node2", "capture", fixture["current"], "--pin", "after-upgrade")["identity"]
    assert observed == fixture["current_identity"], "storage upgrade changed the canonical file generation"
    run.read("node5", fixture["retained_identity"], fixture["retained_bytes"])
    recovered = fixture["prefix"] + "/recovered.txt"
    identity = fixture["retained_identity"]
    request = [recovered, "--object", identity["object_id"], "--version", identity["version"],
               "--operation-id", operation_id()]
    result = run.history("node3", "recover", *request)
    run.expect("node1", recovered, fixture["retained_bytes"])
    assert run.history("node3", "recover", *request) == result, "restore replay changed its result"
    run.write("node6", fixture["prefix"] + "/new-format-write.txt", "new-format writer")
    run.expect("node2", fixture["prefix"] + "/new-format-write.txt", "new-format writer")
    jobs = []
    for number in range(1, 7):
        raw = run.cli(f"node{number}", f"node{number}-migration-status", "dfs", "status", "--json")
        status = json.loads(raw)
        for job in status.get("storage_migrations", []):
            assert job["identity"]["target_version"] == 3, job
            assert job["phase"] in ("complete", "superseded"), job
            if job["phase"] == "complete":
                assert job["copied_records"] == job["verified_records"], job
            jobs.append(job)
    assert any(job["phase"] == "complete" for job in jobs), "upgrade did not expose a completed durable migration job"
    count = snapshot_headers(run, b"SMIGLOG1")
    run.restart()
    run.read("node4", fixture["retained_identity"], fixture["retained_bytes"])
    run.expect("node5", fixture["current"], fixture["current_bytes"])
    run.expect("node6", recovered, fixture["retained_bytes"])
    assert run.history("node3", "recover", *request) == result
    return {"fixture": fixture, "migration_jobs": jobs, "current_snapshot_count": count}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["prepare", "verify"])
    parser.add_argument("--project", required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--fixture", type=Path)
    parser.add_argument("--compose-override", type=Path)
    args = parser.parse_args()
    if args.phase == "verify" and args.fixture is None:
        parser.error("verify requires --fixture from the baseline prepare phase")
    if args.artifacts.exists() and any(args.artifacts.iterdir()):
        parser.error("use a fresh artifact directory")
    args.max_write_retries = 0
    helper = load_history_verifier()
    run = helper.Acceptance(args)
    if args.compose_override:
        override = str(args.compose_override.resolve())
        run.compose += ["-f", override]
        run.env["SINOMO_TEST_COMPOSE_OVERRIDE"] = override
    report = {"phase": args.phase, "project": args.project, "binary": str(run.binary),
              "binary_sha256": run.binary_hash,
              "compose_override": str(args.compose_override) if args.compose_override else None}
    (run.artifacts / "verifier.py").write_bytes(Path(__file__).read_bytes())
    try:
        report["details"] = prepare(run, helper.operation_id) if args.phase == "prepare" else verify(run, args.fixture, helper.operation_id)
        report["passed"] = True
    except Exception as error:
        report.update(passed=False, error=str(error))
        raise
    finally:
        report["completed_commands"] = run.completed
        (run.artifacts / "result.json").write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
