#!/usr/bin/env python3
"""Verify a data-preserving DFS format upgrade on a disposable ready cluster.

Prepare fixtures using the older executable. Upgrade/recreate only that owned
project while keeping its volumes, then verify with the candidate executable.
The script never installs binaries or resets volumes itself.
"""

import argparse
import hashlib
import importlib.util
import json
import time
from pathlib import Path

CURRENT_NAMESPACE_PREPARE_CODEC = "packed-v6"
MAX_OBSERVED_METADATA_SHARDS = 128


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


def validate_baseline_snapshot(node, path, prefix, checked_header):
    """Admit old bytes without mistaking the stream's outer magic for its floor."""
    assert path.startswith("/data/dfs/metadata/") and path.endswith("/records/tables/snapshot.sobj"), path
    record = {"node": node, "path": path, "raw_prefix_hex": prefix.hex(),
              "checked_stream_header": checked_header}
    if checked_header is not None:
        assert checked_header["format_version"] == 3, "baseline already has candidate reader floor"
        generation = checked_header["generation"]
        assert len(generation) == 64 and all(char in "0123456789abcdefABCDEF" for char in generation), checked_header
    if prefix.startswith(b"SOBJ"):
        assert len(prefix) >= 12, "truncated legacy baseline snapshot header"
        document_version = int.from_bytes(prefix[4:6], "little")
        assert document_version in (1, 2, 3), "baseline already has candidate or unknown SOBJ version"
        record.update(layout="legacy-sobj", document_version=document_version)
    elif prefix.startswith(b"SMIGLOG1"):
        assert len(prefix) == 16, "truncated baseline stream prefix"
        assert 0 < int.from_bytes(prefix[8:16], "little") <= 1024 * 1024, "invalid bounded stream header length"
        assert checked_header is not None, "stream baseline requires a checked inner header; pass --inspector-binary"
        record.update(layout="migration-stream", snapshot_format_version=3)
    else:
        raise AssertionError(f"unknown baseline snapshot format on {node}: {prefix.hex()}")
    return record


def inspect_baseline_snapshots(run, operation_id):
    inspector = getattr(run.args, "inspector_binary", None)
    inspector_hash = None
    if inspector:
        inspector = inspector.resolve()
        assert inspector.is_file(), f"read-only inspector binary is missing: {inspector}"
        inspector_hash = hashlib.sha256(inspector.read_bytes()).hexdigest()
        assert inspector_hash != run.binary_hash, "inspector must support the new read-only header probe"
    layouts = []
    for number in range(1, 7):
        node = f"node{number}"
        script = ('seen=0; for snapshot in /data/dfs/metadata/*/records/tables/snapshot.sobj; do '
                  '[ ! -f "$snapshot" ] && continue; seen=$((seen + 1)); '
                  f'[ "$seen" -le {MAX_OBSERVED_METADATA_SHARDS} ] || exit 1; '
                  'printf "%s\\t" "$snapshot"; head -c 16 "$snapshot" | od -An -tx1 | tr -d " \\n"; '
                  'printf "\\n"; done')
        raw = run.run(f"{node}-baseline-physical-snapshot-prefixes",
                      [*run.compose, "exec", "-T", node, "sh", "-c", script])
        prefixes = []
        for line in raw.decode().splitlines():
            path, encoded = line.split("\t", 1)
            prefixes.append((path, bytes.fromhex(encoded)))
        assert len(prefixes) <= MAX_OBSERVED_METADATA_SHARDS, node
        checked = {}
        if inspector:
            staged = "/tmp/sinomo-snapshot-inspector-" + operation_id()
            run.run(f"{node}-stage-read-only-inspector",
                    [*run.compose, "cp", str(inspector), f"{node}:{staged}"])
            try:
                report = json.loads(run.run(f"{node}-inspect-baseline-reader-floor",
                    [*run.compose, "exec", "-T", node, staged, "test", "run",
                     "cluster.dfs.metadata-snapshot-reader-floor", "--require-cluster", "--json",
                     "--param", "require-floor6=false"]))
                assert report.get("status") == "passed", report
                for snapshot in report["diagnostics"]["snapshots"]:
                    checked[snapshot["shard_root"] + "/records/tables/snapshot.sobj"] = snapshot["header"]
            finally:
                run.run(f"{node}-remove-read-only-inspector",
                        [*run.compose, "exec", "-T", node, "rm", "-f", staged])
        for path, prefix in prefixes:
            layouts.append(validate_baseline_snapshot(node, path, prefix, checked.get(path)))
    assert layouts, "fixture has no materialized baseline snapshots"
    run.verify_processes("baseline-after-read-only-inspection")
    return layouts, inspector_hash


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
    layouts, inspector_hash = inspect_baseline_snapshots(run, operation_id)
    fixture = {"project": run.args.project, "prefix": prefix, "current": current,
               "current_bytes": current_bytes, "current_identity": current_identity,
               "retained_bytes": retained_bytes, "retained_identity": retained_identity,
               "baseline_binary_sha256": run.binary_hash, "baseline_snapshot_count": len(layouts),
               "baseline_snapshot_layouts": layouts, "read_only_inspector_binary_sha256": inspector_hash}
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
                    and all(shard["namespace_prepare_codec"] == CURRENT_NAMESPACE_PREPARE_CODEC for shard in shards)):
                pending.remove(node)
        if pending:
            if time.monotonic() >= deadline:
                raise RuntimeError(f"codec transition did not settle on {sorted(pending)} within {timeout}s")
            time.sleep(1)


def validate_migration_jobs(jobs):
    """Physical transfer receipts and local migration execution are distinct evidence."""
    for job in jobs:
        target = job["identity"]["target_version"]
        assert target in (3, 6), job
        assert job["identity"]["owner"] == "sinomo-dfs", job
        assert job["identity"]["migration"] == f"metadata-snapshot-v{target}", job
        assert job["phase"] in ("complete", "superseded"), job
        assert job["verified_records"] <= job["copied_records"], job
        if job["phase"] == "complete":
            assert job["copied_records"] == job["verified_records"], job
    assert any(job["phase"] == "complete" and job["identity"]["target_version"] == 6
               for job in jobs), "namespaced grant authoring did not expose a completed local floor-six migration"


def verify(run, fixture_path, operation_id):
    fixture = json.loads(fixture_path.read_text())
    assert fixture["project"] == run.args.project
    assert fixture["prefix"].startswith("/services/cluster-tests/storage-migration-")
    assert fixture["current"] == fixture["prefix"] + "/current.txt"
    run.verify_processes("candidate")
    wait_for_current_codec(run)
    run.wait_ready("upgraded")
    for number in range(1, 7):
        run.probe(f"node{number}", "shard-layout",
                  expected_namespace_prepare_codec=CURRENT_NAMESPACE_PREPARE_CODEC)
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
        jobs.extend(status.get("storage_migrations", []))
    validate_migration_jobs(jobs)
    count = snapshot_headers(run, b"SMIGLOG1")
    physical_floors = []
    for number in range(1, 7):
        raw = run.probe(f"node{number}", "metadata-snapshot-reader-floor", require_floor6="true")
        physical_floors.append(json.loads(raw))
    run.restart()
    run.read("node4", fixture["retained_identity"], fixture["retained_bytes"])
    run.expect("node5", fixture["current"], fixture["current_bytes"])
    run.expect("node6", recovered, fixture["retained_bytes"])
    assert run.history("node3", "recover", *request) == result
    restart_floors = []
    for number in range(1, 7):
        raw = run.probe(f"node{number}", "metadata-snapshot-reader-floor", require_floor6="true")
        restart_floors.append(json.loads(raw))
    return {"fixture": fixture, "migration_jobs": jobs, "current_snapshot_count": count,
            "physical_reader_floors": physical_floors, "restarted_reader_floors": restart_floors}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["prepare", "verify"])
    parser.add_argument("--project", required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--fixture", type=Path)
    parser.add_argument("--compose-override", type=Path)
    parser.add_argument("--restart-stagger-seconds", type=int, default=0,
                        help="delay node4 by 0..10 seconds after starting its peers")

    parser.add_argument("--inspector-binary", type=Path,
                        help="candidate executable used only for bounded read-only baseline header inspection")
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
              "inspector_binary": str(args.inspector_binary) if args.inspector_binary else None,
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
