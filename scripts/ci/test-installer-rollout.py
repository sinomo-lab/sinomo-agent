#!/usr/bin/env python3
"""Real isolated install -> evolution approval -> rollout -> rollback -> uninstall.

Run on a clean native CI account with the complete headless distribution. The
script never discovers cluster targets: every rollout names its fixture endpoint.
The candidate driver replaces the model with a deterministic conflict resolver,
while using the actual evolution workspace, verifier, bundle and approval engine.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cli", type=Path, required=True)
    parser.add_argument("--candidate-driver", type=Path, required=True)
    parser.add_argument("--builder", type=Path, required=True,
                        help="Isolated clean source checkout; replaced with each approved fixture revision")
    parser.add_argument("--target", required=True)
    parser.add_argument("--profile", default="dev")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    cli, driver, builder = (p.resolve() for p in (args.cli, args.candidate_driver, args.builder))
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    data, setup = root / "data", root / "setup"
    env = dict(os.environ, SINOMO_DATA_DIR=str(data))
    env.pop("SINOMO_INSTALL_DIR", None)
    env.update(SINOMO_SHADOW_MAIN_HOST="127.0.0.1")
    if os.name != "nt":
        env.update(HOME=str(root / "home"), XDG_CONFIG_HOME=str(root / "config"), XDG_DATA_HOME=str(root / "xdg-data"))
        Path(env["HOME"]).mkdir()
    # Ports remain fixed for the whole fixture; concrete endpoints prevent any
    # accidental rollout to an operator's existing cluster.
    sockets = [socket.socket(), socket.socket()]
    for sock in sockets:
        sock.bind(("127.0.0.1", 0))
    main_port, shadow_port = (s.getsockname()[1] for s in sockets)
    for sock in sockets:
        sock.close()
    endpoint = f"127.0.0.1:{main_port}"
    env["SINOMO_SHADOW_MAIN_PORT"] = str(main_port)
    suffix = ".exe" if os.name == "nt" else ""
    installed = data / "bin" / ("sinomo" + suffix)

    def run(label, command, *, expect_success=True, timeout=300, cwd=None, extra_env=None):
        # Commands can launch persistent services. Log files preserve timeout
        # evidence without waiting for EOF on inherited background-process pipes.
        stdout, stderr = root / f"{label}.out", root / f"{label}.err"
        with stdout.open("w", encoding="utf-8") as output, stderr.open("w", encoding="utf-8") as error:
            result = subprocess.run(list(map(str, command)), env=env | (extra_env or {}), cwd=cwd,
                                    stdout=output, stderr=error, timeout=timeout)
        if (result.returncode == 0) != expect_success:
            raise RuntimeError(f"{label}: exit={result.returncode}\n{stderr.read_text(encoding='utf-8', errors='replace')}")
        return stdout.read_text(encoding="utf-8", errors="replace")

    def digest(path):
        with path.open("rb") as stream:
            return hashlib.file_digest(stream, "sha256").hexdigest()

    def ready(label, revision=None):
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            result = subprocess.run([str(cli), "daemon", "status"], env=env, capture_output=True, text=True, timeout=10)
            if result.returncode == 0 and "is running" in result.stdout and (revision is None or revision[:12] in result.stdout):
                (root / f"{label}.out").write_text(result.stdout)
                return
            time.sleep(0.25)
        raise RuntimeError(f"{label}: fixture daemon did not reach revision {revision}")

    run("install", [cli, "install", "--headless", "--install-dir", setup, "--data-dir", data,
                    "--host", "127.0.0.1", "--port", main_port, "--shadow-port", shadow_port])
    if (setup / ("sinomo" + suffix)).exists():
        raise RuntimeError("Installer left a second CLI execution location")
    ready("installed-main")
    run("initialize", [cli, "cluster", "init"])
    ready("initialized-main")
    run("installed-evolution", [installed, "evolve", "status"])
    managed = data / "source/sinomo"
    initial_head = run("source-before", ["git", "-C", managed, "rev-parse", "HEAD"]).strip()
    for key, value in (("user.name", "Acceptance node"), ("user.email", "node@invalid"), ("commit.gpgsign", "false"), ("core.longpaths", "true")):
        run("builder-config-" + key, ["git", "-C", builder, "config", key, value])
    (builder / "node-only-divergence.txt").write_text("node-local source that must be replaced\n")
    run("builder-diverge-add", ["git", "-C", builder, "add", "node-only-divergence.txt"])
    run("builder-diverge-commit", ["git", "-C", builder, "commit", "-m", "Node-only acceptance divergence"])
    approved = json.loads(run("evolution-approve", [driver, data, "merge", builder]).strip().splitlines()[-1])
    revision = approved["revision"]
    if (builder / "node-only-divergence.txt").exists() or run("builder-head", ["git", "-C", builder, "rev-parse", "HEAD"]).strip() != revision:
        raise RuntimeError("Node source import merged local content instead of replacing it")
    if revision == initial_head:
        raise RuntimeError("Evolution did not produce a new approved source revision")
    if (builder / "installer-acceptance.txt").read_text() != "local and upstream changes resolved by evolution\n":
        raise RuntimeError("Node source import did not replace it with the approved merged snapshot")

    def build(label, revision):
        run(label, ["cargo", "build", "--locked", "--manifest-path", builder / "Cargo.toml", "--target", args.target,
                    "--profile", args.profile, "-p", "sinomo"], timeout=7200,
            extra_env={"SINOMO_ROLLOUT_BUILD_COMMIT_OVERRIDE": revision})
        target = Path(env.get("CARGO_TARGET_DIR", builder / "target"))
        artifact = root / f"{label}{suffix}"
        shutil.copy2(target / args.target / ("debug" if args.profile == "dev" else args.profile) / ("sinomo" + suffix), artifact)
        return artifact

    candidate = build("approved-build", revision)
    run("rollout-approved", [cli, "rollout", "cluster", "--repo", managed, "--binary", candidate,
                             "--skip-native-modules", "--target", endpoint], timeout=600)
    ready("approved-main", revision)
    if digest(installed) != digest(candidate):
        raise RuntimeError("Rollout did not replace the PATH/service binary")
    run("restart-shadow-stop", [cli, "daemon", "shadow", "stop"])
    run("restart-main-stop", [cli, "daemon", "stop"])
    # Explicit start through the canonical executable also exercises its data binding.
    run("restart-main", [installed, "daemon", "start", "--host", "127.0.0.1", "--port", main_port])
    ready("restarted-approved-main", revision)
    run("restart-shadow", [data / "bin" / ("sinomo-shadow" + suffix), "daemon", "shadow", "start",
                           "--host", "127.0.0.1", "--port", shadow_port])
    failed = json.loads(run("evolution-fault-fixture", [driver, data, "fail-startup", builder]).strip().splitlines()[-1])
    broken = build("broken-build", failed["revision"])
    run("rollout-startup-failure", [cli, "rollout", "cluster", "--repo", managed, "--binary", broken,
                                   "--skip-native-modules", "--target", endpoint], expect_success=False, timeout=600)
    ready("rollback-main", revision)
    if digest(installed) != digest(candidate):
        raise RuntimeError("Failed activation did not restore the approved binary")
    observed_fault = None
    for report in (data / "rollout").rglob("*.json"):
        try:
            evidence = json.loads(report.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if isinstance(evidence, dict) and evidence.get("exit_code") == 37 and evidence.get("status") == "failed":
            observed_fault = evidence
            break
    if observed_fault is None:
        raise RuntimeError("Fault candidate was not observed exiting during activation; rollback was not exercised")
    (root / "observed-failed-activation.json").write_text(json.dumps(observed_fault, indent=2))
    # Failed activation remains a failed operation even after restoring main.
    # Settlement proves no replacements remain and checks stable main identity
    # plus authoritative/local Raft readiness before retiring that failed plan.
    run("settle-rollback", [cli, "rollout", "cancel"], timeout=180)
    native_before = {str(p.relative_to(data)): digest(p) for p in (data / "module/cache/native-dylib").rglob("*") if p.is_file()}
    run("uninstall-keep", [installed, "uninstall"])
    if digest(installed) != digest(candidate) or not (data / "config/uninstalled").is_file():
        raise RuntimeError("Uninstall did not retain the evolved binary and fence revival")
    if native_before != {name: digest(data / name) for name in native_before}:
        raise RuntimeError("Keep-data uninstall altered native artifacts")
    run("fenced-start", [installed, "daemon", "start"], expect_success=False)
    run("reinstall", [installed, "install", "--headless", "--install-dir", setup, "--data-dir", data, "--no-start"])
    if digest(installed) != digest(candidate):
        raise RuntimeError("Reinstall overwrote the evolved binary")
    run("uninstall-delete", [installed, "uninstall", "--delete-data"])
    if data.exists():
        raise RuntimeError("Explicit data deletion did not complete")
    (root / "acceptance.json").write_text(json.dumps({"initial_source": initial_head, "approved": revision,
        "failed_candidate": failed["revision"], "canonical_binary": True, "approval_and_source_replacement": True,
        "real_restart": True, "activation_rollback": True, "native_artifacts_retained": True,
        "revival_fenced": True, "reinstall_preserved_evolution": True, "delete_data": True}, indent=2))
    print("PASS: install, evolution conflict resolution/approval, approved-source replacement, real binary rollout/restart/rollback and retained/deleted data")


if __name__ == "__main__":
    main()
