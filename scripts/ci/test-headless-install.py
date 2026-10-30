#!/usr/bin/env python3
"""Exercise the standalone CLI in a container without GUI libraries or a user init manager."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if sys.platform != "linux":
        parser.error("Run this acceptance check inside a minimal Linux container")
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    source = root / "source"
    source.mkdir()
    cli = source / "sinomo"
    with tarfile.open(args.archive, "r:gz") as archive:
        payload = archive.getmember("initial-payload.tar")
        with archive.extractfile(payload) as input_file, (source / "initial-payload.tar").open("wb") as output:
            shutil.copyfileobj(input_file, output)
        for library in archive.getmembers():
            if library.name.startswith("lib/"):
                archive.extract(library, source, filter="data")
        member = archive.getmember("sinomo")
        if not member.isfile():
            raise RuntimeError("CLI artifact must be a regular executable")
        with archive.extractfile(member) as input_file, cli.open("wb") as output:
            shutil.copyfileobj(input_file, output)
    cli.chmod(0o755)
    env = dict(os.environ)
    for key in ("DISPLAY", "WAYLAND_DISPLAY", "DBUS_SESSION_BUS_ADDRESS", "LD_LIBRARY_PATH",
                "SINOMO_DATA_DIR", "SINOMO_INSTALL_DIR"):
        env.pop(key, None)
    env["XDG_CONFIG_HOME"] = str(root / "config")
    env["XDG_DATA_HOME"] = str(root / "xdg-data")
    attempted_gui = root / "GUI-WAS-INVOKED"
    env["SINOMO_TEST_GUI_MARKER"] = str(attempted_gui)
    # Catch both PATH-based and sibling-executable graphical dispatch.
    for name in ("sinomo-setup", "zenity", "kdialog", "xdg-open"):
        guard = source / name
        guard.write_text('#!/bin/sh\nprintf GUI > "$SINOMO_TEST_GUI_MARKER"\nexit 97\n')
        guard.chmod(0o755)
    env["PATH"] = str(source) + os.pathsep + env.get("PATH", "")
    if shutil.which("systemctl", path=env["PATH"]):
        raise RuntimeError("Use a minimal image without systemctl for this acceptance gate")
    linked = subprocess.run(["ldd", str(cli)], capture_output=True, text=True)
    dependencies = linked.stdout + linked.stderr
    if "not found" in dependencies or re.search(r"lib(?:gtk|gdk|X11|wayland|GL\.|EGL|vulkan)", dependencies):
        raise RuntimeError("Standalone CLI has missing or graphical runtime dependencies:\n" + dependencies)
    (root / "dependencies.txt").write_text(dependencies)

    def run(log, executable, *arguments):
        result = subprocess.run([str(executable), *map(str, arguments)], env=env,
                                capture_output=True, text=True, timeout=150)
        (root / (log + ".out")).write_text(result.stdout)
        (root / (log + ".err")).write_text(result.stderr)
        if result.returncode:
            raise RuntimeError(f"{log} failed ({result.returncode}): {result.stderr}")
        return result.stdout

    application = root / "application with spaces"
    data = root / "data % $ with spaces"
    install = ["install", "--headless", "--install-dir", application, "--data-dir", data]
    run("install", cli, *install)
    installed = data / "bin/sinomo"
    if (application / "sinomo").exists():
        raise RuntimeError("Duplicate installed CLI")
    receipt = json.loads((application / "sinomo-install.json").read_text())
    services = receipt["services"]
    entries = [Path(services[key]) for key in ("main_entry", "shadow_entry")]
    for entry in entries:
        if not entry.is_file():
            raise RuntimeError(f"Missing service unit: {entry}")
    for link, target in services["enabled_links"]:
        if Path(link).readlink() != Path(target):
            raise RuntimeError("Service was not enabled by its persistent unit link")
    pids = []
    for role in (["daemon"], ["daemon", "shadow"]):
        status = run("-".join(role) + "-status", installed, *role, "status")
        if "is running" not in status:
            raise RuntimeError("Daemon did not answer RPC: " + status)
        pids.append(int(re.search(r"^  pid: (\d+)$", status, re.MULTILINE).group(1)))
    run("uninstall-keep", installed, "uninstall")
    if not data.is_dir() or any(entry.exists() for entry in entries):
        raise RuntimeError("Uninstall did not retain data and remove service registration")
    for process in pids:
        deadline = time.monotonic() + 15
        while Path(f"/proc/{process}/stat").exists():
            # A container without an init reaper may retain an exited orphan until PID 1 exits.
            try:
                state = Path(f"/proc/{process}/stat").read_text().rsplit(")", 1)[1].split()[0]
            except FileNotFoundError:
                break
            if state == "Z":
                break
            if time.monotonic() >= deadline:
                raise RuntimeError(f"Uninstall left a daemon running: {process}")
            time.sleep(0.1)
    run("reinstall", cli, *install)
    run("delete-data", cli, "uninstall", "--install-dir", application, "--delete-data")
    if data.exists():
        raise RuntimeError("Explicit data deletion left the selected data directory")
    run("stage", cli, *install, "--no-start")
    if (data / "daemon/status.sobj").exists():
        raise RuntimeError("--no-start launched a daemon")
    run("remove-stage", cli, "uninstall", "--install-dir", application, "--delete-data")
    if attempted_gui.exists():
        raise RuntimeError("CLI installation invoked a graphical helper")
    (root / "acceptance.json").write_text(json.dumps({
        "gui_invoked": False, "service_manager_present": False,
        "registered_and_started": True, "uninstalled_and_retained_data": True,
        "explicit_data_deletion": True, "no_start": True,
    }, indent=2))
    print("PASS: minimal container, no GUI dispatch/libraries, enabled services and live main/shadow RPC, removal and data retention")


if __name__ == "__main__":
    main()
