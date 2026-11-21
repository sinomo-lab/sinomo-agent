#!/usr/bin/env python3
"""Compare actual AppImage and CLI setup against the same offline desktop payload."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import select
import subprocess
import tarfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    image = args.image.resolve()
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    subprocess.run([str(image), "--appimage-extract"], cwd=root, check=True, stdout=subprocess.DEVNULL)
    payload = root / "desktop-payload"
    with tarfile.open(root / "squashfs-root/usr/bin/sinomo-payload.tar") as archive:
        archive.extractall(payload, filter="data")

    def digest(path):
        with path.open("rb") as stream:
            return hashlib.file_digest(stream, "sha256").hexdigest()

    summaries = []
    for mode in ("gui", "cli"):
        fixture = root / mode
        home, data, setup = fixture / "home", fixture / "data", fixture / "setup"
        home.mkdir(parents=True)
        env = dict(os.environ, HOME=str(home), SINOMO_DATA_DIR=str(data),
                   XDG_CONFIG_HOME=str(fixture / "config"), XDG_DATA_HOME=str(fixture / "xdg-data"))
        for key in ("DISPLAY", "WAYLAND_DISPLAY", "DBUS_SESSION_BUS_ADDRESS", "SINOMO_INSTALL_DIR"):
            env.pop(key, None)

        def run(label, command):
            result = subprocess.run(list(map(str, command)), env=env, capture_output=True, text=True, timeout=180)
            (fixture / f"{label}.out").write_text(result.stdout)
            (fixture / f"{label}.err").write_text(result.stderr)
            if result.returncode:
                raise RuntimeError(f"{mode}/{label} failed: {result.stderr}")
            return result.stdout

        if mode == "gui":
            run("install", [image, "--appimage-extract-and-run", "install", "--install-dir", setup, "--data-dir", data, "--no-dialogs"])
        else:
            run("install", [payload / "sinomo", "install", "--install-dir", setup, "--data-dir", data])
        cli = data / "bin/sinomo"
        receipt = json.loads((setup / "sinomo-install.json").read_text())
        if (setup / "sinomo").exists() or (setup / "Sinomo.AppImage").exists():
            raise RuntimeError("Setup retained a second executable distribution")
        for role in (["daemon"], ["daemon", "shadow"]):
            if "is running" not in run("-".join(role) + "-status", [cli, *role, "status"]):
                raise RuntimeError("Installer did not start its daemon services")
        run("evolution", [cli, "evolve", "--help"])
        initial_digest = digest(cli)
        initial_receipt = (setup / "sinomo-install.json").read_bytes()
        if mode == "gui":
            run("verify-cli", [cli, "install"])
        else:
            run("verify-gui", [image, "--appimage-extract-and-run", "install", "--install-dir", setup, "--data-dir", data, "--no-dialogs"])
        if digest(cli) != initial_digest or (setup / "sinomo-install.json").read_bytes() != initial_receipt:
            raise RuntimeError("Cross-frontend verification changed the installation")
        run("configure-on", [cli, "install", "--add-to-path", "--menu-shortcut", "--desktop-shortcut", "--no-startup"])
        changed = json.loads((setup / "sinomo-install.json").read_text())
        if changed["configuration"] != dict(add_to_path=True, menu_shortcut=True, desktop_shortcut=True, startup=False):
            raise RuntimeError("Integration settings were not reconciled")
        run("preserve-settings", [cli, "install"])
        if json.loads((setup / "sinomo-install.json").read_text())["configuration"] != changed["configuration"]:
            raise RuntimeError("Default verification reset configured preferences")
        run("configure-off", [cli, "install", "--no-add-to-path", "--no-menu-shortcut", "--no-desktop-shortcut", "--startup"])

        for role in (["daemon", "shadow"], ["daemon"]):
            status = run("status-before-repair-" + "-".join(role), [cli, *role, "status"])
            pid = int(re.search(r"^  pid: (\d+)$", status, re.MULTILINE)[1])
            # A stop response acknowledges the request. The daemon still has to
            # append its shutdown event before we snapshot the retained files.
            pidfd = os.pidfd_open(pid)
            try:
                run("stop-before-repair-" + "-".join(role), [cli, *role, "stop"])
                if not select.select([pidfd], [], [], 30)[0]:
                    raise RuntimeError(f"{' '.join(role)} pid {pid} did not exit before repair")
            finally:
                os.close(pidfd)
        sentinel = data / "repair-retained-data"
        sentinel.write_bytes(b"application data survives binary repair")
        excluded = ("bin/", "module/", "rollout/", "daemon/", "shadow-daemon/", "boot/")
        retained = {p.relative_to(data).as_posix(): digest(p) for p in data.rglob("*")
                    if p.is_file() and not p.relative_to(data).as_posix().startswith(excluded)}
        cli.write_bytes(b"damaged executable")
        (data / "bin/sinomo-shadow").write_bytes(b"damaged shadow")
        if mode == "gui":
            run("repair", [image, "--appimage-extract-and-run", "install", "--repair", "--no-start", "--no-dialogs", "--install-dir", setup, "--data-dir", data])
        else:
            run("repair", [payload / "sinomo", "install", "--repair", "--no-start", "--install-dir", setup, "--data-dir", data])
        if digest(cli) != initial_digest or digest(data / "bin/sinomo-shadow") != initial_digest:
            raise RuntimeError("Repair did not restore packaged daemon binaries")
        for path, before in retained.items():
            if digest(data / path) != before:
                raise RuntimeError(f"Repair changed retained file: {path}")
        run("start-after-repair", [cli, "install"])

        # Content identity is independent of chosen paths and fallback listener ports.
        summaries.append({"cli": digest(cli), "app": digest(data / "bin/sinomo-app"),
            "native": sorted(digest(p) for p in (data / "module/cache/native-dylib").rglob("*.so")),
            "desktop": json.loads(run("app-status", [cli, "rollout", "app", "status"]))["active_release"],
            "service_count": len(receipt["services"]["enabled_links"])})
        if mode == "gui":
            run("uninstall-keep", [cli, "uninstall"])
        else:
            run("uninstall-keep", [image, "--appimage-extract-and-run", "uninstall", "--install-dir", setup, "--no-dialogs"])
        if not cli.is_file() or not (data / "config/uninstalled").is_file():
            raise RuntimeError("Uninstall did not retain and fence the deployment")
        run("offline-reinstall", [cli, "install", "--install-dir", setup, "--data-dir", data, "--no-start"])
        run("delete-data", [cli, "uninstall", "--delete-data"])
        if data.exists():
            raise RuntimeError("Explicit data removal did not complete")
    if summaries[0] != summaries[1] or not summaries[0]["native"]:
        raise RuntimeError(f"Default CLI and graphical installations differ: {summaries}")
    (root / "acceptance.json").write_text(json.dumps(summaries[0], indent=2))
    print("PASS: actual AppImage/CLI default parity, live services/default modules, cross-uninstall and offline reinstall")


if __name__ == "__main__":
    main()
