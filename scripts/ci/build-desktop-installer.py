#!/usr/bin/env python3
"""Build initial-installation packages from a host-native Sinomo desktop release."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tomllib

ROOT = Path(__file__).resolve().parents[2]
PACKAGER_VERSION = "0.11.8"


def run(*args, **kwargs):
    subprocess.run([str(arg) for arg in args], cwd=ROOT, check=True, **kwargs)


def write_icon(path):
    """Stage the Sinomo logo without requiring image conversion tools in CI."""
    shutil.copyfile(ROOT / "crates/sinomo-app/assets/logo.png", path)


def configuration(stage, output, target, version):
    windows = "windows" in target
    binaries = ("sinomo-setup", "sinomo", "sinomo-app", "sinomo-ui-extension-host") if windows else ("sinomo-setup",)
    config = {
        "name": "sinomo-setup", "productName": "Sinomo Setup", "version": version,
        "identifier": "net.sinomo.setup", "publisher": "Sinomo", "authors": ["Sinomo"],
        "description": "Install the Sinomo CLI and desktop app",
        "category": "Utility", "binariesDir": str(stage), "outDir": str(output),
        "targetTriple": target, "icons": [str(stage / "icon.png")],
        "binaries": [{"path": name, "main": name == "sinomo-setup"} for name in binaries],
        "formats": ["nsis" if windows else "appimage"],
    }
    manifest = stage / "desktop-release.json"
    if windows:
        config["resources"] = [{"src": str(manifest), "target": "desktop-release.json"},
                               {"src": str(ROOT / "crates/sinomo-installer/src/initial_setup/windows-integration.ps1"),
                                "target": "windows-integration.ps1"},
                               {"src": str(stage / "initial-payload.tar"), "target": "initial-payload.tar"}]
        config["nsis"] = {"installMode": "currentUser", "template": str(ROOT / "packaging/windows/installer.nsi"),
                          "installerIcon": str(ROOT / "crates/sinomo-app/assets/logo.ico")}
    else:
        # linuxdeploy handles the setup executable and its own dependencies.
        # The installable binaries retain their verified bytes inside the payload.
        config["appimage"] = {"files": {str(stage / "sinomo-payload.tar"): "usr/bin/sinomo-payload.tar"}}
    return config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True, choices=["x86_64-pc-windows-msvc", "x86_64-unknown-linux-gnu"])
    parser.add_argument("--profile", default="release", choices=["release", "dev"])
    parser.add_argument("--skip-build", action="store_true", help="Package an existing host-native build")
    parser.add_argument("--packager", default="cargo-packager")
    parser.add_argument("--test-root", type=Path, help="Directory for native acceptance fixtures; useful when the build volume is constrained")
    args = parser.parse_args()
    windows = "windows" in args.target
    if windows != (sys.platform == "win32") or (not windows and sys.platform != "linux"):
        parser.error("Run this packaging job on the target OS; desktop installers are host-native builds")
    target_dir = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target" )).resolve()
    binaries = target_dir / args.target / ("debug" if args.profile == "dev" else args.profile)
    output = Path(os.environ.get("SINOMO_DIST_DIR", ROOT / "dist/installers")) / args.target
    output = output.resolve()
    stage = target_dir / "installer-stage" / args.target
    if stage.exists():
        raise RuntimeError(f"Staging directory already exists: {stage}. Use a fresh build directory.")
    stage.mkdir(parents=True)
    test_root = args.test_root.resolve() if args.test_root else stage
    if not args.skip_build:
        run("cargo", "build", "--locked", "--profile", args.profile, "--target", args.target,
            "-p", "sinomo-app", "-p", "sinomo-ui-extension-host")
        # Setup owns installation authority; keep its host features out of the GUI build.
        run("cargo", "build", "--locked", "--profile", args.profile, "--target", args.target, "-p", "sinomo-setup")
        # Feature unification with the app must not add native media/GUI prerequisites to CLI.
        run("cargo", "build", "--locked", "--profile", args.profile, "--target", args.target, "-p", "sinomo")
    suffix = ".exe" if windows else ""
    for name in ("sinomo", "sinomo-app", "sinomo-ui-extension-host", "sinomo-setup"):
        shutil.copy2(binaries / (name + suffix), stage / (name + suffix))
    revision = os.environ.get("CI_COMMIT_SHA") or subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    run("cargo", "run", "--locked", "-p", "sinomo-installer", "--bin", "sinomo-package", "--",
        ROOT, binaries, stage, args.target, args.profile)
    if not windows:
        for name in ("sinomo-app", "sinomo-ui-extension-host", "sinomo-setup"):
            run("patchelf", "--force-rpath", "--set-rpath", "$ORIGIN/lib", stage / name)
    run(stage / ("sinomo" + suffix), "rollout", "app", "package", "--directory", stage,
        "--revision", revision, "--target-os", "windows" if windows else "linux", "--target-arch", "x86_64",
        env=dict(os.environ, SINOMO_DATA_DIR=str(stage / "manifest-data")))
    if not windows:
        with tarfile.open(stage / "sinomo-payload.tar", "w") as archive:
            for name in ("sinomo", "sinomo-app", "sinomo-ui-extension-host", "desktop-release.json", "initial-payload.tar", "sinomo-setup"):
                archive.add(stage / name, arcname=name, recursive=False)
            for library in sorted((stage / "lib").glob("*")):
                archive.add(library, arcname="lib/" + library.name, recursive=False)
        # The minimal-container gate also checks the exact CLI installed by this desktop package.
        with tarfile.open(stage / "headless-cli.tar.gz", "w:gz") as archive:
            archive.add(stage / "sinomo", arcname="sinomo", recursive=False)
            archive.add(stage / "initial-payload.tar", arcname="initial-payload.tar", recursive=False)
            for library in sorted((stage / "lib").glob("*")):
                archive.add(library, arcname="lib/" + library.name, recursive=False)
    write_icon(stage / "icon.png")
    version = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    output.mkdir(parents=True, exist_ok=True)
    cli_archive = output / f"sinomo-cli-{version}-{args.target}.{'zip' if windows else 'tar.gz'}"
    if windows:
        import zipfile
        with zipfile.ZipFile(cli_archive, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name in ("sinomo.exe", "sinomo-app.exe", "sinomo-ui-extension-host.exe", "sinomo-setup.exe", "desktop-release.json", "initial-payload.tar"):
                archive.write(stage / name, arcname=name)
    else:
        with tarfile.open(cli_archive, "w:gz") as archive:
            for name in ("sinomo", "sinomo-app", "sinomo-ui-extension-host", "sinomo-setup", "desktop-release.json", "initial-payload.tar"):
                archive.add(stage / name, arcname=name, recursive=False)
            for library in sorted((stage / "lib").glob("*")):
                archive.add(library, arcname="lib/" + library.name, recursive=False)
    config = stage / "packager.json"
    config.write_text(json.dumps(configuration(stage, output, args.target, version), indent=2), encoding="utf-8")
    tool_version = subprocess.check_output([args.packager, "--version"], text=True).strip()
    if tool_version != f"cargo-packager {PACKAGER_VERSION}":
        raise RuntimeError(f"Expected cargo-packager {PACKAGER_VERSION}, found {tool_version}")
    run(args.packager, "--config", config)
    artifacts = sorted(output.glob("*.exe" if windows else "*.AppImage"))
    if not artifacts:
        raise RuntimeError("Packager produced no installer")
    if not windows:
        for artifact in artifacts:
            # Exercise the final image and verify the untouched archive after ELF rewriting.
            # Extraction mode works in unprivileged containers without FUSE or a display.
            run(artifact, "--appimage-extract-and-run", "--verify-package", "--no-dialogs")
    else:
        for index, artifact in enumerate(artifacts):
            run("powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
                "-File", ROOT / "scripts/ci/test-desktop-installer-windows.ps1",
                "-Installer", artifact, "-SandboxRoot", test_root / f"smoke-test-{index}")
        run("powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
            "-File", ROOT / "scripts/ci/test-cli-install-windows.ps1", "-Cli", stage / "sinomo.exe",
            "-SandboxRoot", test_root / "cli-smoke-test")
    sums = []
    for artifact in [*artifacts, cli_archive]:
        with artifact.open("rb") as source:
            sums.append(f"{hashlib.file_digest(source, 'sha256').hexdigest()}  {artifact.name}\n")
    (output / "SHA256SUMS").write_text("".join(sums), encoding="utf-8")
    # GitLab artifact metadata is independent of the binary version; record exact input identity.
    (output / "build.json").write_text(json.dumps({"revision": revision, "target": args.target,
        "version": version, "packager": PACKAGER_VERSION, "profile": args.profile,
        "source_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip())},
        indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
