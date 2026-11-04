#!/usr/bin/env python3
"""Verify real KWin/Wayland portals and native application effects in Docker.

No host GUI/session sockets are mounted. KWin uses Mesa software rendering and
the existing /dev/udmabuf device. Use --build-artifacts for a container-only build.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import select
import subprocess
import time
import tarfile
import uuid

SOURCE = Path(__file__).resolve().parent
REPO = SOURCE.parents[3]


def run(args, **kwargs):
    return subprocess.run([str(arg) for arg in args], check=True, **kwargs)


def digest(path):
    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


class Probe:
    def __init__(self, container, output):
        self.log = (output / "requests.jsonl").open("w")
        self.errors = (output / "probe-stderr.log").open("w")
        self.process = subprocess.Popen(["docker", "exec", "-i", container, "bash", "-c",
            "source /out/session.env; exec /artifacts/probe /artifacts/provider.so /out/node/identity/local-permissions.sobj /out/frames"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.errors, text=True, bufsize=1)
        assert self.read()["probe"] == "ready"

    def read(self):
        if not select.select([self.process.stdout], [], [], 15)[0]:
            raise RuntimeError("native probe response deadline exceeded")
        line = self.process.stdout.readline()
        if not line:
            raise RuntimeError("native probe exited; inspect probe-stderr.log")
        return json.loads(line)

    def call(self, request):
        self.process.stdin.write(json.dumps(request) + "\n")
        self.process.stdin.flush()
        result = self.read()
        self.log.write(json.dumps({"request": request, "response": result}) + "\n")
        self.log.flush()
        if "probe_error" in result:
            raise RuntimeError(result)
        return result

    def command(self, value, save=False):
        return self.call({"command": value, "save_frame": save})

    def success(self, value):
        result = self.command(value)["response"]
        if result["status"] == "failed":
            raise RuntimeError(result)
        return result

    def close(self):
        try:
            if self.process.poll() is None:
                self.call({"probe": "unload"})
                self.process.stdin.close()
                if self.process.wait(timeout=10) != 0:
                    raise RuntimeError("native probe failed during shutdown")
        finally:
            if self.process.poll() is None:
                self.process.kill()
                self.process.wait()
            self.log.close()
            self.errors.close()


def verify(container, output, checks):
    def desktop(command):
        return subprocess.check_output(["docker", "exec", container, "bash", "-c", "source /out/session.env; " + command], text=True, timeout=15)

    def nodes():
        return json.loads(desktop("python3 /opt/wayland-test/ui_inspect.py"))

    def click(name):
        # Structured argv keeps control labels out of shell syntax.
        run(["docker", "exec", container, "bash", "-c", 'source /out/session.env; exec python3 /opt/wayland-test/ui_inspect.py "$1"', "ui", name], stdout=subprocess.DEVNULL)

    def wait_for(predicate, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = predicate()
            if result:
                return result
            time.sleep(.1)
        raise RuntimeError("condition did not become true before deadline")

    def state():
        return json.loads((output / "fixture-state.json").read_text())

    wait_for(lambda: (output / "session-ready").exists() and (output / "fixture-state.json").exists(), 30)
    support = desktop("qdbus6 org.kde.KWin /KWin supportInformation")
    (output / "kwin-support.txt").write_text(support)
    assert "Compositing is active" in support, "KWin has no renderer; inspect udmabuf access and kwin.log"
    assert "llvmpipe" in support, "expected software rendering"
    assert state()["display"] == "GdkWaylandDisplay", state()
    checks.append("real Wayland app and software compositor")
    probe = Probe(container, output)
    try:
        caps = probe.success({"action": "capabilities"})["capabilities"]
        assert caps["capture"] and caps["keyboard_mouse"] and not caps["background_window_input"], caps
        denied = probe.command({"action": "targets", "mode": "raw"})["response"]
        assert denied["error"]["code"] == "permission_denied", denied
        probe.call({"probe": "remote_management", "enabled": True})
        assert probe.success({"action": "targets", "mode": "raw"})["targets"] == []
        wait_for(lambda: any(n["name"] == "Allow" for n in nodes()))
        click("Deny")
        def cancelled():
            result = probe.command({"action": "targets", "mode": "raw"})["response"]
            return result if result["status"] == "failed" else None
        denied = wait_for(cancelled)
        assert denied["error"]["code"] == "permission_denied" and "cancelled" in denied["error"]["detail"], denied
        checks.append("real portal denial fails closed")
        assert probe.success({"action": "targets", "mode": "raw"})["targets"] == []
        wait_for(lambda: any(n["name"] == "Allow" for n in nodes()))
        remember = "Allow the application to do this without asking next time"
        if any(n["name"] == remember and n["checked"] for n in nodes()):
            click(remember)
            wait_for(lambda: not any(n["name"] == remember and n["checked"] for n in nodes()))
        click("Allow")
        info = wait_for(lambda: next(iter(probe.success({"action": "targets", "mode": "raw"})["targets"]), None))
        target = info["target"]
        denied = probe.command({"action": "open_input", "mode": "raw", "target": target, "lease_ms": 300000})["response"]
        assert denied["error"]["code"] == "permission_denied", denied
        for operation in ["capture", "input"]:
            probe.call({"probe": "expose", "resource": info["resource_id"], "operation": operation})
        capture = probe.success({"action": "open_capture", "config": {"target": target, "frames_per_second": 10, "include_cursor": False, "lease_ms": 300000}})["id"]
        handle = probe.success({"action": "open_input", "mode": "raw", "target": target, "lease_ms": 300000})["id"]
        checks.append("real local chooser plus separate exact-resource grants")
        wait_for(lambda: state().get("active"))

        def events(values):
            return probe.success({"action": "input", "id": handle, "events": values})

        def point(widget):
            s = state()
            x, y, w, h = s[widget]
            bounds = info["bounds"]
            return int((x + w / 2) * bounds[2] / s["size"][0]), int((y + h / 2) * bounds[3] / s["size"][1])

        def tap(widget):
            x, y = point(widget)
            events([{"kind": "move", "x": x, "y": y}, {"kind": "button", "button": "left", "down": True}, {"kind": "button", "button": "left", "down": False}])

        first = wait_for(lambda: probe.command({"action": "frame", "id": capture, "after_sequence": 0, "timeout_ms": 100}, True).get("frame"))
        assert first["bytes"] == first["width"] * first["height"] * 4, first
        time.sleep(.3)
        second = wait_for(lambda: probe.command({"action": "frame", "id": capture, "after_sequence": first["sequence"], "timeout_ms": 100}, True).get("frame"))
        assert second["sequence"] > first["sequence"] and second["timestamp_ns"] >= first["timestamp_ns"]
        assert second["checksum"] != first["checksum"], "expected animated native desktop frames"
        (output / "capture-metadata.json").write_text(json.dumps({"target": info, "frames": [first, second]}, indent=2))
        desktop("python3 -c 'from PIL import Image; import json; p=\"/out/frames/\"; m=json.load(open(p+\"frame.json\")); Image.frombytes(\"RGBA\", (m[\"width\"],m[\"height\"]),open(p+\"frame.bgra\",\"rb\").read(),\"raw\",\"BGRA\").save(p+\"frame.png\")'")
        checks.append("real compositor pixels and fresh native frames")
        before = state()["clicks"]
        tap("button")
        wait_for(lambda: state()["clicks"] == before + 1)
        tap("entry")
        events([{"kind": "text", "text": "Wayland Ω test"}])
        wait_for(lambda: state()["text"] == "Wayland Ω test")
        checks.append("native click and Unicode text observed by the app")
        events([{"kind": "key", "code": 29, "down": True}, {"kind": "key", "code": 30, "down": True}, {"kind": "key", "code": 30, "down": False}, {"kind": "key", "code": 29, "down": False}, {"kind": "key", "code": 14, "down": True}, {"kind": "key", "code": 14, "down": False}])
        wait_for(lambda: state()["text"] == "")
        events([{"kind": "key", "code": 42, "down": True}])
        probe.success({"action": "close_input", "id": handle})
        handle = probe.success({"action": "open_input", "mode": "raw", "target": target, "lease_ms": 300000})["id"]
        events([{"kind": "key", "code": 30, "down": True}, {"kind": "key", "code": 30, "down": False}])
        wait_for(lambda: state()["text"] == "a")
        checks.append("held Shift released on lease close")
        x, y = point("canvas")
        motion_before = state()["motion"]
        events([{"kind": "move", "x": x, "y": y}])
        wait_for(lambda: state()["motion"] > motion_before)
        motion_before, position_before = state()["motion"], state()["last_motion"]
        events([{"kind": "relative_move", "dx": 5, "dy": 5}])
        wait_for(lambda: state()["motion"] > motion_before)
        assert all(after > before for after, before in zip(state()["last_motion"], position_before)), state()
        before = state()["scrolls"]
        events([{"kind": "wheel", "delta": 120}])
        wait_for(lambda: state()["scrolls"] > before)
        assert state()["scroll_y"] < 0, state()
        checks.append("relative pointer and wheel effects")
        probe.call({"probe": "revoke", "resource": info["resource_id"], "operation": "capture"})
        denied = probe.command({"action": "frame", "id": capture, "after_sequence": 0, "timeout_ms": 100})["response"]
        assert denied["error"]["code"] == "permission_denied", denied
        probe.call({"probe": "remote_management", "enabled": False})
        denied = probe.command({"action": "input", "id": handle, "events": [{"kind": "move", "x": 0, "y": 0}]})["response"]
        assert denied["status"] == "failed", denied
        probe.call({"probe": "unload"})
        probe.call({"probe": "reload"})
        probe.call({"probe": "remote_management", "enabled": True})
        denied = probe.command({"action": "open_input", "mode": "raw", "target": target, "lease_ms": 1000})["response"]
        assert denied["status"] == "failed", denied
        probe.call({"probe": "remote_management", "enabled": False})
        checks.append("cached-frame revocation, input denial, unload and stale-target rejection")
    finally:
        probe.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="sinomo-wayland-compositor:local")
    parser.add_argument("--build-image", action="store_true")
    parser.add_argument("--build-artifacts", action="store_true")
    parser.add_argument("--provider", type=Path, default=Path("target/debug/libsinomo_app_use_module.so"))
    parser.add_argument("--probe", type=Path, default=Path("target/debug/examples/android_native_probe"))
    parser.add_argument("--scale", type=float, default=1)
    parser.add_argument("--output", type=Path, default=Path("target/app-use-wayland-compositor"))
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()
    if not Path("/dev/udmabuf").exists():
        parser.error("this KWin headless lane requires existing /dev/udmabuf; use a VM with virtual graphics if unavailable")
    output = args.output.resolve() / (time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:6])
    output.mkdir(parents=True)
    if args.build_image:
        with (output / "image-build.log").open("w") as log:
            run(["docker", "build", "--target", "runtime", "-t", args.image, SOURCE / "compositor"], stdout=log, stderr=subprocess.STDOUT)
    if args.build_artifacts:
        builder = args.image + "-builder"
        with (output / "artifact-build.log").open("w") as log, open("/tmp/sinomo-maintenance-build.lock", "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            run(["docker", "build", "--target", "builder", "-t", builder, SOURCE / "compositor"], stdout=log, stderr=subprocess.STDOUT)
            build, cargo = output / "build", output / "cargo"
            build.mkdir(); cargo.mkdir()
            run(["docker", "run", "--rm", "--user", f"{os.getuid()}:{os.getgid()}", "--memory", "16g", "--memory-swap", "16g",
                 "--mount", f"type=bind,src={REPO},dst=/src,readonly", "--mount", f"type=bind,src={build},dst=/build",
                 "--mount", f"type=bind,src={cargo},dst=/cargo", "-e", "HOME=/tmp/build-home", "-e", "CARGO_HOME=/cargo", "-e", "CARGO_TARGET_DIR=/build", "-w", "/src", builder,
                 "-c", "cargo build --locked -p sinomo-app-use-module && cargo build --locked -p sinomo-app-use-module --example android_native_probe"], stdout=log, stderr=subprocess.STDOUT)
            args.provider = build / "debug/libsinomo_app_use_module.so"
            args.probe = build / "debug/examples/android_native_probe"
    provider, probe = args.provider.resolve(), args.probe.resolve()
    name = "sinomo-wayland-" + uuid.uuid4().hex[:12]
    manifest = {"container": name, "scale": args.scale, "source_head": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                "provider": str(provider), "provider_sha256": digest(provider), "probe": str(probe), "probe_sha256": digest(probe),
                "checks": [], "passed": False}
    (output / "source.diff").write_bytes(subprocess.check_output(["git", "diff", "HEAD"]))
    untracked = subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "-z", "modules/desktop/app-use"]).decode().strip("\0").split("\0")
    with tarfile.open(output / "untracked-source.tar", "w") as archive:
        for path in filter(None, untracked):
            archive.add(path, arcname=path, recursive=False)
    try:
        run(["docker", "run", "-d", "--name", name, "--init", "--network", "none", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
             "--user", f"{os.getuid()}:{os.getgid()}", "--memory", "4g", "--pids-limit", "512", "--shm-size", "512m",
             "--device", "/dev/udmabuf", "--group-add", str(Path("/dev/udmabuf").stat().st_gid),
             "-e", "HOME=/tmp/test-home", "-e", f"SINOMO_TEST_SCALE={args.scale}", "--mount", f"type=bind,src={provider},dst=/artifacts/provider.so,readonly",
             "--mount", f"type=bind,src={probe},dst=/artifacts/probe,readonly", "--mount", f"type=bind,src={output},dst=/out", args.image], stdout=subprocess.DEVNULL)
        manifest["container_inspect"] = json.loads(subprocess.check_output(["docker", "inspect", name]))[0]
        manifest["packages"] = subprocess.check_output(["docker", "exec", name, "pacman", "-Q", "kwin", "xdg-desktop-portal", "xdg-desktop-portal-kde", "pipewire", "wireplumber", "mesa"], text=True)
        verify(name, output, manifest["checks"])
        manifest["passed"] = True
    except Exception as error:
        manifest["error"] = str(error)
        raise
    finally:
        with (output / "container.log").open("w") as log:
            subprocess.run(["docker", "logs", name], stdout=log, stderr=subprocess.STDOUT)
        cleanup_failed = False
        if not args.keep:
            cleanup = subprocess.run(["docker", "rm", "-f", name], capture_output=True, text=True)
            manifest["container_removed"] = cleanup.returncode == 0
            if cleanup.returncode != 0:
                manifest["passed"] = False
                manifest["cleanup_error"] = cleanup.stderr.strip()
                cleanup_failed = True
        else:
            manifest["container_retained"] = True
        (output / "result.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(output)
        if cleanup_failed:
            raise RuntimeError("test container cleanup failed; inspect result.json")


if __name__ == "__main__":
    main()
