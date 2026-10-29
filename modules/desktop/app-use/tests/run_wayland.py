#!/usr/bin/env python3
"""Exercise public portal D-Bus contracts and real PipeWire pixels in isolation.

Requires a selected PipeWire runtime/tools/plugins, a C build toolchain and
dbus-run-session. ABI headers are bundled. Does not start a compositor or approve
a real desktop grant.
"""
import argparse
import json
import os
import signal
from pathlib import Path
import subprocess
import tempfile
import threading
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pipewire-bin-dir", type=Path, default=Path("/usr/bin"))
    parser.add_argument("--output", type=Path, default=Path("target/app-use-wayland"))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    subprocess.run(["cargo", "build", "--locked", "-p", "sinomo-app-use-module", "--example", "wayland_video_fixture"], check=True)
    producer_binary = Path(os.environ.get("CARGO_TARGET_DIR", "target")) / "debug/examples/wayland_video_fixture"
    config = Path(__file__).with_name("wayland-pipewire.conf").resolve()
    with tempfile.TemporaryDirectory(prefix="sinomo-wayland-") as runtime:
        env = os.environ.copy()
        env["XDG_RUNTIME_DIR"] = runtime
        env.pop("PIPEWIRE_REMOTE", None)
        with open(args.output / "pipewire-fixture.log", "w") as log:
            daemon = subprocess.Popen([str(args.pipewire_bin_dir / "pipewire"), "-c", str(config)], env=env, stdout=log, stderr=log)
            stop = threading.Event()
            linker = None
            producer = None
            try:
                deadline = time.monotonic() + 10
                while True:
                    if daemon.poll() is not None:
                        raise RuntimeError("private PipeWire failed; inspect pipewire-fixture.log")
                    if Path(runtime, "pipewire-0").exists():
                        if producer is None:
                            producer = subprocess.Popen([str(producer_binary)], env=env, stdin=subprocess.PIPE, stdout=log, stderr=log)
                        result = subprocess.run([str(args.pipewire_bin_dir / "pw-dump")], env=env, capture_output=True, text=True, timeout=3)
                        if result.returncode == 0:
                            nodes = json.loads(result.stdout)
                            source = next((n for n in nodes if n.get("info", {}).get("props", {}).get("node.name") == "sinomo-wayland-test-source"), None)
                            if source is not None:
                                break
                    if time.monotonic() > deadline:
                        raise RuntimeError("private PipeWire source did not appear")
                    time.sleep(.05)

                def link_streams():
                    linked = set()
                    while not stop.wait(.05):
                        try:
                            inputs = subprocess.check_output([str(args.pipewire_bin_dir / "pw-link"), "-i"], env=env, text=True, stderr=subprocess.DEVNULL, timeout=2).splitlines()
                            for port in inputs:
                                if not port.startswith("sinomo-app-use-capture:") or port in linked:
                                    continue
                                outputs = subprocess.check_output([str(args.pipewire_bin_dir / "pw-link"), "-o"], env=env, text=True, timeout=2).splitlines()
                                source_port = next((p for p in outputs if p.startswith("sinomo-wayland-test-source:")), None)
                                if source_port is None:
                                    continue
                                result = subprocess.run([str(args.pipewire_bin_dir / "pw-link"), source_port, port], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2)
                                if result.returncode == 0:
                                    linked.add(port)
                        except (subprocess.SubprocessError, OSError):
                            # Native capture has its own bounded startup deadline.
                            pass

                linker = threading.Thread(target=link_streams)
                linker.start()
                test_env = env.copy()
                test_env.update(SINOMO_TEST_PRIVATE_PORTAL_BUS="1", SINOMO_TEST_PIPEWIRE_SOCKET=str(Path(runtime, "pipewire-0")),
                                SINOMO_TEST_PIPEWIRE_NODE=str(source["id"]), SINOMO_TEST_PIPEWIRE_SERIAL=str(source["info"]["props"]["object.serial"]),
                                PIPEWIRE_REMOTE="must-not-be-used-by-the-provider")
                result = subprocess.run(["dbus-run-session", "--", "cargo", "test", "--locked", "-p", "sinomo-app-use-module", "wayland_", "--", "--ignored", "--test-threads=1", "--nocapture"], env=test_env)
                return result.returncode
            finally:
                stop.set()
                if linker is not None:
                    linker.join(timeout=5)
                producer_status = 0
                if producer is not None:
                    producer.stdin.close()
                    try:
                        producer_status = producer.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        producer.kill()
                        producer.wait()
                        producer_status = -signal.SIGKILL
                daemon.terminate()
                try:
                    status = daemon.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    daemon.kill()
                    daemon.wait()
                    raise RuntimeError("private PipeWire did not shut down promptly")
                if status not in (0, -signal.SIGTERM):
                    raise RuntimeError(f"private PipeWire exited abnormally: {status}")
                if producer_status != 0:
                    raise RuntimeError(f"native fixture producer exited abnormally: {producer_status}")


if __name__ == "__main__":
    raise SystemExit(main())
