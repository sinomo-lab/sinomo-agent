#!/usr/bin/env python3
"""Render a raw Google pprof profile as a self-contained flamegraph HTML file.

The helper uses Go's bundled pprof web renderer. It does not start a browser,
does not require Graphviz, and streams the generated page to disk.
"""

from __future__ import annotations

import argparse
import contextlib
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from typing import BinaryIO
import urllib.error
import urllib.request


MAX_INPUT_BYTES = 32 * 1024 * 1024
MAX_OUTPUT_BYTES = 128 * 1024 * 1024
STARTUP_TIMEOUT_SECONDS = 10.0
HTTP_CHUNK_BYTES = 64 * 1024
LOG_TAIL_BYTES = 16 * 1024


class RenderError(RuntimeError):
    """A user-facing profile rendering failure."""


def _go_binary() -> str:
    go = shutil.which("go")
    if go is None:
        raise RenderError("Go is required, but no 'go' executable was found in PATH")
    return go


def _reserve_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _read_log_tail(log_path: Path) -> str:
    try:
        with log_path.open("rb") as stream:
            stream.seek(0, os.SEEK_END)
            size = stream.tell()
            stream.seek(max(0, size - LOG_TAIL_BYTES))
            return stream.read().decode("utf-8", errors="replace").strip()
    except OSError:
        return ""


def _stop_process(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=3.0)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=3.0)


def _copy_flamegraph_response(response: BinaryIO, output: Path) -> int:
    markers = {
        b"<!doctype html": False,
        b"flamegraph": False,
        b"<script": False,
        b"stack": False,
    }
    overlap = b""
    written = 0
    output.parent.mkdir(parents=True, exist_ok=True)

    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", prefix=f".{output.name}.", suffix=".tmp", dir=output.parent, delete=False
        ) as destination:
            temporary_name = destination.name
            while True:
                chunk = response.read(HTTP_CHUNK_BYTES)
                if not chunk:
                    break
                written += len(chunk)
                if written > MAX_OUTPUT_BYTES:
                    raise RenderError(
                        f"rendered HTML exceeds the {MAX_OUTPUT_BYTES // (1024 * 1024)} MiB safety limit"
                    )
                destination.write(chunk)
                searchable = (overlap + chunk).lower()
                for marker in markers:
                    if marker in searchable:
                        markers[marker] = True
                overlap = searchable[-64:]
            destination.flush()
            os.fsync(destination.fileno())

        missing = [marker.decode("ascii") for marker, found in markers.items() if not found]
        if missing:
            raise RenderError(
                "pprof returned HTML without expected flamegraph markers: " + ", ".join(missing)
            )
        os.replace(temporary_name, output)
        temporary_name = None
        return written
    finally:
        if temporary_name is not None:
            with contextlib.suppress(OSError):
                os.unlink(temporary_name)


def render_profile(profile: Path, output: Path) -> int:
    profile = profile.expanduser().resolve()
    output = output.expanduser().resolve()
    if not profile.is_file():
        raise RenderError(f"profile is not a regular file: {profile}")
    if profile == output:
        raise RenderError("profile and output paths must be different")

    size = profile.stat().st_size
    if size == 0:
        raise RenderError(f"profile is empty: {profile}")
    if size > MAX_INPUT_BYTES:
        raise RenderError(
            f"profile is {size} bytes; the renderer limit is {MAX_INPUT_BYTES} bytes (32 MiB)"
        )

    go = _go_binary()
    port = _reserve_loopback_port()
    url = f"http://127.0.0.1:{port}/ui/flamegraph"

    with tempfile.TemporaryDirectory(prefix="sinomo-pprof-render-") as temporary_dir:
        temporary_path = Path(temporary_dir)
        log_path = temporary_path / "pprof.log"
        environment = os.environ.copy()
        environment["PPROF_TMPDIR"] = str(temporary_path / "pprof-tmp")
        environment.setdefault("GOMEMLIMIT", "256MiB")
        environment.setdefault("GOMAXPROCS", "1")
        Path(environment["PPROF_TMPDIR"]).mkdir()

        command = [
            go,
            "tool",
            "pprof",
            f"-http=127.0.0.1:{port}",
            "-no_browser",
            "-sample_index=cpu",
            str(profile),
        ]

        process: subprocess.Popen[bytes] | None = None
        error: BaseException | None = None
        try:
            with log_path.open("wb") as log:
                process = subprocess.Popen(
                    command,
                    stdin=subprocess.DEVNULL,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    env=environment,
                )

                deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
                last_http_error = "server did not accept a connection"
                while True:
                    if process.poll() is not None:
                        raise RenderError(f"go tool pprof exited with status {process.returncode}")
                    try:
                        with urllib.request.urlopen(url, timeout=1.0) as response:
                            content_type = response.headers.get_content_type()
                            if content_type != "text/html":
                                raise RenderError(
                                    f"pprof returned unexpected content type {content_type!r}"
                                )
                            return _copy_flamegraph_response(response, output)
                    except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
                        last_http_error = str(exc)
                    if time.monotonic() >= deadline:
                        raise RenderError(
                            f"timed out waiting for pprof flamegraph endpoint: {last_http_error}"
                        )
                    time.sleep(0.05)
        except BaseException as exc:
            error = exc
            raise
        finally:
            if process is not None:
                _stop_process(process)
            if error is not None:
                log_tail = _read_log_tail(log_path)
                if log_tail and isinstance(error, RenderError):
                    error.args = (f"{error}\npprof log:\n{log_tail}",)


def _varint(value: int) -> bytes:
    if value < 0:
        value &= (1 << 64) - 1
    encoded = bytearray()
    while value >= 0x80:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def _field_varint(number: int, value: int) -> bytes:
    return _varint(number << 3) + _varint(value)


def _field_bytes(number: int, value: bytes) -> bytes:
    return _varint((number << 3) | 2) + _varint(len(value)) + value


def _value_type(type_index: int, unit_index: int) -> bytes:
    return _field_varint(1, type_index) + _field_varint(2, unit_index)


def _function(identifier: int, name_index: int, filename_index: int) -> bytes:
    return b"".join(
        (
            _field_varint(1, identifier),
            _field_varint(2, name_index),
            _field_varint(3, name_index),
            _field_varint(4, filename_index),
            _field_varint(5, 1),
        )
    )


def _location(identifier: int, function_id: int, line: int) -> bytes:
    line_message = _field_varint(1, function_id) + _field_varint(2, line)
    return _field_varint(1, identifier) + _field_bytes(4, line_message)


def _synthetic_profile() -> bytes:
    strings = [
        "",
        "samples",
        "count",
        "cpu",
        "nanoseconds",
        "thread",
        "main",
        "root",
        "work",
        "fixture.rs",
    ]
    label = _field_varint(1, 5) + _field_varint(2, 6)
    sample = b"".join(
        (
            _field_bytes(1, _varint(2) + _varint(1)),
            _field_bytes(2, _varint(10) + _varint(100_000_000)),
            _field_bytes(3, label),
        )
    )
    fields = [
        _field_bytes(1, _value_type(1, 2)),
        _field_bytes(1, _value_type(3, 4)),
        _field_bytes(2, sample),
        _field_bytes(4, _location(1, 1, 10)),
        _field_bytes(4, _location(2, 2, 20)),
        _field_bytes(5, _function(1, 7, 9)),
        _field_bytes(5, _function(2, 8, 9)),
    ]
    fields.extend(_field_bytes(6, value.encode("utf-8")) for value in strings)
    fields.extend(
        (
            _field_varint(9, 1_700_000_000_000_000_000),
            _field_varint(10, 1_000_000_000),
            _field_bytes(11, _value_type(3, 4)),
            _field_varint(12, 10_000_000),
        )
    )
    return b"".join(fields)


def run_self_test() -> None:
    go = _go_binary()
    with tempfile.TemporaryDirectory(prefix="sinomo-pprof-render-test-") as temporary_dir:
        directory = Path(temporary_dir)
        profile = directory / "cpu.pprof.pb"
        output = directory / "cpu.flamegraph.html"
        profile.write_bytes(_synthetic_profile())

        parsed = subprocess.run(
            [go, "tool", "pprof", "-raw", "-sample_index=cpu", str(profile)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
            text=True,
            timeout=10.0,
        )
        if parsed.returncode != 0:
            raise RenderError(
                f"go tool pprof could not parse the synthetic profile:\n{parsed.stdout.strip()}"
            )
        if "cpu/nanoseconds" not in parsed.stdout or "work" not in parsed.stdout:
            raise RenderError(
                "go tool pprof parsed the fixture but did not expose its CPU sample and work frame"
            )

        written = render_profile(profile, output)
        if output.stat().st_size != written:
            raise RenderError("rendered output size did not match streamed byte count")
        print(
            "self-test passed: raw prost-compatible pprof parsed and rendered "
            f"to self-contained flamegraph HTML ({written} bytes)"
        )


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", nargs="?", type=Path, help="raw cpu.pprof.pb artifact")
    parser.add_argument("-o", "--output", type=Path, help="output flamegraph HTML path")
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="validate Go pprof parsing and rendering with an in-memory fixture",
    )
    args = parser.parse_args(argv)
    if args.self_test:
        if args.profile is not None or args.output is not None:
            parser.error("--self-test does not accept a profile or output path")
    elif args.profile is None or args.output is None:
        parser.error("profile and --output are required unless --self-test is used")
    return args


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    try:
        if args.self_test:
            run_self_test()
        else:
            written = render_profile(args.profile, args.output)
            print(f"rendered {args.output} ({written} bytes)")
        return 0
    except (OSError, RenderError, subprocess.SubprocessError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
