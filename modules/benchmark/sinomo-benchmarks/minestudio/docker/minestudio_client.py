#!/usr/bin/env python3
"""Small CLI client for the Sinomo MineStudio HTTP wrapper."""

from __future__ import annotations

import argparse
import base64
import json
import urllib.request
from pathlib import Path
from typing import Any


def request(base_url: str, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        base_url.rstrip("/") + path,
        data=data,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    with urllib.request.urlopen(req, timeout=120) as response:
        return json.loads(response.read())


def write_frame(result: dict[str, Any], output: str | None) -> None:
    if not output:
        return
    frame = result.get("frame") or {}
    data = frame.get("data")
    if not data:
        return
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(base64.b64decode(data))


def print_without_frame(result: dict[str, Any]) -> None:
    value = dict(result)
    if isinstance(value.get("frame"), dict):
        value["frame"] = {key: item for key, item in value["frame"].items() if key != "data"}
    print(json.dumps(value, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:18080")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("health")
    reset = sub.add_parser("reset")
    reset.add_argument("--output")
    observe = sub.add_parser("observe")
    observe.add_argument("--output")
    step = sub.add_parser("step")
    step.add_argument("--ticks", type=int, default=1)
    step.add_argument("--macro-json")
    step.add_argument("--action-json")
    step.add_argument("--output")
    sub.add_parser("close")
    args = parser.parse_args()

    if args.command == "health":
        result = request(args.url, "GET", "/health")
    elif args.command == "reset":
        result = request(args.url, "POST", "/reset", {})
        write_frame(result, args.output)
    elif args.command == "observe":
        result = request(args.url, "GET", "/observe")
        write_frame(result, args.output)
    elif args.command == "step":
        payload: dict[str, Any] = {"ticks": args.ticks}
        if args.macro_json:
            payload["macro"] = json.loads(args.macro_json)
        if args.action_json:
            payload["action"] = json.loads(args.action_json)
        result = request(args.url, "POST", "/step", payload)
        write_frame(result, args.output)
    elif args.command == "close":
        result = request(args.url, "POST", "/close", {})
    else:
        raise AssertionError(args.command)
    print_without_frame(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
