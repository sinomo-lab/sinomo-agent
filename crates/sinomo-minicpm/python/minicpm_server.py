#!/usr/bin/env python3
"""Warm MiniCPM-o HTTP server for cluster-local benchmark runs."""

from __future__ import annotations

import argparse
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from minicpm_bridge import (
    complete_loaded_model,
    load_model,
    mock_response,
    runtime_status,
)


class MiniCpmHandler(BaseHTTPRequestHandler):
    server: "MiniCpmHttpServer"

    def do_GET(self) -> None:
        if self.path != "/health":
            self.send_error(404, "not found")
            return
        self.write_json(self.server.status())

    def do_POST(self) -> None:
        if self.path != "/complete":
            self.send_error(404, "not found")
            return
        try:
            content_length = int(self.headers.get("Content-Length", "0"))
            request = json.loads(self.rfile.read(content_length))
            if self.server.mock:
                response = mock_response(request, self.server.model_id, self.server.device)
            else:
                response = complete_loaded_model(
                    self.server.model,
                    self.server.resolved_device,
                    request,
                    self.server.model_id,
                )
            self.write_json(response)
        except Exception as exc:  # pragma: no cover - host/model dependent
            self.write_json({"error": str(exc)}, status=500)

    def log_message(self, fmt: str, *args: Any) -> None:
        print(fmt % args, file=sys.stderr)

    def write_json(self, payload: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class MiniCpmHttpServer(ThreadingHTTPServer):
    def __init__(
        self,
        address: tuple[str, int],
        model_id: str,
        device: str,
        mode: str,
        mock: bool,
    ) -> None:
        self.model_id = model_id
        self.device = device
        self.mode = mode
        self.mock = mock
        if mock:
            self.model = None
            self.resolved_device = device
        else:
            self.model, self.resolved_device = load_model(model_id, device, mode)
        super().__init__(address, MiniCpmHandler)

    def status(self) -> dict[str, Any]:
        diagnostic = "mock runtime" if self.mock else "ready"
        return runtime_status(self.model_id, self.resolved_device, diagnostic)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=32153)
    parser.add_argument("--model", default=os.environ.get("SINOMO_MINICPM_MODEL", "openbmb/MiniCPM-o-4_5"))
    parser.add_argument("--device", choices=["auto", "cpu", "cuda"], default=os.environ.get("SINOMO_MINICPM_DEVICE", "auto"))
    parser.add_argument("--mode", choices=["text", "vision", "audio", "duplex"], default=os.environ.get("SINOMO_MINICPM_SERVER_MODE", "vision"))
    parser.add_argument("--mock", action="store_true", default=os.environ.get("SINOMO_MINICPM_MOCK") == "1")
    args = parser.parse_args()

    server = MiniCpmHttpServer((args.bind, args.port), args.model, args.device, args.mode, args.mock)
    print(
        json.dumps(
            {
                "listening": f"http://{args.bind}:{args.port}",
                "model": args.model,
                "device": server.resolved_device,
                "mode": args.mode,
                "mock": args.mock,
            }
        ),
        flush=True,
    )
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
