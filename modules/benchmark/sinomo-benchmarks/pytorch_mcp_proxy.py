#!/usr/bin/env python3
"""Proxy PyTorch model HTTP requests through the Windows MCP PowerShell channel.

This is a deployment workaround for Windows hosts where the model server can
bind locally but the Windows firewall blocks direct inbound benchmark traffic.
Local Docker benchmark containers can call this proxy on the Linux host; the
proxy forwards to the Windows GPU node's localhost PyTorch model server over MCP.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


def mcp_call(mcp_url: str, session_id: str, command: str, timeout: int) -> str:
    payload = {
        "jsonrpc": "2.0",
        "id": "pytorch-model-proxy",
        "method": "tools/call",
        "params": {
            "name": "PowerShell",
            "arguments": {"command": command, "timeout": timeout},
        },
    }
    request = urllib.request.Request(
        mcp_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
            "mcp-session-id": session_id,
        },
        method="POST",
    )
    data_line = None
    with urllib.request.urlopen(request, timeout=timeout + 10) as response:
        while True:
            raw_line = response.readline()
            if not raw_line:
                break
            line = raw_line.decode("utf-8").strip()
            if line.startswith("data: "):
                data_line = line.removeprefix("data: ").strip()
                break
    if data_line is None:
        raise RuntimeError("MCP response did not include a data event")
    data = json.loads(data_line)
    if data.get("error"):
        raise RuntimeError(json.dumps(data["error"], ensure_ascii=False))
    result = data["result"].get("structuredContent", {}).get("result")
    if result is None:
        result = data["result"]["content"][0]["text"]
    if result.startswith("Response: "):
        result = result.removeprefix("Response: ")
    marker = "\n\nStatus Code:"
    if marker in result:
        result = result.split(marker, 1)[0]
    result = result.strip()
    if result.startswith("#< CLIXML") or "<Objs " in result:
        raise RuntimeError(result[:1000])
    try:
        json.loads(result)
    except json.JSONDecodeError as source:
        raise RuntimeError(f"PyTorch model MCP proxy returned non-JSON response: {source}: {result[:1000]}")
    return result


class ProxyHandler(BaseHTTPRequestHandler):
    server: "PytorchModelMcpProxy"

    def do_GET(self) -> None:
        if self.path != "/health":
            self.send_error(404, "not found")
            return
        command = (
            "$r = Invoke-WebRequest -UseBasicParsing -TimeoutSec 30 "
            f"-Uri '{self.server.target}/health'; $r.Content"
        )
        self.forward_mcp_json(command, 40)

    def do_POST(self) -> None:
        if self.path != "/complete":
            self.send_error(404, "not found")
            return
        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length)
        encoded = base64.b64encode(body).decode("ascii")
        command = (
            f"$body = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('{encoded}')); "
            "$r = Invoke-WebRequest -UseBasicParsing -TimeoutSec 300 "
            f"-Uri '{self.server.target}/complete' -Method Post "
            "-ContentType 'application/json' -Body $body; $r.Content"
        )
        self.forward_mcp_json(command, 320)

    def log_message(self, fmt: str, *args: Any) -> None:
        if self.server.verbose:
            super().log_message(fmt, *args)

    def write_json_text(self, text: str) -> None:
        body = text.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def forward_mcp_json(self, command: str, timeout: int) -> None:
        try:
            self.write_json_text(
                mcp_call(self.server.mcp_url, self.server.session_id, command, timeout)
            )
        except Exception as source:
            body = json.dumps({"error": str(source)}).encode("utf-8")
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)


class PytorchModelMcpProxy(ThreadingHTTPServer):
    def __init__(
        self,
        address: tuple[str, int],
        mcp_url: str,
        session_id: str,
        target: str,
        verbose: bool,
    ) -> None:
        self.mcp_url = mcp_url
        self.session_id = session_id
        self.target = target.rstrip("/")
        self.verbose = verbose
        super().__init__(address, ProxyHandler)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=32154)
    parser.add_argument("--mcp-url", default=os.environ.get("SINOMO_WINDOWS_MCP_URL", "http://127.0.0.1:39999/mcp"))
    parser.add_argument("--mcp-session", default=os.environ.get("SINOMO_WINDOWS_MCP_SESSION"))
    parser.add_argument("--target", default=os.environ.get("SINOMO_WINDOWS_PYTORCH_TARGET", os.environ.get("SINOMO_WINDOWS_MINICPM_TARGET", "http://127.0.0.1:32153")))
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    if not args.mcp_session:
        parser.error("--mcp-session or SINOMO_WINDOWS_MCP_SESSION is required")

    server = PytorchModelMcpProxy(
        (args.bind, args.port),
        args.mcp_url,
        args.mcp_session,
        args.target,
        args.verbose,
    )
    print(json.dumps({"listening": f"http://{args.bind}:{args.port}", "target": args.target}), flush=True)
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
