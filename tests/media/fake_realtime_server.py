#!/usr/bin/env python3
"""Dependency-free fake MiniCPM/OpenAI-style realtime WebSocket server.

The native media E2E test uses this server as the model endpoint. It accepts the
subset of realtime events Sinomo sends, records audio/video counts to a summary
file, then emits a short synthetic audio response once required input arrives.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
import signal
import struct
import sys
import time
from dataclasses import dataclass, field
from typing import Any


WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


@dataclass
class Summary:
    path: str
    audio_chunks: int = 0
    audio_bytes: int = 0
    video_frames: int = 0
    video_bytes: int = 0
    session_updates: int = 0
    response_create: int = 0
    errors: list[str] = field(default_factory=list)

    def write(self) -> None:
        tmp = f"{self.path}.tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(
                {
                    "audio_chunks": self.audio_chunks,
                    "audio_bytes": self.audio_bytes,
                    "video_frames": self.video_frames,
                    "video_bytes": self.video_bytes,
                    "session_updates": self.session_updates,
                    "response_create": self.response_create,
                    "errors": self.errors,
                },
                handle,
                sort_keys=True,
            )
            handle.write("\n")
        os.replace(tmp, self.path)


async def read_http_request(reader: asyncio.StreamReader) -> dict[str, str]:
    data = await reader.readuntil(b"\r\n\r\n")
    lines = data.decode("latin1").split("\r\n")
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        headers[key.strip().lower()] = value.strip()
    return headers


async def websocket_handshake(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    headers = await read_http_request(reader)
    key = headers.get("sec-websocket-key")
    if not key:
        raise RuntimeError("missing Sec-WebSocket-Key")
    accept = base64.b64encode(hashlib.sha1((key + WS_GUID).encode("ascii")).digest()).decode(
        "ascii"
    )
    writer.write(
        (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {accept}\r\n"
            "\r\n"
        ).encode("ascii")
    )
    await writer.drain()


async def read_ws_frame(reader: asyncio.StreamReader) -> tuple[int, bytes]:
    first, second = await reader.readexactly(2)
    opcode = first & 0x0F
    masked = bool(second & 0x80)
    length = second & 0x7F
    if length == 126:
        length = struct.unpack("!H", await reader.readexactly(2))[0]
    elif length == 127:
        length = struct.unpack("!Q", await reader.readexactly(8))[0]
    mask = await reader.readexactly(4) if masked else b""
    payload = await reader.readexactly(length) if length else b""
    if masked:
        payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    return opcode, payload


async def send_ws_text(writer: asyncio.StreamWriter, value: dict[str, Any]) -> None:
    payload = json.dumps(value, separators=(",", ":")).encode("utf-8")
    writer.write(encode_ws_frame(0x1, payload))
    await writer.drain()


async def send_ws_close(writer: asyncio.StreamWriter) -> None:
    writer.write(encode_ws_frame(0x8, b""))
    await writer.drain()


def encode_ws_frame(opcode: int, payload: bytes) -> bytes:
    header = bytearray([0x80 | opcode])
    length = len(payload)
    if length < 126:
        header.append(length)
    elif length <= 0xFFFF:
        header.append(126)
        header.extend(struct.pack("!H", length))
    else:
        header.append(127)
        header.extend(struct.pack("!Q", length))
    header.extend(payload)
    return bytes(header)


def decode_b64_field(value: dict[str, Any], field: str) -> bytes:
    raw = value.get(field)
    if not isinstance(raw, str) or not raw:
        return b""
    try:
        return base64.b64decode(raw, validate=False)
    except Exception:
        return b""


def synthetic_f32_audio(sample_count: int = 480) -> bytes:
    out = bytearray()
    for _ in range(sample_count):
        out.extend(struct.pack("<f", 0.0))
    return bytes(out)


async def delayed_response(
    writer: asyncio.StreamWriter, delay_ms: int, closed: asyncio.Event
) -> None:
    await asyncio.sleep(max(delay_ms, 0) / 1000.0)
    audio = base64.b64encode(synthetic_f32_audio()).decode("ascii")
    await send_ws_text(
        writer,
        {
            "type": "response.output_audio.delta",
            "audio": audio,
            "text": "synthetic response",
            "end_of_turn": True,
        },
    )
    await send_ws_text(
        writer,
        {
            "type": "response.output_audio_transcript.delta",
            "delta": "synthetic response",
        },
    )
    await send_ws_text(writer, {"type": "response.done"})
    await send_ws_close(writer)
    closed.set()


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    args: argparse.Namespace,
    summary: Summary,
) -> None:
    response_task: asyncio.Task[None] | None = None
    closed = asyncio.Event()
    try:
        await websocket_handshake(reader, writer)
        await send_ws_text(writer, {"type": "session.queue_done"})
        while not closed.is_set():
            opcode, payload = await read_ws_frame(reader)
            if opcode == 0x8:
                break
            if opcode != 0x1:
                continue
            try:
                value = json.loads(payload.decode("utf-8"))
            except Exception as exc:
                summary.errors.append(f"json: {exc}")
                summary.write()
                continue
            event_type = value.get("type", "")
            if event_type == "session.update":
                summary.session_updates += 1
                summary.write()
                await send_ws_text(
                    writer,
                    {"type": "session.created", "session": {"id": "fake-realtime-session"}},
                )
            elif event_type == "input_audio_buffer.append":
                summary.audio_chunks += 1
                summary.audio_bytes += len(decode_b64_field(value, "audio"))
                summary.write()
            elif event_type == "input_image_buffer.append":
                summary.video_frames += 1
                summary.video_bytes += len(decode_b64_field(value, "image"))
                summary.write()
            elif event_type == "response.create":
                summary.response_create += 1
                summary.write()

            if (
                response_task is None
                and summary.audio_chunks >= args.min_audio_chunks
                and summary.video_frames >= args.min_video_frames
            ):
                response_task = asyncio.create_task(
                    delayed_response(writer, args.response_delay_ms, closed)
                )
    except asyncio.IncompleteReadError:
        pass
    except Exception as exc:
        summary.errors.append(str(exc))
        summary.write()
    finally:
        if response_task is not None and not response_task.done():
            response_task.cancel()
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        summary.write()


async def main_async(args: argparse.Namespace) -> int:
    summary = Summary(args.summary)
    summary.write()
    done = asyncio.Event()

    async def client_done(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await handle_client(reader, writer, args, summary)
        done.set()

    server = await asyncio.start_server(client_done, args.host, args.port)
    sockets = server.sockets or []
    port = sockets[0].getsockname()[1]
    ready = {
        "host": args.host,
        "port": port,
        "websocket_url": f"ws://{args.host}:{port}/realtime",
        "summary": args.summary,
    }
    print(json.dumps(ready, sort_keys=True), flush=True)

    loop = asyncio.get_running_loop()
    for signame in ("SIGTERM", "SIGINT"):
        signum = getattr(signal, signame, None)
        if signum is not None:
            try:
                loop.add_signal_handler(signum, done.set)
            except NotImplementedError:
                pass

    async with server:
        try:
            await asyncio.wait_for(done.wait(), timeout=args.timeout_s)
        except asyncio.TimeoutError:
            summary.errors.append("server timeout")
            summary.write()
            return 2
        finally:
            server.close()
            await server.wait_closed()
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--summary", required=True)
    parser.add_argument("--timeout-s", type=float, default=30.0)
    parser.add_argument("--response-delay-ms", type=int, default=1000)
    parser.add_argument("--min-audio-chunks", type=int, default=1)
    parser.add_argument("--min-video-frames", type=int, default=1)
    return parser.parse_args()


def main() -> int:
    return asyncio.run(main_async(parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
