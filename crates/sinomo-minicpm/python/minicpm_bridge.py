#!/usr/bin/env python3
"""Process-isolated MiniCPM-o bridge.

The bridge reads one JSON request from stdin and writes one JSON response to
stdout. It intentionally keeps all PyTorch/Transformers imports on the Python
side so Rust builds do not depend on local model packages.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any


MIN_TRANSFORMERS_VERSION = (4, 53)
MAX_TRANSFORMERS_VERSION = (4, 54)


def parse_version_prefix(version: str) -> tuple[int, int]:
    parts = version.split(".", 2)
    if len(parts) < 2:
        raise ValueError(f"unsupported transformers version format: {version}")
    return int(parts[0]), int(parts[1])


def ensure_transformers_compatible(transformers_module: Any) -> None:
    version = parse_version_prefix(transformers_module.__version__)
    if version < MIN_TRANSFORMERS_VERSION or version >= MAX_TRANSFORMERS_VERSION:
        raise RuntimeError(
            "MiniCPM-o 4.5 trusted remote code requires transformers>=4.53,<4.54; "
            f"found transformers {transformers_module.__version__}"
        )


def runtime_status(model: str, device: str, diagnostic: str | None = None) -> dict[str, Any]:
    return {
        "model": model,
        "device": "cuda" if device == "cuda" else "cpu" if device == "cpu" else "auto",
        "backend": "pytorch-transformers",
        "local": True,
        "streaming_supported": True,
        "vision_supported": True,
        "audio_supported": True,
        "tts_supported": True,
        "diagnostic": diagnostic,
    }


def mock_response(request: dict[str, Any], model: str, device: str) -> dict[str, Any]:
    user_text = ""
    for message in request.get("messages", []):
        if message.get("role") == "user":
            user_text = message.get("content", "")
    mode = request.get("mode", "text")
    image = request.get("image_path") or request.get("image_name")
    suffix = f" image={Path(image).name}" if image else ""
    return {
        "content": f"[mock-minicpm:{mode}] {user_text}{suffix}".strip(),
        "runtime": runtime_status(model, device, "mock runtime"),
        "usage": None,
        "elapsed_ms": 0,
        "output_tokens_per_second": None,
    }


def load_model(model_id: str, device: str, mode: str):
    import torch
    import transformers
    from transformers import AutoModel

    ensure_transformers_compatible(transformers)
    use_cuda = device == "cuda" or (device == "auto" and torch.cuda.is_available())
    torch_dtype = torch.bfloat16 if use_cuda else torch.float32
    init_vision = mode in {"vision", "duplex"}
    init_audio = mode in {"audio", "duplex"}
    init_tts = mode in {"audio", "duplex"}
    model = AutoModel.from_pretrained(
        model_id,
        trust_remote_code=True,
        attn_implementation="sdpa",
        torch_dtype=torch_dtype,
        init_vision=init_vision,
        init_audio=init_audio,
        init_tts=init_tts,
    )
    model.eval()
    if use_cuda:
        model.cuda()
    return model, "cuda" if use_cuda else "cpu"


def request_image_path(request: dict[str, Any], temp_dir: str | None) -> str | None:
    image_base64 = request.get("image_base64")
    if image_base64:
        if temp_dir is None:
            raise ValueError("image_base64 requires a temporary directory")
        image_name = Path(request.get("image_name") or "image.png").name
        image_path = Path(temp_dir) / image_name
        image_path.write_bytes(base64.b64decode(image_base64))
        return str(image_path)
    return request.get("image_path")


def complete_loaded_model(
    model: Any,
    resolved_device: str,
    request: dict[str, Any],
    model_id: str,
) -> dict[str, Any]:
    from PIL import Image

    mode = request.get("mode", "text")
    started = time.perf_counter()
    with tempfile.TemporaryDirectory() as temp_dir:
        image_path = request_image_path(request, temp_dir)
        msgs: list[dict[str, Any]] = []
        system_prompt = request.get("system_prompt")
        if system_prompt:
            msgs.append({"role": "system", "content": system_prompt})
        for message in request.get("messages", []):
            role = message.get("role", "user")
            content: Any = message.get("content", "")
            if mode in {"vision", "duplex"} and role == "user" and image_path:
                image = Image.open(image_path).convert("RGB")
                content = [image, content]
            msgs.append({"role": role, "content": content})

        response = model.chat(msgs=msgs, use_tts_template=mode in {"audio", "duplex"})

    elapsed_ms = max(1, int((time.perf_counter() - started) * 1000))
    response_text = str(response)
    output_tokens = max(1, len(response_text.split()))
    return {
        "content": response_text,
        "runtime": runtime_status(model_id, resolved_device, None),
        "usage": {"output_tokens": output_tokens},
        "elapsed_ms": elapsed_ms,
        "output_tokens_per_second": output_tokens / (elapsed_ms / 1000.0),
    }


def run_request(request: dict[str, Any], model_id: str, device: str) -> dict[str, Any]:
    mode = request.get("mode", "text")
    if mode == "probe":
        try:
            import torch  # noqa: F401
            import transformers
            ensure_transformers_compatible(transformers)
            return {
                "content": "MiniCPM dependencies are importable.",
                "runtime": runtime_status(model_id, device, None),
                "usage": None,
            }
        except Exception as exc:  # pragma: no cover - host dependent
            return {
                "content": "MiniCPM dependencies are not importable.",
                "runtime": runtime_status(model_id, device, str(exc)),
                "usage": None,
            }

    model, resolved_device = load_model(model_id, device, mode)
    return complete_loaded_model(model, resolved_device, request, model_id)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", required=True)
    parser.add_argument("--device", choices=["auto", "cpu", "cuda"], default="auto")
    parser.add_argument("--timeout-seconds", type=int, default=300)
    args = parser.parse_args()

    request = json.load(sys.stdin)
    if os.environ.get("SINOMO_MINICPM_MOCK") == "1":
        json.dump(mock_response(request, args.model, args.device), sys.stdout)
        return 0
    try:
        json.dump(run_request(request, args.model, args.device), sys.stdout)
        return 0
    except Exception as exc:
        print(f"MiniCPM bridge failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
