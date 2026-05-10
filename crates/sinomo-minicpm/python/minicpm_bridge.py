#!/usr/bin/env python3
"""Process-isolated MiniCPM-o bridge.

The bridge reads one JSON request from stdin and writes one JSON response to
stdout. It intentionally keeps all PyTorch/Transformers imports on the Python
side so Rust builds do not depend on local model packages.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any


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
    image = request.get("image_path")
    suffix = f" image={Path(image).name}" if image else ""
    return {
        "content": f"[mock-minicpm:{mode}] {user_text}{suffix}".strip(),
        "runtime": runtime_status(model, device, "mock runtime"),
        "usage": None,
    }


def load_model(model_id: str, device: str, mode: str):
    import torch
    from transformers import AutoModel

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


def run_request(request: dict[str, Any], model_id: str, device: str) -> dict[str, Any]:
    mode = request.get("mode", "text")
    if mode == "probe":
        try:
            import torch  # noqa: F401
            import transformers  # noqa: F401
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
    from PIL import Image

    msgs: list[dict[str, Any]] = []
    system_prompt = request.get("system_prompt")
    if system_prompt:
        msgs.append({"role": "system", "content": system_prompt})
    for message in request.get("messages", []):
        role = message.get("role", "user")
        content: Any = message.get("content", "")
        if mode in {"vision", "duplex"} and role == "user" and request.get("image_path"):
            image = Image.open(request["image_path"]).convert("RGB")
            content = [image, content]
        msgs.append({"role": role, "content": content})

    response = model.chat(msgs=msgs, use_tts_template=mode in {"audio", "duplex"})
    return {
        "content": str(response),
        "runtime": runtime_status(model_id, resolved_device, None),
        "usage": None,
    }


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
