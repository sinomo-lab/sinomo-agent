#!/usr/bin/env python3
"""HTTP control wrapper for a real MineStudio MinecraftSim environment."""

from __future__ import annotations

import argparse
import base64
import io
import os
import re
import time
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from PIL import Image

from minestudio_action_bridge import MineStudioActionBridge, MineStudioActionMacro, json_safe


class ResetRequest(BaseModel):
    obs_width: int | None = None
    obs_height: int | None = None
    render_width: int | None = None
    render_height: int | None = None
    init_commands: list[str] = Field(default_factory=list)
    record_video: bool | None = None
    record_path: str | None = None
    record_fps: int | None = None
    record_frame_type: str | None = None
    recreate: bool = False


class StepRequest(BaseModel):
    action: dict[str, Any] | None = None
    macro: dict[str, Any] | None = None
    ticks: int = Field(default=1, ge=1, le=200)


class InspectRequest(BaseModel):
    commands: list[str] = Field(default_factory=list)


def compact_value(value: Any, depth: int = 0) -> Any:
    if depth > 4:
        return str(type(value).__name__)
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            if str(key) in {"pov", "image"}:
                result[str(key)] = compact_frame_meta(item)
            else:
                result[str(key)] = compact_value(item, depth + 1)
        return result
    if isinstance(value, (list, tuple)):
        if len(value) > 20:
            return {
                "type": type(value).__name__,
                "len": len(value),
                "sample": [compact_value(item, depth + 1) for item in list(value)[:5]],
            }
        return [compact_value(item, depth + 1) for item in value]
    if hasattr(value, "shape"):
        if list(value.shape) == [] and hasattr(value, "item"):
            try:
                return value.item()
            except Exception:
                pass
        return {
            "type": type(value).__name__,
            "shape": list(value.shape),
            "dtype": str(getattr(value, "dtype", "")),
        }
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def compact_frame_meta(value: Any) -> dict[str, Any]:
    try:
        image = as_pil_image(value)
        return {"type": "frame", "width": image.width, "height": image.height}
    except Exception:
        if hasattr(value, "shape"):
            return {"type": type(value).__name__, "shape": list(value.shape)}
        return {"type": type(value).__name__}


def as_pil_image(value: Any) -> Image.Image:
    if isinstance(value, Image.Image):
        return value.convert("RGB")
    if isinstance(value, bytes):
        return Image.open(io.BytesIO(value)).convert("RGB")
    if hasattr(value, "shape"):
        return Image.fromarray(value).convert("RGB")
    raise TypeError(f"unsupported frame type: {type(value)!r}")


def encode_png(value: Any) -> dict[str, Any]:
    image = as_pil_image(value)
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    data = buffer.getvalue()
    return {
        "encoding": "base64",
        "format": "png",
        "width": image.width,
        "height": image.height,
        "bytes": len(data),
        "data": base64.b64encode(data).decode("ascii"),
    }


def pick_frame(obs: Any, info: dict[str, Any]) -> Any:
    if isinstance(info, dict) and info.get("pov") is not None:
        return info["pov"]
    if isinstance(obs, dict) and obs.get("image") is not None:
        return obs["image"]
    if isinstance(obs, dict) and obs.get("pov") is not None:
        return obs["pov"]
    return obs


class MineStudioService:
    def __init__(self) -> None:
        self.env: Any | None = None
        self.obs: Any | None = None
        self.info: dict[str, Any] = {}
        self.last_reward: float | None = None
        self.terminated = False
        self.truncated = False
        self.step_index = 0
        self.created_at = time.time()
        self.bridge = MineStudioActionBridge()
        self.record_path: str | None = None

    def create_env(self, req: ResetRequest | None = None) -> None:
        from minestudio.simulator import entry as minestudio_entry
        from minestudio.simulator.callbacks import CommandsCallback, RecordCallback

        minestudio_entry.check_engine(skip_confirmation=True)
        MinecraftSim = minestudio_entry.MinecraftSim

        req = req or ResetRequest()
        obs_width = req.obs_width or int(os.environ.get("MINESTUDIO_OBS_WIDTH", "128"))
        obs_height = req.obs_height or int(os.environ.get("MINESTUDIO_OBS_HEIGHT", "128"))
        render_width = req.render_width or int(os.environ.get("MINESTUDIO_RENDER_WIDTH", "640"))
        render_height = req.render_height or int(os.environ.get("MINESTUDIO_RENDER_HEIGHT", "360"))
        record_video = req.record_video
        if record_video is None:
            record_video = os.environ.get("MINESTUDIO_RECORD_VIDEO", "0") in {"1", "true", "yes", "on"}
        callbacks = []
        self.record_path = None
        if record_video:
            self.record_path = req.record_path or os.environ.get("MINESTUDIO_RECORD_PATH", "/workspace/benchmark-results/minestudio-recording")
            callbacks.append(
                RecordCallback(
                    record_path=self.record_path,
                    fps=req.record_fps or int(os.environ.get("MINESTUDIO_RECORD_FPS", "20")),
                    frame_type=req.record_frame_type or os.environ.get("MINESTUDIO_RECORD_FRAME_TYPE", "pov"),
                )
            )
        if req.init_commands:
            callbacks.append(CommandsCallback(req.init_commands))
        self.close()
        self.env = MinecraftSim(
            action_type="env",
            obs_size=(obs_width, obs_height),
            render_size=(render_width, render_height),
            callbacks=callbacks,
        )

    def ensure_env(self) -> None:
        if self.env is None:
            self.create_env()

    def reset(self, req: ResetRequest | None = None) -> dict[str, Any]:
        req = req or ResetRequest()
        if req.recreate or self.env is None:
            self.create_env(req)
        assert self.env is not None
        self.obs, self.info = self.env.reset()
        self.last_reward = None
        self.terminated = False
        self.truncated = False
        self.step_index = 0
        return self.snapshot()

    def snapshot(self) -> dict[str, Any]:
        if self.obs is None:
            raise HTTPException(status_code=409, detail="environment has not been reset")
        frame = pick_frame(self.obs, self.info)
        return {
            "ok": True,
            "step_index": self.step_index,
            "reward": self.last_reward,
            "terminated": self.terminated,
            "truncated": self.truncated,
            "frame": encode_png(frame),
            "info_keys": sorted(str(key) for key in self.info.keys()) if isinstance(self.info, dict) else [],
        }

    def inspect(self, req: InspectRequest) -> dict[str, Any]:
        if self.obs is None:
            raise HTTPException(status_code=409, detail="environment has not been reset")
        command_results = []
        for command in req.commands:
            try:
                if self.env is None or not hasattr(self.env, "env") or not hasattr(self.env.env, "execute_cmd"):
                    raise RuntimeError("MineStudio env does not expose env.execute_cmd")
                obs, reward, done, info = self.env.env.execute_cmd(command)
                command_results.append(
                    {
                        "command": command,
                        "ok": True,
                        "reward": json_safe(reward),
                        "done": bool(done),
                        "obs": compact_value(obs),
                        "info": compact_value(info),
                    }
                )
            except Exception as source:
                command_results.append({"command": command, "ok": False, "error": str(source)})
        return {
            "ok": True,
            "step_index": self.step_index,
            "reward": self.last_reward,
            "terminated": self.terminated,
            "truncated": self.truncated,
            "obs": compact_value(self.obs),
            "info": compact_value(self.info),
            "commands": command_results,
        }

    def step(self, req: StepRequest) -> dict[str, Any]:
        self.ensure_env()
        if self.obs is None:
            self.reset()
        assert self.env is not None
        if req.macro is not None:
            macro = MineStudioActionMacro.from_dict(req.macro)
            macro.duration_ticks = min(max(1, macro.duration_ticks), req.ticks)
            command_meta = self.try_macro_command(macro)
            if command_meta is not None and command_meta.get("command_ok"):
                snapshot = self.snapshot()
                snapshot["applied"] = json_safe({"macro": macro.__dict__, **command_meta})
                return snapshot
            action = self.bridge.to_env_action(self.env, macro)
            action_meta = {"macro": macro.__dict__, "action": action}
            if command_meta is not None:
                action_meta["command_attempt"] = command_meta
            ticks = macro.duration_ticks
        elif req.action is not None:
            action = req.action
            action_meta = {"action": action}
            ticks = req.ticks
        else:
            action = self.bridge._no_op_action(self.env) or {}
            action_meta = {"action": action, "no_op": True}
            ticks = req.ticks

        for _ in range(ticks):
            self.obs, self.last_reward, self.terminated, self.truncated, self.info = self.env.step(action)
            self.step_index += 1
            if self.terminated or self.truncated:
                break
        snapshot = self.snapshot()
        snapshot["applied"] = json_safe(action_meta)
        return snapshot

    def try_macro_command(self, macro: MineStudioActionMacro) -> dict[str, Any] | None:
        if self.env is None or not hasattr(self.env, "env") or not hasattr(self.env.env, "execute_cmd"):
            return None
        target = self.normalize_item_target(macro.target)
        command = None
        if macro.verb in {"select", "equip"} and target:
            command = f"equip {target}"
        elif macro.verb == "craft" and target:
            command = f"craft {target}"
        elif macro.verb == "place" and target:
            command = f"place {target}"
        elif macro.verb == "use" and target:
            equip_meta = self.execute_command(f"equip {target}")
            if equip_meta.get("command_ok"):
                return {"prepare_command": equip_meta["command"], "prepare_command_ok": True}
            return {
                "prepare_command": equip_meta["command"],
                "prepare_command_ok": False,
                "prepare_command_error": equip_meta.get("command_error"),
            }
        elif macro.verb == "open" and target in {"crafting_table", "furnace", "chest"}:
            command = f"place {target}" if target == "crafting_table" else None
        if command is None:
            return None
        return self.execute_command(command)

    def execute_command(self, command: str) -> dict[str, Any]:
        try:
            self.obs, self.last_reward, done, self.info = self.env.env.execute_cmd(command)
            self.terminated = bool(done)
            self.truncated = False
            self.step_index += 1
            return {"command": command, "command_ok": True}
        except Exception as source:
            return {"command": command, "command_ok": False, "command_error": str(source)}

    def normalize_item_target(self, target: str) -> str | None:
        normalized = target.lower().strip()
        if not normalized or normalized in {"inventory", "task-relevant target", "requested target", "selected task-relevant item"}:
            return None
        normalized = normalized.removeprefix("minecraft:")
        normalized = re.sub(r"[^a-z0-9_]+", "_", normalized).strip("_")
        aliases = {
            "crafting_table": "crafting_table",
            "oak_planks": "planks OAK",
            "wooden_planks": "planks",
            "dirt_block": "dirt",
        }
        return aliases.get(normalized, normalized)

    def close(self) -> None:
        if self.env is not None:
            close = getattr(self.env, "close", None)
            if callable(close):
                close()
        self.env = None
        self.obs = None
        self.info = {}


service = MineStudioService()
app = FastAPI(title="Sinomo MineStudio Control Wrapper")


@app.get("/health")
def health() -> dict[str, Any]:
        return {
            "ok": True,
            "service": "sinomo-minestudio",
            "has_env": service.env is not None,
            "has_observation": service.obs is not None,
            "record_path": service.record_path,
            "display": os.environ.get("DISPLAY"),
            "uptime_s": round(time.time() - service.created_at, 3),
        }


@app.post("/reset")
def reset(req: ResetRequest = ResetRequest()) -> dict[str, Any]:
    try:
        return service.reset(req)
    except Exception as source:
        raise HTTPException(status_code=500, detail=str(source)) from source


@app.get("/observe")
def observe() -> dict[str, Any]:
    try:
        return service.snapshot()
    except HTTPException:
        raise
    except Exception as source:
        raise HTTPException(status_code=500, detail=str(source)) from source


@app.post("/inspect")
def inspect(req: InspectRequest = InspectRequest()) -> dict[str, Any]:
    try:
        return service.inspect(req)
    except HTTPException:
        raise
    except Exception as source:
        raise HTTPException(status_code=500, detail=str(source)) from source


@app.post("/step")
def step(req: StepRequest = StepRequest()) -> dict[str, Any]:
    try:
        return service.step(req)
    except Exception as source:
        raise HTTPException(status_code=500, detail=str(source)) from source


@app.post("/close")
def close() -> dict[str, Any]:
    service.close()
    return {"ok": True}


def smoke() -> int:
    service.reset(ResetRequest())
    first = service.snapshot()
    service.step(StepRequest(ticks=1))
    second = service.snapshot()
    print(
        {
            "ok": True,
            "first_frame": {key: first["frame"][key] for key in ["width", "height", "bytes"]},
            "second_frame": {key: second["frame"][key] for key in ["width", "height", "bytes"]},
            "step_index": second["step_index"],
        }
    )
    service.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=os.environ.get("MINESTUDIO_BIND", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("MINESTUDIO_PORT", "8080")))
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    if args.smoke:
        return smoke()
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
