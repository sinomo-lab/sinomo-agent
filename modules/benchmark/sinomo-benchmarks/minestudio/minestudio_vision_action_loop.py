#!/usr/bin/env python3
"""Vision-only MineStudio action-loop benchmark for MiniCPM-o.

This runner starts a real MineStudio Minecraft simulator. Each decision sees
only the task instruction plus the current rendered frame from the simulator.
It does not send structured game-state text to the model.
"""

from __future__ import annotations

import argparse
import base64
import io
import json
import os
import time
import urllib.request
from pathlib import Path
from typing import Any

from PIL import Image

from minestudio_action_bridge import (
    ACTION_VERBS,
    MineStudioActionBridge,
    MineStudioTrajectoryRecorder,
    json_safe,
    recover_action_macro,
    score_action_response,
)


def as_pil_image(value: Any) -> Image.Image:
    if isinstance(value, Image.Image):
        return value.convert("RGB")
    if isinstance(value, bytes):
        return Image.open(io.BytesIO(value)).convert("RGB")
    if hasattr(value, "shape"):
        return Image.fromarray(value).convert("RGB")
    raise TypeError(f"unsupported frame type: {type(value)!r}")


def observation_frame(obs: Any, info: dict[str, Any]) -> Image.Image:
    if isinstance(info, dict) and info.get("pov") is not None:
        return as_pil_image(info["pov"])
    if isinstance(obs, dict) and obs.get("image") is not None:
        return as_pil_image(obs["image"])
    if isinstance(obs, dict) and obs.get("pov") is not None:
        return as_pil_image(obs["pov"])
    return as_pil_image(obs)


def image_bytes(image: Image.Image) -> bytes:
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def http_json(url: str, method: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    with urllib.request.urlopen(request, timeout=300) as response:
        return json.loads(response.read())


def frame_from_wrapper(result: dict[str, Any]) -> Image.Image:
    frame = result.get("frame") or {}
    data = frame.get("data")
    if not isinstance(data, str):
        raise RuntimeError("MineStudio wrapper response did not include frame.data")
    return Image.open(io.BytesIO(base64.b64decode(data))).convert("RGB")


class MineStudioHttpEnv:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def reset(self) -> tuple[dict[str, Any], dict[str, Any]]:
        result = http_json(self.base_url + "/reset", "POST", {})
        frame = frame_from_wrapper(result)
        return {"image": frame}, {"pov": frame, "wrapper": result}

    def step(self, action: dict[str, Any]) -> tuple[dict[str, Any], float, bool, bool, dict[str, Any]]:
        result = http_json(self.base_url + "/step", "POST", {"action": action, "ticks": 1})
        frame = frame_from_wrapper(result)
        return (
            {"image": frame},
            float(result.get("reward") or 0.0),
            bool(result.get("terminated")),
            bool(result.get("truncated")),
            {"pov": frame, "wrapper": result},
        )

    def close(self) -> None:
        try:
            http_json(self.base_url + "/close", "POST", {})
        except Exception:
            pass


def call_minicpm_vision(
    endpoint: str,
    instruction: str,
    frame: Image.Image,
    step_index: int,
    timeout: int,
) -> dict[str, Any]:
    encoded = base64.b64encode(image_bytes(frame)).decode("ascii")
    prompt = (
        f"Task: {instruction}\n"
        "Use only the attached Minecraft video frame as the observation. "
        "Do not assume inventory, position, block names, health, or any structured game state that is not visible in the frame.\n"
        f"Allowed verbs: {', '.join(sorted(ACTION_VERBS))}.\n"
        "Choose one next action for a MineStudio controller. Return a single send_game_action block with verb, target, duration_ticks, and stop_condition. "
        "Use only one of the allowed verbs exactly."
    )
    payload = {
        "mode": "vision",
        "system_prompt": (
            "You are a vision-only Minecraft control policy. Infer the next safe action from pixels. "
            "Prefer short closed-loop action macros."
        ),
        "messages": [{"role": "user", "content": prompt}],
        "image_name": f"minestudio-step-{step_index:04d}.png",
        "image_base64": encoded,
        "stream": False,
        "metadata": {"benchmark": "minestudio-vision-action-loop", "step": step_index},
    }
    request = urllib.request.Request(
        endpoint.rstrip("/") + "/complete",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    start = time.monotonic()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        data = json.loads(response.read())
    data["elapsed_ms_client"] = int((time.monotonic() - start) * 1000)
    return data


def create_minestudio_env(args: argparse.Namespace, run_dir: Path) -> Any:
    try:
        from minestudio.simulator import MinecraftSim
        from minestudio.simulator.callbacks import RecordCallback
    except Exception as source:
        raise RuntimeError(
            "MineStudio is required for the real vision action loop. Install it in a compatible Python environment, for example Python 3.10/3.11 with `pip install minestudio`."
        ) from source

    callbacks = []
    if args.record_video:
        callbacks.append(
            RecordCallback(
                record_path=str(run_dir / "minestudio-recording"),
                fps=args.record_fps,
                frame_type="pov",
            )
        )
    kwargs: dict[str, Any] = {
        "obs_size": (args.obs_width, args.obs_height),
        "render_size": (args.render_width, args.render_height),
        "callbacks": callbacks,
    }
    return MinecraftSim(**kwargs)


def apply_macro(
    env: Any,
    bridge: MineStudioActionBridge,
    macro: Any,
) -> tuple[Any, float | None, bool, bool, dict[str, Any], dict[str, Any], int]:
    action = bridge.to_env_action(env, macro)
    obs = None
    info: dict[str, Any] = {}
    reward: float | None = None
    terminated = False
    truncated = False
    ticks = max(1, macro.duration_ticks)
    for _ in range(ticks):
        obs, reward, terminated, truncated, info = env.step(action)
        if terminated or truncated:
            break
    return obs, reward, bool(terminated), bool(truncated), info, action, ticks


def write_report(run_dir: Path, results: list[dict[str, Any]]) -> None:
    successful_steps = [item for item in results if item["ok"]]
    lines = [
        "# MineStudio Vision-Only Action Loop",
        "",
        "This report evaluates MiniCPM-o action selection in a real MineStudio Minecraft simulator. The model receives task text plus rendered `pov` frames, but no structured game-state text.",
        "",
        "## Summary",
        "",
        f"- Steps attempted: {len(results)}",
        f"- Completed model calls: {len(successful_steps)}",
        f"- Actions recovered: {sum(1 for item in results if item.get('recovered_action'))}",
        f"- Average action-readiness score: {sum(item['score']['score'] for item in results) / max(len(results), 1):.2f}/4",
        f"- Terminated steps: {sum(1 for item in results if item.get('terminated'))}",
        f"- Truncated steps: {sum(1 for item in results if item.get('truncated'))}",
        "",
        "## Steps",
        "",
    ]
    for item in results:
        lines.append(f"### Step {item['step_index']}")
        lines.append("")
        lines.append(f"- Score: {item['score']['score']}/{item['score']['max_score']}")
        lines.append(f"- Verb hits: {', '.join(item['score']['verb_hits']) or 'none'}")
        lines.append(f"- Recovered action: `{json.dumps(item.get('recovered_action'))}`")
        lines.append(f"- MineStudio action: `{json.dumps(item.get('minestudio_action'))}`")
        lines.append(f"- Reward: {item.get('reward')}")
        lines.append(f"- Terminated: {item.get('terminated')}")
        lines.append(f"- Truncated: {item.get('truncated')}")
        lines.append(f"- Latency: {item['elapsed_ms']} ms")
        lines.append(f"- Frame: `{item['frame_path']}`")
        if item.get("error"):
            lines.append(f"- Error: `{item['error']}`")
        lines.append("")
        lines.append("Model response:")
        lines.append("")
        lines.append("```text")
        lines.append(item["content"][:2000])
        lines.append("```")
        lines.append("")
    lines.append("## Observations")
    lines.append("")
    lines.append("- This is a real rendered-frame loop: each next request uses a frame returned by `MinecraftSim.reset` or `MinecraftSim.step`.")
    lines.append("- The model still receives the task instruction because the benchmark must define the goal; it does not receive simulator state dictionaries, inventory, coordinates, nearby blocks, health, or reward.")
    lines.append("- Performance should be compared against the structured-state MCU runner after the same tasks are backed by full MineStudio episodes.")
    (run_dir / "vision_case_studies.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default=os.environ.get("SINOMO_PYTORCH_ENDPOINT", os.environ.get("SINOMO_MINICPM_ENDPOINT")))
    parser.add_argument("--results-dir", default=os.environ.get("SINOMO_BENCHMARK_RESULTS", "benchmark-results"))
    parser.add_argument("--task", default="Explore the Minecraft world safely and describe one useful next action from vision.")
    parser.add_argument("--steps", type=int, default=3)
    parser.add_argument("--action-ticks", type=int, default=5)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--obs-width", type=int, default=128)
    parser.add_argument("--obs-height", type=int, default=128)
    parser.add_argument("--render-width", type=int, default=640)
    parser.add_argument("--render-height", type=int, default=360)
    parser.add_argument("--record-video", action="store_true")
    parser.add_argument("--record-fps", type=int, default=20)
    parser.add_argument(
        "--environment-url",
        default=os.environ.get("SINOMO_MINESTUDIO_ENV_URL"),
        help="Use an existing MineStudio HTTP wrapper instead of importing MineStudio locally.",
    )
    args = parser.parse_args()
    if not args.endpoint:
        parser.error("--endpoint or SINOMO_PYTORCH_ENDPOINT is required")

    run_id = f"minestudio-real-vision-{time.strftime('%Y%m%d-%H%M%S')}"
    run_dir = Path(args.results_dir) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    recorder = MineStudioTrajectoryRecorder(Path(args.results_dir) / "_trajectories", run_id)
    bridge = MineStudioActionBridge()
    env = MineStudioHttpEnv(args.environment_url) if args.environment_url else create_minestudio_env(args, run_dir)
    results = []
    try:
        obs, info = env.reset()
        for step_index in range(args.steps):
            frame = observation_frame(obs, info)
            frame_path = run_dir / f"step-{step_index:04d}.png"
            frame.save(frame_path)
            try:
                response = call_minicpm_vision(args.endpoint, args.task, frame, step_index, args.timeout)
                content = str(response.get("content", ""))
                ok = True
                error = None
                elapsed_ms = int(response.get("elapsed_ms") or response.get("elapsed_ms_client") or 0)
            except Exception as source:
                content = ""
                ok = False
                error = str(source)
                elapsed_ms = 0

            score = score_action_response(content)
            macro = recover_action_macro(content)
            if macro:
                macro.duration_ticks = min(max(1, macro.duration_ticks), args.action_ticks)
            minestudio_action = None
            reward = None
            terminated = False
            truncated = False
            applied_ticks = 0
            if macro is not None:
                obs, reward, terminated, truncated, info, minestudio_action, applied_ticks = apply_macro(
                    env, bridge, macro
                )

            trajectory_record = recorder.record_step(
                step_index,
                {
                    "input_mode": "vision-only",
                    "environment": "minestudio.http" if args.environment_url else "minestudio.MinecraftSim",
                    "instruction": args.task,
                    "frame": str(frame_path),
                },
                macro,
                minestudio_action,
                {"pov": frame},
            )
            item = {
                "step_index": step_index,
                "instruction": args.task,
                "ok": ok,
                "error": error,
                "content": content,
                "score": score,
                "recovered_action": macro.__dict__ if macro else None,
                "minestudio_action": minestudio_action,
                "trajectory_record": trajectory_record,
                "elapsed_ms": elapsed_ms,
                "frame_path": str(frame_path),
                "reward": reward,
                "terminated": bool(terminated),
                "truncated": bool(truncated),
                "applied_ticks": applied_ticks,
            }
            results.append(item)
            (run_dir / f"step-{step_index:04d}.json").write_text(json.dumps(json_safe(item), indent=2), encoding="utf-8")
            if terminated or truncated:
                break
    finally:
        close = getattr(env, "close", None)
        if callable(close):
            close()

    summary = {
        "benchmark": "MineStudio real vision-only action loop",
        "endpoint": args.endpoint,
        "task": args.task,
        "step_count": len(results),
        "completed_calls": sum(1 for item in results if item["ok"]),
        "actions_recovered": sum(1 for item in results if item.get("recovered_action")),
        "average_action_readiness_score": sum(item["score"]["score"] for item in results) / max(len(results), 1),
        "results": results,
    }
    (run_dir / "summary.json").write_text(json.dumps(json_safe(summary), indent=2), encoding="utf-8")
    write_report(run_dir, results)
    print(json.dumps({"results_dir": str(run_dir), "summary": {k: summary[k] for k in summary if k != "results"}}, indent=2))
    return 0 if results and all(item["ok"] for item in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
