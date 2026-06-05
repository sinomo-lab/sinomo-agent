#!/usr/bin/env python3
"""Live-style MineStudio + MiniCPM-o duplex streaming benchmark.

This harness emulates the production workflow for a realtime multimodal agent:

1. MineStudio produces a stream of rendered Minecraft frames.
2. The MiniCPM-o endpoint receives an image sequence with `stream=true`.
3. The harness consumes streamed model output chunks as they arrive.
4. Once an action macro is recoverable, the macro is applied to MineStudio.

The environment is allowed to pause between phases, so the benchmark is not
wall-clock realtime. The IO shape still mirrors the live path: continuous frame
input, streaming model output, then closed-loop environment action.
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
    MineStudioTrajectoryRecorder,
    json_safe,
    recover_action_macro,
    score_action_response,
)


def http_json(url: str, method: str, payload: dict[str, Any] | None = None, timeout: int = 300) -> dict[str, Any]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read())


def decode_frame(result: dict[str, Any]) -> Image.Image:
    frame = result.get("frame") or {}
    data = frame.get("data")
    if not isinstance(data, str):
        raise RuntimeError("MineStudio wrapper response did not include frame.data")
    return Image.open(io.BytesIO(base64.b64decode(data))).convert("RGB")


def encode_frame(image: Image.Image) -> str:
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def compact_wrapper_event(event: dict[str, Any] | None) -> dict[str, Any] | None:
    if event is None:
        return None
    value = dict(event)
    frame = value.get("frame")
    if isinstance(frame, dict):
        value["frame"] = {key: item for key, item in frame.items() if key != "data"}
    return value


def docker_visible_record_path(host_path: Path) -> str:
    path = host_path
    if not path.is_absolute() and path.parts and path.parts[0] == "benchmark-results":
        return "/workspace/" + path.as_posix()
    return str(path)


def collect_minestudio_recordings(record_dir: Path) -> dict[str, Any]:
    videos = []
    for path in sorted(record_dir.glob("*.mp4")):
        videos.append(
            {
                "uri": str(path),
                "kind": "minestudio-record-callback-video",
                "byte_len": path.stat().st_size,
            }
        )
    return {
        "ok": bool(videos),
        "record_dir": str(record_dir),
        "videos": videos,
        "error": None if videos else "MineStudio RecordCallback did not produce an mp4",
    }


class MineStudioStreamClient:
    def __init__(self, base_url: str, timeout: int) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def reset(self, payload: dict[str, Any] | None = None) -> tuple[Image.Image, dict[str, Any]]:
        result = http_json(self.base_url + "/reset", "POST", payload or {}, self.timeout)
        return decode_frame(result), result

    def observe(self) -> tuple[Image.Image, dict[str, Any]]:
        result = http_json(self.base_url + "/observe", "GET", None, self.timeout)
        return decode_frame(result), result

    def step_noop(self, ticks: int) -> tuple[Image.Image, dict[str, Any]]:
        result = http_json(self.base_url + "/step", "POST", {"ticks": ticks}, self.timeout)
        return decode_frame(result), result

    def step_macro(self, macro: dict[str, Any], ticks: int) -> tuple[Image.Image, dict[str, Any]]:
        result = http_json(
            self.base_url + "/step",
            "POST",
            {"macro": macro, "ticks": ticks},
            self.timeout,
        )
        return decode_frame(result), result

    def inspect(self, commands: list[str] | None = None) -> dict[str, Any]:
        return http_json(self.base_url + "/inspect", "POST", {"commands": commands or []}, self.timeout)

    def close(self) -> None:
        try:
            http_json(self.base_url + "/close", "POST", {}, self.timeout)
        except Exception:
            pass


def collect_frame_stream(
    env: MineStudioStreamClient,
    run_dir: Path,
    cycle_index: int,
    frame_count: int,
    stride_ticks: int,
) -> tuple[list[Image.Image], list[dict[str, Any]]]:
    frames = []
    events = []
    for frame_index in range(frame_count):
        if frame_index == 0:
            frame, event = env.observe()
        else:
            frame, event = env.step_noop(stride_ticks)
        path = run_dir / f"cycle-{cycle_index:04d}-input-{frame_index:04d}.png"
        frame.save(path)
        event = compact_wrapper_event(event) or {}
        event["saved_frame"] = str(path)
        frames.append(frame)
        events.append(event)
    return frames, events


def call_minicpm_stream(
    endpoint: str,
    instruction: str,
    frames: list[Image.Image],
    cycle_index: int,
    timeout: int,
) -> dict[str, Any]:
    prompt = (
        f"Task: {instruction}\n"
        "You are receiving a short stream of Minecraft video frames in chronological order. "
        "Use only visible information from those frames. Do not assume hidden structured game state.\n"
        f"Allowed verbs: {', '.join(sorted(ACTION_VERBS))}.\n"
        "As soon as the next safe action is clear, emit one send_game_action block with verb, target, duration_ticks, and stop_condition."
    )
    payload = {
        "mode": "duplex",
        "stream": True,
        "system_prompt": (
            "You are the live multimodal control policy for a Minecraft agent. "
            "Video input may continue while you stream the action decision."
        ),
        "messages": [{"role": "user", "content": prompt}],
        "image_name": f"cycle-{cycle_index:04d}-latest.png",
        "image_base64": encode_frame(frames[-1]),
        "image_sequence_names": [f"cycle-{cycle_index:04d}-frame-{index:04d}.png" for index in range(len(frames))],
        "image_sequence_base64": [encode_frame(frame) for frame in frames],
        "metadata": {
            "benchmark": "minestudio-live-duplex-loop",
            "cycle": cycle_index,
            "frame_count": len(frames),
        },
    }
    request = urllib.request.Request(
        endpoint.rstrip("/") + "/complete",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "text/event-stream, application/json"},
        method="POST",
    )
    started = time.monotonic()
    events = []
    content = ""
    with urllib.request.urlopen(request, timeout=timeout) as response:
        content_type = response.headers.get("Content-Type", "")
        if "text/event-stream" in content_type:
            for raw_line in response:
                line = raw_line.decode("utf-8", errors="replace").strip()
                if not line.startswith("data:"):
                    continue
                event = json.loads(line[5:].strip())
                events.append(event)
                if event.get("content"):
                    content = str(event["content"])
                elif event.get("delta"):
                    content += str(event["delta"])
        else:
            data = json.loads(response.read())
            events.append({"done": True, "content": data.get("content", ""), "response": data})
            content = str(data.get("content", ""))
    elapsed_ms = int((time.monotonic() - started) * 1000)
    return {
        "content": content,
        "stream_events": events,
        "elapsed_ms": elapsed_ms,
        "transport": "sse" if len(events) > 1 else "json",
    }


def write_report(run_dir: Path, results: list[dict[str, Any]], video_artifact: dict[str, Any] | None) -> None:
    lines = [
        "# MineStudio Live Duplex Loop",
        "",
        "This run emulates live realtime control without requiring wall-clock realtime execution.",
        "MineStudio supplies image-frame streams, MiniCPM-o is called with `stream=true`, streamed output is consumed incrementally, and recovered actions are stepped back into the environment.",
        "",
        "## Summary",
        "",
        f"- Cycles: {len(results)}",
        f"- Actions recovered: {sum(1 for item in results if item.get('recovered_action'))}",
        f"- Average score: {sum(item['score']['score'] for item in results) / max(len(results), 1):.2f}/4",
    ]
    if video_artifact:
        if video_artifact.get("ok"):
            videos = video_artifact.get("videos") or []
            lines.append(f"- MineStudio video recordings: {len(videos)}")
            for video in videos:
                lines.append(f"- Video: `{video.get('uri')}`")
        else:
            lines.append(f"- Video export error: `{video_artifact.get('error') or video_artifact.get('stderr')}`")
    lines.append("")
    for item in results:
        lines.append(f"## Cycle {item['cycle_index']}")
        lines.append("")
        lines.append(f"- Input frames: {len(item['input_frames'])}")
        lines.append(f"- Stream events: {len(item['stream_events'])}")
        lines.append(f"- Transport: {item['transport']}")
        lines.append(f"- Recovered action: `{json.dumps(item.get('recovered_action'))}`")
        lines.append(f"- Environment step index: {item.get('environment_step_index')}")
        lines.append("")
        lines.append("```text")
        lines.append(item["content"][:2000])
        lines.append("```")
        lines.append("")
    (run_dir / "live_duplex_report.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default=os.environ.get("SINOMO_PYTORCH_ENDPOINT", os.environ.get("SINOMO_MINICPM_ENDPOINT")))
    parser.add_argument("--environment-url", default=os.environ.get("SINOMO_MINESTUDIO_ENV_URL", "http://127.0.0.1:18080"))
    parser.add_argument("--results-dir", default=os.environ.get("SINOMO_BENCHMARK_RESULTS", "benchmark-results"))
    parser.add_argument("--task", default="Explore safely and choose the next useful Minecraft action from the video stream.")
    parser.add_argument("--cycles", type=int, default=2)
    parser.add_argument("--frames-per-cycle", type=int, default=4)
    parser.add_argument("--frame-stride-ticks", type=int, default=1)
    parser.add_argument("--action-ticks", type=int, default=5)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--record-video", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--record-fps", type=int, default=20)
    args = parser.parse_args()
    if not args.endpoint:
        parser.error("--endpoint or SINOMO_PYTORCH_ENDPOINT is required")

    run_id = f"minestudio-live-duplex-{time.strftime('%Y%m%d-%H%M%S')}"
    run_dir = Path(args.results_dir) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    record_dir = run_dir / "minestudio-recording"
    recorder = MineStudioTrajectoryRecorder(Path(args.results_dir) / "_trajectories", run_id)
    env = MineStudioStreamClient(args.environment_url, args.timeout)
    results = []
    try:
        initial_frame, reset_event = env.reset(
            {
                "record_video": args.record_video,
                "record_path": docker_visible_record_path(record_dir),
                "record_fps": args.record_fps,
                "record_frame_type": "pov",
            }
        )
        reset_path = run_dir / "reset.png"
        initial_frame.save(reset_path)
        for cycle_index in range(args.cycles):
            frames, input_events = collect_frame_stream(
                env,
                run_dir,
                cycle_index,
                args.frames_per_cycle,
                args.frame_stride_ticks,
            )
            response = call_minicpm_stream(args.endpoint, args.task, frames, cycle_index, args.timeout)
            score = score_action_response(response["content"])
            macro = recover_action_macro(response["content"])
            action_result = None
            post_frame = None
            if macro is not None:
                macro.duration_ticks = min(max(1, macro.duration_ticks), args.action_ticks)
                post_frame, action_result = env.step_macro(macro.__dict__, macro.duration_ticks)
                post_action_path = run_dir / f"cycle-{cycle_index:04d}-post-action.png"
                post_frame.save(post_action_path)
            compact_action_result = compact_wrapper_event(action_result)
            trajectory_record = recorder.record_step(
                cycle_index,
                {
                    "input_mode": "duplex-video-stream",
                    "environment": "minestudio.http",
                    "instruction": args.task,
                    "frames": [event["saved_frame"] for event in input_events],
                },
                macro,
                (action_result or {}).get("applied"),
                {"pov": post_frame or frames[-1]},
            )
            item = {
                "cycle_index": cycle_index,
                "input_frames": [event["saved_frame"] for event in input_events],
                "input_events": input_events,
                "content": response["content"],
                "stream_events": response["stream_events"],
                "transport": response["transport"],
                "elapsed_ms": response["elapsed_ms"],
                "score": score,
                "recovered_action": macro.__dict__ if macro else None,
                "action_result": compact_action_result,
                "environment_step_index": (compact_action_result or input_events[-1]).get("step_index"),
                "trajectory_record": trajectory_record,
            }
            results.append(item)
            (run_dir / f"cycle-{cycle_index:04d}.json").write_text(json.dumps(json_safe(item), indent=2), encoding="utf-8")
            if compact_action_result and (compact_action_result.get("terminated") or compact_action_result.get("truncated")):
                break
    finally:
        env.close()
    video_artifact = None
    if args.record_video:
        video_artifact = collect_minestudio_recordings(record_dir)
    summary = {
        "benchmark": "MineStudio live duplex streaming loop",
        "endpoint": args.endpoint,
        "environment_url": args.environment_url,
        "reset": compact_wrapper_event(reset_event),
        "video": video_artifact,
        "cycle_count": len(results),
        "actions_recovered": sum(1 for item in results if item.get("recovered_action")),
        "average_score": sum(item["score"]["score"] for item in results) / max(len(results), 1),
        "results": results,
    }
    (run_dir / "summary.json").write_text(json.dumps(json_safe(summary), indent=2), encoding="utf-8")
    write_report(run_dir, results, video_artifact)
    print(json.dumps({"results_dir": str(run_dir), "cycle_count": len(results)}, indent=2))
    return 0 if results else 1


if __name__ == "__main__":
    raise SystemExit(main())
