#!/usr/bin/env python3
"""Run a small MCU/MineStudio task subset through the Sinomo main agent.

The full MCU benchmark executes MineStudio simulator episodes. This runner
consumes the same MCU YAML task configs and records a lightweight integration
benchmark for Sinomo's main-agent loop: task parsing, Minecraft-specific prompt
shape, MiniCPM-o response quality, and action-macro usefulness.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

BENCHMARKS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARKS_DIR / "minestudio"))
from minestudio_action_bridge import (  # noqa: E402
    MineStudioActionBridge,
    MineStudioActionMacro,
    MineStudioTrajectoryRecorder,
)


DEFAULT_TASKS = [
    "simple/look_at_the_sky",
    "simple/collect_dirt",
    "simple/craft_oak_planks",
    "hard/collect_dirt",
    "compositional/mine_diamond_ore_and_collect_dirt",
]

TASK_OBSERVATIONS = {
    "look_at_the_sky": "The agent is outdoors in a clear flat area at noon. The hotbar includes a compass and spyglass. The sky is visible above.",
    "collect_dirt": "The agent stands on grass and dirt. The selected hotbar slot contains a wooden shovel. Health and hunger are full.",
    "craft_oak_planks": "The agent has oak logs, a crafting table, and food in inventory. The crafting table is nearby and reachable.",
    "mine_diamond_ore_and_collect_dirt": "The agent is underground near exposed stone, dirt, and a visible diamond ore vein. Inventory includes an iron pickaxe and wooden shovel.",
}

ACTION_VERBS = {
    "look",
    "move",
    "turn",
    "jump",
    "attack",
    "mine",
    "dig",
    "place",
    "craft",
    "use",
    "select",
    "equip",
    "wait",
    "open",
    "close",
    "sneak",
    "sprint",
}


@dataclass
class McuTask:
    suite: str
    name: str
    path: Path
    text: str
    commands: list[str]
    thinking: str | None


def parse_simple_yaml(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {}
    current_list: str | None = None
    current_key: str | None = None
    current_block: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        if not raw.strip() or raw.strip().startswith("#"):
            continue
        if raw.startswith("-") and current_list:
            result.setdefault(current_list, []).append(raw.strip()[1:].strip().strip("'\""))
            continue
        if raw.startswith(" ") and current_key:
            current_block.append(raw.strip().strip("'\""))
            continue
        if current_key and current_block:
            result[current_key] = " ".join(current_block).strip()
            current_key = None
            current_block = []
        match = re.match(r"^([A-Za-z0-9_]+):(?:\s*(.*))?$", raw)
        if not match:
            continue
        key, value = match.group(1), (match.group(2) or "").strip()
        current_list = None
        if value == "":
            result[key] = []
            current_list = key
        else:
            result[key] = value.strip("'\"")
            current_key = key
    if current_key and current_block:
        result[current_key] = " ".join(current_block).strip()
    return result


def load_task(root: Path, task_ref: str) -> McuTask:
    suite, name = task_ref.split("/", 1)
    path = root / "MCU_benchmark" / "task_configs" / suite / f"{name}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"MCU task config not found: {path}")
    data = parse_simple_yaml(path)
    commands = data.get("custom_init_commands") or []
    if not isinstance(commands, list):
        commands = []
    return McuTask(
        suite=suite,
        name=name,
        path=path,
        text=str(data.get("text") or name.replace("_", " ")),
        commands=[str(command) for command in commands],
        thinking=str(data["thinking"]) if data.get("thinking") else None,
    )


def task_prompt(task: McuTask) -> str:
    observation = TASK_OBSERVATIONS.get(task.name, "The agent is in a Minecraft world with the task resources initialized.")
    return json.dumps(
        {
            "benchmark": "MCU-turbo on MineStudio",
            "task": {
                "suite": task.suite,
                "name": task.name,
                "instruction": task.text,
                "known_solution_hint_from_config": task.thinking,
                "init_commands": task.commands,
            },
            "current_observation": observation,
            "controller_contract": {
                "goal": "Choose the next smooth Minecraft control macro for this moment.",
                "allowed_macro_verbs": sorted(ACTION_VERBS),
                "strict_instruction": "Return a single send_game_action tool call. Avoid broad prose. Include verb, target, duration_ticks, and stop_condition.",
                "required_output": {
                    "response_mode": "answer",
                    "text": "one concise rationale, no more than one sentence",
                    "tool_calls": [
                        {
                            "tool": "send_game_action",
                            "input": {
                                "verb": "one allowed macro verb",
                                "target": "block, item, entity, or view direction",
                                "duration_ticks": "integer if applicable",
                                "stop_condition": "observable condition",
                            },
                            "async": False,
                            "summary": "why this action advances the MCU task",
                        }
                    ],
                    "next_state": "waiting",
                },
            },
        },
        indent=2,
    )


def run_agent(prompt: str, args: argparse.Namespace) -> dict[str, Any]:
    command = [
        args.sinomo_bin,
        "agent",
        "run",
        "--provider",
        args.provider,
        "--model",
        args.model,
        "--prompt",
        prompt,
    ]
    env = dict(os.environ)
    if args.endpoint:
        env["SINOMO_PYTORCH_ENDPOINT"] = args.endpoint
        env["SINOMO_MINICPM_ENDPOINT"] = args.endpoint
    start = time.monotonic()
    proc = subprocess.run(command, text=True, capture_output=True, check=False, timeout=args.timeout, env=env)
    elapsed_ms = int((time.monotonic() - start) * 1000)
    return {
        "command": command,
        "return_code": proc.returncode,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
        "elapsed_ms": elapsed_ms,
    }


def score_response(text: str) -> dict[str, Any]:
    lowered = text.lower()
    verb_hits = sorted(verb for verb in ACTION_VERBS if re.search(rf"\b{re.escape(verb)}\b", lowered))
    mentions_tool = "send_game_action" in lowered
    has_stop_condition = "stop" in lowered or "until" in lowered or "condition" in lowered
    recovered_action = infer_action_macro(text)
    score = 0
    if verb_hits:
        score += 1
    if mentions_tool or recovered_action:
        score += 1
    if has_stop_condition or (recovered_action and recovered_action.get("stop_condition")):
        score += 1
    if len(text.split()) <= 180:
        score += 1
    return {
        "score": score,
        "max_score": 4,
        "verb_hits": verb_hits,
        "mentions_send_game_action": mentions_tool,
        "mentions_stop_condition": has_stop_condition,
        "recovered_action": recovered_action,
        "word_count": len(text.split()),
    }


def infer_action_macro(text: str) -> dict[str, Any] | None:
    lowered = text.lower()
    verb = None
    for pattern in [
        r'verb:\s*"([^"]+)"',
        r'"verb"\s*:\s*"([^"]+)"',
        r'send(?:ing)? the "([^"]+)" action',
        r'execute the "([^"]+)" action',
        r'i(?: will|\'ll) send the "([^"]+)" action',
    ]:
        match = re.search(pattern, lowered)
        if match and match.group(1) in ACTION_VERBS:
            verb = match.group(1)
            break
    if verb is None:
        for candidate in sorted(ACTION_VERBS):
            if re.search(rf"\b{re.escape(candidate)}\b", lowered):
                verb = candidate
                break
    if verb is None:
        return None
    target = None
    for pattern in [
        r'target:\s*"([^"]+)"',
        r'"target"\s*:\s*"([^"]+)"',
        r"targeting the ([^.\n,]+)",
        r"targeting ([^.\n,]+)",
    ]:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            target = match.group(1).strip()
            break
    if not target:
        target = {
            "look": "requested view direction",
            "use": "selected task-relevant item",
            "mine": "task-relevant block",
            "dig": "task-relevant block",
            "craft": "task-relevant recipe",
        }.get(verb, "task-relevant target")
    return {
        "tool": "send_game_action",
        "input": {
            "verb": verb,
            "target": target,
            "duration_ticks": 20,
            "stop_condition": "stop when progress is visible or the state changes unexpectedly",
        },
    }


def write_case_studies(result_dir: Path, results: list[dict[str, Any]]) -> None:
    lines = [
        "# MCU MiniCPM-o Case Studies",
        "",
        "This report is generated by `modules/benchmark/sinomo-benchmarks/mcu/sinomo_mcu_runner.py`.",
        "It uses MCU task configs on top of MineStudio semantics and evaluates the Sinomo main-agent response shape for a small task subset.",
        "",
        "## Summary",
        "",
    ]
    completed = sum(1 for item in results if item["agent"]["return_code"] == 0)
    avg_score = sum(item["score"]["score"] for item in results) / max(len(results), 1)
    lines.append(f"- Tasks attempted: {len(results)}")
    lines.append(f"- Agent calls completed: {completed}")
    lines.append(f"- Average action-readiness score: {avg_score:.2f}/4")
    lines.append("")
    lines.append("## Cases")
    lines.append("")
    for item in results:
        task = item["task"]
        score = item["score"]
        agent = item["agent"]
        lines.append(f"### {task['suite']}/{task['name']}")
        lines.append("")
        lines.append(f"- Instruction: {task['text']}")
        lines.append(f"- Score: {score['score']}/{score['max_score']}")
        lines.append(f"- Verb hits: {', '.join(score['verb_hits']) or 'none'}")
        if score.get("recovered_action"):
            lines.append(f"- Recovered action: `{json.dumps(score['recovered_action'])}`")
        if item.get("minestudio_action"):
            lines.append(f"- MineStudio action: `{json.dumps(item['minestudio_action'])}`")
        if item.get("trajectory_record"):
            lines.append(
                f"- Trajectory record: `{item['trajectory_record']['trajectory_id']}` step `{item['trajectory_record']['step_index']}`"
            )
        lines.append(f"- Latency: {agent['elapsed_ms']} ms")
        if agent["return_code"] != 0:
            lines.append(f"- Error: `{agent['stderr']}`")
        lines.append("")
        lines.append("Model response:")
        lines.append("")
        lines.append("```text")
        lines.append(agent["stdout"][:2000])
        lines.append("```")
        lines.append("")
    lines.append("## Suggestions")
    lines.append("")
    lines.append("- Add a first-class game-state packet to Sinomo events so prompts do not have to encode inventory, pose, mobs, blocks, and current task as ad hoc text.")
    lines.append("- Promote the benchmark `send_game_action` bridge into a real MineStudio environment loop that calls `env.step` and feeds post-action observations back into Sinomo events.")
    lines.append("- Keep MiniCPM-o in a fast vision loop for observation and use stronger background models for long-horizon decomposition or failed-task repair.")
    lines.append("- Store trajectory snippets, screenshots, and structured action traces in SIFS so game-environment agents can replay failures.")
    (result_dir / "case_studies.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mcu-root", default=os.environ.get("MCU_ROOT", "/tmp/mcu"))
    parser.add_argument("--results-dir", default=os.environ.get("SINOMO_BENCHMARK_RESULTS", "benchmark-results"))
    parser.add_argument("--task", action="append", dest="tasks", help="Task ref such as simple/collect_dirt. Repeatable.")
    parser.add_argument("--sinomo-bin", default=os.environ.get("SINOMO_BIN", "sinomo"))
    parser.add_argument("--provider", default=os.environ.get("SINOMO_MCU_PROVIDER", "minicpm-o"))
    parser.add_argument("--model", default=os.environ.get("SINOMO_MCU_MODEL", "openbmb/MiniCPM-o-4_5"))
    parser.add_argument("--endpoint", default=os.environ.get("SINOMO_PYTORCH_ENDPOINT", os.environ.get("SINOMO_MINICPM_ENDPOINT")))
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args()

    mcu_root = Path(args.mcu_root)
    run_dir = Path(args.results_dir) / f"mcu-minicpm-{time.strftime('%Y%m%d-%H%M%S')}"
    run_dir.mkdir(parents=True, exist_ok=True)

    results = []
    bridge = MineStudioActionBridge()
    recorder = MineStudioTrajectoryRecorder(
        Path(args.results_dir) / "_trajectories",
        f"mcu-minicpm-{time.strftime('%Y%m%d-%H%M%S')}",
    )
    for task_ref in args.tasks or DEFAULT_TASKS:
        task = load_task(mcu_root, task_ref)
        prompt = task_prompt(task)
        agent = run_agent(prompt, args)
        score = score_response(agent["stdout"])
        minestudio_action = None
        trajectory_record = None
        if score.get("recovered_action"):
            macro = MineStudioActionMacro.from_dict(score["recovered_action"])
            minestudio_action = bridge.to_minestudio_action(macro)
            trajectory_record = recorder.record_step(
                len(results),
                {
                    "task": {
                        "suite": task.suite,
                        "name": task.name,
                        "text": task.text,
                        "commands": task.commands,
                    },
                    "observation": TASK_OBSERVATIONS.get(task.name),
                },
                macro,
                minestudio_action,
            )
        item = {
            "task": {
                "suite": task.suite,
                "name": task.name,
                "path": str(task.path),
                "text": task.text,
                "commands": task.commands,
                "thinking": task.thinking,
            },
            "prompt": json.loads(prompt),
            "agent": agent,
            "score": score,
            "minestudio_action": minestudio_action,
            "trajectory_record": trajectory_record,
        }
        results.append(item)
        (run_dir / f"{task.suite}-{task.name}.json").write_text(json.dumps(item, indent=2), encoding="utf-8")

    summary = {
        "benchmark": "MCU-turbo MineStudio task subset",
        "mcu_root": str(mcu_root),
        "provider": args.provider,
        "model": args.model,
        "endpoint": args.endpoint,
        "task_count": len(results),
        "completed_agent_calls": sum(1 for item in results if item["agent"]["return_code"] == 0),
        "average_action_readiness_score": sum(item["score"]["score"] for item in results) / max(len(results), 1),
        "results": results,
    }
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    write_case_studies(run_dir, results)
    print(json.dumps({"results_dir": str(run_dir), "summary": {k: summary[k] for k in summary if k != "results"}}, indent=2))
    return 0 if all(item["agent"]["return_code"] == 0 for item in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
