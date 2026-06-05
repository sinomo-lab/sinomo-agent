#!/usr/bin/env python3
"""MineStudio adapter for Sinomo interactive action macros.

The general contracts live in the Rust `sinomo-events` crate. This file is
MineStudio-specific: it maps generic action macros such as `mine`,
`craft`, and `use` into a simple MineStudio/VPT-style action dictionary and
records trajectory artifacts for benchmark replay and repair.
"""

from __future__ import annotations

import base64
import json
import time
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class MineStudioActionMacro:
    verb: str
    target: str
    duration_ticks: int = 1
    stop_condition: str | None = None
    parameters: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "MineStudioActionMacro":
        payload = value.get("input", value)
        return cls(
            verb=str(payload.get("verb", "wait")).lower(),
            target=str(payload.get("target", "")),
            duration_ticks=int(payload.get("duration_ticks") or 1),
            stop_condition=payload.get("stop_condition"),
            parameters=dict(payload.get("parameters") or {}),
        )


class MineStudioActionBridge:
    """Convert and execute Sinomo action macros against a MineStudio env."""

    def to_minestudio_action(self, macro: MineStudioActionMacro) -> dict[str, Any]:
        action: dict[str, Any] = {
            "buttons": [],
            "camera": [0.0, 0.0],
            "hotbar": None,
            "duration_ticks": max(1, macro.duration_ticks),
            "metadata": {
                "sinomo_verb": macro.verb,
                "target": macro.target,
                "stop_condition": macro.stop_condition,
            },
        }
        if macro.verb in {"mine", "dig", "attack"}:
            action["buttons"].append("attack")
        elif macro.verb in {"use", "place", "open"}:
            action["buttons"].append("use")
        elif macro.verb == "jump":
            action["buttons"].append("jump")
        elif macro.verb == "sneak":
            action["buttons"].append("sneak")
        elif macro.verb == "sprint":
            action["buttons"].append("sprint")
        elif macro.verb == "move":
            action["buttons"].append(macro.parameters.get("direction", "forward"))
        elif macro.verb == "look":
            action["camera"] = [
                float(macro.parameters.get("pitch_delta", -15.0)),
                float(macro.parameters.get("yaw_delta", 0.0)),
            ]
        elif macro.verb == "turn":
            action["camera"] = [
                float(macro.parameters.get("pitch_delta", 0.0)),
                float(macro.parameters.get("yaw_delta", 30.0)),
            ]
        elif macro.verb == "select":
            action["hotbar"] = macro.parameters.get("slot", macro.target)
        elif macro.verb == "craft":
            action["buttons"].append("inventory")
            action["metadata"]["recipe"] = macro.target
        elif macro.verb == "wait":
            pass
        else:
            action["metadata"]["unsupported"] = True
        return action

    def to_env_action(self, env: Any, macro: MineStudioActionMacro) -> dict[str, Any]:
        """Convert an action macro to the concrete MineStudio action-space shape."""
        action = self.to_minestudio_action(macro)
        no_op = self._no_op_action(env)
        if no_op is None:
            return action
        env_action = deepcopy(no_op)
        for button in action["buttons"]:
            self._set_button(env_action, str(button), 1)
        if "camera" in env_action:
            env_action["camera"] = action["camera"]
        hotbar = action.get("hotbar")
        if hotbar is not None:
            self._set_hotbar(env_action, hotbar)
        return env_action

    def step_macro(self, env: Any, macro: MineStudioActionMacro) -> list[dict[str, Any]]:
        action = self.to_env_action(env, macro)
        observations = []
        for tick in range(max(1, macro.duration_ticks)):
            obs, reward, terminated, truncated, info = env.step(action)
            observations.append(
                {
                    "tick": tick,
                    "reward": reward,
                    "terminated": bool(terminated),
                    "truncated": bool(truncated),
                    "info": info,
                }
            )
            if terminated or truncated:
                break
        return observations

    def _no_op_action(self, env: Any) -> dict[str, Any] | None:
        action_space = getattr(env, "action_space", None)
        if action_space is None and hasattr(env, "env"):
            action_space = getattr(env.env, "action_space", None)
        no_op = getattr(action_space, "no_op", None)
        if not callable(no_op):
            return {
                "attack": 0,
                "back": 0,
                "camera": [0.0, 0.0],
                "forward": 0,
                "hotbar.1": 0,
                "hotbar.2": 0,
                "hotbar.3": 0,
                "hotbar.4": 0,
                "hotbar.5": 0,
                "hotbar.6": 0,
                "hotbar.7": 0,
                "hotbar.8": 0,
                "hotbar.9": 0,
                "inventory": 0,
                "jump": 0,
                "left": 0,
                "right": 0,
                "sneak": 0,
                "sprint": 0,
                "use": 0,
            }
        value = no_op()
        return value if isinstance(value, dict) else None

    def _set_button(self, env_action: dict[str, Any], button: str, value: int) -> None:
        normalized = button.lower().replace("-", "_")
        aliases = {
            "use": ["use", "place"],
            "inventory": ["inventory"],
            "forward": ["forward"],
            "back": ["back"],
            "left": ["left"],
            "right": ["right"],
        }
        for candidate in aliases.get(normalized, [normalized]):
            if candidate in env_action:
                env_action[candidate] = value
                return
        buttons = env_action.get("buttons")
        if isinstance(buttons, list) and button not in buttons:
            buttons.append(button)

    def _set_hotbar(self, env_action: dict[str, Any], hotbar: Any) -> None:
        try:
            slot = int(hotbar)
        except (TypeError, ValueError):
            return
        for key in [f"hotbar.{slot}", f"hotbar_{slot}", f"hotbar{slot}"]:
            if key in env_action:
                env_action[key] = 1
                return
        if "hotbar" in env_action:
            env_action["hotbar"] = slot


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

ACTION_ALIASES = {
    "break": "mine",
    "collect": "dig",
    "open_inventory": "open",
    "inventory": "open",
    "select_hotbar": "select",
    "walk": "move",
    "forward": "move",
}


def recover_action_macro(text: str) -> MineStudioActionMacro | None:
    import json
    import re

    def macro_from_json(value: Any) -> MineStudioActionMacro | None:
        if not isinstance(value, dict):
            return None
        payload = value.get("send_game_action") or value.get("action") or value
        if isinstance(payload, dict) and payload.get("verb"):
            verb_value = normalize_action_verb(str(payload.get("verb")))
            if verb_value in ACTION_VERBS:
                return MineStudioActionMacro.from_dict(
                    {
                        "verb": verb_value,
                        "target": payload.get("target") or payload.get("item") or payload.get("block") or "",
                        "duration_ticks": payload.get("duration_ticks") or payload.get("ticks") or 20,
                        "stop_condition": payload.get("stop_condition"),
                        "parameters": payload.get("parameters") or {},
                    }
                )
        for key, item in value.items():
            verb_value = normalize_action_verb(str(key))
            if verb_value not in ACTION_VERBS:
                continue
            if isinstance(item, dict):
                return MineStudioActionMacro.from_dict(
                    {
                        "verb": verb_value,
                        "target": item.get("target") or item.get("item") or item.get("block") or item.get("id") or "",
                        "duration_ticks": item.get("duration_ticks") or item.get("ticks") or 20,
                        "stop_condition": item.get("stop_condition"),
                        "parameters": item.get("parameters") or {},
                    }
                )
            if isinstance(item, str):
                return MineStudioActionMacro(verb=verb_value, target=item, duration_ticks=20)
            if item is None:
                return MineStudioActionMacro(verb=verb_value, target="", duration_ticks=20)
        return None

    stripped = text.strip()
    start = stripped.find("{")
    end = stripped.rfind("}")
    if 0 <= start < end:
        try:
            macro = macro_from_json(json.loads(stripped[start : end + 1]))
            if macro is not None:
                return macro
        except Exception:
            pass

    lowered = text.lower()
    verb = None
    for pattern in [
        r'verb:\s*"([^"]+)"',
        r'verb\s*=\s*"([^"]+)"',
        r'"verb"\s*:\s*"([^"]+)"',
        r'"verb"\s*=\s*"([^"]+)"',
        r'send(?:ing)? the "([^"]+)" action',
        r'execute the "([^"]+)" action',
        r'i(?: will|\'ll) send the "([^"]+)" action',
    ]:
        match = re.search(pattern, lowered)
        if match:
            candidate = normalize_action_verb(match.group(1))
            if candidate in ACTION_VERBS:
                verb = candidate
                break
    if verb is None:
        for candidate in sorted(ACTION_VERBS | set(ACTION_ALIASES)):
            if re.search(rf"\b{re.escape(candidate)}\b", lowered):
                verb = normalize_action_verb(candidate)
                break
    if verb is None:
        return None

    target = None
    for pattern in [
        r'target:\s*"([^"]+)"',
        r'target\s*=\s*"([^"]+)"',
        r'"target"\s*:\s*"([^"]+)"',
        r'"target"\s*=\s*"([^"]+)"',
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

    duration_ticks = 20
    duration_match = re.search(r"duration_ticks\s*[:=]\s*(\d+)", lowered)
    if duration_match:
        duration_ticks = int(duration_match.group(1))
    stop_condition = "stop when progress is visible or the frame changes unexpectedly"
    stop_match = re.search(r'stop_condition\s*[:=]\s*"([^"]+)"', text, re.IGNORECASE)
    if stop_match:
        stop_condition = stop_match.group(1)
    return MineStudioActionMacro(
        verb=verb,
        target=target,
        duration_ticks=duration_ticks,
        stop_condition=stop_condition,
    )


def normalize_action_verb(value: str) -> str:
    value = value.lower().strip().replace("-", "_")
    return ACTION_ALIASES.get(value, value)


def score_action_response(text: str) -> dict[str, Any]:
    import re

    lowered = text.lower()
    verb_hits = sorted(
        verb for verb in ACTION_VERBS if re.search(rf"\b{re.escape(verb)}\b", lowered)
    )
    macro = recover_action_macro(text)
    return {
        "score": int(bool(verb_hits))
        + int(macro is not None)
        + int("stop" in lowered or "until" in lowered or "condition" in lowered)
        + int(len(text.split()) <= 180),
        "max_score": 4,
        "verb_hits": verb_hits,
        "recovered_action": macro.__dict__ if macro else None,
        "word_count": len(text.split()),
    }


class MineStudioTrajectoryRecorder:
    def __init__(self, root: str | Path, trajectory_id: str) -> None:
        self.root = Path(root)
        self.trajectory_id = trajectory_id
        self.trajectory_dir = self.root / trajectory_id
        self.trajectory_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = self.trajectory_dir / "trajectory.jsonl"

    def record_step(
        self,
        step_index: int,
        state: dict[str, Any],
        macro: MineStudioActionMacro | None,
        action: dict[str, Any] | None,
        observation: Any = None,
    ) -> dict[str, Any]:
        artifacts = []
        screenshot = self._write_screenshot(step_index, observation)
        if screenshot:
            artifacts.append(screenshot)
        record = {
            "trajectory_id": self.trajectory_id,
            "step_index": step_index,
            "state": state,
            "macro": macro.__dict__ if macro else None,
            "minestudio_action": action,
            "artifacts": artifacts,
            "recorded_at_unix_ms": int(time.time() * 1000),
        }
        with self.log_path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(json_safe(record), sort_keys=True) + "\n")
        return record

    def _write_screenshot(self, step_index: int, observation: Any) -> dict[str, Any] | None:
        if observation is None:
            return None
        pov = observation.get("pov") if isinstance(observation, dict) else None
        if pov is None:
            return None
        path = self.trajectory_dir / f"step-{step_index:04d}.png"
        if isinstance(pov, bytes):
            path.write_bytes(pov)
        elif isinstance(pov, str):
            path.write_bytes(base64.b64decode(pov))
        elif hasattr(pov, "save"):
            pov.save(path)
        else:
            return None
        data = path.read_bytes()
        return {
            "uri": str(path),
            "kind": "screenshot",
            "byte_len": len(data),
        }


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [json_safe(item) for item in value]
    if hasattr(value, "tolist"):
        return value.tolist()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def main() -> int:
    macro = MineStudioActionMacro.from_dict(
        {
            "verb": "mine",
            "target": "dirt",
            "duration_ticks": 3,
            "stop_condition": "block broken",
        }
    )
    print(json.dumps(MineStudioActionBridge().to_minestudio_action(macro), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
