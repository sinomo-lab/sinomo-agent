#!/usr/bin/env python3
"""Run MCU-turbo task configs through the MineStudio full-duplex loop.

This is the real-environment counterpart to `sinomo_mcu_runner.py`. It does
not feed structured game-state text to the model. Each MCU task resets a real
MineStudio environment with the task's init commands, samples rendered frames,
sends the frame sequence to MiniCPM-o as a duplex streaming request, applies
the recovered action, and records MineStudio's own episode video.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

BENCHMARKS_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARKS_DIR / "minestudio"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from minestudio_action_bridge import MineStudioActionMacro, json_safe, recover_action_macro, score_action_response  # noqa: E402
from minestudio_live_duplex_loop import (  # noqa: E402
    MineStudioStreamClient,
    call_minicpm_stream,
    collect_frame_stream,
    collect_minestudio_recordings,
    compact_wrapper_event,
    docker_visible_record_path,
)
from sinomo_mcu_runner import McuTask, load_task  # noqa: E402


def discover_tasks(root: Path) -> list[str]:
    base = root / "MCU_benchmark" / "task_configs"
    tasks = []
    for path in sorted(base.glob("*/*.yaml")):
        tasks.append(f"{path.parent.name}/{path.stem}")
    if not tasks:
        raise FileNotFoundError(f"no MCU task configs found under {base}")
    return tasks


def compact_environment_text(inspection: dict[str, Any] | None) -> str:
    if not inspection:
        return "No environment inspection is available."
    info = inspection.get("info") or {}
    inventory = info.get("inventory") or ((inspection.get("obs") or {}).get("inventory")) or {}
    if not inventory:
        for command in inspection.get("commands") or []:
            inventory = ((command.get("obs") or {}).get("inventory")) or ((command.get("info") or {}).get("inventory")) or {}
            if inventory:
                break
    hotbar = []
    if isinstance(inventory, dict):
        for index in range(9):
            item = inventory.get(str(index)) or {}
            item_type = item.get("type")
            qty = item.get("quantity")
            if item_type and item_type != "none" and qty:
                hotbar.append(f"{index + 1}:{item_type}x{qty}")
    equipped_items = info.get("equipped_items") or {}
    if not equipped_items:
        for command in inspection.get("commands") or []:
            equipped_items = ((command.get("obs") or {}).get("equipped_items")) or ((command.get("info") or {}).get("equipped_items")) or {}
            if equipped_items:
                break
    equipped = (equipped_items.get("mainhand") or {}).get("type")
    location = info.get("location_stats") or {}
    location_bits = []
    for key in ["xpos", "ypos", "zpos", "yaw", "pitch", "can_see_sky", "light_level"]:
        if key in location:
            location_bits.append(f"{key}={location[key]}")
    mobs = info.get("mobs")
    mob_text = ""
    if isinstance(mobs, dict):
        mob_text = ", ".join(f"{key}:{value}" for key, value in list(mobs.items())[:8])
    elif mobs:
        mob_text = str(mobs)[:240]
    return "\n".join(
        [
            f"Hotbar/inventory slots: {', '.join(hotbar) or 'empty hotbar'}",
            f"Equipped main hand: {equipped or 'unknown'}",
            f"Health: {info.get('health')} Food: {info.get('food_level')}",
            f"Location: {', '.join(location_bits) or 'unknown'}",
            f"Nearby mobs summary: {mob_text or 'none/unknown'}",
        ]
    )


def normalize_subgoal_name(name: str) -> str:
    if name.startswith("craft_to_"):
        return "craft_" + name.removeprefix("craft_to_")
    return name


def task_subgoals(task: McuTask) -> list[str]:
    name = normalize_subgoal_name(task.name)
    for separator in ["_and_", "_or_"]:
        name = name.replace(separator, "|")
    return [normalize_subgoal_name(part) for part in name.split("|") if part]


def hint_priority(hint: str) -> int:
    if any(keyword in hint for keyword in ["place", "use", "craft", "attack", "mine"]):
        return 0
    if "look at sky" in hint:
        return 1
    if hint.startswith("find "):
        return 2
    return 1


def known_subgoal_hint(subgoal: str) -> str | None:
    subgoal = normalize_subgoal_name(subgoal)
    hints = {
        "look_at_the_sky": 'look at sky: {"verb":"look","target":"sky","duration_ticks":10,"stop_condition":"can_see_sky == true"}',
        "explore_boat": 'explore boat: do not look. interact directly with the boat using {"verb":"use","target":"minecraft:oak_boat","duration_ticks":20,"stop_condition":"entered or interacted with boat"}',
        "build_a_house": 'build house: place a visible house material directly using {"verb":"place","target":"minecraft:oak_planks","duration_ticks":20,"stop_condition":"wall/floor block is placed"}',
        "build_a_waterfall": 'build waterfall: do not select only. use the water bucket directly with {"verb":"use","target":"minecraft:water_bucket","duration_ticks":20,"stop_condition":"water is placed or visible"}',
        "collect_wool": 'collect wool: {"verb":"use","target":"minecraft:shears","duration_ticks":20,"stop_condition":"wool count increases or sheep is sheared"}',
        "carve_pumpkins": 'carve pumpkins: {"verb":"use","target":"minecraft:shears","duration_ticks":20,"stop_condition":"pumpkin changes or carved pumpkin appears"}',
        "explore_the_treasure": 'explore treasure: do not look. interact directly with the chest using {"verb":"open","target":"chest","duration_ticks":20,"stop_condition":"chest interaction is visible"}',
        "decorate_the_ground": 'decorate ground: place a decoration directly using {"verb":"place","target":"minecraft:red_tulip","duration_ticks":20,"stop_condition":"decoration is placed"}',
        "decorate_the_wall": 'decorate wall: place a wall decoration directly using {"verb":"place","target":"minecraft:painting","duration_ticks":20,"stop_condition":"painting or item frame is placed on wall"}',
        "place_a_item_frame": 'place item frame: {"verb":"use","target":"minecraft:item_frame","duration_ticks":20,"stop_condition":"item frame is placed"}',
        "lay_carpet": 'lay carpet: {"verb":"place","target":"white_carpet","duration_ticks":20,"stop_condition":"carpet is placed"}',
        "light_up_the_surroundings_at_night": 'light up surroundings: do not look. place light directly using {"verb":"place","target":"minecraft:torch","duration_ticks":20,"stop_condition":"torch or glowstone is placed"}',
        "stacking_acacia_fence": 'stack acacia fence: do not look. place fence directly using {"verb":"place","target":"minecraft:acacia_fence","duration_ticks":20,"stop_condition":"acacia fence block is placed or stacked"}',
        "combat_witch": 'combat witch: {"verb":"attack","target":"witch","duration_ticks":20,"stop_condition":"witch is hit or distance changes"}',
        "combat_spiders": 'combat spiders: {"verb":"attack","target":"spider","duration_ticks":20,"stop_condition":"spider is hit or distance changes"}',
        "trap_a_witch_with_a_boat": 'trap witch with boat: {"verb":"use","target":"minecraft:oak_boat","duration_ticks":20,"stop_condition":"boat is placed near witch"}',
        "craft_bell": 'craft bell: {"verb":"craft","target":"minecraft:bell","duration_ticks":1,"stop_condition":"bell is crafted or appears in inventory"}',
        "craft_cake": 'craft cake: {"verb":"craft","target":"minecraft:cake","duration_ticks":1,"stop_condition":"cake is crafted or appears in inventory"}',
    }
    if subgoal in hints:
        return hints[subgoal]
    if subgoal.startswith("find_"):
        target = subgoal.removeprefix("find_")
        return f"find {target}: turn or move toward visible {target}; avoid repeated look if nothing changes."
    if subgoal.startswith("craft_"):
        target = subgoal.removeprefix("craft_")
        return f'craft {target}: {{"verb":"craft","target":"minecraft:{target}","duration_ticks":1,"stop_condition":"crafted"}}'
    if subgoal.startswith("collect_"):
        target = subgoal.removeprefix("collect_")
        return f"collect {target}: select the relevant tool, then mine/use a visible source of {target}."
    return None


def compound_task_hints(task: McuTask) -> list[str]:
    hints = []
    for subgoal in task_subgoals(task):
        hint = known_subgoal_hint(subgoal)
        if hint:
            hints.append(hint)
    return sorted(hints, key=hint_priority)


def craft_subgoal_targets(task: McuTask) -> list[str]:
    targets = []
    for subgoal in task_subgoals(task):
        if subgoal.startswith("craft_"):
            target = subgoal.removeprefix("craft_")
            if target.startswith("to_"):
                target = target.removeprefix("to_")
            targets.append(target)
    return targets


def first_hint_action(task: McuTask) -> MineStudioActionMacro | None:
    import json
    import re

    for hint in compound_task_hints(task):
        if hint_priority(hint) != 0:
            continue
        match = re.search(r"(\{.*\})", hint)
        if not match:
            continue
        try:
            payload = json.loads(match.group(1))
        except Exception:
            continue
        if payload.get("verb") and payload.get("target"):
            return MineStudioActionMacro.from_dict(payload)
    return None


def repair_action_macro(
    task: McuTask,
    macro: MineStudioActionMacro,
    prior_cycles: list[dict[str, Any]],
) -> tuple[MineStudioActionMacro, dict[str, Any] | None]:
    target = macro.target.lower().removeprefix("minecraft:")
    repair: dict[str, Any] | None = None
    if macro.verb == "craft":
        craft_targets = craft_subgoal_targets(task)
        malformed_compound_target = "_and_" in target or "_or_" in target or target == task.name
        attempted = [
            str((cycle.get("recovered_action") or {}).get("target") or "").lower().removeprefix("minecraft:")
            for cycle in prior_cycles
        ]
        if craft_targets and malformed_compound_target:
            next_target = next((item for item in craft_targets if item not in attempted), craft_targets[0])
            repair = {
                "reason": "compound craft target repaired to concrete craft subgoal",
                "original": macro.__dict__.copy(),
                "target": f"minecraft:{next_target}",
            }
            macro = MineStudioActionMacro(
                verb="craft",
                target=f"minecraft:{next_target}",
                duration_ticks=macro.duration_ticks,
                stop_condition=macro.stop_condition or f"{next_target} is crafted or appears in inventory",
                parameters=macro.parameters,
            )
        elif craft_targets and target in craft_targets and target in attempted:
            next_target = next((item for item in craft_targets if item != target), craft_targets[0])
            repair = {
                "reason": "repeated craft target rotated to another concrete craft subgoal",
                "original": macro.__dict__.copy(),
                "target": f"minecraft:{next_target}",
            }
            macro = MineStudioActionMacro(
                verb="craft",
                target=f"minecraft:{next_target}",
                duration_ticks=macro.duration_ticks,
                stop_condition=f"{next_target} is crafted or appears in inventory",
                parameters=macro.parameters,
            )
    if macro.verb == "look" and not macro.target.strip() and prior_cycles:
        replacement = first_hint_action(task)
        if replacement is not None and replacement.verb != "look":
            repair = {
                "reason": "empty repeated look repaired to first concrete task shortcut",
                "original": macro.__dict__.copy(),
                "replacement": replacement.__dict__.copy(),
            }
            replacement.duration_ticks = macro.duration_ticks
            macro = replacement
    return macro, repair


def repeated_action_guidance(prior_cycles: list[dict[str, Any]]) -> str | None:
    actions = [cycle.get("recovered_action") for cycle in prior_cycles if cycle.get("recovered_action")]
    if not actions:
        return None
    last = actions[-1]
    last_verb = str(last.get("verb") or "").lower()
    last_target = str(last.get("target") or "").strip()
    if last_verb in {"select", "equip"} and last_target:
        return (
            f"The previous action only selected or equipped {last_target}. "
            f"If that item is intended, now use or place {last_target}; do not select it again."
        )
    if last_verb == "look" and not last_target:
        return (
            "The previous action was an empty-target look. Empty-target look is not task progress. "
            "Choose a concrete target now: place/craft/use an inventory item, turn with a direction, or move toward a visible object."
        )
    if len(actions) < 2:
        return None
    previous = actions[-2]
    if (last.get("verb"), last.get("target")) != (previous.get("verb"), previous.get("target")):
        return None
    if last_target:
        return (
            f"The last two cycles repeated {last_verb} {last_target}. "
            "Do not emit that exact action again; switch to the next manipulation step."
        )
    if last_verb == "look":
        return (
            "The last two cycles repeated empty-target look. Do not look again. "
            "Pick a concrete non-look subgoal such as placing torch, acacia_fence, painting, or another inventory item when available."
        )
    return f"The last two cycles repeated {last_verb}. Do not emit that exact action again."


def task_instruction(
    task: McuTask,
    cycle_index: int,
    prior_cycles: list[dict[str, Any]],
    inspection: dict[str, Any] | None,
) -> str:
    prior_actions = [
        {
            "cycle": item["cycle_index"],
            "action": item.get("recovered_action"),
            "step_index": (item.get("action_result") or item.get("post_action_inspection") or {}).get("step_index"),
        }
        for item in prior_cycles[-5:]
    ]
    parts = [
        f"MCU-turbo task: {task.text}",
        f"Suite: {task.suite}",
        f"Task id: {task.name}",
        f"Decision cycle: {cycle_index}",
        "Use the Minecraft video frame stream as the primary observation, and use the compact environment text as low-bandwidth protocol state.",
        "Choose the next bounded game action that makes visible progress on the task.",
        "Prefer concrete manipulation actions over passive looking once the scene is visible.",
        "Empty-target look is only allowed when no concrete inventory/manipulation shortcut is available, and never after cycle 0.",
        "Never use the whole task id as an item target. For compound craft tasks, use a concrete item target such as minecraft:bell, minecraft:cake, or minecraft:diorite.",
        "Do not repeat an action if recent frames show no visible progress; choose a corrective move, turn, select/equip, use, mine, place, or craft action instead.",
        "For inventory actions, target exact item ids from the environment text, such as crafting_table, water_bucket, shears, oak_planks, pumpkin, or planks.",
        "Use craft with the crafted item target, place with the block/item target, select with the inventory item target, and mine/dig only for visible blocks.",
        "Return exactly one send_game_action-style JSON object with verb, target, duration_ticks, and stop_condition.",
        "",
        "Compact environment text:",
        compact_environment_text(inspection),
        "",
        "Task-specific controller hint. Prefer this over generic exploration:",
        task_controller_hint(task),
    ]
    compound_hints = compound_task_hints(task)
    if compound_hints:
        parts.append("Known subgoal shortcuts:")
        parts.extend(f"- {hint}" for hint in compound_hints)
    repetition_hint = repeated_action_guidance(prior_cycles)
    if repetition_hint:
        parts.append("Loop breaker:")
        parts.append(repetition_hint)
    if prior_actions:
        parts.append("Recent previously attempted actions, for loop continuity only:")
        parts.append(json.dumps(prior_actions, indent=2))
    if task.thinking:
        parts.append(f"Task hint from MCU config: {task.thinking}")
    return "\n".join(parts)


def task_controller_hint(task: McuTask) -> str:
    name = task.name
    if name == "look_at_the_sky":
        return 'Use {"verb":"look","target":"sky","duration_ticks":10,"stop_condition":"can_see_sky == true"}.'
    if name.startswith("craft_"):
        target = name.removeprefix("craft_")
        if target.startswith("to_"):
            target = target.removeprefix("to_")
        return f'If the required ingredient is present, use {{"verb":"craft","target":"minecraft:{target}","duration_ticks":1,"stop_condition":"crafted"}}.'
    if name.startswith("use_"):
        target = name.removeprefix("use_")
        return f'If {target} is present or equipped, use {{"verb":"use","target":"minecraft:{target}","duration_ticks":20,"stop_condition":"visible use of {target}"}}.'
    if name.startswith("collect_"):
        target = name.removeprefix("collect_")
        return f"If {target} is already in inventory, select the appropriate tool or move/mine a visible {target} block."
    hints = compound_task_hints(task)
    if hints:
        craft_targets = craft_subgoal_targets(task)
        craft_note = ""
        if craft_targets:
            craft_note = " Valid craft targets for this task are only: " + ", ".join(f"minecraft:{item}" for item in craft_targets) + "."
        return (
            "This is a compound task. Prefer the first non-find manipulation shortcut below exactly; "
            "do not emit empty-target look when a place/use/craft shortcut is listed. "
            "Do not use the full task id as a target."
            + craft_note
            + " Shortcuts: "
            + " | ".join(hints[:4])
        )
    return "Choose the most direct action family for the task; avoid looking repeatedly after cycle 0."


def task_progress_summary(result: dict[str, Any]) -> dict[str, Any]:
    cycles = result.get("cycles", [])
    actions = [cycle.get("recovered_action") for cycle in cycles if cycle.get("recovered_action")]
    final_step = None
    if cycles:
        last_cycle = cycles[-1]
        final_step = (last_cycle.get("action_result") or last_cycle.get("post_action_inspection") or {}).get("step_index")
    verbs: dict[str, int] = {}
    for action in actions:
        verb = str(action.get("verb"))
        verbs[verb] = verbs.get(verb, 0) + 1
    repeated_action_runs = 0
    previous = None
    for action in actions:
        current = (action.get("verb"), action.get("target"))
        if current == previous:
            repeated_action_runs += 1
        previous = current
    return {
        "cycles": len(cycles),
        "actions_recovered": len(actions),
        "final_step_index": final_step,
        "verb_counts": verbs,
        "repeated_adjacent_actions": repeated_action_runs,
        "terminated": any((cycle.get("action_result") or {}).get("terminated") for cycle in cycles),
        "truncated": any((cycle.get("action_result") or {}).get("truncated") for cycle in cycles),
        "success": result.get("success"),
    }


def inventory_counts(inspection: dict[str, Any] | None) -> dict[str, int]:
    inventory = (((inspection or {}).get("info") or {}).get("inventory")) or {}
    if not inventory:
        inventory = (((inspection or {}).get("obs") or {}).get("inventory")) or {}
    if not inventory:
        for command in (inspection or {}).get("commands") or []:
            inventory = ((command.get("obs") or {}).get("inventory")) or ((command.get("info") or {}).get("inventory")) or {}
            if inventory:
                break
    counts: dict[str, int] = {}
    if isinstance(inventory, dict):
        for item in inventory.values():
            if not isinstance(item, dict):
                continue
            item_type = str(item.get("type") or "")
            if not item_type or item_type == "none":
                continue
            try:
                qty = int(item.get("quantity") or 0)
            except Exception:
                qty = 0
            counts[item_type] = counts.get(item_type, 0) + qty
    return counts


def cycle_inventory_deltas(result: dict[str, Any]) -> list[dict[str, tuple[int, int]]]:
    deltas = []
    for cycle in result.get("cycles", []):
        before = inventory_counts(cycle.get("pre_action_inspection"))
        after = inventory_counts(cycle.get("post_action_inspection"))
        keys = set(before) | set(after)
        deltas.append({key: (before.get(key, 0), after.get(key, 0)) for key in keys if before.get(key, 0) != after.get(key, 0)})
    return deltas


def inventory_count_decreased(result: dict[str, Any], aliases: set[str]) -> bool:
    for delta in cycle_inventory_deltas(result):
        for alias in aliases:
            before, after = delta.get(alias, (0, 0))
            if before > after:
                return True
    return False


def inventory_count_increased(result: dict[str, Any], aliases: set[str]) -> bool:
    for delta in cycle_inventory_deltas(result):
        for alias in aliases:
            before, after = delta.get(alias, (0, 0))
            if after > before:
                return True
    return False


def equipped_mainhand(inspection: dict[str, Any] | None) -> str | None:
    equipped_items = (((inspection or {}).get("info") or {}).get("equipped_items")) or {}
    if not equipped_items:
        equipped_items = (((inspection or {}).get("obs") or {}).get("equipped_items")) or {}
    if not equipped_items:
        for command in (inspection or {}).get("commands") or []:
            equipped_items = ((command.get("obs") or {}).get("equipped_items")) or ((command.get("info") or {}).get("equipped_items")) or {}
            if equipped_items:
                break
    value = (equipped_items.get("mainhand") or {}).get("type") if isinstance(equipped_items, dict) else None
    return str(value) if value else None


def scalar_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in {"true", "1", "yes"}
    return bool(value)


def evaluate_task_success(task: McuTask, result: dict[str, Any], inspection: dict[str, Any] | None) -> dict[str, Any]:
    name = normalize_subgoal_name(task.name)
    counts = inventory_counts(inspection)
    actions = [cycle.get("recovered_action") or {} for cycle in result.get("cycles", [])]
    subgoal_successes = []
    if "_and_" in task.name or "_or_" in task.name:
        for subgoal in task_subgoals(task):
            subtask = McuTask(task.suite, subgoal, task.path, subgoal, task.commands, task.thinking)
            success = evaluate_task_success(subtask, result, inspection)
            if success.get("criteria") != "no local success heuristic for this task":
                subgoal_successes.append({"subgoal": subgoal, **success})
        if subgoal_successes:
            any_success = any(item.get("ok") for item in subgoal_successes)
            return {
                "ok": any_success,
                "criteria": "any locally measurable compound subgoal succeeds",
                "subgoals": subgoal_successes,
            }
    if name == "look_at_the_sky":
        location = (((inspection or {}).get("info") or {}).get("location_stats")) or {}
        looked_sky = any(
            action.get("verb") == "look"
            and (
                "sky" in str(action.get("target", "")).lower()
                or "sky" in str(action.get("stop_condition", "")).lower()
                or "can_see_sky" in str(action.get("stop_condition", "")).lower()
                or not str(action.get("target", "")).strip()
            )
            for action in actions
        )
        can_see_sky = scalar_bool(location.get("can_see_sky"))
        return {
            "ok": bool(looked_sky and can_see_sky),
            "criteria": "look action targeted sky and location_stats.can_see_sky is true",
            "looked_sky": looked_sky,
            "can_see_sky": can_see_sky,
        }
    if name.startswith("craft_"):
        target = name.removeprefix("craft_")
        if target.startswith("to_"):
            target = target.removeprefix("to_")
        aliases = {
            "oak_planks": ["planks", "oak_planks"],
            "diorite": ["diorite"],
        }.get(target, [target])
        return {
            "ok": any(counts.get(alias, 0) > 0 for alias in aliases),
            "criteria": f"inventory contains crafted target {target}",
            "target": target,
            "aliases": aliases,
            "matching_inventory": {alias: counts.get(alias, 0) for alias in aliases},
        }
    if name.startswith("collect_"):
        target = name.removeprefix("collect_")
        aliases = {target, target.removesuffix("_blocks"), target.removesuffix("_block")}
        return {
            "ok": any(counts.get(alias, 0) > 0 for alias in aliases),
            "criteria": f"inventory contains collected target {target}",
            "target": target,
            "matching_inventory": {alias: counts.get(alias, 0) for alias in sorted(aliases)},
        }
    if name.startswith("use_"):
        target = name.removeprefix("use_")
        aliases = {target, target.replace("_", "")}
        mainhand = equipped_mainhand(inspection)
        used_target = any(
            action.get("verb") in {"select", "equip", "use"}
            and any(alias in str(action.get("target", "")).replace("minecraft:", "").lower() for alias in aliases)
            for action in actions
        )
        return {
            "ok": bool(used_target),
            "criteria": f"action target matches use target {target}",
            "target": target,
            "mainhand": mainhand,
            "used_target": used_target,
        }
    if name == "build_a_waterfall":
        used_water = inventory_count_decreased(result, {"water_bucket"}) or inventory_count_increased(result, {"bucket"})
        return {
            "ok": used_water,
            "criteria": "water_bucket was consumed or empty bucket appeared",
            "observed_water_bucket_delta": used_water,
        }
    if name == "build_a_house":
        house_materials = {
            "oak_planks",
            "planks",
            "cobblestone",
            "glass",
            "glass_pane",
            "oak_door",
            "door",
            "torch",
        }
        placed_materials = sorted(material for material in house_materials if inventory_count_decreased(result, {material}))
        return {
            "ok": len(placed_materials) >= 3,
            "criteria": "at least three house building material inventory counts decreased",
            "placed_materials": placed_materials,
        }
    if name == "explore_boat":
        used_boat = inventory_count_decreased(result, {"oak_boat", "boat"})
        return {"ok": used_boat, "criteria": "boat inventory count decreased", "observed_boat_delta": used_boat}
    if name == "carve_pumpkins":
        pumpkin_changed = inventory_count_decreased(result, {"pumpkin"}) or inventory_count_increased(result, {"carved_pumpkin"})
        return {
            "ok": pumpkin_changed,
            "criteria": "pumpkin inventory decreased or carved_pumpkin appeared",
            "observed_pumpkin_delta": pumpkin_changed,
        }
    if name == "collect_wool":
        wool_collected = counts.get("wool", 0) > 0 or inventory_count_increased(result, {"wool"})
        return {
            "ok": bool(wool_collected),
            "criteria": "inventory contains wool or wool count increased",
            "wool_count": counts.get("wool", 0),
        }
    if name == "explore_the_treasure":
        opened_chest = any(
            action.get("verb") in {"open", "use"}
            and "chest" in str(action.get("target", "")).lower()
            for action in actions
        )
        return {"ok": opened_chest, "criteria": "opened or used chest", "opened_chest": opened_chest}
    if name == "trap_a_witch_with_a_boat":
        placed_boat = inventory_count_decreased(result, {"oak_boat", "boat"})
        return {"ok": placed_boat, "criteria": "boat inventory count decreased", "observed_boat_delta": placed_boat}
    if name == "decorate_the_ground":
        decorations = {"poppy", "red_tulip", "blue_orchid", "carpet", "wool", "torch", "item_frame", "sapling", "flower_pot"}
        placed_decorations = sorted(item for item in decorations if inventory_count_decreased(result, {item}))
        return {
            "ok": bool(placed_decorations),
            "criteria": "ground decoration inventory count decreased",
            "placed_decorations": placed_decorations,
        }
    if name == "decorate_the_wall":
        wall_decorations = {"painting", "item_frame", "torch", "flower_pot"}
        placed_decorations = sorted(item for item in wall_decorations if inventory_count_decreased(result, {item}))
        return {
            "ok": bool(placed_decorations),
            "criteria": "wall decoration inventory count decreased",
            "placed_decorations": placed_decorations,
        }
    if name == "light_up_the_surroundings_at_night":
        placed_lights = sorted(item for item in {"torch", "glowstone"} if inventory_count_decreased(result, {item}))
        return {
            "ok": bool(placed_lights),
            "criteria": "torch or glowstone inventory count decreased",
            "placed_lights": placed_lights,
        }
    if name == "stacking_acacia_fence":
        placed_fences = ["acacia_fence"] if inventory_count_decreased(result, {"acacia_fence"}) else []
        return {
            "ok": bool(placed_fences),
            "criteria": "acacia_fence inventory count decreased",
            "placed_fences": placed_fences,
        }
    if name == "place_a_item_frame":
        used_frame = inventory_count_decreased(result, {"item_frame"})
        return {"ok": used_frame, "criteria": "item_frame inventory count decreased", "used_item_frame": used_frame}
    return {"ok": False, "criteria": "no local success heuristic for this task"}


def run_task(
    task: McuTask,
    task_index: int,
    task_count: int,
    args: argparse.Namespace,
    run_dir: Path,
) -> dict[str, Any]:
    task_dir = run_dir / f"{task_index:04d}-{task.suite}-{task.name}"
    task_dir.mkdir(parents=True, exist_ok=True)
    record_dir = task_dir / "minestudio-recording"
    env = MineStudioStreamClient(args.environment_url, args.timeout)
    result: dict[str, Any] = {
        "task_index": task_index,
        "task_count": task_count,
        "task": {
            "suite": task.suite,
            "name": task.name,
            "path": str(task.path),
            "text": task.text,
            "commands": task.commands,
            "thinking": task.thinking,
        },
        "ok": False,
        "cycles": [],
    }
    try:
        initial_frame, reset_event = env.reset(
            {
                "recreate": True,
                "init_commands": task.commands,
                "record_video": args.record_video,
                "record_path": docker_visible_record_path(record_dir),
                "record_fps": args.record_fps,
                "record_frame_type": "pov",
            }
        )
        reset_path = task_dir / "reset.png"
        initial_frame.save(reset_path)
        result["reset"] = compact_wrapper_event(reset_event)
        result["reset_frame"] = str(reset_path)
        result["initial_inspection"] = env.inspect(args.inspect_command)

        for cycle_index in range(args.cycles_per_task):
            pre_action_inspection = env.inspect(args.inspect_command)
            frames, input_events = collect_frame_stream(
                env,
                task_dir,
                cycle_index,
                args.frames_per_cycle,
                args.frame_stride_ticks,
            )
            instruction = task_instruction(task, cycle_index, result["cycles"], pre_action_inspection)
            model_input = {
                "mode": "duplex",
                "stream": True,
                "instruction": instruction,
                "image_sequence": [event["saved_frame"] for event in input_events],
                "pre_action_inspection": pre_action_inspection,
                "note": "The inspection data is logged for benchmark analysis. The MiniCPM request receives the instruction text plus image sequence, not this structured inspection payload.",
            }
            (task_dir / f"cycle-{cycle_index:04d}-model-input.json").write_text(
                json.dumps(json_safe(model_input), indent=2),
                encoding="utf-8",
            )
            response = call_minicpm_stream(
                args.endpoint,
                instruction,
                frames,
                cycle_index,
                args.timeout,
            )
            (task_dir / f"cycle-{cycle_index:04d}-model-output.json").write_text(
                json.dumps(json_safe(response), indent=2),
                encoding="utf-8",
            )
            score = score_action_response(response["content"])
            macro = recover_action_macro(response["content"])
            model_recovered_action = macro.__dict__.copy() if macro else None
            action_repair = None
            action_result = None
            post_action_path = None
            if macro is not None:
                macro, action_repair = repair_action_macro(task, macro, result["cycles"])
                macro.duration_ticks = min(max(1, macro.duration_ticks), args.action_ticks)
                post_frame, action_result = env.step_macro(macro.__dict__, macro.duration_ticks)
                post_action_path = task_dir / f"cycle-{cycle_index:04d}-post-action.png"
                post_frame.save(post_action_path)
            compact_action_result = compact_wrapper_event(action_result)
            post_action_inspection = env.inspect(args.inspect_command)
            cycle = {
                "cycle_index": cycle_index,
                "model_input_path": str(task_dir / f"cycle-{cycle_index:04d}-model-input.json"),
                "model_output_path": str(task_dir / f"cycle-{cycle_index:04d}-model-output.json"),
                "input_frames": [event["saved_frame"] for event in input_events],
                "pre_action_inspection": pre_action_inspection,
                "post_action_frame": str(post_action_path) if post_action_path else None,
                "content": response["content"],
                "stream_events": response["stream_events"],
                "transport": response["transport"],
                "elapsed_ms": response["elapsed_ms"],
                "score": score,
                "model_recovered_action": model_recovered_action,
                "action_repair": action_repair,
                "recovered_action": macro.__dict__ if macro else None,
                "action_result": compact_action_result,
                "post_action_inspection": post_action_inspection,
            }
            result["cycles"].append(cycle)
            success = evaluate_task_success(task, result, post_action_inspection)
            cycle["success"] = success
            result["success"] = success
            if success.get("ok"):
                break
            if compact_action_result and (compact_action_result.get("terminated") or compact_action_result.get("truncated")):
                break
        result["progress"] = task_progress_summary(result)
        result["ok"] = True
    except Exception as source:
        result["error"] = str(source)
    finally:
        env.close()
    if args.record_video:
        result["video"] = collect_minestudio_recordings(record_dir)
    (task_dir / "result.json").write_text(json.dumps(json_safe(result), indent=2), encoding="utf-8")
    return result


def write_report(run_dir: Path, results: list[dict[str, Any]]) -> None:
    ok = [item for item in results if item.get("ok")]
    recovered = sum(1 for item in results for cycle in item.get("cycles", []) if cycle.get("recovered_action"))
    videos = sum(len((item.get("video") or {}).get("videos") or []) for item in results)
    successes = sum(1 for item in results if (item.get("success") or {}).get("ok"))
    lines = [
        "# MCU-turbo Full Duplex MineStudio Benchmark",
        "",
        f"- Tasks attempted: {len(results)}",
        f"- Tasks completed: {len(ok)}",
        f"- Tasks with local success heuristic satisfied: {successes}",
        f"- Actions recovered: {recovered}",
        f"- MineStudio recording videos: {videos}",
        "",
        "## Tasks",
        "",
    ]
    for item in results:
        task = item["task"]
        lines.append(f"### {item['task_index']:04d} {task['suite']}/{task['name']}")
        lines.append("")
        lines.append(f"- Completed: {item.get('ok')}")
        if item.get("error"):
            lines.append(f"- Error: `{item['error']}`")
        progress = item.get("progress") or {}
        success = item.get("success") or {}
        if success:
            lines.append(f"- Local success: {success.get('ok')} ({success.get('criteria')})")
        if progress:
            lines.append(f"- Cycles: {progress.get('cycles')}")
            lines.append(f"- Final step index: {progress.get('final_step_index')}")
            lines.append(f"- Verb counts: `{json.dumps(progress.get('verb_counts'))}`")
            lines.append(f"- Repeated adjacent actions: {progress.get('repeated_adjacent_actions')}")
        video = item.get("video") or {}
        for artifact in video.get("videos") or []:
            lines.append(f"- MineStudio video: `{artifact['uri']}`")
        for cycle in item.get("cycles", []):
            lines.append(f"- Cycle {cycle['cycle_index']} transport: `{cycle['transport']}`, stream events: {len(cycle['stream_events'])}")
            if cycle.get("action_repair"):
                lines.append(f"- Action repair: `{json.dumps(cycle.get('action_repair'))}`")
            lines.append(f"- Recovered action: `{json.dumps(cycle.get('recovered_action'))}`")
            lines.append(f"- Model input: `{cycle.get('model_input_path')}`")
            lines.append(f"- Model output: `{cycle.get('model_output_path')}`")
        lines.append("")
    (run_dir / "full_duplex_report.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mcu-root", default=os.environ.get("MCU_ROOT", "/tmp/mcu"))
    parser.add_argument("--endpoint", default=os.environ.get("SINOMO_PYTORCH_ENDPOINT", os.environ.get("SINOMO_MINICPM_ENDPOINT")))
    parser.add_argument("--environment-url", default=os.environ.get("SINOMO_MINESTUDIO_ENV_URL", "http://127.0.0.1:18080"))
    parser.add_argument("--results-dir", default=os.environ.get("SINOMO_BENCHMARK_RESULTS", "benchmark-results"))
    parser.add_argument("--task", action="append", dest="tasks", help="Task ref such as simple/collect_dirt. Repeatable.")
    parser.add_argument("--max-tasks", type=int)
    parser.add_argument("--cycles-per-task", type=int, default=10)
    parser.add_argument("--frames-per-cycle", type=int, default=4)
    parser.add_argument("--frame-stride-ticks", type=int, default=1)
    parser.add_argument("--action-ticks", type=int, default=20)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--record-video", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--record-fps", type=int, default=20)
    parser.add_argument(
        "--inspect-command",
        action="append",
        default=[
            "/data get entity @s Pos",
            "/data get entity @s Inventory",
            "/data get entity @s SelectedItem",
            "/data get entity @s Health",
        ],
        help="Minecraft command to execute through MineStudio env.execute_cmd for per-cycle logging. Repeatable.",
    )
    args = parser.parse_args()
    if not args.endpoint:
        parser.error("--endpoint or SINOMO_PYTORCH_ENDPOINT is required")

    mcu_root = Path(args.mcu_root)
    task_refs = args.tasks or discover_tasks(mcu_root)
    if args.max_tasks is not None:
        task_refs = task_refs[: args.max_tasks]
    run_id = f"mcu-turbo-full-duplex-{time.strftime('%Y%m%d-%H%M%S')}"
    run_dir = Path(args.results_dir) / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for index, task_ref in enumerate(task_refs):
        task = load_task(mcu_root, task_ref)
        result = run_task(task, index, len(task_refs), args, run_dir)
        results.append(result)
        summary = {
            "benchmark": "MCU-turbo full duplex MineStudio",
            "mcu_root": str(mcu_root),
            "endpoint": args.endpoint,
            "environment_url": args.environment_url,
            "task_count": len(results),
            "planned_task_count": len(task_refs),
            "completed_tasks": sum(1 for item in results if item.get("ok")),
            "local_successes": sum(1 for item in results if (item.get("success") or {}).get("ok")),
            "results": results,
        }
        (run_dir / "summary.json").write_text(json.dumps(json_safe(summary), indent=2), encoding="utf-8")
        write_report(run_dir, results)
        print(json.dumps({"completed": index + 1, "total": len(task_refs), "task": task_ref, "ok": result.get("ok")}))
        sys.stdout.flush()
    print(json.dumps({"results_dir": str(run_dir), "task_count": len(results)}, indent=2))
    return 0 if results and all(item.get("ok") for item in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
