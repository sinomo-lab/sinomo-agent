#!/usr/bin/env python3
"""Live/model eval scenarios for Sinomo skill disclosure and evolution.

Plan mode lists the verification tasks. Fixture mode exercises the scoring path
without model calls. Run mode seeds benchmark-only skills into a temporary
SINOMO_DATA_DIR, calls the configured Sinomo CLI, and verifies that a real model
can use skill_search/skill_describe to answer hidden-skill tasks and
skill_update to persist a learned shortcut for a follow-up run.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path


def find_repo_root(start: Path) -> Path:
    for directory in (start.parent, *start.parents):
        if (directory / "Cargo.toml").exists() and (directory / "crates" / "sinomo").exists():
            return directory
    raise SystemExit(f"failed to find Sinomo workspace root from {start}")


REPO_ROOT = find_repo_root(Path(__file__).resolve())
DEFAULT_RESULTS_DIR = REPO_ROOT / "benchmark-results" / "agent-skill"
DEFAULT_MODEL_TIMEOUT_SECONDS = 900


def utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class SkillFixture:
    skill_id: str
    version: str
    text: str


@dataclass(frozen=True)
class SkillEvalScenario:
    scenario_id: str
    title: str
    purpose: str
    skills: list[SkillFixture]
    prompt: str
    expected_markers: list[str]
    forbidden_markers: list[str] = field(default_factory=list)
    requires_update: bool = False
    update_skill_id: str | None = None
    required_updated_markers: list[str] = field(default_factory=list)
    followup_prompt: str | None = None
    followup_expected_markers: list[str] = field(default_factory=list)
    fixture_response: str = ""
    fixture_followup_response: str = ""


def scenario_definitions() -> list[SkillEvalScenario]:
    hidden_skill = SkillFixture(
        skill_id="benchmark.skill.rune-routing",
        version="1.0.0",
        text="""---
name: benchmark.skill.rune-routing
description: Benchmark-only routing procedure for amber depot checks.
target_agents:
  - main
activation_tags:
  - benchmark
  - skill-eval
---

# Rune Routing

Use this skill only for amber depot routing checks.

When asked for the amber depot final route, report exactly:

`RUNE-CHECKPOINT-41 via north dock`

Do not use the decoy violet-depot route for amber depot checks.
""",
    )
    decoy_skill = SkillFixture(
        skill_id="benchmark.skill.rune-routing-decoy",
        version="1.0.0",
        text="""---
name: benchmark.skill.rune-routing-decoy
description: Benchmark-only decoy routing procedure for violet depot checks.
target_agents:
  - main
activation_tags:
  - benchmark
  - skill-eval
---

# Rune Routing Decoy

Use this skill only for violet depot routing checks.

When asked for the violet depot final route, report:

`RUNE-CHECKPOINT-00 via south gate`
""",
    )
    shortcut_skill = SkillFixture(
        skill_id="benchmark.skill.shortcut-ledger",
        version="1.0.0",
        text="""---
name: benchmark.skill.shortcut-ledger
description: Benchmark-only long-form ledger procedure that should learn shortcuts.
target_agents:
  - main
activation_tags:
  - benchmark
  - skill-eval
---

# Shortcut Ledger

Current long-form process:

1. For a cobalt ledger checksum, multiply 17 by 19.
2. Subtract 8.
3. Report the checksum with prefix `LEDGER-LONG`.

No shortcut has been recorded yet.
""",
    )
    return [
        SkillEvalScenario(
            scenario_id="hidden-skill-disclosure",
            title="Hidden skill text is required to answer",
            purpose=(
                "Verifies that the model uses skill_search/skill_describe instead of guessing; "
                "the expected amber route marker exists only inside SKILL.md."
            ),
            skills=[hidden_skill, decoy_skill],
            prompt=(
                "Benchmark task SKILL-READ-001.\n"
                "A task-specific procedure may be available as an active skill. Search and "
                "describe relevant skills before answering. Do not guess if you have not inspected "
                "the skill text.\n\n"
                "Question: For the amber depot routing check, what final route should be reported? "
                "Answer with only the route code and route phrase."
            ),
            expected_markers=["RUNE-CHECKPOINT-41", "north dock"],
            forbidden_markers=["RUNE-CHECKPOINT-00", "south gate"],
            fixture_response="RUNE-CHECKPOINT-41 via north dock",
        ),
        SkillEvalScenario(
            scenario_id="skill-update-followup-shortcut",
            title="Learned shortcut is persisted and used later",
            purpose=(
                "Verifies that a model calls skill_update with full replacement SKILL.md after a "
                "shortcut is revealed, and that a fresh follow-up run can use the updated skill."
            ),
            skills=[shortcut_skill],
            prompt=(
                "Benchmark task SKILL-UPDATE-001.\n"
                "Use active skills to understand the cobalt ledger process. Then persist the newly "
                "revealed shortcut by calling skill_update for skill "
                "`benchmark.skill.shortcut-ledger` with a full replacement SKILL.md.\n\n"
                "The replacement SKILL.md must preserve the existing purpose and include this exact "
                "new shortcut sentence: `Shortcut: Cobalt ledger shortcut is LEDGER-SHORTCUT-315.`\n"
                "It must also say to use the shortcut on follow-up runs instead of recomputing.\n\n"
                "Do not edit files directly for this update; use the skill_update tool. After the "
                "tool succeeds, briefly report the current long-form checksum result."
            ),
            expected_markers=[],
            requires_update=True,
            update_skill_id="benchmark.skill.shortcut-ledger",
            required_updated_markers=[
                "Shortcut: Cobalt ledger shortcut is LEDGER-SHORTCUT-315.",
                "follow-up runs",
            ],
            followup_prompt=(
                "Benchmark task SKILL-UPDATE-FOLLOWUP.\n"
                "Use active skills. What cobalt ledger shortcut should be used now? "
                "Answer with only the shortcut token."
            ),
            followup_expected_markers=["LEDGER-SHORTCUT-315"],
            fixture_response="Updated benchmark.skill.shortcut-ledger; current long-form result is LEDGER-LONG-315.",
            fixture_followup_response="LEDGER-SHORTCUT-315",
        ),
    ]


def validate_scenario_definitions() -> None:
    seen = set()
    for scenario in scenario_definitions():
        if scenario.scenario_id in seen:
            raise SystemExit(f"duplicate scenario id: {scenario.scenario_id}")
        seen.add(scenario.scenario_id)
        if not scenario.skills:
            raise SystemExit(f"{scenario.scenario_id} has no skills")
        if scenario.requires_update:
            if not scenario.update_skill_id:
                raise SystemExit(f"{scenario.scenario_id} requires update_skill_id")
            if not scenario.required_updated_markers:
                raise SystemExit(f"{scenario.scenario_id} has no update markers")
            if not scenario.followup_prompt or not scenario.followup_expected_markers:
                raise SystemExit(f"{scenario.scenario_id} has no follow-up check")


def selected_scenarios(ids: list[str]) -> list[SkillEvalScenario]:
    scenarios = scenario_definitions()
    if not ids:
        return scenarios
    by_id = {scenario.scenario_id: scenario for scenario in scenarios}
    unknown = sorted(set(ids).difference(by_id))
    if unknown:
        raise SystemExit(f"unknown scenario id(s): {', '.join(unknown)}")
    return [by_id[scenario_id] for scenario_id in ids]


def contains_marker(text: str, marker: str) -> bool:
    return marker.lower() in text.lower()


def marker_hits(text: str, markers: list[str]) -> dict[str, bool]:
    return {marker: contains_marker(text, marker) for marker in markers}


def write_json(path: Path | None, payload: dict[str, object]) -> None:
    if not path:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


class ProgressWriter:
    def __init__(self, path: Path | None):
        self.path = path
        if path:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("", encoding="utf-8")

    def write(self, event: dict[str, object]) -> None:
        if not self.path:
            return
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"time": utc_now(), **event}) + "\n")


def source_data_dir(args: argparse.Namespace) -> Path:
    if args.config_source_data_dir:
        return args.config_source_data_dir
    env_data_dir = os.environ.get("SINOMO_DATA_DIR", "").strip()
    if env_data_dir:
        return Path(env_data_dir)
    return Path.home() / ".sinomo"


def copy_path_if_present(source_root: Path, dest_root: Path, relative: str) -> None:
    source = source_root / relative
    if not source.exists():
        return
    dest = dest_root / relative
    if source.is_dir():
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(source, dest)
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, dest)


def copy_provider_state(source_root: Path, dest_root: Path) -> list[str]:
    copied: list[str] = []
    for relative in [
        "configs/lm",
        "config/models.json",
        "providers",
    ]:
        before = (dest_root / relative).exists()
        copy_path_if_present(source_root, dest_root, relative)
        if (dest_root / relative).exists() and not before:
            copied.append(relative)
    return copied


def command_for_args(args: argparse.Namespace) -> list[str]:
    if args.sinomo_bin.exists():
        return [str(args.sinomo_bin)]
    return ["cargo", "run", "-p", "sinomo", "--"]


def run_sinomo(
    args: argparse.Namespace,
    data_dir: Path,
    subargs: list[str],
    *,
    timeout_seconds: int | None = None,
    redact_prompt: bool = False,
) -> dict[str, object]:
    command = [*command_for_args(args), *subargs]
    started = utc_now()
    display_command = [
        "<prompt>" if redact_prompt and index > 0 and command[index - 1] == "--prompt" else part
        for index, part in enumerate(command)
    ]
    if args.dry_run:
        return {
            "command": display_command,
            "started_at": started,
            "completed_at": utc_now(),
            "exit_code": 0,
            "stdout": "",
            "stderr": "",
            "dry_run": True,
        }
    env = os.environ.copy()
    env["SINOMO_DATA_DIR"] = str(data_dir)
    try:
        completed = subprocess.run(
            command,
            cwd=REPO_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            env=env,
            timeout=timeout_seconds,
        )
        return {
            "command": display_command,
            "started_at": started,
            "completed_at": utc_now(),
            "exit_code": completed.returncode,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
            "dry_run": False,
        }
    except subprocess.TimeoutExpired as error:
        return {
            "command": display_command,
            "started_at": started,
            "completed_at": utc_now(),
            "exit_code": 124,
            "stdout": error.stdout or "",
            "stderr": f"timed out after {timeout_seconds}s",
            "dry_run": False,
        }


def write_skill_files(workspace: Path, scenario: SkillEvalScenario) -> list[tuple[SkillFixture, Path]]:
    paths: list[tuple[SkillFixture, Path]] = []
    for skill in scenario.skills:
        path = workspace / "skills" / scenario.scenario_id / skill.skill_id / "SKILL.md"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(skill.text, encoding="utf-8")
        paths.append((skill, path))
    return paths


def import_skills(
    args: argparse.Namespace,
    data_dir: Path,
    scenario: SkillEvalScenario,
    workspace: Path,
) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    for skill, path in write_skill_files(workspace, scenario):
        results.append(
            run_sinomo(
                args,
                data_dir,
                [
                    "skill",
                    "import",
                    str(path),
                    "--id",
                    skill.skill_id,
                    "--version",
                    skill.version,
                    "--activate",
                    "--scope",
                    "cluster",
                    "--json",
                ],
            )
        )
    return results


def import_single_skill_text(
    args: argparse.Namespace,
    data_dir: Path,
    skill_id: str,
    version: str,
    text: str,
    workspace: Path,
) -> dict[str, object]:
    path = workspace / "followup-skill" / skill_id / "SKILL.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return run_sinomo(
        args,
        data_dir,
        [
            "skill",
            "import",
            str(path),
            "--id",
            skill_id,
            "--version",
            version,
            "--activate",
            "--scope",
            "cluster",
            "--json",
        ],
    )


def show_skill_text(
    args: argparse.Namespace,
    data_dir: Path,
    skill_id: str,
) -> dict[str, object]:
    return run_sinomo(args, data_dir, ["skill", "show", skill_id, "--text"])


def run_agent_prompt(
    args: argparse.Namespace,
    data_dir: Path,
    prompt: str,
) -> dict[str, object]:
    command = [
        "agent",
        "run",
        "--provider",
        args.provider,
        "--model",
        args.model,
        "--prompt",
        prompt,
    ]
    if args.account:
        command.extend(["--account", args.account])
    return run_sinomo(
        args,
        data_dir,
        command,
        timeout_seconds=args.model_timeout_seconds,
        redact_prompt=True,
    )


def fixture_updated_skill_text(scenario: SkillEvalScenario) -> str:
    base = next(skill for skill in scenario.skills if skill.skill_id == scenario.update_skill_id)
    return (
        base.text.rstrip()
        + "\n\n# Learned Shortcut\n\n"
        + "Shortcut: Cobalt ledger shortcut is LEDGER-SHORTCUT-315.\n\n"
        + "Use the shortcut on follow-up runs instead of recomputing.\n"
    )


def evaluate_markers(
    text: str,
    expected: list[str],
    forbidden: list[str],
) -> dict[str, object]:
    expected_hits = marker_hits(text, expected)
    forbidden_hits = marker_hits(text, forbidden)
    return {
        "expected_hits": expected_hits,
        "forbidden_hits": forbidden_hits,
        "missing_expected": [marker for marker, hit in expected_hits.items() if not hit],
        "present_forbidden": [marker for marker, hit in forbidden_hits.items() if hit],
    }


def new_data_dir(
    args: argparse.Namespace,
    scenario_id: str,
    suffix: str,
) -> tempfile.TemporaryDirectory[str]:
    if args.data_dir:
        args.data_dir.mkdir(parents=True, exist_ok=True)
        return tempfile.TemporaryDirectory(
            prefix=f"sinomo-skill-eval-{scenario_id}-{suffix}-",
            dir=args.data_dir,
        )
    return tempfile.TemporaryDirectory(prefix=f"sinomo-skill-eval-{scenario_id}-{suffix}-")


def prepared_data_dir(
    args: argparse.Namespace,
    scenario_id: str,
    suffix: str,
) -> tuple[Path, tempfile.TemporaryDirectory[str], list[str]]:
    temp = new_data_dir(args, scenario_id, suffix)
    data_dir = Path(temp.name)  # type: ignore[union-attr]
    data_dir.mkdir(parents=True, exist_ok=True)
    copied = copy_provider_state(source_data_dir(args), data_dir)
    return data_dir, temp, copied


def evaluate_scenario(
    args: argparse.Namespace,
    scenario: SkillEvalScenario,
    progress: ProgressWriter,
) -> dict[str, object]:
    progress.write({"scenario_id": scenario.scenario_id, "event": "started"})
    workspace = args.results_dir / "work" / scenario.scenario_id
    workspace.mkdir(parents=True, exist_ok=True)

    first_response = scenario.fixture_response
    first_invocation: dict[str, object] | None = None
    import_invocations: list[dict[str, object]] = []
    update_text = ""
    update_invocation: dict[str, object] | None = None
    update_markers: dict[str, bool] = {}
    followup_response = scenario.fixture_followup_response
    followup_invocation: dict[str, object] | None = None
    followup_import_invocation: dict[str, object] | None = None
    copied_provider_state: list[str] = []

    if args.mode == "fixture":
        update_text = fixture_updated_skill_text(scenario) if scenario.requires_update else ""
    else:
        data_dir, temp, copied_provider_state = prepared_data_dir(
            args, scenario.scenario_id, "first"
        )
        try:
            import_invocations = import_skills(args, data_dir, scenario, workspace)
            failed_import = next(
                (result for result in import_invocations if result["exit_code"] != 0),
                None,
            )
            if failed_import:
                first_invocation = failed_import
                first_response = failed_import.get("stdout", "") or failed_import.get("stderr", "")
            else:
                first_invocation = run_agent_prompt(args, data_dir, scenario.prompt)
                first_response = str(first_invocation.get("stdout", ""))
                if scenario.requires_update and scenario.update_skill_id:
                    update_invocation = show_skill_text(args, data_dir, scenario.update_skill_id)
                    update_text = str(update_invocation.get("stdout", ""))
                    update_skill_version = next(
                        skill.version
                        for skill in scenario.skills
                        if skill.skill_id == scenario.update_skill_id
                    )
                    followup_data_dir, followup_temp, followup_copied = prepared_data_dir(
                        args, scenario.scenario_id, "followup"
                    )
                    copied_provider_state.extend(
                        [f"followup:{relative}" for relative in followup_copied]
                    )
                    try:
                        followup_import_invocation = import_single_skill_text(
                            args,
                            followup_data_dir,
                            scenario.update_skill_id,
                            update_skill_version,
                            update_text,
                            workspace,
                        )
                        if followup_import_invocation["exit_code"] != 0:
                            followup_response = str(
                                followup_import_invocation.get("stderr", "")
                            )
                        else:
                            followup_invocation = run_agent_prompt(
                                args,
                                followup_data_dir,
                                scenario.followup_prompt or "",
                            )
                            followup_response = str(followup_invocation.get("stdout", ""))
                    finally:
                        if followup_temp and not args.keep_temp_data_dirs:
                            followup_temp.cleanup()
        finally:
            if temp and not args.keep_temp_data_dirs:
                temp.cleanup()

    first_markers = evaluate_markers(
        first_response, scenario.expected_markers, scenario.forbidden_markers
    )
    if scenario.requires_update:
        update_markers = marker_hits(update_text, scenario.required_updated_markers)
    followup_markers = (
        evaluate_markers(followup_response, scenario.followup_expected_markers, {})
        if scenario.followup_expected_markers
        else {
            "expected_hits": {},
            "forbidden_hits": {},
            "missing_expected": [],
            "present_forbidden": [],
        }
    )

    failed_invocations = [
        invocation
        for invocation in [
            *import_invocations,
            first_invocation,
            update_invocation,
            followup_import_invocation,
            followup_invocation,
        ]
        if invocation and invocation.get("exit_code") not in (0, None)
    ]
    missing_update = [
        marker for marker, hit in update_markers.items() if not hit
    ]
    passed = (
        not failed_invocations
        and not first_markers["missing_expected"]
        and not first_markers["present_forbidden"]
        and not missing_update
        and not followup_markers["missing_expected"]
    )
    result = {
        "scenario_id": scenario.scenario_id,
        "title": scenario.title,
        "purpose": scenario.purpose,
        "passed": passed,
        "first_response": first_response.strip(),
        "first_markers": first_markers,
        "requires_update": scenario.requires_update,
        "update_skill_id": scenario.update_skill_id,
        "update_marker_hits": update_markers,
        "missing_update_markers": missing_update,
        "followup_response": followup_response.strip() if scenario.requires_update else None,
        "followup_markers": followup_markers,
        "copied_provider_state_paths": copied_provider_state,
        "invocations": {
            "imports": import_invocations,
            "first": first_invocation,
            "show_updated_skill": update_invocation,
            "followup_import": followup_import_invocation,
            "followup": followup_invocation,
        },
    }
    progress.write(
        {
            "scenario_id": scenario.scenario_id,
            "event": "completed",
            "passed": passed,
        }
    )
    return result


def aggregate_results(results: list[dict[str, object]]) -> dict[str, object]:
    passed_count = sum(1 for result in results if result["passed"])
    scenario_count = len(results)
    return {
        "scenario_count": scenario_count,
        "passed_scenario_count": passed_count,
        "failed_scenario_count": scenario_count - passed_count,
        "scenario_pass_rate": passed_count / max(1, scenario_count),
    }


def scenario_plan(scenario: SkillEvalScenario) -> dict[str, object]:
    payload = asdict(scenario)
    payload["skill_ids"] = [skill.skill_id for skill in scenario.skills]
    payload.pop("skills", None)
    return payload


def example_run_command(args: argparse.Namespace, summary_out: Path) -> str:
    command = [
        sys.executable,
        str(Path(__file__).relative_to(REPO_ROOT)),
        "--mode",
        "run",
        "--provider",
        args.provider,
        "--model",
        args.model,
        "--summary-out",
        str(summary_out),
    ]
    if args.account:
        command.extend(["--account", args.account])
    return " ".join(shlex.quote(part) for part in command)


def build_plan(args: argparse.Namespace) -> dict[str, object]:
    scenarios = selected_scenarios(args.scenario)
    summary_out = args.summary_out or args.results_dir / "summary.json"
    return {
        "name": "agent-skill-quality",
        "repo_root": str(REPO_ROOT),
        "results_dir": str(args.results_dir),
        "mode": args.mode,
        "provider": args.provider,
        "model": args.model,
        "scenario_count": len(scenarios),
        "evaluation_axes": [
            "progressive-skill-disclosure",
            "skill-selection-against-decoys",
            "skill-evolution-writeback",
            "fresh-session-shortcut-reuse",
        ],
        "live_model_required_for_run": True,
        "harness_checks": {
            "fixture_mode_scores_expected_and_forbidden_markers": True,
            "run_mode_seeds_temp_skill_catalog": True,
            "run_mode_verifies_skill_update_text": True,
            "followup_uses_fresh_data_dir_except_provider_config": True,
        },
        "scenarios": [scenario_plan(scenario) for scenario in scenarios],
        "example_run_command": example_run_command(args, summary_out),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["plan", "fixture", "run"], default="plan")
    parser.add_argument("--scenario", action="append", default=[])
    parser.add_argument("--provider", default=os.environ.get("SINOMO_AGENT_PROVIDER", "codex"))
    parser.add_argument("--model", default=os.environ.get("SINOMO_AGENT_MODEL", "gpt-5.5"))
    parser.add_argument("--account")
    parser.add_argument(
        "--sinomo-bin", type=Path, default=REPO_ROOT / "target" / "debug" / "sinomo"
    )
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--summary-out", type=Path)
    parser.add_argument("--progress-out", type=Path)
    parser.add_argument("--data-dir", type=Path)
    parser.add_argument("--config-source-data-dir", type=Path)
    parser.add_argument("--model-timeout-seconds", type=int, default=DEFAULT_MODEL_TIMEOUT_SECONDS)
    parser.add_argument("--keep-temp-data-dirs", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    validate_scenario_definitions()
    args = parse_args()
    plan = build_plan(args)
    if args.mode == "plan":
        print(json.dumps(plan, indent=2))
        write_json(args.summary_out, plan)
        return 0

    progress = ProgressWriter(args.progress_out)
    results = [
        evaluate_scenario(args, scenario, progress)
        for scenario in selected_scenarios(args.scenario)
    ]
    aggregate = aggregate_results(results)
    passed = aggregate["failed_scenario_count"] == 0
    summary: dict[str, object] = {
        **plan,
        **aggregate,
        "status": "passed" if passed else "failed",
        "evaluated_at": utc_now(),
        "results": results,
    }
    write_json(args.summary_out or DEFAULT_RESULTS_DIR / "summary.json", summary)
    print(json.dumps(summary, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
