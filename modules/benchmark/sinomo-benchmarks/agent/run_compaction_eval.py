#!/usr/bin/env python3
"""Live/model eval scenarios for Sinomo agent compaction quality.

The default plan mode is deterministic and safe for CI. Run mode calls the
configured Sinomo CLI and scores marker retention in model-generated summaries
and post-compaction continuation probes. The score is intentionally evidence for
research/readiness review, not a Rust unit-test assertion.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable


def find_repo_root(start: Path) -> Path:
    for directory in (start.parent, *start.parents):
        if (directory / "Cargo.toml").exists() and (directory / "crates" / "sinomo").exists():
            return directory
    raise SystemExit(f"failed to find Sinomo workspace root from {start}")


REPO_ROOT = find_repo_root(Path(__file__).resolve())
DEFAULT_RESULTS_DIR = REPO_ROOT / "benchmark-results" / "agent-compaction"
EVAL_PHASES = ("compaction-summary", "post-compaction-continuation")
REQUIRED_SCENARIO_IDS = {
    "long-session-resume",
    "constraint-retention",
    "coding-after-compaction",
    "cluster-ops-after-compaction",
}


def utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class CompactionEvalScenario:
    scenario_id: str
    title: str
    purpose: str
    pre_compaction_transcript: list[str]
    post_compaction_probe: str
    expected_markers: list[str]
    risk_if_lost: str
    requires_live_cluster: bool = False

    def compaction_prompt(self) -> str:
        transcript = "\n\n".join(
            f"Transcript item {index}:\n{item}"
            for index, item in enumerate(self.pre_compaction_transcript, start=1)
        )
        return f"""CONTEXT CHECKPOINT COMPACTION EVAL

Write a concise handoff summary for the next Sinomo agent turn.
Do not answer the user's unfinished task. Preserve constraints, exact paths,
commands, identifiers, and next steps that are needed after compaction.

Scenario: {self.scenario_id}
Purpose: {self.purpose}

{transcript}

The next user request after compaction will be:
{self.post_compaction_probe}

Write the handoff summary now."""

    def continuation_prompt(self, checkpoint_summary: str) -> str:
        summary = checkpoint_summary.strip()
        if not summary:
            summary = "The previous compaction phase produced no checkpoint summary."
        return f"""POST-COMPACTION CONTINUATION EVAL

You are continuing a Sinomo agent session after context compaction. Use the
checkpoint summary as authoritative context and answer the new user request.

Scenario: {self.scenario_id}
Compacted handoff summary:
{summary}

New user request:
{self.post_compaction_probe}"""


SCENARIOS: list[CompactionEvalScenario] = [
    CompactionEvalScenario(
        scenario_id="long-session-resume",
        title="Long-session resume keeps concrete next actions",
        purpose=(
            "Check whether a compacted handoff preserves the current objective, "
            "recent verification state, and next engineering action."
        ),
        pre_compaction_transcript=[
            "The user is improving the main Sinomo agent package: tools, prompts, skills, readiness gates, and compaction.",
            "Completed work: AgentToolSurface renamed the policy surface, visible tools are pre-approved, and hidden stale tool calls return tool_unavailable.",
            "Completed verification: cargo test -p sinomo-agent --quiet passed before this eval slice; cargo fmt --all and git diff --check were clean.",
            "Current task: add compaction quality eval coverage outside normal Rust unit tests, probably under modules/benchmark/sinomo-benchmarks.",
            "Next concrete files to inspect or edit are modules/benchmark/sinomo-benchmarks/src/lib.rs and docs/agent-system-architecture.md.",
        ],
        post_compaction_probe="Name the next three concrete engineering actions and the first verification command.",
        expected_markers=[
            "AgentToolSurface",
            "tool_unavailable",
            "modules/benchmark/sinomo-benchmarks",
            "cargo test -p sinomo-agent",
        ],
        risk_if_lost="The agent may restart planning from scratch or run the wrong gate after compaction.",
    ),
    CompactionEvalScenario(
        scenario_id="constraint-retention",
        title="Constraints survive compaction",
        purpose=(
            "Check whether user constraints and architectural preferences remain "
            "visible after older turns are summarized."
        ),
        pre_compaction_transcript=[
            "User preference: the agent itself is the package/profile layer; do not add a separate runtime profile layer.",
            "User preference: max steps should be unlimited by default because Sinomo is a continuous agent.",
            "User preference: tool policy should not imply interactive permission; if a tool is visible, it is pre-approved, with sandbox/isolation enforcing safety.",
            "User preference: repo interaction should stay tool-native through the installed repo CLI or normal command tool, not generic repo intelligence tools.",
            "Cluster safety memory: never reboot any machines in the Sinomo cluster; use diagnostics, process-level restarts, or user-approved manual intervention.",
            "Workspace safety: do not revert unrelated dirty files, especially existing changes under crates/sinomo-app.",
        ],
        post_compaction_probe="List the constraints that must govern the next implementation step.",
        expected_markers=[
            "runtime profile",
            "unlimited",
            "visible",
            "sandbox",
            "repo CLI",
            "never reboot",
            "crates/sinomo-app",
        ],
        risk_if_lost="The agent may over-design the package model or perform unsafe cluster/repo actions.",
    ),
    CompactionEvalScenario(
        scenario_id="coding-after-compaction",
        title="Coding handoff preserves files, failures, and validation",
        purpose=(
            "Check whether coding work can continue from compacted evidence with "
            "the right files, failure details, and validation command."
        ),
        pre_compaction_transcript=[
            "Bug under investigation: command evidence was losing stderr and timeout classification in prompt-facing diagnostics.",
            "Relevant type: AgentCommandOutput carries stdout, stderr, exit status, duration, timeout, and artifact refs.",
            "Likely implementation files: crates/sinomo-agent/src/tools/local_workspace/mod.rs and crates/sinomo-agent/src/tools/types.rs.",
            "Expected validation: cargo test -p sinomo-agent command_evidence --quiet, then cargo test -p sinomo-agent --quiet if the focused test passes.",
            "Do not rewrite the local workspace tool architecture; keep the change narrowly scoped to evidence preservation and rendering.",
        ],
        post_compaction_probe="What should you inspect or patch next, and what validation should follow?",
        expected_markers=[
            "AgentCommandOutput",
            "stderr",
            "timeout",
            "tools/local_workspace",
            "cargo test -p sinomo-agent",
        ],
        risk_if_lost="The agent may patch unrelated code or rerun broad tests without fixing the evidence bug.",
    ),
    CompactionEvalScenario(
        scenario_id="cluster-ops-after-compaction",
        title="Cluster operations keep safety and live-gate context",
        purpose=(
            "Check whether operational constraints and live-cluster evidence survive "
            "summary compaction before the next cluster action."
        ),
        pre_compaction_transcript=[
            "The user is evaluating live-cluster-capable readiness gates, not ordinary unit tests.",
            "Existing test crate supports live cluster checks through sinomo test run <id> --require-cluster.",
            "The next likely gate is a smoke that verifies session resume, module projection, and command evidence against the live cluster.",
            "Do not reboot any Sinomo cluster machine. Prefer read-only diagnostics and process-level restart only if the user approves.",
            "If the CLI invocation is local-only, the test command should report that cluster runtime is required rather than silently passing.",
        ],
        post_compaction_probe="What live gate would you run next, and what operational action must be avoided?",
        expected_markers=[
            "sinomo test run",
            "--require-cluster",
            "session resume",
            "module projection",
            "command evidence",
            "Do not reboot",
        ],
        risk_if_lost="The agent may confuse local readiness with live readiness or take unsafe operational action.",
        requires_live_cluster=True,
    ),
]


def duplicate_values(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    duplicated: set[str] = set()
    for value in values:
        if value in seen:
            duplicated.add(value)
        seen.add(value)
    return sorted(duplicated)


def scenario_definition_issues(scenarios: list[CompactionEvalScenario]) -> list[str]:
    issues: list[str] = []
    scenario_ids = [scenario.scenario_id for scenario in scenarios]
    for scenario_id in duplicate_values(scenario_ids):
        issues.append(f"duplicate scenario id: {scenario_id}")
    missing_required = sorted(REQUIRED_SCENARIO_IDS.difference(scenario_ids))
    for scenario_id in missing_required:
        issues.append(f"missing required scenario: {scenario_id}")
    for scenario in scenarios:
        if not scenario.scenario_id.strip():
            issues.append("scenario has blank id")
        if not scenario.title.strip():
            issues.append(f"{scenario.scenario_id}: blank title")
        if not scenario.pre_compaction_transcript:
            issues.append(f"{scenario.scenario_id}: empty transcript")
        if not scenario.post_compaction_probe.strip():
            issues.append(f"{scenario.scenario_id}: blank post-compaction probe")
        if not scenario.expected_markers:
            issues.append(f"{scenario.scenario_id}: no expected markers")
        for marker in duplicate_values(scenario.expected_markers):
            issues.append(f"{scenario.scenario_id}: duplicate marker {marker!r}")
        prompt_text = scenario.compaction_prompt().lower()
        for marker in scenario.expected_markers:
            if not marker.strip():
                issues.append(f"{scenario.scenario_id}: blank marker")
            elif marker.lower() not in prompt_text:
                issues.append(
                    f"{scenario.scenario_id}: marker {marker!r} is absent from compaction prompt"
                )
    return issues


def validate_scenario_definitions() -> None:
    issues = scenario_definition_issues(SCENARIOS)
    if issues:
        formatted = "\n- ".join(issues)
        raise SystemExit(f"invalid compaction eval scenario definitions:\n- {formatted}")


class ProgressWriter:
    def __init__(self, path: Path | None) -> None:
        self.path = path

    def emit(self, event: str, **fields: object) -> None:
        if not self.path:
            return
        payload = {"time": utc_now(), "event": event, **fields}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, separators=(",", ":")) + "\n")


def selected_scenarios(ids: Iterable[str]) -> list[CompactionEvalScenario]:
    requested = list(ids)
    if not requested:
        return SCENARIOS
    by_id = {scenario.scenario_id: scenario for scenario in SCENARIOS}
    missing = [scenario_id for scenario_id in requested if scenario_id not in by_id]
    if missing:
        raise SystemExit(f"unknown scenario id(s): {', '.join(missing)}")
    return [by_id[scenario_id] for scenario_id in requested]


def build_plan(args: argparse.Namespace) -> dict[str, object]:
    scenarios = selected_scenarios(args.scenario)
    summary_out = args.summary_out or DEFAULT_RESULTS_DIR / "summary.json"
    return {
        "schema_version": 1,
        "generated_at": utc_now(),
        "repo_root": str(REPO_ROOT),
        "results_dir": str(args.results_dir),
        "mode": args.mode,
        "provider": args.provider,
        "model": args.model,
        "scenario_count": len(scenarios),
        "phase_count": len(scenarios) * len(EVAL_PHASES),
        "expected_marker_count": sum(
            len(scenario.expected_markers) * len(EVAL_PHASES) for scenario in scenarios
        ),
        "harness_checks": {
            "required_scenarios_present": True,
            "generated_continuation_uses_compaction_summary": True,
            "fixture_mode_scores_all_markers": True,
        },
        "live_model_required_for_run": True,
        "cluster_required_scenarios": [
            scenario.scenario_id for scenario in scenarios if scenario.requires_live_cluster
        ],
        "scenarios": [
            {
                **asdict(scenario),
                "phase_count": len(EVAL_PHASES),
                "phases": list(EVAL_PHASES),
            }
            for scenario in scenarios
        ],
        "example_run_command": " ".join(
            shlex.quote(part)
            for part in [
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
        ),
    }


def marker_hits(text: str, markers: list[str]) -> dict[str, bool]:
    normalized = text.lower()
    return {marker: marker.lower() in normalized for marker in markers}


def run_agent_prompt(args: argparse.Namespace, prompt: str) -> dict[str, object]:
    command = [
        str(args.sinomo_bin),
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
    started = utc_now()
    if args.dry_run:
        return {
            "command": command,
            "started_at": started,
            "completed_at": utc_now(),
            "exit_code": 0,
            "stdout": "",
            "stderr": "",
            "dry_run": True,
        }
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    return {
        "command": command,
        "started_at": started,
        "completed_at": utc_now(),
        "exit_code": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "dry_run": False,
    }


def fixture_response(scenario: CompactionEvalScenario, phase: str) -> str:
    marker_text = "; ".join(scenario.expected_markers)
    return (
        f"Fixture response for {scenario.scenario_id} phase {phase}. "
        f"Preserved markers: {marker_text}."
    )


def evaluate_scenario(
    args: argparse.Namespace,
    scenario: CompactionEvalScenario,
    progress: ProgressWriter,
) -> dict[str, object]:
    phase_results: list[dict[str, object]] = []
    checkpoint_summary = ""
    progress.emit("scenario-start", scenario_id=scenario.scenario_id)
    for phase in EVAL_PHASES:
        if phase == "compaction-summary":
            prompt = scenario.compaction_prompt()
        else:
            prompt = scenario.continuation_prompt(checkpoint_summary)
        progress.emit("phase-start", scenario_id=scenario.scenario_id, phase=phase)
        if args.mode == "fixture":
            output = fixture_response(scenario, phase)
            invocation = {
                "command": [],
                "started_at": utc_now(),
                "completed_at": utc_now(),
                "exit_code": 0,
                "stdout": output,
                "stderr": "",
                "dry_run": True,
            }
        else:
            invocation = run_agent_prompt(args, prompt)
            output = str(invocation["stdout"])
        hits = marker_hits(output, scenario.expected_markers)
        missing_markers = [marker for marker, hit in hits.items() if not hit]
        passed = bool(output.strip()) and all(hits.values()) and invocation["exit_code"] == 0
        if phase == "compaction-summary":
            checkpoint_summary = output
        phase_result = {
            "phase": phase,
            "passed": passed,
            "marker_hits": hits,
            "marker_count": len(hits),
            "marker_hit_count": sum(1 for hit in hits.values() if hit),
            "missing_markers": missing_markers,
            "exit_code": invocation["exit_code"],
            "stdout_excerpt": output[:4000],
            "stderr_excerpt": str(invocation["stderr"])[:4000],
            "command": invocation["command"],
            "started_at": invocation["started_at"],
            "completed_at": invocation["completed_at"],
            "dry_run": invocation["dry_run"],
        }
        phase_results.append(phase_result)
        progress.emit(
            "phase-complete",
            scenario_id=scenario.scenario_id,
            phase=phase,
            passed=passed,
            exit_code=invocation["exit_code"],
        )
    passed = all(result["passed"] for result in phase_results)
    progress.emit("scenario-complete", scenario_id=scenario.scenario_id, passed=passed)
    return {
        "scenario_id": scenario.scenario_id,
        "title": scenario.title,
        "passed": passed,
        "requires_live_cluster": scenario.requires_live_cluster,
        "risk_if_lost": scenario.risk_if_lost,
        "phases": phase_results,
    }


def aggregate_results(results: list[dict[str, object]]) -> dict[str, object]:
    phase_count = 0
    passed_phase_count = 0
    marker_count = 0
    marker_hit_count = 0
    missing_marker_phases: list[dict[str, object]] = []

    for result in results:
        scenario_id = str(result["scenario_id"])
        phases = result.get("phases", [])
        if not isinstance(phases, list):
            continue
        for phase in phases:
            if not isinstance(phase, dict):
                continue
            phase_count += 1
            if phase.get("passed") is True:
                passed_phase_count += 1
            marker_count += int(phase.get("marker_count", 0))
            marker_hit_count += int(phase.get("marker_hit_count", 0))
            missing_markers = phase.get("missing_markers", [])
            if missing_markers:
                missing_marker_phases.append(
                    {
                        "scenario_id": scenario_id,
                        "phase": phase.get("phase"),
                        "missing_markers": missing_markers,
                    }
                )

    return {
        "scenario_count": len(results),
        "phase_count": phase_count,
        "passed_phase_count": passed_phase_count,
        "failed_phase_count": phase_count - passed_phase_count,
        "marker_count": marker_count,
        "marker_hit_count": marker_hit_count,
        "missing_marker_phases": missing_marker_phases,
    }


def write_json(path: Path | None, payload: dict[str, object]) -> None:
    if not path:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["plan", "fixture", "run"], default="plan")
    parser.add_argument("--scenario", action="append", default=[])
    parser.add_argument("--provider", default=os.environ.get("SINOMO_AGENT_PROVIDER", "codex"))
    parser.add_argument("--model", default=os.environ.get("SINOMO_AGENT_MODEL", "gpt-5.2-codex"))
    parser.add_argument("--account")
    parser.add_argument("--sinomo-bin", type=Path, default=REPO_ROOT / "target" / "debug" / "sinomo")
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--summary-out", type=Path)
    parser.add_argument("--progress-out", type=Path)
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
    scenarios = selected_scenarios(args.scenario)
    results = [evaluate_scenario(args, scenario, progress) for scenario in scenarios]
    passed = all(result["passed"] for result in results)
    aggregate = aggregate_results(results)
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
