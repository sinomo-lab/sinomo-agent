#!/usr/bin/env python3
"""Live/model eval scenarios for Sinomo agent compaction quality.

The default plan mode is deterministic and safe for CI. Run mode calls the
configured Sinomo CLI and scores marker retention in model-generated summaries
and post-compaction continuation probes. The checkpoint command uses
``--compact-after`` to submit the runtime's real compaction operation and
requires persisted projection evidence. Each continuation is a second CLI
process that resumes the durable session created by that compaction. The score
is intentionally evidence for research/readiness review, not a Rust unit-test
assertion.
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
    summary_markers: list[str]
    continuation_markers: list[str]
    risk_if_lost: str
    requires_live_cluster: bool = False

    def compaction_prompt(self) -> str:
        transcript = "\n\n".join(
            f"Transcript item {index}:\n{item}"
            for index, item in enumerate(self.pre_compaction_transcript, start=1)
        )
        exact_markers = "\n".join(f"- {marker}" for marker in self.summary_markers)
        return f"""CONTEXT CHECKPOINT COMPACTION EVAL

This is a read-only memory evaluation. Do not call tools, inspect a workspace,
execute commands, or modify files. Return the requested text directly.

Write a concise handoff summary for the next Sinomo agent turn.
Do not answer the user's unfinished task. Preserve constraints, exact paths,
commands, identifiers, and next steps that are needed after compaction.

Scenario: {self.scenario_id}
Purpose: {self.purpose}

{transcript}

The next user request after compaction will be:
{self.post_compaction_probe}

Preserve each of these exact marker strings in the handoff summary:
{exact_markers}

Write the handoff summary now."""

    def continuation_prompt(self) -> str:
        return f"""POST-COMPACTION CONTINUATION EVAL

This command resumes the same durable Sinomo agent session in a new CLI process
after the preceding context checkpoint. Use the retained session context and
answer the new user request. Do not ask the user to repeat the prior context.
This is a read-only memory evaluation: do not call tools, inspect a workspace,
execute commands, or modify files. Answer directly from retained context.

Scenario: {self.scenario_id}
New user request:
{self.post_compaction_probe}"""

    def markers_for_phase(self, phase: str) -> list[str]:
        if phase == "compaction-summary":
            return self.summary_markers
        if phase == "post-compaction-continuation":
            return self.continuation_markers
        raise ValueError(f"unknown compaction eval phase: {phase}")


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
            "Once the eval exists, the first verification command should be cargo test -p sinomo-benchmarks.",
        ],
        post_compaction_probe="Name the next three concrete engineering actions and the first verification command.",
        summary_markers=[
            "AgentToolSurface",
            "tool_unavailable",
            "modules/benchmark/sinomo-benchmarks",
            "cargo test -p sinomo-agent",
        ],
        continuation_markers=[
            "modules/benchmark/sinomo-benchmarks",
            "docs/agent-system-architecture.md",
            "cargo test -p sinomo-benchmarks",
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
        summary_markers=[
            "runtime profile",
            "unlimited",
            "visible",
            "sandbox",
            "repo CLI",
            "never reboot",
            "crates/sinomo-app",
        ],
        continuation_markers=[
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
        summary_markers=[
            "AgentCommandOutput",
            "stderr",
            "timeout",
            "tools/local_workspace",
            "cargo test -p sinomo-agent",
        ],
        continuation_markers=[
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
        summary_markers=[
            "sinomo test run",
            "--require-cluster",
            "session resume",
            "module projection",
            "command evidence",
            "Do not reboot",
        ],
        continuation_markers=[
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
        if not scenario.summary_markers:
            issues.append(f"{scenario.scenario_id}: no summary markers")
        if not scenario.continuation_markers:
            issues.append(f"{scenario.scenario_id}: no continuation markers")
        for marker in duplicate_values(scenario.summary_markers):
            issues.append(f"{scenario.scenario_id}: duplicate summary marker {marker!r}")
        for marker in duplicate_values(scenario.continuation_markers):
            issues.append(
                f"{scenario.scenario_id}: duplicate continuation marker {marker!r}"
            )
        prompt_text = scenario.compaction_prompt().lower()
        for marker in scenario.summary_markers:
            if not marker.strip():
                issues.append(f"{scenario.scenario_id}: blank summary marker")
            elif marker.lower() not in prompt_text:
                issues.append(
                    f"{scenario.scenario_id}: summary marker {marker!r} is absent from compaction prompt"
                )
        source_text = "\n".join(
            [*scenario.pre_compaction_transcript, scenario.post_compaction_probe]
        ).lower()
        for marker in scenario.continuation_markers:
            if not marker.strip():
                issues.append(f"{scenario.scenario_id}: blank continuation marker")
            elif marker.lower() not in source_text:
                issues.append(
                    f"{scenario.scenario_id}: continuation marker {marker!r} is absent from source context"
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
        "schema_version": 3,
        "generated_at": utc_now(),
        "repo_root": str(REPO_ROOT),
        "results_dir": str(args.results_dir),
        "mode": args.mode,
        "agent": args.agent,
        "provider": args.provider,
        "model": args.model,
        "scenario_count": len(scenarios),
        "phase_count": len(scenarios) * len(EVAL_PHASES),
        "expected_marker_count": sum(
            len(scenario.summary_markers) + len(scenario.continuation_markers)
            for scenario in scenarios
        ),
        "harness_checks": {
            "required_scenarios_present": True,
            "continuation_resumes_durable_cli_session": True,
            "continuation_does_not_reinject_summary": True,
            "phase_one_invokes_agent_op_compact": True,
            "phase_one_requires_persisted_projection_evidence": True,
            "fixture_mode_scores_phase_specific_markers": True,
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
                "--agent",
                args.agent,
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


def parse_agent_run_json(stdout: str) -> dict[str, object]:
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as source:
        return {
            "session_id": None,
            "resumed": None,
            "agent_output": "",
            "compaction": None,
            "json_error": f"invalid agent run JSON: {source}",
        }
    if not isinstance(payload, dict):
        return {
            "session_id": None,
            "resumed": None,
            "agent_output": "",
            "compaction": None,
            "json_error": "agent run JSON must be an object",
        }
    session_id = payload.get("session")
    resumed = payload.get("resumed")
    decision = payload.get("decision")
    compaction = payload.get("compaction")
    decision_text = decision.get("text") if isinstance(decision, dict) else None
    issues: list[str] = []
    if not isinstance(session_id, str) or not session_id.strip():
        issues.append("missing non-empty session")
        session_id = None
    if not isinstance(resumed, bool):
        issues.append("missing boolean resumed")
        resumed = None
    if not isinstance(decision_text, str):
        issues.append("missing decision.text")
        decision_text = ""
    return {
        "session_id": session_id,
        "resumed": resumed,
        "agent_output": decision_text,
        "compaction": compaction,
        "json_error": "; ".join(issues) if issues else None,
    }


def compaction_evidence_issues(compaction: object) -> list[str]:
    if not isinstance(compaction, dict):
        return ["missing compaction evidence object"]
    issues: list[str] = []
    for field in ("op", "compaction_id", "reason", "strategy_id", "summary"):
        value = compaction.get(field)
        if not isinstance(value, str) or not value.strip():
            issues.append(f"compaction.{field} must be a non-empty string")
    if compaction.get("trigger") != "manual":
        issues.append("compaction.trigger must be manual")
    if compaction.get("persisted") is not True:
        issues.append("compaction.persisted must be true")
    projection = compaction.get("projection")
    if not isinstance(projection, dict):
        issues.append("compaction.projection must be an object")
    else:
        projection_id = projection.get("projection_id")
        if not isinstance(projection_id, str) or not projection_id.strip():
            issues.append("compaction.projection.projection_id must be a non-empty string")
        source_item_count = projection.get("source_item_count")
        if (
            isinstance(source_item_count, bool)
            or not isinstance(source_item_count, int)
            or source_item_count <= 0
        ):
            issues.append("compaction.projection.source_item_count must be positive")
    return issues


def run_agent_prompt(
    args: argparse.Namespace,
    prompt: str,
    session_id: str | None = None,
    compact_after: bool = False,
) -> dict[str, object]:
    command = [
        str(args.sinomo_bin),
        "agent",
        "run",
        "--json",
        "--agent",
        args.agent,
        "--provider",
        args.provider,
        "--model",
        args.model,
        "--prompt",
        prompt,
    ]
    if session_id:
        command.extend(["--session", session_id])
    if compact_after:
        command.append("--compact-after")
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
            "session_id": session_id,
            "resumed": bool(session_id),
            "agent_output": "",
            "compaction": None,
            "json_error": None,
        }
    eval_workspace = args.results_dir / "workspace"
    eval_workspace.mkdir(parents=True, exist_ok=True)
    completed = subprocess.run(
        command,
        cwd=eval_workspace,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if completed.returncode == 0:
        parsed = parse_agent_run_json(completed.stdout)
    else:
        parsed = {
            "session_id": None,
            "resumed": None,
            "agent_output": "",
            "compaction": None,
            "json_error": None,
        }
    return {
        "command": command,
        "started_at": started,
        "completed_at": utc_now(),
        "exit_code": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "dry_run": False,
        **parsed,
    }


def fixture_response(scenario: CompactionEvalScenario, phase: str) -> str:
    marker_text = "; ".join(scenario.markers_for_phase(phase))
    return (
        f"Fixture response for {scenario.scenario_id} phase {phase}. "
        f"Preserved markers: {marker_text}."
    )


def fixture_compaction_evidence(scenario: CompactionEvalScenario) -> dict[str, object]:
    return {
        "op": f"fixture-compact-op-{scenario.scenario_id}",
        "compaction_id": f"fixture-compaction-{scenario.scenario_id}",
        "reason": "CLI --compact-after fixture",
        "strategy_id": "fixture-codex-style",
        "trigger": "manual",
        "summary": fixture_response(scenario, "compaction-summary"),
        "token_estimate_before": 100,
        "retained_tail_count": 1,
        "item_count_after": 2,
        "transcript_item_count_after": 2,
        "raw_item_count_after": len(scenario.pre_compaction_transcript) + 2,
        "projection": {
            "projection_id": f"fixture-projection-{scenario.scenario_id}",
            "reason": "CLI --compact-after fixture",
            "summary": fixture_response(scenario, "compaction-summary"),
            "source_item_count": len(scenario.pre_compaction_transcript) + 2,
            "retained_item_ids": ["fixture-retained-item"],
            "working_state": {},
        },
        "persisted": True,
    }


def evaluate_scenario(
    args: argparse.Namespace,
    scenario: CompactionEvalScenario,
    progress: ProgressWriter,
) -> dict[str, object]:
    phase_results: list[dict[str, object]] = []
    session_id: str | None = None
    progress.emit("scenario-start", scenario_id=scenario.scenario_id)
    for phase in EVAL_PHASES:
        compact_after = phase == "compaction-summary"
        if phase == "compaction-summary":
            prompt = scenario.compaction_prompt()
        else:
            prompt = scenario.continuation_prompt()
        progress.emit("phase-start", scenario_id=scenario.scenario_id, phase=phase)
        if args.mode == "fixture":
            output = fixture_response(scenario, phase)
            fixture_session_id = f"fixture-{scenario.scenario_id}"
            invocation = {
                "command": [],
                "started_at": utc_now(),
                "completed_at": utc_now(),
                "exit_code": 0,
                "stdout": output,
                "stderr": "",
                "dry_run": True,
                "session_id": fixture_session_id,
                "resumed": phase != "compaction-summary",
                "agent_output": output,
                "compaction": (
                    fixture_compaction_evidence(scenario) if compact_after else None
                ),
                "json_error": None,
            }
        else:
            invocation = run_agent_prompt(
                args,
                prompt,
                session_id,
                compact_after=compact_after,
            )
            output = str(invocation["agent_output"])
        returned_session_id = invocation.get("session_id")
        resumed = invocation.get("resumed")
        if phase == "compaction-summary":
            session_contract_ok = (
                isinstance(returned_session_id, str)
                and bool(returned_session_id)
                and resumed is False
            )
            if session_contract_ok:
                session_id = returned_session_id
        else:
            session_contract_ok = (
                isinstance(session_id, str)
                and returned_session_id == session_id
                and resumed is True
            )
        compaction = invocation.get("compaction")
        if compact_after:
            compaction_issues = compaction_evidence_issues(compaction)
        elif compaction is None:
            compaction_issues = []
        else:
            compaction_issues = ["continuation unexpectedly compacted the resumed session"]
        compaction_contract_ok = not compaction_issues
        if compact_after and isinstance(compaction, dict):
            persisted_summary = compaction.get("summary")
            marker_output = persisted_summary if isinstance(persisted_summary, str) else ""
            marker_output_source = "compaction.summary"
        else:
            marker_output = output
            marker_output_source = "decision.text"
        hits = marker_hits(marker_output, scenario.markers_for_phase(phase))
        missing_markers = [marker for marker, hit in hits.items() if not hit]
        passed = (
            bool(marker_output.strip())
            and all(hits.values())
            and invocation["exit_code"] == 0
            and invocation.get("json_error") is None
            and session_contract_ok
            and compaction_contract_ok
        )
        phase_result = {
            "phase": phase,
            "passed": passed,
            "marker_hits": hits,
            "marker_count": len(hits),
            "marker_hit_count": sum(1 for hit in hits.values() if hit),
            "missing_markers": missing_markers,
            "exit_code": invocation["exit_code"],
            "stdout_excerpt": output[:4000],
            "marker_output_excerpt": marker_output[:4000],
            "marker_output_source": marker_output_source,
            "stderr_excerpt": str(invocation["stderr"])[:4000],
            "session_id": returned_session_id,
            "resumed": resumed,
            "session_contract_ok": session_contract_ok,
            "compaction": compaction,
            "compaction_contract_ok": compaction_contract_ok,
            "compaction_issues": compaction_issues,
            "json_error": invocation.get("json_error"),
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
    failed_compaction_contract_phases: list[dict[str, object]] = []

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
            if phase.get("compaction_contract_ok") is not True:
                failed_compaction_contract_phases.append(
                    {
                        "scenario_id": scenario_id,
                        "phase": phase.get("phase"),
                        "compaction_issues": phase.get("compaction_issues", []),
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
        "failed_compaction_contract_phases": failed_compaction_contract_phases,
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
    parser.add_argument(
        "--agent", default=os.environ.get("SINOMO_AGENT_PROFILE", "coding-agent")
    )
    parser.add_argument("--provider", default=os.environ.get("SINOMO_AGENT_PROVIDER", "codex"))
    parser.add_argument("--model", default=os.environ.get("SINOMO_AGENT_MODEL", "gpt-5.5"))
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
