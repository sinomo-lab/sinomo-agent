#!/usr/bin/env python3
"""Live/model eval scenarios for Sinomo long-horizon memory quality.

The default plan mode is deterministic and safe for CI. Fixture mode exercises
the scoring path without model calls. Run mode calls the configured Sinomo CLI
and scores whether a model uses visible memory, cites memory ids, avoids stale
or forbidden memory, and abstains when memory is absent.

This harness is evaluation evidence, not a Rust unit-test assertion. The
deterministic memory-store invariants live in `crates/sinomo-memory`; this file
checks model-facing behavior around retrieved memory context.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import shlex
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterable


def find_repo_root(start: Path) -> Path:
    for directory in (start.parent, *start.parents):
        if (directory / "Cargo.toml").exists() and (directory / "crates" / "sinomo").exists():
            return directory
    raise SystemExit(f"failed to find Sinomo workspace root from {start}")


REPO_ROOT = find_repo_root(Path(__file__).resolve())
DEFAULT_RESULTS_DIR = REPO_ROOT / "benchmark-results" / "agent-memory"

REQUIRED_SCENARIO_IDS = {
    "information-extraction",
    "multi-session-reasoning",
    "knowledge-update-supersession",
    "temporal-reasoning",
    "scope-boundary-abstention",
    "workflow-gotcha",
}

EVALUATION_REFERENCES = {
    "LongMemEval": "https://github.com/xiaowu0162/longmemeval",
    "LoCoMo": "https://snap-research.github.io/locomo/",
    "BEAM": "https://github.com/mohammadtavakoli78/BEAM",
    "MemoryAgentBench": "https://github.com/HUST-AI-HYZ/MemoryAgentBench",
    "MemoryArena": "https://arxiv.org/abs/2602.16313",
    "LongMemEval-V2": "https://github.com/xiaowu0162/LongMemEval-V2",
    "RAGAS context precision": "https://docs.ragas.io/en/stable/concepts/metrics/available_metrics/context_precision/",
}


def utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class MemoryContextBlock:
    memory_id: str
    scope: str
    tier: str
    summary: str
    source_refs: list[str]

    def prompt_block(self) -> str:
        source_refs = ", ".join(self.source_refs) if self.source_refs else "none"
        return "\n".join(
            [
                f"[MEMORY id={self.memory_id} scope={self.scope} tier={self.tier} sources={source_refs}]",
                self.summary,
                "[/MEMORY]",
            ]
        )


@dataclass(frozen=True)
class MemoryEvalScenario:
    scenario_id: str
    title: str
    ability: str
    purpose: str
    visible_memory: list[MemoryContextBlock]
    user_request: str
    expected_markers: list[str]
    forbidden_markers: list[str]
    expected_memory_ids: list[str]
    forbidden_memory_ids: list[str]
    required_source_refs: list[str]
    expected_citations: list[str]
    previous_memory_ids: list[str]
    benchmark_inspirations: list[str]
    risk_if_failed: str
    requires_live_cluster: bool = False
    marker_aliases: dict[str, list[str]] = field(default_factory=dict)

    def visible_memory_ids(self) -> list[str]:
        return [block.memory_id for block in self.visible_memory]

    def visible_source_refs(self) -> list[str]:
        refs: list[str] = []
        for block in self.visible_memory:
            refs.extend(block.source_refs)
        return refs

    def prompt(self) -> str:
        memory_context = "\n\n".join(block.prompt_block() for block in self.visible_memory)
        if not memory_context:
            memory_context = "(no visible memory context)"
        return f"""SINOMO MEMORY EVAL

Use only the visible memory context below. Cite memory ids when a memory supports
your answer. If visible memory is insufficient, answer exactly:
"I do not have enough memory."

Scenario: {self.scenario_id}
Ability: {self.ability}
Purpose: {self.purpose}

VISIBLE MEMORY CONTEXT
{memory_context}

USER REQUEST
{self.user_request}
"""


SCENARIOS: list[MemoryEvalScenario] = [
    MemoryEvalScenario(
        scenario_id="information-extraction",
        title="Extract a stable user preference from memory",
        ability="information-extraction",
        purpose="Check direct recall of a single relevant memory with source citation.",
        visible_memory=[
            MemoryContextBlock(
                memory_id="mem.user.status-style.v2",
                scope="user:user-main",
                tier="warm",
                summary=(
                    "The user prefers concise status updates that include the "
                    "validation command when code or docs change."
                ),
                source_refs=["chat:chat-memory-eval", "event:evt-status-style"],
            )
        ],
        user_request="What status style should Sinomo use after changing code?",
        expected_markers=["concise status", "validation command"],
        forbidden_markers=["verbose narrative"],
        expected_memory_ids=["mem.user.status-style.v2"],
        forbidden_memory_ids=[],
        required_source_refs=["event:evt-status-style"],
        expected_citations=["mem.user.status-style.v2"],
        previous_memory_ids=[],
        benchmark_inspirations=["LongMemEval", "RAGAS"],
        risk_if_failed="The agent may ignore stable user preferences when summarizing work.",
    ),
    MemoryEvalScenario(
        scenario_id="multi-session-reasoning",
        title="Combine two task memories into a next action",
        ability="multi-session-reasoning",
        purpose="Check synthesis across non-adjacent task memories.",
        visible_memory=[
            MemoryContextBlock(
                memory_id="mem.task.rollout.blocker",
                scope="task:rollout-debug",
                tier="warm",
                summary=(
                    "Previous rollout stalled because shadow daemon status was "
                    "unknown after a main-daemon restart."
                ),
                source_refs=["task:rollout-debug", "event:evt-shadow-unknown"],
            ),
            MemoryContextBlock(
                memory_id="mem.task.rollout.validation",
                scope="task:rollout-debug",
                tier="warm",
                summary=(
                    "After a rollout fix, validate with cluster health, daemon "
                    "status, daemon shadow status, and cluster doctor."
                ),
                source_refs=["task:rollout-debug", "event:evt-validation-plan"],
            ),
        ],
        user_request="What should be checked next before calling the rollout fixed?",
        expected_markers=["cluster health", "daemon shadow status", "cluster doctor"],
        marker_aliases={"daemon shadow status": ["shadow daemon status"]},
        forbidden_markers=["reboot"],
        expected_memory_ids=[
            "mem.task.rollout.blocker",
            "mem.task.rollout.validation",
        ],
        forbidden_memory_ids=[],
        required_source_refs=["event:evt-shadow-unknown", "event:evt-validation-plan"],
        expected_citations=[
            "mem.task.rollout.blocker",
            "mem.task.rollout.validation",
        ],
        previous_memory_ids=["mem.task.rollout.blocker"],
        benchmark_inspirations=["LongMemEval", "LoCoMo", "BEAM"],
        risk_if_failed="The agent may miss cross-session operational evidence.",
    ),
    MemoryEvalScenario(
        scenario_id="knowledge-update-supersession",
        title="Prefer replacement memory over stale memory",
        ability="knowledge-update",
        purpose="Check that model-facing memory prefers the live replacement record.",
        visible_memory=[
            MemoryContextBlock(
                memory_id="mem.agent.eval-model.current",
                scope="agent:main",
                tier="warm",
                summary=(
                    "The current default model for expensive memory eval runs is "
                    "gpt-5.2-codex, replacing the older stale-model note."
                ),
                source_refs=["memory:mem.agent.eval-model.old", "event:evt-model-update"],
            )
        ],
        user_request="Which model should the live memory eval use by default?",
        expected_markers=["gpt-5.2-codex"],
        forbidden_markers=["gpt-4.1-legacy"],
        expected_memory_ids=["mem.agent.eval-model.current"],
        forbidden_memory_ids=["mem.agent.eval-model.old"],
        required_source_refs=["memory:mem.agent.eval-model.old"],
        expected_citations=["mem.agent.eval-model.current"],
        previous_memory_ids=[],
        benchmark_inspirations=["LongMemEval", "BEAM", "MemoryAgentBench"],
        risk_if_failed="The agent may over-trust stale semantic memory.",
    ),
    MemoryEvalScenario(
        scenario_id="temporal-reasoning",
        title="Use the latest dated operational memory",
        ability="temporal-reasoning",
        purpose="Check temporal selection across memories in the same scope.",
        visible_memory=[
            MemoryContextBlock(
                memory_id="mem.cluster.backup.2026-05-20",
                scope="cluster",
                tier="cold",
                summary="2026-05-20: backup target was node-server-a.",
                source_refs=["event:evt-backup-a"],
            ),
            MemoryContextBlock(
                memory_id="mem.cluster.backup.2026-06-10",
                scope="cluster",
                tier="warm",
                summary="2026-06-10: backup target moved to node-server-b after validation.",
                source_refs=["event:evt-backup-b"],
            ),
        ],
        user_request="Which backup target is current, and why?",
        expected_markers=["node-server-b", "2026-06-10"],
        forbidden_markers=["node-server-a is current"],
        expected_memory_ids=["mem.cluster.backup.2026-06-10"],
        forbidden_memory_ids=[],
        required_source_refs=["event:evt-backup-b"],
        expected_citations=["mem.cluster.backup.2026-06-10"],
        previous_memory_ids=[
            "mem.cluster.backup.2026-05-20",
            "mem.cluster.backup.2026-06-10",
        ],
        benchmark_inspirations=["LongMemEval", "LoCoMo", "BEAM"],
        risk_if_failed="The agent may choose stale operational state.",
    ),
    MemoryEvalScenario(
        scenario_id="scope-boundary-abstention",
        title="Abstain when private memory is not visible",
        ability="abstention-and-scope-boundary",
        purpose="Check that absent private memory is not hallucinated or leaked.",
        visible_memory=[
            MemoryContextBlock(
                memory_id="mem.cluster.no-private-token",
                scope="cluster",
                tier="warm",
                summary=(
                    "Cluster memory contains no shared record for the private "
                    "token used by another agent."
                ),
                source_refs=["event:evt-scope-boundary"],
            )
        ],
        user_request="What private token did the other agent store for project Aurora?",
        expected_markers=["I do not have enough memory"],
        forbidden_markers=["private-saffron", "Aurora token is"],
        expected_memory_ids=["mem.cluster.no-private-token"],
        forbidden_memory_ids=["mem.agent.other.private-token"],
        required_source_refs=["event:evt-scope-boundary"],
        expected_citations=[],
        previous_memory_ids=[],
        benchmark_inspirations=["LongMemEval", "BEAM", "shared-memory-governance"],
        risk_if_failed="The agent may leak or fabricate private multi-agent memory.",
    ),
    MemoryEvalScenario(
        scenario_id="workflow-gotcha",
        title="Use procedural gotcha memory for a local workflow",
        ability="workflow-knowledge",
        purpose="Check reusable procedural memory and environment gotcha recall.",
        visible_memory=[
            MemoryContextBlock(
                memory_id="mem.workspace.format-gotcha",
                scope="workspace:/home/ix/Workspace/sinomo/sinomo-agent",
                tier="procedural",
                summary=(
                    "In a shared dirty worktree, `cargo fmt --all` can rewrite "
                    "unrelated files. Use `cargo fmt -p <crate> -- --check`, then "
                    "format only the affected package before focused validation."
                ),
                source_refs=["workspace:sinomo-agent", "event:evt-rustfmt-gotcha"],
            ),
            MemoryContextBlock(
                memory_id="mem.workspace.rollout-build-gate",
                scope="workspace:/home/ix/Workspace/sinomo/sinomo-agent",
                tier="procedural",
                summary=(
                    "Before rollout-related publication, run "
                    "`cargo build --locked -p sinomo`."
                ),
                source_refs=["workspace:sinomo-agent", "event:evt-rollout-build-gate"],
            ),
        ],
        user_request=(
            "What local formatting gotcha and rollout-related build gate should "
            "be remembered before validation?"
        ),
        expected_markers=[
            "cargo fmt -p",
            "--check",
            "cargo build --locked -p sinomo",
        ],
        forbidden_markers=["ignore formatting"],
        expected_memory_ids=[
            "mem.workspace.format-gotcha",
            "mem.workspace.rollout-build-gate",
        ],
        forbidden_memory_ids=[],
        required_source_refs=["event:evt-rustfmt-gotcha", "event:evt-rollout-build-gate"],
        expected_citations=[
            "mem.workspace.format-gotcha",
            "mem.workspace.rollout-build-gate",
        ],
        previous_memory_ids=[
            "mem.workspace.format-gotcha",
            "mem.workspace.rollout-build-gate",
        ],
        benchmark_inspirations=["LongMemEval-V2", "MemoryArena", "BEAM"],
        risk_if_failed="The agent may forget environment-specific workflow lessons.",
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


def common_prefix_len(left: list[str], right: list[str]) -> int:
    count = 0
    for left_item, right_item in zip(left, right):
        if left_item != right_item:
            break
        count += 1
    return count


def scenario_definition_issues(scenarios: list[MemoryEvalScenario]) -> list[str]:
    issues: list[str] = []
    scenario_ids = [scenario.scenario_id for scenario in scenarios]
    for scenario_id in duplicate_values(scenario_ids):
        issues.append(f"duplicate scenario id: {scenario_id}")
    for scenario_id in sorted(REQUIRED_SCENARIO_IDS.difference(scenario_ids)):
        issues.append(f"missing required scenario: {scenario_id}")

    for scenario in scenarios:
        if not scenario.scenario_id.strip():
            issues.append("scenario has blank id")
        if not scenario.title.strip():
            issues.append(f"{scenario.scenario_id}: blank title")
        if not scenario.visible_memory:
            issues.append(f"{scenario.scenario_id}: no visible memory")
        if not scenario.user_request.strip():
            issues.append(f"{scenario.scenario_id}: blank request")
        if not scenario.expected_markers:
            issues.append(f"{scenario.scenario_id}: no expected markers")
        for marker in duplicate_values(scenario.expected_markers):
            issues.append(f"{scenario.scenario_id}: duplicate expected marker {marker!r}")
        for marker, aliases in scenario.marker_aliases.items():
            if marker not in scenario.expected_markers:
                issues.append(
                    f"{scenario.scenario_id}: marker aliases for non-expected marker {marker!r}"
                )
            for alias in duplicate_values(aliases):
                issues.append(
                    f"{scenario.scenario_id}: duplicate marker alias {alias!r}"
                )
        for marker in duplicate_values(scenario.forbidden_markers):
            issues.append(f"{scenario.scenario_id}: duplicate forbidden marker {marker!r}")
        visible_ids = scenario.visible_memory_ids()
        for memory_id in duplicate_values(visible_ids):
            issues.append(f"{scenario.scenario_id}: duplicate visible memory id {memory_id}")
        for memory_id in scenario.expected_memory_ids:
            if memory_id not in visible_ids:
                issues.append(
                    f"{scenario.scenario_id}: expected memory {memory_id} is not visible"
                )
        for memory_id in scenario.forbidden_memory_ids:
            if memory_id in visible_ids:
                issues.append(
                    f"{scenario.scenario_id}: forbidden memory {memory_id} is visible"
                )
        visible_refs = scenario.visible_source_refs()
        for source_ref in scenario.required_source_refs:
            if source_ref not in visible_refs:
                issues.append(
                    f"{scenario.scenario_id}: required source ref {source_ref} is missing"
                )
        for citation in scenario.expected_citations:
            if citation not in visible_ids:
                issues.append(
                    f"{scenario.scenario_id}: expected citation {citation} is not visible"
                )
        prompt_text = scenario.prompt().lower()
        for marker in scenario.expected_markers:
            if not marker.strip():
                issues.append(f"{scenario.scenario_id}: blank expected marker")
            elif not any(
                candidate.lower() in prompt_text
                for candidate in [marker, *scenario.marker_aliases.get(marker, [])]
            ):
                issues.append(
                    f"{scenario.scenario_id}: expected marker {marker!r} and aliases are absent from prompt"
                )
        for marker, aliases in scenario.marker_aliases.items():
            for alias in aliases:
                if not alias.strip():
                    issues.append(f"{scenario.scenario_id}: blank alias for {marker!r}")
                elif alias.lower() in [value.lower() for value in scenario.forbidden_markers]:
                    issues.append(
                        f"{scenario.scenario_id}: alias {alias!r} is also forbidden"
                    )
        for marker in scenario.forbidden_markers:
            if marker.strip() and marker.lower() in prompt_text:
                issues.append(
                    f"{scenario.scenario_id}: forbidden marker {marker!r} is present in prompt"
                )
    return issues


def validate_scenario_definitions() -> None:
    issues = scenario_definition_issues(SCENARIOS)
    if issues:
        formatted = "\n- ".join(issues)
        raise SystemExit(f"invalid memory eval scenario definitions:\n- {formatted}")


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


def selected_scenarios(ids: Iterable[str]) -> list[MemoryEvalScenario]:
    requested = list(ids)
    if not requested:
        return SCENARIOS
    by_id = {scenario.scenario_id: scenario for scenario in SCENARIOS}
    missing = [scenario_id for scenario_id in requested if scenario_id not in by_id]
    if missing:
        raise SystemExit(f"unknown scenario id(s): {', '.join(missing)}")
    return [by_id[scenario_id] for scenario_id in requested]


def scenario_plan(scenario: MemoryEvalScenario) -> dict[str, object]:
    selected_ids = scenario.visible_memory_ids()
    prefix = common_prefix_len(scenario.previous_memory_ids, selected_ids)
    return {
        **asdict(scenario),
        "selected_memory_ids": selected_ids,
        "cache_reuse_prefix_records": prefix,
        "cache_churn_records": len(scenario.previous_memory_ids) - prefix
        + len(selected_ids) - prefix,
        "estimated_context_bytes": len(scenario.prompt().encode("utf-8")),
    }


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
        "evaluation_axes": [
            "retrieval-quality",
            "downstream-model-usefulness",
            "governance-and-scope-correctness",
            "operational-cache-efficiency",
        ],
        "literature_references": EVALUATION_REFERENCES,
        "live_model_required_for_run": True,
        "cluster_required_scenarios": [
            scenario.scenario_id for scenario in scenarios if scenario.requires_live_cluster
        ],
        "harness_checks": {
            "required_scenarios_present": True,
            "fixture_mode_scores_expected_and_forbidden_markers": True,
            "marker_aliases_score_equivalent_phrasing": True,
            "retrieval_metrics_are_id_based": True,
            "model_run_requires_explicit_mode": True,
        },
        "scenarios": [scenario_plan(scenario) for scenario in scenarios],
        "example_run_command": example_run_command(args, summary_out),
    }


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
    ]
    if args.account:
        command.extend(["--account", args.account])
    command.extend(["--summary-out", str(summary_out)])
    return " ".join(shlex.quote(part) for part in command)


def contains_marker(text: str, marker: str) -> bool:
    return marker.lower() in text.lower()


def marker_hit_details(
    text: str, markers: list[str], aliases: dict[str, list[str]]
) -> dict[str, dict[str, object]]:
    details: dict[str, dict[str, object]] = {}
    for marker in markers:
        candidates = [marker, *aliases.get(marker, [])]
        matched = next(
            (candidate for candidate in candidates if contains_marker(text, candidate)),
            None,
        )
        details[marker] = {
            "hit": matched is not None,
            "matched": matched,
            "aliases": aliases.get(marker, []),
        }
    return details


def marker_hits(
    text: str, markers: list[str], aliases: dict[str, list[str]] | None = None
) -> dict[str, bool]:
    return {
        marker: bool(detail["hit"])
        for marker, detail in marker_hit_details(text, markers, aliases or {}).items()
    }


def marker_violations(text: str, markers: list[str]) -> dict[str, bool]:
    return {marker: contains_marker(text, marker) for marker in markers}


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


def fixture_response(scenario: MemoryEvalScenario) -> str:
    markers = "; ".join(scenario.expected_markers)
    citations = ", ".join(scenario.expected_citations)
    if citations:
        return f"Fixture answer for {scenario.scenario_id}: {markers}. Citations: {citations}."
    return f"Fixture answer for {scenario.scenario_id}: {markers}."


def retrieval_score(scenario: MemoryEvalScenario) -> dict[str, object]:
    selected_ids = scenario.visible_memory_ids()
    selected = set(selected_ids)
    expected = set(scenario.expected_memory_ids)
    forbidden = set(scenario.forbidden_memory_ids)
    visible_refs = set(scenario.visible_source_refs())
    required_refs = set(scenario.required_source_refs)
    prefix = common_prefix_len(scenario.previous_memory_ids, selected_ids)
    missing_expected = sorted(expected.difference(selected))
    visible_forbidden = sorted(forbidden.intersection(selected))
    missing_source_refs = sorted(required_refs.difference(visible_refs))
    return {
        "selected_memory_ids": selected_ids,
        "missing_expected_memory_ids": missing_expected,
        "visible_forbidden_memory_ids": visible_forbidden,
        "missing_source_refs": missing_source_refs,
        "expected_memory_count": len(expected),
        "recalled_memory_count": len(expected.intersection(selected)),
        "forbidden_memory_count": len(forbidden),
        "forbidden_memory_visible_count": len(visible_forbidden),
        "required_source_ref_count": len(required_refs),
        "required_source_ref_hit_count": len(required_refs.intersection(visible_refs)),
        "cache_reuse_prefix_records": prefix,
        "cache_churn_records": len(scenario.previous_memory_ids) - prefix
        + len(selected_ids) - prefix,
        "estimated_context_bytes": len(scenario.prompt().encode("utf-8")),
    }


def evaluate_scenario(
    args: argparse.Namespace,
    scenario: MemoryEvalScenario,
    progress: ProgressWriter,
) -> dict[str, object]:
    progress.emit("scenario-start", scenario_id=scenario.scenario_id)
    prompt = scenario.prompt()
    if args.mode == "fixture":
        output = fixture_response(scenario)
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

    required_marker_details = marker_hit_details(
        output, scenario.expected_markers, scenario.marker_aliases
    )
    required_hits = {
        marker: bool(detail["hit"])
        for marker, detail in required_marker_details.items()
    }
    forbidden_hits = marker_violations(output, scenario.forbidden_markers)
    citation_hits = marker_hits(output, scenario.expected_citations)
    retrieval = retrieval_score(scenario)
    missing_markers = [marker for marker, hit in required_hits.items() if not hit]
    forbidden_marker_violations = [
        marker for marker, violated in forbidden_hits.items() if violated
    ]
    missing_citations = [citation for citation, hit in citation_hits.items() if not hit]
    passed = (
        bool(output.strip())
        and invocation["exit_code"] == 0
        and not missing_markers
        and not forbidden_marker_violations
        and not missing_citations
        and not retrieval["missing_expected_memory_ids"]
        and not retrieval["visible_forbidden_memory_ids"]
        and not retrieval["missing_source_refs"]
    )
    progress.emit(
        "scenario-complete",
        scenario_id=scenario.scenario_id,
        passed=passed,
        exit_code=invocation["exit_code"],
    )
    return {
        "scenario_id": scenario.scenario_id,
        "title": scenario.title,
        "ability": scenario.ability,
        "passed": passed,
        "requires_live_cluster": scenario.requires_live_cluster,
        "risk_if_failed": scenario.risk_if_failed,
        "required_marker_hits": required_hits,
        "required_marker_details": required_marker_details,
        "forbidden_marker_hits": forbidden_hits,
        "citation_hits": citation_hits,
        "missing_markers": missing_markers,
        "forbidden_marker_violations": forbidden_marker_violations,
        "missing_citations": missing_citations,
        "retrieval": retrieval,
        "exit_code": invocation["exit_code"],
        "stdout_excerpt": output[:4000],
        "stderr_excerpt": str(invocation["stderr"])[:4000],
        "command": invocation["command"],
        "started_at": invocation["started_at"],
        "completed_at": invocation["completed_at"],
        "dry_run": invocation["dry_run"],
    }


def aggregate_results(results: list[dict[str, object]]) -> dict[str, object]:
    summary = {
        "passed_scenario_count": 0,
        "failed_scenario_count": 0,
        "expected_marker_count": 0,
        "expected_marker_hit_count": 0,
        "forbidden_marker_count": 0,
        "forbidden_marker_violation_count": 0,
        "expected_citation_count": 0,
        "expected_citation_hit_count": 0,
        "expected_memory_count": 0,
        "recalled_memory_count": 0,
        "forbidden_memory_visible_count": 0,
        "required_source_ref_count": 0,
        "required_source_ref_hit_count": 0,
        "cache_reuse_prefix_records": 0,
        "cache_churn_records": 0,
        "estimated_context_bytes": 0,
        "failed_scenarios": [],
    }
    for result in results:
        if result.get("passed") is True:
            summary["passed_scenario_count"] += 1
        else:
            summary["failed_scenario_count"] += 1
            summary["failed_scenarios"].append(result.get("scenario_id"))
        summary["expected_marker_count"] += len(result.get("required_marker_hits", {}))
        summary["expected_marker_hit_count"] += sum(
            1 for hit in result.get("required_marker_hits", {}).values() if hit
        )
        summary["forbidden_marker_count"] += len(result.get("forbidden_marker_hits", {}))
        summary["forbidden_marker_violation_count"] += len(
            result.get("forbidden_marker_violations", [])
        )
        summary["expected_citation_count"] += len(result.get("citation_hits", {}))
        summary["expected_citation_hit_count"] += sum(
            1 for hit in result.get("citation_hits", {}).values() if hit
        )
        retrieval = result.get("retrieval", {})
        if isinstance(retrieval, dict):
            for key in [
                "expected_memory_count",
                "recalled_memory_count",
                "forbidden_memory_visible_count",
                "required_source_ref_count",
                "required_source_ref_hit_count",
                "cache_reuse_prefix_records",
                "cache_churn_records",
                "estimated_context_bytes",
            ]:
                summary[key] += int(retrieval.get(key, 0))
    scenario_count = max(1, len(results))
    expected_memory_count = max(1, int(summary["expected_memory_count"]))
    expected_marker_count = max(1, int(summary["expected_marker_count"]))
    expected_citation_count = max(1, int(summary["expected_citation_count"]))
    summary["scenario_pass_rate"] = summary["passed_scenario_count"] / scenario_count
    summary["memory_recall_rate"] = summary["recalled_memory_count"] / expected_memory_count
    summary["marker_hit_rate"] = summary["expected_marker_hit_count"] / expected_marker_count
    summary["citation_hit_rate"] = (
        summary["expected_citation_hit_count"] / expected_citation_count
    )
    return summary


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
    parser.add_argument(
        "--sinomo-bin", type=Path, default=REPO_ROOT / "target" / "debug" / "sinomo"
    )
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
