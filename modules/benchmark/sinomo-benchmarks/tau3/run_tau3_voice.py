#!/usr/bin/env python3
"""Plan or run revision-pinned tau3-bench voice evaluations through Sinomo."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PINNED_TAU3_REVISION = "f0927a4b9cdfa7b374269efaa29ff7fdac90d8fc"
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[3]
DEFAULT_SUITE = SCRIPT_DIR / "development_suite.json"
DEFAULT_BRIDGE = REPO_ROOT / "target/debug/sinomo-tau3-voice-bridge"
DEFAULT_RESULTS_ROOT = REPO_ROOT / "benchmark-results/tau3-voice"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("plan", "smoke", "run"), default="plan")
    parser.add_argument("--tau3-root", type=Path, required=True)
    parser.add_argument("--bridge-bin", type=Path, default=DEFAULT_BRIDGE)
    parser.add_argument("--suite", type=Path, default=DEFAULT_SUITE)
    parser.add_argument("--results-root", type=Path, default=DEFAULT_RESULTS_ROOT)
    parser.add_argument("--run-id")
    parser.add_argument("--domain", action="append", choices=("airline", "retail", "telecom"))
    parser.add_argument("--full-base", action="store_true")
    parser.add_argument("--model", default="gpt-realtime-2")
    parser.add_argument("--user-model", default="gpt-4.1")
    parser.add_argument("--speech-complexity", default="regular")
    parser.add_argument("--tick-ms", type=int, default=200)
    parser.add_argument("--max-seconds", type=int, default=600)
    parser.add_argument("--num-trials", type=int, default=1)
    parser.add_argument("--max-concurrency", type=int, default=1)
    parser.add_argument("--send-audio-instant", action="store_true")
    return parser.parse_args()


def git_output(root: Path, *args: str) -> str | None:
    try:
        return subprocess.run(
            ["git", "-C", str(root), *args],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def sha256(path: Path) -> str | None:
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_suite(path: Path) -> dict[str, Any]:
    suite = json.loads(path.read_text())
    if suite.get("schema") != "sinomo.benchmark.tau3-voice-suite.v1":
        raise ValueError(f"unsupported tau3 suite schema in {path}")
    if suite.get("upstream_revision") != PINNED_TAU3_REVISION:
        raise ValueError("suite and runner tau3 revisions do not match")
    return suite


def build_plan(args: argparse.Namespace, suite: dict[str, Any]) -> dict[str, Any]:
    tau3_root = args.tau3_root.expanduser().resolve()
    bridge = args.bridge_bin.expanduser().resolve()
    revision = git_output(tau3_root, "rev-parse", "HEAD")
    dirty = git_output(tau3_root, "status", "--porcelain")
    domains = args.domain or list(suite["domains"])
    selected = {
        domain: (
            None
            if args.full_base
            else suite["domains"][domain][:(1 if args.mode == "smoke" else None)]
        )
        for domain in domains
    }
    return {
        "schema": "sinomo.benchmark.tau3-voice-plan.v1",
        "mode": args.mode,
        "tau3_root": str(tau3_root),
        "tau3_revision": revision,
        "required_tau3_revision": PINNED_TAU3_REVISION,
        "tau3_revision_matches": revision == PINNED_TAU3_REVISION,
        "tau3_worktree_dirty": bool(dirty),
        "python_version": ".".join(map(str, sys.version_info[:3])),
        "python_supported": (3, 12) <= sys.version_info[:2] < (3, 14),
        "bridge_binary": str(bridge),
        "bridge_exists": bridge.is_file(),
        "bridge_executable": bridge.is_file() and os.access(bridge, os.X_OK),
        "bridge_sha256": sha256(bridge),
        "suite": str(args.suite.expanduser().resolve()),
        "suite_name": suite["name"],
        "task_split": suite["task_split"],
        "selected_tasks": selected,
        "credentials": {
            "openai_env_present": any(
                bool(os.environ.get(name))
                for name in ("SINOMO_OPENAI_API_KEY", "OPENAI_API_KEY")
            ),
            "user_simulator_env_present": any(
                bool(os.environ.get(name))
                for name in (
                    "SINOMO_OPENAI_API_KEY",
                    "OPENAI_API_KEY",
                    "ANTHROPIC_API_KEY",
                    "GOOGLE_API_KEY",
                )
            ),
            "elevenlabs_env_present": bool(os.environ.get("ELEVENLABS_API_KEY")),
            "deepgram_env_present": bool(os.environ.get("DEEPGRAM_API_KEY")),
        },
        "config": {
            "agent_model": args.model,
            "user_model": args.user_model,
            "speech_complexity": args.speech_complexity,
            "tick_ms": args.tick_ms,
            "max_seconds": args.max_seconds,
            "num_trials": args.num_trials,
            "max_concurrency": args.max_concurrency,
            "send_audio_instant": args.send_audio_instant,
        },
    }


def validate_run_plan(plan: dict[str, Any]) -> None:
    failures = []
    if not plan["tau3_revision_matches"]:
        failures.append(
            "tau3 checkout is not at the pinned revision "
            + plan["required_tau3_revision"]
        )
    if not plan["python_supported"]:
        failures.append("tau3 voice runs require Python >=3.12,<3.14")
    if not plan["bridge_exists"]:
        failures.append(
            "bridge is missing; build it with `cargo build -p sinomo-benchmarks "
            "--features tau3-voice-bench --bin sinomo-tau3-voice-bridge`"
        )
    elif not plan["bridge_executable"]:
        failures.append("bridge exists but is not executable")
    config = plan["config"]
    for field in ("tick_ms", "max_seconds", "num_trials", "max_concurrency"):
        if config[field] <= 0:
            failures.append(f"{field} must be greater than zero")
    if failures:
        raise RuntimeError("; ".join(failures))


def summarize_results(results: Any) -> dict[str, Any]:
    simulations = results.simulations
    rewards = [
        simulation.reward_info.reward
        for simulation in simulations
        if simulation.reward_info is not None
    ]
    reasons = [
        getattr(item.termination_reason, "value", str(item.termination_reason))
        for item in simulations
    ]
    return {
        "simulation_count": len(simulations),
        "graded_count": len(rewards),
        "mean_reward": sum(rewards) / len(rewards) if rewards else None,
        "termination_reasons": {
            reason: reasons.count(reason) for reason in sorted(set(reasons)) if reason
        },
    }


def run(args: argparse.Namespace, plan: dict[str, Any]) -> Path:
    validate_run_plan(plan)
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    result_dir = args.results_root.expanduser().resolve() / run_id

    tau3_root = args.tau3_root.expanduser().resolve()
    sys.path.insert(0, str(tau3_root / "src"))
    sys.path.insert(0, str(SCRIPT_DIR))
    os.environ["SINOMO_TAU3_BRIDGE_BIN"] = str(args.bridge_bin.expanduser().resolve())

    from sinomo_adapter import SinomoVoiceRunConfig, install_tau3_agent_factory
    from tau2.data_model.simulation import AudioNativeConfig
    from tau2.runner import get_tasks, run_domain

    install_tau3_agent_factory()
    for domain, task_ids in plan["selected_tasks"].items():
        get_tasks(
            task_set_name=domain,
            task_split_name=plan["task_split"],
            task_ids=task_ids,
        )

    result_dir.mkdir(parents=True, exist_ok=False)
    (result_dir / "manifest.json").write_text(json.dumps(plan, indent=2) + "\n")
    summaries: dict[str, Any] = {}
    for domain, task_ids in plan["selected_tasks"].items():
        domain_dir = result_dir / domain
        config = SinomoVoiceRunConfig(
            domain=domain,
            task_split_name=plan["task_split"],
            task_ids=task_ids,
            llm_user=args.user_model,
            num_trials=args.num_trials,
            max_concurrency=args.max_concurrency,
            save_to=str(domain_dir),
            speech_complexity=args.speech_complexity,
            audio_native_config=AudioNativeConfig(
                provider="openai",
                model=args.model,
                tick_duration_seconds=args.tick_ms / 1000,
                max_steps_seconds=args.max_seconds,
                send_audio_instant=args.send_audio_instant,
            ),
        )
        summaries[domain] = summarize_results(run_domain(config))

    summary = {
        "schema": "sinomo.benchmark.tau3-voice-summary.v1",
        "run_id": run_id,
        "domains": summaries,
    }
    (result_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return result_dir


def main() -> int:
    args = parse_args()
    try:
        suite = load_suite(args.suite.expanduser().resolve())
        plan = build_plan(args, suite)
        if args.mode == "plan":
            print(json.dumps(plan, indent=2))
            return 0
        result_dir = run(args, plan)
        print(json.dumps({"ok": True, "result_dir": str(result_dir)}, indent=2))
        return 0
    except Exception as error:
        print(json.dumps({"ok": False, "error": str(error)}, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
