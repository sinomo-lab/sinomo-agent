#!/usr/bin/env python3
"""Run Sinomo and mini-SWE-agent on the local SWE-bench Verified 10-task suite."""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
import urllib.request
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SUITE = Path(__file__).with_name("verified_10.json")
DEFAULT_RESULTS_DIR = REPO_ROOT / "benchmark-results" / "swebench"
HF_ROWS_URL = (
    "https://datasets-server.huggingface.co/rows?"
    "dataset=princeton-nlp%2FSWE-bench_Verified&config=default&split=test"
)


def load_suite(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        suite = json.load(handle)
    ids = [item["instance_id"] for item in suite.get("instances", [])]
    if len(ids) != 10 or len(ids) != len(set(ids)):
        raise SystemExit(f"{path} must contain exactly 10 unique SWE-bench instances")
    return suite


def select_suite_instances(suite: dict, *, instance_id: str | None, limit: int | None) -> dict:
    instances = list(suite.get("instances", []))
    original_count = len(instances)
    if instance_id:
        instances = [item for item in instances if item["instance_id"] == instance_id]
        if not instances:
            raise SystemExit(f"{instance_id} is not present in {suite['name']}")
    if limit is not None:
        if limit < 1:
            raise SystemExit("--limit must be at least 1")
        instances = instances[:limit]
    if not instances:
        raise SystemExit("selected suite is empty")
    if len(instances) == original_count:
        return suite
    selected = dict(suite)
    selected["name"] = f"{suite['name']}-selected-{len(instances)}"
    selected["instances"] = instances
    selected["selection"] = (
        f"{suite.get('selection', 'unspecified selection')}; "
        f"runtime selection contains {len(instances)} of {original_count} instances"
    )
    return selected


def suite_filter_regex(instance_ids: list[str]) -> str:
    return "^(" + "|".join(re.escape(instance_id) for instance_id in instance_ids) + ")$"


def run_command(command: list[str], *, cwd: Path | None = None, dry_run: bool = False) -> None:
    print("$ " + shlex.join(command), flush=True)
    if dry_run:
        return
    subprocess.run(command, cwd=cwd, check=True)


def fetch_suite_rows(instance_ids: list[str]) -> dict[str, dict]:
    wanted = set(instance_ids)
    found: dict[str, dict] = {}
    for offset in range(0, 500, 100):
        with urllib.request.urlopen(f"{HF_ROWS_URL}&offset={offset}&length=100", timeout=60) as response:
            payload = json.load(response)
        for row in payload.get("rows", []):
            task = row["row"]
            instance_id = task["instance_id"]
            if instance_id in wanted:
                found[instance_id] = task
        if wanted.issubset(found):
            return found
    missing = sorted(wanted.difference(found))
    raise SystemExit(f"could not fetch SWE-bench rows for: {', '.join(missing)}")


def prepare_workspace(task: dict, workspaces_dir: Path, *, dry_run: bool) -> Path:
    instance_id = task["instance_id"]
    repo = task["repo"]
    base_commit = task["base_commit"]
    workspace = workspaces_dir / instance_id
    repo_url = f"https://github.com/{repo}.git"
    if not workspace.exists():
        run_command(["git", "clone", repo_url, str(workspace)], dry_run=dry_run)
    if dry_run:
        print("$ " + " ".join(["git", "checkout", "-f", base_commit]) + f"  # cwd={workspace}")
        print("$ " + " ".join(["git", "clean", "-fdx"]) + f"  # cwd={workspace}")
        return workspace
    run_command(["git", "fetch", "--tags", "--force", "origin"], cwd=workspace)
    run_command(["git", "checkout", "-f", base_commit], cwd=workspace)
    run_command(["git", "clean", "-fdx"], cwd=workspace)
    return workspace


def ensure_sinomo_agent_binary(binary: Path, *, dry_run: bool) -> None:
    if binary.exists() or dry_run:
        return
    run_command(
        ["cargo", "build", "-p", "sinomo-benchmarks", "--bin", "sinomo-swebench-agent"],
        cwd=REPO_ROOT,
    )


def run_sinomo(args: argparse.Namespace, suite: dict) -> Path:
    instance_ids = [item["instance_id"] for item in suite["instances"]]
    tasks = fetch_suite_rows(instance_ids)
    output_dir = args.results_dir / "sinomo"
    reports_dir = output_dir / "reports"
    task_dir = output_dir / "tasks"
    workspaces_dir = output_dir / "workspaces"
    for directory in (reports_dir, task_dir, workspaces_dir):
        if not args.dry_run:
            directory.mkdir(parents=True, exist_ok=True)
    agent_bin = args.sinomo_agent_bin
    ensure_sinomo_agent_binary(agent_bin, dry_run=args.dry_run)
    predictions_path = output_dir / "predictions.jsonl"
    if not args.dry_run:
        predictions_path.parent.mkdir(parents=True, exist_ok=True)
        predictions_path.write_text("", encoding="utf-8")
    for instance_id in instance_ids:
        task = tasks[instance_id]
        workspace = prepare_workspace(task, workspaces_dir, dry_run=args.dry_run)
        task_path = task_dir / f"{instance_id}.json"
        report_path = reports_dir / f"{instance_id}.json"
        prediction_path = reports_dir / f"{instance_id}.prediction.json"
        if not args.dry_run:
            task_path.write_text(json.dumps(task, indent=2), encoding="utf-8")
        command = [
            str(agent_bin),
            "--workspace",
            str(workspace),
            "--task-json",
            str(task_path),
            "--prediction-out",
            str(prediction_path),
            "--report-out",
            str(report_path),
            "--provider",
            args.provider,
            "--model",
            args.model,
            "--max-steps",
            str(args.max_steps),
        ]
        if args.account:
            command.extend(["--account", args.account])
        run_command(command, cwd=REPO_ROOT, dry_run=args.dry_run)
        if not args.dry_run:
            prediction = json.loads(prediction_path.read_text(encoding="utf-8"))
            with predictions_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(prediction, separators=(",", ":")) + "\n")
    print(f"Sinomo predictions: {predictions_path}")
    return predictions_path


def run_mini(args: argparse.Namespace, suite: dict) -> Path:
    instance_ids = [item["instance_id"] for item in suite["instances"]]
    output_dir = args.results_dir / "mini-swe-agent"
    command = [
        "mini-extra",
        "swebench",
        "--model",
        args.baseline_model,
        "--subset",
        "verified",
        "--split",
        "test",
        "--filter",
        suite_filter_regex(instance_ids),
        "--workers",
        str(args.workers),
        "--output",
        str(output_dir),
    ]
    if args.environment_class:
        command.extend(["--environment-class", args.environment_class])
    run_command(command, cwd=REPO_ROOT, dry_run=args.dry_run)
    print(f"mini-SWE-agent output: {output_dir}")
    return output_dir / "preds.json"


def evaluate_predictions(args: argparse.Namespace, predictions_path: Path, suite: dict) -> None:
    evaluation_dir = args.results_dir / "evaluation"
    if not args.dry_run:
        evaluation_dir.mkdir(parents=True, exist_ok=True)
    instance_ids = [item["instance_id"] for item in suite["instances"]]
    command = [
        sys.executable,
        "-m",
        "swebench.harness.run_evaluation",
        "--dataset_name",
        "princeton-nlp/SWE-bench_Verified",
        "--split",
        suite["split"],
        "--instance_ids",
        *instance_ids,
        "--predictions_path",
        str(predictions_path),
        "--max_workers",
        str(args.workers),
        "--run_id",
        args.run_id,
    ]
    run_command(command, cwd=evaluation_dir, dry_run=args.dry_run)


def collect_official_evaluation_summary(args: argparse.Namespace, suite: dict) -> dict:
    evaluation_dir = args.results_dir / "evaluation"
    instance_ids = [item["instance_id"] for item in suite["instances"]]
    summary = {
        "requested": bool(args.evaluate),
        "results_dir": str(evaluation_dir),
        "parse_status": "not-requested" if not args.evaluate else "not-run",
        "resolved_instances": None,
        "unresolved_instances": None,
        "error_instances": None,
        "summary_path": None,
    }
    if not args.evaluate:
        return summary
    if args.dry_run:
        summary["parse_status"] = "dry-run"
        return summary
    if not evaluation_dir.exists():
        summary["parse_status"] = "missing-results-dir"
        return summary

    parsed = parse_official_evaluation_results(evaluation_dir, instance_ids)
    summary.update(parsed)
    return summary


def parse_official_evaluation_results(evaluation_dir: Path, instance_ids: list[str]) -> dict:
    wanted = set(instance_ids)
    resolved: set[str] = set()
    unresolved: set[str] = set()
    errors: set[str] = set()
    summary_path: Path | None = None
    for path in sorted(evaluation_dir.rglob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        before = (len(resolved), len(unresolved), len(errors))
        resolved.update(extract_id_list(payload, wanted, {"resolved_ids", "resolved"}))
        unresolved.update(extract_id_list(payload, wanted, {"unresolved_ids", "unresolved"}))
        errors.update(extract_id_list(payload, wanted, {"error_ids", "errored_ids", "failed_ids"}))
        for instance_id, was_resolved in extract_instance_resolution_flags(payload, path, wanted):
            if was_resolved:
                resolved.add(instance_id)
            else:
                unresolved.add(instance_id)
        after = (len(resolved), len(unresolved), len(errors))
        if after != before and summary_path is None:
            summary_path = path

    resolved &= wanted
    unresolved &= wanted
    errors &= wanted
    if resolved or unresolved or errors:
        unresolved.update(wanted.difference(resolved).difference(errors))
        return {
            "parse_status": "parsed",
            "resolved_instances": len(resolved),
            "unresolved_instances": len(unresolved),
            "error_instances": len(errors),
            "summary_path": str(summary_path) if summary_path else None,
        }
    return {
        "parse_status": "unparsed",
        "resolved_instances": None,
        "unresolved_instances": None,
        "error_instances": None,
        "summary_path": None,
    }


def extract_id_list(payload: object, wanted: set[str], keys: set[str]) -> set[str]:
    found: set[str] = set()
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key in keys:
                found.update(ids_from_value(value, wanted))
            else:
                found.update(extract_id_list(value, wanted, keys))
    elif isinstance(payload, list):
        for value in payload:
            found.update(extract_id_list(value, wanted, keys))
    return found


def ids_from_value(value: object, wanted: set[str]) -> set[str]:
    if isinstance(value, str):
        return {value} if value in wanted else set()
    if isinstance(value, list):
        ids: set[str] = set()
        for item in value:
            ids.update(ids_from_value(item, wanted))
        return ids
    if isinstance(value, dict):
        return {key for key in value.keys() if key in wanted}
    return set()


def extract_instance_resolution_flags(
    payload: object,
    path: Path,
    wanted: set[str],
) -> list[tuple[str, bool]]:
    flags: list[tuple[str, bool]] = []
    if isinstance(payload, dict):
        instance_id = payload.get("instance_id")
        resolved = payload.get("resolved")
        if isinstance(instance_id, str) and instance_id in wanted and isinstance(resolved, bool):
            flags.append((instance_id, resolved))
        for key, value in payload.items():
            if key in wanted and isinstance(value, dict):
                resolved = value.get("resolved")
                if isinstance(resolved, bool):
                    flags.append((key, resolved))
            flags.extend(extract_instance_resolution_flags(value, path, wanted))
    elif isinstance(payload, list):
        for value in payload:
            flags.extend(extract_instance_resolution_flags(value, path, wanted))
    else:
        for instance_id in wanted:
            if instance_id in path.name:
                return []
    return flags


def write_summary(
    args: argparse.Namespace,
    suite: dict,
    *,
    predictions_path: Path | None,
    mini_predictions_path: Path | None,
) -> None:
    if not args.summary_out:
        return
    instance_ids = [item["instance_id"] for item in suite["instances"]]
    reports_dir = args.results_dir / "sinomo" / "reports"
    reports = []
    for instance_id in instance_ids:
        report_path = reports_dir / f"{instance_id}.json"
        if report_path.exists():
            reports.append(json.loads(report_path.read_text(encoding="utf-8")))
    official_evaluation = collect_official_evaluation_summary(args, suite)
    sinomo_summary = {
        "prediction_path": str(predictions_path) if predictions_path else None,
        "report_dir": str(reports_dir),
        "report_count": len(reports),
        "error_instances": len(instance_ids) - len(reports),
        "resolved_instances": official_evaluation.get("resolved_instances"),
        "unresolved_instances": official_evaluation.get("unresolved_instances"),
        "agent_model_turns": sum(int(report.get("model_turns", 0)) for report in reports),
        "agent_tool_results": sum(int(report.get("tool_results", 0)) for report in reports),
        "agent_input_tokens": sum(int(report.get("agent_input_tokens", 0)) for report in reports),
        "agent_output_tokens": sum(int(report.get("agent_output_tokens", 0)) for report in reports),
    }
    summary = {
        "benchmark": "swebench",
        "suite": suite["name"],
        "dataset": suite["dataset"],
        "split": suite["split"],
        "instances": instance_ids,
        "total_instances": len(instance_ids),
        "mode": args.mode,
        "results_dir": str(args.results_dir),
        "sinomo": sinomo_summary,
        "mini_swe_agent": {
            "prediction_path": str(mini_predictions_path) if mini_predictions_path else None,
        },
        "official_evaluation": official_evaluation,
    }
    if not args.dry_run:
        args.summary_out.parent.mkdir(parents=True, exist_ok=True)
        args.summary_out.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"summary: {args.summary_out}")


def print_plan(args: argparse.Namespace, suite: dict) -> None:
    instance_ids = [item["instance_id"] for item in suite["instances"]]
    print(json.dumps({
        "suite": suite["name"],
        "dataset": suite["dataset"],
        "split": suite["split"],
        "instances": instance_ids,
        "sinomo_model": {
            "provider": args.provider,
            "model": args.model,
            "max_steps": args.max_steps,
        },
        "mini_swe_agent": {
            "model": args.baseline_model,
            "filter": suite_filter_regex(instance_ids),
            "workers": args.workers,
        },
        "results_dir": str(args.results_dir),
    }, indent=2))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", type=Path, default=DEFAULT_SUITE)
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--instance-id", help="run only one checked-in suite instance")
    parser.add_argument("--limit", type=int, help="run only the first N checked-in suite instances")
    parser.add_argument("--mode", choices=["plan", "sinomo", "mini", "both"], default="plan")
    parser.add_argument("--provider", default="deepseek")
    parser.add_argument("--model", default="deepseek-v4-flash")
    parser.add_argument("--account")
    parser.add_argument("--baseline-model", default="deepseek/deepseek-v4-flash")
    parser.add_argument("--sinomo-agent-bin", type=Path, default=REPO_ROOT / "target" / "debug" / "sinomo-swebench-agent")
    parser.add_argument("--max-steps", type=int, default=20)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--environment-class", choices=["docker", "singularity", "bubblewrap", "local"], default=None)
    parser.add_argument("--evaluate", action="store_true")
    parser.add_argument("--run-id", default="sinomo-deepseek-v4-flash-verified-10")
    parser.add_argument("--summary-out", type=Path, help="write a machine-readable benchmark summary JSON")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    args.results_dir = args.results_dir.resolve()
    args.sinomo_agent_bin = args.sinomo_agent_bin.resolve()
    if args.summary_out:
        args.summary_out = args.summary_out.resolve()
    return args


def main() -> int:
    args = parse_args()
    suite = load_suite(args.suite)
    suite = select_suite_instances(suite, instance_id=args.instance_id, limit=args.limit)
    print_plan(args, suite)
    predictions_path: Path | None = None
    mini_predictions_path: Path | None = None
    if args.mode in {"sinomo", "both"}:
        predictions_path = run_sinomo(args, suite)
    if args.mode in {"mini", "both"}:
        mini_predictions_path = run_mini(args, suite)
    if args.evaluate:
        if predictions_path is None:
            predictions_path = args.results_dir / "sinomo" / "predictions.jsonl"
        evaluate_predictions(args, predictions_path, suite)
    write_summary(
        args,
        suite,
        predictions_path=predictions_path,
        mini_predictions_path=mini_predictions_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
