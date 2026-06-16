#!/usr/bin/env python3
"""Run Sinomo and mini-SWE-agent on the local SWE-bench Verified 10-task suite."""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
import re
import shutil
import shlex
import subprocess
import sys
import urllib.request
from pathlib import Path


def find_repo_root(start: Path) -> Path:
    for directory in (start.parent, *start.parents):
        if (directory / "Cargo.toml").exists() and (directory / "crates" / "sinomo").exists():
            return directory
    raise SystemExit(f"failed to find Sinomo workspace root from {start}")


REPO_ROOT = find_repo_root(Path(__file__).resolve())
DEFAULT_SUITE = Path(__file__).with_name("verified_10.json")
DEFAULT_RESULTS_DIR = REPO_ROOT / "benchmark-results" / "swebench"
DEFAULT_AGENT_BIN = REPO_ROOT / "target" / "debug" / "sinomo-swebench-agent"
HF_ROWS_URL = (
    "https://datasets-server.huggingface.co/rows?"
    "dataset=princeton-nlp%2FSWE-bench_Verified&config=default&split=test"
)


def utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


class ProgressWriter:
    def __init__(self, path: Path | None) -> None:
        self.path = path

    def emit(self, event: str, **fields: object) -> None:
        if not self.path:
            return
        payload = {
            "time": utc_now(),
            "event": event,
            **fields,
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, separators=(",", ":")) + "\n")
        details = " ".join(
            f"{key}={value}" for key, value in payload.items() if key not in {"time", "event"}
        )
        suffix = f" {details}" if details else ""
        print(f"progress: {event}{suffix}", flush=True)


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


def run_command(
    command: list[str],
    *,
    cwd: Path | None = None,
    dry_run: bool = False,
    progress: ProgressWriter | None = None,
    phase: str | None = None,
    **progress_fields: object,
) -> None:
    print("$ " + shlex.join(command), flush=True)
    command_text = shlex.join(command)
    if progress:
        progress.emit(
            "command-start",
            phase=phase,
            command=command_text,
            cwd=str(cwd) if cwd else str(REPO_ROOT),
            dry_run=dry_run,
            **progress_fields,
        )
    if dry_run:
        if progress:
            progress.emit(
                "command-complete",
                phase=phase,
                command=command_text,
                exit_code=0,
                dry_run=True,
                **progress_fields,
            )
        return
    try:
        subprocess.run(command, cwd=cwd, check=True)
    except subprocess.CalledProcessError as source:
        if progress:
            progress.emit(
                "command-failed",
                phase=phase,
                command=command_text,
                exit_code=source.returncode,
                **progress_fields,
            )
        raise
    except OSError as source:
        if progress:
            progress.emit(
                "command-failed",
                phase=phase,
                command=command_text,
                exit_code=None,
                os_error=str(source),
                **progress_fields,
            )
        raise
    if progress:
        progress.emit(
            "command-complete",
            phase=phase,
            command=command_text,
            exit_code=0,
            **progress_fields,
        )


def command_output(command: list[str], *, cwd: Path) -> str | None:
    try:
        output = subprocess.run(
            command,
            cwd=cwd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    value = output.stdout.strip()
    return value if value else None


def command_probe(command: list[str], *, cwd: Path = REPO_ROOT) -> dict[str, object]:
    executable = shutil.which(command[0])
    probe: dict[str, object] = {
        "command": command,
        "available": executable is not None,
        "executable": executable,
        "version": None,
        "error": None,
    }
    if executable is None:
        return probe
    try:
        completed = subprocess.run(
            command,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired) as source:
        probe["error"] = str(source)
        return probe
    probe["exit_code"] = completed.returncode
    version = (completed.stdout or completed.stderr).strip().splitlines()
    probe["version"] = version[0] if version else None
    if completed.returncode != 0:
        probe["error"] = completed.stderr.strip() or completed.stdout.strip() or "non-zero exit"
    return probe


def python_import_probe(module: str) -> dict[str, object]:
    try:
        completed = subprocess.run(
            [sys.executable, "-c", f"import {module}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired) as source:
        return {
            "module": module,
            "importable": False,
            "error": str(source),
        }
    return {
        "module": module,
        "importable": completed.returncode == 0,
        "error": None if completed.returncode == 0 else completed.stderr.strip(),
    }


def docker_reachability_probe() -> dict[str, object]:
    docker = shutil.which("docker")
    has_socket = Path("/var/run/docker.sock").exists()
    probe: dict[str, object] = {
        "cli_available": docker is not None,
        "executable": docker,
        "docker_host": os.environ.get("DOCKER_HOST"),
        "default_socket_exists": has_socket,
        "reachable": False,
        "error": None,
    }
    if docker is None:
        probe["error"] = "Docker CLI is not available"
        return probe
    try:
        completed = subprocess.run(
            ["docker", "info", "--format", "{{json .ServerVersion}}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired) as source:
        probe["error"] = str(source)
        return probe
    probe["reachable"] = completed.returncode == 0
    probe["server_version"] = completed.stdout.strip().strip('"') if completed.stdout.strip() else None
    if completed.returncode != 0:
        probe["error"] = completed.stderr.strip() or completed.stdout.strip() or "docker info failed"
    return probe


def host_dependency_probe(args: argparse.Namespace, suite: dict) -> dict[str, object]:
    instance_count = len(suite.get("instances", []))
    commands = {
        "git": command_probe(["git", "--version"]),
        "cargo": command_probe(["cargo", "--version"]),
        "python": command_probe([sys.executable, "--version"]),
        "mini-extra": command_probe(["mini-extra", "--version"]),
    }
    python_imports = {
        "swebench": python_import_probe("swebench"),
    }
    docker = docker_reachability_probe()
    agent_binary = {
        "path": str(args.sinomo_agent_bin),
        "explicit": args.sinomo_agent_bin_explicit,
        "exists": args.sinomo_agent_bin.exists(),
        "default_binary_will_be_rebuilt": not args.sinomo_agent_bin_explicit
        and args.mode in {"sinomo", "both"},
    }
    required = {
        "git": args.mode in {"sinomo", "both"},
        "cargo": args.mode in {"sinomo", "both"} and not args.sinomo_agent_bin_explicit,
        "mini-extra": args.mode in {"mini", "both"},
        "swebench_python_package": bool(args.evaluate),
        "docker": bool(args.evaluate or args.environment_class == "docker"),
        "explicit_agent_binary": args.mode in {"sinomo", "both"} and args.sinomo_agent_bin_explicit,
    }
    issues: list[str] = []
    if required["git"] and not commands["git"]["available"]:
        issues.append("git CLI is required for Sinomo SWE-bench workspace preparation")
    if required["cargo"] and not commands["cargo"]["available"]:
        issues.append("cargo is required to rebuild the default Sinomo SWE-bench agent binary")
    if required["mini-extra"] and not commands["mini-extra"]["available"]:
        issues.append("mini-extra CLI is required for mini-SWE-agent baseline mode")
    if required["swebench_python_package"] and not python_imports["swebench"]["importable"]:
        issues.append("Python package `swebench` is required for official evaluation")
    if required["docker"] and not docker["cli_available"]:
        issues.append("Docker CLI is required for official SWE-bench evaluation")
    if required["docker"] and not docker["reachable"]:
        issues.append("Docker daemon is not reachable for official SWE-bench evaluation")
    if required["explicit_agent_binary"] and not agent_binary["exists"]:
        issues.append(f"explicit Sinomo agent binary does not exist: {args.sinomo_agent_bin}")
    return {
        "generated_at": utc_now(),
        "suite_instance_count": instance_count,
        "mode": args.mode,
        "evaluate": bool(args.evaluate),
        "environment_class": args.environment_class,
        "commands": commands,
        "python_imports": python_imports,
        "docker": docker,
        "agent_binary": agent_binary,
        "required": required,
        "blocking_issues": issues,
        "blocking_issue_count": len(issues),
    }


def host_dependency_blocking_issues(host_dependencies: dict[str, object]) -> list[str]:
    issues = host_dependencies.get("blocking_issues")
    if not isinstance(issues, list):
        return []
    return [str(issue) for issue in issues if str(issue).strip()]


def should_stop_for_host_dependency_blockers(
    args: argparse.Namespace,
    host_dependencies: dict[str, object],
) -> bool:
    return bool(host_dependency_blocking_issues(host_dependencies)) and not args.dry_run and args.mode != "plan"


def host_dependency_harness_status(
    args: argparse.Namespace,
    host_dependencies: dict[str, object],
) -> str:
    if should_stop_for_host_dependency_blockers(args, host_dependencies):
        return "host-dependency-blocked"
    if host_dependency_blocking_issues(host_dependencies):
        return "host-dependency-issues-recorded"
    return "ready"


def repo_source_provenance() -> dict[str, object]:
    revision = command_output(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT)
    status = command_output(["git", "status", "--short"], cwd=REPO_ROOT)
    return {
        "repo_root": str(REPO_ROOT),
        "git_revision": revision,
        "git_dirty": bool(status),
    }


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def agent_binary_provenance(
    binary: Path,
    *,
    explicit: bool,
    rebuild_policy: str,
    dry_run: bool,
) -> dict[str, object]:
    exists = binary.exists()
    provenance: dict[str, object] = {
        "path": str(binary),
        "explicit": explicit,
        "rebuild_policy": rebuild_policy,
        "dry_run": dry_run,
        "exists": exists,
        "source": repo_source_provenance(),
    }
    if exists:
        stat = binary.stat()
        provenance.update(
            {
                "size_bytes": stat.st_size,
                "modified_time_ns": stat.st_mtime_ns,
                "sha256": file_sha256(binary) if not dry_run else None,
            }
        )
    return provenance


def fetch_suite_rows(instance_ids: list[str], progress: ProgressWriter | None = None) -> dict[str, dict]:
    wanted = set(instance_ids)
    found: dict[str, dict] = {}
    if progress:
        progress.emit("fetch-suite-start", requested_instances=len(instance_ids))
    for offset in range(0, 500, 100):
        if progress:
            progress.emit("fetch-suite-page-start", offset=offset, length=100)
        with urllib.request.urlopen(f"{HF_ROWS_URL}&offset={offset}&length=100", timeout=60) as response:
            payload = json.load(response)
        for row in payload.get("rows", []):
            task = row["row"]
            instance_id = task["instance_id"]
            if instance_id in wanted:
                found[instance_id] = task
        if progress:
            progress.emit(
                "fetch-suite-page-complete",
                offset=offset,
                found_instances=len(found),
                requested_instances=len(instance_ids),
            )
        if wanted.issubset(found):
            if progress:
                progress.emit("fetch-suite-complete", found_instances=len(found))
            return found
    missing = sorted(wanted.difference(found))
    raise SystemExit(f"could not fetch SWE-bench rows for: {', '.join(missing)}")


def prepare_workspace(
    task: dict,
    workspaces_dir: Path,
    *,
    dry_run: bool,
    progress: ProgressWriter | None,
) -> Path:
    instance_id = task["instance_id"]
    repo = task["repo"]
    base_commit = task["base_commit"]
    workspace = workspaces_dir / instance_id
    repo_url = f"https://github.com/{repo}.git"
    if not workspace.exists():
        run_command(
            ["git", "clone", repo_url, str(workspace)],
            dry_run=dry_run,
            progress=progress,
            phase="workspace-clone",
            instance_id=instance_id,
        )
    if dry_run:
        print("$ " + " ".join(["git", "checkout", "-f", base_commit]) + f"  # cwd={workspace}")
        print("$ " + " ".join(["git", "clean", "-fdx"]) + f"  # cwd={workspace}")
        return workspace
    run_command(
        ["git", "fetch", "--tags", "--force", "origin"],
        cwd=workspace,
        progress=progress,
        phase="workspace-fetch",
        instance_id=instance_id,
    )
    run_command(
        ["git", "checkout", "-f", base_commit],
        cwd=workspace,
        progress=progress,
        phase="workspace-checkout",
        instance_id=instance_id,
    )
    run_command(
        ["git", "clean", "-fdx"],
        cwd=workspace,
        progress=progress,
        phase="workspace-clean",
        instance_id=instance_id,
    )
    return workspace


def ensure_sinomo_agent_binary(
    binary: Path,
    *,
    explicit: bool,
    dry_run: bool,
    progress: ProgressWriter | None,
) -> dict[str, object]:
    if explicit:
        if not binary.exists() and not dry_run:
            raise SystemExit(
                f"explicit --sinomo-agent-bin does not exist: {binary}; "
                "omit --sinomo-agent-bin to rebuild the default benchmark agent"
            )
        provenance = agent_binary_provenance(
            binary,
            explicit=True,
            rebuild_policy="explicit-binary-no-build",
            dry_run=dry_run,
        )
        if progress:
            progress.emit(
                "agent-binary-ready",
                binary_path=str(binary),
                explicit=True,
                rebuild_policy=provenance["rebuild_policy"],
                exists=provenance["exists"],
                sha256=provenance.get("sha256"),
            )
        return provenance

    run_command(
        ["cargo", "build", "-p", "sinomo-benchmarks", "--bin", "sinomo-swebench-agent"],
        cwd=REPO_ROOT,
        dry_run=dry_run,
        progress=progress,
        phase="build-agent",
    )
    if not binary.exists() and not dry_run:
        raise SystemExit(
            f"cargo build completed but benchmark agent binary was not found at {binary}"
        )
    provenance = agent_binary_provenance(
        binary,
        explicit=False,
        rebuild_policy="rebuilt-default-before-run",
        dry_run=dry_run,
    )
    if progress:
        progress.emit(
            "agent-binary-ready",
            binary_path=str(binary),
            explicit=False,
            rebuild_policy=provenance["rebuild_policy"],
            exists=provenance["exists"],
            sha256=provenance.get("sha256"),
        )
    return provenance


def write_agent_run_blocked_report(
    *,
    task: dict,
    report_path: Path,
    prediction_path: Path,
    agent_trace_path: Path,
    workspace: Path,
    provider: str,
    model: str,
    command: list[str],
    source: BaseException,
) -> dict[str, object]:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    exit_code = source.returncode if isinstance(source, subprocess.CalledProcessError) else None
    report = {
        "status": "harness-blocked",
        "blocked": True,
        "blocked_reason": "sinomo-agent-run-failed",
        "failure_class": "harness-or-environment",
        "phase": "agent-run",
        "exit_code": exit_code,
        "error": str(source),
        "command": shlex.join(command),
        "task": {
            "instance_id": task.get("instance_id"),
            "repo": task.get("repo"),
            "base_commit": task.get("base_commit"),
            "difficulty": task.get("difficulty"),
        },
        "provider": provider,
        "model": model,
        "workspace": str(workspace),
        "prediction_path": str(prediction_path),
        "prediction_written": prediction_path.exists(),
        "agent_trace_path": str(agent_trace_path),
    }
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)
        handle.write("\n")
    return report


def run_sinomo(
    args: argparse.Namespace,
    suite: dict,
    progress: ProgressWriter,
) -> tuple[Path, dict[str, object]]:
    instance_ids = [item["instance_id"] for item in suite["instances"]]
    progress.emit("sinomo-start", total_instances=len(instance_ids))
    agent_bin = args.sinomo_agent_bin
    agent_binary = ensure_sinomo_agent_binary(
        agent_bin,
        explicit=args.sinomo_agent_bin_explicit,
        dry_run=args.dry_run,
        progress=progress,
    )
    tasks = fetch_suite_rows(instance_ids, progress)
    output_dir = args.results_dir / "sinomo"
    reports_dir = output_dir / "reports"
    task_dir = output_dir / "tasks"
    workspaces_dir = output_dir / "workspaces"
    for directory in (reports_dir, task_dir, workspaces_dir):
        if not args.dry_run:
            directory.mkdir(parents=True, exist_ok=True)
    predictions_path = output_dir / "predictions.jsonl"
    if not args.dry_run:
        predictions_path.parent.mkdir(parents=True, exist_ok=True)
        predictions_path.write_text("", encoding="utf-8")
    for index, instance_id in enumerate(instance_ids, start=1):
        progress.emit(
            "sinomo-instance-start",
            instance_id=instance_id,
            current=index,
            total=len(instance_ids),
        )
        task = tasks[instance_id]
        workspace = prepare_workspace(
            task,
            workspaces_dir,
            dry_run=args.dry_run,
            progress=progress,
        )
        progress.emit(
            "sinomo-workspace-ready",
            instance_id=instance_id,
            workspace=str(workspace),
            current=index,
            total=len(instance_ids),
        )
        task_path = task_dir / f"{instance_id}.json"
        report_path = reports_dir / f"{instance_id}.json"
        prediction_path = reports_dir / f"{instance_id}.prediction.json"
        agent_trace_path = reports_dir / f"{instance_id}.trace.jsonl"
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
            "--agent-trace-out",
            str(agent_trace_path),
            "--provider",
            args.provider,
            "--model",
            args.model,
            "--repair-steps",
            str(args.repair_steps),
        ]
        if args.max_steps is not None:
            command.extend(["--max-steps", str(args.max_steps)])
        if args.account:
            command.extend(["--account", args.account])
        try:
            run_command(
                command,
                cwd=REPO_ROOT,
                dry_run=args.dry_run,
                progress=progress,
                phase="agent-run",
                instance_id=instance_id,
                current=index,
                total=len(instance_ids),
            )
        except (subprocess.CalledProcessError, OSError) as source:
            if args.dry_run:
                raise
            blocked_report = write_agent_run_blocked_report(
                task=task,
                report_path=report_path,
                prediction_path=prediction_path,
                agent_trace_path=agent_trace_path,
                workspace=workspace,
                provider=args.provider,
                model=args.model,
                command=command,
                source=source,
            )
            progress.emit(
                "sinomo-instance-blocked",
                instance_id=instance_id,
                current=index,
                total=len(instance_ids),
                report_path=str(report_path),
                blocked_reason=blocked_report["blocked_reason"],
                failure_class=blocked_report["failure_class"],
                exit_code=blocked_report["exit_code"],
            )
            continue
        if not args.dry_run:
            prediction = json.loads(prediction_path.read_text(encoding="utf-8"))
            with predictions_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(prediction, separators=(",", ":")) + "\n")
        progress.emit(
            "sinomo-instance-complete",
            instance_id=instance_id,
            current=index,
            total=len(instance_ids),
            report_path=str(report_path),
            prediction_path=str(prediction_path),
        )
    print(f"Sinomo predictions: {predictions_path}")
    progress.emit("sinomo-complete", predictions_path=str(predictions_path))
    return predictions_path, agent_binary


def run_mini(args: argparse.Namespace, suite: dict, progress: ProgressWriter) -> Path:
    instance_ids = [item["instance_id"] for item in suite["instances"]]
    output_dir = args.results_dir / "mini-swe-agent"
    progress.emit("mini-swe-agent-start", total_instances=len(instance_ids))
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
    run_command(
        command,
        cwd=REPO_ROOT,
        dry_run=args.dry_run,
        progress=progress,
        phase="mini-swe-agent-run",
    )
    print(f"mini-SWE-agent output: {output_dir}")
    progress.emit("mini-swe-agent-complete", predictions_path=str(output_dir / "preds.json"))
    return output_dir / "preds.json"


def evaluate_predictions(
    args: argparse.Namespace,
    predictions_path: Path,
    suite: dict,
    progress: ProgressWriter,
) -> None:
    instance_ids = [item["instance_id"] for item in suite["instances"]]
    missing = sinomo_prediction_prerequisite_issues(args, predictions_path, suite)
    missing.extend(official_evaluation_prerequisite_issues())
    if missing:
        write_evaluation_blocked_marker(args, predictions_path, instance_ids, missing, progress)
        return
    evaluation_dir = args.results_dir / "evaluation"
    if not args.dry_run:
        evaluation_dir.mkdir(parents=True, exist_ok=True)
    progress.emit(
        "evaluation-start",
        predictions_path=str(predictions_path),
        total_instances=len(instance_ids),
    )
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
        "--report_dir",
        str(evaluation_dir),
    ]
    run_command(
        command,
        cwd=evaluation_dir,
        dry_run=args.dry_run,
        progress=progress,
        phase="official-evaluation",
    )
    progress.emit("evaluation-complete", results_dir=str(evaluation_dir))


def write_evaluation_blocked_marker(
    args: argparse.Namespace,
    predictions_path: Path,
    instance_ids: list[str],
    issues: list[str],
    progress: ProgressWriter,
) -> None:
    evaluation_dir = args.results_dir / "evaluation"
    if not args.dry_run:
        evaluation_dir.mkdir(parents=True, exist_ok=True)
    blocked = {
        "requested": True,
        "blocked": True,
        "issues": issues,
        "predictions_path": str(predictions_path),
        "total_instances": len(instance_ids),
    }
    if not args.dry_run:
        with (evaluation_dir / "blocked.json").open("w", encoding="utf-8") as handle:
            json.dump(blocked, handle, indent=2)
            handle.write("\n")
    progress.emit("evaluation-blocked", issues=issues, results_dir=str(evaluation_dir))
    print("official evaluation blocked:")
    for issue in issues:
        print(f"- {issue}")


def sinomo_prediction_prerequisite_issues(
    args: argparse.Namespace,
    predictions_path: Path,
    suite: dict,
) -> list[str]:
    if args.mode not in {"sinomo", "both"} or args.dry_run:
        return []
    instance_ids = [item["instance_id"] for item in suite["instances"]]
    reports_dir = args.results_dir / "sinomo" / "reports"
    blocked_ids = []
    for instance_id in instance_ids:
        report_path = reports_dir / f"{instance_id}.json"
        if not report_path.exists():
            continue
        try:
            report = json.loads(report_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if report.get("status") == "harness-blocked":
            blocked_ids.append(instance_id)

    issues = []
    if blocked_ids:
        issues.append(
            "Sinomo agent run blocked for "
            f"{len(blocked_ids)} instance(s): {', '.join(blocked_ids)}"
        )

    if not predictions_path.exists():
        issues.append(f"Sinomo predictions file is missing: {predictions_path}")
        return issues

    predicted_ids = set()
    try:
        with predictions_path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    prediction = json.loads(line)
                except json.JSONDecodeError as source:
                    issues.append(
                        f"Sinomo predictions file has invalid JSON on line {line_number}: {source}"
                    )
                    continue
                instance_id = prediction.get("instance_id")
                if isinstance(instance_id, str):
                    predicted_ids.add(instance_id)
    except OSError as source:
        issues.append(f"failed to read Sinomo predictions file {predictions_path}: {source}")
        return issues

    missing_predictions = [instance_id for instance_id in instance_ids if instance_id not in predicted_ids]
    if missing_predictions:
        issues.append(
            "Sinomo predictions are incomplete; missing "
            f"{len(missing_predictions)} instance(s): {', '.join(missing_predictions)}"
        )
    return issues


def official_evaluation_prerequisite_issues() -> list[str]:
    issues: list[str] = []
    try:
        subprocess.run(
            [sys.executable, "-c", "import swebench"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        issues.append(
            "Python package `swebench` is not importable; install it in the benchmark runtime"
        )
    if shutil.which("docker") is None:
        issues.append("Docker CLI is not available in the benchmark runtime")
    if not os.environ.get("DOCKER_HOST") and not Path("/var/run/docker.sock").exists():
        issues.append(
            "Docker daemon is not reachable; mount /var/run/docker.sock or set DOCKER_HOST"
        )
    return issues


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
        "completed_instances": None,
        "empty_patch_instances": None,
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
    if parsed.get("parse_status") == "parsed":
        summary.update(parsed)
        return summary

    blocked_path = evaluation_dir / "blocked.json"
    if blocked_path.exists():
        try:
            with blocked_path.open("r", encoding="utf-8") as handle:
                blocked = json.load(handle)
        except (OSError, json.JSONDecodeError) as source:
            summary["parse_status"] = "blocked-unreadable"
            summary["summary_path"] = str(blocked_path)
            summary["issues"] = [f"failed to read blocked evaluation marker: {source}"]
            return summary
        summary["parse_status"] = "blocked"
        summary["summary_path"] = str(blocked_path)
        summary["issues"] = blocked.get("issues", [])
        return summary

    summary.update(parsed)
    return summary


def parse_official_evaluation_results(evaluation_dir: Path, instance_ids: list[str]) -> dict:
    wanted = set(instance_ids)
    resolved: set[str] = set()
    unresolved: set[str] = set()
    errors: set[str] = set()
    completed_instances: int | None = None
    empty_patch_instances: int | None = None
    summary_path: Path | None = None
    for path in sorted(evaluation_dir.rglob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        before = (len(resolved), len(unresolved), len(errors))
        if isinstance(payload, dict):
            completed_instances = integer_field(payload, "completed_instances", completed_instances)
            empty_patch_instances = integer_field(payload, "empty_patch_instances", empty_patch_instances)
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
            "completed_instances": completed_instances,
            "empty_patch_instances": empty_patch_instances,
            "summary_path": str(summary_path) if summary_path else None,
        }
    return {
        "parse_status": "unparsed",
        "resolved_instances": None,
        "unresolved_instances": None,
        "error_instances": None,
        "completed_instances": completed_instances,
        "empty_patch_instances": empty_patch_instances,
        "summary_path": None,
    }


def integer_field(payload: dict, key: str, fallback: int | None) -> int | None:
    value = payload.get(key)
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return fallback


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
    sinomo_agent_binary: dict[str, object] | None,
    mini_predictions_path: Path | None,
    host_dependencies: dict[str, object],
    progress: ProgressWriter,
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
    harness_blocked_reports = [
        report for report in reports if report.get("status") == "harness-blocked"
    ]
    completed_reports = [
        report for report in reports if report.get("status") != "harness-blocked"
    ]
    harness_blocked_instance_ids = [
        report.get("task", {}).get("instance_id")
        for report in harness_blocked_reports
        if isinstance(report.get("task"), dict)
        and isinstance(report.get("task", {}).get("instance_id"), str)
    ]
    agent_trace_paths = [
        report["agent_trace_path"]
        for report in reports
        if isinstance(report.get("agent_trace_path"), str)
    ]
    post_patch_check_failures = sum(
        1
        for report in reports
        if isinstance(report.get("post_patch_check"), dict)
        and report["post_patch_check"].get("passed") is False
    )
    initial_post_patch_check_failures = sum(
        1
        for report in reports
        if isinstance(report.get("initial_post_patch_check"), dict)
        and report["initial_post_patch_check"].get("passed") is False
    )
    official_evaluation = collect_official_evaluation_summary(args, suite)
    agent_final_decision_sources: dict[str, int] = {}
    for report in reports:
        source = report_agent_model_metric(report, "final_decision_source")
        if isinstance(source, str) and source:
            agent_final_decision_sources[source] = agent_final_decision_sources.get(source, 0) + 1
    first_successful_patch_turns = [
        int(report["first_successful_patch_turn"])
        for report in reports
        if isinstance(report.get("first_successful_patch_turn"), int)
        and not isinstance(report.get("first_successful_patch_turn"), bool)
    ]
    coding_validation_status_counts: dict[str, int] = {}
    coding_stop_recommendation_counts: dict[str, int] = {}
    coding_blocked_reason_counts: dict[str, int] = {}
    coding_validation_failure_kind_counts: dict[str, int] = {}
    for report in reports:
        increment_string_count(
            coding_validation_status_counts,
            report.get("coding_validation_status"),
        )
        increment_string_count(
            coding_stop_recommendation_counts,
            report.get("coding_stop_recommendation"),
        )
        increment_string_count(coding_blocked_reason_counts, report.get("coding_blocked_reason"))
        failure_kinds = report.get("coding_validation_failure_kinds")
        if isinstance(failure_kinds, list):
            for failure_kind in failure_kinds:
                increment_string_count(coding_validation_failure_kind_counts, failure_kind)
    sinomo_summary = {
        "prediction_path": str(predictions_path) if predictions_path else None,
        "agent_binary": sinomo_agent_binary,
        "report_dir": str(reports_dir),
        "report_count": len(reports),
        "completed_report_count": len(completed_reports),
        "harness_blocked_instances": len(harness_blocked_reports),
        "harness_blocked_instance_ids": harness_blocked_instance_ids,
        "agent_trace_paths": agent_trace_paths,
        "agent_trace_records": sum(int(report.get("agent_trace_records", 0)) for report in reports),
        "agent_event_count": sum(int(report.get("agent_event_count", 0)) for report in reports),
        "agent_item_count": sum(int(report.get("agent_item_count", 0)) for report in reports),
        "agent_tool_call_items": sum(
            int(report.get("agent_tool_call_items", 0)) for report in reports
        ),
        "agent_tool_result_items": sum(
            int(report.get("agent_tool_result_items", 0)) for report in reports
        ),
        "orphan_tool_calls": sum(int(report.get("orphan_tool_calls", 0)) for report in reports),
        "orphan_tool_results": sum(
            int(report.get("orphan_tool_results", 0)) for report in reports
        ),
        "patch_bytes": sum(int(report.get("patch_bytes", 0)) for report in reports),
        "changed_files": sum(len(report.get("changed_files", [])) for report in reports),
        "initial_post_patch_check_failures": initial_post_patch_check_failures,
        "post_patch_check_failures": post_patch_check_failures,
        "repair_attempts": sum(1 for report in reports if report.get("repair_attempted")),
        "post_patch_repair_attempts": sum(
            1 for report in reports if report.get("repair_reason") == "post-patch-check-failure"
        ),
        "empty_patch_repair_attempts": sum(
            1 for report in reports if report.get("repair_reason") == "empty-patch"
        ),
        "repair_model_turns": sum(int(report.get("repair_model_turns", 0)) for report in reports),
        "repair_tool_results": sum(int(report.get("repair_tool_results", 0)) for report in reports),
        "deterministic_repair_attempts": sum(
            1 for report in reports if report.get("deterministic_repair_attempted")
        ),
        "deterministic_repair_changed_files": sum(
            len(report.get("deterministic_repair_changed_files", [])) for report in reports
        ),
        "missing_report_instances": len(instance_ids) - len(reports),
        "error_instances": len(instance_ids) - len(completed_reports),
        "resolved_instances": official_evaluation.get("resolved_instances"),
        "unresolved_instances": official_evaluation.get("unresolved_instances"),
        "official_completed_instances": official_evaluation.get("completed_instances"),
        "official_empty_patch_instances": official_evaluation.get("empty_patch_instances"),
        "agent_model_turns": sum(int(report.get("model_turns", 0)) for report in reports),
        "agent_tool_results": sum(int(report.get("tool_results", 0)) for report in reports),
        "command_observation_turns": sum(
            int(report.get("command_observation_turns", 0)) for report in reports
        ),
        "command_tool_results": sum(
            int(report.get("command_tool_results", 0)) for report in reports
        ),
        "cancelled_tool_results": sum(
            int(report.get("cancelled_tool_results", 0)) for report in reports
        ),
        "cancelled_runs": sum(int(report.get("cancelled_runs", 0)) for report in reports),
        "coding_validation_attempts": sum(
            int(report.get("coding_validation_attempts", 0)) for report in reports
        ),
        "coding_validation_status_counts": coding_validation_status_counts,
        "coding_stop_recommendation_counts": coding_stop_recommendation_counts,
        "coding_blocked_reason_counts": coding_blocked_reason_counts,
        "coding_validation_failure_kind_counts": coding_validation_failure_kind_counts,
        "instances_with_successful_patch": len(first_successful_patch_turns),
        "first_successful_patch_turn_sum": sum(first_successful_patch_turns),
        "first_successful_patch_turn_min": (
            min(first_successful_patch_turns) if first_successful_patch_turns else None
        ),
        "first_successful_patch_turn_max": (
            max(first_successful_patch_turns) if first_successful_patch_turns else None
        ),
        "extra_turns_after_patch": sum(
            int(report.get("extra_turns_after_patch", 0)) for report in reports
        ),
        "agent_input_tokens": sum(int(report.get("agent_input_tokens", 0)) for report in reports),
        "agent_output_tokens": sum(int(report.get("agent_output_tokens", 0)) for report in reports),
        "agent_model_response_count": sum(
            report_agent_model_metric_int(report, "model_response_count") for report in reports
        ),
        "agent_model_response_parse_failures": sum(
            report_agent_model_metric_int(report, "model_response_parse_failure_count")
            for report in reports
        ),
        "agent_strict_json_decision_turns": sum(
            report_agent_model_metric_int(report, "strict_json_decision_turn_count")
            for report in reports
        ),
        "agent_text_fallback_turns": sum(
            report_agent_model_metric_int(report, "text_fallback_turn_count") for report in reports
        ),
        "agent_native_tool_call_turns": sum(
            report_agent_model_metric_int(report, "native_tool_call_turn_count")
            for report in reports
        ),
        "agent_native_tool_calls": sum(
            report_agent_model_metric_int(report, "native_tool_call_count") for report in reports
        ),
        "agent_vendor_fallback_tool_call_turns": sum(
            report_agent_model_metric_int(report, "vendor_fallback_tool_call_turn_count")
            for report in reports
        ),
        "agent_vendor_fallback_tool_calls": sum(
            report_agent_model_metric_int(report, "vendor_fallback_tool_call_count")
            for report in reports
        ),
        "agent_model_repair_attempts": sum(
            report_agent_model_metric_int(report, "repair_attempt_count") for report in reports
        ),
        "agent_repair_request_model_visible_bytes": sum(
            report_agent_model_metric_int(report, "repair_request_model_visible_bytes")
            for report in reports
        ),
        "agent_model_repair_reported_usage_responses": sum(
            report_agent_model_metric_int(report, "repair_reported_usage", "response_count")
            for report in reports
        ),
        "agent_model_repair_input_tokens": sum(
            report_agent_model_metric_int(report, "repair_reported_usage", "input_tokens")
            for report in reports
        ),
        "agent_model_repair_output_tokens": sum(
            report_agent_model_metric_int(report, "repair_reported_usage", "output_tokens")
            for report in reports
        ),
        "agent_final_decision_sources": agent_final_decision_sources,
    }
    summary = {
        "benchmark": "swebench",
        "suite": suite["name"],
        "dataset": suite["dataset"],
        "split": suite["split"],
        "instances": instance_ids,
        "total_instances": len(instance_ids),
        "mode": args.mode,
        "harness_status": host_dependency_harness_status(args, host_dependencies),
        "results_dir": str(args.results_dir),
        "progress_path": str(args.progress_out) if args.progress_out else None,
        "host_dependencies": host_dependencies,
        "sinomo": sinomo_summary,
        "mini_swe_agent": {
            "prediction_path": str(mini_predictions_path) if mini_predictions_path else None,
        },
        "official_evaluation": official_evaluation,
    }
    args.summary_out.parent.mkdir(parents=True, exist_ok=True)
    args.summary_out.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"summary: {args.summary_out}")
    progress.emit("summary-written", summary_path=str(args.summary_out), dry_run=args.dry_run)


def report_agent_model_metric(report: dict[str, object], *path: str) -> object | None:
    value: object = report.get("agent_model_metrics")
    for key in path:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def report_agent_model_metric_int(report: dict[str, object], *path: str) -> int:
    value = report_agent_model_metric(report, *path)
    try:
        return int(value) if value is not None else 0
    except (TypeError, ValueError):
        return 0


def increment_string_count(counts: dict[str, int], value: object) -> None:
    if isinstance(value, str) and value:
        counts[value] = counts.get(value, 0) + 1


def print_plan(args: argparse.Namespace, suite: dict, host_dependencies: dict[str, object]) -> None:
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
            "max_steps_policy": "explicit-cap" if args.max_steps is not None else "unlimited-default",
            "repair_steps": args.repair_steps,
        },
        "sinomo_agent_binary": {
            "path": str(args.sinomo_agent_bin),
            "explicit": args.sinomo_agent_bin_explicit,
            "freshness": (
                "explicit binary; runner records provenance but does not rebuild"
                if args.sinomo_agent_bin_explicit
                else "default binary rebuilt before every Sinomo run"
            ),
        },
        "mini_swe_agent": {
            "model": args.baseline_model,
            "filter": suite_filter_regex(instance_ids),
            "workers": args.workers,
        },
        "results_dir": str(args.results_dir),
        "host_dependencies": host_dependencies,
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
    parser.add_argument("--sinomo-agent-bin", type=Path)
    parser.add_argument(
        "--max-steps",
        type=int,
        default=None,
        help="optional model-turn cap; omit for the continuous-agent default",
    )
    parser.add_argument("--repair-steps", type=int, default=6)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--environment-class", choices=["docker", "singularity", "bubblewrap", "local"], default=None)
    parser.add_argument("--evaluate", action="store_true")
    parser.add_argument("--run-id", default="sinomo-deepseek-v4-flash-verified-10")
    parser.add_argument("--summary-out", type=Path, help="write a machine-readable benchmark summary JSON")
    parser.add_argument("--progress-out", type=Path, help="write machine-readable progress JSONL")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    args.results_dir = args.results_dir.resolve()
    args.sinomo_agent_bin_explicit = args.sinomo_agent_bin is not None
    if args.sinomo_agent_bin is None:
        args.sinomo_agent_bin = DEFAULT_AGENT_BIN
    args.sinomo_agent_bin = args.sinomo_agent_bin.resolve()
    if args.summary_out:
        args.summary_out = args.summary_out.resolve()
    if args.progress_out:
        args.progress_out = args.progress_out.resolve()
    return args


def main() -> int:
    args = parse_args()
    progress = ProgressWriter(args.progress_out)
    try:
        suite = load_suite(args.suite)
        suite = select_suite_instances(suite, instance_id=args.instance_id, limit=args.limit)
        progress.emit(
            "suite-selected",
            suite=suite["name"],
            mode=args.mode,
            total_instances=len(suite["instances"]),
            dry_run=args.dry_run,
        )
        host_dependencies = host_dependency_probe(args, suite)
        progress.emit(
            "host-dependency-probe",
            blocking_issue_count=host_dependencies["blocking_issue_count"],
            blocking_issues=host_dependencies["blocking_issues"],
            mode=args.mode,
            evaluate=args.evaluate,
        )
        print_plan(args, suite, host_dependencies)
        predictions_path: Path | None = None
        sinomo_agent_binary: dict[str, object] | None = None
        mini_predictions_path: Path | None = None
        if should_stop_for_host_dependency_blockers(args, host_dependencies):
            blocking_issues = host_dependency_blocking_issues(host_dependencies)
            progress.emit(
                "host-dependency-blocked",
                blocking_issue_count=len(blocking_issues),
                blocking_issues=blocking_issues,
                mode=args.mode,
                evaluate=args.evaluate,
            )
            if args.evaluate:
                write_evaluation_blocked_marker(
                    args,
                    args.results_dir / "sinomo" / "predictions.jsonl",
                    [item["instance_id"] for item in suite["instances"]],
                    blocking_issues,
                    progress,
                )
            write_summary(
                args,
                suite,
                predictions_path=predictions_path,
                sinomo_agent_binary=sinomo_agent_binary,
                mini_predictions_path=mini_predictions_path,
                host_dependencies=host_dependencies,
                progress=progress,
            )
            raise SystemExit(
                "benchmark host dependencies are not ready: " + "; ".join(blocking_issues)
            )
        if args.mode in {"sinomo", "both"}:
            predictions_path, sinomo_agent_binary = run_sinomo(args, suite, progress)
        if args.mode in {"mini", "both"}:
            mini_predictions_path = run_mini(args, suite, progress)
        if args.evaluate:
            if predictions_path is None:
                predictions_path = args.results_dir / "sinomo" / "predictions.jsonl"
            evaluate_predictions(args, predictions_path, suite, progress)
        write_summary(
            args,
            suite,
            predictions_path=predictions_path,
            sinomo_agent_binary=sinomo_agent_binary,
            mini_predictions_path=mini_predictions_path,
            host_dependencies=host_dependencies,
            progress=progress,
        )
        progress.emit("benchmark-complete", summary_path=str(args.summary_out) if args.summary_out else None)
        return 0
    except KeyboardInterrupt:
        progress.emit("benchmark-failed", error="keyboard-interrupt")
        raise
    except SystemExit as source:
        if source.code not in (0, None):
            progress.emit("benchmark-failed", error=str(source))
        raise
    except Exception as source:
        progress.emit("benchmark-failed", error=str(source))
        raise


if __name__ == "__main__":
    raise SystemExit(main())
