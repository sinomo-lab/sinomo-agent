#!/usr/bin/env python3
"""Fail-closed process and filesystem isolation for Sinomo evaluations.

Benchmark cases must never inherit the operator's Sinomo state.  This module
creates one disposable data directory, home, XDG tree, temporary directory and
working directory per case.  Only explicitly selected language-model provider
configuration may be copied into that directory.
"""

from __future__ import annotations

import contextlib
import datetime
import json
import os
import re
import shutil
import signal
import subprocess
import tempfile
import threading
import uuid
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Iterable, Iterator, Mapping, Sequence


ISOLATION_SCHEMA = "sinomo.benchmark.isolation.v1"
SENTINEL_NAME = ".sinomo-benchmark-isolation.json"
SAFE_PROVIDER_ID = re.compile(r"^[A-Za-z0-9._-]+$")
SAFE_ENVIRONMENT_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PATH_BEARING_ENVIRONMENT_NAME_PARTS = (
    "HOME",
    "PATH",
    "FILE",
    "DIR",
    "ROOT",
    "WORKSPACE",
    "CACHE",
    "CONFIG",
    "STATE",
    "TMP",
    "TEMP",
    "SOCKET",
    "PIPE",
    "CREDENTIAL",
)

_INHERITED_ENVIRONMENT = {
    "PATH",
    "LANG",
    "LANGUAGE",
    "TZ",
    "TERM",
    "COLORTERM",
    "RUST_BACKTRACE",
    "RUST_LOG",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "REQUESTS_CA_BUNDLE",
    "CURL_CA_BUNDLE",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
    "SystemRoot",
    "SYSTEMROOT",
    "ComSpec",
    "COMSPEC",
    "PATHEXT",
}


def utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def default_config_source_data_dir() -> Path:
    explicit = os.environ.get("SINOMO_DATA_DIR", "").strip()
    return Path(explicit) if explicit else Path.home() / ".sinomo"


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def paths_overlap(left: Path, right: Path) -> bool:
    left = left.resolve(strict=False)
    right = right.resolve(strict=False)
    return _is_relative_to(left, right) or _is_relative_to(right, left)


def validate_operational_path_separation(
    *,
    config_source_data_dir: Path | None,
    protected_paths: Iterable[Path],
) -> tuple[Path, list[Path]]:
    source_root = (
        config_source_data_dir.expanduser()
        if config_source_data_dir is not None
        else default_config_source_data_dir()
    ).resolve(strict=False)
    protected = [path.expanduser().resolve(strict=False) for path in protected_paths]
    for path in protected:
        if paths_overlap(path, source_root):
            raise ValueError(
                f"benchmark input/output {path} overlaps operational data {source_root}"
            )
    return source_root, protected


def validate_pairwise_disjoint_paths(paths: Iterable[Path]) -> list[Path]:
    resolved = [path.expanduser().resolve(strict=False) for path in paths]
    for index, path in enumerate(resolved):
        for other in resolved[index + 1 :]:
            same_file = False
            if path.exists() and other.exists():
                with contextlib.suppress(OSError):
                    same_file = path.samefile(other)
            if same_file or paths_overlap(path, other):
                raise ValueError(f"benchmark artifact paths overlap: {path} and {other}")
    return resolved


def validate_isolation_boundaries(
    *,
    scratch_root: Path | None,
    config_source_data_dir: Path | None,
    protected_paths: Iterable[Path],
) -> tuple[Path, Path, list[Path]]:
    """Resolve and reject every operational/scratch/result path overlap.

    This check is side-effect free so callers can run it before opening progress
    or result files.  The context manager repeats it before creating a case.
    """

    source_root, protected = validate_operational_path_separation(
        config_source_data_dir=config_source_data_dir,
        protected_paths=protected_paths,
    )
    scratch_candidate = (scratch_root or Path(tempfile.gettempdir())).expanduser().resolve(
        strict=False
    )
    if paths_overlap(scratch_candidate, source_root):
        raise ValueError(
            f"scratch root {scratch_candidate} overlaps operational data {source_root}"
        )
    for path in protected:
        if paths_overlap(path, scratch_candidate):
            raise ValueError(
                f"benchmark input/output {path} overlaps scratch root {scratch_candidate}"
            )
    return source_root, scratch_candidate, protected


def _validate_provider_id(provider: str) -> None:
    if provider in {".", ".."} or not SAFE_PROVIDER_ID.fullmatch(provider):
        raise ValueError(
            "provider id may contain only ASCII letters, digits, dot, underscore and hyphen"
        )


def _copy_regular_file(
    source_root: Path,
    destination_root: Path,
    relative: Path,
) -> bool:
    source = source_root / relative
    if not source.exists():
        return False
    if source.is_symlink() or not source.is_file():
        raise ValueError(f"provider state must be a regular non-symlink file: {source}")
    resolved_root = source_root.resolve(strict=True)
    resolved_source = source.resolve(strict=True)
    if not _is_relative_to(resolved_source, resolved_root):
        raise ValueError(f"provider state escapes its source root: {source}")
    destination = destination_root / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination, follow_symlinks=False)
    return True


def _read_regular_json_file(source_root: Path, relative: Path) -> object | None:
    source = source_root / relative
    if not source.exists():
        return None
    if source.is_symlink() or not source.is_file():
        raise ValueError(f"provider state must be a regular non-symlink file: {source}")
    resolved_root = source_root.resolve(strict=True)
    resolved_source = source.resolve(strict=True)
    if not _is_relative_to(resolved_source, resolved_root):
        raise ValueError(f"provider state escapes its source root: {source}")
    try:
        return json.loads(source.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as source_error:
        raise ValueError(
            f"invalid provider configuration {source}: {source_error}"
        ) from source_error


def _selected_account_config(
    payload: object,
    *,
    provider: str,
    account: str,
) -> object:
    if not isinstance(payload, dict):
        raise ValueError("provider configuration must be a JSON object")
    is_document = payload.get("key") == "lm.providers"
    config = payload.get("value") if is_document else payload
    if not isinstance(config, dict):
        raise ValueError("provider configuration value must be a JSON object")
    accounts = config.get("accounts", [])
    if not isinstance(accounts, list):
        raise ValueError("provider configuration accounts must be an array")

    selected: list[object] = []
    matching_disabled = False
    for candidate in accounts:
        if not isinstance(candidate, dict):
            raise ValueError("provider configuration account entries must be objects")
        candidate_account_value = candidate.get("account", "default")
        if not isinstance(candidate_account_value, str):
            raise ValueError("provider configuration account name must be text")
        candidate_account = candidate_account_value.strip() or "default"
        enabled = candidate.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValueError("provider configuration account enabled flag must be boolean")
        if candidate.get("provider") != provider or candidate_account != account:
            continue
        if enabled:
            selected.append(candidate)
        else:
            matching_disabled = True
    if not selected:
        if matching_disabled:
            raise ValueError(
                f"provider configuration account {provider!r}/{account!r} is disabled"
            )
        raise ValueError(
            f"provider configuration has no account {provider!r}/{account!r}"
        )

    filtered = {
        "default_provider": provider,
        "default_account": account,
        "accounts": selected,
    }
    if not is_document:
        return filtered
    updated_at = payload.get("updated_at")
    if not isinstance(updated_at, str) or not updated_at:
        raise ValueError("provider configuration document has no updated_at timestamp")
    return {
        "key": "lm.providers",
        "value": filtered,
        "updated_at": updated_at,
    }


def _copy_selected_account_config(
    source_root: Path,
    destination_root: Path,
    provider: str,
    account: str,
) -> str:
    account = account.strip()
    if not account:
        raise ValueError("provider account must not be empty")
    payload: object | None = None
    for relative in (Path("configs/lm/providers.json"), Path("config/models.json")):
        payload = _read_regular_json_file(source_root, relative)
        if payload is not None:
            break
    if payload is None:
        raise ValueError(
            f"provider account {provider!r}/{account!r} requires provider configuration"
        )
    filtered = _selected_account_config(payload, provider=provider, account=account)
    destination_relative = Path("configs/lm/providers.json")
    destination = destination_root / destination_relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(filtered, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    # Account configuration may carry the selected account's inline API key.
    return destination_relative.as_posix()


def copy_selected_provider_state(
    source_root: Path,
    destination_root: Path,
    provider: str,
    account: str | None = None,
) -> list[str]:
    """Copy the minimum provider/model configuration needed for a model call.

    Cluster, DFS, node identity, permissions, events, sessions, memory and agent
    state are deliberately not candidates here.
    """

    _validate_provider_id(provider)
    source_root = source_root.expanduser()
    if not source_root.exists():
        if account is not None:
            raise ValueError(
                f"provider account {provider!r}/{account!r} requires provider configuration"
            )
        return []
    copied: list[str] = []
    if account is not None:
        copied.append(
            _copy_selected_account_config(
                source_root,
                destination_root,
                provider,
                account,
            )
        )
    credential_candidates = [
        Path("configs/lm/credentials") / f"{provider}.json",
        Path("providers") / provider / "credentials.json",
    ]
    for relative in credential_candidates:
        if _copy_regular_file(
            source_root, destination_root, relative
        ):
            copied.append(relative.as_posix())
            break
    return copied


def validate_explicit_environment_names(names: Iterable[str]) -> list[str]:
    validated: list[str] = []
    for name in names:
        if not SAFE_ENVIRONMENT_NAME.fullmatch(name):
            raise ValueError(f"invalid environment variable name: {name!r}")
        if name.upper().startswith("SINOMO_"):
            raise ValueError(
                f"environment variable {name!r} is controlled by benchmark isolation"
            )
        if any(part in name.upper() for part in _PATH_BEARING_ENVIRONMENT_NAME_PARTS):
            raise ValueError(
                f"path-bearing environment variable {name!r} can escape benchmark isolation"
            )
        validated.append(name)
    return validated


def isolated_child_environment(
    case: "IsolatedSinomoCase",
    *,
    source_environment: Mapping[str, str] | None = None,
    pass_environment: Iterable[str] = (),
) -> dict[str, str]:
    source = os.environ if source_environment is None else source_environment
    explicit = validate_explicit_environment_names(pass_environment)
    environment: dict[str, str] = {}
    for name, value in source.items():
        if name in _INHERITED_ENVIRONMENT or name.startswith("LC_") or name in explicit:
            environment[name] = value

    environment.update(
        {
            "SINOMO_DATA_DIR": str(case.data_dir),
            "SINOMO_BENCHMARK_EXPERIMENT": "1",
            "HOME": str(case.home_dir),
            "USERPROFILE": str(case.home_dir),
            "XDG_CONFIG_HOME": str(case.xdg_config_dir),
            "XDG_CACHE_HOME": str(case.xdg_cache_dir),
            "XDG_DATA_HOME": str(case.xdg_data_dir),
            "XDG_STATE_HOME": str(case.xdg_state_dir),
            "TMPDIR": str(case.temp_dir),
            "TMP": str(case.temp_dir),
            "TEMP": str(case.temp_dir),
        }
    )
    return environment


def _safe_prefix(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-.")
    return (normalized or "case")[:48]


@dataclass(frozen=True)
class IsolatedSinomoCase:
    root: Path
    scratch_parent: Path
    sentinel_nonce: str
    data_dir: Path
    corpus_dir: Path
    workspace_dir: Path
    home_dir: Path
    xdg_config_dir: Path
    xdg_cache_dir: Path
    xdg_data_dir: Path
    xdg_state_dir: Path
    temp_dir: Path
    copied_provider_state: tuple[str, ...]
    environment: Mapping[str, str]

    def manifest(self) -> dict[str, object]:
        return {
            "schema": ISOLATION_SCHEMA,
            "root": str(self.root),
            "data_dir": str(self.data_dir),
            "workspace_dir": str(self.workspace_dir),
            "copied_provider_state": list(self.copied_provider_state),
            "operational_data_dir_used_as_runtime": False,
            "non_provider_operational_state_copied": False,
            "local_permissions_copied": False,
            "remote_management_permission_copied": False,
            "unsafe_tool_permissions_copied": False,
            "cluster_state_copied": False,
            "dfs_state_copied": False,
            "memory_state_copied": False,
        }


def _assert_generated_root_is_disposable(case: IsolatedSinomoCase) -> None:
    root = case.root.resolve(strict=True)
    parent = case.scratch_parent.resolve(strict=True)
    if root == parent or not _is_relative_to(root, parent):
        raise RuntimeError(f"refusing to clean non-generated benchmark root {root}")
    sentinel = root / SENTINEL_NAME
    try:
        payload = json.loads(sentinel.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as source:
        raise RuntimeError(f"refusing to clean unmarked benchmark root {root}: {source}")
    if payload.get("schema") != ISOLATION_SCHEMA or payload.get("nonce") != case.sentinel_nonce:
        raise RuntimeError(f"refusing to clean benchmark root with mismatched sentinel {root}")


def install_cleanup_signal_handlers() -> None:
    """Turn service-style termination signals into normal stack unwinding."""

    if threading.current_thread() is not threading.main_thread():
        return

    def terminate(signum: int, _frame: object) -> None:
        raise SystemExit(128 + signum)

    for name in ("SIGTERM", "SIGHUP"):
        candidate = getattr(signal, name, None)
        if candidate is not None:
            signal.signal(candidate, terminate)


@contextlib.contextmanager
def isolated_sinomo_case(
    case_id: str,
    *,
    scratch_root: Path | None = None,
    config_source_data_dir: Path | None = None,
    provider: str | None = None,
    account: str | None = None,
    copy_provider_config: bool = False,
    pass_environment: Iterable[str] = (),
    protected_paths: Iterable[Path] = (),
) -> Iterator[IsolatedSinomoCase]:
    source_root, scratch_candidate, protected = validate_isolation_boundaries(
        scratch_root=scratch_root,
        config_source_data_dir=config_source_data_dir,
        protected_paths=protected_paths,
    )

    scratch_candidate.mkdir(parents=True, exist_ok=True)
    scratch_parent = scratch_candidate.resolve(strict=True)
    if paths_overlap(scratch_parent, source_root):
        raise ValueError(
            f"scratch root {scratch_parent} overlaps operational data {source_root}"
        )
    for path in protected:
        if paths_overlap(path, scratch_parent):
            raise ValueError(f"benchmark input/output {path} overlaps scratch root {scratch_parent}")

    root: Path | None = None
    case: IsolatedSinomoCase | None = None
    try:
        root = Path(
            tempfile.mkdtemp(
                prefix=f"sinomo-memory-eval-{_safe_prefix(case_id)}-",
                dir=scratch_parent,
            )
        )
        root = root.resolve(strict=True)
        for path in [source_root, *protected]:
            if paths_overlap(root, path):
                raise ValueError(f"generated benchmark root {root} overlaps {path}")

        nonce = uuid.uuid4().hex
        (root / SENTINEL_NAME).write_text(
            json.dumps(
                {
                    "schema": ISOLATION_SCHEMA,
                    "nonce": nonce,
                    "case_id": case_id,
                    "created_at": utc_now(),
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        directories = {
            "data_dir": root / "data",
            "corpus_dir": root / "corpus",
            "workspace_dir": root / "workspace",
            "home_dir": root / "home",
            "xdg_config_dir": root / "xdg" / "config",
            "xdg_cache_dir": root / "xdg" / "cache",
            "xdg_data_dir": root / "xdg" / "data",
            "xdg_state_dir": root / "xdg" / "state",
            "temp_dir": root / "tmp",
        }
        for directory in directories.values():
            directory.mkdir(parents=True, exist_ok=True)

        copied: list[str] = []
        if copy_provider_config:
            if not provider:
                raise ValueError("provider is required when copying provider configuration")
            copied = copy_selected_provider_state(
                source_root,
                directories["data_dir"],
                provider,
                account,
            )

        case_without_environment = IsolatedSinomoCase(
            root=root,
            scratch_parent=scratch_parent,
            sentinel_nonce=nonce,
            copied_provider_state=tuple(copied),
            environment={},
            **directories,
        )
        environment = isolated_child_environment(
            case_without_environment,
            pass_environment=pass_environment,
        )
        case = replace(case_without_environment, environment=environment)
        yield case
    finally:
        if root is None:
            pass
        elif case is None:
            shutil.rmtree(root)
        else:
            _assert_generated_root_is_disposable(case)
            shutil.rmtree(case.root)


def run_isolated_process(
    command: Sequence[str],
    case: IsolatedSinomoCase,
    *,
    timeout_seconds: float,
    display_command: Sequence[str] | None = None,
) -> dict[str, object]:
    if not command:
        raise ValueError("isolated process command must not be empty")
    if timeout_seconds <= 0:
        raise ValueError("isolated process timeout must be positive")
    started = utc_now()
    creationflags = 0
    start_new_session = os.name != "nt"
    if os.name == "nt":
        creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) | getattr(
            subprocess, "CREATE_NO_WINDOW", 0
        )
    process = subprocess.Popen(
        list(command),
        cwd=case.workspace_dir,
        env=dict(case.environment),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        shell=False,
        close_fds=True,
        start_new_session=start_new_session,
        creationflags=creationflags,
    )
    timed_out = False
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        timed_out = True
        stdout, stderr = _terminate_and_collect(process)
    except BaseException:
        _terminate_and_collect(process)
        raise
    return {
        "command": list(display_command or command),
        "started_at": started,
        "completed_at": utc_now(),
        "exit_code": process.returncode,
        "stdout": stdout,
        "stderr": stderr,
        "timed_out": timed_out,
        "isolation": case.manifest(),
    }


def _terminate_and_collect(process: subprocess.Popen[str]) -> tuple[str, str]:
    _terminate_process_tree(process, force=False)
    try:
        return process.communicate(timeout=2)
    except subprocess.TimeoutExpired:
        _terminate_process_tree(process, force=True)
        return process.communicate()


def _terminate_process_tree(process: subprocess.Popen[str], *, force: bool) -> None:
    if process.poll() is not None:
        return
    if os.name != "nt":
        requested_signal = signal.SIGKILL if force else signal.SIGTERM
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, requested_signal)
        return

    system_root = Path(os.environ.get("SystemRoot", r"C:\Windows"))
    taskkill = system_root / "System32" / "taskkill.exe"
    command = [str(taskkill), "/PID", str(process.pid), "/T"]
    if force:
        command.append("/F")
    completed: subprocess.CompletedProcess[bytes] | None = None
    with contextlib.suppress(OSError):
        completed = subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    if completed is None or completed.returncode != 0:
        with contextlib.suppress(OSError):
            process.kill() if force else process.terminate()
