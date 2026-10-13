#!/usr/bin/env python3
"""Measure one Linux compiler workload, without changing compiler configuration.

Run only after obtaining the shared build slot. The advisory flock covers metadata,
the workload, and evidence collection; it never removes or steals an existing lock.
Use a dedicated target directory for an empty-target measurement. This script never
cleans a target, drops caches, changes jobs, or resets cgroup counters.

Example (dependency caches and OS page cache are still warm/unknown):
  python3 scripts/ci/measure-compile-cost.py --label lm-empty-target \
    --kind build --profile dev --cache-state empty-target \
    --target-dir target/maintenance-coordination/lm-target \
    --output target/maintenance-coordination/lm-empty-target \
    --time-bin /absolute/path/to/gnu/time -- \
    cargo build -p sinomo-lm --lib --locked --offline --timings

GNU time / wait4 maximum RSS describes a largest child high-water mark, NOT
simultaneous process-tree memory. Sampled RSS sums double-count shared pages;
sampled PSS apportions shared pages but remains a sampled estimate. Shared cgroup
current/peak counters include unrelated work and historical usage, not build cost.
"""

import argparse
import contextlib
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time


ENV_KEYS = (
    "CARGO_BUILD_JOBS", "CARGO_BUILD_TARGET", "CARGO_INCREMENTAL", "CARGO_PROFILE_DEV_DEBUG",
    "CARGO_PROFILE_DEV_OPT_LEVEL", "CARGO_PROFILE_TEST_DEBUG", "CARGO_PROFILE_TEST_LTO",
    "CARGO_PROFILE_RELEASE_LTO", "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS",
    "RUSTC", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "RUSTUP_TOOLCHAIN",
    "CARGO_HOME", "RUSTUP_HOME", "CC", "CXX", "LD", "AR",
)

CANCEL_TERM_GRACE_S = 1.0
CANCEL_KILL_WAIT_S = 1.0


class WorkloadInterrupted(Exception):
    def __init__(self, signum):
        self.signum = signum
        super().__init__(f"observer received {signal.Signals(signum).name}")


@contextlib.contextmanager
def workload_signal_handlers():
    """Record signals without interrupting spawn/reap critical sections."""
    received, previous = [], {}

    def receive(signum, _frame):
        received.append(signum)

    try:
        for signum in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
            previous[signum] = signal.getsignal(signum)
            signal.signal(signum, receive)
        yield received
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def sample_pause(interval, received):
    """Keep the sampling interval while noticing cancellation promptly."""
    deadline = time.monotonic() + interval
    while not received:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(remaining, 0.05))


def cancel_workload(process):
    """Bounded TERM/KILL of only the process group created by this observer.

    Keep waiting for group members even if the immediate child exits first. An
    unreapable child retains its inherited lock FD instead of releasing the slot.
    """
    result = {"term_sent": False, "kill_sent": False, "errors": [],
              "term_grace_s": CANCEL_TERM_GRACE_S, "kill_wait_s": CANCEL_KILL_WAIT_S}
    usage, can_reap = None, process.returncode is None

    def group_exists():
        try:
            os.killpg(process.pid, 0)
            return True
        except ProcessLookupError:
            return False

    def send(signum, key):
        try:
            os.killpg(process.pid, signum)
            result[key] = True
        except ProcessLookupError:
            pass
        except OSError as error:
            result["errors"].append(f"{signal.Signals(signum).name}: {error}")

    def reap():
        nonlocal usage, can_reap
        if not can_reap:
            return
        try:
            pid, status, child_usage = os.wait4(process.pid, os.WNOHANG)
            if pid:
                process.returncode = os.waitstatus_to_exitcode(status)
                usage, can_reap = child_usage, False
        except OSError as error:
            can_reap = False
            result["errors"].append(f"wait4: {error}")

    def wait_until(deadline):
        while True:
            reap()
            if not group_exists() and process.returncode is not None:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(remaining, 0.02))

    send(signal.SIGTERM, "term_sent")
    wait_until(time.monotonic() + CANCEL_TERM_GRACE_S)
    if group_exists():
        send(signal.SIGKILL, "kill_sent")
    wait_until(time.monotonic() + CANCEL_KILL_WAIT_S)
    reap()
    result["root_reaped"] = process.returncode is not None
    result["group_exists_after_cleanup"] = group_exists()
    return usage, result


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None


def capture(argv, cwd):
    result = subprocess.run(argv, cwd=cwd, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, check=False)
    return {"argv": argv, "exit_code": result.returncode, "output": result.stdout.strip()}


def source_identity(repo):
    head = capture(["git", "rev-parse", "HEAD"], repo)
    patch = subprocess.run(["git", "diff", "--binary", "HEAD"], cwd=repo,
                           stdout=subprocess.PIPE, check=True).stdout
    return {
        "repo": str(repo), "head": head["output"],
        "branch": capture(["git", "branch", "--show-current"], repo)["output"],
        "tracked_patch_sha256": hashlib.sha256(patch).hexdigest(),
        "status": capture(["git", "status", "--porcelain=v1"], repo)["output"],
        "lockfile_sha256": digest(repo / "Cargo.lock"),
        "root_manifest_sha256": digest(repo / "Cargo.toml"),
    }


def parse_stat(text):
    """Parse /proc/PID/stat; comm can contain whitespace and closing parentheses."""
    left, right = text.index("("), text.rindex(")")
    fields = text[right + 2:].split()
    return {"pid": int(text[:left]), "name": text[left + 1:right],
            "ppid": int(fields[1]), "session": int(fields[3]),
            "start_ticks": int(fields[19]),
            "rss_bytes": int(fields[21]) * os.sysconf("SC_PAGE_SIZE")}


def parse_kib_values(text):
    result = {}
    for line in text.splitlines():
        parts = line.replace(":", "", 1).split()
        if len(parts) >= 2 and parts[1].isdigit():
            result[parts[0]] = int(parts[1]) * (1024 if parts[2:] == ["kB"] else 1)
    return result


def select_processes(root_pid, processes):
    """Include descendants and reparented members of the workload's new session."""
    selected = {root_pid} | {pid for pid, row in processes.items() if row["session"] == root_pid}
    while True:
        extra = {pid for pid, row in processes.items() if row["ppid"] in selected}
        if extra <= selected:
            return [row for pid, row in processes.items() if pid in selected]
        selected |= extra


def read_optional(path):
    try:
        return path.read_text().strip()
    except (OSError, UnicodeError):
        return None


def cgroup_paths(proc_root=Path("/proc"), cgroup_root=Path("/sys/fs/cgroup")):
    own = read_optional(proc_root / "self/cgroup")
    if own is None:
        return []
    for line in own.splitlines():
        if line.startswith("0::"):
            leaf = cgroup_root / line[3:].lstrip("/")
            return [leaf, *[p for p in leaf.parents if p == cgroup_root or cgroup_root in p.parents]]
    return []


def cgroup_snapshot(paths, dedicated_leaf=False):
    return [{"path": str(path), "classification": "dedicated-observer-and-workload"
             if dedicated_leaf and index == 0 else "shared", **{name: read_optional(path / name) for name in
            ("memory.current", "memory.peak", "memory.max", "memory.high",
             "memory.swap.max", "memory.events", "memory.stat", "memory.pressure")}}
            for index, path in enumerate(paths)]


def process_sample(root_pid, elapsed, cgroups, proc_root=Path("/proc")):
    processes, scan_errors = {}, 0
    for entry in proc_root.iterdir():
        if entry.name.isdigit():
            try:
                row = parse_stat((entry / "stat").read_text())
                processes[row["pid"]] = row
            except (OSError, ValueError, IndexError):
                scan_errors += 1
    owned = select_processes(root_pid, processes)
    pss_errors = 0
    for row in owned:
        original_rss = row["rss_bytes"]
        smaps = read_optional(proc_root / str(row["pid"]) / "smaps_rollup")
        values = parse_kib_values(smaps) if smaps is not None else {}
        row["pss_bytes"] = values.get("Pss")
        row["rss_source"] = "smaps_rollup" if "Rss" in values else "stat-approximate"
        row["rss_bytes"] = values.get("Rss", row["rss_bytes"])
        # A PID can disappear or be reused while smaps is being read.
        current_stat = read_optional(proc_root / str(row["pid"]) / "stat")
        if current_stat is None or parse_stat(current_stat)["start_ticks"] != row["start_ticks"]:
            row["pss_bytes"] = None
            row["identity_stable"] = False
            row["rss_bytes"] = original_rss
            row["rss_source"] = "stat-before-identity-change-approximate"
        else:
            row["identity_stable"] = True
        if row["pss_bytes"] is None:
            pss_errors += 1
        # argv is useful to distinguish rustc/linker processes; credentials are never read.
        cmdline = read_optional(proc_root / str(row["pid"]) / "cmdline")
        row["argv"] = cmdline.split("\0")[:-1] if cmdline else []
    return {
        "elapsed_s": elapsed, "processes": owned,
        "tree_rss_bytes": sum(row["rss_bytes"] for row in owned),
        "tree_pss_bytes": None if pss_errors else sum(row["pss_bytes"] for row in owned),
        "pss_known_bytes": sum(row["pss_bytes"] or 0 for row in owned),
        "pss_missing_processes": pss_errors, "proc_scan_errors": scan_errors,
        "host_mem_available_bytes": parse_kib_values(read_optional(proc_root / "meminfo") or "").get("MemAvailable"),
        "shared_cgroup_current_bytes": [read_optional(p / "memory.current") for p in cgroups],
    }


def summarize_samples(samples):
    complete = [s["tree_pss_bytes"] for s in samples if s["tree_pss_bytes"] is not None]
    return {
        "sample_count": len(samples),
        "sampled_tree_peak_rss_bytes": max((s["tree_rss_bytes"] for s in samples), default=None),
        "sampled_tree_peak_pss_bytes": max(complete, default=None),
        "complete_pss_samples": len(complete),
        "pss_missing_process_observations": sum(s["pss_missing_processes"] for s in samples),
        "proc_scan_errors": sum(s["proc_scan_errors"] for s in samples),
        "process_count_peak": max((len(s["processes"]) for s in samples), default=0),
        "host_min_mem_available_bytes": min((s["host_mem_available_bytes"] for s in samples
                                               if s["host_mem_available_bytes"] is not None), default=None),
    }


def parse_gnu_time(text):
    fields = dict(line.strip().split(": ", 1) for line in text.splitlines() if ": " in line)
    elapsed = fields.get("Elapsed (wall clock) time (h:mm:ss or m:ss)")
    wall = None
    if elapsed is not None:
        wall = 0.0
        for component in elapsed.split(":"):
            wall = wall * 60 + float(component)
    def number(key, convert=float):
        return convert(fields[key]) if key in fields else None
    return {"wall_time_s": wall,
            "largest_child_max_rss_kib": number("Maximum resident set size (kbytes)", int),
            "user_cpu_s": number("User time (seconds)"),
            "system_cpu_s": number("System time (seconds)"),
            "exit_code": number("Exit status", int)}


def config_metadata(repo):
    candidates = [repo / "rust-toolchain", repo / "rust-toolchain.toml"]
    for base in [repo, *repo.parents]:
        cargo_dir = base / ".cargo"
        candidates.extend([cargo_dir / "config", cargo_dir / "config.toml"])
    cargo_home = Path(os.environ.get("CARGO_HOME", str(Path.home() / ".cargo")))
    candidates.extend([cargo_home / "config", cargo_home / "config.toml"])
    return [{"path": str(p), "sha256": digest(p)} for p in dict.fromkeys(candidates) if p.is_file()]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--label", required=True)
    parser.add_argument("--kind", choices=("build", "check", "test", "link", "custom"), required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--cache-state", required=True,
                        help="empty-target, dependency-warm-package-clean, no-op-incremental, or precise description")
    parser.add_argument("--target-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--lock", type=Path, default=Path("/tmp/sinomo-maintenance-build.lock"))
    parser.add_argument("--time-bin", type=Path, help="GNU time executable; otherwise wait4 rusage is retained")
    parser.add_argument("--interval", type=float, default=0.25)
    parser.add_argument("--dedicated-cgroup", action="store_true",
                        help="label the fresh self-cgroup leaf as dedicated to observer and workload; ancestors remain shared")
    parser.add_argument("--skip-tool-versions", action="store_true", help="for fake-command harness tests only")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command or args.interval <= 0:
        parser.error("a command and a positive sampling interval are required")
    repo, target, output = args.repo.resolve(), args.target_dir.resolve(), args.output.resolve()
    # Opening an existing lock preserves its inode and content, including when contended.
    with args.lock.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(f"build lock is held: {args.lock}", file=sys.stderr)
            return 75
        if output.exists():
            parser.error("output already exists; use a new evidence directory")
        if args.cache_state == "empty-target" and target.exists() and any(target.iterdir()):
            parser.error("empty-target requires a new or empty target directory; no target is cleaned")
        if args.time_bin and not args.time_bin.resolve().is_file():
            parser.error("GNU time executable is absent")
        output.mkdir(parents=True)
        # Cargo creates its own target directory and CACHEDIR.TAG. Precreating it
        # would prevent recent Cargo versions from marking it safe for cargo clean.
        cgroups = cgroup_paths()
        started = datetime.datetime.now(datetime.timezone.utc).isoformat()
        before = source_identity(repo)
        (output / "source.patch").write_bytes(subprocess.run(
            ["git", "diff", "--binary", "HEAD"], cwd=repo, stdout=subprocess.PIPE, check=True).stdout)
        metadata = {"label": args.label, "kind": args.kind, "profile_label": args.profile,
                    "cache_state": args.cache_state, "command": command, "cwd": str(repo),
                    "target_dir": str(target), "started_utc": started, "source_before": before,
                    "host": read_optional(Path("/proc/sys/kernel/hostname")),
                    "host_meminfo_before": read_optional(Path("/proc/meminfo")),
                    "host_memory_pressure_before": read_optional(Path("/proc/pressure/memory")),
                    "environment": {k: os.environ.get(k) for k in ENV_KEYS},
                    "configs": config_metadata(repo), "cpu_count": os.cpu_count(),
                    "observer_sha256": digest(Path(__file__).resolve()),
                    "lock_path": str(args.lock.resolve()), "workload_inherits_lock_fd": True,
                    "cancellation_timeouts_s": {"term_grace": CANCEL_TERM_GRACE_S,
                                                "kill_wait": CANCEL_KILL_WAIT_S},
                    "sampling_interval_s": args.interval, "dedicated_cgroup_requested": args.dedicated_cgroup,
                    "cgroups_before": cgroup_snapshot(cgroups, args.dedicated_cgroup),
                    "gnu_time": None if not args.time_bin else {"path": str(args.time_bin.resolve()),
                        "sha256": digest(args.time_bin.resolve()),
                        "version": capture([str(args.time_bin.resolve()), "--version"], repo)}}
        if not args.skip_tool_versions:
            metadata["tools"] = {name: {"resolved_path": shutil.which(name),
                **capture([name, *flags], repo)} for name, flags in
                (("cargo", ["--version"]), ("rustc", ["-vV"]))}
        (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
        shutil.copy2(Path(__file__).resolve(), output / "observer.py")
        env = os.environ.copy()
        env["CARGO_TARGET_DIR"] = str(target)
        env["LC_ALL"] = "C"  # Stable GNU time field labels; no compiler-setting changes.
        measured = ([str(args.time_bin.resolve()), "-v", "-o", str(output / "gnu-time.txt"), "--"]
                    if args.time_bin else []) + command
        return run_measurement(args, repo, target, output, cgroups, metadata, env, measured, lock)


def run_measurement(args, repo, target, output, cgroups, metadata, env, measured, lock):
    """Retain incomplete observations as evidence, without hiding workload status."""
    with workload_signal_handlers() as received:
        samples = []
        process, usage, cleanup = None, None, None
        errors = []
        begin = time.monotonic()
        try:
            with (output / "stdout.log").open("wb") as stdout, (output / "stderr.log").open("wb") as stderr, \
                    (output / "process-samples.jsonl").open("w") as sample_log:
                # flock is attached to this open-file description. The workload
                # keeps the slot if the observer unexpectedly dies; never issue
                # LOCK_UN, which would also unlock the inherited description.
                process = subprocess.Popen(measured, cwd=repo, env=env, stdout=stdout,
                                           stderr=stderr, start_new_session=True,
                                           pass_fds=(lock.fileno(),))
                metadata["workload_pid"] = process.pid
                while True:
                    if received:
                        raise WorkloadInterrupted(received[0])
                    sample = process_sample(process.pid, time.monotonic() - begin, cgroups)
                    if args.dedicated_cgroup:
                        sample["cgroup_current_bytes"] = sample.pop("shared_cgroup_current_bytes")
                    samples.append(sample)
                    sample_log.write(json.dumps(sample) + "\n")
                    sample_log.flush()
                    pid, status, usage = os.wait4(process.pid, os.WNOHANG)
                    if pid:
                        process.returncode = os.waitstatus_to_exitcode(status)
                        break
                    sample_pause(args.interval, received)
        except BaseException as error:
            errors.append({"stage": "workload-observation", "type": type(error).__name__,
                           "message": str(error)})
            if isinstance(error, KeyboardInterrupt) and not received:
                received.append(signal.SIGINT)
            if process is not None:
                try:
                    cleanup_usage, cleanup = cancel_workload(process)
                    if cleanup_usage is not None:
                        usage = cleanup_usage
                except BaseException as cleanup_error:
                    errors.append({"stage": "workload-cleanup", "type": type(cleanup_error).__name__,
                                   "message": str(cleanup_error)})
        elapsed = time.monotonic() - begin
        try:
            after = source_identity(repo)
        except Exception as error:
            after = None
            errors.append({"stage": "source-after", "type": type(error).__name__, "message": str(error)})
        timing_files = []
        timing_dir = target / "cargo-timings"
        try:
            if timing_dir.is_dir():
                for path in timing_dir.glob("*.html"):
                    if path.stat().st_mtime >= datetime.datetime.fromisoformat(metadata["started_utc"]).timestamp():
                        destination = output / "cargo-timings" / path.name
                        destination.parent.mkdir(exist_ok=True)
                        shutil.copy2(path, destination)
                        timing_files.append({"path": str(destination), "sha256": digest(destination)})
        except Exception as error:
            errors.append({"stage": "cargo-timings", "type": type(error).__name__, "message": str(error)})
        try:
            gnu_measurement = parse_gnu_time(read_optional(output / "gnu-time.txt") or "")
        except (ValueError, OverflowError) as error:
            gnu_measurement = parse_gnu_time("")
            errors.append({"stage": "gnu-time", "type": type(error).__name__, "message": str(error)})
        workload_exit = process.returncode if process is not None else None
        observer_exit = (128 + received[0] if received else 1 if errors or workload_exit is None
                         else workload_exit if workload_exit >= 0 else 128 - workload_exit)
        before = metadata["source_before"]
        summary = {**metadata, "exit_code": workload_exit, "observer_exit_code": observer_exit,
                   "measurement_complete": not errors and not received and workload_exit is not None,
                   "interruption_signal": received[0] if received else None,
                   "errors": errors, "cleanup": cleanup, "wall_time_s": elapsed,
                   "gnu_time_measurement": gnu_measurement,
                   "source_after": after, "tracked_source_stable": after is not None and all(before[k] == after[k] for k in
                       ("head", "tracked_patch_sha256", "lockfile_sha256", "root_manifest_sha256")),
                   "measurement": summarize_samples(samples),
                   "wait4": None if usage is None else {"largest_child_max_rss_kib": usage.ru_maxrss,
                             "user_cpu_s": usage.ru_utime, "system_cpu_s": usage.ru_stime},
                   "cgroups_after": cgroup_snapshot(cgroups, args.dedicated_cgroup), "cargo_timings": timing_files,
                   "host_meminfo_after": read_optional(Path("/proc/meminfo")),
                   "limitations": ["Sampled peaks can miss short-lived processes or between-sample peaks.",
                       "RSS sums double-count shared pages; PSS completeness is reported per sample.",
                       ("Dedicated leaf includes observer, workload, charged page cache and kernel memory; ancestors remain shared."
                        if args.dedicated_cgroup else
                        "Cgroup counters include unrelated processes and historical usage; no counters were reset."),
                       "GNU time/wait4 RSS is a largest-child high-water mark, not simultaneous aggregate memory.",
                       "Cargo unit timings include codegen/link activity; isolated linker duration is unavailable.",
                       "OS page cache and Cargo registry/dependency cache are not reset."]}
        summary["limitations"].extend([
            "Tracked source digest excludes untracked build inputs; inspect the recorded git status.",
            "Descendants creating a new session and reparenting can escape process-tree sampling.",
            "Observer process memory is outside the workload tree; observed wall time is not overhead-adjusted.",
            "Cancellation targets the workload's original process group; descendants changing groups can escape.",
            "Inherited flock survives observer death only while workloads retain its FD; SIGKILL cannot write a final summary."])
        if args.dedicated_cgroup:
            summary["limitations"].append(
                "Dedicated classification is the caller's assertion of a fresh scope; no cgroup counters or limits were changed.")
            leaf = summary["cgroups_after"][0] if cgroups else None
            summary["dedicated_cgroup_measurement"] = None if leaf is None else {
                "path": leaf["path"],
                "peak_bytes": int(leaf["memory.peak"]) if (leaf["memory.peak"] or "").isdigit() else None,
                "current_bytes_after": int(leaf["memory.current"]) if (leaf["memory.current"] or "").isdigit() else None,
                "includes_observer_page_cache_and_kernel": True,
            }
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps({"output": str(output), "exit_code": workload_exit,
                          "observer_exit_code": observer_exit, "measurement_complete": summary["measurement_complete"],
                          "wall_time_s": elapsed, **summary["measurement"]}, indent=2))
        return observer_exit


if __name__ == "__main__":
    sys.exit(main())
