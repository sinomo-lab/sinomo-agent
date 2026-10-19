#!/usr/bin/env python3
"""Bound one native test invocation's temporary state and preserve failures.

Run inside the existing build/resource guard. This owns only its newly created
private TMPDIR, never a shared sinomo-tests directory or an older run. Successful
runs are reclaimed after their processes drain. Failed runs are archived and
compared against the originals before reclamation. An archival/use-check error
retains the originals and fails the wrapper. No age-based sweep is performed.

For separate resource measurements, --defer-cleanup checkpoints a completed
invocation without reclaiming it. Run --finalize ARTIFACTS under the same ordinary
guard in a subsequent scope. Its exit status describes cleanup only; the original
test status stays in pending-cleanup.json and result.json. Do not abandon pending
roots or count successful finalization as a test pass.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time


def write_json(path, value):
    with path.open("x") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def storage(path):
    value = os.statvfs(path)
    return {"free_bytes": value.f_bavail * value.f_frsize,
            "free_inodes": value.f_favail, "reports_inodes": bool(value.f_files)}


def require_storage(path):
    value = storage(path)
    if value["free_bytes"] < 5 * 1024**3 or (
            value["reports_inodes"] and value["free_inodes"] < 65536):
        raise RuntimeError(f"temporary/storage reserve unavailable at {path}: {value}")
    return value


def require_durable_artifacts(path):
    filesystem = subprocess.check_output(["findmnt", "-n", "-o", "FSTYPE", "-T", str(path)], text=True).strip()
    if filesystem in {"tmpfs", "ramfs", "devtmpfs"} or path.stat().st_uid != os.getuid():
        raise RuntimeError(f"failure artifacts require a user-owned durable filesystem: {path} ({filesystem})")
    return filesystem


def active_uses(root, process_group=None):
    """Check the owned child group and visible references; never read environments."""
    uses = []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit() or int(proc.name) == os.getpid():
            continue
        try:
            if process_group is not None:
                fields = (proc / "stat").read_text().rsplit(")", 1)[1].split()
                if int(fields[2]) == process_group and fields[0] != "Z":
                    uses.append({"pid": int(proc.name), "kind": "owned-process-group"})
            links = [proc / "cwd", proc / "exe"]
            try:
                links.extend((proc / "fd").iterdir())
            except (PermissionError, FileNotFoundError):
                pass
            for link in links:
                try:
                    target = os.readlink(link).removesuffix(" (deleted)")
                except OSError:
                    continue
                if target == str(root) or target.startswith(str(root) + "/"):
                    uses.append({"pid": int(proc.name), "kind": link.name, "path": target})
            try:
                for line in (proc / "maps").read_text().splitlines():
                    fields = line.split(maxsplit=5)
                    if len(fields) == 6:
                        target = fields[5].removesuffix(" (deleted)")
                        if target == str(root) or target.startswith(str(root) + "/"):
                            uses.append({"pid": int(proc.name), "kind": "mapping", "path": target})
            except (PermissionError, FileNotFoundError, ProcessLookupError):
                pass
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue
    # A completed launcher may leave a container referring to its temporary
    # bind source. Preserve it even if the container is currently stopped.
    # Request mount metadata only; never inspect container environments.
    if shutil.which("docker"):
        ids = subprocess.check_output(["docker", "ps", "-aq"], text=True).split()
        for offset in range(0, len(ids), 100):
            batch = ids[offset:offset + 100]
            output = subprocess.check_output(["docker", "inspect", "--format", "{{json .Mounts}}", *batch], text=True)
            for identifier, line in zip(batch, output.splitlines(), strict=True):
                for mount in json.loads(line):
                    source = mount.get("Source", "")
                    if source and (source == str(root) or source.startswith(str(root) + "/")
                                   or str(root).startswith(source.rstrip("/") + "/")):
                        uses.append({"container": identifier, "kind": "mount", "path": source})
    return uses


def archive_verified(root, artifacts):
    archive = artifacts / "failed-fixtures.tar"
    command = ["tar", "--create", "--file", str(archive), "--acls", "--xattrs",
               "--numeric-owner", "--directory", str(root.parent), "--", root.name]
    with (artifacts / "archive.log").open("x") as log:
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
        with archive.open("rb") as stream:
            os.fsync(stream.fileno())
        compare = ["tar", "--compare", "--file", str(archive), "--acls", "--xattrs",
                   "--directory", str(root.parent)]
        subprocess.run(compare, stdout=log, stderr=subprocess.STDOUT, check=True)
    require_storage(artifacts)
    with archive.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    receipt = {"original_root": str(root), "archive": str(archive), "sha256": digest,
               "bytes": archive.stat().st_size, "create_command": command,
               "compare_command": compare, "compare_exit_code": 0,
               "recovery": "Extract the archive into a new empty directory; its top-level name is the original private TMPDIR name."}
    write_json(artifacts / "archive.json", receipt)
    fd = os.open(artifacts, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return receipt


def finalize(artifacts):
    """Finalize only the exact private root checkpointed by this launcher.

    The explicit split lets execution and evidence processing run in separate
    capped scopes. An interrupted or rejected finalization retains the root.
    The caller must hold the ordinary shared build/test guard in both phases.
    """
    if (artifacts / "result.json").exists():
        raise RuntimeError("finalization already recorded; preserve the receipt and remaining state")
    pending = json.loads((artifacts / "pending-cleanup.json").read_text())
    root, temp_base = Path(pending["temporary_root"]), Path(pending["temp_base"])
    code = pending["command_exit_code"]
    result = {"command_exit_code": code, "elapsed_seconds": pending["elapsed_seconds"],
              "temporary_root": str(root), "reclaimed": False}
    try:
        require_durable_artifacts(artifacts)
        invocation = json.loads((artifacts / "invocation.json").read_text())
        current = root.lstat()
        identity = (pending["device"], pending["inode"])
        if (invocation["temporary_root"] != str(root) or root.parent != temp_base
                or not root.name.startswith("sinomo-run-") or root.is_symlink()
                or not root.is_dir() or root.resolve() != root
                or current.st_uid != os.getuid()
                or (current.st_dev, current.st_ino) != identity):
            raise RuntimeError("fixture ownership changed before finalization")
        uses = active_uses(root, pending["process_group"])
        if uses:
            raise RuntimeError(f"owned fixture still in use: {uses}")
        if code:
            require_storage(artifacts)
            result["archive"] = archive_verified(root, artifacts)
        uses = active_uses(root, pending["process_group"])
        current = root.stat()
        if uses or (current.st_dev, current.st_ino) != identity:
            raise RuntimeError(f"fixture ownership changed or still active: {uses}")
        shutil.rmtree(root)
        result["reclaimed"] = True
    except Exception as error:
        result["preservation_error"] = str(error)
    result["after"] = {"temporary": storage(temp_base), "artifacts": storage(artifacts)}
    write_json(artifacts / "result.json", result)
    print(json.dumps(result), flush=True)
    return result


def run(command, artifacts, temp_base, retention="all", defer_cleanup=False):
    artifacts = artifacts.resolve()
    artifacts.mkdir(parents=True, exist_ok=False)
    filesystem = require_durable_artifacts(artifacts)
    before = {"temporary": require_storage(temp_base), "artifacts": require_storage(artifacts)}
    root = Path(tempfile.mkdtemp(prefix="sinomo-run-", dir=temp_base)).resolve()
    identity = root.stat()
    # Retain all fixtures until the command outcome is known, including fixtures
    # dropped before a later assertion fails. The testdir crate's own cleanup
    # tests explicitly select `failures` to exercise its normal Drop semantics.
    env = dict(os.environ, TMPDIR=str(root), SINOMO_TEST_KEEP_TMP="1" if retention == "all" else "0")
    write_json(artifacts / "invocation.json", {"command": command, "cwd": str(Path.cwd()),
               "temporary_root": str(root), "retention": retention, "artifact_filesystem": filesystem, "before": before})
    start = time.monotonic()
    with (artifacts / "output.log").open("x") as log:
        child = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT,
                                 start_new_session=True)
        code = child.wait()
    write_json(artifacts / "pending-cleanup.json", {
        "command_exit_code": code, "elapsed_seconds": time.monotonic() - start,
        "temporary_root": str(root), "temp_base": str(root.parent),
        "device": identity.st_dev, "inode": identity.st_ino, "process_group": child.pid})
    fd = os.open(artifacts, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    if defer_cleanup:
        print(json.dumps({"finalization_required": str(artifacts), "command_exit_code": code}), flush=True)
        return code
    result = finalize(artifacts)
    return (code or 1) if not result["reclaimed"] else code


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", type=Path)
    parser.add_argument("--defer-cleanup", action="store_true",
                        help="checkpoint the owned root; require a subsequent guarded --finalize")
    parser.add_argument("--finalize", type=Path,
                        help="archive/reclaim a checkpointed root in a separate scope; exit status describes cleanup only")
    parser.add_argument("--temp-base", type=Path, default=Path(tempfile.gettempdir()))
    parser.add_argument("--retention", choices=["all", "failures"], default="all")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.finalize:
        if args.command or args.artifacts or args.defer_cleanup:
            parser.error("--finalize cannot start another invocation")
        raise SystemExit(0 if finalize(args.finalize.resolve())["reclaimed"] else 1)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command or args.artifacts is None:
        parser.error("a command and --artifacts are required")
    raise SystemExit(run(command, args.artifacts, args.temp_base, args.retention, args.defer_cleanup))
