#!/usr/bin/env python3
"""Bind an explicit change-scope review to the actual working diff.

The check enforces accounting and freshness, not semantic correctness or user
approval. It never changes the index, creates commits, or applies source edits.
Snapshots/reviews belong in ignored task artifacts or outside the checkout.
"""

from __future__ import annotations

import argparse
import base64
import difflib
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import sys


def git(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args])


def names(repo, *args):
    return set(os.fsdecode(p) for p in git(repo, *args).split(b"\0") if p)


def digest(value):
    return hashlib.sha256(value).hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True).encode()


def in_scope(path, scope):
    return any(root == "." or path == root or path.startswith(root + "/") for root in scope)


def normalize_scope(value):
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or ".git" in path.parts:
        raise ValueError(f"scope must be a repository-relative path: {value}")
    return str(path)


def record(body, mode, keep_body=True):
    result = {"sha256": digest(body), "mode": mode}
    if keep_body:
        result["body"] = base64.b64encode(body).decode()
    return result


def working_file(repo, path, keep_body=True):
    source = repo / path
    try:
        before = source.lstat()
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(before.st_mode):
        body, mode = os.fsencode(os.readlink(source)), "120000"
    elif stat.S_ISREG(before.st_mode):
        body = source.read_bytes()
        mode = "100755" if before.st_mode & 0o111 else "100644"
    else:
        raise ValueError(f"unsupported source kind (use its own review scope): {path}")
    after = source.lstat()
    if (before.st_ino, before.st_size, before.st_mtime_ns, before.st_mode) != (
        after.st_ino, after.st_size, after.st_mtime_ns, after.st_mode
    ):
        raise ValueError(f"source changed while reading: {path}; retry after its writer settles")
    return record(body, mode, keep_body)


def identity(value):
    return None if value is None else {key: value[key] for key in ("sha256", "mode")}


def base_file(repo, snapshot, path):
    if path in snapshot["overrides"]:
        return snapshot["overrides"][path]
    if path not in snapshot["tracked"]:
        return None
    entry = git(repo, "ls-tree", "-z", snapshot["head"], "--", path)
    if not entry:
        return None
    header, _ = entry.rstrip(b"\0").split(b"\t", 1)
    mode, kind, oid = header.split()
    if kind != b"blob":
        raise ValueError(f"submodule requires its own snapshot: {path}")
    return record(git(repo, "cat-file", "blob", oid.decode()), mode.decode())


def capture(repo, scope, requirements):
    head = git(repo, "rev-parse", "HEAD").decode().strip()
    tracked = names(repo, "ls-tree", "-rz", "--name-only", head)
    dirty = names(repo, "diff", "--name-only", "-z", head, "--")
    untracked = names(repo, "ls-files", "--others", "--exclude-standard", "-z")
    # Preserve exact working bytes in planned files, even when Git considers
    # them clean after line-ending normalization or a clean filter.
    captured = dirty | {path for path in tracked if in_scope(path, scope)}
    overrides = {path: working_file(repo, path) for path in sorted(captured)}
    for path in sorted(untracked):
        # Existing unrelated untracked files are recognized without archiving
        # their contents. A later external edit stays explicitly unattributed.
        overrides[path] = working_file(repo, path, in_scope(path, scope))
    if git(repo, "rev-parse", "HEAD").decode().strip() != head:
        raise ValueError("HEAD changed during capture")
    return {
        "version": 1, "repo": str(repo), "head": head, "scope": scope,
        "requirements": requirements, "tracked": sorted(tracked), "overrides": overrides,
    }


def text_body(value):
    if value is None:
        return ""
    if "body" not in value:
        return None
    body = base64.b64decode(value["body"], validate=True)
    if b"\0" in body:
        return None
    try:
        return body.decode("utf-8")
    except UnicodeDecodeError:
        return None


def changes(repo, snapshot):
    candidates = set(snapshot["overrides"])
    candidates |= names(repo, "diff", "--name-only", "-z", snapshot["head"], "--")
    candidates |= names(repo, "ls-files", "--others", "--exclude-standard", "-z")
    result = []
    for path in sorted(candidates):
        old, new = base_file(repo, snapshot, path), working_file(repo, path)
        if identity(old) == identity(new):
            continue
        context = {
            "path": path, "planned": in_scope(path, snapshot["scope"]),
            "before": identity(old), "after": identity(new),
            "baseline_content_available": old is None or "body" in old,
        }
        old_text, new_text = text_body(old), text_body(new)
        units = []
        if old_text is not None and new_text is not None:
            a, b = old_text.splitlines(keepends=True), new_text.splitlines(keepends=True)
            for op, i, j, k, l in difflib.SequenceMatcher(None, a, b, autojunk=False).get_opcodes():
                if op != "equal":
                    units.append({"kind": op, "old_line": i + 1, "new_line": k + 1,
                                  "old": "".join(a[i:j]), "new": "".join(b[k:l])})
        if not units:
            units = [{"kind": "content-or-mode", "old_line": None, "new_line": None,
                      "old": old_text, "new": new_text}]
        for unit in units:
            detail = {**context, **unit}
            result.append({"id": digest(encoded(detail)), **detail})
    return result


def draft(snapshot_bytes, units):
    return {"snapshot_sha256": digest(snapshot_bytes), "changes": [
        {**unit, "basis": None, "requirement": None, "reason": "",
         "intent_effect": None, "decision": "", "scope_reason": ""}
        for unit in units
    ]}


def nonempty(value):
    return isinstance(value, str) and bool(value.strip())


def verify(snapshot_bytes, snapshot, units, review):
    errors = []
    if review.get("snapshot_sha256") != digest(snapshot_bytes):
        errors.append("review belongs to a different starting snapshot")
    entries = review.get("changes", [])
    if not isinstance(entries, list) or any(not isinstance(entry, dict) for entry in entries):
        return errors + ["changes must be a list of review entries"]
    reviewed = {entry.get("id"): entry for entry in entries}
    if len(reviewed) != len(entries):
        errors.append("duplicate review entries")
    actual = {unit["id"]: unit for unit in units}
    if reviewed.keys() != actual.keys():
        errors.append(f"unaccounted changes: {len(actual.keys() - reviewed.keys())}; "
                      f"stale entries: {len(reviewed.keys() - actual.keys())}")
    for key in actual.keys() & reviewed.keys():
        unit, entry = actual[key], reviewed[key]
        label = f"{unit['path']}:{unit['new_line'] or '-'}"
        if any(entry.get(field) != value for field, value in unit.items()):
            errors.append(f"{label}: recorded diff does not match source")
        basis = entry.get("basis")
        if basis not in {"requested", "necessary", "additional", "external"}:
            errors.append(f"{label}: classify requested/necessary/additional/external")
        if not nonempty(entry.get("reason")):
            errors.append(f"{label}: explain the reason, not just the filename")
        if basis in {"requested", "necessary"} and entry.get("requirement") not in snapshot["requirements"]:
            errors.append(f"{label}: bind the edit to a captured requirement")
        if not unit["planned"] and not nonempty(entry.get("scope_reason")):
            errors.append(f"{label}: explain work outside the planned paths or its external owner")
        if not unit["baseline_content_available"] and basis != "external":
            errors.append(f"{label}: no baseline contents for this pre-existing untracked file")
        effect = entry.get("intent_effect")
        if effect not in {"preserves", "restores", "changes", "external"}:
            errors.append(f"{label}: classify the effect on the contract")
        if (basis == "external") != (effect == "external"):
            errors.append(f"{label}: external ownership and effect must agree")
        if (basis == "additional" or effect in {"restores", "changes"}) and not nonempty(entry.get("decision")):
            errors.append(f"{label}: state the extra improvement or before/after contract decision")
    return errors


def write_artifact(repo, path, text):
    path = path.resolve()
    if path.is_relative_to(repo):
        relative = path.relative_to(repo)
        if ".git" in relative.parts or subprocess.run(
            ["git", "-C", str(repo), "check-ignore", "-q", "--", str(relative)]
        ).returncode != 0:
            raise ValueError("review artifacts must be ignored or outside the checkout")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x") as stream:
        stream.write(text)


def write_new(repo, path, value):
    write_artifact(repo, path, json.dumps(value, indent=2, ensure_ascii=True) + "\n")


def render(snapshot, units, review, errors):
    entries = review.get("changes", [])
    by_id = {entry.get("id"): entry for entry in entries if isinstance(entry, dict)} if isinstance(entries, list) else {}
    lines = ["# Change scope review", "", "Accounting: " + ("INCOMPLETE" if errors else "CURRENT"), "",
             "This report discloses changes; it does not certify semantic correctness or authorization.", "",
             "## Captured requirements", ""]
    for key, value in snapshot["requirements"].items():
        lines.append(f"- {key}: {value}")
    if errors:
        lines += ["", "## Accounting failures", ""] + [f"- {error}" for error in errors]
    for basis in ("additional", "requested", "necessary", "external", None):
        def category(unit):
            value = by_id.get(unit["id"], {}).get("basis")
            return value if value in ("requested", "necessary", "additional", "external") else None
        selected = [unit for unit in units if category(unit) == basis]
        if not selected:
            continue
        lines += ["", f"## {basis or 'Unclassified'} changes", ""]
        for unit in selected:
            entry = by_id.get(unit["id"], {})
            lines += [f"### {unit['path']}:{unit['new_line'] or '-'}", "",
                      f"Effect: {entry.get('intent_effect') or 'unclassified'}; "
                      f"planned path: {unit['planned']}; requirement: {entry.get('requirement') or 'none'}.", "",
                      str(entry.get("reason") or "Reason missing."), ""]
            for label, field in (("Decision", "decision"), ("Scope", "scope_reason")):
                if entry.get(field):
                    lines += [f"{label}: {entry[field]}", ""]
            # Render actual source, never the review file's copy of that source.
            body = "".join("-" + line for line in (unit["old"] or "").splitlines(keepends=True))
            body += "".join("+" + line for line in (unit["new"] or "").splitlines(keepends=True))
            fence = "`" * max(3, max((len(part) for part in re.findall(r"`+", body)), default=0) + 1)
            lines += [f"Before: {unit['before']}; after: {unit['after']}", "", fence + "diff", body.rstrip("\n"), fence, ""]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    commands = parser.add_subparsers(dest="command", required=True)
    start = commands.add_parser("capture", help="capture requirements and existing work before editing")
    start.add_argument("--path", action="append", required=True)
    start.add_argument("--requirement", action="append", required=True, metavar="ID=TEXT")
    start.add_argument("--output", type=Path, required=True)
    prepare = commands.add_parser("prepare", help="generate a fresh review of actual changes")
    prepare.add_argument("--snapshot", type=Path, required=True)
    prepare.add_argument("--output", type=Path, required=True)
    check = commands.add_parser("check", help="fail on unaccounted, stale or undeclared changes")
    check.add_argument("--snapshot", type=Path, required=True)
    check.add_argument("--review", type=Path, required=True)
    check.add_argument("--report", type=Path, help="write a fresh readable report with actual changed blocks")
    args = parser.parse_args()
    try:
        repo = Path(git(args.repo, "rev-parse", "--show-toplevel").decode().strip()).resolve()
        if args.command == "capture":
            requirements = {}
            for item in args.requirement:
                key, separator, value = item.partition("=")
                if not separator or not key.strip() or not value.strip() or key in requirements:
                    raise ValueError("requirements need unique nonempty ID=TEXT entries")
                requirements[key] = value
            snapshot = capture(repo, [normalize_scope(p) for p in args.path], requirements)
            write_new(repo, args.output, snapshot)
            print(f"captured {len(requirements)} requirement(s); existing dirty work retained in baseline")
            return 0
        raw = args.snapshot.read_bytes()
        snapshot = json.loads(raw)
        if snapshot.get("version") != 1 or snapshot.get("repo") != str(repo):
            raise ValueError("snapshot does not belong to this checkout/version")
        units = changes(repo, snapshot)
        if args.command == "prepare":
            write_new(repo, args.output, draft(raw, units))
            print(f"prepared {len(units)} change block(s); review every block and classify it")
            return 0
        review = json.loads(args.review.read_bytes())
        if not isinstance(review, dict):
            raise ValueError("review must be an object")
        errors = verify(raw, snapshot, units, review)
        if args.report:
            write_artifact(repo, args.report, render(snapshot, units, review, errors))
        entries = review.get("changes", [])
        for entry in entries if isinstance(entries, list) else []:
            if not isinstance(entry, dict):
                continue
            print(f"{entry.get('basis') or 'UNREVIEWED'} | {entry.get('intent_effect') or '?'} | "
                  f"{'planned' if entry.get('planned') else 'OUTSIDE PLAN'} | "
                  f"{entry.get('path')}:{entry.get('new_line') or '-'} | {entry.get('reason', '')}")
            if entry.get("decision"):
                print(f"  Decision: {entry['decision']}")
        if errors:
            print("\n".join(errors), file=sys.stderr)
            return 1
        print(f"accounted: {len(units)} current change block(s). This is scope disclosure, not semantic approval.")
        return 0
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
