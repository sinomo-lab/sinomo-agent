#!/usr/bin/env python3
"""Reject recognizable development-log markers and provenance in documentation.

This is a review aid, not a semantic proof that no development log remains.
Generic Git commands, transactional commits and application session concepts are
valid; concrete history citations are not. Bibliographic dates and protocol
versions remain meaningful subject matter. No third-party dependencies are used.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

from check_links import DEFAULT_ROOTS, REPO_ROOT, markdown_files


ROOTS = DEFAULT_ROOTS + (
    REPO_ROOT / "AGENTS.md",
    REPO_ROOT / ".agents/skills",
    REPO_ROOT / "skills",
)
MONTH = (
    r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
)
YEAR = r"(?:19|20)\d{2}"
DAY = r"(?:0?[1-9]|[12]\d|3[01])"
HEX = r"[0-9a-f]{7,40}"
UUID = r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}"
CALENDAR_RULES = (
    ("date", re.compile(
        rf"(?<![\w])(?:{YEAR}[-/](?:0?[1-9]|1[0-2])(?:[-/]{DAY})?"
        rf"|{DAY}[-/](?:0?[1-9]|1[0-2])[-/]{YEAR}"
        rf"|{YEAR}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01]))(?![\w])"
    )),
    ("date", re.compile(
        rf"\b(?:{MONTH}\.?\s+(?:{YEAR}|{DAY}(?:st|nd|rd|th)?\b)"
        rf"|{DAY}(?:st|nd|rd|th)?\s+{MONTH}\b)", re.IGNORECASE
    )),
)
PROVENANCE_LABEL = re.compile(
    r"\b(?:last reviewed|research date|reviewed|updated|completed|landed|added|"
    r"retired|tested|validated|implemented|finished|done|status|as of)\b", re.IGNORECASE
)
RULES = (
    ("development-log", re.compile(
        rf"\b{MONTH}(?:\s+{YEAR})?\s+(?:update|patch|change|refactor)\s+"
        r"(?:changed|added|removed|replaced|fixed|introduced)\b", re.IGNORECASE
    )),
    ("development-log", re.compile(
        r"\b(?:the|this|our)\s+(?:(?:latest|recent|previous|new|last)\s+)?"
        r"(?:patch|update|refactor|fix)\s+(?:added|changed|removed|replaced|introduced|fixed)\b",
        re.IGNORECASE
    )),
    ("commit-reference", re.compile(
        rf"\b(?:commit|revision|merge|checkout|cherry-pick|fixed in|tested at|verified at|checked against)\s+(?:`)?{HEX}(?![\w])", re.IGNORECASE
    )),
    ("commit-reference", re.compile(rf"^\s*[-*]\s+`{HEX}`\s*[—:-]", re.IGNORECASE | re.MULTILINE)),
    ("commit-reference", re.compile(
        rf"\bgit(?:\s+-C\s+\S+)?\s+(?:show|checkout|reset|revert|cherry-pick)\s+(?:--\S+\s+)*{HEX}(?![\w])", re.IGNORECASE
    )),
    ("commit-reference", re.compile(rf"/(?:commit|commits|tree|blob|resolve)/{HEX}(?![\w])", re.IGNORECASE)),
    ("agent-history", re.compile(
        rf"(?:\.codex[/\\](?:sessions|memories)|rollout_summaries[/\\]|"
        rf"\b(?:thread_id|rollout_path|session_meta)\s*[:=]|\bterm_{UUID}\b|"
        rf"\b(?:session|conversation|thread)\s+`?{UUID}\b)", re.IGNORECASE
    )),
    ("agent-history", re.compile(
        r"\bas (?:discussed|established|agreed|requested) (?:in|during) "
        r"(?:the |a |our )?(?:(?:previous|earlier|prior|agent|Codex) )+"
        r"(?:conversation|session|chat)\b", re.IGNORECASE
    )),
)


def violations(text: str) -> list[tuple[int, str, str]]:
    findings = set()
    # Date syntax alone is not a violation: papers and protocols use it too.
    # Check dates only in recognizable development-status statements.
    for line_number, line in enumerate(text.splitlines(), 1):
        label = PROVENANCE_LABEL.search(line)
        if label:
            for _, pattern in CALENDAR_RULES:
                for match in pattern.finditer(line, label.end()):
                    findings.add((line_number, "development-date", match.group()))
    for rule, pattern in RULES:
        for match in pattern.finditer(text):
            line = text.count("\n", 0, match.start()) + 1
            findings.add((line, rule, match.group().replace("\n", " ")))
    return sorted(findings)


def validate(files: list[Path]) -> list[str]:
    failures = []
    for path in files:
        relative = path.relative_to(REPO_ROOT)
        for _, pattern in CALENDAR_RULES:
            for match in pattern.finditer(str(relative)):
                failures.append(f"{relative}: filename: development-date: {match.group()}")
        for line, rule, value in violations(path.read_text(encoding="utf-8")):
            failures.append(f"{relative}:{line}: {rule}: {value}")
    return failures


def main() -> int:
    roots = tuple(Path(argument) for argument in sys.argv[1:]) or ROOTS
    try:
        files = markdown_files(roots)
        failures = validate(files)
    except (ValueError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    if failures:
        print("\n".join(failures), file=sys.stderr)
        print(f"failed: {len(failures)} documentation content violation(s)", file=sys.stderr)
        return 1
    print(f"ok: {len(files)} Markdown files; no recognized development-log markers")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
