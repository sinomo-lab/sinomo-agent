#!/usr/bin/env python3
"""Validate repository-local Markdown links and heading anchors.

The checker has no third-party dependencies. By default it scans the public
documentation entry points, docs/, first-party crate/module READMEs, and Docker
guides. Paths passed on the command line replace the default roots.
"""

from __future__ import annotations

import re
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path
from urllib.parse import unquote


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ROOTS = (
    REPO_ROOT / "README.md",
    REPO_ROOT / "CONTRIBUTING.md",
    REPO_ROOT / "docs",
    REPO_ROOT / "crates",
    REPO_ROOT / "docker",
    REPO_ROOT / "modules",
)
EXCLUDED_PARTS = {
    ".git",
    ".codegraph",
    "target",
    "dist",
    "benchmark-results",
    "_apalache-out",
    "vendor",
}
EXCLUDED_PREFIXES = ("work-", "out-")
INLINE_LINK = re.compile(r"(?<!!)\[[^\]]*\]\(([^)]+)\)")
REFERENCE_LINK = re.compile(r"^\s*\[[^\]]+\]:\s*(\S+)")
HEADING = re.compile(r"^#{1,6}\s+(.+?)\s*#*\s*$")
HTML_ANCHOR = re.compile(r"<(?:a|span)\s+(?:name|id)=[\"']([^\"']+)[\"']")
EXTERNAL_SCHEMES = ("http://", "https://", "mailto:", "ftp://", "data:")


def markdown_files(roots: tuple[Path, ...]) -> list[Path]:
    files: set[Path] = set()
    for root in roots:
        path = root if root.is_absolute() else (REPO_ROOT / root)
        if path.is_file() and path.suffix.lower() == ".md":
            files.add(path.resolve())
            continue
        if not path.exists():
            raise ValueError(f"scan root does not exist: {path}")
        for candidate in path.rglob("*.md"):
            relative_parts = candidate.resolve().relative_to(REPO_ROOT).parts
            generated = any(
                part in EXCLUDED_PARTS or part.startswith(EXCLUDED_PREFIXES)
                for part in relative_parts
            )
            if not generated:
                files.add(candidate.resolve())
    return sorted(files)


def github_slug(text: str) -> str:
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"[`*_~]", "", text).strip().lower()
    normalized = unicodedata.normalize("NFKC", text)
    characters = []
    for character in normalized:
        if character in {"-", "_", " "} or character.isalnum():
            characters.append(character)
    return re.sub(r"\s+", "-", "".join(characters))


def document_anchors(path: Path) -> set[str]:
    counts: defaultdict[str, int] = defaultdict(int)
    anchors: set[str] = set()
    in_fence = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        heading = HEADING.match(line)
        if heading:
            base = github_slug(heading.group(1))
            occurrence = counts[base]
            counts[base] += 1
            anchors.add(base if occurrence == 0 else f"{base}-{occurrence}")
        for anchor in HTML_ANCHOR.findall(line):
            anchors.add(anchor)
    return anchors


def link_targets(path: Path) -> list[tuple[int, str]]:
    targets: list[tuple[int, str]] = []
    in_fence = False
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        if line.lstrip().startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        targets.extend((line_number, target) for target in INLINE_LINK.findall(line))
        reference = REFERENCE_LINK.match(line)
        if reference:
            targets.append((line_number, reference.group(1)))
    return targets


def strip_optional_title(raw_target: str) -> str:
    target = raw_target.strip()
    if target.startswith("<") and ">" in target:
        return target[1 : target.index(">")]
    # Local repository paths in this tree do not contain unescaped spaces. A
    # later whitespace-delimited value is therefore a Markdown link title.
    return target.split(maxsplit=1)[0]


def validate(files: list[Path]) -> list[str]:
    failures: list[str] = []
    anchor_cache: dict[Path, set[str]] = {}

    for source in files:
        for line_number, raw_target in link_targets(source):
            target = strip_optional_title(raw_target)
            if not target or target.lower().startswith(EXTERNAL_SCHEMES):
                continue

            path_text, separator, fragment = target.partition("#")
            path_text = unquote(path_text)
            fragment = unquote(fragment)
            destination = source if not path_text else (source.parent / path_text).resolve()

            try:
                destination.relative_to(REPO_ROOT)
            except ValueError:
                failures.append(
                    f"{source.relative_to(REPO_ROOT)}:{line_number}: "
                    f"link escapes repository: {raw_target}"
                )
                continue

            if not destination.exists():
                failures.append(
                    f"{source.relative_to(REPO_ROOT)}:{line_number}: "
                    f"missing target: {raw_target}"
                )
                continue

            if separator and fragment and destination.suffix.lower() == ".md":
                anchors = anchor_cache.setdefault(
                    destination, document_anchors(destination)
                )
                if fragment not in anchors:
                    failures.append(
                        f"{source.relative_to(REPO_ROOT)}:{line_number}: "
                        f"missing anchor #{fragment} in "
                        f"{destination.relative_to(REPO_ROOT)}"
                    )

    return failures


def main() -> int:
    roots = tuple(Path(argument) for argument in sys.argv[1:]) or DEFAULT_ROOTS
    try:
        files = markdown_files(roots)
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    failures = validate(files)
    if failures:
        print("\n".join(failures), file=sys.stderr)
        print(
            f"failed: {len(failures)} invalid local link(s) in "
            f"{len(files)} Markdown file(s)",
            file=sys.stderr,
        )
        return 1

    print(f"ok: {len(files)} Markdown files; local links and anchors resolve")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
