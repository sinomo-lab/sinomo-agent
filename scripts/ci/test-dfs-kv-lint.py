#!/usr/bin/env python3
"""Compile positive/negative Dylint fixtures; unrelated failures never count as passes."""
import argparse
import json
import os
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    args.artifacts.mkdir(parents=True, exist_ok=True)
    cases = {
        "direct": False, "helper": False, "closure": False,
        "trait_helper": False, "function_pointer": False, "async_helper": False,
        "lifecycle_escape": False, "empty_reason": False, "route_escape": False,
        "approved": True, "cached": True,
    }
    results = []
    for case, expected_success in cases.items():
        command = ["cargo", "dylint", "--no-metadata", "--lib-path", str(args.library.resolve()),
                   "--manifest-path", str(root / "lints/sinomo-architecture/ui/Cargo.toml"),
                   "--", "--bin", case]
        result = subprocess.run(command, cwd=root, env=os.environ, text=True, capture_output=True)
        output = result.stdout + result.stderr
        (args.artifacts / f"{case}.log").write_text(output)
        passed = result.returncode == 0 if expected_success else (
            result.returncode != 0 and "DFS operation" in output and "dfs_implicit_kv" in output
        )
        results.append({"case": case, "expected_success": expected_success,
                        "exit_code": result.returncode, "passed": passed, "command": command})
        print(f"{case}: {'PASS' if passed else 'FAIL'}", flush=True)
    (args.artifacts / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    return 0 if all(row["passed"] for row in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
