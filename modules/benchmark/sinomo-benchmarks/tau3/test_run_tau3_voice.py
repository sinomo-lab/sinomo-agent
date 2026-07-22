from __future__ import annotations

import argparse
import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("run_tau3_voice.py")
SPEC = importlib.util.spec_from_file_location("run_tau3_voice", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


class Tau3VoiceRunnerTests(unittest.TestCase):
    def arguments(self, **overrides):
        values = {
            "mode": "plan",
            "tau3_root": Path("/missing/tau3"),
            "bridge_bin": Path("/missing/bridge"),
            "suite": runner.DEFAULT_SUITE,
            "results_root": runner.DEFAULT_RESULTS_ROOT,
            "run_id": None,
            "domain": None,
            "full_base": False,
            "model": "gpt-realtime-2",
            "user_model": "gpt-4.1",
            "speech_complexity": "regular",
            "tick_ms": 200,
            "max_seconds": 600,
            "num_trials": 1,
            "max_concurrency": 1,
            "send_audio_instant": False,
        }
        values.update(overrides)
        return argparse.Namespace(**values)

    def test_development_suite_is_revision_pinned_and_balanced(self):
        suite = runner.load_suite(runner.DEFAULT_SUITE)

        self.assertEqual(suite["upstream_revision"], runner.PINNED_TAU3_REVISION)
        self.assertEqual(
            {domain: len(tasks) for domain, tasks in suite["domains"].items()},
            {"airline": 4, "retail": 4, "telecom": 4},
        )

    def test_smoke_plan_selects_one_fixed_task_per_domain(self):
        suite = runner.load_suite(runner.DEFAULT_SUITE)
        plan = runner.build_plan(self.arguments(mode="smoke"), suite)

        self.assertEqual(
            {domain: len(tasks) for domain, tasks in plan["selected_tasks"].items()},
            {"airline": 1, "retail": 1, "telecom": 1},
        )
        self.assertFalse(plan["tau3_revision_matches"])
        self.assertFalse(plan["bridge_exists"])

    def test_full_base_plan_uses_tau3_split_without_task_ids(self):
        suite = runner.load_suite(runner.DEFAULT_SUITE)
        plan = runner.build_plan(
            self.arguments(mode="run", full_base=True, domain=["airline"]), suite
        )

        self.assertEqual(plan["selected_tasks"], {"airline": None})

    def test_run_validation_fails_closed_on_unpinned_checkout(self):
        suite = runner.load_suite(runner.DEFAULT_SUITE)
        plan = runner.build_plan(self.arguments(mode="run"), suite)

        with self.assertRaisesRegex(RuntimeError, "pinned revision"):
            runner.validate_run_plan(plan)


if __name__ == "__main__":
    unittest.main()
