#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


AGENT_DIR = Path(__file__).resolve().parent
if str(AGENT_DIR) not in sys.path:
    sys.path.insert(0, str(AGENT_DIR))

import run_memory_eval as RUNNER  # noqa: E402


class SyntheticMemoryEvalContainmentTests(unittest.TestCase):
    def test_live_scenario_uses_disposable_state_and_redacts_its_prompt(self) -> None:
        with tempfile.TemporaryDirectory(prefix="sinomo-memory-eval-live-test-") as temp:
            base = Path(temp)
            operational = base / "operational"
            scratch = base / "scratch"
            results = base / "results"
            credential = operational / "configs/lm/credentials/codex.json"
            credential.parent.mkdir(parents=True)
            credential.write_text('{"access_token":"operator-copy"}', encoding="utf-8")
            operational_sentinel = operational / "events/agent/live-session.jsonl"
            operational_sentinel.parent.mkdir(parents=True)
            operational_sentinel.write_text("do-not-change", encoding="utf-8")
            args = argparse.Namespace(
                mode="run",
                provider="codex",
                model="test-model",
                account=None,
                sinomo_bin=Path("/unused/sinomo"),
                results_dir=results,
                summary_out=results / "summary.json",
                progress_out=None,
                config_source_data_dir=operational,
                scratch_root=scratch,
                pass_env=[],
                model_timeout_seconds=10.0,
                dry_run=False,
            )
            scenario = RUNNER.SCENARIOS[0]
            observed_root: Path | None = None
            observed_data_dir: Path | None = None

            def fake_process(
                command: list[str],
                isolated: object,
                *,
                timeout_seconds: float,
                display_command: list[str],
            ) -> dict[str, object]:
                nonlocal observed_root, observed_data_dir
                case = isolated
                observed_root = case.root  # type: ignore[attr-defined]
                observed_data_dir = case.data_dir  # type: ignore[attr-defined]
                self.assertEqual(timeout_seconds, 10.0)
                self.assertEqual(
                    case.environment["SINOMO_DATA_DIR"],  # type: ignore[attr-defined]
                    str(observed_data_dir),
                )
                self.assertNotEqual(observed_data_dir, operational)
                self.assertTrue(
                    (observed_data_dir / "configs/lm/credentials/codex.json").is_file()
                )
                self.assertNotIn(scenario.user_request, display_command)
                self.assertIn("<memory-eval-prompt>", display_command)
                return {
                    "command": display_command,
                    "started_at": "start",
                    "completed_at": "complete",
                    "exit_code": 0,
                    "stdout": RUNNER.fixture_response(scenario),
                    "stderr": "",
                    "timed_out": False,
                    "isolation": case.manifest(),  # type: ignore[attr-defined]
                }

            with mock.patch.object(
                RUNNER, "run_isolated_process", side_effect=fake_process
            ):
                result = RUNNER.evaluate_scenario(
                    args, scenario, RUNNER.ProgressWriter(None)
                )

            assert observed_root is not None
            assert observed_data_dir is not None
            self.assertTrue(result["passed"])
            self.assertFalse(observed_root.exists())
            self.assertFalse(observed_data_dir.exists())
            self.assertEqual(
                operational_sentinel.read_text(encoding="utf-8"), "do-not-change"
            )


if __name__ == "__main__":
    unittest.main()
