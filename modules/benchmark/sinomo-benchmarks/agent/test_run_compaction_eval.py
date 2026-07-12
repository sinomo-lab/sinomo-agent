#!/usr/bin/env python3

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import unittest
from pathlib import Path
from unittest import mock


HARNESS_PATH = Path(__file__).with_name("run_compaction_eval.py")
SPEC = importlib.util.spec_from_file_location("run_compaction_eval", HARNESS_PATH)
assert SPEC is not None and SPEC.loader is not None
HARNESS = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = HARNESS
SPEC.loader.exec_module(HARNESS)


def test_args(**overrides: object) -> argparse.Namespace:
    values: dict[str, object] = {
        "mode": "run",
        "agent": "coding-agent",
        "provider": "codex",
        "model": "test-model",
        "account": None,
        "sinomo_bin": Path("/tmp/sinomo"),
        "results_dir": Path("/tmp/sinomo-compaction-eval-tests"),
        "dry_run": True,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


class CompactionEvalHarnessTests(unittest.TestCase):
    def test_agent_json_extracts_session_resume_and_decision_text(self) -> None:
        compaction = HARNESS.fixture_compaction_evidence(HARNESS.SCENARIOS[0])
        parsed = HARNESS.parse_agent_run_json(
            json.dumps(
                {
                    "session": "01durablesession",
                    "resumed": True,
                    "decision": {"text": "retained answer", "tool_calls": []},
                    "compaction": compaction,
                }
            )
        )

        self.assertEqual(parsed["session_id"], "01durablesession")
        self.assertIs(parsed["resumed"], True)
        self.assertEqual(parsed["agent_output"], "retained answer")
        self.assertEqual(parsed["compaction"], compaction)
        self.assertIsNone(parsed["json_error"])

    def test_agent_command_uses_json_session_and_compaction_flags(self) -> None:
        invocation = HARNESS.run_agent_prompt(
            test_args(),
            "continue",
            session_id="01durablesession",
            compact_after=True,
        )

        command = invocation["command"]
        self.assertIn("--json", command)
        agent_index = command.index("--agent")
        self.assertEqual(command[agent_index + 1], "coding-agent")
        session_index = command.index("--session")
        self.assertEqual(command[session_index + 1], "01durablesession")
        self.assertIn("--compact-after", command)

    def test_scenario_continuation_resumes_without_reinjecting_summary(self) -> None:
        scenario = HARNESS.SCENARIOS[0]
        checkpoint = "unique checkpoint " + "; ".join(scenario.summary_markers)
        calls: list[tuple[str, str | None, bool]] = []

        def fake_run(
            _args: argparse.Namespace,
            prompt: str,
            session_id: str | None = None,
            compact_after: bool = False,
        ) -> dict[str, object]:
            calls.append((prompt, session_id, compact_after))
            first_turn = session_id is None
            return {
                "command": ["sinomo", "agent", "run"],
                "started_at": "start",
                "completed_at": "complete",
                "exit_code": 0,
                "stdout": "{}",
                "stderr": "",
                "dry_run": False,
                "session_id": "01durablesession",
                "resumed": not first_turn,
                "agent_output": "; ".join(scenario.markers_for_phase(
                    "compaction-summary" if compact_after else "post-compaction-continuation"
                )),
                "compaction": (
                    HARNESS.fixture_compaction_evidence(scenario)
                    if compact_after
                    else None
                ),
                "json_error": None,
            }

        with mock.patch.object(HARNESS, "run_agent_prompt", side_effect=fake_run):
            result = HARNESS.evaluate_scenario(
                test_args(dry_run=False), scenario, HARNESS.ProgressWriter(None)
            )

        self.assertTrue(result["passed"])
        self.assertEqual(calls[0][1], None)
        self.assertEqual(calls[1][1], "01durablesession")
        self.assertIs(calls[0][2], True)
        self.assertIs(calls[1][2], False)
        self.assertNotIn(checkpoint, calls[1][0])
        self.assertTrue(result["phases"][0]["session_contract_ok"])
        self.assertTrue(result["phases"][1]["session_contract_ok"])
        self.assertTrue(result["phases"][0]["compaction_contract_ok"])
        self.assertTrue(result["phases"][1]["compaction_contract_ok"])
        self.assertIs(result["phases"][1]["resumed"], True)

    def test_live_prompts_are_read_only_and_checkpoint_lists_exact_markers(self) -> None:
        scenario = HARNESS.SCENARIOS[0]

        checkpoint = scenario.compaction_prompt()
        continuation = scenario.continuation_prompt()

        self.assertIn("Do not call tools", checkpoint)
        self.assertIn("do not call tools", continuation)
        for marker in scenario.summary_markers:
            self.assertIn(f"- {marker}", checkpoint)
        for marker in [*scenario.summary_markers, *scenario.continuation_markers]:
            self.assertNotIn(marker, continuation)

    def test_live_subprocess_uses_isolated_results_workspace(self) -> None:
        args = test_args(
            dry_run=False,
            results_dir=Path("/tmp/sinomo-compaction-eval-isolation-test"),
        )
        completed = mock.Mock(returncode=0, stdout='{"session":"01session","decision":{"text":"ok"}}', stderr="")

        with mock.patch.object(HARNESS.subprocess, "run", return_value=completed) as run:
            HARNESS.run_agent_prompt(args, "read-only probe")

        self.assertEqual(
            run.call_args.kwargs["cwd"],
            args.results_dir / "workspace",
        )

    def test_compaction_contract_rejects_missing_or_unpersisted_projection(self) -> None:
        self.assertIn(
            "missing compaction evidence object",
            HARNESS.compaction_evidence_issues(None),
        )
        compaction = HARNESS.fixture_compaction_evidence(HARNESS.SCENARIOS[0])
        compaction["persisted"] = False

        self.assertIn(
            "compaction.persisted must be true",
            HARNESS.compaction_evidence_issues(compaction),
        )

    def test_compaction_phase_scores_the_persisted_summary(self) -> None:
        scenario = HARNESS.SCENARIOS[0]
        marker_rich_output = "; ".join(
            [*scenario.summary_markers, *scenario.continuation_markers]
        )

        def fake_run(
            _args: argparse.Namespace,
            _prompt: str,
            session_id: str | None = None,
            compact_after: bool = False,
        ) -> dict[str, object]:
            compaction = (
                HARNESS.fixture_compaction_evidence(scenario)
                if compact_after
                else None
            )
            if compaction is not None:
                compaction["summary"] = "The persisted projection lost the required state."
            return {
                "command": ["sinomo", "agent", "run"],
                "started_at": "start",
                "completed_at": "complete",
                "exit_code": 0,
                "stdout": "{}",
                "stderr": "",
                "dry_run": False,
                "session_id": "01durablesession",
                "resumed": session_id is not None,
                "agent_output": marker_rich_output,
                "compaction": compaction,
                "json_error": None,
            }

        with mock.patch.object(HARNESS, "run_agent_prompt", side_effect=fake_run):
            result = HARNESS.evaluate_scenario(
                test_args(dry_run=False), scenario, HARNESS.ProgressWriter(None)
            )

        first_phase = result["phases"][0]
        self.assertFalse(first_phase["passed"])
        self.assertEqual(first_phase["marker_output_source"], "compaction.summary")
        self.assertEqual(
            first_phase["missing_markers"], scenario.summary_markers
        )

    def test_invalid_cli_json_fails_the_session_contract(self) -> None:
        parsed = HARNESS.parse_agent_run_json("not-json")

        self.assertIsNone(parsed["session_id"])
        self.assertIsNone(parsed["resumed"])
        self.assertEqual(parsed["agent_output"], "")
        self.assertIn("invalid agent run JSON", parsed["json_error"])


if __name__ == "__main__":
    unittest.main()
