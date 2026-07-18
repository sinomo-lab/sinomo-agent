#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


AGENT_DIR = Path(__file__).resolve().parent
if str(AGENT_DIR) not in sys.path:
    sys.path.insert(0, str(AGENT_DIR))

import run_longmemeval_s as RUNNER  # noqa: E402


def case_payload(question_id: str = "question-1") -> dict[str, object]:
    return {
        "question_id": question_id,
        "question_type": "multi-session",
        "question": "QUESTION-ONLY-SECRET: Which launch color was selected?",
        "question_date": "2026/01/20 (Tue) 10:30",
        "answer": "GOLD-ONLY-SECRET: cobalt",
        "haystack_session_ids": ["raw-session-secret-a", "raw-session-secret-b"],
        "haystack_dates": [
            "2026/01/03 (Sat) 09:00",
            "2026/01/11 (Sun) 14:15",
        ],
        "haystack_sessions": [
            [
                {
                    "role": "user",
                    "content": "We selected cobalt for the launch banner.",
                    "has_answer": True,
                },
                {
                    "role": "assistant",
                    "content": "I will remember the launch banner choice.",
                    "has_answer": False,
                },
            ],
            [
                {
                    "role": "user",
                    "content": "The launch is still scheduled for Friday.",
                    "has_answer": False,
                }
            ],
        ],
        "answer_session_ids": ["raw-session-secret-a"],
    }


def parsed_case(question_id: str = "question-1") -> RUNNER.LongMemEvalCase:
    return RUNNER.parse_longmemeval_case(case_payload(question_id), 0)


def evidence(path_hint: str, turn_index: int) -> dict[str, object]:
    return {
        "anchor": {"source_version": {"file_ref": {"path_hint": path_hint}}},
        "content": (
            f"retrieved line <!-- sinomo-longmemeval-turn:{turn_index:06d} -->"
        ),
    }


class LongMemEvalDataTests(unittest.TestCase):
    def test_json_array_streams_across_tiny_chunks(self) -> None:
        payloads = [case_payload("question-unicode-λ"), case_payload("question-2")]
        with tempfile.TemporaryDirectory(prefix="sinomo-longmemeval-stream-test-") as temp:
            dataset = Path(temp) / "dataset.json"
            dataset.write_text(
                json.dumps(payloads, ensure_ascii=False, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )

            streamed = list(RUNNER.iter_json_array(dataset, chunk_bytes=3))

        self.assertEqual(streamed, payloads)

    def test_schema_rejects_missing_or_inconsistent_gold_labels(self) -> None:
        missing_answer = case_payload()
        missing_answer.pop("answer")
        unknown_session = case_payload()
        unknown_session["answer_session_ids"] = ["not-in-haystack"]
        missing_turn_label = case_payload()
        for session in missing_turn_label["haystack_sessions"]:  # type: ignore[union-attr]
            for turn in session:
                turn["has_answer"] = False
        unsupported_type = case_payload()
        unsupported_type["question_type"] = "unknown-type"
        divergent_duplicate = case_payload()
        divergent_duplicate["haystack_session_ids"] = [
            "raw-session-secret-a",
            "raw-session-secret-a",
        ]

        for payload, detail in (
            (missing_answer, "answer is missing"),
            (unknown_session, "contains unknown ids"),
            (missing_turn_label, "no has_answer turn labels"),
            (unsupported_type, "not a supported LongMemEval type"),
            (divergent_duplicate, "repeats session id.*with different turns"),
        ):
            with self.subTest(detail=detail):
                with self.assertRaisesRegex(RUNNER.LongMemEvalDataError, detail):
                    RUNNER.parse_longmemeval_case(payload, 0)

    def test_rendered_corpus_excludes_question_and_gold_metadata(self) -> None:
        payload = case_payload()
        case = RUNNER.parse_longmemeval_case(payload, 0)
        with tempfile.TemporaryDirectory(prefix="sinomo-longmemeval-corpus-test-") as temp:
            corpus = Path(temp) / "corpus"
            mapping = RUNNER.render_case_corpus(case, corpus)
            files = sorted(corpus.iterdir())
            rendered = "\n".join(path.read_text(encoding="utf-8") for path in files)
            rendered_line_count = sum(
                len(path.read_text(encoding="utf-8").splitlines()) for path in files
            )

        self.assertEqual(len(files), 2)
        self.assertEqual(rendered_line_count, 3)
        self.assertEqual(set(mapping.values()), set(payload["haystack_session_ids"]))
        self.assertIn("We selected cobalt for the launch banner.", rendered)
        self.assertIn("sinomo-longmemeval-turn:000000", rendered)
        self.assertNotIn("QUESTION-ONLY-SECRET", rendered)
        self.assertNotIn("GOLD-ONLY-SECRET", rendered)
        self.assertNotIn("answer_session_ids", rendered)
        self.assertNotIn("has_answer", rendered)
        for session_id in payload["haystack_session_ids"]:  # type: ignore[union-attr]
            self.assertNotIn(session_id, rendered)
            self.assertTrue(all(session_id not in path.name for path in files))

    def test_selected_case_stream_filters_without_loading_unselected_cases(self) -> None:
        payloads = [case_payload("question-1"), case_payload("question-2")]
        payloads[1]["question_type"] = "knowledge-update"
        with tempfile.TemporaryDirectory(prefix="sinomo-longmemeval-select-test-") as temp:
            dataset = Path(temp) / "dataset.json"
            dataset.write_text(json.dumps(payloads), encoding="utf-8")

            selected = list(
                RUNNER.iter_selected_cases(
                    dataset,
                    question_ids=[],
                    question_types=["knowledge-update"],
                    limit=1,
                )
            )

        self.assertEqual([case.question_id for case in selected], ["question-2"])

    def test_session_gold_and_turn_gold_are_independent_official_labels(self) -> None:
        payload = case_payload()
        payload["answer_session_ids"] = [
            "raw-session-secret-a",
            "raw-session-secret-b",
        ]

        case = RUNNER.parse_longmemeval_case(payload, 0)

        self.assertEqual(len(case.answer_session_ids), 2)
        self.assertEqual(case.answer_turn_ids, ("raw-session-secret-a::turn-000000",))

    def test_repeated_haystack_session_ids_are_valid_official_data(self) -> None:
        payload = case_payload()
        payload["haystack_session_ids"] = [
            "raw-session-secret-a",
            "raw-session-secret-a",
        ]
        first_session = payload["haystack_sessions"][0]  # type: ignore[index]
        payload["haystack_sessions"][1] = [  # type: ignore[index]
            dict(turn) for turn in first_session
        ]

        case = RUNNER.parse_longmemeval_case(payload, 0)

        self.assertEqual(len(case.sessions), 2)
        self.assertEqual(case.sessions[0].session_id, case.sessions[1].session_id)

    def test_empty_unlabelled_turn_keeps_its_corpus_ordinal(self) -> None:
        payload = case_payload()
        payload["haystack_sessions"][1][0]["content"] = ""  # type: ignore[index]
        case = RUNNER.parse_longmemeval_case(payload, 0)

        with tempfile.TemporaryDirectory(prefix="sinomo-longmemeval-empty-turn-") as temp:
            corpus = Path(temp) / "corpus"
            RUNNER.render_case_corpus(case, corpus)
            second_session = sorted(corpus.iterdir())[1].read_text(encoding="utf-8")

        self.assertIn("user:  <!-- sinomo-longmemeval-turn:000000 -->", second_session)


class LongMemEvalScoringAndTraceTests(unittest.TestCase):
    def test_ranking_metrics_match_upstream_longmemeval_discount(self) -> None:
        metrics = RUNNER.ranking_metrics(
            ["noise", "gold-b", "gold-a", "gold-b"],
            ["gold-a", "gold-b"],
            [1, 2, 3],
        )

        expected_ndcg_at_2 = 0.5
        self.assertEqual(metrics["mrr"], 0.5)
        self.assertEqual(metrics["recall_any@1"], 0.0)
        self.assertEqual(metrics["recall_any@2"], 1.0)
        self.assertEqual(metrics["recall_all@2"], 0.0)
        self.assertEqual(metrics["recall_all@3"], 1.0)
        self.assertAlmostEqual(metrics["ndcg_any@2"], expected_ndcg_at_2)
        self.assertEqual(RUNNER.ranking_metrics(["anything"], [], [1]), {})

    def test_evidence_rankings_deduplicate_sessions_and_turns(self) -> None:
        bundle = {
            "evidence": [
                evidence("/corpus/first.md", 1),
                evidence("/corpus/first.md", 1),
                evidence("/corpus/second.md", 0),
                evidence("/corpus/not-owned.md", 0),
            ]
        }
        sessions, turns = RUNNER.evidence_rankings(
            bundle,
            {"first.md": "session-a", "second.md": "session-b"},
        )

        self.assertEqual(sessions, ["session-a", "session-b"])
        self.assertEqual(
            turns,
            [
                "session-a::turn-000001",
                "session-b::turn-000000",
            ],
        )

    def test_model_visible_rankings_use_latest_context_frame(self) -> None:
        with tempfile.TemporaryDirectory(prefix="sinomo-longmemeval-trace-test-") as temp:
            data_dir = Path(temp)
            trace = data_dir / "events/agent/traces/session-reader.jsonl"
            trace.parent.mkdir(parents=True)
            records = [
                {
                    "record": {
                        "kind": "context_frame",
                        "frame": {
                            "blocks": [
                                {
                                    "kind": "retrieved_memory",
                                    "payload": evidence("/corpus/old.md", 0),
                                }
                            ]
                        },
                    }
                },
                {"record": {"kind": "model_event", "event": {}}},
                {
                    "record": {
                        "kind": "context_frame",
                        "frame": {
                            "blocks": [
                                {"kind": "workspace_facts", "payload": {}},
                                {
                                    "kind": "retrieved_memory",
                                    "payload": evidence("/corpus/latest.md", 2),
                                },
                            ]
                        },
                    }
                },
            ]
            trace.write_text(
                "".join(json.dumps(record) + "\n" for record in records),
                encoding="utf-8",
            )

            sessions, turns = RUNNER.model_visible_rankings(
                data_dir,
                "session-reader",
                {"old.md": "old-session", "latest.md": "latest-session"},
            )

        self.assertEqual(sessions, ["latest-session"])
        self.assertEqual(turns, ["latest-session::turn-000002"])

    def test_invalid_trace_json_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory(prefix="sinomo-longmemeval-trace-invalid-") as temp:
            data_dir = Path(temp)
            trace = data_dir / "events/agent/traces/session-reader.jsonl"
            trace.parent.mkdir(parents=True)
            trace.write_text("not-json\n", encoding="utf-8")

            with self.assertRaisesRegex(
                RUNNER.LongMemEvalExecutionError, "invalid agent trace JSON"
            ):
                RUNNER.model_visible_rankings(data_dir, "session-reader", {})

    def test_evaluate_case_emits_schema_metrics_trace_and_unique_roots(self) -> None:
        with tempfile.TemporaryDirectory(prefix="sinomo-longmemeval-evaluate-test-") as temp:
            base = Path(temp)
            operational = base / "operational"
            scratch = base / "scratch"
            operational.mkdir()
            args = argparse.Namespace(
                reader="none",
                scratch_root=scratch,
                config_source_data_dir=operational,
                provider="codex",
                model="test-model",
                account=None,
                pass_env=[],
                max_records=10,
                max_bytes=4096,
                sinomo_bin=Path("/unused/sinomo"),
                command_timeout_seconds=10.0,
                model_timeout_seconds=10.0,
            )
            observed_roots: list[Path] = []

            def fake_run(
                _args: argparse.Namespace,
                isolated: object,
                subcommand: list[str],
                **_kwargs: object,
            ) -> dict[str, object]:
                case = isolated
                root = case.root  # type: ignore[attr-defined]
                if root not in observed_roots:
                    observed_roots.append(root)
                if subcommand[:3] == ["memory", "root", "add"]:
                    payload: dict[str, object] = {"root_id": "root-test"}
                elif subcommand[:2] == ["memory", "index"]:
                    payload = {"indexed_files": 2, "status": "complete"}
                elif subcommand[:2] == ["memory", "query"]:
                    first_file = sorted(case.corpus_dir.iterdir())[0]  # type: ignore[attr-defined]
                    payload = {
                        "evidence": [evidence(str(first_file), 0)],
                        "coverage": {"routed_partition_count": 1},
                        "trace": {"searched_partition_count": 1, "stale_partition_count": 0},
                    }
                else:
                    self.fail(f"unexpected Sinomo command: {subcommand}")
                return {
                    "exit_code": 0,
                    "timed_out": False,
                    "stdout": json.dumps(payload),
                    "stderr": "",
                }

            with mock.patch.object(RUNNER, "run_sinomo", side_effect=fake_run):
                first, first_prediction = RUNNER.evaluate_case(
                    args, parsed_case("question-1"), []
                )
                second_payload = case_payload("question-2")
                second_payload["haystack_session_ids"] = [
                    "second-session-a",
                    "second-session-b",
                ]
                second_payload["answer_session_ids"] = ["second-session-a"]
                second = RUNNER.parse_longmemeval_case(second_payload, 1)
                second_result, second_prediction = RUNNER.evaluate_case(args, second, [])

        self.assertIsNone(first_prediction)
        self.assertIsNone(second_prediction)
        self.assertEqual(first["schema"], RUNNER.RESULT_SCHEMA)
        retrieval = first["retrieval_results"]
        self.assertEqual(retrieval["ranked_session_ids"], ["raw-session-secret-a"])
        self.assertEqual(retrieval["ranked_turn_ids"], ["raw-session-secret-a::turn-000000"])
        self.assertEqual(retrieval["metrics"]["session"]["mrr"], 1.0)
        self.assertEqual(
            retrieval["trace"],
            {"searched_partition_count": 1, "stale_partition_count": 0},
        )
        self.assertEqual(first["performance"]["index_report"]["indexed_files"], 2)
        self.assertEqual(len(observed_roots), 2)
        self.assertEqual(len(set(observed_roots)), 2)
        self.assertTrue(all(not root.exists() for root in observed_roots))
        self.assertNotEqual(first["isolation"]["root"], second_result["isolation"]["root"])


if __name__ == "__main__":
    unittest.main()
