#!/usr/bin/env python3
"""Run cleaned LongMemEval-S against Sinomo's real indexed-file memory path.

Each benchmark question is a distinct synthetic user.  It is therefore run in
its own disposable Sinomo data directory and working directory.  No cluster,
DFS, permissions, chat, session, memory or index state is copied from the
operator's normal data directory.

The default run is retrieval-only and deterministic.  ``--reader sinomo`` also
runs the Sinomo agent in the same isolated case and emits the official
``{question_id, hypothesis}`` JSONL contract for LongMemEval's external judge.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Iterator, Mapping, Sequence


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from eval_isolation import (  # noqa: E402
    IsolatedSinomoCase,
    default_config_source_data_dir,
    install_cleanup_signal_handlers,
    isolated_sinomo_case,
    run_isolated_process,
    validate_explicit_environment_names,
    validate_isolation_boundaries,
    validate_operational_path_separation,
    validate_pairwise_disjoint_paths,
)


def find_repo_root(start: Path) -> Path:
    for directory in (start.parent, *start.parents):
        if (directory / "Cargo.toml").exists() and (directory / "crates" / "sinomo").exists():
            return directory
    raise SystemExit(f"failed to find Sinomo workspace root from {start}")


REPO_ROOT = find_repo_root(Path(__file__).resolve())
DEFAULT_RESULTS_DIR = REPO_ROOT / "benchmark-results" / "agent-memory" / "longmemeval-s"
DEFAULT_SINOMO_BIN = REPO_ROOT / "target" / "debug" / (
    "sinomo.exe" if os.name == "nt" else "sinomo"
)
OFFICIAL_DATASET_URL = (
    "https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned/resolve/"
    "98d7416c24c778c2fee6e6f3006e7a073259d48f/longmemeval_s_cleaned.json"
)
OFFICIAL_DATASET_REVISION = "98d7416c24c778c2fee6e6f3006e7a073259d48f"
OFFICIAL_DATASET_SHA256 = "d6f21ea9d60a0d56f34a05b609c79c88a451d2ae03597821ea3d5a9678c3a442"
OFFICIAL_DATASET_BYTES = 277_383_467
LONGMEMEVAL_LICENSE = "MIT"
RESULT_SCHEMA = "sinomo.benchmark.longmemeval-s.v1"
TURN_MARKER = re.compile(r"<!-- sinomo-longmemeval-turn:(\d{6}) -->\s*$")
QUESTION_TYPES = {
    "single-session-user",
    "single-session-assistant",
    "single-session-preference",
    "temporal-reasoning",
    "knowledge-update",
    "multi-session",
}
SESSION_CUTOFFS = (1, 3, 5, 10, 30, 50)
TURN_CUTOFFS = (1, 3, 5, 10, 30, 50)


def utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class LongMemEvalTurn:
    role: str
    content: str
    has_answer: bool


@dataclass(frozen=True)
class LongMemEvalSession:
    session_id: str
    date: str
    turns: tuple[LongMemEvalTurn, ...]


@dataclass(frozen=True)
class LongMemEvalCase:
    question_id: str
    question_type: str
    question: str
    question_date: str
    sessions: tuple[LongMemEvalSession, ...]
    answer_session_ids: tuple[str, ...]

    @property
    def is_abstention(self) -> bool:
        return self.question_id.endswith("_abs")

    @property
    def answer_turn_ids(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                turn_id(session.session_id, index)
                for session in self.sessions
                for index, turn in enumerate(session.turns)
                if turn.has_answer
            )
        )


class LongMemEvalDataError(ValueError):
    pass


class LongMemEvalExecutionError(RuntimeError):
    pass


def turn_id(session_id: str, turn_index: int) -> str:
    return f"{session_id}::turn-{turn_index:06d}"


def _required_string(payload: Mapping[str, object], field: str, context: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise LongMemEvalDataError(f"{context}.{field} must be a non-empty string")
    return value


def parse_longmemeval_case(payload: object, index: int) -> LongMemEvalCase:
    context = f"case[{index}]"
    if not isinstance(payload, dict):
        raise LongMemEvalDataError(f"{context} must be an object")
    question_id = _required_string(payload, "question_id", context)
    question_type = _required_string(payload, "question_type", context)
    if question_type not in QUESTION_TYPES:
        raise LongMemEvalDataError(
            f"{context}.question_type is not a supported LongMemEval type: {question_type!r}"
        )
    question = _required_string(payload, "question", context)
    question_date = _required_string(payload, "question_date", context)
    if "answer" not in payload:
        raise LongMemEvalDataError(f"{context}.answer is missing")

    session_ids = payload.get("haystack_session_ids")
    dates = payload.get("haystack_dates")
    session_payloads = payload.get("haystack_sessions")
    answer_session_ids = payload.get("answer_session_ids")
    if not isinstance(session_ids, list) or not isinstance(dates, list) or not isinstance(
        session_payloads, list
    ):
        raise LongMemEvalDataError(
            f"{context} haystack session ids, dates and sessions must be arrays"
        )
    if len(session_ids) != len(dates) or len(session_ids) != len(session_payloads):
        raise LongMemEvalDataError(
            f"{context} haystack session ids, dates and sessions must have equal lengths"
        )
    if not isinstance(answer_session_ids, list) or not all(
        isinstance(value, str) and value for value in answer_session_ids
    ):
        raise LongMemEvalDataError(f"{context}.answer_session_ids must be a string array")
    if len(set(answer_session_ids)) != len(answer_session_ids):
        raise LongMemEvalDataError(f"{context}.answer_session_ids contains duplicates")
    sessions: list[LongMemEvalSession] = []
    for session_index, (session_id, date, turns_payload) in enumerate(
        zip(session_ids, dates, session_payloads)
    ):
        session_context = f"{context}.haystack_sessions[{session_index}]"
        if not isinstance(session_id, str) or not session_id:
            raise LongMemEvalDataError(f"{session_context} has an invalid session id")
        if not isinstance(date, str) or not date:
            raise LongMemEvalDataError(f"{session_context} has an invalid date")
        if not isinstance(turns_payload, list) or not turns_payload:
            raise LongMemEvalDataError(f"{session_context} must be a non-empty turn array")
        turns: list[LongMemEvalTurn] = []
        for turn_index_value, turn_payload in enumerate(turns_payload):
            turn_context = f"{session_context}[{turn_index_value}]"
            if not isinstance(turn_payload, dict):
                raise LongMemEvalDataError(f"{turn_context} must be an object")
            role = turn_payload.get("role")
            content = turn_payload.get("content")
            has_answer = turn_payload.get("has_answer", False)
            if role not in {"user", "assistant"}:
                raise LongMemEvalDataError(f"{turn_context}.role must be user or assistant")
            # The pinned cleaned split contains a handful of unlabeled empty
            # turns. Keep them as marker-only corpus lines so turn ordinals stay
            # aligned with the source data without inventing or dropping text.
            if not isinstance(content, str):
                raise LongMemEvalDataError(f"{turn_context}.content must be text")
            if not isinstance(has_answer, bool):
                raise LongMemEvalDataError(f"{turn_context}.has_answer must be boolean")
            turns.append(LongMemEvalTurn(role, content, has_answer))
        sessions.append(LongMemEvalSession(session_id, date, tuple(turns)))

    repeated_session_turns: dict[str, tuple[LongMemEvalTurn, ...]] = {}
    for session in sessions:
        previous_turns = repeated_session_turns.setdefault(session.session_id, session.turns)
        if previous_turns != session.turns:
            raise LongMemEvalDataError(
                f"{context} repeats session id {session.session_id!r} with different turns"
            )

    session_id_set = {session.session_id for session in sessions}
    unknown_answer_sessions = sorted(set(answer_session_ids).difference(session_id_set))
    if unknown_answer_sessions:
        raise LongMemEvalDataError(
            f"{context}.answer_session_ids contains unknown ids: {unknown_answer_sessions}"
        )
    case = LongMemEvalCase(
        question_id=question_id,
        question_type=question_type,
        question=question,
        question_date=question_date,
        sessions=tuple(sessions),
        answer_session_ids=tuple(answer_session_ids),
    )
    if not case.is_abstention and not case.answer_session_ids:
        raise LongMemEvalDataError(f"{context} has no answer session ids")
    if not case.is_abstention and not case.answer_turn_ids:
        raise LongMemEvalDataError(f"{context} has no has_answer turn labels")
    return case


def iter_json_array(path: Path, chunk_bytes: int = 1024 * 1024) -> Iterator[object]:
    """Stream a top-level JSON array without retaining the 277 MB dataset."""

    decoder = json.JSONDecoder()
    with path.open("r", encoding="utf-8") as handle:
        buffer = ""
        position = 0
        eof = False

        def fill() -> bool:
            nonlocal buffer, position, eof
            if eof:
                return False
            if position:
                buffer = buffer[position:]
                position = 0
            chunk = handle.read(chunk_bytes)
            if not chunk:
                eof = True
                return False
            buffer += chunk
            return True

        fill()
        while True:
            while position >= len(buffer) and fill():
                pass
            while position < len(buffer) and buffer[position].isspace():
                position += 1
            if position >= len(buffer) or buffer[position] != "[":
                raise LongMemEvalDataError(f"dataset {path} must be a top-level JSON array")
            position += 1
            break

        expect_value = True
        while True:
            while True:
                while position < len(buffer) and buffer[position].isspace():
                    position += 1
                if position < len(buffer):
                    break
                if not fill():
                    raise LongMemEvalDataError(f"dataset {path} ended before closing array")

            if buffer[position] == "]":
                position += 1
                break
            if not expect_value:
                if buffer[position] != ",":
                    raise LongMemEvalDataError(
                        f"dataset {path} must separate array entries with commas"
                    )
                position += 1
                while True:
                    while position < len(buffer) and buffer[position].isspace():
                        position += 1
                    if position < len(buffer):
                        break
                    if not fill():
                        raise LongMemEvalDataError(
                            f"dataset {path} ended after an array separator"
                        )

            while True:
                try:
                    value, end = decoder.raw_decode(buffer, position)
                    position = end
                    yield value
                    expect_value = False
                    break
                except json.JSONDecodeError as source:
                    if fill():
                        continue
                    raise LongMemEvalDataError(
                        f"invalid JSON in {path} near character {source.pos}: {source.msg}"
                    ) from source

        while True:
            while position < len(buffer) and buffer[position].isspace():
                position += 1
            if position < len(buffer):
                raise LongMemEvalDataError(f"dataset {path} has content after its JSON array")
            if not fill():
                return


def iter_selected_cases(
    path: Path,
    *,
    question_ids: Sequence[str],
    question_types: Sequence[str],
    limit: int | None,
) -> Iterator[LongMemEvalCase]:
    requested_ids = set(question_ids)
    requested_types = set(question_types)
    seen_ids: set[str] = set()
    selected_ids: set[str] = set()
    selected_count = 0
    for index, payload in enumerate(iter_json_array(path)):
        case = parse_longmemeval_case(payload, index)
        if case.question_id in seen_ids:
            raise LongMemEvalDataError(f"duplicate question id {case.question_id!r}")
        seen_ids.add(case.question_id)
        if requested_ids and case.question_id not in requested_ids:
            continue
        if requested_types and case.question_type not in requested_types:
            continue
        selected_ids.add(case.question_id)
        yield case
        selected_count += 1
        if limit is not None and selected_count >= limit:
            break
    missing = sorted(requested_ids.difference(selected_ids))
    if missing and (limit is None or selected_count < limit):
        raise LongMemEvalDataError(f"requested question ids were not found: {missing}")
    if selected_count == 0:
        raise LongMemEvalDataError("dataset filters selected no LongMemEval cases")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def flatten_turn_content(content: str) -> str:
    return re.sub(r"\s+", " ", content.replace("\x00", " ")).strip()


def render_case_corpus(
    case: LongMemEvalCase,
    corpus_dir: Path,
) -> dict[str, str]:
    """Write one source file per session and return filename-to-session mapping.

    Gold answer text, answer session IDs, has_answer labels and the question are
    intentionally absent from every source file.
    """

    corpus_dir.mkdir(parents=True, exist_ok=True)
    mapping: dict[str, str] = {}
    for session_index, session in enumerate(case.sessions):
        digest = hashlib.sha256(session.session_id.encode("utf-8")).hexdigest()[:20]
        filename = f"session-{session_index:04d}-{digest}.md"
        lines = [
            (
                f"- [{flatten_turn_content(session.date)}] {turn.role}: "
                f"{flatten_turn_content(turn.content)} "
                f"<!-- sinomo-longmemeval-turn:{turn_index_value:06d} -->"
            )
            for turn_index_value, turn in enumerate(session.turns)
        ]
        (corpus_dir / filename).write_text("\n".join(lines) + "\n", encoding="utf-8")
        mapping[filename] = session.session_id
    return mapping


def _json_object(stdout: object, context: str) -> dict[str, object]:
    try:
        payload = json.loads(str(stdout))
    except json.JSONDecodeError as source:
        raise LongMemEvalExecutionError(f"{context} returned invalid JSON: {source}") from source
    if not isinstance(payload, dict):
        raise LongMemEvalExecutionError(f"{context} JSON must be an object")
    return payload


def _required_invocation(
    invocation: Mapping[str, object],
    context: str,
) -> dict[str, object]:
    if invocation.get("timed_out"):
        raise LongMemEvalExecutionError(f"{context} timed out")
    if invocation.get("exit_code") != 0:
        stderr = str(invocation.get("stderr", ""))[:2000]
        raise LongMemEvalExecutionError(f"{context} failed: {stderr}")
    return _json_object(invocation.get("stdout", ""), context)


def run_sinomo(
    args: argparse.Namespace,
    isolated: IsolatedSinomoCase,
    subcommand: Sequence[str],
    *,
    timeout_seconds: float | None = None,
    redact_prompt: bool = False,
) -> dict[str, object]:
    command = [str(args.sinomo_bin), *subcommand]
    display = list(command)
    if redact_prompt and "--prompt" in display:
        prompt_index = display.index("--prompt") + 1
        if prompt_index < len(display):
            display[prompt_index] = "<longmemeval-question>"
    return run_isolated_process(
        command,
        isolated,
        timeout_seconds=timeout_seconds or args.command_timeout_seconds,
        display_command=display,
    )


def evidence_rankings(
    bundle: Mapping[str, object],
    path_to_session: Mapping[str, str],
) -> tuple[list[str], list[str]]:
    evidence = bundle.get("evidence")
    if not isinstance(evidence, list):
        raise LongMemEvalExecutionError("memory query result has no evidence array")
    ranked_sessions: list[str] = []
    ranked_turns: list[str] = []
    seen_sessions: set[str] = set()
    seen_turns: set[str] = set()
    for item in evidence:
        if not isinstance(item, dict):
            continue
        anchor = item.get("anchor")
        source_version = anchor.get("source_version") if isinstance(anchor, dict) else None
        file_ref = source_version.get("file_ref") if isinstance(source_version, dict) else None
        path_hint = file_ref.get("path_hint") if isinstance(file_ref, dict) else None
        if not isinstance(path_hint, str):
            continue
        filename = path_hint.replace("\\", "/").rsplit("/", 1)[-1]
        session_id = path_to_session.get(filename)
        if session_id is None:
            continue
        if session_id not in seen_sessions:
            ranked_sessions.append(session_id)
            seen_sessions.add(session_id)
        content = item.get("content")
        marker = TURN_MARKER.search(content) if isinstance(content, str) else None
        if marker:
            ranked_turn_id = turn_id(session_id, int(marker.group(1)))
            if ranked_turn_id not in seen_turns:
                ranked_turns.append(ranked_turn_id)
                seen_turns.add(ranked_turn_id)
    return ranked_sessions, ranked_turns


def _dcg(relevances: Sequence[int], cutoff: int) -> float:
    """Match LongMemEval's reference retrieval evaluator exactly.

    The upstream implementation gives ranks one and two unit weight, then
    discounts subsequent ranks by log2(rank). Keeping that convention makes
    ``ndcg_any`` directly comparable with published LongMemEval retrieval logs.
    """

    selected = relevances[:cutoff]
    if not selected:
        return 0.0
    return float(selected[0]) + sum(
        relevance / math.log2(rank)
        for rank, relevance in enumerate(selected[1:], start=2)
    )


def ranking_metrics(
    ranked_ids: Sequence[str],
    correct_ids: Sequence[str],
    cutoffs: Sequence[int],
) -> dict[str, float]:
    correct = set(correct_ids)
    if not correct:
        return {}
    deduplicated = list(dict.fromkeys(ranked_ids))
    first_relevant_rank = next(
        (index for index, value in enumerate(deduplicated, start=1) if value in correct),
        None,
    )
    metrics: dict[str, float] = {
        "mrr": 0.0 if first_relevant_rank is None else 1.0 / first_relevant_rank
    }
    for cutoff in cutoffs:
        retrieved = set(deduplicated[:cutoff])
        relevances = [1 if value in correct else 0 for value in deduplicated[:cutoff]]
        ideal = [1] * min(len(correct), cutoff)
        ideal_dcg = _dcg(ideal, cutoff)
        metrics[f"recall_any@{cutoff}"] = float(bool(retrieved.intersection(correct)))
        metrics[f"recall_all@{cutoff}"] = float(correct.issubset(retrieved))
        metrics[f"ndcg_any@{cutoff}"] = (
            0.0 if ideal_dcg == 0.0 else _dcg(relevances, cutoff) / ideal_dcg
        )
    return metrics


def parse_agent_run_json(stdout: object) -> tuple[str, str]:
    payload = _json_object(stdout, "sinomo agent run")
    session_id = payload.get("session")
    decision = payload.get("decision")
    hypothesis = decision.get("text") if isinstance(decision, dict) else None
    if not isinstance(session_id, str) or not session_id:
        raise LongMemEvalExecutionError("sinomo agent run JSON has no session")
    if not isinstance(hypothesis, str) or not hypothesis.strip():
        raise LongMemEvalExecutionError("sinomo agent run JSON has no decision.text")
    return session_id, hypothesis


def model_visible_rankings(
    data_dir: Path,
    session_id: str,
    path_to_session: Mapping[str, str],
) -> tuple[list[str], list[str]]:
    trace_path = data_dir / "events" / "agent" / "traces" / f"{session_id}.jsonl"
    if not trace_path.is_file():
        raise LongMemEvalExecutionError(f"agent trace is missing: {trace_path}")
    latest_blocks: list[object] | None = None
    with trace_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as source:
                raise LongMemEvalExecutionError(
                    f"invalid agent trace JSON at line {line_number}: {source}"
                ) from source
            record = payload.get("record") if isinstance(payload, dict) else None
            if not isinstance(record, dict) or record.get("kind") != "context_frame":
                continue
            frame = record.get("frame")
            blocks = frame.get("blocks") if isinstance(frame, dict) else None
            if isinstance(blocks, list):
                latest_blocks = blocks
    if latest_blocks is None:
        raise LongMemEvalExecutionError("agent trace has no context_frame record")
    evidence = []
    for block in latest_blocks:
        if not isinstance(block, dict) or block.get("kind") != "retrieved_memory":
            continue
        payload = block.get("payload")
        if isinstance(payload, dict):
            evidence.append(payload)
    return evidence_rankings({"evidence": evidence}, path_to_session)


def reader_prompt(case: LongMemEvalCase) -> str:
    return f"""LONGMEMEVAL-S READER EVALUATION

Answer the question using only memory evidence automatically supplied by the
host. Do not call tools. If the memory evidence is insufficient, say that the
question cannot be answered from available memory. Return only the answer.

Question date: {case.question_date}
Question: {case.question}
"""


def evaluate_case(
    args: argparse.Namespace,
    case: LongMemEvalCase,
    protected_paths: Sequence[Path],
) -> tuple[dict[str, object], dict[str, str] | None]:
    copy_provider = args.reader == "sinomo"
    with isolated_sinomo_case(
        case.question_id,
        scratch_root=args.scratch_root,
        config_source_data_dir=args.config_source_data_dir,
        provider=args.provider,
        account=args.account,
        copy_provider_config=copy_provider,
        pass_environment=args.pass_env,
        protected_paths=protected_paths,
    ) as isolated:
        path_to_session = render_case_corpus(case, isolated.corpus_dir)
        corpus_bytes = sum(path.stat().st_size for path in isolated.corpus_dir.iterdir())

        root_started = time.perf_counter()
        root_invocation = run_sinomo(
            args, isolated, ["memory", "root", "add", str(isolated.corpus_dir)]
        )
        root_payload = _required_invocation(root_invocation, "memory root add")
        root_seconds = time.perf_counter() - root_started
        root_id = root_payload.get("root_id")
        if not isinstance(root_id, str) or not root_id:
            raise LongMemEvalExecutionError("memory root add JSON has no root_id")

        index_started = time.perf_counter()
        index_invocation = run_sinomo(args, isolated, ["memory", "index", root_id])
        index_payload = _required_invocation(index_invocation, "memory index")
        index_seconds = time.perf_counter() - index_started

        query_started = time.perf_counter()
        query_invocation = run_sinomo(
            args,
            isolated,
            [
                "memory",
                "query",
                case.question,
                "--max-records",
                str(args.max_records),
                "--max-bytes",
                str(args.max_bytes),
            ],
        )
        query_payload = _required_invocation(query_invocation, "memory query")
        query_seconds = time.perf_counter() - query_started
        ranked_sessions, ranked_turns = evidence_rankings(query_payload, path_to_session)

        metrics: dict[str, object]
        if case.is_abstention:
            metrics = {"skipped": True, "reason": "official retrieval evaluation skips abstention"}
        else:
            metrics = {
                "session": ranking_metrics(
                    ranked_sessions, case.answer_session_ids, SESSION_CUTOFFS
                ),
                "turn": ranking_metrics(
                    ranked_turns, case.answer_turn_ids, TURN_CUTOFFS
                ),
            }

        prediction: dict[str, str] | None = None
        reader_result: dict[str, object] | None = None
        if args.reader == "sinomo":
            prompt = reader_prompt(case)
            reader_command = [
                "agent",
                "run",
                "--json",
                "--provider",
                args.provider,
                "--model",
                args.model,
                "--prompt",
                prompt,
            ]
            if args.account:
                reader_command.extend(["--account", args.account])
            reader_started = time.perf_counter()
            reader_invocation = run_sinomo(
                args,
                isolated,
                reader_command,
                timeout_seconds=args.model_timeout_seconds,
                redact_prompt=True,
            )
            if reader_invocation.get("timed_out") or reader_invocation.get("exit_code") != 0:
                raise LongMemEvalExecutionError(
                    f"sinomo reader failed: {str(reader_invocation.get('stderr', ''))[:2000]}"
                )
            agent_session_id, hypothesis = parse_agent_run_json(reader_invocation.get("stdout"))
            visible_sessions, visible_turns = model_visible_rankings(
                isolated.data_dir, agent_session_id, path_to_session
            )
            reader_result = {
                "elapsed_seconds": time.perf_counter() - reader_started,
                "session_id": agent_session_id,
                "model_visible_session_ids": visible_sessions,
                "model_visible_turn_ids": visible_turns,
                "model_visible_metrics": (
                    {"skipped": True}
                    if case.is_abstention
                    else {
                        "session": ranking_metrics(
                            visible_sessions, case.answer_session_ids, (1, 3, 5, 8)
                        ),
                        "turn": ranking_metrics(
                            visible_turns, case.answer_turn_ids, (1, 3, 5, 8)
                        ),
                    }
                ),
                "hypothesis": hypothesis,
                "invocation": {
                    key: value
                    for key, value in reader_invocation.items()
                    if key not in {"stdout", "stderr"}
                },
            }
            prediction = {"question_id": case.question_id, "hypothesis": hypothesis}

        result = {
            "schema": RESULT_SCHEMA,
            "question_id": case.question_id,
            "question_type": case.question_type,
            "is_abstention": case.is_abstention,
            "session_count": len(case.sessions),
            "turn_count": sum(len(session.turns) for session in case.sessions),
            "gold_session_count": len(case.answer_session_ids),
            "gold_turn_count": len(case.answer_turn_ids),
            "retrieval_results": {
                "ranked_session_ids": ranked_sessions,
                "ranked_turn_ids": ranked_turns,
                "metrics": metrics,
                "coverage": query_payload.get("coverage"),
                "trace": query_payload.get("trace"),
                "evidence_count": len(query_payload.get("evidence", []))
                if isinstance(query_payload.get("evidence"), list)
                else 0,
            },
            "performance": {
                "corpus_bytes": corpus_bytes,
                "root_registration_seconds": root_seconds,
                "index_seconds": index_seconds,
                "query_seconds": query_seconds,
                "index_report": index_payload,
            },
            "reader": reader_result,
            "isolation": isolated.manifest(),
        }
        return result, prediction


def mean(values: Iterable[float]) -> float | None:
    collected = list(values)
    return None if not collected else sum(collected) / len(collected)


def aggregate_results(results: Sequence[Mapping[str, object]]) -> dict[str, object]:
    scored = [result for result in results if not result.get("is_abstention")]
    session_metric_names = [
        "mrr",
        *(
            metric
            for cutoff in SESSION_CUTOFFS
            for metric in (
                f"recall_any@{cutoff}",
                f"recall_all@{cutoff}",
                f"ndcg_any@{cutoff}",
            )
        ),
    ]
    turn_metric_names = [
        "mrr",
        *(
            metric
            for cutoff in TURN_CUTOFFS
            for metric in (
                f"recall_any@{cutoff}",
                f"recall_all@{cutoff}",
                f"ndcg_any@{cutoff}",
            )
        ),
    ]

    def metric_value(result: Mapping[str, object], level: str, name: str) -> float | None:
        retrieval = result.get("retrieval_results")
        metrics = retrieval.get("metrics") if isinstance(retrieval, dict) else None
        level_metrics = metrics.get(level) if isinstance(metrics, dict) else None
        value = level_metrics.get(name) if isinstance(level_metrics, dict) else None
        return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None

    return {
        "case_count": len(results),
        "retrieval_scored_case_count": len(scored),
        "abstention_case_count": sum(bool(result.get("is_abstention")) for result in results),
        "session": {
            name: mean(
                value
                for result in scored
                if (value := metric_value(result, "session", name)) is not None
            )
            for name in session_metric_names
        },
        "turn": {
            name: mean(
                value
                for result in scored
                if (value := metric_value(result, "turn", name)) is not None
            )
            for name in turn_metric_names
        },
        "performance": {
            name: mean(
                float(performance[name])
                for result in results
                if isinstance((performance := result.get("performance")), dict)
                and isinstance(performance.get(name), (int, float))
            )
            for name in ("corpus_bytes", "index_seconds", "query_seconds")
        },
    }


class JsonlWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    def append(self, payload: Mapping[str, object]) -> None:
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, separators=(",", ":"), ensure_ascii=False) + "\n")


class ProgressWriter:
    def __init__(self, path: Path | None) -> None:
        self.path = path
        if path:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("", encoding="utf-8")

    def emit(self, event: str, **fields: object) -> None:
        if self.path:
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps({"time": utc_now(), "event": event, **fields}) + "\n"
                )


def git_revision() -> str | None:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    return completed.stdout.strip() if completed.returncode == 0 else None


def build_plan(args: argparse.Namespace) -> dict[str, object]:
    return {
        "schema": RESULT_SCHEMA,
        "mode": args.mode,
        "dataset": {
            "name": "LongMemEval-S cleaned",
            "revision": OFFICIAL_DATASET_REVISION,
            "sha256": OFFICIAL_DATASET_SHA256,
            "bytes": OFFICIAL_DATASET_BYTES,
            "url": OFFICIAL_DATASET_URL,
            "license": LONGMEMEVAL_LICENSE,
            "download_is_explicit": True,
        },
        "reader": args.reader,
        "provider": args.provider if args.reader == "sinomo" else None,
        "model": args.model if args.reader == "sinomo" else None,
        "account": args.account if args.reader == "sinomo" else None,
        "selection": {
            "question_ids": list(args.question_id),
            "question_types": list(args.question_type),
            "limit": args.limit,
        },
        "allows_unpinned_dataset": args.allow_unpinned_dataset,
        "retrieval": {
            "implementation": "sinomo indexed-file memory CLI",
            "granularity": "turn anchors with session source files",
            "max_records": args.max_records,
            "max_bytes": args.max_bytes,
            "session_cutoffs": list(SESSION_CUTOFFS),
            "turn_cutoffs": list(TURN_CUTOFFS),
            "metrics": ["recall_any", "recall_all", "ndcg_any"],
            "metric_compatibility": "upstream LongMemEval retrieval evaluator",
            "official_abstention_policy": "skip retrieval scoring",
        },
        "containment": {
            "fresh_data_dir_per_question": True,
            "fresh_home_xdg_tmp_workspace_per_question": True,
            "copy_allowlist": [
                "configs/lm/providers.json (selected account only, when --account is used)",
                "configs/lm/credentials/<selected-provider>.json",
                "providers/<selected-provider>/credentials.json (legacy fallback only)",
            ],
            "copies_cluster_dfs_permissions_events_sessions_memory": False,
            "inherits_sinomo_environment": False,
            "local_permissions_start_deny_all": True,
            "operational_path_overlap_rejected_before_writes": True,
            "cleanup_after_every_case": True,
            "explicit_environment_names": list(args.pass_env),
        },
        "timeouts_seconds": {
            "command": args.command_timeout_seconds,
            "model": args.model_timeout_seconds if args.reader == "sinomo" else None,
        },
        "example_download_command": (
            "curl -L --fail --output "
            "benchmark-results/datasets/longmemeval_s_cleaned.json "
            f"{OFFICIAL_DATASET_URL}"
        ),
        "example_retrieval_command": " ".join(
            [
                sys.executable,
                str(Path(__file__).relative_to(REPO_ROOT)),
                "--mode run",
                "--dataset benchmark-results/datasets/longmemeval_s_cleaned.json",
                "--reader none",
                "--limit 50",
            ]
        ),
    }


def write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["plan", "run"], default="plan")
    parser.add_argument("--dataset", type=Path)
    parser.add_argument("--allow-unpinned-dataset", action="store_true")
    parser.add_argument("--question-id", action="append", default=[])
    parser.add_argument("--question-type", action="append", choices=sorted(QUESTION_TYPES), default=[])
    parser.add_argument("--limit", type=int)
    parser.add_argument("--reader", choices=["none", "sinomo"], default="none")
    parser.add_argument("--provider", default="codex")
    parser.add_argument("--model", default="gpt-5.2-codex")
    parser.add_argument("--account")
    parser.add_argument(
        "--sinomo-bin", type=Path, default=DEFAULT_SINOMO_BIN
    )
    parser.add_argument("--config-source-data-dir", type=Path)
    parser.add_argument("--scratch-root", type=Path)
    parser.add_argument("--pass-env", action="append", default=[])
    parser.add_argument("--max-records", type=int, default=50)
    parser.add_argument("--max-bytes", type=int, default=1024 * 1024)
    parser.add_argument("--command-timeout-seconds", type=float, default=120.0)
    parser.add_argument("--model-timeout-seconds", type=float, default=600.0)
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--summary-out", type=Path)
    parser.add_argument("--retrieval-out", type=Path)
    parser.add_argument("--predictions-out", type=Path)
    parser.add_argument("--progress-out", type=Path)
    parser.add_argument("--fail-fast", action="store_true")
    args = parser.parse_args()
    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be positive")
    if args.max_records <= 0 or args.max_bytes <= 0:
        parser.error("retrieval record and byte limits must be positive")
    if args.command_timeout_seconds <= 0 or args.model_timeout_seconds <= 0:
        parser.error("timeouts must be positive")
    try:
        args.pass_env = validate_explicit_environment_names(args.pass_env)
    except ValueError as source:
        parser.error(str(source))
    if args.mode == "run" and args.dataset is None:
        parser.error("--dataset is required in run mode")
    args.sinomo_bin = args.sinomo_bin.expanduser().resolve(strict=False)
    args.results_dir = args.results_dir.expanduser().resolve(strict=False)
    for attribute in (
        "summary_out",
        "retrieval_out",
        "predictions_out",
        "progress_out",
        "scratch_root",
    ):
        value = getattr(args, attribute)
        if value is not None:
            setattr(args, attribute, value.expanduser().resolve(strict=False))
    args.config_source_data_dir = (
        args.config_source_data_dir.expanduser().resolve(strict=False)
        if args.config_source_data_dir
        else default_config_source_data_dir().resolve(strict=False)
    )
    return args


def main() -> int:
    install_cleanup_signal_handlers()
    args = parse_args()
    plan = build_plan(args)
    if args.mode == "plan":
        if args.summary_out:
            validate_operational_path_separation(
                config_source_data_dir=args.config_source_data_dir,
                protected_paths=[args.summary_out],
            )
        print(json.dumps(plan, indent=2))
        if args.summary_out:
            write_json(args.summary_out, plan)
        return 0

    assert args.dataset is not None
    dataset = args.dataset.expanduser().resolve(strict=True)
    if not dataset.is_file():
        raise SystemExit(f"LongMemEval dataset is not a file: {dataset}")
    if not args.sinomo_bin.is_file():
        raise SystemExit(f"Sinomo binary does not exist; build it first: {args.sinomo_bin}")
    dataset_bytes = dataset.stat().st_size
    dataset_sha256 = file_sha256(dataset)
    dataset_matches_official = (
        dataset_bytes == OFFICIAL_DATASET_BYTES and dataset_sha256 == OFFICIAL_DATASET_SHA256
    )
    if not dataset_matches_official and not args.allow_unpinned_dataset:
        raise SystemExit(
            "dataset does not match the pinned cleaned LongMemEval-S revision; "
            "use --allow-unpinned-dataset only for an intentional fixture or alternate revision"
        )

    summary_out = args.summary_out or args.results_dir / "summary.json"
    retrieval_out = args.retrieval_out or args.results_dir / "retrieval.jsonl"
    predictions_out = args.predictions_out or args.results_dir / "predictions.jsonl"
    progress_out = args.progress_out or args.results_dir / "progress.jsonl"
    validate_pairwise_disjoint_paths(
        [dataset, summary_out, retrieval_out, predictions_out, progress_out]
    )
    protected_paths = [dataset, summary_out, retrieval_out, predictions_out, progress_out]
    validate_isolation_boundaries(
        scratch_root=args.scratch_root,
        config_source_data_dir=args.config_source_data_dir,
        protected_paths=protected_paths,
    )
    for output in (summary_out, retrieval_out, predictions_out, progress_out):
        output.parent.mkdir(parents=True, exist_ok=True)
    retrieval_writer = JsonlWriter(retrieval_out)
    prediction_writer = JsonlWriter(predictions_out) if args.reader == "sinomo" else None
    progress = ProgressWriter(progress_out)

    results: list[dict[str, object]] = []
    failures: list[dict[str, str]] = []
    started_at = utc_now()
    progress.emit("run-start", dataset_sha256=dataset_sha256, reader=args.reader)
    try:
        selected = iter_selected_cases(
            dataset,
            question_ids=args.question_id,
            question_types=args.question_type,
            limit=args.limit,
        )
        for case in selected:
            progress.emit("case-start", question_id=case.question_id)
            try:
                result, prediction = evaluate_case(args, case, protected_paths)
                retrieval_writer.append(result)
                if prediction is not None and prediction_writer is not None:
                    prediction_writer.append(prediction)
                results.append(result)
                progress.emit("case-complete", question_id=case.question_id)
            except Exception as source:  # keep the remaining 500-case run useful
                failure = {"question_id": case.question_id, "error": str(source)}
                failures.append(failure)
                retrieval_writer.append(
                    {
                        "schema": RESULT_SCHEMA,
                        "question_id": case.question_id,
                        "question_type": case.question_type,
                        "status": "failed",
                        "error": str(source),
                    }
                )
                progress.emit("case-failed", **failure)
                if args.fail_fast:
                    break
    except LongMemEvalDataError as source:
        failures.append({"question_id": "<dataset>", "error": str(source)})

    aggregate = aggregate_results(results)
    status = "passed" if not failures else "failed"
    summary = {
        **plan,
        "status": status,
        "started_at": started_at,
        "completed_at": utc_now(),
        "repo_git_revision": git_revision(),
        "sinomo_binary": str(args.sinomo_bin),
        "sinomo_binary_sha256": file_sha256(args.sinomo_bin),
        "dataset_path": str(dataset),
        "dataset_bytes": dataset_bytes,
        "dataset_sha256": dataset_sha256,
        "dataset_matches_pinned_revision": dataset_matches_official,
        "retrieval_output": str(retrieval_out),
        "predictions_output": str(predictions_out) if prediction_writer else None,
        "progress_output": str(progress_out),
        "aggregate": aggregate,
        "failure_count": len(failures),
        "failures": failures,
    }
    write_json(summary_out, summary)
    progress.emit("run-complete", status=status, failure_count=len(failures))
    print(json.dumps(summary, indent=2))
    return 0 if status == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
