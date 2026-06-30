#!/usr/bin/env python3
"""Channel and multi-agent discussion eval harness for Sinomo.

The default fixture mode is deterministic and safe for CI. Run mode calls the
configured Sinomo CLI and scores whether role-based agents communicate useful
messages, stay silent when appropriate, combine private context through the
visible channel transcript, and stop at the configured loop bound.

The clembench mode is an adapter for an upstream clembench checkout. It prepares
a small workspace with a generated Sinomo clemcore backend and an optional
trimmed game instance file, then runs the official `clem` CLI when available.
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import re
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterable


def find_repo_root(start: Path) -> Path:
    for directory in (start.parent, *start.parents):
        if (directory / "Cargo.toml").exists() and (directory / "crates" / "sinomo").exists():
            return directory
    raise SystemExit(f"failed to find Sinomo workspace root from {start}")


REPO_ROOT = find_repo_root(Path(__file__).resolve())
DEFAULT_RESULTS_DIR = REPO_ROOT / "benchmark-results" / "agent-channel-discussion"
NATIVE_SUITE = "native"
CLEMBENCH_LITE_SUITE = "clembench-lite"
ALL_SUITES = (NATIVE_SUITE, CLEMBENCH_LITE_SUITE)


def utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class ScriptStep:
    publish: bool
    content: str = ""
    note: str = "scripted"
    required_trigger_markers: list[str] = field(default_factory=list)
    required_transcript_markers: list[str] = field(default_factory=list)
    repeatable: bool = False


@dataclass(frozen=True)
class RoleSpec:
    key: str
    name: str
    private_context: str
    scripted_steps: list[ScriptStep]


@dataclass(frozen=True)
class ChannelCase:
    case_id: str
    suite: str
    title: str
    source: str
    purpose: str
    user_messages: list[str]
    roles: list[RoleSpec]
    expected_markers: list[str]
    forbidden_markers: list[str] = field(default_factory=list)
    expected_speaker_roles: list[str] = field(default_factory=list)
    expected_silent_roles: list[str] = field(default_factory=list)
    role_forbidden_markers: dict[str, list[str]] = field(default_factory=dict)
    min_agent_messages: int = 1
    max_agent_messages: int = 8
    max_agent_messages_per_ingress: int = 8
    expect_limit_reached: bool = False


@dataclass
class ChannelMessage:
    sequence: int
    author_kind: str
    author_key: str
    author_name: str
    content: str


@dataclass
class PublicationDecisionRecord:
    case_id: str
    trigger_sequence: int
    role_key: str
    role_name: str
    publish: bool
    note: str
    raw_output: str = ""


class ProgressWriter:
    def __init__(self, path: Path | None) -> None:
        self.path = path

    def emit(self, event: str, **fields: object) -> None:
        if not self.path:
            return
        payload = {"time": utc_now(), "event": event, **fields}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, separators=(",", ":")) + "\n")


def native_cases() -> list[ChannelCase]:
    return [
        ChannelCase(
            case_id="native-consensus-arithmetic",
            suite=NATIVE_SUITE,
            title="Consensus arithmetic with verifier",
            source="sinomo-native-fixture",
            purpose="Check that multiple agents can contribute a short answer and verification.",
            user_messages=["Compute 37 + 5. One agent should answer and another should verify."],
            roles=[
                RoleSpec(
                    key="analyst",
                    name="Analyst",
                    private_context="You solve the arithmetic directly.",
                    scripted_steps=[
                        ScriptStep(
                            publish=True,
                            content="37 + 5 = 42. FINAL_ANSWER: 42",
                            required_trigger_markers=["37 + 5"],
                        )
                    ],
                ),
                RoleSpec(
                    key="verifier",
                    name="Verifier",
                    private_context="You verify another agent's answer if one appears.",
                    scripted_steps=[
                        ScriptStep(
                            publish=True,
                            content="Verified: the final answer is 42.",
                            required_transcript_markers=["FINAL_ANSWER: 42"],
                        )
                    ],
                ),
            ],
            expected_markers=["FINAL_ANSWER: 42", "Verified"],
            expected_speaker_roles=["analyst", "verifier"],
            min_agent_messages=2,
            max_agent_messages=3,
            max_agent_messages_per_ingress=3,
        ),
        ChannelCase(
            case_id="native-hidden-info-merge",
            suite=NATIVE_SUITE,
            title="Private shards combine through shared transcript",
            source="sinomo-native-fixture",
            purpose="Check that agents combine private role context only by publishing visible messages.",
            user_messages=[
                "Each shard holder should publish its shard. The synthesizer should combine the visible shards."
            ],
            roles=[
                RoleSpec(
                    key="east-shard",
                    name="East Shard",
                    private_context="Your private shard is RIVER. Publish only the shard.",
                    scripted_steps=[
                        ScriptStep(publish=True, content="Left shard: RIVER.")
                    ],
                ),
                RoleSpec(
                    key="west-shard",
                    name="West Shard",
                    private_context="Your private shard is LAMP. Publish only the shard.",
                    scripted_steps=[
                        ScriptStep(publish=True, content="Right shard: LAMP.")
                    ],
                ),
                RoleSpec(
                    key="synthesizer",
                    name="Synthesizer",
                    private_context="Combine only shards that are visible in the channel transcript.",
                    scripted_steps=[
                        ScriptStep(
                            publish=True,
                            content="Combined visible code: FINAL_CODE: RIVER-LAMP.",
                            required_transcript_markers=["RIVER", "LAMP"],
                        )
                    ],
                ),
            ],
            expected_markers=["Left shard: RIVER", "Right shard: LAMP", "FINAL_CODE: RIVER-LAMP"],
            expected_speaker_roles=["east-shard", "west-shard", "synthesizer"],
            min_agent_messages=3,
            max_agent_messages=5,
            max_agent_messages_per_ingress=5,
        ),
        ChannelCase(
            case_id="native-silence-routing",
            suite=NATIVE_SUITE,
            title="Irrelevant role stays silent",
            source="sinomo-native-fixture",
            purpose="Check that an agent can decline to speak when it has no useful contribution.",
            user_messages=["Please tighten the wording in the release note. No security review is needed."],
            roles=[
                RoleSpec(
                    key="docs",
                    name="Docs",
                    private_context="You handle release note wording.",
                    scripted_steps=[
                        ScriptStep(
                            publish=True,
                            content="Suggested wording: 'The update improves channel discussion reliability.'",
                            required_trigger_markers=["release note"],
                        )
                    ],
                ),
                RoleSpec(
                    key="security",
                    name="Security",
                    private_context="You speak only for explicit security review or policy risk.",
                    scripted_steps=[
                        ScriptStep(publish=False, note="no security review requested", repeatable=True)
                    ],
                ),
            ],
            expected_markers=["channel discussion reliability"],
            expected_speaker_roles=["docs"],
            expected_silent_roles=["security"],
            min_agent_messages=1,
            max_agent_messages=2,
            max_agent_messages_per_ingress=2,
        ),
        ChannelCase(
            case_id="native-loop-bound",
            suite=NATIVE_SUITE,
            title="Self-sustaining replies hit response limit",
            source="sinomo-native-fixture",
            purpose="Check that bounded channel ingress stops an unproductive discussion loop.",
            user_messages=["Start a short echo loop."],
            roles=[
                RoleSpec(
                    key="echo-a",
                    name="Echo A",
                    private_context="Echo back when you see an echo message.",
                    scripted_steps=[
                        ScriptStep(publish=True, content="ECHO from A.", repeatable=True)
                    ],
                ),
                RoleSpec(
                    key="echo-b",
                    name="Echo B",
                    private_context="Echo back when you see an echo message.",
                    scripted_steps=[
                        ScriptStep(publish=True, content="ECHO from B.", repeatable=True)
                    ],
                ),
            ],
            expected_markers=["ECHO from A", "ECHO from B"],
            max_agent_messages_per_ingress=2,
            min_agent_messages=2,
            max_agent_messages=2,
            expect_limit_reached=True,
        ),
        ChannelCase(
            case_id="native-final-handoff",
            suite=NATIVE_SUITE,
            title="Research, verification, final handoff",
            source="sinomo-native-fixture",
            purpose="Check that discussion can progress from finding to verification to final handoff.",
            user_messages=["Decide whether to ship the small docs-only patch."],
            roles=[
                RoleSpec(
                    key="researcher",
                    name="Researcher",
                    private_context="You inspect the change scope.",
                    scripted_steps=[
                        ScriptStep(
                            publish=True,
                            content="Finding: the patch only changes research notes and has no runtime code path.",
                        )
                    ],
                ),
                RoleSpec(
                    key="reviewer",
                    name="Reviewer",
                    private_context="You verify whether the finding is enough for release.",
                    scripted_steps=[
                        ScriptStep(
                            publish=True,
                            content="Review: docs-only change is low risk after whitespace validation.",
                            required_transcript_markers=["docs", "no runtime code path"],
                        )
                    ],
                ),
                RoleSpec(
                    key="finalizer",
                    name="Finalizer",
                    private_context="You publish a final decision after finding and review are visible.",
                    scripted_steps=[
                        ScriptStep(
                            publish=True,
                            content="FINAL_DECISION: ship the docs-only patch.",
                            required_transcript_markers=["Finding:", "Review:"],
                        )
                    ],
                ),
            ],
            expected_markers=["Finding:", "Review:", "FINAL_DECISION: ship"],
            expected_speaker_roles=["researcher", "reviewer", "finalizer"],
            min_agent_messages=3,
            max_agent_messages=5,
            max_agent_messages_per_ingress=5,
        ),
    ]


def clembench_lite_cases() -> list[ChannelCase]:
    return [
        ChannelCase(
            case_id="clem-taboo-mini",
            suite=CLEMBENCH_LITE_SUITE,
            title="Taboo-style two-player word game",
            source="clembench-taboo-derived",
            purpose="Check communicative clue giving, guessing, and forbidden-word discipline.",
            user_messages=[
                "Play one mini Taboo round. The describer gives a clue, then the guesser makes one final guess."
            ],
            roles=[
                RoleSpec(
                    key="describer",
                    name="Describer",
                    private_context=(
                        "Target word: volcano. Forbidden clue words: volcano, lava, mountain. "
                        "Give one clue without using any forbidden word."
                    ),
                    scripted_steps=[
                        ScriptStep(
                            publish=True,
                            content="A natural opening in the Earth's crust can erupt ash, steam, and hot rock.",
                        )
                    ],
                ),
                RoleSpec(
                    key="guesser",
                    name="Guesser",
                    private_context=(
                        "Candidate words: volcano, library, compass, lantern. "
                        "After a visible clue, answer with FINAL_GUESS: <word>."
                    ),
                    scripted_steps=[
                        ScriptStep(
                            publish=True,
                            content="FINAL_GUESS: volcano",
                            required_transcript_markers=["erupt", "ash"],
                        )
                    ],
                ),
            ],
            expected_markers=["FINAL_GUESS: volcano"],
            expected_speaker_roles=["describer", "guesser"],
            role_forbidden_markers={"describer": ["volcano", "lava", "mountain"]},
            min_agent_messages=2,
            max_agent_messages=3,
            max_agent_messages_per_ingress=3,
        ),
        ChannelCase(
            case_id="clem-privateshared-mini",
            suite=CLEMBENCH_LITE_SUITE,
            title="Private/shared slot-filling miniature",
            source="clembench-privateshared-derived",
            purpose="Check asymmetric private slot disclosure and common-ground tracking.",
            user_messages=[
                "Run a mini travel slot-filling exchange. Ask for origin, destination, and date, then summarize shared slots."
            ],
            roles=[
                RoleSpec(
                    key="questioner",
                    name="Questioner",
                    private_context="You need to fill origin, destination, and travel date.",
                    scripted_steps=[
                        ScriptStep(publish=True, content="QUESTION: What city are you leaving from?"),
                        ScriptStep(
                            publish=True,
                            content="QUESTION: What city are you travelling to?",
                            required_transcript_markers=["ANSWER: Boston"],
                        ),
                        ScriptStep(
                            publish=True,
                            content="QUESTION: What day are you leaving?",
                            required_transcript_markers=["ANSWER: Rome"],
                        ),
                        ScriptStep(
                            publish=True,
                            content="SHARED_SLOTS: origin=Boston; destination=Rome; date=Friday.",
                            required_transcript_markers=["ANSWER: Friday"],
                        ),
                    ],
                ),
                RoleSpec(
                    key="answerer",
                    name="Answerer",
                    private_context="Private slots: origin=Boston, destination=Rome, date=Friday.",
                    scripted_steps=[
                        ScriptStep(
                            publish=True,
                            content="ANSWER: Boston",
                            required_trigger_markers=["leaving from"],
                        ),
                        ScriptStep(
                            publish=True,
                            content="ANSWER: Rome",
                            required_trigger_markers=["travelling to"],
                        ),
                        ScriptStep(
                            publish=True,
                            content="ANSWER: Friday",
                            required_trigger_markers=["What day"],
                        ),
                    ],
                ),
            ],
            expected_markers=[
                "ANSWER: Boston",
                "ANSWER: Rome",
                "ANSWER: Friday",
                "SHARED_SLOTS: origin=Boston; destination=Rome; date=Friday",
            ],
            expected_speaker_roles=["questioner", "answerer"],
            min_agent_messages=7,
            max_agent_messages=8,
            max_agent_messages_per_ingress=8,
        ),
        ChannelCase(
            case_id="clem-reference-mini",
            suite=CLEMBENCH_LITE_SUITE,
            title="Reference-game miniature",
            source="clembench-referencegame-derived",
            purpose="Check reference description and target selection through a shared channel.",
            user_messages=[
                "Play a mini reference game. Director describes the private target; matcher selects one candidate."
            ],
            roles=[
                RoleSpec(
                    key="director",
                    name="Director",
                    private_context=(
                        "Private target is candidate B: a red square in the top-left cell. "
                        "Do not reveal hidden IDs other than the candidate label if needed."
                    ),
                    scripted_steps=[
                        ScriptStep(
                            publish=True,
                            content="The target is the candidate with a red square in the top-left cell.",
                        )
                    ],
                ),
                RoleSpec(
                    key="matcher",
                    name="Matcher",
                    private_context=(
                        "Candidates: A=blue circle center; B=red square top-left; C=green triangle bottom-right. "
                        "Select with FINAL_SELECTION: <label>."
                    ),
                    scripted_steps=[
                        ScriptStep(
                            publish=True,
                            content="FINAL_SELECTION: B",
                            required_transcript_markers=["red square", "top-left"],
                        )
                    ],
                ),
            ],
            expected_markers=["FINAL_SELECTION: B"],
            expected_speaker_roles=["director", "matcher"],
            min_agent_messages=2,
            max_agent_messages=3,
            max_agent_messages_per_ingress=3,
        ),
    ]


def all_cases() -> list[ChannelCase]:
    return native_cases() + clembench_lite_cases()


def selected_cases(args: argparse.Namespace) -> list[ChannelCase]:
    suites = args.suite or [NATIVE_SUITE]
    if "all" in suites:
        suites = list(ALL_SUITES)
    cases = [case for case in all_cases() if case.suite in suites]
    if args.case:
        by_id = {case.case_id: case for case in cases}
        missing = [case_id for case_id in args.case if case_id not in by_id]
        if missing:
            known = ", ".join(sorted(by_id))
            raise SystemExit(f"unknown case id(s): {', '.join(missing)}; known: {known}")
        cases = [by_id[case_id] for case_id in args.case]
    if args.max_cases is not None:
        cases = cases[: args.max_cases]
    return cases


def duplicate_values(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    duplicated: set[str] = set()
    for value in values:
        if value in seen:
            duplicated.add(value)
        seen.add(value)
    return sorted(duplicated)


def validate_case_definitions() -> None:
    cases = all_cases()
    issues: list[str] = []
    for case_id in duplicate_values(case.case_id for case in cases):
        issues.append(f"duplicate case id: {case_id}")
    for case in cases:
        if case.suite not in ALL_SUITES:
            issues.append(f"{case.case_id}: unknown suite {case.suite}")
        if not case.user_messages:
            issues.append(f"{case.case_id}: no user messages")
        role_keys = [role.key for role in case.roles]
        for role_key in duplicate_values(role_keys):
            issues.append(f"{case.case_id}: duplicate role key {role_key}")
        if case.min_agent_messages > case.max_agent_messages:
            issues.append(f"{case.case_id}: min_agent_messages exceeds max_agent_messages")
        for role in case.roles:
            if not role.scripted_steps:
                issues.append(f"{case.case_id}/{role.key}: no scripted steps")
    if issues:
        formatted = "\n- ".join(issues)
        raise SystemExit(f"invalid channel discussion cases:\n- {formatted}")


def transcript_text(messages: list[ChannelMessage]) -> str:
    return "\n".join(
        f"[{message.sequence}] {message.author_name}: {message.content}" for message in messages
    )


def contains_all(text: str, markers: list[str]) -> bool:
    normalized = text.lower()
    return all(marker.lower() in normalized for marker in markers)


def step_matches(step: ScriptStep, trigger: ChannelMessage, transcript: list[ChannelMessage]) -> bool:
    trigger_text = trigger.content.lower()
    full_text = transcript_text(transcript).lower()
    return all(marker.lower() in trigger_text for marker in step.required_trigger_markers) and all(
        marker.lower() in full_text for marker in step.required_transcript_markers
    )


def scripted_decision(
    case: ChannelCase,
    role: RoleSpec,
    trigger: ChannelMessage,
    transcript: list[ChannelMessage],
    used_steps: dict[str, set[int]],
) -> tuple[bool, str, str]:
    used = used_steps.setdefault(role.key, set())
    for index, step in enumerate(role.scripted_steps):
        if not step.repeatable and index in used:
            continue
        if step_matches(step, trigger, transcript):
            if not step.repeatable:
                used.add(index)
            return step.publish, step.content, step.note
    return False, "", "no scripted contribution matched"


def extract_json_object(text: str) -> dict[str, object] | None:
    stripped = text.strip()
    if not stripped:
        return None
    try:
        value = json.loads(stripped)
        return value if isinstance(value, dict) else None
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{.*\}", stripped, flags=re.DOTALL)
    if not match:
        return None
    try:
        value = json.loads(match.group(0))
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def is_runtime_decision_json(value: dict[str, object]) -> bool:
    return any(
        key in value
        for key in ("response_mode", "text", "tool_calls", "background_handoff", "next_state")
    )


def run_sinomo_prompt(args: argparse.Namespace, prompt: str) -> dict[str, object]:
    command = [
        str(args.sinomo_bin),
        "agent",
        "run",
        "--provider",
        args.provider,
        "--model",
        args.model,
        "--prompt",
        prompt,
    ]
    if args.account:
        command.extend(["--account", args.account])
    started = utc_now()
    if args.dry_run:
        return {
            "command": command,
            "started_at": started,
            "completed_at": utc_now(),
            "exit_code": 0,
            "stdout": (
                '{"response_mode":"silent","text":"","tool_calls":[],'
                '"background_handoff":null,"next_state":"idle"}'
            ),
            "stderr": "",
            "dry_run": True,
        }
    try:
        completed = subprocess.run(
            command,
            cwd=REPO_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=args.model_timeout_seconds,
        )
        return {
            "command": command,
            "started_at": started,
            "completed_at": utc_now(),
            "exit_code": completed.returncode,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
            "dry_run": False,
        }
    except subprocess.TimeoutExpired as source:
        return {
            "command": command,
            "started_at": started,
            "completed_at": utc_now(),
            "exit_code": 124,
            "stdout": source.stdout or "",
            "stderr": f"model call timed out after {args.model_timeout_seconds}s",
            "dry_run": False,
        }


def live_decision_prompt(
    case: ChannelCase,
    role: RoleSpec,
    trigger: ChannelMessage,
    transcript: list[ChannelMessage],
) -> str:
    role_messages = [
        message.content
        for message in transcript
        if message.author_kind == "agent" and message.author_key == role.key
    ]
    visible_final_tags = [
        tag
        for tag in [
            "FINAL_ANSWER",
            "FINAL_CODE",
            "FINAL_DECISION",
            "FINAL_GUESS",
            "FINAL_SELECTION",
            "SHARED_SLOTS",
        ]
        if tag in transcript_text(transcript)
    ]
    return json.dumps(
        {
            "instruction": (
                "You are one participant in a shared multi-agent channel. "
                "Use Sinomo's normal agent turn protocol. Decide whether this "
                "role should publish a channel message after the latest trigger. "
                "If you have nothing useful, non-duplicative, or timely to add, "
                "return response_mode `silent` with empty text. If you reply, "
                "make `text` the exact message to publish in the channel. "
                "If a final tag is already visible in the channel, stay silent "
                "unless you are correcting a clear error. "
                "If the case requires a tag such as FINAL_ANSWER, FINAL_GUESS, "
                "FINAL_SELECTION, SHARED_SLOTS, QUESTION, or ANSWER, preserve that exact format."
            ),
            "case_id": case.case_id,
            "case_title": case.title,
            "case_purpose": case.purpose,
            "role": {
                "key": role.key,
                "name": role.name,
                "private_context": role.private_context,
            },
            "latest_trigger": asdict(trigger),
            "visible_channel_transcript": [asdict(message) for message in transcript],
            "this_role_already_published": role_messages,
            "visible_final_tags": visible_final_tags,
            "max_message_length": 500,
        },
        indent=2,
    )


def live_decision(
    args: argparse.Namespace,
    case: ChannelCase,
    role: RoleSpec,
    trigger: ChannelMessage,
    transcript: list[ChannelMessage],
) -> tuple[bool, str, str, dict[str, object]]:
    invocation = run_sinomo_prompt(args, live_decision_prompt(case, role, trigger, transcript))
    stdout = str(invocation["stdout"]).strip()
    parsed = extract_json_object(stdout)
    if invocation["exit_code"] != 0:
        return False, "", f"model invocation failed: {invocation['stderr']}", invocation
    if parsed is not None and is_runtime_decision_json(parsed):
        response_mode = str(parsed.get("response_mode", "answer")).strip() or "answer"
        text = str(parsed.get("text", "")).strip()
        if response_mode == "silent" or not text:
            return False, "", f"runtime response_mode={response_mode}; no channel text", invocation
        return True, text[:1000], f"runtime response_mode={response_mode}", invocation
    if parsed is not None:
        return (
            bool(stdout),
            stdout[:500],
            "non-runtime JSON output treated as channel text",
            invocation,
        )
    return bool(stdout), stdout[:500], "plain runtime text treated as channel text", invocation


def evaluate_case(
    args: argparse.Namespace,
    case: ChannelCase,
    progress: ProgressWriter,
) -> dict[str, object]:
    progress.emit("case-start", case_id=case.case_id, mode=args.mode)
    transcript: list[ChannelMessage] = []
    decisions: list[PublicationDecisionRecord] = []
    invocations: list[dict[str, object]] = []
    used_steps: dict[str, set[int]] = {}
    next_sequence = 1
    emitted_agent_messages = 0
    limit_reached = False

    for user_message in case.user_messages:
        message = ChannelMessage(
            sequence=next_sequence,
            author_kind="user",
            author_key="user",
            author_name="User",
            content=user_message,
        )
        next_sequence += 1
        transcript.append(message)
        queue: list[ChannelMessage] = [message]

        while queue:
            trigger = queue.pop(0)
            for role in case.roles:
                if trigger.author_kind == "agent" and trigger.author_key == role.key:
                    continue
                if emitted_agent_messages >= case.max_agent_messages_per_ingress:
                    limit_reached = True
                    queue.clear()
                    break
                if args.mode == "fixture":
                    publish, content, note = scripted_decision(
                        case, role, trigger, transcript, used_steps
                    )
                    invocation = {
                        "command": [],
                        "started_at": utc_now(),
                        "completed_at": utc_now(),
                        "exit_code": 0,
                        "stdout": content,
                        "stderr": "",
                        "dry_run": True,
                    }
                else:
                    publish, content, note, invocation = live_decision(
                        args, case, role, trigger, transcript
                    )
                    invocations.append(invocation)
                decisions.append(
                    PublicationDecisionRecord(
                        case_id=case.case_id,
                        trigger_sequence=trigger.sequence,
                        role_key=role.key,
                        role_name=role.name,
                        publish=publish,
                        note=note,
                        raw_output=str(invocation.get("stdout", ""))[:2000],
                    )
                )
                if not publish:
                    continue
                if not content.strip():
                    continue
                agent_message = ChannelMessage(
                    sequence=next_sequence,
                    author_kind="agent",
                    author_key=role.key,
                    author_name=role.name,
                    content=content.strip(),
                )
                next_sequence += 1
                transcript.append(agent_message)
                queue.append(agent_message)
                emitted_agent_messages += 1
                if emitted_agent_messages >= case.max_agent_messages_per_ingress:
                    limit_reached = True
                    queue.clear()
                    break

    result = score_case(case, transcript, decisions, limit_reached)
    result.update(
        {
            "case_id": case.case_id,
            "suite": case.suite,
            "source": case.source,
            "title": case.title,
            "mode": args.mode,
            "provider": args.provider if args.mode == "run" else None,
            "model": args.model if args.mode == "run" else None,
            "transcript": [asdict(message) for message in transcript],
            "decisions": [asdict(decision) for decision in decisions],
            "invocations": invocation_summaries(invocations),
        }
    )
    progress.emit(
        "case-complete",
        case_id=case.case_id,
        passed=result["passed"],
        agent_message_count=result["agent_message_count"],
    )
    return result


def invocation_summaries(invocations: list[dict[str, object]]) -> list[dict[str, object]]:
    summaries: list[dict[str, object]] = []
    for invocation in invocations:
        command = list(invocation.get("command", []))
        if "--prompt" in command:
            prompt_index = command.index("--prompt") + 1
            if prompt_index < len(command):
                command[prompt_index] = "<prompt>"
        summaries.append(
            {
                "command": command,
                "exit_code": invocation.get("exit_code"),
                "started_at": invocation.get("started_at"),
                "completed_at": invocation.get("completed_at"),
                "stderr_excerpt": str(invocation.get("stderr", ""))[:2000],
                "stdout_excerpt": str(invocation.get("stdout", ""))[:2000],
                "dry_run": invocation.get("dry_run", False),
            }
        )
    return summaries


def score_case(
    case: ChannelCase,
    transcript: list[ChannelMessage],
    decisions: list[PublicationDecisionRecord],
    limit_reached: bool,
) -> dict[str, object]:
    text = transcript_text(transcript)
    marker_hits = {marker: marker.lower() in text.lower() for marker in case.expected_markers}
    forbidden_hits = {
        marker: marker.lower() in text.lower()
        for marker in case.forbidden_markers
        if marker.lower() in text.lower()
    }
    role_forbidden_hits: dict[str, list[str]] = {}
    for role_key, markers in case.role_forbidden_markers.items():
        role_text = "\n".join(
            message.content
            for message in transcript
            if message.author_kind == "agent" and message.author_key == role_key
        ).lower()
        hits = [marker for marker in markers if marker.lower() in role_text]
        if hits:
            role_forbidden_hits[role_key] = hits
    agent_message_count = sum(1 for message in transcript if message.author_kind == "agent")
    speaker_roles = sorted(
        {
            message.author_key
            for message in transcript
            if message.author_kind == "agent" and message.content.strip()
        }
    )
    missing_speaker_roles = sorted(set(case.expected_speaker_roles).difference(speaker_roles))
    silent_role_violations = sorted(set(case.expected_silent_roles).intersection(speaker_roles))
    issues: list[str] = []
    for marker, hit in marker_hits.items():
        if not hit:
            issues.append(f"missing marker: {marker}")
    for marker in forbidden_hits:
        issues.append(f"forbidden marker present: {marker}")
    for role_key, hits in role_forbidden_hits.items():
        issues.append(f"{role_key} used forbidden marker(s): {', '.join(hits)}")
    for role_key in missing_speaker_roles:
        issues.append(f"expected role did not speak: {role_key}")
    for role_key in silent_role_violations:
        issues.append(f"expected silent role spoke: {role_key}")
    if agent_message_count < case.min_agent_messages:
        issues.append(
            f"too few agent messages: {agent_message_count} < {case.min_agent_messages}"
        )
    if agent_message_count > case.max_agent_messages:
        issues.append(
            f"too many agent messages: {agent_message_count} > {case.max_agent_messages}"
        )
    if case.expect_limit_reached and not limit_reached:
        issues.append("expected loop limit was not reached")
    if not case.expect_limit_reached and limit_reached:
        issues.append("unexpected loop limit reached")
    return {
        "passed": not issues,
        "issues": issues,
        "marker_hits": marker_hits,
        "forbidden_hits": forbidden_hits,
        "role_forbidden_hits": role_forbidden_hits,
        "agent_message_count": agent_message_count,
        "speaker_roles": speaker_roles,
        "missing_speaker_roles": missing_speaker_roles,
        "silent_role_violations": silent_role_violations,
        "limit_reached": limit_reached,
    }


def aggregate_case_results(results: list[dict[str, object]]) -> dict[str, object]:
    passed = [result for result in results if result.get("passed") is True]
    return {
        "case_count": len(results),
        "passed_case_count": len(passed),
        "failed_case_count": len(results) - len(passed),
        "agent_message_count": sum(int(result.get("agent_message_count", 0)) for result in results),
        "failed_cases": [
            {"case_id": result.get("case_id"), "issues": result.get("issues", [])}
            for result in results
            if result.get("passed") is not True
        ],
    }


def build_plan(args: argparse.Namespace) -> dict[str, object]:
    cases = selected_cases(args)
    return {
        "schema_version": 1,
        "generated_at": utc_now(),
        "repo_root": str(REPO_ROOT),
        "results_dir": str(args.results_dir),
        "mode": args.mode,
        "provider": args.provider,
        "model": args.model,
        "suite": args.suite or [NATIVE_SUITE],
        "case_count": len(cases),
        "cases": [
            {
                "case_id": case.case_id,
                "suite": case.suite,
                "title": case.title,
                "source": case.source,
                "purpose": case.purpose,
                "role_count": len(case.roles),
                "max_agent_messages_per_ingress": case.max_agent_messages_per_ingress,
            }
            for case in cases
        ],
        "official_clembench_adapter": {
            "mode": "clembench",
            "requires": ["clembench checkout", "clem CLI from clemcore"],
            "default_game": args.clembench_game,
            "generated_backend": "sinomo_api.py",
            "generated_model_registry": "model_registry.json",
            "example_command": " ".join(
                [
                    sys.executable,
                    str(Path(__file__).relative_to(REPO_ROOT)),
                    "--mode",
                    "clembench",
                    "--clembench-root",
                    "/path/to/clembench",
                    "--clembench-game",
                    args.clembench_game,
                    "--provider",
                    args.provider,
                    "--model",
                    args.model,
                ]
            ),
        },
    }


def write_json(path: Path | None, payload: dict[str, object]) -> None:
    if not path:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def resolve_repo_relative_path(path: Path) -> Path:
    return path if path.is_absolute() else REPO_ROOT / path


def run_cases(args: argparse.Namespace) -> int:
    cases = selected_cases(args)
    progress = ProgressWriter(args.progress_out)
    results = [evaluate_case(args, case, progress) for case in cases]
    aggregate = aggregate_case_results(results)
    passed = aggregate["failed_case_count"] == 0
    summary: dict[str, object] = {
        **build_plan(args),
        **aggregate,
        "status": "passed" if passed else "failed",
        "evaluated_at": utc_now(),
        "results": results,
    }
    write_json(args.summary_out or args.results_dir / "summary.json", summary)
    print(json.dumps(summary, indent=2))
    return 0 if passed else 1


def sinomo_backend_source() -> str:
    return r'''
from __future__ import annotations

import json
import os
import subprocess
from typing import Dict, List, Tuple, Any

from clemcore import backends


class Sinomo(backends.Backend):
    def get_model_for(self, model_spec: backends.ModelSpec) -> backends.Model:
        return SinomoModel(model_spec)


class SinomoModel(backends.Model):
    def __init__(self, model_spec: backends.ModelSpec):
        super().__init__(model_spec)
        self.set_gen_args(temperature=float(getattr(model_spec, "temperature", 0.0)), max_tokens=int(getattr(model_spec, "max_tokens", 1024)))

    def generate_response(self, messages: List[Dict]) -> Tuple[Any, Any, str]:
        provider = getattr(self.model_spec, "provider", os.environ.get("SINOMO_AGENT_PROVIDER", "codex"))
        model = getattr(self.model_spec, "model_id", os.environ.get("SINOMO_AGENT_MODEL", self.model_spec.model_name))
        account = getattr(self.model_spec, "account", os.environ.get("SINOMO_AGENT_ACCOUNT", ""))
        prompt = {
            "instruction": (
                "You are acting as a clembench game player through Sinomo. "
                "Continue the dialogue by returning only the next assistant message. "
                "Follow the game format exactly and do not add commentary."
            ),
            "messages": messages,
        }
        command = [
            os.environ.get("SINOMO_BIN", "sinomo"),
            "agent",
            "run",
            "--provider",
            provider,
            "--model",
            model,
            "--prompt",
            json.dumps(prompt, ensure_ascii=False),
        ]
        if account:
            command.extend(["--account", account])
        completed = subprocess.run(command, text=True, capture_output=True, check=False)
        response = {
            "command": [part if part != json.dumps(prompt, ensure_ascii=False) else "<prompt>" for part in command],
            "exit_code": completed.returncode,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
        }
        if completed.returncode != 0:
            response_text = "ERROR: Sinomo backend call failed"
        else:
            response_text = completed.stdout.strip()
        return prompt, response, response_text
'''


def copy_clembench_game_subset(
    clembench_root: Path,
    workspace: Path,
    game_name: str,
    instances_name: str,
    max_experiments: int,
    max_instances: int,
) -> str:
    source_game = clembench_root / game_name
    if not source_game.exists():
        raise SystemExit(f"clembench game directory does not exist: {source_game}")
    target_game = workspace / game_name
    if target_game.exists():
        shutil.rmtree(target_game)
    shutil.copytree(
        source_game,
        target_game,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "results"),
    )

    source_instances = target_game / "in" / f"{instances_name}.json"
    if not source_instances.exists():
        raise SystemExit(f"clembench instances file does not exist: {source_instances}")
    payload = json.loads(source_instances.read_text(encoding="utf-8"))
    experiments = payload.get("experiments")
    if not isinstance(experiments, list):
        raise SystemExit(f"unsupported clembench instances shape: {source_instances}")
    trimmed_experiments = []
    for experiment in experiments[:max_experiments]:
        if not isinstance(experiment, dict):
            continue
        trimmed = dict(experiment)
        game_instances = trimmed.get("game_instances")
        if isinstance(game_instances, list):
            trimmed["game_instances"] = game_instances[:max_instances]
        trimmed_experiments.append(trimmed)
    subset_name = "instances_sinomo_subset"
    subset_payload = {**payload, "experiments": trimmed_experiments}
    (target_game / "in" / f"{subset_name}.json").write_text(
        json.dumps(subset_payload, indent=2) + "\n",
        encoding="utf-8",
    )

    clemgame_path = target_game / "clemgame.json"
    clemgame = json.loads(clemgame_path.read_text(encoding="utf-8"))
    if isinstance(clemgame, dict):
        clemgame["instances"] = subset_name
        selected_game_name = str(clemgame.get("game_name", game_name))
    elif isinstance(clemgame, list):
        selected_game_name = game_name
        for entry in clemgame:
            if isinstance(entry, dict) and entry.get("game_name") == game_name:
                entry["instances"] = subset_name
                selected_game_name = game_name
                break
    else:
        raise SystemExit(f"unsupported clemgame shape: {clemgame_path}")
    clemgame_path.write_text(json.dumps(clemgame, indent=2) + "\n", encoding="utf-8")
    return selected_game_name


def prepare_clembench_workspace(args: argparse.Namespace) -> tuple[Path, str]:
    clembench_root = args.clembench_root
    if clembench_root is None:
        raise SystemExit("--clembench-root is required for --mode clembench")
    clembench_root = clembench_root.resolve()
    if not clembench_root.exists():
        raise SystemExit(f"clembench root does not exist: {clembench_root}")
    workspace = args.results_dir / "official-clembench-workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    selected_game_name = copy_clembench_game_subset(
        clembench_root=clembench_root,
        workspace=workspace,
        game_name=args.clembench_game,
        instances_name=args.clembench_instances,
        max_experiments=args.official_max_experiments,
        max_instances=args.official_max_instances,
    )
    (workspace / "sinomo_api.py").write_text(sinomo_backend_source(), encoding="utf-8")
    model_name = args.clembench_model_name or f"sinomo-{args.model}"
    model_registry = [
        {
            "model_name": model_name,
            "model_id": args.model,
            "backend": "sinomo",
            "provider": args.provider,
            "account": args.account or "",
            "temperature": 0.0,
            "max_tokens": 1024,
        }
    ]
    (workspace / "model_registry.json").write_text(
        json.dumps(model_registry, indent=2) + "\n",
        encoding="utf-8",
    )
    return workspace, selected_game_name


def run_official_clembench(args: argparse.Namespace) -> int:
    workspace, selected_game_name = prepare_clembench_workspace(args)
    model_name = args.clembench_model_name or f"sinomo-{args.model}"
    results_path = args.results_dir / "official-clembench-results"
    clem_bin = Path(args.clem_bin)
    clem_command = (
        str(resolve_repo_relative_path(clem_bin).resolve())
        if clem_bin.parent != Path(".")
        else str(args.clem_bin)
    )
    command = [
        clem_command,
        "run",
        "-g",
        selected_game_name,
        "-m",
        model_name,
        "-r",
        str(results_path),
    ]
    score_command = [
        clem_command,
        "score",
        "-g",
        selected_game_name,
        "-m",
        model_name,
        "-r",
        str(results_path),
    ]
    summary: dict[str, object] = {
        "schema_version": 1,
        "generated_at": utc_now(),
        "mode": "clembench",
        "workspace": str(workspace),
        "clembench_root": str(args.clembench_root),
        "game": selected_game_name,
        "model_name": model_name,
        "provider": args.provider,
        "model": args.model,
        "command": command,
        "score_command": score_command,
        "results_path": str(results_path),
    }
    if args.dry_run:
        summary.update({"status": "dry-run", "exit_code": 0, "stdout": "", "stderr": ""})
        write_json(args.summary_out or args.results_dir / "official-clembench-summary.json", summary)
        print(json.dumps(summary, indent=2))
        return 0
    env = os.environ.copy()
    env.setdefault("SINOMO_BIN", str(resolve_repo_relative_path(args.sinomo_bin).resolve()))
    env.setdefault("SINOMO_AGENT_PROVIDER", args.provider)
    env.setdefault("SINOMO_AGENT_MODEL", args.model)
    try:
        completed = subprocess.run(
            command,
            cwd=workspace,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=args.official_timeout_seconds,
            env=env,
        )
    except FileNotFoundError as source:
        summary.update(
            {
                "status": "failed",
                "exit_code": 127,
                "stdout_excerpt": "",
                "stderr_excerpt": str(source),
                "completed_at": utc_now(),
            }
        )
        write_json(args.summary_out or args.results_dir / "official-clembench-summary.json", summary)
        print(json.dumps(summary, indent=2))
        return 127
    except subprocess.TimeoutExpired as source:
        summary.update(
            {
                "status": "failed",
                "exit_code": 124,
                "stdout_excerpt": str(source.stdout or "")[-8000:],
                "stderr_excerpt": f"official clembench run timed out after {args.official_timeout_seconds}s",
                "completed_at": utc_now(),
            }
        )
        write_json(args.summary_out or args.results_dir / "official-clembench-summary.json", summary)
        print(json.dumps(summary, indent=2))
        return 124
    summary.update(
        {
            "status": "passed" if completed.returncode == 0 else "failed",
            "exit_code": completed.returncode,
            "stdout_excerpt": completed.stdout[-8000:],
            "stderr_excerpt": completed.stderr[-8000:],
            "completed_at": utc_now(),
        }
    )
    if completed.returncode == 0:
        try:
            score_completed = subprocess.run(
                score_command,
                cwd=workspace,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
                timeout=args.official_timeout_seconds,
                env=env,
            )
            summary.update(
                {
                    "score_status": "passed" if score_completed.returncode == 0 else "failed",
                    "score_exit_code": score_completed.returncode,
                    "score_stdout_excerpt": score_completed.stdout[-8000:],
                    "score_stderr_excerpt": score_completed.stderr[-8000:],
                }
            )
            if score_completed.returncode != 0:
                summary["status"] = "failed"
                summary["exit_code"] = score_completed.returncode
        except FileNotFoundError as source:
            summary.update(
                {
                    "status": "failed",
                    "exit_code": 127,
                    "score_status": "failed",
                    "score_exit_code": 127,
                    "score_stdout_excerpt": "",
                    "score_stderr_excerpt": str(source),
                }
            )
        except subprocess.TimeoutExpired as source:
            summary.update(
                {
                    "status": "failed",
                    "exit_code": 124,
                    "score_status": "failed",
                    "score_exit_code": 124,
                    "score_stdout_excerpt": str(source.stdout or "")[-8000:],
                    "score_stderr_excerpt": f"official clembench scoring timed out after {args.official_timeout_seconds}s",
                }
            )
    write_json(args.summary_out or args.results_dir / "official-clembench-summary.json", summary)
    print(json.dumps(summary, indent=2))
    return int(summary.get("exit_code", completed.returncode))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["plan", "fixture", "run", "clembench"], default="plan")
    parser.add_argument("--suite", action="append", choices=[*ALL_SUITES, "all"], default=[])
    parser.add_argument("--case", action="append", default=[])
    parser.add_argument("--max-cases", type=int)
    parser.add_argument("--provider", default=os.environ.get("SINOMO_AGENT_PROVIDER", "codex"))
    parser.add_argument("--model", default=os.environ.get("SINOMO_AGENT_MODEL", "gpt-5.4-mini"))
    parser.add_argument("--account")
    parser.add_argument("--sinomo-bin", type=Path, default=REPO_ROOT / "target" / "debug" / "sinomo")
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--summary-out", type=Path)
    parser.add_argument("--progress-out", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--model-timeout-seconds", type=int, default=300)
    parser.add_argument("--clembench-root", type=Path)
    parser.add_argument("--clembench-game", default="privateshared")
    parser.add_argument("--clembench-instances", default="instances")
    parser.add_argument("--clembench-model-name")
    parser.add_argument("--clem-bin", default=os.environ.get("CLEM_BIN", "clem"))
    parser.add_argument("--official-max-experiments", type=int, default=1)
    parser.add_argument("--official-max-instances", type=int, default=1)
    parser.add_argument("--official-timeout-seconds", type=int, default=1800)
    return parser.parse_args()


def main() -> int:
    validate_case_definitions()
    args = parse_args()
    if args.mode == "plan":
        plan = build_plan(args)
        write_json(args.summary_out, plan)
        print(json.dumps(plan, indent=2))
        return 0
    if args.mode == "clembench":
        return run_official_clembench(args)
    return run_cases(args)


if __name__ == "__main__":
    raise SystemExit(main())
