#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


AGENT_DIR = Path(__file__).resolve().parent
if str(AGENT_DIR) not in sys.path:
    sys.path.insert(0, str(AGENT_DIR))

import eval_isolation as ISOLATION  # noqa: E402


def write_text(root: Path, relative: str, content: str) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


class EvalIsolationTests(unittest.TestCase):
    def test_environment_and_provider_copy_are_allowlisted(self) -> None:
        with tempfile.TemporaryDirectory(prefix="sinomo-eval-isolation-test-") as temp:
            base = Path(temp)
            operational = base / "operational"
            scratch = base / "scratch"
            write_text(
                operational,
                "configs/lm/providers.json",
                json.dumps(
                    {
                        "key": "lm.providers",
                        "updated_at": "2026-07-18T00:00:00Z",
                        "value": {
                            "accounts": [
                                {
                                    "provider": "codex",
                                    "account": "benchmark",
                                    "api_key": "selected-inline-token",
                                },
                                {
                                    "provider": "openai",
                                    "account": "operational",
                                    "api_key": "unrelated-inline-token",
                                },
                            ]
                        },
                    }
                ),
            )
            write_text(
                operational,
                "configs/lm/credentials/codex.json",
                '{"access_token":"copied-token"}',
            )
            write_text(
                operational,
                "providers/codex/credentials.json",
                '{"access_token":"stale-legacy-token"}',
            )
            forbidden = [
                "identity/local-permissions.sobj",
                "cluster/state.sobj",
                "dfs/content/blob",
                "memory/index/catalog.json",
                "events/agent/traces/live.jsonl",
                "agents/main-agent/session.json",
            ]
            for relative in forbidden:
                write_text(operational, relative, "operational-sentinel")

            parent_environment = {
                "PATH": os.environ.get("PATH", ""),
                "LANG": "C.UTF-8",
                "SAFE_PROVIDER_TOKEN": "explicit-token",
                "SINOMO_DATA_DIR": str(operational),
                "SINOMO_CODEX_CREDENTIALS_FILE": str(
                    operational / "configs/lm/credentials/codex.json"
                ),
                "SINOMO_AGENT_HOME_HOST_PATH": str(operational / "agent-home"),
                "SINOMO_SANDBOX_SIMPLE_READ_PATHS": "/",
                "SINOMO_DAEMON_DEFAULT_HOST": "live-daemon.invalid",
                "UNRELATED_SECRET": "must-not-pass",
            }
            case_root: Path | None = None
            with mock.patch.dict(os.environ, parent_environment, clear=True):
                with ISOLATION.isolated_sinomo_case(
                    "containment",
                    scratch_root=scratch,
                    config_source_data_dir=operational,
                    provider="codex",
                    account="benchmark",
                    copy_provider_config=True,
                    pass_environment=["SAFE_PROVIDER_TOKEN"],
                ) as case:
                    case_root = case.root
                    environment = case.environment
                    self.assertEqual(environment["SINOMO_DATA_DIR"], str(case.data_dir))
                    self.assertEqual(environment["HOME"], str(case.home_dir))
                    self.assertEqual(environment["USERPROFILE"], str(case.home_dir))
                    self.assertEqual(environment["SAFE_PROVIDER_TOKEN"], "explicit-token")
                    self.assertNotIn("SINOMO_CODEX_CREDENTIALS_FILE", environment)
                    self.assertNotIn("SINOMO_AGENT_HOME_HOST_PATH", environment)
                    self.assertNotIn("SINOMO_SANDBOX_SIMPLE_READ_PATHS", environment)
                    self.assertNotIn("SINOMO_DAEMON_DEFAULT_HOST", environment)
                    self.assertNotIn("UNRELATED_SECRET", environment)

                    expected_copies = {
                        "configs/lm/providers.json",
                        "configs/lm/credentials/codex.json",
                    }
                    self.assertEqual(set(case.copied_provider_state), expected_copies)
                    for relative in expected_copies:
                        self.assertTrue((case.data_dir / relative).is_file())
                    for relative in forbidden:
                        self.assertFalse((case.data_dir / relative).exists())
                    self.assertEqual(
                        (case.data_dir / "configs/lm/credentials/codex.json").read_text(
                            encoding="utf-8"
                        ),
                        '{"access_token":"copied-token"}',
                    )
                    filtered_config = (
                        case.data_dir / "configs/lm/providers.json"
                    ).read_text(encoding="utf-8")
                    self.assertIn("selected-inline-token", filtered_config)
                    self.assertNotIn("unrelated-inline-token", filtered_config)
                    self.assertNotIn('"provider": "openai"', filtered_config)
                    self.assertFalse(
                        (case.data_dir / "providers/codex/credentials.json").exists()
                    )
                    self.assertFalse(
                        case.manifest()["operational_data_dir_used_as_runtime"]
                    )
                    self.assertFalse(
                        case.manifest()["non_provider_operational_state_copied"]
                    )
                    for directory in (
                        case.data_dir,
                        case.corpus_dir,
                        case.workspace_dir,
                        case.home_dir,
                        case.xdg_config_dir,
                        case.xdg_cache_dir,
                        case.xdg_data_dir,
                        case.xdg_state_dir,
                        case.temp_dir,
                    ):
                        self.assertTrue(directory.resolve().is_relative_to(case.root.resolve()))

            assert case_root is not None
            self.assertFalse(case_root.exists())
            for relative in forbidden:
                self.assertEqual(
                    (operational / relative).read_text(encoding="utf-8"),
                    "operational-sentinel",
                )

    def test_provider_config_is_not_copied_without_explicit_account(self) -> None:
        with tempfile.TemporaryDirectory(prefix="sinomo-eval-no-account-test-") as temp:
            base = Path(temp)
            operational = base / "operational"
            scratch = base / "scratch"
            write_text(
                operational,
                "configs/lm/providers.json",
                '{"accounts":[{"provider":"other","api_key":"unrelated"}]}',
            )
            write_text(
                operational,
                "configs/lm/credentials/codex.json",
                '{"access_token":"selected-provider"}',
            )

            with ISOLATION.isolated_sinomo_case(
                "no-account",
                scratch_root=scratch,
                config_source_data_dir=operational,
                provider="codex",
                copy_provider_config=True,
            ) as case:
                self.assertEqual(
                    case.copied_provider_state,
                    ("configs/lm/credentials/codex.json",),
                )
                self.assertFalse(
                    (case.data_dir / "configs/lm/providers.json").exists()
                )

    def test_cleanup_runs_after_success_and_body_exception(self) -> None:
        with tempfile.TemporaryDirectory(prefix="sinomo-eval-cleanup-test-") as temp:
            base = Path(temp)
            operational = base / "operational"
            scratch = base / "scratch"
            operational.mkdir()

            with ISOLATION.isolated_sinomo_case(
                "success",
                scratch_root=scratch,
                config_source_data_dir=operational,
            ) as case:
                success_root = case.root
                write_text(case.data_dir, "probe", "success")
            self.assertFalse(success_root.exists())

            exceptional_root: Path | None = None
            with self.assertRaisesRegex(RuntimeError, "injected failure"):
                with ISOLATION.isolated_sinomo_case(
                    "exception",
                    scratch_root=scratch,
                    config_source_data_dir=operational,
                ) as case:
                    exceptional_root = case.root
                    write_text(case.data_dir, "probe", "exception")
                    raise RuntimeError("injected failure")
            assert exceptional_root is not None
            self.assertFalse(exceptional_root.exists())

    def test_setup_exception_does_not_leak_generated_root(self) -> None:
        with tempfile.TemporaryDirectory(prefix="sinomo-eval-setup-cleanup-test-") as temp:
            base = Path(temp)
            operational = base / "operational"
            scratch = base / "scratch"
            scratch.mkdir()
            target = write_text(base, "outside-credential.json", "token")
            credential = operational / "configs/lm/credentials/codex.json"
            credential.parent.mkdir(parents=True)
            try:
                credential.symlink_to(target)
            except OSError as source:
                self.skipTest(f"symlinks unavailable: {source}")

            with self.assertRaisesRegex(ValueError, "regular non-symlink"):
                with ISOLATION.isolated_sinomo_case(
                    "setup-failure",
                    scratch_root=scratch,
                    config_source_data_dir=operational,
                    provider="codex",
                    copy_provider_config=True,
                ):
                    self.fail("invalid provider state must fail before yielding")
            self.assertEqual(list(scratch.iterdir()), [])

    def test_setup_base_exception_does_not_leak_generated_root(self) -> None:
        with tempfile.TemporaryDirectory(prefix="sinomo-eval-interrupt-cleanup-test-") as temp:
            base = Path(temp)
            operational = base / "operational"
            scratch = base / "scratch"
            operational.mkdir()
            scratch.mkdir()

            with mock.patch.object(
                ISOLATION,
                "copy_selected_provider_state",
                side_effect=KeyboardInterrupt,
            ):
                with self.assertRaises(KeyboardInterrupt):
                    with ISOLATION.isolated_sinomo_case(
                        "setup-interrupt",
                        scratch_root=scratch,
                        config_source_data_dir=operational,
                        provider="codex",
                        copy_provider_config=True,
                    ):
                        self.fail("interrupted setup must not yield")
            self.assertEqual(list(scratch.iterdir()), [])

    def test_interrupt_while_resolving_generated_root_does_not_leak_it(self) -> None:
        with tempfile.TemporaryDirectory(prefix="sinomo-eval-root-interrupt-test-") as temp:
            base = Path(temp)
            operational = base / "operational"
            scratch = base / "scratch"
            operational.mkdir()
            scratch.mkdir()
            original_resolve = Path.resolve

            def interrupt_generated_root(path: Path, strict: bool = False) -> Path:
                if path.name.startswith("sinomo-memory-eval-root-interrupt-"):
                    raise KeyboardInterrupt
                return original_resolve(path, strict=strict)

            with mock.patch.object(Path, "resolve", new=interrupt_generated_root):
                with self.assertRaises(KeyboardInterrupt):
                    with ISOLATION.isolated_sinomo_case(
                        "root-interrupt",
                        scratch_root=scratch,
                        config_source_data_dir=operational,
                    ):
                        self.fail("interrupted setup must not yield")
            self.assertEqual(list(scratch.iterdir()), [])

    def test_disabled_selected_provider_account_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "is disabled"):
            ISOLATION._selected_account_config(
                {
                    "accounts": [
                        {
                            "provider": "codex",
                            "account": "benchmark",
                            "enabled": False,
                            "api_key": "must-not-be-used",
                        }
                    ]
                },
                provider="codex",
                account="benchmark",
            )

    def test_provider_id_cannot_escape_copy_allowlist(self) -> None:
        with tempfile.TemporaryDirectory(prefix="sinomo-eval-provider-id-test-") as temp:
            root = Path(temp)
            for provider in (".", "..", "nested/provider"):
                with self.subTest(provider=provider):
                    with self.assertRaisesRegex(ValueError, "provider id"):
                        ISOLATION.copy_selected_provider_state(
                            root / "source", root / "destination", provider
                        )

    def test_cases_get_unique_roots_and_isolated_subprocess_environment(self) -> None:
        with tempfile.TemporaryDirectory(prefix="sinomo-eval-unique-test-") as temp:
            base = Path(temp)
            operational = base / "operational"
            scratch = base / "scratch"
            operational.mkdir()
            roots: list[Path] = []

            with ISOLATION.isolated_sinomo_case(
                "same-case-id",
                scratch_root=scratch,
                config_source_data_dir=operational,
            ) as first:
                with ISOLATION.isolated_sinomo_case(
                    "same-case-id",
                    scratch_root=scratch,
                    config_source_data_dir=operational,
                ) as second:
                    roots.extend([first.root, second.root])
                    self.assertNotEqual(first.root, second.root)
                    command = [
                        sys.executable,
                        "-c",
                        (
                            "import json, os; from pathlib import Path; "
                            "data=Path(os.environ['SINOMO_DATA_DIR']); "
                            "(data/'child-probe').write_text('ok'); "
                            "print(json.dumps({'cwd': os.getcwd(), "
                            "'data': str(data), 'home': os.environ['HOME']}))"
                        ),
                    ]
                    invocation = ISOLATION.run_isolated_process(
                        command, first, timeout_seconds=10
                    )
                    self.assertEqual(invocation["exit_code"], 0)
                    self.assertFalse(invocation["timed_out"])
                    child = json.loads(str(invocation["stdout"]))
                    self.assertEqual(Path(child["cwd"]), first.workspace_dir)
                    self.assertEqual(Path(child["data"]), first.data_dir)
                    self.assertEqual(Path(child["home"]), first.home_dir)
                    self.assertEqual(
                        (first.data_dir / "child-probe").read_text(encoding="utf-8"),
                        "ok",
                    )

            self.assertEqual(len(set(roots)), 2)
            self.assertTrue(all(not root.exists() for root in roots))

    def test_sinomo_environment_cannot_be_explicitly_inherited(self) -> None:
        for name in (
            "SINOMO_DATA_DIR",
            "SINOMO_OPENAI_API_KEY",
            "SINOMO_PYTORCH_ENDPOINT",
        ):
            with self.subTest(name=name):
                with self.assertRaisesRegex(ValueError, "controlled by benchmark isolation"):
                    ISOLATION.validate_explicit_environment_names([name])

        for name in (
            "CODEX_HOME",
            "AWS_CONFIG_FILE",
            "GOOGLE_APPLICATION_CREDENTIALS",
            "PYTHONPATH",
        ):
            with self.subTest(name=name):
                with self.assertRaisesRegex(ValueError, "path-bearing environment"):
                    ISOLATION.validate_explicit_environment_names([name])

    def test_operational_scratch_and_output_overlaps_fail_before_writes(self) -> None:
        with tempfile.TemporaryDirectory(prefix="sinomo-eval-overlap-test-") as temp:
            base = Path(temp)
            operational = base / "operational"
            operational.mkdir()

            nested_scratch = operational / "benchmark-scratch"
            with self.assertRaisesRegex(ValueError, "overlaps operational data"):
                ISOLATION.validate_isolation_boundaries(
                    scratch_root=nested_scratch,
                    config_source_data_dir=operational,
                    protected_paths=[],
                )
            self.assertFalse(nested_scratch.exists())

            scratch = base / "scratch"
            with self.assertRaisesRegex(ValueError, "overlaps operational data"):
                ISOLATION.validate_isolation_boundaries(
                    scratch_root=scratch,
                    config_source_data_dir=operational,
                    protected_paths=[operational / "results.json"],
                )
            with self.assertRaisesRegex(ValueError, "overlaps scratch root"):
                ISOLATION.validate_isolation_boundaries(
                    scratch_root=scratch,
                    config_source_data_dir=operational,
                    protected_paths=[scratch / "results.json"],
                )
            with self.assertRaisesRegex(ValueError, "artifact paths overlap"):
                ISOLATION.validate_pairwise_disjoint_paths(
                    [base / "dataset.json", base / "dataset.json"]
                )
            dataset = write_text(base, "dataset.json", "fixture")
            hardlink = base / "hardlinked-output.json"
            try:
                os.link(dataset, hardlink)
            except OSError as source:
                self.skipTest(f"hardlinks unavailable: {source}")
            with self.assertRaisesRegex(ValueError, "artifact paths overlap"):
                ISOLATION.validate_pairwise_disjoint_paths([dataset, hardlink])

    def test_timeout_terminates_child_before_case_cleanup(self) -> None:
        with tempfile.TemporaryDirectory(prefix="sinomo-eval-timeout-test-") as temp:
            base = Path(temp)
            operational = base / "operational"
            scratch = base / "scratch"
            operational.mkdir()
            with ISOLATION.isolated_sinomo_case(
                "timeout",
                scratch_root=scratch,
                config_source_data_dir=operational,
            ) as case:
                case_root = case.root
                invocation = ISOLATION.run_isolated_process(
                    [sys.executable, "-c", "import time; time.sleep(60)"],
                    case,
                    timeout_seconds=0.05,
                )
                self.assertTrue(invocation["timed_out"])
                self.assertNotEqual(invocation["exit_code"], 0)
            self.assertFalse(case_root.exists())


if __name__ == "__main__":
    unittest.main()
