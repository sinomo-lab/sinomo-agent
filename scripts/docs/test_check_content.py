import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import check_content


class ContentRules(unittest.TestCase):
    def test_change_narration_is_rejected_even_without_a_date(self):
        for text in (
            "October update changed the reader into a writer.",
            "The patch added a retry queue.",
            "This refactor replaced the old owner.",
        ):
            with self.subTest(text=text):
                self.assertTrue(any(rule == "development-log" for _, rule, _ in check_content.violations(text)))

    def test_development_dates_are_rejected(self):
        for text in (
            "Reviewed 2026-10-23", "Updated 2026/10/23", "Completed 23/10/2026",
            "Last reviewed October 23", "Tested 23 October", "Research date: Sept. 2026",
            "Validated in report-20261023.md", "````\nUpdated 2026-10-23T10:00:00Z\n````",
            "Something is done in 2026-08-08.",
        ):
            with self.subTest(text=text):
                self.assertTrue(any(rule == "development-date" for _, rule, _ in check_content.violations(text)))

    def test_commit_citations_and_pinned_urls_are_rejected(self):
        for text in (
            "Fixed in `a1b2c3d4`", "commit deadbeef", "revision\n123abcd",
            "- `abcd1234` — restore write progress",
            "git -C repo show abcd1234:path", "git checkout --detach abcd1234",
            "https://example.test/project/commit/0123456789abcdef0123456789abcdef01234567",
            "https://example.test/project/tree/abcd1234/file", "dataset/resolve/abcd1234/file",
        ):
            with self.subTest(text=text):
                self.assertTrue(any(rule == "commit-reference" for _, rule, _ in check_content.violations(text)))

    def test_agent_provenance_is_rejected(self):
        for text in (
            "~/.codex/sessions/run.jsonl", "rollout_summaries/run.md", "thread_id: value",
            "term_01234567-89ab-cdef-0123-456789abcdef",
            "session `01234567-89ab-cdef-0123-456789abcdef`",
            "As discussed in the previous agent conversation, keep the old behavior.",
        ):
            with self.subTest(text=text):
                self.assertTrue(any(rule == "agent-history" for _, rule, _ in check_content.violations(text)))

    def test_product_concepts_and_generic_procedures_are_valid(self):
        text = """Commit after W receipts; N is desired placement.
Agent sessions retain conversation history and transcript timestamps.
May use R2 directly. The queue holds 2048 bytes. Version 1.2.3 is supported.
git rev-parse HEAD
git checkout "$SOURCE_REVISION"
QUINT_RUN_SEED=0x51a020261006
{"time": "<RFC3339 timestamp>"}
https://aclanthology.org/2025.acl-long.421/
| Question IDs | `e47becba`, `118b2229` |
Transformer-XL (2019, ACL); Mamba (2024, COLM).
Conference proceedings published July 2024.
`anthropic-version: 2023-06-01`
Stateless MCP protocol version `2026-07-28`.
The retry owner preserves an unfinished operation across cancellation.
The conference's October proceedings describe the algorithm.
"""
        self.assertEqual(check_content.violations(text), [])

    def test_line_numbers_and_filename_checks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bad = root / "validation-20261023.md"
            bad.write_text("# Guide\n\nFixed in `abcd1234`.\n")
            with patch.object(check_content, "REPO_ROOT", root):
                findings = check_content.validate([bad])
            self.assertTrue(any("filename: development-date:" in line for line in findings))
            self.assertTrue(any(":3: commit-reference:" in line for line in findings))

    def test_cli_fails_for_content_and_succeeds_for_standalone_text(self):
        with tempfile.TemporaryDirectory(prefix=".docs-test-", dir=check_content.REPO_ROOT) as directory:
            doc = Path(directory) / "guide.md"
            doc.write_text("# Guide\nReviewed 2026-10-23.\n")
            command = [sys.executable, str(Path(check_content.__file__)), str(doc)]
            bad = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(bad.returncode, 1)
            self.assertIn("date:", bad.stderr)
            doc.write_text("# Guide\nThe owner validates the original operation.\n")
            good = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(good.returncode, 0, good.stderr)


if __name__ == "__main__":
    unittest.main()
