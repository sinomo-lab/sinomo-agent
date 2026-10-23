import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import check_scope as scope


class ScopeReview(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name).resolve()
        self.run_git("init", "-q")
        self.run_git("config", "user.name", "Scope fixture")
        self.run_git("config", "user.email", "scope@example.invalid")
        (self.repo / ".gitignore").write_text("target/\n")
        (self.repo / "guide.md").write_text("Stable intent, ownership and major flows.\n\nA development log.\n")
        (self.repo / "other.txt").write_text("unrelated\n")
        self.run_git("add", ".gitignore", "guide.md", "other.txt")
        self.run_git("-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null",
                     "commit", "-q", "--no-verify", "-m", "fixture")

    def run_git(self, *args):
        return subprocess.check_output(["git", "-C", str(self.repo), *args], stderr=subprocess.PIPE)

    def capture(self, paths=None):
        self.snapshot = scope.capture(self.repo, paths or ["guide.md"], {"cleanup": "Remove dev logs while preserving valid architecture text"})
        self.raw = scope.encoded(self.snapshot)

    def units(self):
        return scope.changes(self.repo, self.snapshot)

    def reviewed(self, units):
        review = scope.draft(self.raw, units)
        for entry in review["changes"]:
            entry.update(basis="requested", requirement="cleanup",
                         reason="Remove the development-log paragraph.",
                         intent_effect="preserves")
        return review

    def errors(self, units, review):
        return scope.verify(self.raw, self.snapshot, units, review)

    def test_small_extra_edit_is_separate_from_the_requested_cleanup(self):
        self.capture()
        (self.repo / "guide.md").write_text("Stable purpose, ownership and major flows.\n\n")
        units = self.units()
        self.assertEqual(len(units), 2)
        requested = [unit for unit in units if "development log" in unit["old"]]
        self.assertEqual(len(requested), 1)
        self.assertTrue(any("unaccounted changes: 1" in error for error in self.errors(units, self.reviewed(requested))))

    def test_changed_source_invalidates_a_completed_review(self):
        self.capture()
        (self.repo / "guide.md").write_text("Stable intent, ownership and major flows.\n\n")
        units = self.units()
        review = self.reviewed(units)
        self.assertEqual(self.errors(units, review), [])
        (self.repo / "guide.md").write_text("Stable purpose, ownership and major flows.\n\n")
        self.assertTrue(any("stale entries:" in error for error in self.errors(self.units(), review)))

    def test_initial_dirty_work_is_part_of_the_baseline_not_this_task(self):
        (self.repo / "guide.md").write_text("User's existing edit.\n")
        (self.repo / "other.txt").write_text("Other owner's existing edit.\n")
        self.capture()
        self.assertEqual(self.units(), [])
        (self.repo / "guide.md").write_text("User's existing edit.\nRequested addition.\n")
        units = self.units()
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]["old"], "")
        self.assertEqual(units[0]["new"], "Requested addition.\n")

    def test_existing_scoped_untracked_file_has_a_real_before_image(self):
        (self.repo / "draft.md").write_text("Existing draft.\n")
        self.capture(["draft.md"])
        (self.repo / "draft.md").write_text("Revised draft.\n")
        units = self.units()
        self.assertEqual(units[0]["old"], "Existing draft.\n")
        self.assertTrue(units[0]["baseline_content_available"])

    def test_git_normalized_files_preserve_actual_working_newlines(self):
        self.run_git("config", "core.autocrlf", "true")
        (self.repo / "guide.md").write_bytes(b"Stable intent, ownership and major flows.\r\n\r\nA development log.\r\n")
        self.capture()
        (self.repo / "guide.md").write_bytes(b"Stable intent, ownership and major flows.\r\n\r\n")
        units = self.units()
        self.assertEqual(len(units), 1)
        self.assertEqual(units[0]["old"], "A development log.\r\n")
        self.assertEqual(units[0]["new"], "")

    def test_unplanned_changes_require_explicit_accounting(self):
        self.capture()
        (self.repo / "other.txt").write_text("Concurrent edit.\n")
        units = self.units()
        self.assertFalse(units[0]["planned"])
        review = self.reviewed(units)
        self.assertTrue(any("outside the planned paths" in error for error in self.errors(units, review)))
        review["changes"][0].update(basis="external", requirement=None, intent_effect="external",
                                     reason="Concurrent work by the other owner.", scope_reason="Not part of this task; preserve it.")
        self.assertEqual(self.errors(units, review), [])

    def test_uncaptured_untracked_work_cannot_be_claimed_as_reviewed_task_edits(self):
        (self.repo / "outside.md").write_text("Existing unrelated draft.\n")
        self.capture()
        (self.repo / "outside.md").write_text("Changed draft.\n")
        units = self.units()
        self.assertFalse(units[0]["baseline_content_available"])
        review = self.reviewed(units)
        review["changes"][0]["scope_reason"] = "Expanding the task."
        self.assertTrue(any("no baseline contents" in error for error in self.errors(units, review)))

    def test_additional_work_and_contract_changes_need_decision_text(self):
        self.capture()
        (self.repo / "guide.md").write_text("Different requirement.\n")
        units = self.units()
        review = self.reviewed(units)
        review["changes"][0].update(basis="additional", requirement=None, intent_effect="changes")
        self.assertTrue(any("contract decision" in error for error in self.errors(units, review)))
        review["changes"][0]["decision"] = "Proposed wording change alters the stated obligation; separate from cleanup."
        self.assertEqual(self.errors(units, review), [])

    def test_forged_diff_and_requirement_are_rejected(self):
        self.capture()
        (self.repo / "guide.md").write_text("Replacement.\n")
        units = self.units()
        review = self.reviewed(units)
        review["changes"][0].update(new="Concealed replacement.", requirement="invented")
        errors = self.errors(units, review)
        self.assertTrue(any("does not match source" in error for error in errors))
        self.assertTrue(any("captured requirement" in error for error in errors))

    def test_readable_report_surfaces_additional_work_and_renders_actual_source(self):
        self.capture()
        (self.repo / "guide.md").write_text("Stable purpose, ownership and major flows.\n\nA development log.\n")
        units = self.units()
        review = self.reviewed(units)
        review["changes"][0].update(basis="additional", requirement=None,
                                     reason="Proposed terminology change.", decision="Outside the requested cleanup.",
                                     new="A false description of the diff.")
        report = scope.render(self.snapshot, units, review, self.errors(units, review))
        self.assertIn("## additional changes", report)
        self.assertIn("Accounting: INCOMPLETE", report)
        self.assertIn("+Stable purpose", report)
        self.assertNotIn("A false description of the diff.", report)

    def test_intermediate_commit_does_not_erase_changes_from_the_starting_point(self):
        self.capture()
        (self.repo / "guide.md").write_text("Requested change.\n")
        self.run_git("add", "guide.md")
        self.run_git("-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null",
                     "commit", "-q", "--no-verify", "-m", "intermediate fixture")
        units = self.units()
        self.assertEqual(len(units), 1)
        self.assertIn("Stable intent", units[0]["old"])
        self.assertEqual(units[0]["new"], "Requested change.\n")

    def test_deletion_binary_and_mode_changes_are_not_omitted(self):
        (self.repo / "binary.dat").write_bytes(b"\0old")
        self.capture(["guide.md", "binary.dat", "other.txt"])
        (self.repo / "guide.md").unlink()
        (self.repo / "binary.dat").write_bytes(b"\0new")
        (self.repo / "other.txt").chmod(0o755)
        units = self.units()
        self.assertEqual({unit["path"] for unit in units}, {"guide.md", "binary.dat", "other.txt"})
        binary = next(unit for unit in units if unit["path"] == "binary.dat")
        self.assertIsNone(binary["new"])
        self.assertNotEqual(binary["before"], binary["after"])

    def test_cli_preserves_index_and_requires_fresh_explicit_review(self):
        index = (self.repo / ".git/index").read_bytes()
        script = str(Path(scope.__file__).resolve())
        def run(*args):
            return subprocess.run([sys.executable, script, "--repo", str(self.repo), *args], capture_output=True, text=True)
        baseline = self.repo / "target/scope/baseline.json"
        review_path = self.repo / "target/scope/review.json"
        result = run("capture", "--path", "guide.md", "--requirement", "cleanup=Remove dev logs", "--output", str(baseline))
        self.assertEqual(result.returncode, 0, result.stderr)
        (self.repo / "guide.md").write_text("Stable intent, ownership and major flows.\n\n")
        result = run("prepare", "--snapshot", str(baseline), "--output", str(review_path))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(run("check", "--snapshot", str(baseline), "--review", str(review_path)).returncode, 1)
        review = json.loads(review_path.read_text())
        for entry in review["changes"]:
            entry.update(basis="requested", requirement="cleanup", reason="Remove dev log.", intent_effect="preserves")
        review_path.write_text(json.dumps(review))
        result = run("check", "--snapshot", str(baseline), "--review", str(review_path))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("not semantic approval", result.stdout)
        report = self.repo / "target/scope/REVIEW.md"
        self.assertEqual(run("check", "--snapshot", str(baseline), "--review", str(review_path), "--report", str(report)).returncode, 0)
        self.assertIn("-A development log.", report.read_text())
        self.assertEqual((self.repo / ".git/index").read_bytes(), index)
        review_path.write_text('{"changes": "malformed"}')
        malformed = run("check", "--snapshot", str(baseline), "--review", str(review_path))
        self.assertEqual(malformed.returncode, 1)
        self.assertNotIn("Traceback", malformed.stderr)
        self.assertNotEqual(run("capture", "--path", "guide.md", "--requirement", "cleanup=Remove dev logs", "--output", str(baseline)).returncode, 0)


if __name__ == "__main__":
    unittest.main()
