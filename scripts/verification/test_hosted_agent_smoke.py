"""Offline negative controls for candidate/target preflight evidence only."""
import copy
import unittest

from hosted_agent_smoke import digest, qualify


class PreflightEvidenceTests(unittest.TestCase):
    def setUp(self):
        agent = {"id": "smoke", "placement": {"home_server_node_id": "node"},
                 "tool_runtime": {"sandbox_node_id": "node"}}
        self.pins = dict(candidate="a" * 40, cluster_id="cluster", node_id="node",
                         agent_id="smoke", agent_sha256=digest(agent))
        status = dict(commit="a" * 40, target_os="linux", pid=123, started_at="generation-one",
                      cluster=dict(cluster_id="cluster", node_id="node", process="daemon-main"))
        self.records = [
            dict(kind="intent", schema=1, model_turn_submitted=False,
                 **{k: v for k, v in self.pins.items() if k != "agent_sha256"}),
            dict(kind="before", status=copy.deepcopy(status)),
            dict(kind="catalog", snapshot=dict(error=None, selected_agent=agent)),
            dict(kind="after", status=copy.deepcopy(status)),
            dict(kind="result", preflight_observed=True, error=None,
                 model_turn_submitted=False, hosted_smoke_passed=False, rollout_accepted=False),
        ]

    def test_consistent_receipt_never_qualifies_live_smoke_or_rollout(self):
        report = qualify(self.records, **self.pins)
        self.assertTrue(report["receipt_consistent"])
        for key in ["execution_ready", "fresh_live_identity_verified", "hosted_smoke_passed", "rollout_accepted"]:
            self.assertIs(report[key], False)

    def test_old_commit_wrong_node_cluster_role_or_platform_is_rejected(self):
        for section, key, value in [("status", "commit", "b" * 40), ("status", "target_os", "windows"),
                                    ("cluster", "node_id", "other"), ("cluster", "cluster_id", "other"),
                                    ("cluster", "process", "shadow")]:
            for index in (1, 3):
                with self.subTest(section=section, key=key, index=index):
                    rows = copy.deepcopy(self.records)
                    target = rows[index]["status"]
                    if section == "cluster":
                        target = target["cluster"]
                    target[key] = value
                    with self.assertRaises(ValueError):
                        qualify(rows, **self.pins)

    def test_restart_or_pid_reuse_is_rejected(self):
        for key, value in [("pid", 456), ("started_at", "generation-two"), ("pid", True)]:
            rows = copy.deepcopy(self.records)
            rows[3]["status"][key] = value
            with self.assertRaises(ValueError):
                qualify(rows, **self.pins)

    def test_partial_repeated_and_reordered_receipts_are_rejected(self):
        for rows in [self.records[:-1], self.records + [self.records[-1]],
                     [self.records[0], self.records[2], self.records[1], *self.records[3:]]]:
            with self.assertRaises(ValueError):
                qualify(rows, **self.pins)

    def test_fallback_catalog_and_changed_tool_authority_are_rejected(self):
        for change in ("fallback", "error", "tools"):
            rows = copy.deepcopy(self.records)
            snapshot = rows[2]["snapshot"]
            if change == "fallback":
                snapshot["selected_agent"]["id"] = "main-agent"
            elif change == "error":
                snapshot["error"] = "catalog unavailable"
            else:
                snapshot["selected_agent"]["tools"] = [{"name": "exec"}]
            with self.assertRaises(ValueError):
                qualify(rows, **self.pins)

    def test_reviewed_cross_node_placement_is_still_rejected(self):
        rows = copy.deepcopy(self.records)
        agent = rows[2]["snapshot"]["selected_agent"]
        agent["tool_runtime"]["sandbox_node_id"] = "other"
        pins = dict(self.pins, agent_sha256=digest(agent))
        with self.assertRaises(ValueError):
            qualify(rows, **pins)

    def test_invented_smoke_pass_and_submitted_effect_are_rejected(self):
        for key in ("hosted_smoke_passed", "rollout_accepted", "model_turn_submitted"):
            rows = copy.deepcopy(self.records)
            rows[-1][key] = True
            with self.assertRaises(ValueError):
                qualify(rows, **self.pins)

    def test_abbreviated_candidate_and_missing_review_pin_are_rejected(self):
        for pins in [dict(self.pins, candidate="a" * 8), dict(self.pins, agent_sha256="")]:
            with self.assertRaises(ValueError):
                qualify(self.records, **pins)


if __name__ == "__main__":
    unittest.main()
