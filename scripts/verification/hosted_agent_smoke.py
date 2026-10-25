#!/usr/bin/env python3
"""Offline qualification of hosted_agent_smoke_rpc preflight receipts.

This reads a retained receipt; it does not contact nodes or execute model turns.
Use --help for required candidate, node and reviewed-agent identity pins. Catalog
success is only a prerequisite. Model completion, durable continuity, quorum and
process-family quietness must be established separately on the actual candidate.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                    ensure_ascii=False).encode()).hexdigest()


def qualify(records, *, candidate, cluster_id, node_id, agent_id, agent_sha256):
    """Reject incomplete/mixed observations without treating them as live evidence."""
    if not re.fullmatch(r"[0-9a-f]{40}", candidate):
        raise ValueError("candidate must be a full lowercase Git commit")
    if not re.fullmatch(r"[0-9a-f]{64}", agent_sha256):
        raise ValueError("reviewed agent SHA-256 must be explicit")
    if not all(isinstance(v, str) and v.strip() for v in (cluster_id, node_id, agent_id)):
        raise ValueError("cluster, node and agent identities must be explicit")
    if [r.get("kind") for r in records] != ["intent", "before", "catalog", "after", "result"]:
        raise ValueError("partial, duplicated or out-of-order preflight receipt")
    intent, before, catalog, after, result = records
    expected = dict(candidate=candidate, cluster_id=cluster_id, node_id=node_id, agent_id=agent_id)
    if intent.get("schema") != 1 or any(intent.get(k) != v for k, v in expected.items()):
        raise ValueError("receipt intent does not match the selected candidate and target")
    if intent.get("model_turn_submitted") is not False:
        raise ValueError("unexpected effect in a read-only preflight")
    identities = []
    for record in (before, after):
        status = record.get("status") or {}
        cluster = status.get("cluster") or {}
        if (status.get("commit") != candidate or status.get("target_os") != "linux"
                or cluster.get("cluster_id") != cluster_id or cluster.get("node_id") != node_id
                or cluster.get("process") != "daemon-main"):
            raise ValueError("wrong candidate, cluster, node, OS or daemon role")
        pid, started_at = status.get("pid"), status.get("started_at")
        if type(pid) is not int or pid <= 0 or not isinstance(started_at, str) or not started_at:
            raise ValueError("missing process generation identity")
        identities.append((pid, started_at))
    if identities[0] != identities[1]:
        raise ValueError("daemon process changed during the observation")
    snapshot = catalog.get("snapshot") or {}
    agent = snapshot.get("selected_agent") or {}
    if snapshot.get("error") is not None or agent.get("id") != agent_id:
        raise ValueError("catalog failure or fallback agent selection")
    if digest(agent) != agent_sha256:
        raise ValueError("agent definition differs from the independently reviewed definition")
    for target in ((agent.get("placement") or {}).get("home_server_node_id"),
                   (agent.get("tool_runtime") or {}).get("sandbox_node_id")):
        if target is not None and target != node_id:
            raise ValueError("agent definition selects another execution or sandbox node")
    if (result.get("preflight_observed") is not True or result.get("error") is not None
            or result.get("model_turn_submitted") is not False
            or result.get("hosted_smoke_passed") is not False
            or result.get("rollout_accepted") is not False):
        raise ValueError("failed preflight or unsupported acceptance claim")
    return {
        "schema": 1, **expected, "agent_sha256": agent_sha256,
        "receipt_consistent": True, "evidence_kind": "offline_retained_preflight_review",
        "fresh_live_identity_verified": False, "execution_ready": False,
        "hosted_smoke_passed": False, "rollout_accepted": False,
        "pending": [
            "fresh candidate artifact and current-term quorum verification",
            "exact-node hosted turn dispatch without ownership fallback",
            "approved existing provider/account/model and reviewed tool authority",
            "completed model turn and durable same-session continuation receipts",
            "ordinary correctness and complete owned-process quietness gates",
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("receipt", type=Path, help="JSONL from hosted_agent_smoke_rpc")
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--cluster-id", required=True)
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--agent-id", required=True)
    parser.add_argument("--agent-sha256", required=True,
                        help="SHA-256 of independently reviewed agent JSON, sorted keys, compact UTF-8")
    parser.add_argument("--output", required=True, type=Path, help="new offline review receipt")
    args = parser.parse_args()
    raw = args.receipt.read_bytes()
    try:
        records = [json.loads(line) for line in raw.decode().splitlines()]
        report = qualify(records, candidate=args.candidate, cluster_id=args.cluster_id,
                         node_id=args.node_id, agent_id=args.agent_id, agent_sha256=args.agent_sha256)
    except (ValueError, TypeError, AttributeError) as error:
        report = {"receipt_consistent": False, "execution_ready": False,
                  "hosted_smoke_passed": False, "rollout_accepted": False, "error": str(error)}
    report["input_sha256"] = hashlib.sha256(raw).hexdigest()
    with args.output.open("x") as output:
        json.dump(report, output, indent=2)
        output.write("\n")
    return 0 if report["receipt_consistent"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
