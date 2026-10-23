#!/usr/bin/env python3
"""Offline artifact/loader-contract review. NO kernel BPF backend or apply mode.

Snapshots here are synthetic review inputs, never live proof. A future backend
must pin kernel objects, check effective attachments atomically and hold its own
FDs; this module neither creates those objects nor authenticates supplied facts.
"""
import argparse
import json
from pathlib import Path, PurePosixPath
import re

from daemon_scope import integer, require, span, validate_roots
from owned_counters import BINDINGS, digest

MAP = dict(type="percpu_array", key_size=4, value_size=24, max_entries=2, flags=8)
PROGRAMS = {"ingress":"sinomo_ip_ingress", "egress":"sinomo_ip_egress"}
SEMANTICS = "cgroup_skb.len-per-hook-visit"


def positive(value):
    require(integer(value) > 0, "missing kernel object identifier")
    return value


def plan(manifest, artifact, snapshot):
    require(manifest["purpose"] == "runtime", "review requires runtime ownership roles")
    require(all(type(manifest[k]) is str and manifest[k] for k in ("node_id","boot_id")), "missing host identity")
    require(re.fullmatch(r"[0-9a-f]{40}",manifest["source_revision"]), "exact source revision required")
    require(re.fullmatch(r"[0-9a-f]{64}",artifact["object_sha256"]), "object hash required")
    require(snapshot["boot_id"] == manifest["boot_id"] and snapshot["node_id"] == manifest["node_id"],
            "host/node/boot mismatch")
    start, end = span(manifest["review_valid_ns"])
    require(start <= integer(snapshot["now_ns"]) <= end and end-start <= 120_000_000_000,
            "stale or unbounded review observation")
    require(artifact["programs"] == PROGRAMS and artifact["map"] == MAP, "unexpected object contract")
    require(artifact["semantics"] == SEMANTICS, "byte semantics mismatch")
    roots = manifest["groups"]
    validate_roots({k:dict(path=v["path"],incarnation=str(v["identity"]),source="cgroup-v2-cpu.stat") for k,v in roots.items()})
    roles = [o["role"] for o in manifest["owners"]]
    require(roles.count("main") == roles.count("shadow") == 1 and all(r in ("main","shadow","helper") for r in roles),
            "main/shadow/helper ownership missing")
    ids = [o["id"] for o in manifest["owners"]]
    pids = [positive(o["pid"]) for o in manifest["owners"]]
    require(len(pids) == len(set(pids)), "duplicate owner PID")
    require(len(ids) == len(set(ids)) and set(ids) == snapshot["owners"].keys(), "owner inventory mismatch")
    require(set(roots) == snapshot["groups"].keys(), "cgroup inventory mismatch")
    require(all(o["group"] in roots and o["ownership_ref"] for o in manifest["owners"]), "unbound helper or handoff")
    require({o["group"] for o in manifest["owners"]} == set(roots), "unowned cgroup root")
    for owner in manifest["owners"]:
        current = snapshot["owners"][owner["id"]]
        require(all(current[k] == owner[k] for k in ("pid","start_ticks","executable","sha256","argv","group")),
                "runtime incarnation/executable mismatch")
    output = {}
    seen_kernel_ids = set()
    for key, root in roots.items():
        path = PurePosixPath(root["path"])
        require(path.name == root["unit"] and path.name.endswith((".service",".scope"))
                and not re.fullmatch(r"(?:user@\d+\.service|session-\d+\.scope)",path.name),
                "broad host/user/slice/session root forbidden")
        for owner in manifest["owners"]:
            if owner["group"] == key and owner["role"] in ("main","shadow"):
                expected = 'sinomo-daemon.service' if owner["role"] == 'main' else 'sinomo-shadow-daemon.service'
                require(root["unit"] == expected, "main/shadow service binding mismatch")
        current = snapshot["groups"][key]
        require(all(current[k] == root[k] for k in ("path","identity","unit","kernel_id")), "cgroup incarnation mismatch")
        kid = positive(current["kernel_id"])
        require(kid not in seen_kernel_ids, "duplicate kernel cgroup ID")
        seen_kernel_ids.add(kid)
        require(current["query_complete"] is True and current["effective_ingress_ids"] == []
                and current["effective_egress_ids"] == [], "existing/unknown effective attachments; refuse")
        positive(current["revision"])
        expected_owners = sorted(o['id'] for o in manifest['owners'] if o['group'] == key)
        require(sorted(current["owner_ids"]) == expected_owners, "unclassified cgroup members")
        output[key] = dict(root, expected_revision=current["revision"], owner_ids=expected_owners,
                           programs=dict(PROGRAMS), fresh_object_per_root=True)
    return dict(schema="sinomo.packet-loader-review.v1", node_id=manifest["node_id"],boot_id=manifest["boot_id"],
                source_revision=manifest["source_revision"],object_sha256=artifact["object_sha256"],roots=output,
                map=dict(MAP),semantics=SEMANTICS,allow_verdict=1,replace_existing=False,counter_reset=False,
                review_valid=True,execution_ready=False,acceptance=False,kernel_actions_performed=False,
                blockers=["native kernel backend not implemented or authorized",
                          "installed-kernel verifier/attachment permissions unqualified",
                          "IP-hook bytes not approved as interface-equivalent bytes",
                          "complete helper/socket handoff coverage unproved"])


def validate_bundle(plan_value, root, bundle, previous=None):
    require(bundle["object_sha256"] == plan_value["object_sha256"], "loaded object changed")
    require(bundle["root_kernel_id"] == plan_value["roots"][root]["kernel_id"], "loaded root changed")
    require(bundle["map"] == MAP, "map contract changed")
    require(bundle["program_type"] == "cgroup_skb", "program type changed")
    positive(bundle["map_id"])
    require(bundle["program_ids"].keys() == PROGRAMS.keys(), "program pair missing")
    identifiers = [positive(v) for v in bundle["program_ids"].values()]
    require(len(set(identifiers)) == 2, "direction programs aliased")
    if previous is not None:
        require(bundle == previous, "immutable program/map identity changed")


def validate_bundles(plan_value, bundles, previous=None):
    require(bundles.keys() == plan_value['roots'].keys(), "missing root object")
    maps,programs=set(),set()
    for root,bundle in bundles.items():
        validate_bundle(plan_value,root,bundle,previous[root] if previous else None)
        require(bundle['map_id'] not in maps, "counter map shared across roots")
        require(not programs.intersection(bundle['program_ids'].values()), "program shared across roots")
        maps.add(bundle['map_id']);programs.update(bundle['program_ids'].values())


def cleanup_actions(owned, observed):
    """Synthetic FD-close contract, not a detach command or real cleanup backend."""
    actions=[]
    require(len({x['handle'] for x in owned}) == len(owned), "duplicate owned link handle")
    for original in owned:
        current=observed.get(original["handle"])
        require(current == original, "owned link identity changed/missing; preserve unknown outcome")
        for key in ("link_id","program_id","root_kernel_id"):
            positive(original[key])
        require(original['direction'] in PROGRAMS, "unknown direction")
        actions.append(dict(operation="close_owned_link_fd",handle=original["handle"],expected_link_id=original["link_id"]))
    return actions


def counter_delta(new, old):
    require(all(new[k] == old[k] for k in ("boot_id","object_sha256","root_kernel_id","map_id","program_ids")),
            "counter identity changed")
    require(new["per_cpu"].keys() == old["per_cpu"].keys() and new["per_cpu"], "CPU-slot inventory changed")
    gap=span(new["read_ns"])[0]-span(old["read_ns"])[1]
    require(0 < gap <= 5_000_000_000, "counter read overlap/stall")
    delta={direction:dict(skb_bytes=0,skb_packets=0) for direction in PROGRAMS}
    for cpu in new["per_cpu"]:
        for direction in PROGRAMS:
            before,after=old["per_cpu"][cpu][direction],new["per_cpu"][cpu][direction]
            require(integer(before["overflow"]) == integer(after["overflow"]) == 0, "counter overflow")
            for key in ("skb_bytes","skb_packets"):
                value=integer(after[key])-integer(before[key])
                require(value >= 0, "counter reset")
                delta[direction][key]+=value
    return dict(semantics=SEMANTICS,minimum_interval_ns=gap,directions=delta,acceptance=False,
                interface_equivalent=False,numeric_gate_applied=False)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest',type=Path,required=True)
    parser.add_argument('--manifest-sha256',required=True)
    parser.add_argument('--artifact',type=Path,required=True)
    parser.add_argument('--artifact-sha256',required=True)
    parser.add_argument('--object',type=Path,required=True)
    parser.add_argument('--synthetic-snapshot',type=Path,required=True)
    args=parser.parse_args()
    require(digest(args.manifest) == args.manifest_sha256, "manifest pin mismatch")
    require(digest(args.artifact) == args.artifact_sha256, "artifact receipt pin mismatch")
    manifest=json.loads(args.manifest.read_text());artifact=json.loads(args.artifact.read_text())
    require(digest(args.object) == artifact['object_sha256'], "object digest mismatch")
    receipt=manifest['runtime_receipt']
    require(digest(receipt['path']) == receipt['sha256'], "runtime receipt pin mismatch")
    binding=json.loads(Path(receipt['path']).read_text())
    require(all(binding[k] == manifest[k] for k in BINDINGS), "runtime receipt binding mismatch")
    result=plan(manifest,artifact,json.loads(args.synthetic_snapshot.read_text()))
    print(json.dumps(result,indent=2))
    return 0  # Offline review validity only; execution_ready/acceptance stay false.


if __name__=='__main__':raise SystemExit(main())
