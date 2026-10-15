#!/usr/bin/env python3
"""Disposable Linux counter calibration; NEVER native-cluster acceptance.

Uses one labelled, read-only, network-none container from an existing pinned image.
It leaves that owned container stopped, keeps raw evidence, and never mounts DATA,
changes capabilities/settings, enters a namespace, or controls a real daemon.
Run only after the build slot is available, under the shared build lock.
"""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import resource
import subprocess
import time

import controller as c

IMAGE = "sha256:061b6e52a07ab675f0e4a9428c5a8ee6bed996983427f4691f6bebf29c56d9dc"
WORKER = r'''
import json,os,pathlib,socket,threading,time
def counters():
 with open('/proc/self/net/dev') as f:
  identity=os.fstat(f.fileno());raw=f.read()
 rows={}
 for line in raw.splitlines()[2:]:
  name,rest=line.split(':',1);v=rest.split();rows[name.strip()]=[int(v[0]),int(v[8])]
 return {'counter_file':f'{identity.st_dev}:{identity.st_ino}','interfaces':rows}
def network(family,kind,payload):
 address='127.0.0.1' if family==socket.AF_INET else '::1'
 receiver=socket.socket(family,kind);receiver.bind((address,0));received=[]
 if kind==socket.SOCK_STREAM:receiver.listen(1)
 def receive():
  if kind==socket.SOCK_STREAM:
   peer,_=receiver.accept()
   with peer:
    while True:
     block=peer.recv(65536)
     if not block:break
     received.append(block)
  else:
   for _ in range(payload//1024):received.append(receiver.recvfrom(2048)[0])
 t=threading.Thread(target=receive);t.start()
 sender=socket.socket(family,kind)
 with sender:
  sender.connect(receiver.getsockname())
  if kind==socket.SOCK_STREAM:sender.sendall(b'q'*payload);sender.shutdown(socket.SHUT_WR)
  else:
   for _ in range(payload//1024):sender.send(b'q'*1024)
 t.join(5);receiver.close()
 assert not t.is_alive() and b''.join(received)==b'q'*payload
 time.sleep(.2)
 return payload
stat=pathlib.Path('/proc/self/stat').read_text();fields=stat[stat.rfind(')')+2:].split()
print(json.dumps({'pid_inside':os.getpid(),'start_ticks':int(fields[19]),**counters()}),flush=True)
for line in __import__('sys').stdin:
 cmd=json.loads(line)
 if cmd['kind']=='quit':break
 if cmd['kind']=='cpu':
  begin=time.process_time_ns()
  while time.process_time_ns()-begin<750000000:pass
  result={'kind':'cpu','cpu_ns':time.process_time_ns()-begin}
 else:
  before=counters();family=socket.AF_INET6 if cmd['ipv6'] else socket.AF_INET
  kind=socket.SOCK_DGRAM if cmd['udp'] else socket.SOCK_STREAM
  payload=32768 if cmd['udp'] else 262144
  network(family,kind,payload);after=counters()
  result={'kind':'network','ipv6':cmd['ipv6'],'udp':cmd['udp'],'payload_bytes_verified':payload,'before':before,'after':after}
 print(json.dumps(result),flush=True)
'''


def command(argv):
    return subprocess.run(argv, check=True, capture_output=True, text=True, timeout=30).stdout


def reply(process, timeout=8):
    import select
    if not select.select([process.stdout], [], [], timeout)[0]:
        raise TimeoutError("owned disposable worker response missing")
    line = process.stdout.readline()
    if not line:
        raise EOFError("owned disposable worker exited")
    return json.loads(line)


def calibrate(collector, output):
    output.mkdir(parents=True, exist_ok=False)
    name = "sinomo-measurement-" + str(os.getpid()) + "-" + str(time.time_ns())
    receipt = dict(schema="sinomo.linux-counter-fixtures.v1", acceptance=False,
                   scope="controlled disposable counter calibration; entire native host is NOT accepted",
                   image=IMAGE, name=name, data_mounted=False, production_changes=False,
                   permissions_changed=False, results=[], assertions=[], helpers={})
    receipt["tool_sources_sha256"] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                      for p in Path(__file__).parent.iterdir() if p.suffix in (".py", ".c")}
    receipt["collector_sha256"] = hashlib.sha256(collector.read_bytes()).hexdigest()
    cid = None
    worker = None
    reader = None
    previous = None
    ephemeral_future = None
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    executor.submit(lambda: None).result()  # Persistent owned fixture worker.
    before_usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    started = time.monotonic_ns()
    try:
        command(["docker", "image", "inspect", IMAGE])  # Never pull/install implicitly.
        cid = command(["docker", "run", "-d", "--name", name, "--interactive", "--read-only",
                       "--network", "none", "--label", "sinomo.measurement.owner=main-20261015",
                       "--entrypoint", "python3", IMAGE, "-c", "import time;time.sleep(300)"]).strip()
        info = json.loads(command(["docker", "inspect", cid]))[0]
        receipt.update(container_id=cid, container_started_at=info["State"]["StartedAt"],
                       container_main_host_pid=info["State"]["Pid"])
        worker_stderr = open(output / "worker.stderr", "xb")
        worker = subprocess.Popen(["docker", "exec", "-i", cid, "python3", "-u", "-c", WORKER],
                                  text=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=worker_stderr)
        identity = reply(worker)
        receipt["worker_identity"] = identity
        reader = c.Reader([str(collector), "--raw", str((output / "raw.ndjson").resolve())], output / "collector.stderr")
        ready = reader.receive(started + c.DEADLINE_NS)["sample"]
        receipt["collector"] = ready
        tick = time.monotonic_ns()
        barrier = c.JointWindow(["fixture-host"], started)
        first_ack = None
        for i in range(34):
            now = time.monotonic_ns()
            if now < tick:
                time.sleep((tick - now) / c.SECOND)
            nonce = f"fixture-{i}"
            sent = reader.send(f"SAMPLE {i} {nonce}\n")
            response = reader.receive(started + c.DEADLINE_NS)
            row = response["sample"]
            first_ack = first_ack or response["received_ns"]
            checks = {}
            if previous is not None:
                try:
                    checks["fixture-host"] = c.interval(row, previous, ready["hertz"])
                except (ValueError, KeyError, TypeError) as e:
                    checks["fixture-host"] = dict(counters_within=False, coverage_complete=False, error=str(e))
            joint = barrier.observe(i, nonce, {"fixture-host": sent}, {"fixture-host": response},
                                    checks, end=sent-first_ack >= c.QUIET_NS)
            receipt["results"].append(dict(sequence=i, sent_ns=sent, response=response, checks=checks, joint=joint))
            if i == 0:
                raw_rows = [json.loads(line) for line in (output / "raw.ndjson").read_text().splitlines()]
                main_threads = [r for r in raw_rows if r["type"] == "thread" and r["tid"] == info["State"]["Pid"]]
                assert len(main_threads) == 1, "owned host PID counter binding missing/ambiguous"
                host_key = ":".join(map(str, main_threads[0]["counter_file"]))
                assert host_key in row["groups"], "owned root-user namespace counter missing"
                # Proc mount devices differ across mount namespaces. Do not
                # compare (dev,ino) across proc mounts or dedup by inode alone.
                assert host_key.split(":")[1] == identity["counter_file"].split(":")[1]
                identity["host_counter_file"] = host_key
                receipt["assertions"].append("Public all-thread census observes the root-user network-none namespace without privilege changes")
                worker.stdin.write(json.dumps(dict(kind="cpu")) + "\n"); worker.stdin.flush()
                receipt["cpu_fixture"] = reply(worker)
            elif i in (1, 2, 3, 4):
                worker.stdin.write(json.dumps(dict(kind="network", ipv6=i >= 3, udp=i in (2, 4))) + "\n")
                worker.stdin.flush()
                receipt.setdefault("network_fixtures", []).append(reply(worker))
            elif i == 6:
                # A complete namespace can be born, transfer bytes, and disappear
                # BETWEEN two successful one-second censuses. Snapshot success
                # cannot close this blind spot. No production isolation changes.
                ephemeral = WORKER.split("stat=pathlib.Path('/proc/self/stat')")[0] + "\nbefore=counters();network(socket.AF_INET,socket.SOCK_STREAM,262144);print(json.dumps({'before':before,'after':counters()}))\n"
                def transfer():
                    begin = time.monotonic_ns()
                    result = command(["docker", "run", "--rm", "--read-only", "--network", "none",
                                      "--cidfile", str(output / "ephemeral.cid"),
                                      "--label", "sinomo.measurement.owner=main-20261015",
                                      "--entrypoint", "python3", IMAGE, "-u", "-c", ephemeral])
                    return dict(begin_ns=begin, end_ns=time.monotonic_ns(), data=json.loads(result))
                # Docker startup/removal may exceed a tick. It MUST NOT delay
                # sampling; even our negative fixtures keep the same cadence.
                ephemeral_future = executor.submit(transfer)
            previous = row
            tick += c.CADENCE_NS
        assert not any(r["joint"]["covered_window"] for r in receipt["results"]), "coverage gap must never accept native host"
        assert receipt["results"][-1]["sent_ns"] - first_ack >= c.QUIET_NS
        cadence_errors = [r["joint"]["error"] for r in receipt["results"] if "error" in r["joint"]]
        assert not cadence_errors, f"controller cadence/barrier failure: {cadence_errors}"
        receipt["common_barrier_envelope_ns"] = receipt["results"][-1]["sent_ns"] - first_ack
        receipt["assertions"].append("A real >=30s controller barrier envelope remains BLOCKED by native host coverage; no independent-clock substitution")
        for index, probe in enumerate(receipt["network_fixtures"]):
            a, b = probe["before"]["interfaces"]["lo"], probe["after"]["interfaces"]["lo"]
            delta = [v-u for u, v in zip(a, b)]
            assert delta[0] == delta[1] and delta[0] >= probe["payload_bytes_verified"]
            lo_a = receipt["results"][index+1]["response"]["sample"]["groups"][identity["host_counter_file"]]["interfaces"]["lo"]
            lo_b = receipt["results"][index+2]["response"]["sample"]["groups"][identity["host_counter_file"]]["interfaces"]["lo"]
            assert [v-u for u, v in zip(lo_a, lo_b)] == delta, "outside/public and inside loopback counter delta mismatch"
            probe["calibrated_delta_rx_tx"] = delta
        receipt["assertions"].append("TCP/UDP IPv4/IPv6 payloads verified; public loopback RX and TX deltas exactly match inside counters, including header cost")
        receipt["ephemeral_namespace"] = ephemeral_future.result(timeout=5)
        ephemeral = receipt["ephemeral_namespace"]
        # This fixture proc mount may have another device number. Compare the
        # known owned inode against all HOST proc keys solely to establish that
        # no matching endpoint was observed, never to deduplicate a live group.
        inode = ephemeral["data"]["before"]["counter_file"].split(":")[1]
        receipt["ephemeral_seen_sequences"] = [r["sequence"] for r in receipt["results"]
            if any(k.split(":")[1] == inode for k in r["response"]["sample"]["groups"])]
        # Retain both facts: the CLI lifetime may cross a tick even when its
        # active namespace never appears in any live-thread census (cleanup is
        # outside the active process lifetime). Do not invent a tighter bound.
        receipt["ephemeral_full_cli_lifetime_between_adjacent_censuses"] = any(
            a["response"]["received_ns"] <= ephemeral["begin_ns"] < ephemeral["end_ns"] < b["sent_ns"]
            for a, b in zip(receipt["results"], receipt["results"][1:]))
        receipt["ephemeral_counter_census_gap_demonstrated"] = not receipt["ephemeral_seen_sequences"]
        if not receipt["ephemeral_seen_sequences"]:
            receipt["assertions"].append("Controlled namespace transferred loopback bytes without appearing in any cadence-preserved census; coverage=false prevents acceptance")
        else:
            lifecycle_errors = [r["checks"].get("fixture-host", {}).get("error", "") for r in receipt["results"]]
            assert any("inventory changed" in error for error in lifecycle_errors)
            receipt["assertions"].append("Controlled namespace birth/exit was observed and its inventory discontinuity invalidated the interval")
        receipt["status"] = "FIXTURES_PASSED_NATIVE_ACCEPTANCE_BLOCKED"
    except Exception as e:
        receipt["status"] = "FIXTURE_FAILED_EVIDENCE_RETAINED"
        receipt["error"] = f"{type(e).__name__}: {e}"
    finally:
        cleanup_errors = []
        # All submitted work uses a 30s subprocess deadline and only owned
        # disposable resources. Do not leave a delayed fixture task behind.
        executor.shutdown(wait=True, cancel_futures=True)
        ephemeral_cid_path = output / "ephemeral.cid"
        if ephemeral_cid_path.exists():
            ephemeral_cid = ephemeral_cid_path.read_text().strip()
            inspection = subprocess.run(["docker", "inspect", ephemeral_cid], capture_output=True, text=True, timeout=10)
            if inspection.returncode == 0:
                try:
                    command(["docker", "stop", "--time", "5", ephemeral_cid])
                    stopped = json.loads(command(["docker", "inspect", ephemeral_cid]))[0]["State"]
                    if stopped["Running"]:cleanup_errors.append("owned ephemeral fixture still running")
                except (OSError, ValueError, subprocess.SubprocessError) as e:
                    cleanup_errors.append(f"owned ephemeral fixture stop proof unavailable: {e}")
            else:
                # CLI diagnostic wording/case is not an absence proof. Require
                # a successful independent inventory query for this exact CID.
                try:
                    matches = command(["docker", "ps", "--all", "--no-trunc", "--filter",
                                       "id=" + ephemeral_cid, "--format", "{{.ID}}"])
                    receipt["ephemeral_removal_inventory"] = dict(container_id=ephemeral_cid,
                                                                  matching_ids=matches.splitlines())
                    if matches.strip():
                        cleanup_errors.append("owned ephemeral fixture removal not proven by inventory")
                except (OSError, subprocess.SubprocessError) as e:
                    cleanup_errors.append(f"owned ephemeral fixture removal inventory unavailable: {e}")
        if worker is not None:
            try:
                try:
                    worker.stdin.write('{"kind":"quit"}\n'); worker.stdin.flush()
                    worker.communicate(timeout=5)
                except (OSError, subprocess.TimeoutExpired):
                    # This is the owned docker-exec client only, never a real process.
                    if worker.poll() is None:
                        worker.terminate()
                    worker.communicate(timeout=5)
            except (OSError, subprocess.TimeoutExpired) as e:
                cleanup_errors.append(f"owned worker client drain unavailable: {e}")
            receipt["helpers"]["worker_client_exit"] = worker.returncode
            worker_stderr.close()
        if cid:
            try:
                command(["docker", "stop", "--time", "5", cid])
                final = json.loads(command(["docker", "inspect", cid]))[0]
                receipt["final_owned_container"] = final["State"]
                if final["State"]["Running"]:
                    cleanup_errors.append("owned fixture container still running")
            except (OSError, ValueError, subprocess.SubprocessError) as e:
                cleanup_errors.append(f"owned fixture stop/proof unavailable: {e}")
        if reader is not None:
            # Capture fixture/docker-exec/drain costs before the counter itself
            # exits. Independent native observer/transport drain is STILL unknown.
            try:
                i = len(receipt["results"])
                reader.send(f"SAMPLE {i} fixture-drain\n")
                receipt["after_fixture_drain"] = reader.receive(time.monotonic_ns() + 5*c.SECOND)
            except (OSError, ValueError, TimeoutError, EOFError) as e:
                cleanup_errors.append(f"fixture drain counter unavailable: {e}")
            finally:
                reader.close()
                receipt["helpers"]["collector_exit"] = reader.process.returncode
                cleanup_errors.extend(reader.cleanup_errors)
        if cleanup_errors:
            receipt["cleanup_errors"] = cleanup_errors
            receipt["status"] = "FIXTURE_FAILED_EVIDENCE_RETAINED"
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        receipt["owned_direct_child_cpu_seconds_including_startup_and_drain"] = (usage.ru_utime+usage.ru_stime-before_usage.ru_utime-before_usage.ru_stime)
        receipt["native_observer_transport_drain_coverage_complete"] = False
        receipt["finished_ns"] = time.monotonic_ns()
        (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    # CPU calibration uses the exact held main worker TID in the public census,
    # matched by owned namespace + reported start ticks, not a command substring.
    if reader is not None and (output / "raw.ndjson").exists():
        attribution = c.attribution(output / "raw.ndjson", [])
        (output / "attribution.json").write_text(json.dumps(attribution, indent=2) + "\n")
        if "cpu_fixture" in receipt:
            before = []; after = []
            sequence = None
            for line in (output / "raw.ndjson").read_text().splitlines():
                r = json.loads(line)
                if r["type"] == "begin":sequence = r["sequence"]
                if r["type"] == "thread" and r["tgid"] == r["tid"] and r["start_ticks"] == identity["start_ticks"] and ":".join(map(str,r["counter_file"])) == identity.get("host_counter_file"):
                    if sequence == 0:before.append(r)
                    if sequence == 1:after.append(r)
            if len(before) == len(after) == 1 and before[0]["thread_epoch"] == after[0]["thread_epoch"]:
                delta = sum(after[0]["cpu_ticks"]) - sum(before[0]["cpu_ticks"])
                measured = receipt["cpu_fixture"]["cpu_ns"] / c.SECOND
                receipt["cpu_fixture"]["public_thread_delta_ticks"] = delta
                receipt["cpu_fixture"]["hertz"] = ready["hertz"]
                receipt["cpu_fixture"]["within_two_ticks"] = abs(delta/ready["hertz"] - measured) <= 2/ready["hertz"]
                if not receipt["cpu_fixture"]["within_two_ticks"]:
                    receipt["status"] = "FIXTURE_FAILED_EVIDENCE_RETAINED"
            else:
                receipt["status"] = "FIXTURE_FAILED_EVIDENCE_RETAINED"
                receipt["cpu_fixture"]["error"] = "exact held worker incarnation missing/ambiguous"
        receipt["raw_sha256"] = hashlib.sha256((output / "raw.ndjson").read_bytes()).hexdigest()
        (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collector", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = calibrate(args.collector.resolve(), args.output.resolve())
    print(json.dumps({k: result[k] for k in ("status", "acceptance", "assertions", "helpers")}))
    return 0 if result["status"] == "FIXTURES_PASSED_NATIVE_ACCEPTANCE_BLOCKED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
