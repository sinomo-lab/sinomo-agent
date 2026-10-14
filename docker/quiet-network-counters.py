#!/usr/bin/env python3
"""One exact all-interface counter group for the maintained Docker quiet gate.

No polling, daemon mutation, logical-byte substitution, or threshold tuning.
Each call uses the gate's inherited absolute deadline and selected project IDs.
"""
import argparse
import concurrent.futures
from decimal import Decimal
import json
import os
import select
import stat
import time
from pathlib import Path
import re
import subprocess

LIMIT_BYTES_PER_SECOND = 65536
UPTIME_QUANTUM_MS = 10


def monotonic_ms():
    value = Path('/proc/uptime').read_text().split()[0]
    if not re.fullmatch(r'[0-9]+\.[0-9]{2}', value):
        raise ValueError('unsupported monotonic uptime precision')
    return int(Decimal(value) * 1000)


def parse_counters(text):
    lines = text.splitlines()
    if not lines or not re.fullmatch(r'net:\[[0-9]+\]', lines[0]):
        raise ValueError('network namespace identity unavailable')
    if len(lines) < 5 or not re.fullmatch(r'[1-9][0-9]*\s+[0-9]+\s+[1-9][0-9]*\s+[0-9]+', lines[1]):
        raise ValueError('main/shadow generation unavailable')
    if ('Receive' not in lines[2] or 'Transmit' not in lines[2]
            or 'face' not in lines[3] or lines[3].count('bytes') != 2):
        raise ValueError('Linux all-interface network counters unavailable')
    interfaces = {}
    for line in lines[4:]:
        name, separator, body = line.partition(':')
        name = name.strip()
        fields = body.split()
        if not separator or not name or name in interfaces or len(fields) != 16:
            raise ValueError('malformed or repeated network interface')
        if any(not re.fullmatch(r'[0-9]+', field) for field in fields):
            raise ValueError('noninteger network counter')
        received, sent = int(fields[0]), int(fields[8])
        if received > 2**64 - 1 or sent > 2**64 - 1:
            raise ValueError('network counter exceeds u64')
        interfaces[name] = {'rx_bytes': str(received), 'tx_bytes': str(sent)}
    if not interfaces or 'lo' not in interfaces:
        raise ValueError('loopback or all-interface coverage unavailable')
    return lines[0], tuple(lines[1].split()), interfaces


def counter_delta(current, previous):
    if previous is None:
        return {'status': 'baseline', 'delta_rx_bytes': None, 'delta_tx_bytes': None,
                'minimum_interval_ms': None, 'within_limit': None}
    if previous.get('error') or any(current.get(key) != previous.get(key)
                                  for key in ('container_id', 'container_started_at', 'namespace', 'generations')):
        raise ValueError('container or network namespace generation changed')
    old, new = previous['interfaces'], current['interfaces']
    if old.keys() != new.keys():
        raise ValueError('network interface inventory changed')
    interval = current['capture_started_ms'] - previous['capture_closed_ms']
    if interval <= 0 or interval > 5000:
        raise ValueError('overlapping, nonmonotonic or stalled network sample')
    received = sent = 0
    for name, values in new.items():
        rx, tx = int(values['rx_bytes']), int(values['tx_bytes'])
        old_rx, old_tx = int(old[name]['rx_bytes']), int(old[name]['tx_bytes'])
        if rx < old_rx or tx < old_tx:
            raise ValueError('network interface counter reset')
        received += rx - old_rx
        sent += tx - old_tx
    # Arbitrary precision integer cross multiplication is the gate authority.
    # The minimum possible interval includes every counter-read uncertainty;
    # response latency and decimal formatting cannot hide an above-limit rate.
    within = (received + sent) * 1000 <= LIMIT_BYTES_PER_SECOND * interval
    return {'status': 'within' if within else 'above', 'delta_rx_bytes': str(received),
            'delta_tx_bytes': str(sent), 'minimum_interval_ms': interval, 'within_limit': within}


def counter_program():
    return '''scan=$(sh -c "$1" sh /proc) || exit 1
set -- $scan
[ "$#" -eq 4 ] || exit 1
main_pid="$1"; shadow_pid="$3"
namespace=$(readlink /proc/self/ns/net) || exit 1
inode=${namespace#net:[}; inode=${inode%]}
# One bounded process snapshot and one stat process cover all namespaces.
# Forking readlink once per PID made the observer consume the idle CPU budget.
# Missing live processes still fail closed; only vanished helpers may disappear.
set -- /proc/[0-9]*/ns/net
observed=$(stat -Lc '%n:%i' "$@" 2>/dev/null)
for process_namespace do
  case "
$observed
" in
    *"
$process_namespace:$inode
"*) ;;
    *)
      process_root=${process_namespace%/ns/net}
      [ -d "$process_root" ] || continue
      # Zombies retain a PID but no network namespace or running work.
      # An unreadable live process is not an absent namespace.
      IFS= read -r process_stat < "$process_root/stat" || {
        [ ! -d "$process_root" ] && continue
        exit 1
      }
      case "${process_stat##*) }" in "Z "*) ;; *) exit 1 ;; esac
      ;;
  esac
done
for required_pid in "$main_pid" "$shadow_pid"; do
  case "
$observed
" in
    *"
/proc/$required_pid/ns/net:$inode
"*) ;;
    *) exit 1 ;;
  esac
done
printf "%s\n%s\n" "$namespace" "$scan"
cat /proc/net/dev'''


def read_owned_worker_counter(owner_pid, read_fd, write_fd, deadline_ms):
    # Bash coprocess descriptors are close-on-exec. Duplicate only the named
    # caller-owned pipes, rather than leaking every worker FD into every child.
    descriptors = []
    try:
        for descriptor, mode in [(read_fd, os.O_RDONLY), (write_fd, os.O_WRONLY)]:
            if owner_pid <= 0 or descriptor < 3:
                raise ValueError('invalid caller-owned worker descriptor')
            descriptors.append(os.open(f'/proc/{owner_pid}/fd/{descriptor}', mode | os.O_NONBLOCK))
            if not stat.S_ISFIFO(os.fstat(descriptors[-1]).st_mode):
                raise ValueError('counter reader descriptor is not a caller-owned pipe')
        return read_worker_counter(*descriptors, deadline_ms)
    finally:
        for descriptor in descriptors:
            os.close(descriptor)


def read_worker_counter(read_fd, write_fd, deadline_ms):
    # Reuse the exact selected container's existing bounded read-only worker.
    # The nonce rejects a stale/partial response; all sampling still follows
    # the caller's stats event and inherited absolute deadline.
    nonce = f'net-{os.getpid()}-{time.monotonic_ns()}'
    os.write(write_fd, (nonce + '\n').encode())
    data = b''
    deadline_ms = min(deadline_ms, monotonic_ms() + 5000)
    while True:
        remaining = (deadline_ms - monotonic_ms()) / 1000
        if remaining <= 0 or not select.select([read_fd], [], [], remaining)[0]:
            raise ValueError('persistent network counter worker exceeded the inherited deadline')
        chunk = os.read(read_fd, 65536)
        if not chunk:
            raise ValueError('persistent network counter worker exited before a complete response')
        data += chunk
        if len(data) > 65536:
            raise ValueError('network counter response exceeds bound')
        if data.endswith((nonce + '\t0\n').encode()):
            lines = data.decode().splitlines()
            if lines[0] != nonce:
                raise ValueError('network counter worker response nonce differs')
            return '\n'.join(lines[1:-1]) + '\n'
        if data.endswith(b'\n') and data.splitlines()[-1].startswith((nonce + '\t').encode()):
            raise ValueError('persistent network counter worker could not cover every live namespace')


def read_counter(member, project, deadline_ms, generation_scanner, reader=None):
    def command(argv):
        remaining = (deadline_ms - monotonic_ms()) / 1000
        if remaining <= 0:
            raise ValueError('quiet absolute deadline exhausted')
        result = subprocess.run(argv, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                timeout=min(remaining, 5), check=False)
        if result.returncode:
            raise ValueError('read-only Docker counter command unavailable: ' + result.stderr.strip()[:300])
        if len(result.stdout) > 65536:
            raise ValueError('network counter response exceeds bound')
        return result.stdout
    container = member['container_id']
    if not re.fullmatch(r'[a-f0-9]{64}', container):
        raise ValueError('expected exact selected container ID')
    # Read no environment, mount payload, credentials, or unrelated container.
    template = '{"id":{{json .Id}},"project":{{json (index .Config.Labels "com.docker.compose.project")}},"started_at":{{json .State.StartedAt}},"running":{{json .State.Running}},"network_mode":{{json .HostConfig.NetworkMode}},"pid_mode":{{json .HostConfig.PidMode}}}'
    identity = json.loads(command(['docker', 'inspect', '--format', template, container]))
    if identity['id'] != container or identity['project'] != project or not identity['running']:
        raise ValueError('counter container is not this running Compose project')
    if (identity['network_mode'] == 'host' or identity['network_mode'].startswith('container:')
            or identity['pid_mode'] == 'host' or identity['pid_mode'].startswith('container:')):
        raise ValueError('shared host/container network or PID namespace is unsupported for isolated counter coverage')
    begin = monotonic_ms()

    raw = (read_owned_worker_counter(*reader, deadline_ms) if reader else
           command(['docker', 'exec', container, 'sh', '-c', counter_program(), 'sh', generation_scanner]))
    closed = monotonic_ms() + UPTIME_QUANTUM_MS
    namespace, generations, interfaces = parse_counters(raw)
    return {'container_id': container, 'container_started_at': identity['started_at'],
            'capture_started_ms': begin, 'capture_closed_ms': closed,
            'namespace': namespace, 'generations': list(generations), 'interfaces': interfaces,
            'total_rx_bytes': str(sum(int(row['rx_bytes']) for row in interfaces.values())),
            'total_tx_bytes': str(sum(int(row['tx_bytes']) for row in interfaces.values())),
            'raw_proc_net_dev': raw, 'collector_command_completed': True}


def sample_group(members, project, deadline_ms, previous, generation_scanner, readers=None):
    if not members or len({row['node'] for row in members}) != len(members):
        raise ValueError('missing or repeated selected node')
    if len({row['container_id'] for row in members}) != len(members):
        raise ValueError('repeated selected container')
    if previous and (previous.get('schema') != 'sinomo.test.container-network-counters.v1'
                     or previous.get('project') != project):
        raise ValueError('previous counter group is not this project/schema')
    output = {'schema': 'sinomo.test.container-network-counters.v1', 'project': project,
              'max_combined_bytes_per_second': LIMIT_BYTES_PER_SECOND,
              'interfaces': 'all including loopback', 'members': {}}
    def sample(member):
        current = {'container_id': member['container_id']}
        try:
            current = read_counter(member, project, deadline_ms, generation_scanner,
                                   (readers or {}).get(member['node']))
            current.update(counter_delta(current, previous.get('members', {}).get(member['node'])))
        except (ValueError, TypeError, OSError, subprocess.SubprocessError, KeyError) as error:
            current.update(status='unavailable', error=str(error), within_limit=None)
        return member['node'], current
    # One read group follows the existing stats event, never a new timer.
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(members)) as pool:
        output['members'] = dict(pool.map(sample, members))
    return output


def main():
    if os.sys.argv[1:] == ['--print-program']:
        print(counter_program())
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--containers', required=True, type=Path)
    parser.add_argument('--project', required=True)
    parser.add_argument('--deadline-ms', type=int, required=True)
    parser.add_argument('--previous', type=Path)
    parser.add_argument('--generation-scanner', required=True, type=Path)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--reader', nargs=4, action='append', default=[])
    args = parser.parse_args()
    readers = {node: (int(owner), int(read_fd), int(write_fd)) for node, owner, read_fd, write_fd in args.reader}
    if len(readers) != len(args.reader):
        raise ValueError('duplicate persistent counter reader')
    previous = json.loads(args.previous.read_text()) if args.previous else {}
    members = json.loads(args.containers.read_text())
    result = sample_group(members, args.project, args.deadline_ms, previous, args.generation_scanner.read_text(), readers)
    # Preserve all raw endpoints and every failing/unavailable row before the
    # caller classifies the gate. Counters remain decimal strings losslessly.
    args.output.write_text(json.dumps(result, sort_keys=True, indent=2) + '\n')
    for node, row in result['members'].items():
        print('\t'.join(str(value) if value is not None else 'unavailable' for value in (
            node, row.get('capture_started_ms'), row.get('capture_closed_ms'), row['status'],
            row.get('delta_rx_bytes'), row.get('delta_tx_bytes'), row.get('minimum_interval_ms'),
            row.get('namespace'), row.get('total_rx_bytes'), row.get('total_tx_bytes'))))


if __name__ == '__main__':
    main()
