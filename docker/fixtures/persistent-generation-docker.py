#!/usr/bin/env python3
import os,sys,time
from pathlib import Path
assert sys.argv[1:3] == ['exec','-i'], sys.argv
container=sys.argv[3]
root=Path(os.environ['PROC_FIXTURES'])
with (root/'launches').open('a') as output:
    output.write(f'{container}\t{os.getpid()}\n')
mode=os.environ.get('PERSISTENT_GENERATION_DOCKER_MODE','normal')
if mode == 'die': sys.exit(7)
if mode == 'malformed':
    request=sys.stdin.readline().rstrip('\n')
    print(f'{request}\t28\tbad\t1\t101',flush=True)
    sys.stdin.read(); sys.exit(0)
if mode == 'closed_stdin':
    os.close(0); time.sleep(10); sys.exit(0)
if mode == 'zero_pid':
    request=sys.stdin.readline().rstrip('\n')
    print(f'{request}\t0\t202\t1\t101',flush=True)
    sys.stdin.read(); sys.exit(0)
if mode == 'same_pid':
    request=sys.stdin.readline().rstrip('\n')
    print(f'{request}\t28\t202\t28\t101',flush=True)
    sys.stdin.read(); sys.exit(0)
if mode == 'stale':
    request=sys.stdin.readline()
    print('0-0\t28\t202\t1\t101',flush=True)
    sys.stdin.read(); sys.exit(0)
if mode == 'stall':
    sys.stdin.readline(); time.sleep(10); sys.exit(0)
args=sys.argv[4:]
args[-2]=str(root/container)
os.execvp(args[0],args)
