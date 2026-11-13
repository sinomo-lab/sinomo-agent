#!/usr/bin/env python3
"""Exercise the production Android JNI decoder adapter on identical H264 frames."""
import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import uuid
import zipfile

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--library', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
root = Path(__file__).resolve().parents[3]
sdk = Path(os.environ.get('ANDROID_HOME', str(Path.home() / '.local/share/android-sdk')))
ndk = Path(os.environ.get('ANDROID_NDK_HOME', str(sdk / 'ndk/27.2.12479018')))
compiler = ndk / 'toolchains/llvm/prebuilt/linux-x86_64'
a.output.mkdir(parents=True, exist_ok=True)
classes = a.output / 'classes'
classes.mkdir(exist_ok=True)
android_jar = sdk / 'platforms/android-35/android.jar'
source = root / 'crates/sinomo-app/java/net/sinomo/app/runtime'
def run(*args, **kw):
    result = subprocess.run(list(map(str, args)), capture_output=True, text=True, **kw)
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result
run('javac', '--release', '8', '-classpath', android_jar, '-d', classes,
    source / 'SinomoVideoDecoder.java', source / 'SinomoVideoEncoder.java',
    Path(__file__).parent / 'decoder-probe/DecoderProbe.java')
run(sdk / 'build-tools/35.0.0/d8', '--lib', android_jar, '--min-api', '29',
    '--output', a.output, *sorted(classes.rglob('*.class')))
jar = a.output / 'probe.jar'
with zipfile.ZipFile(jar, 'w') as z:
    z.write(a.output / 'classes.dex', 'classes.dex')
lib = a.output / 'libprobe.so'
run(compiler / 'bin/llvm-strip', '--strip-debug', '-o', lib, a.library)
remote = '/data/local/tmp/sinomo-decoder-' + uuid.uuid4().hex
try:
    run('adb', 'shell', 'mkdir', remote)
    for path in [jar, lib, compiler / 'sysroot/usr/lib/aarch64-linux-android/libc++_shared.so']:
        run('adb', 'push', path, remote + '/' + path.name)
    command = ['env', 'CLASSPATH=' + remote + '/probe.jar', 'LD_LIBRARY_PATH=' + remote,
               'app_process', '/system/bin', 'net.sinomo.probe.DecoderProbe', remote + '/libprobe.so', remote]
    result = subprocess.run(['adb', 'shell', shlex.join(command)], capture_output=True, text=True, timeout=120)
    (a.output / 'stdout.log').write_text(result.stdout)
    (a.output / 'stderr.log').write_text(result.stderr)
    for name in ['SOFTWARE', 'FALLBACK', 'HARDWARE']:
        rows = [line[len(name)+1:] for line in result.stdout.splitlines() if line.startswith(name + ' ')]
        if len(rows) == 1:
            (a.output / (name.lower() + '.json')).write_text(json.dumps(json.loads(rows[0]), indent=2))
    if result.returncode:
        raise RuntimeError('Device decoder probe failed; see stdout.log and stderr.log')
finally:
    run('adb', 'shell', 'rm', '-rf', remote)
print(a.output)
