#!/usr/bin/env python3
"""Build the model-free Android codec probe APK and DEX JAR; never install or grant permissions."""
import argparse
import hashlib
import json
import shutil
import os
from pathlib import Path
import subprocess
import zipfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--sdk', type=Path, default=os.environ.get('ANDROID_HOME'))
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--keystore', type=Path, required=True)
parser.add_argument('--password-env', required=True)
parser.add_argument('--image-binary', type=Path)
parser.add_argument('--baseline-image-binary', type=Path)
args = parser.parse_args()
if args.password_env not in os.environ:
    parser.error('signing password environment variable is not set')
if args.sdk is None:
    parser.error('--sdk or ANDROID_HOME is required')
classes = args.output / 'classes'
dex = args.output / 'dex'
classes.mkdir(parents=True, exist_ok=True)
dex.mkdir(parents=True, exist_ok=True)
jar = args.sdk / 'platforms/android-35/android.jar'
tools = args.sdk / 'build-tools/35.0.0'
subprocess.run(['javac', '--release', '8', '-classpath', str(jar), '-d', str(classes), *map(str, Path(__file__).parent.glob('*.java'))], check=True)
subprocess.run([str(tools / 'd8'), '--lib', str(jar), '--min-api', '29', '--output', str(dex), *map(str, classes.rglob('*.class'))], check=True)
with zipfile.ZipFile(args.output / 'codec-probe.jar', 'w') as output:
    output.write(dex / 'classes.dex', 'classes.dex')
print(args.output / 'codec-probe.jar')

subprocess.run([str(tools / 'aapt2'), 'link', '-I', str(jar), '--manifest', str(Path(__file__).with_name('AndroidManifest.xml')), '-o', str(args.output / 'unsigned.apk')], check=True)
with zipfile.ZipFile(args.output / 'unsigned.apk', 'a') as output:
    output.write(dex / 'classes.dex', 'classes.dex')
    if args.image_binary:
        output.write(args.image_binary, 'lib/arm64-v8a/libimagebench.so')
    if args.baseline_image_binary:
        output.write(args.baseline_image_binary, 'lib/arm64-v8a/libimagebaseline.so')
subprocess.run([str(tools / 'zipalign'), '-f', '4', str(args.output / 'unsigned.apk'), str(args.output / 'aligned.apk')], check=True)
subprocess.run([str(tools / 'apksigner'), 'sign', '--ks', str(args.keystore), '--ks-pass', 'env:' + args.password_env, '--out', str(args.output / 'codec-probe.apk'), str(args.output / 'aligned.apk')], check=True)
subprocess.run([str(tools / 'apksigner'), 'verify', str(args.output / 'codec-probe.apk')], check=True)

source_copy = args.output / 'source'
source_copy.mkdir(exist_ok=True)
source_files = [*Path(__file__).parent.glob('*.java'), Path(__file__).with_name('AndroidManifest.xml')]
for source in source_files:
    shutil.copyfile(source, source_copy / source.name)
(args.output / 'receipt.json').write_text(json.dumps({
    'apk_sha256': hashlib.file_digest((args.output / 'codec-probe.apk').open('rb'), 'sha256').hexdigest(),
    'sources': {source.name: hashlib.file_digest(source.open('rb'), 'sha256').hexdigest() for source in source_files},
    'image_binary_sha256': hashlib.file_digest(args.image_binary.open('rb'), 'sha256').hexdigest() if args.image_binary else None,
}, indent=2) + '\n')
