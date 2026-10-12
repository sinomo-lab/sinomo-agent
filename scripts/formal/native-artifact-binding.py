"""Verify the existing six-field native runner binding against actual artifacts."""
import hashlib
import json
from pathlib import Path
import re

FIELDS = (
    'implementation_revision', 'implementation_build_id', 'executable_sha256',
    'source_manifest_sha256', 'schema_sha256', 'model_sha256',
)


def pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError(f'duplicate native artifact key {key}')
        result[key] = value
    return result


def binding(value):
    if not isinstance(value, dict) or set(value) != set(FIELDS):
        raise ValueError('native artifact binding must contain exactly the six runner fields')
    if any(not isinstance(value[key], str) or not value[key] for key in FIELDS):
        raise ValueError('native artifact binding fields must be nonempty strings')
    if not re.fullmatch(r'[0-9a-f]{40}', value['implementation_revision']):
        raise ValueError('native artifact source revision is not canonical')
    for key in FIELDS[2:]:
        if not re.fullmatch(r'[0-9a-f]{64}', value[key]):
            raise ValueError(f'native artifact {key} is not canonical SHA256')
    return {key: value[key] for key in FIELDS}


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as source:
        for chunk in iter(lambda: source.read(65536), b''):
            digest.update(chunk)
    return digest.hexdigest()


def verify_manifest(manifest_path, source_manifest_path, executable=None):
    manifest_path = Path(manifest_path)
    manifest = json.loads(manifest_path.read_text(), object_pairs_hook=pairs)
    if manifest.get('artifact_version') != 1 or not manifest.get('captures'):
        raise ValueError('native capture manifest is missing its actual capture roster')
    expected = binding(manifest.get('artifact_binding'))
    actual_executable = Path(executable or manifest.get('actual_executable', ''))
    if not str(actual_executable) or not actual_executable.is_file():
        raise FileNotFoundError('the actual captured executable is unavailable')
    if file_sha256(actual_executable) != expected['executable_sha256']:
        raise ValueError('actual executable differs from the native capture binding')
    if file_sha256(source_manifest_path) != expected['source_manifest_sha256']:
        raise ValueError('actual frozen source manifest differs from the native capture binding')
    labels = set()
    for capture in manifest['captures']:
        header_path = manifest_path.parent / capture['header_file']
        raw_path = manifest_path.parent / capture['raw_file']
        header = json.loads(header_path.read_text(), object_pairs_hook=pairs)
        raw = json.loads(raw_path.read_text(), object_pairs_hook=pairs)
        if binding(header.get('artifact_binding')) != expected:
            raise ValueError('native raw header and actual capture manifest bindings differ')
        if header.get('actual_mount_label') != raw.get('actual_mount_label'):
            raise ValueError('native raw header borrows another capture mount')
        if raw.get('actual_mount_root') != capture['actual_data_root']:
            raise ValueError('native capture manifest borrows another physical data root')
        label = raw['actual_mount_label']
        if label in labels:
            raise ValueError('duplicate native mount in artifact manifest')
        labels.add(label)
    return expected, manifest
