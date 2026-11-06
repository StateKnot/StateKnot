#!/usr/bin/env python3
"""Publish bounded original evidence and the four verified test sources."""
import gzip
import hashlib
import json
from pathlib import Path
import subprocess

BASE = Path(__file__).resolve().parent
REPO = BASE.parents[1]
NAME = 'experimental-frame-return-process-faults-2026-10-16'
OUT = REPO / 'docs/qualification/evidence' / NAME
SOURCE = BASE / 'graph-frame-return-process-fault-source-v3'
PATHS = [
    'crates/stateknot-runtime/tests/postgres/commit_proxy.rs',
    'crates/stateknot-runtime/tests/postgres/nested_driver.rs',
    'crates/stateknot-runtime/tests/postgres/frame_return_process_faults.rs',
    'crates/stateknot-store-postgres/tests/postgres/frame_barriers.rs',
]
sha = lambda b: hashlib.sha256(b).hexdigest()
canon = lambda x: json.dumps(x, sort_keys=True, separators=(',', ':')).encode()
rows = []


def save(raw, destination, origin):
    target = OUT / destination
    target.parent.mkdir(parents=True, exist_ok=True)
    assert not target.exists()
    compress = destination.endswith('.gz')
    data = gzip.compress(raw, mtime=0) if compress else raw
    target.write_bytes(data)
    rows.append({'path': destination, 'origin': origin, 'raw_bytes': len(raw), 'raw_sha256': sha(raw),
                 'stored_bytes': len(data), 'stored_sha256': sha(data), 'gzip': compress})


def copy_file(path, destination):
    save(path.read_bytes(), destination, str(path.relative_to(BASE)))


def directory(relative, destination, exclude=()):
    for path in sorted((BASE / relative).iterdir()):
        if path.is_file() and path.name not in exclude:
            suffix = '.gz' if path.suffix in {'.log', '.stdout', '.stderr', '.jsonl'} or path.name == 'freshness.json' else ''
            copy_file(path, destination + '/' + path.name + suffix)


def main():
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO).decode().strip() == 'a544ad9bfcb516a1ac7a6b8232c66019b46ad1f5'
    OUT.mkdir(exist_ok=False)
    for package in PATHS:
        copy_file(SOURCE / package, 'source/' + package + '.gz')
    for version in ['v1', 'v2']:
        directory('graph-frame-return-process-fault-qualification-' + version, 'history/' + version,
                  exclude=tuple(p.name for p in (BASE / ('graph-frame-return-process-fault-qualification-' + version)).iterdir()
                                if p.name not in {'clippy.receipt.json', 'clippy.log', 'fmt.receipt.json', 'fmt.log'}))
    directory('graph-frame-return-process-fault-qualification-v3', 'gates-v3', exclude=('gate.lock',))
    directory('graph-frame-return-process-fault-qualification-v3-network-enabled', 'gates-v3-network-enabled', exclude=('gate.lock',))
    for name in ['graph-frame-return-process-fault-v3-gate-aggregate-v1', 'graph-frame-return-process-fault-v3-increment-review-v1',
                 'graph-frame-return-process-fault-spec-review-v1', 'graph-frame-return-process-fault-v2-increment-review-v1',
                 'graph-frame-return-process-fault-v3-independent-review-v1']:
        directory('evidence/' + name, 'reviews/' + name)
    for package in ['store', 'runtime']:
        directory('graph-frame-return-process-fault-native-v3/build-candidate-' + package, 'native/build-' + package)
        directory('graph-frame-return-process-fault-native-v3/pg16-' + package + '-exact', 'native/pg16-' + package)
        directory('graph-frame-return-process-fault-native-v4/pg17-' + package + '-exact-network-enabled', 'native/pg17-' + package)
    directory('graph-frame-return-process-fault-native-v4/pg17-store-exact', 'history/pg17-sandbox-inspect')
    directory('graph-frame-return-process-fault-native-v3/exact-four-execution-v1', 'history/v3-exact-driver')
    for version in ['v3', 'v4']:
        directory('graph-frame-return-process-fault-native-runner-' + version, 'native/runner-' + version)
    copy_file(BASE / 'graph-frame-return-process-fault-native-v3/run-exact-four-v1.py', 'native/run-exact-four-v1.py')
    copy_file(BASE / 'graph-frame-return-process-fault-native-v4/verify-four-v1.py', 'native/verify-four-v1.py')
    copy_file(BASE / 'graph-frame-return-process-fault-native-v4/four-case-offline-qualification-v1/qualification.receipt.json', 'native/supplementary-four-case.receipt.json')
    supplied = json.loads((BASE / 'graph-frame-return-process-fault-qualification-v3/compiled-inputs.json').read_bytes())
    assert supplied['count'] == len(supplied['files']) == 541 and sha(canon(supplied['files'])) == supplied['sha256']
    changed = []
    for row in supplied['files']:
        old = subprocess.run(['git', 'show', 'HEAD:' + row['path']], cwd=REPO, capture_output=True)
        if old.returncode or (len(old.stdout), sha(old.stdout)) != (row['bytes'], row['sha256']):
            changed.append(row['path'])
    assert set(changed) == set(PATHS)
    selected = json.loads((BASE / 'graph-frame-return-process-fault-qualification-v3/frozen-source-manifest.json').read_bytes())
    full_changes = []
    for row in selected['files']:
        old = subprocess.run(['git', 'show', 'HEAD:' + row['path']], cwd=REPO, capture_output=True)
        if old.returncode or (len(old.stdout), sha(old.stdout)) != (row['bytes'], row['sha256']):
            full_changes.append(row['path'])
    assert set(full_changes) == set(PATHS + ['CHANGELOG.md', 'docs/a2a-server.md', 'docs/a2a-server.zh-CN.md'])
    manifest = {'base_commit': 'a544ad9bfcb516a1ac7a6b8232c66019b46ad1f5', 'source_tree': selected['source_tree_sha256'],
                'selected_source_count': len(selected['files']), 'supplied_compilation_count': 541,
                'supplied_compilation_sha256': supplied['sha256'], 'adopted_test_paths': PATHS,
                'selected_tree_differences_from_base': full_changes,
                'excluded_older_noncompilation_docs': ['CHANGELOG.md', 'docs/a2a-server.md', 'docs/a2a-server.zh-CN.md'],
                'production_Rust_SQL_API_or_migrations_changed': False,
                'six_gate_classes_passed_in_two_plus_four_commands': True, 'single_six_driver_passed': False,
                'workspace_passed': 2121, 'workspace_original_ignored': 6, 'doctests_passed': 48,
                'actual_Native_exact_cases': 4, 'actual_return_cuts_per_runtime_case': 2,
                'original_pg16_runtime_native_exit': 0, 'original_pg16_runtime_wrapper_exit': 1,
                'pg16_runtime_offline_supplement_not_rewrite': True,
                'full_Native_CI_capacity_or_R1_qualification': False}
    save((json.dumps(manifest, indent=2) + '\n').encode(), 'manifest.json', 'new publication scope')
    save(Path(__file__).read_bytes(), 'package-originals.py', str(Path(__file__).relative_to(BASE)))
    (OUT / 'artifact-index.json').write_text(json.dumps({'files': rows}, indent=2) + '\n')
    for path in PATHS:
        (REPO / path).write_bytes((SOURCE / path).read_bytes())
    print(json.dumps({'evidence_files': len(rows), 'adopted_test_paths': PATHS, 'manifest_sha256': sha((OUT / 'manifest.json').read_bytes()),
                      'artifact_index_sha256': sha((OUT / 'artifact-index.json').read_bytes())}))


if __name__ == '__main__':
    main()
