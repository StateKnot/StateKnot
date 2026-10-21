#!/usr/bin/env python3
"""Verify archived bytes and actual outputs; optionally compare supplied workspace inputs."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent
sha = lambda b: hashlib.sha256(b).hexdigest()
canon = lambda x: json.dumps(x, sort_keys=True, separators=(',', ':')).encode()


def read(name):
    path = ROOT / name
    raw = path.read_bytes()
    return gzip.decompress(raw) if path.suffix == '.gz' else raw


def obj(name):
    return json.loads(read(name))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workspace-root', type=Path)
    args = parser.parse_args()
    index = obj('artifact-index.json')['files']
    assert len({r['path'] for r in index}) == len(index)
    for row in index:
        path = Path(row['path'])
        assert not path.is_absolute() and '..' not in path.parts
        stored = (ROOT / path).read_bytes()
        raw = gzip.decompress(stored) if row['gzip'] else stored
        assert (len(stored), sha(stored)) == (row['stored_bytes'], row['stored_sha256'])
        assert (len(raw), sha(raw)) == (row['raw_bytes'], row['raw_sha256'])
    manifest = obj('manifest.json')
    source = obj('gates-v3/frozen-source-manifest.json')
    supplied = obj('gates-v3/compiled-inputs.json')
    assert source['count'] == len(source['files']) == 932
    assert sha(canon(source['files'])) == source['source_tree_sha256'] == manifest['source_tree']
    assert supplied['count'] == len(supplied['files']) == 541
    assert supplied['files'] == sorted(supplied['files'], key=lambda row: row['path'])
    assert sha(canon(supplied['files'])) == supplied['sha256'] == manifest['supplied_compilation_sha256']
    source_rows = {row['path']: row for row in source['files']}
    assert len(source_rows) == 932 and all(source_rows[row['path']] == row for row in supplied['files'])
    for name in manifest['adopted_test_paths']:
        raw = read('source/' + name + '.gz')
        assert (len(raw), sha(raw)) == (source_rows[name]['bytes'], source_rows[name]['sha256'])
    assert not (set(manifest['excluded_older_noncompilation_docs']) & {row['path'] for row in supplied['files']})
    counts = {}
    for gate in ['fmt', 'clippy', 'workspace', 'doctests', 'rustdoc', 'nightly']:
        folder = 'gates-v3' if gate in ['fmt', 'clippy'] else 'gates-v3-network-enabled'
        receipt = obj(folder + '/' + gate + '.receipt.json')
        raw = read(folder + '/' + gate + '.log.gz')
        assert receipt['exit_code'] == 0
        assert receipt['source_before_after_exact'] is True and receipt['source_tree'] == manifest['source_tree']
        assert receipt['log_sha256'] == sha(raw)
        if gate in ['workspace', 'doctests']:
            summaries = re.findall(rb'^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;', raw, re.M)
            counts[gate] = [len(summaries)] + [sum(int(row[i]) for row in summaries) for i in range(5)]
    assert counts == {'workspace': [100, 2121, 0, 6, 0, 0], 'doctests': [10, 48, 0, 0, 0, 0]}
    cases = {'store': 'frame_barriers::nested_entry_after_scoped_barrier_authenticates_parent_successor_and_usage_floor',
             'runtime': 'nested_driver::frame_return_process_faults::frame_return_commit_loss_survives_real_kill_and_cold_higher_fence_driver'}
    for major in [16, 17]:
        for package, count in [('store', 264), ('runtime', 191)]:
            folder = 'native/pg' + str(major) + '-' + package
            receipt = obj(folder + '/test.receipt.json')
            assert receipt['exit_code'] == 0 and receipt['timed_out'] is False
            assert receipt['full_inventory_count'] == count and receipt['expected_filtered'] == count - 1
            assert receipt['argv'][1:] == [cases[package], '--exact', '--test-threads=1', '--nocapture']
            output = read(folder + '/test.stdout.gz')
            for stream in ['test.stdout', 'test.stderr']:
                recorded = receipt['output_streams'][stream]
                assert recorded['saved_is_raw'] is True and recorded['redactions'] == 0
                assert sha(read(folder + '/' + stream + '.gz')) == recorded['saved_sha256'] == recorded['raw_sha256']
            summary = re.findall(rb'^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;', output, re.M)
            assert summary == [(b'1', b'0', b'0', b'0', str(count - 1).encode())]
            for scope in ['server', 'database']:
                before = obj(folder + '/' + scope + '-settings-before.stdout.gz')
                assert before == obj(folder + '/' + scope + '-settings-after.stdout.gz')
                assert int(before['server_version_num']) // 10000 == major
                assert all(before[k] == 'on' for k in ['fsync', 'synchronous_commit', 'full_page_writes'])
            if package == 'runtime':
                prefix = 'STATEKNOT_FRAME_RETURN_COMMIT_LOSS_EVIDENCE='
                markers = [json.loads(line[len(prefix):]) for line in output.decode().splitlines() if line.startswith(prefix)]
                assert len(markers) == 2 and {m['cut'] for m in markers} == {'commit_not_forwarded', 'commit_response_withheld'}
                for marker in markers:
                    committed = marker['cut'] == 'commit_response_withheld'
                    assert marker['profile'] == 'frame-return-commit-loss-v1'
                    assert marker['commit_forwarded'] is committed and marker['original_return_recovered'] is committed
                    assert marker['commit_response_forwarded'] is False
                    assert marker['writer_killed_and_reaped'] is True and marker['cold_higher_fence_driver_verified'] is True
                    assert marker['leaf_application_redispatches'] == 0 and marker['parent_application_calls'] == 1
                    assert marker['invariants'] == 'passed' and marker['termination'] == 'SIGKILL'
    assert obj('native/pg16-runtime/observations.json')['exact_case_passed'] is False
    assert obj('native/supplementary-four-case.receipt.json')['all_four_actual_native_cases_qualified'] is True
    if args.workspace_root:
        for row in supplied['files']:
            path = args.workspace_root / row['path']
            assert not path.is_symlink() and (path.stat().st_size, sha(path.read_bytes())) == (row['bytes'], row['sha256']), row['path']
    print(json.dumps({'artifact_files_verified': len(index), 'six_gate_classes': True, 'actual_Native_exact_cases': 4,
                      'supplied_workspace_inputs_checked': bool(args.workspace_root),
                      'Rust_DB_CI_executed': False, 'full_CI_or_R1_qualified': False}))


if __name__ == '__main__':
    main()
