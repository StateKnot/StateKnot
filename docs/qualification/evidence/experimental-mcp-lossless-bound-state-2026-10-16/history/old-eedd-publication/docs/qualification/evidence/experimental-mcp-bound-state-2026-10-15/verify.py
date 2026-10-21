#!/usr/bin/env python3
"""Offline component evidence check; --workspace also checks actual adopted code/CI."""
from pathlib import Path
import argparse, collections, difflib, gzip, hashlib, io, json, re, tarfile

HERE = Path(__file__).resolve().parent
TREE = 'eeddc9f7e9c92023a0b0c1f9a7220cccc1b4c5d846ae4281d4a4aeff9b0f30f4'
BASE = '3a84c68eb9dd9df1804a7e24801516e11c9ee01ca0c6f243a4fdfaf76cbe124c'
CI = '6568a90f4de078ef9f4bc7469e7891b72af7b00619301ee645dced64fea14fd9'
MARKER = 'STATEKNOT_MCP_BOUND_STATE_POSTGRES: consumers=3 positive_commits=6 refused_rebinds=12 tenant_ledgers=2 complete_journals=true'
SUMMARY = re.compile(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;')
sha = lambda b: hashlib.sha256(b).hexdigest()
canon = lambda value: json.dumps(value, sort_keys=True, separators=(',', ':')).encode()
load = lambda p: json.loads(p.read_text())

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workspace', action='store_true')
    parser.add_argument('--workspace-root', type=Path)
    args = parser.parse_args()
    repo = args.workspace_root.resolve() if args.workspace_root else HERE.parents[3]
    for v in load(HERE / 'manifest.json')['files']:
        b = (HERE / v['path']).read_bytes()
        assert len(b) == v['bytes'] and sha(b) == v['sha256'], v['path']
    for v in load(HERE / 'reports.json')['files']:
        b = (HERE.parents[3] / v['path']).read_bytes()
        assert len(b) == v['bytes'] and sha(b) == v['sha256'], v['path']
    entries = load(HERE / 'artifact-index.json')['artifacts']
    index = {v['path']: v for v in entries}; assert len(index) == len(entries)
    def raw(name):
        n = name if name in index or (HERE / name).exists() else name + '.gz'
        b = (HERE / n).read_bytes()
        if n in index:
            v = index[n]; assert len(b) == v['stored_bytes'] and sha(b) == v['stored_sha256']
            if v['storage_encoding'] == 'gzip':
                assert b[4:8] == b'\0' * 4 and not b[3] & 8
                b = gzip.decompress(b)
            else:
                assert v['storage_encoding'] == 'identity'
            assert len(b) == v['raw_bytes'] and sha(b) == v['raw_sha256']
        elif n.endswith('.gz'):
            b = gzip.decompress(b)
        return b
    obj = lambda name: json.loads(raw(name))
    for name in index:
        raw(name)
    refs = load(HERE / 'source/base-references.json')['files']
    for v in refs:
        b = (repo / v['path']).read_bytes(); assert len(b) == v['bytes'] and sha(b) == v['sha256']
    with tarfile.open(fileobj=io.BytesIO((repo / refs[0]['path']).read_bytes()), mode='r:gz') as archive:
        members = archive.getmembers()
        assert len(members) == len({m.name for m in members}) == 923
        assert all(m.isfile() and m.name.startswith('source/') and '..' not in Path(m.name).parts for m in members)
        base = {m.name[7:]: archive.extractfile(m).read() for m in members}
    parent_manifest = json.loads(gzip.decompress((repo / refs[1]['path']).read_bytes()))
    rows = lambda files, order: [{'path': v['path'], 'bytes': len(files[v['path']]), 'sha256': sha(files[v['path']])} for v in order]
    assert rows(base, parent_manifest['files']) == parent_manifest['files']
    assert sha(canon(parent_manifest['files'])) == '250066800739a33390a316c7fbb40181335de67394cd9de6bb67ccb425226433'
    base['crates/stateknot-runtime/tests/postgres/child_join_pitr.rs'] = gzip.decompress((repo / refs[2]['path']).read_bytes())
    assert sha(canon(rows(base, parent_manifest['files']))) == BASE
    def reconstruct(manifest, changes):
        files = dict(base)
        assert len(changes) == 12 and len({v['path'] for v in changes}) == 12
        assert sum(v['path'].startswith('crates/stateknot-integrations/src/') for v in changes) == 8
        assert sum(v['path'].startswith('crates/stateknot/tests/') for v in changes) == 4
        for v in changes:
            n = v['path']; assert (sha(base[n]) if n in base else None) == v['before_sha256']
            b = gzip.decompress((HERE / v['payload']).read_bytes())
            assert len(b) == v['bytes'] and sha(b) == v['after_sha256']; files[n] = b
        assert len(files) == len(manifest['files']) == 928 and set(files) == {v['path'] for v in manifest['files']}
        assert rows(files, manifest['files']) == manifest['files']
        assert sha(canon(manifest['files'])) == manifest['source_tree_sha256']
        return files
    for stage in load(HERE / 'source/historical-reconstruction.json')['stages']:
        m = obj('history/' + stage['stage'] + '/source-manifest.json')
        reconstruct(m, stage['changes_from3a84'])
    old_manifest = obj('history/default-parallel-be326/source-manifest.json')
    old = reconstruct(old_manifest, load(HERE / 'history/default-parallel-be326/reconstruction.json')['changes_from3a84'])
    manifest = obj('source/final-source-manifest.json')
    final_delta = load(HERE / 'source/final-reconstruction.json')
    files = reconstruct(manifest, final_delta['changes']); assert manifest['source_tree_sha256'] == TREE
    n = 'crates/stateknot/tests/mcp_bound_state/durable.rs'; before = old[n]
    pos = before.index(b'STATEKNOT_MCP_BOUND_STATE_POSTGRES: consumers=3')
    assert files[n] == before[:pos] + bytes([92, 110]) + before[pos:]
    assert [n for n in old if old[n] != files[n]] == [n]
    patch = ''
    for v in final_delta['changes']:
        n = v['path']; patch += ''.join(difflib.unified_diff(base.get(n, b'').decode().splitlines(True), files[n].decode().splitlines(True),
            fromfile='a/' + n if n in base else '/dev/null', tofile='b/' + n))
    assert gzip.decompress((HERE / 'source/candidate.patch.gz').read_bytes()) == patch.encode()
    supplied = load(HERE / 'source/compiled-inputs.json')
    expected = [v for v in manifest['files'] if v['path'].startswith(('crates/', '.cargo/')) or v['path'] in ['Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml']]
    assert supplied['files'] == expected and supplied['count'] == len(expected) == 536
    assert sha(canon(sorted(expected, key=lambda v: v['path']))) == supplied['canonical_sha256'] == '31b3be83c5a1d38e8204d6d2fd9caf7989206b93d401aeac4045149e7f298807'
    gates = obj('execution/six-gates-summary.json')
    assert gates['state'] == 'passed' and gates['gate_count'] == 6 and gates['source_tree'] == TREE and gates['all_source_before_after_exact']
    for v in gates['receipts']:
        name = v['gate']; r = obj('execution/gates/' + name + '.receipt.json'); b = raw('execution/gates/' + name + '.log')
        assert v == r and r['exit_code'] == 0 and r['source_before_after_exact'] and r['source_tree'] == TREE and sha(b) == r['log_sha256']
    for name, count, ignored, suites in [('workspace', 2095, 6, 99), ('doctests', 48, 0, 10)]:
        result = SUMMARY.findall(raw('execution/gates/' + name + '.log').decode())
        assert len(result) == suites and all(v[0] == 'ok' and v[2] == '0' for v in result)
        assert sum(int(v[1]) for v in result) == count and sum(int(v[3]) for v in result) == ignored
    build = obj('execution/native/build.receipt.json'); binary = obj('execution/native/binary.json')
    assert build['exit_code'] == 0 and sha(raw('execution/native/build.log')) == build['log_sha256']
    assert build['command'] == ['cargo', '+1.88.0', 'test', '-p', 'stateknot', '--test', 'mcp_bound_state', '--no-run', '--locked', '--message-format=json']
    events = [json.loads(v) for v in raw('execution/native/build.log').decode().splitlines() if v.startswith('{')]
    event = binary['cargo_artifact']; profile = event['profile']
    assert event in events and event['fresh'] is False and profile['opt_level'] == '0' and profile['debuginfo'] == 0 and profile['debug_assertions'] and profile['overflow_checks']
    names = re.findall(r'^(\S+): test$', raw('execution/native/list.log').decode(), re.M)
    assert names == binary['tests'] and len(names) == len(set(names)) == 14
    assert binary['sha256'] == '3d3b32d05d1b442c2e72953e5bdee2f70c0ac3119724ef747b4d3d54810ba2f6'
    native = obj('execution/native/summary.json'); assert native['state'] == 'passed' and native['source_tree'] == TREE
    for major in [16, 17]:
        r = obj(f'execution/native/pg{major}.receipt.json'); b = raw(f'execution/native/pg{major}.log')
        assert r['exit_code'] == 0 and r['command'][1:] == ['--nocapture', '--test-threads=1'] and r['source_tree'] == TREE and sha(b) == r['log_sha256']
        assert SUMMARY.findall(b.decode()) == [('ok', '14', '0', '0', '0', '0')]
        assert b.decode().splitlines().count(MARKER) == 1 and 'STATEKNOT_MCP_BOUND_STATE_POSTGRES_SKIPPED' not in b.decode()
        database = obj(f'execution/native/pg{major}-database.json')
        assert database['new_absent_database_created'] and database['no_database_dropped'] and database['database'] == 'stateknot_r1_mcp_bound_20261015v3'
    for v in load(HERE / 'execution/native/input-checks.json')['files']:
        b = v['raw_text'].encode(); assert sha(b) == v['raw_sha256'] and len(b) == v['raw_bytes']
        check = json.loads(b); assert check['source_tree'] == TREE and check['source_files'] == 928 and check['exact']
    for prefix, exit_key in [('history/first-clippy/first-clippy', 'exit_code'), ('history/second-clippy/retry-clippy', 'exit_code'),
                             ('history/workspace-HTTP-fixture/retry-workspace', 'exit_code'), ('history/workspace-HTTP-fixture/minimized-repro', 'exit_code'),
                             ('history/native-invalid-event-kind/native/pg16', 'exit_code')]:
        r = obj(prefix + '.receipt.json'); assert r[exit_key] == 101 and sha(raw(prefix + '.log')) == r['log_sha256']
    assert b'8 passed; 6 failed;' in raw('history/workspace-HTTP-fixture/retry-workspace.log')
    assert b'13 passed; 1 failed;' in raw('history/native-invalid-event-kind/native/pg16.log')
    serial = obj('history/serial-marker-red/receipt.json'); old_log = raw('history/serial-marker-red/test.log')
    assert serial['product_exit_code'] == 0 and serial['anchored_marker_exact_lines'] == 0 and sha(old_log) == serial['log_sha256']
    assert old_log.decode().splitlines().count(MARKER) == 0 and SUMMARY.findall(old_log.decode()) == [('ok', '1', '0', '0', '0', '13')]
    for major in [16, 17]:
        r = obj(f'history/default-parallel-be326/native/pg{major}.receipt.json'); b = raw(f'history/default-parallel-be326/native/pg{major}.log')
        assert r['exit_code'] == 0 and r['command'][1:] == ['--nocapture'] and sha(b) == r['log_sha256']
        assert SUMMARY.findall(b.decode()) == [('ok', '14', '0', '0', '0', '0')]
    resource = obj('execution/final-resource-observation.json')
    assert resource['source_tree'] == TREE and resource['no_cleanup_or_database_lifecycle_action']
    assert len(resource['checks']) == 3 and all(v['lsof_exit'] == 1 and v['no_open_files'] and not v['cleanup_performed'] for v in resource['checks'])
    assert resource['available_kib'] > 50 * 1024 ** 2 and resource['task_cache_logical_kib'] <= 20 * 1024 ** 2
    candidate = raw('CI-candidate/candidate.yml'); ci = obj('CI-candidate/receipt.json')
    assert sha(candidate) == ci['candidate_sha256'] == CI and ci['baseline_postgres_steps'] == 53 and ci['candidate_postgres_steps'] == 55 and ci['candidate_postgres_run_steps'] == 32 and ci['postgres_job_timeout_minutes'] == 30
    text = candidate.decode().splitlines(); start = text.index('  postgres:'); stop = next((i for i in range(start + 1, len(text)) if text[i].startswith('  ') and not text[i].startswith('    ') and text[i].strip()), len(text)); job = text[start:stop]
    assert sum(v.startswith('      - ') for v in job) == 55 and sum(v.startswith('        run:') for v in job) == 32
    record = load(HERE.parents[1] / 'experimental-mcp-bound-state-2026-10-15.json')
    assert record['component_six_gates_and_dual_serial_Native_passed'] and record['source']['tree_sha256'] == TREE
    assert record['source']['conservative_supplied_inputs'] == 536 and record['Native']['binary_sha256'] == binary['sha256']
    assert record['Native']['non_DB_test_cases'] == 13 and record['Native']['non_DB_TCP_cases'] == 12 and record['Native']['construction_configuration_cases'] == 1
    assert record['CI_candidate']['Ubuntu_testopt1_fullCI_pending'] and not record['CI_candidate']['actual_CI_run_qualified']
    assert not record['claim_limits']['complete_CI_30m_qualified'] and not record['claim_limits']['Markdown_guide_example_compiled']
    if args.workspace:
        paths = sorted({str(p.relative_to(repo)) for root in ['crates', '.cargo'] for p in (repo / root).rglob('*') if p.is_file()} | {'Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml'})
        assert paths == sorted(v['path'] for v in expected), 'actual full536 input path set not adopted or later changed'
        for v in expected:
            b = (repo / v['path']).read_bytes(); assert len(b) == v['bytes'] and sha(b) == v['sha256'], v['path']
        assert sha((repo / '.github/workflows/ci.yml').read_bytes()) == CI, 'reviewed MCP CI candidate not adopted'
        for v in obj('adoption-docs-v2/candidate-documents.json')['files'] + load(HERE / 'reports.json')['files']:
            b = (repo / v['path']).read_bytes(); assert len(b) == v['bytes'] and sha(b) == v['sha256'], v['path']
    print(json.dumps({'state': 'bounded MCP component evidence verified', 'source_tree': TREE, 'supplied_inputs': 536,
        'strict_gates': 6, 'serial_Native_per_major': 14, 'historical_reds_preserved': True, 'whole_CI_R1_qualified': False, 'workspace_checked': args.workspace}))

if __name__ == '__main__':
    main()
