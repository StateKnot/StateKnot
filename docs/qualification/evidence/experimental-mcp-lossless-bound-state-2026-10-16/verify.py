#!/usr/bin/env python3
"""Offline frozen-evidence verification; optional adopted workspace bytes only."""
from pathlib import Path
import argparse
import gzip
import hashlib
import io
import json
import re
import tarfile

HERE = Path(__file__).resolve().parent
TREE = '69af3a0f28d333f4e67d2b2cfcc2afc071938a6ce398527cc098d31ee8db60d8'
BASE = '3a84c68eb9dd9df1804a7e24801516e11c9ee01ca0c6f243a4fdfaf76cbe124c'
SORTED = 'cfd0f2f8b48e062567fb591be2e5c427781cb7db616ee20b942cc2174a775b2d'
ORDERED = 'ded1a866797889ed38df63f59f671c8a2c34a5b7373a688932cc714eae582794'
BINARY = '0751e63128bf2fc1a28342f4e05bf62d7fd1416e9ae991032fa4136cf22bc8ed'
CI = '2c69f7e78b0b4c9b703e52864cdfd029b3cfc74d9ecceb31510768195b0e45ad'
MARKER = 'STATEKNOT_MCP_BOUND_STATE_POSTGRES: consumers=3 positive_commits=6 refused_rebinds=12 tenant_ledgers=2 complete_journals=true'
SUMMARY = re.compile(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;')
sha = lambda raw: hashlib.sha256(raw).hexdigest()
canon = lambda value: json.dumps(value, sort_keys=True, separators=(',', ':')).encode()
load = lambda path: json.loads(path.read_text())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workspace', action='store_true')
    parser.add_argument('--workspace-root', type=Path)
    args = parser.parse_args()
    repo = args.workspace_root.resolve() if args.workspace_root else HERE.parents[3]
    seal = load(HERE / 'seal.json')
    manifest = load(HERE / 'manifest.json')
    assert sha((HERE / 'manifest.json').read_bytes()) == seal['manifest_sha256']
    declared = {v['path'] for v in manifest['files']}
    assert len(declared) == len(manifest['files'])
    assert {p.relative_to(HERE).as_posix() for p in HERE.rglob('*') if p.is_file()} == declared | {'manifest.json', 'seal.json'}
    for row in manifest['files']:
        data = (HERE / row['path']).read_bytes()
        assert (len(data), sha(data)) == (row['bytes'], row['sha256']), row['path']
    for row in load(HERE / 'reports.json')['files']:
        data = (HERE.parents[3] / row['path']).read_bytes()
        assert (len(data), sha(data)) == (row['bytes'], row['sha256'])
    entries = load(HERE / 'artifact-index.json')['artifacts']
    index = {v['path']: v for v in entries}
    assert len(index) == len(entries)

    def raw(name):
        name = name if name in index or (HERE / name).exists() else name + '.gz'
        data = (HERE / name).read_bytes()
        if name in index:
            row = index[name]
            assert (len(data), sha(data)) == (row['stored_bytes'], row['stored_sha256'])
            if row['storage_encoding'] == 'gzip':
                assert data[4:8] == b'\0' * 4 and not data[3] & 8
                data = gzip.decompress(data)
            else:
                assert row['storage_encoding'] == 'identity'
            assert (len(data), sha(data)) == (row['raw_bytes'], row['raw_sha256'])
        elif name.endswith('.gz'):
            data = gzip.decompress(data)
        return data

    obj = lambda name: json.loads(raw(name))
    for name in index:
        raw(name)
    old = HERE / 'history/old-eedd-publication'
    old_manifest = load(old / 'publication-manifest.json')
    assert sha((old / 'publication-manifest.json').read_bytes()) == '698416c2a88df3adc07397d60dd38398710b34ac259dc49d82d30514348699a5'
    assert len(old_manifest['files']) == 224
    for row in old_manifest['files']:
        data = (old / row['path']).read_bytes()
        assert (len(data), sha(data)) == (row['bytes'], row['sha256'])
    assert load(old / 'status.json')['adoptable'] is False
    assert load(old / 'status.json')['invalidated_by_actual_numeric_counterexample']
    olde = old / 'docs/qualification/evidence/experimental-mcp-bound-state-2026-10-15'

    refs = load(HERE / 'source/base-references.json')['files']
    for row in refs:
        data = (repo / row['path']).read_bytes()
        assert (len(data), sha(data)) == (row['bytes'], row['sha256'])
    with tarfile.open(fileobj=io.BytesIO((repo / refs[0]['path']).read_bytes()), mode='r:gz') as archive:
        members = archive.getmembers()
        assert len(members) == len({m.name for m in members}) == 923
        assert all(m.isfile() and m.name.startswith('source/') and '..' not in Path(m.name).parts for m in members)
        base = {m.name[7:]: archive.extractfile(m).read() for m in members}
    parent = json.loads(gzip.decompress((repo / refs[1]['path']).read_bytes()))
    base['crates/stateknot-runtime/tests/postgres/child_join_pitr.rs'] = gzip.decompress((repo / refs[2]['path']).read_bytes())
    rows = lambda files, order: [{'path': v['path'], 'bytes': len(files[v['path']]), 'sha256': sha(files[v['path']])} for v in order]
    assert sha(canon(rows(base, parent['files']))) == BASE

    def reconstruct(source_manifest, changes, payload_root=HERE):
        files = dict(base)
        for change in changes:
            name = change['path']
            assert (sha(base[name]) if name in base else None) == change['before_sha256']
            data = gzip.decompress((payload_root / change['payload']).read_bytes())
            assert (len(data), sha(data)) == (change['bytes'], change['after_sha256'])
            files[name] = data
        assert len(files) == len(source_manifest['files']) == 928
        assert set(files) == {v['path'] for v in source_manifest['files']}
        assert rows(files, source_manifest['files']) == source_manifest['files']
        expected_tree = source_manifest.get('source_tree_sha256', source_manifest.get('tree_sha256'))
        assert sha(canon(source_manifest['files'])) == expected_tree
        return files

    source_manifest = obj('source/final-source-manifest.json')
    final_delta = load(HERE / 'source/final-reconstruction.json')['changes']
    assert len(final_delta) == 12 and sum(v['before_sha256'] is None for v in final_delta) == 5
    assert sum(v['path'].startswith('crates/stateknot-integrations/src/') for v in final_delta) == 8
    final = reconstruct(source_manifest, final_delta)
    assert source_manifest['source_tree_sha256'] == TREE
    expected = [v for v in source_manifest['files'] if v['path'].startswith(('crates/', '.cargo/')) or v['path'] in ['Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml']]
    supplied = load(HERE / 'source/compiled-inputs.json')
    root_supplied = obj('source/root-compiled-inputs.json')
    assert supplied['files'] == root_supplied['files'] == expected and len(expected) == 536
    assert sha(canon(expected)) == root_supplied['sha256'] == supplied['original_manifest_order_sha256'] == ORDERED
    assert sha(canon(sorted(expected, key=lambda v: v['path']))) == supplied['canonical_sha256'] == SORTED
    assert obj('source/actual-main-parent-before-adoption.json')['actual531_parent_bytemap_exact']

    old_source_manifest = json.loads(gzip.decompress((olde / 'source/final-source-manifest.json.gz').read_bytes()))
    eedd = reconstruct(old_source_manifest, load(olde / 'source/final-reconstruction.json')['changes'], olde)
    probe_manifest = obj('history/numeric-alias-red/source-manifest.json')
    probe = reconstruct(probe_manifest, load(HERE / 'history/numeric-alias-red/reconstruction.json')['changes_from3a84'])
    assert [n for n in eedd if probe[n] != eedd[n]] == ['crates/stateknot/tests/mcp_bound_state/network.rs']
    red = obj('history/numeric-alias-red/receipt.json')
    red_log = raw('history/numeric-alias-red/test.log')
    assert red['exit_code'] == 101 and sha(red_log) == red['log_sha256'] == '5428660fd12dc58c3f872c96917fd80fe254cc28991f4232c6452c299582bb0b'
    assert b'business_before=2 business_after=3' in red_log and b'"_fixture_http_status":200' in red_log
    assert SUMMARY.findall(red_log.decode()) == [('FAILED', '0', '1', '0', '0', '14')]
    a87_manifest = obj('history/a87-unit-lifetime-red/frozen-source-manifest.json')
    a87 = reconstruct(a87_manifest, load(HERE / 'history/a87-unit-lifetime-red/reconstruction.json')['changes_from3a84'])
    assert [n for n in a87 if a87[n] != final[n]] == ['crates/stateknot-integrations/src/mcp_server_bound_state.rs']
    lint = obj('history/a87-unit-lifetime-red/clippy.receipt.json')
    assert lint['exit_code'] == 101 and b'E0597' in raw('history/a87-unit-lifetime-red/clippy.log')
    sandbox = obj('history/69af-sandbox-loopback-red/workspace.receipt.json')
    sandbox_log = raw('history/69af-sandbox-loopback-red/workspace.log')
    assert sandbox['exit_code'] == 101 and sandbox['source_tree'] == TREE
    assert SUMMARY.findall(sandbox_log.decode()) == [('FAILED', '29', '5', '0', '0', '0')]
    assert sandbox_log.count(b'Operation not permitted') == 5
    assert obj('execution/retry-basis.json')['code_or_assertions_not_modified']

    gates = obj('execution/six-gates-summary.json')
    assert gates['state'] == 'passed' and gates['gate_count'] == 6 and gates['source_tree'] == TREE
    for row in gates['receipts']:
        name = row['gate']
        receipt = obj('execution/gates/' + name + '.receipt.json')
        data = raw('execution/gates/' + name + '.log')
        assert row == receipt and row['exit_code'] == 0 and row['source_before_after_exact']
        assert sha(data) == row['log_sha256'] and row['source_tree'] == TREE
    for name, groups, total, ignored in [('workspace', 99, 2098, 6), ('doctests', 10, 48, 0)]:
        summaries = SUMMARY.findall(raw('execution/gates/' + name + '.log').decode())
        assert len(summaries) == groups and all(row[0] == 'ok' and row[2] == '0' for row in summaries)
        assert sum(int(row[1]) for row in summaries) == total and sum(int(row[3]) for row in summaries) == ignored
    binary = obj('execution/native/binary.json')
    build = obj('execution/native/build.receipt.json')
    assert build['exit_code'] == 0 and sha(raw('execution/native/build.log')) == build['log_sha256']
    events = [json.loads(row) for row in raw('execution/native/build.log').decode().splitlines() if row.startswith('{')]
    event = binary['cargo_artifact']
    assert event in events and event['fresh'] is False
    assert event['features'] == [] and event['profile'] == {'opt_level': '0', 'debuginfo': 0, 'debug_assertions': True, 'overflow_checks': True, 'test': True}
    assert binary['sha256'] == BINARY
    names = re.findall(r'^(\S+): test$', raw('execution/native/list.log').decode(), re.M)
    assert names == binary['tests'] and len(names) == len(set(names)) == 15
    assert 'network::lossless_numeric_argument_binding_is_enforced_over_tcp' in names
    native = obj('execution/native/summary.json')
    assert native['state'] == 'passed' and native['source_tree'] == TREE
    for major in [16, 17]:
        receipt = obj(f'execution/native/pg{major}.receipt.json')
        data = raw(f'execution/native/pg{major}.log')
        assert receipt['exit_code'] == 0 and receipt['command'][1:] == ['--nocapture', '--test-threads=1']
        assert sha(data) == receipt['log_sha256'] and receipt['source_tree'] == TREE
        assert SUMMARY.findall(data.decode()) == [('ok', '15', '0', '0', '0', '0')]
        assert data.decode().splitlines().count(MARKER) == 1 and 'STATEKNOT_MCP_BOUND_STATE_POSTGRES_SKIPPED' not in data.decode()
        assert b'network::lossless_numeric_argument_binding_is_enforced_over_tcp ... ok' in data
        db = obj(f'execution/native/pg{major}-database.json')
        assert db['new_absent_database_created'] and db['no_database_dropped']
        assert db['database'] == 'stateknot_r1_mcp_bound_20261016v4'
        positive = obj(f'reviews/CI15-positive/pg{major}.json')
        assert positive['bash_predicate_exit_code'] == 0 and positive['accepted'] and positive['workflow_sha256'] == CI
        assert positive['log_sha256'] == sha(data)
        negative = obj(f'reviews/CI15-preparation/offline-old14/pg{major}-candidate15-negative.json')
        assert negative['bash_predicate_exit_code'] == 1 and not negative['accepted'] and negative['workflow_sha256'] == CI
    resource = obj('execution/final-resource-observation.json')
    assert resource['source_tree'] == TREE and resource['source_928_before_after_exact']
    assert resource['no_cleanup_or_database_lifecycle_action']
    assert len(resource['checks']) == 3 and all(v['lsof_exit'] == 1 and v['no_open_files'] for v in resource['checks'])
    assert resource['available_kib'] == 315550848 and resource['task_cache_logical_kib'] == 11458960
    candidate = raw('reviews/CI15-preparation/candidate.yml')
    assert sha(candidate) == CI
    lines = candidate.decode().splitlines()
    start = lines.index('  postgres:')
    stop = next((i for i in range(start + 1, len(lines)) if lines[i].startswith('  ') and not lines[i].startswith('    ') and lines[i].strip()), len(lines))
    job = lines[start:stop]
    assert sum(v.startswith('      - ') for v in job) == 55 and sum(v.startswith('        run:') for v in job) == 32
    assert '    timeout-minutes: 30' in job and "^running 15 tests$" in candidate.decode()

    record = load(HERE.parents[1] / 'experimental-mcp-lossless-bound-state-2026-10-16.json')
    assert record['source']['tree_sha256'] == TREE and record['source']['canonical_supplied_input_sha256'] == SORTED
    assert record['source']['original_manifest_order_sha256'] == ORDERED
    assert record['old_eedd_adoptable'] is False and record['actual_CI_Ubuntu_opt1_full_job_qualified'] is False
    if args.workspace:
        paths = sorted({p.relative_to(repo).as_posix() for prefix in ['crates', '.cargo'] for p in (repo / prefix).rglob('*') if p.is_file()} | {'Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml'})
        assert paths == sorted(v['path'] for v in expected), 'actual full536 path set differs'
        for row in expected:
            data = (repo / row['path']).read_bytes()
            assert (len(data), sha(data)) == (row['bytes'], row['sha256']), row['path']
        assert sha((repo / '.github/workflows/ci.yml').read_bytes()) == CI, 'reviewed CI15 candidate not adopted'
        for row in obj('adoption-docs/candidate-documents.json')['files'] + load(HERE / 'reports.json')['files']:
            data = (repo / row['path']).read_bytes()
            assert (len(data), sha(data)) == (row['bytes'], row['sha256']), row['path']
    print(json.dumps({'state': 'bounded lossless MCP component evidence verified', 'source': TREE,
        'supplied_inputs': 536, 'canonical_sorted_digest': SORTED, 'original_manifest_order_digest': ORDERED,
        'strict_gates': 6, 'workspace': [2098, 0, 6], 'doctests': 48, 'serial_per_PG': [15, 0, 0, 0],
        'old_eedd_adoptable': False, 'historical_reds_preserved': True, 'workspace_checked': args.workspace,
        'actual_CI_opt1_Ubuntu_full_job_RFC_R1_qualified': False}))


if __name__ == '__main__':
    main()
