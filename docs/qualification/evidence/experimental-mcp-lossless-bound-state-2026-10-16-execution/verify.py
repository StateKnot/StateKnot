#!/usr/bin/env python3
"""Verify saved execution evidence only; never run Cargo, Git, DB, or Docker."""
from pathlib import Path
import argparse
import gzip
import hashlib
import json
import re
import subprocess
import sys

HERE = Path(__file__).resolve().parent
sha = lambda value: hashlib.sha256(value).hexdigest()
canon = lambda value: json.dumps(value, sort_keys=True, separators=(',', ':')).encode()
load = lambda path: json.loads(path.read_bytes())
SUMMARY = re.compile(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;')
GATES = ['fmt', 'clippy', 'workspace', 'doctests', 'rustdoc', 'nightly']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workspace', action='store_true')
    parser.add_argument('--workspace-root', type=Path)
    args = parser.parse_args()
    repo = args.workspace_root.resolve() if args.workspace_root else HERE.parents[3]
    seal = load(HERE / 'seal.json')
    manifest = load(HERE / 'manifest.json')
    assert sha((HERE / 'manifest.json').read_bytes()) == seal['manifest_sha256']
    declared = {row['path'] for row in manifest['files']}
    assert len(declared) == len(manifest['files'])
    assert {p.relative_to(HERE).as_posix() for p in HERE.rglob('*') if p.is_file()} == declared | {'manifest.json', 'seal.json'}
    for row in manifest['files']:
        data = (HERE / row['path']).read_bytes()
        assert (len(data), sha(data)) == (row['bytes'], row['sha256']), row['path']
    entries = load(HERE / 'artifact-index.json')['artifacts']
    index = {row['path']: row for row in entries}
    assert len(index) == len(entries) == 49

    def raw(name):
        name += '.gz'
        row = index[name]
        data = (HERE / name).read_bytes()
        assert (len(data), sha(data)) == (row['stored_bytes'], row['stored_sha256'])
        assert row['storage_encoding'] == 'gzip' and data[4:8] == b'\0' * 4 and not data[3] & 8
        data = gzip.decompress(data)
        assert (len(data), sha(data)) == (row['raw_bytes'], row['raw_sha256'])
        return data

    obj = lambda name: json.loads(raw(name))
    for name in index:
        raw(name[:-3])
    record = load(HERE / 'record.json')
    parent = record['parent_P2']
    pe = repo / parent['published_evidence']
    assert sha(raw('parent/publication-manifest.json')) == parent['publication_manifest_sha256']
    publication = obj('parent/publication-manifest.json')
    assert len(publication['files']) == 449
    for row in publication['files']:
        data = (repo / row['path']).read_bytes()
        assert (len(data), sha(data)) == (row['bytes'], row['sha256']), row['path']
    assert sha((pe / 'manifest.json').read_bytes()) == parent['evidence_manifest_sha256']
    assert sha((pe / 'seal.json').read_bytes()) == parent['seal_sha256']
    assert sha((pe / 'verify.py').read_bytes()) == parent['verifier_sha256']
    assert parent['initial_environment_fully_self_contained'] is False
    assert parent['original_seal_modified'] is False
    source = record['source']
    isolated = obj('isolated/six-gates-summary.json')
    assert isolated['state'] == 'passed' and isolated['source_tree'] == source['tree_sha256']
    assert isolated['gate_count'] == 6 and [r['gate'] for r in isolated['receipts']] == GATES
    assert all(r['exit_code'] == 0 and r['source_before_after_exact'] for r in isolated['receipts'])
    assert raw('isolated/six-gates-summary.json') == gzip.decompress((pe / 'execution/six-gates-summary.json.gz').read_bytes())
    frozen = obj('isolated/frozen-source-manifest.json')
    assert frozen['count'] == len(frozen['files']) == 928
    assert sha(canon(frozen['files'])) == frozen['source_tree_sha256'] == source['tree_sha256']
    original_source = json.loads(gzip.decompress((pe / 'source/final-source-manifest.json.gz').read_bytes()))
    assert frozen['files'] == original_source['files']
    rows = obj('actual-main/compiled-inputs.json')
    assert rows == sorted(rows, key=lambda r: r['path']) and len(rows) == 536
    assert sha(canon(rows)) == source['canonical_relative_path_string_sorted_sha256']
    assert rows == sorted(load(pe / 'source/compiled-inputs.json')['files'], key=lambda r: r['path'])
    before = obj('actual-main/all-inputs-before.json')
    after = obj('actual-main/all-inputs-after.json')
    assert before == after and len(before) == 2810
    observed = {r['path']: r for r in before}
    assert len(observed) == 2810 and all(observed[r['path']] == r for r in rows)
    for row in publication['files']:
        assert observed[row['path']] == row
    assert observed['.github/workflows/ci.yml']['sha256'] == source['CI15_sha256']
    for row in source['adoption_docs7']:
        assert observed[row['path']] == {k: row[k] for k in ['path', 'bytes', 'sha256']}
    context = obj('actual-main/git-context.json')
    assert context['HEAD'] == record['runners']['actual_main']['HEAD'] and context['status'].strip()
    assert context['actual_working_input_files'] == 2810 and context['conservative_compilation_superset'] == 536
    summary = obj('actual-main/summary.json')
    assert summary['state'] == 'passed' and summary['gates'] == 6 and summary['original_profile_opt_level'] == 0
    assert summary['actual_main_all_inputs_before_after_exact'] and summary['supplied_compilation_inputs'] == 536
    assert summary['Native_execution_transferred_or_R1_claimed'] is False
    for kind, expected in [('workspace', (99, 2098, 0, 6)), ('doctests', (10, 48, 0, 0))]:
        values = SUMMARY.findall(raw('actual-main/' + kind + '.log').decode())
        assert values and all(v[0] == 'ok' and int(v[4]) == 0 and int(v[5]) == 0 for v in values)
        totals = (len(values), sum(int(v[1]) for v in values), sum(int(v[2]) for v in values), sum(int(v[3]) for v in values))
        assert totals == expected
        assert summary['totals'][kind] == dict(zip(['summaries', 'passed', 'failed', 'ignored'], totals))
    for number, gate in enumerate(GATES):
        receipt = obj('actual-main/' + gate + '.receipt.json')
        assert receipt == summary['receipts'][number] and receipt['gate'] == gate and receipt['exit_code'] == 0
        assert receipt['command'] == isolated['receipts'][number]['command']
        assert receipt['log_sha256'] == sha(raw('actual-main/' + gate + '.log'))
        assert receipt['runner_sha256'] == record['runners']['actual_main']['sha256']
        assert receipt['actual_main_all_inputs_before_after_exact'] and receipt['supplied_compilation_inputs_exact_final_candidate']
        assert receipt['Native_database_executed'] is False
    for label, filename, cwd in [('isolated', 'isolated/run-gate.py', 'cwd=S'), ('actual_main', 'actual-main/runner.py', 'cwd=R')]:
        data = raw(filename)
        assert sha(data) == record['runners'][label]['sha256']
        text = data.decode()
        for token in ['STATEKNOT_', 'CARGO_PROFILE_', 'CARGO_ENCODED_RUSTFLAGS', 'RUST_MIN_STACK', 'RUSTC_BOOTSTRAP', "CARGO_INCREMENTAL='0'", "CARGO_PROFILE_DEV_DEBUG='0'", "CARGO_PROFILE_TEST_DEBUG='0'", "RUSTFLAGS='-D warnings'", "RUSTDOCFLAGS='-D warnings'", cwd]:
            assert token in text, (label, token)
        assert 'opt-level' not in text and 'OPT_LEVEL' not in text
    assert json.loads(raw('actual-main/draft-preflight.stdout'))['nothing_executed']
    assert raw('actual-main/draft-preflight.stderr') == b''
    if args.workspace:
        subprocess.run([sys.executable, str(pe / 'verify.py'), '--workspace', '--workspace-root', str(repo)], check=True)
    print(json.dumps({'state': 'execution appendix verified', 'originals': 49, 'P2_initial_environment_fully_self_contained': False,
        'P2_original_seal_unchanged': True, 'actual_main_six_gates': 6, 'observed_nonignored_files': 2810,
        'conservative_inputs': 536, 'workspace': [2098, 0, 6], 'doc_tests': 48, 'workspace_checked': args.workspace,
        'new_product_execution_by_verifier': False, 'new_Native_CI_R1_or_whole_final_tree_qualification': False}))


if __name__ == '__main__':
    main()
