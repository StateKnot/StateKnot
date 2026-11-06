#!/usr/bin/env python3
"""Read existing files only. --workspace additionally checks the proposed adoption."""
from pathlib import Path
import argparse, collections, difflib, gzip, hashlib, io, json, re, tarfile, zipfile

HERE = Path(__file__).resolve().parent
TREE = '3a84c68eb9dd9df1804a7e24801516e11c9ee01ca0c6f243a4fdfaf76cbe124c'
DIGEST = 'ddaf1aba9089af825b870d3c1466969d8967d59ee8805564b302b4c16eebc4b8'
SUM = re.compile(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;')
sha = lambda b: hashlib.sha256(b).hexdigest()
load = lambda p: json.loads(p.read_text())
canon = lambda v: json.dumps(v, sort_keys=True, separators=(',', ':')).encode()

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--workspace', action='store_true')
    p.add_argument('--workspace-root', type=Path)
    args = p.parse_args()
    repo = args.workspace_root.resolve() if args.workspace_root else HERE.parents[3]
    manifest = load(HERE / 'manifest.json')
    for v in manifest['files']:
        b = (HERE / v['path']).read_bytes()
        assert len(b) == v['bytes'] and sha(b) == v['sha256'], v['path']
    artifacts = {v['path']: v for v in load(HERE / 'artifact-index.json')['artifacts']}
    def raw(name):
        path = name if name in artifacts or (HERE / name).exists() else name + '.gz'
        b = (HERE / path).read_bytes()
        if path in artifacts:
            v = artifacts[path]
            assert len(b) == v['stored_bytes'] and sha(b) == v['stored_sha256']
            if v['storage_encoding'] == 'gzip':
                assert b[4:8] == b'\0' * 4 and not b[3] & 8
                b = gzip.decompress(b)
            else:
                assert v['storage_encoding'] == 'identity'
            assert len(b) == v['raw_bytes'] and sha(b) == v['raw_sha256']
        elif path.endswith('.gz'):
            b = gzip.decompress(b)
        return b
    obj = lambda name: json.loads(raw(name))
    for name in artifacts:
        raw(name)
    original_seal_bytes = raw('execution/execution-seal.json')
    assert sha(original_seal_bytes) == '9b3853142130afacee6873668b51ae76c4a9671403902897a9f04851aad3877d'
    original_seal = json.loads(original_seal_bytes)
    original_entries = {v['path']: v for v in original_seal['files'] + original_seal['external_binding_files']}
    for v in artifacts.values():
        if v['origin'] in original_entries:
            old = original_entries[v['origin']]
            assert v['raw_sha256'] == old['sha256'] and v['raw_bytes'] == old['bytes']
    for v in load(HERE / 'references.json')['files']:
        b = (repo / v['path']).read_bytes()
        assert len(b) == v['bytes'] and sha(b) == v['sha256'], v['path']
    source = obj('source/source-manifest.json')
    reconstruct = load(HERE / 'source/reconstruction.json')
    with tarfile.open(fileobj=io.BytesIO((repo / reconstruct['parent_archive']).read_bytes()), mode='r:gz') as archive:
        members = archive.getmembers()
        assert len(members) == len({m.name for m in members}) == 923
        assert all(m.isfile() and m.name.startswith('source/') and '..' not in Path(m.name).parts for m in members)
        files = {m.name[len('source/'):]: archive.extractfile(m).read() for m in members}
    assert set(files) == {v['path'] for v in source['files']}
    # The original source manifests use Path-component order, not string order.
    rows = lambda: [{'path': v['path'], 'bytes': len(files[v['path']]), 'sha256': sha(files[v['path']])} for v in source['files']]
    assert sha(canon(rows())) == reconstruct['parent_tree_sha256']
    n = reconstruct['file']; before = files[n]; after = raw('source/child_join_pitr.rs')
    assert sha(before) == reconstruct['before_sha256'] and sha(after) == reconstruct['after_sha256']
    patch = ''.join(difflib.unified_diff(before.decode().splitlines(True), after.decode().splitlines(True), fromfile='a/' + n, tofile='b/' + n)).encode()
    assert gzip.decompress((HERE / 'source/pitr-only.patch.gz').read_bytes()) == patch
    files[n] = after
    assert rows() == source['files'] and sha(canon(rows())) == source['source_tree_sha256'] == TREE
    binding = obj('source/main-binding.json')
    supplied = [v for v in rows() if v['path'].startswith(('crates/', '.cargo/')) or v['path'] in ['Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml']]
    assert supplied == binding['files'] and len(supplied) == binding['count'] == 531
    assert sha(canon(sorted(supplied, key=lambda v: v['path']))) == binding['compiled_input_digest'] == DIGEST
    prior = repo / 'docs/qualification/evidence/experimental-composed-request-facts-pitr-2026-10-15'
    assert json.loads(gzip.decompress((prior / 'expected-compiled-inputs.json.gz').read_bytes())) == supplied
    opt0 = obj('source/opt0-six-receipts-bridge.json')
    assert opt0['actual_main_all_input_count'] == 2182 and not opt0['frozen923_whole_tree_executed_under_opt0_claimed']
    assert gzip.decompress((prior / 'all-inputs-before.json.gz').read_bytes()) == gzip.decompress((prior / 'all-inputs-after.json.gz').read_bytes())
    before_inputs = json.loads(gzip.decompress((prior / 'all-inputs-before.json.gz').read_bytes()))
    assert len(before_inputs) == 2182
    old_supplied = sorted([v for v in before_inputs if v['path'].startswith(('crates/', '.cargo/')) or v['path'] in ['Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml']], key=lambda v: v['path'])
    assert old_supplied == sorted(supplied, key=lambda v: v['path'])
    for v in opt0['receipts']:
        assert v['exit_code'] == 0 and sha((prior / (v['gate'] + '.receipt.json')).read_bytes()) == v['raw_receipt_sha256']
    checks = load(HERE / 'source/step-input-checks.json')['files']
    for v in checks:
        b = v['raw_text'].encode(); assert len(b) == v['raw_bytes'] and sha(b) == v['raw_sha256']
        assert v['raw_sha256'] == original_entries[v['origin']]['sha256']
        d = json.loads(b); assert d['source_tree'] == TREE and d['file_count'] == 923 and d['full_snapshot_and_main_compilation_inputs_equal']
    assert {'inputs-initial.json', 'inputs-final.json'} <= {v['basename'] for v in checks}
    for gate in ['fmt', 'clippy', 'workspace', 'doctests', 'rustdoc', 'nightly']:
        r = obj('gates/' + gate + '.receipt.json'); b = raw('gates/' + gate + '.log')
        assert r['exit_code'] == 0 and r['source_tree'] == TREE and sha(b) == r['log_sha256']
        assert 'inputs-gate-' + gate + '-before.json' in {v['basename'] for v in checks}
        assert 'inputs-gate-' + gate + '-after.json' in {v['basename'] for v in checks}
    for gate, expected in [('workspace', (2073, 0, 6)), ('doctests', (45, 0, 0))]:
        sums = SUM.findall(raw('gates/' + gate + '.log').decode())
        assert sums and all(v[0] == 'ok' for v in sums)
        assert tuple(sum(int(v[i]) for v in sums) for i in [1, 2, 3]) == expected
    environment = obj('execution/environment.json')
    expected_env = {'CARGO_INCREMENTAL': '0', 'CARGO_PROFILE_DEV_DEBUG': '0', 'CARGO_PROFILE_TEST_DEBUG': '0', 'CARGO_PROFILE_TEST_OPT_LEVEL': '1', 'CARGO_PROFILE_TEST_DEBUG_ASSERTIONS': 'true', 'CARGO_PROFILE_TEST_OVERFLOW_CHECKS': 'true', 'RUSTFLAGS': '-D warnings', 'RUSTDOCFLAGS': '-D warnings'}
    assert all(environment[k] == v for k, v in expected_env.items())
    fresh = obj('build/fresh-binaries.json')
    events = [json.loads(line) for line in raw('build/workspace-fresh-build.log').decode().splitlines() if line.startswith('{')]
    assert obj('build/workspace-fresh-build.receipt.json')['exit_code'] == 0
    for event in obj('build/ordinary-runtime-library-profiles.json')['workspace'] + obj('build/ordinary-runtime-library-profiles.json')['serde_cpu_libraries']:
        profile = event['profile']
        assert event in events and event['fresh'] is False and profile['opt_level'] == '1' and profile['debuginfo'] == 0 and profile['debug_assertions'] and profile['overflow_checks']
    counts = {'store': 264, 'runtime': 189, 'provider': 2, 'example': 1, 'facts': 6}
    for name, v in fresh.items():
        event = v['cargo_artifact']; profile = event['profile']
        assert event in events and event['fresh'] is False and profile['opt_level'] == '1' and profile['debuginfo'] == 0 and profile['debug_assertions'] and profile['overflow_checks']
        names = re.findall(r'^(\S+): test$', raw('build/list-' + name + '.log').decode(), re.M)
        assert names == v['tests'] and len(names) == counts[name]
    native = obj('execution/native-summary.json')
    assert native['state'] == 'passed-so-far' and not native['complete_ci_job']
    expected_markers = obj('execution/runtime-markers-actual922.json')['logs'][0]['markers']
    for pg in native['postgres']:
        major = pg['major']; assert len(pg['suites']) == 5
        for v in pg['suites']:
            name = v['suite']; prefix = f'native/pg{major}/{name}'; r = obj(prefix + '.receipt.json'); b = raw(prefix + '.log')
            assert r['exit_code'] == 0 and r['source_tree'] == TREE and sha(b) == r['log_sha256']
            assert SUM.findall(b.decode()) == [('ok', str(counts[name]), '0', '0', '0', '0')]
            assert v['binary_sha256'] == fresh[name]['sha256']
            markers = collections.defaultdict(list)
            for key, payload in re.findall(r'^(STATEKNOT_[A-Z0-9_]+)=(\{[^\n]+\})$', b.decode(), re.M):
                markers[key].append(json.loads(payload))
            assert dict(markers) == v['markers']
            if name == 'runtime':
                assert all(len(markers[k]) == count for k, count in expected_markers.items())
            if name == 'facts':
                for key in ['STATEKNOT_MCP_AUTHORIZATION_EVIDENCE', 'STATEKNOT_A2A_AUTHORIZATION_EVIDENCE']:
                    assert len(markers[key]) == 2 and {x['case'] for x in markers[key]} == {'denial', 'lost_response'}
                    for x in markers[key]:
                        assert x['policy_calls'] == 1 and x['calls'] == (0 if x['case'] == 'denial' else 1)
                        if 'A2A' in key:
                            assert x['durable_start_verified'] and x['payload_from_full_input'] and x['duplicate_ledger_unchanged']
                        else:
                            assert x['durable_before_authorization'] and x['duplicate_ledger_unchanged' if x['case'] == 'denial' else 'reconciliation_idempotent']
    assert {v['major'] for v in native['postgres']} == {16, 17}
    failure = obj('execution/failure.json'); semantic = obj('execution/semantic-execution-summary.json')
    assert failure['state'] == 'failed-or-precondition-not-met' and failure['exception'] == 'FileNotFoundError' and 'CACHEDIR.TAG' in failure['error']
    assert semantic['original_runner_exit_code'] == 1 and semantic['original_summary_json_absent'] and not semantic['complete_CI_job_30m_qualified']
    assert b'for cache in [TARGET,NIGHTLY]: inactive_owned_cache(cache)' in raw('execution/runner.py.txt')
    audit = obj('execution/post-failure-resource-audit.json')
    assert audit['runner_exit_code'] == 1 and audit['no_recorded_pid_active'] and audit['saved_binary_directory_no_open_files'] and audit['selected_programs_no_open_files']
    audit_raw = load(HERE / 'execution/post-failure-audit-raw.json')['files']
    by_name = {v['basename']: v for v in audit_raw}
    for v in audit_raw:
        b = v['raw_text'].encode(); assert sha(b) == v['raw_sha256'] and len(b) == v['raw_bytes']
        assert v['raw_sha256'] == original_entries[v['origin']]['sha256']
    assert b'Command::new(std::env::current_exe().unwrap())' in files['crates/stateknot-runtime/tests/postgres/process_harness.rs']
    for result in [audit['ps_recorded_pids'], audit['saved_binary_directory_lsof'], audit['worker_path_proof']['all_selected_programs_lsof_raw']] + [v['lsof_raw'] for v in audit['cache_paths']]:
        assert result['exit_code'] == 1
        for suffix in ['stdout', 'stderr']:
            v = by_name[Path(result[suffix + '_path']).name]
            assert v['raw_sha256'] == result[suffix + '_sha256'] and v['raw_text'] == ''
    assert len(audit['cache_paths']) == 2 and all(not v['Cargo_CACHEDIR_TAG_exists'] and v['retained'] and not v['cleanup_performed'] and all(x['nonblocking_lock_acquired'] for x in v['locks']) for v in audit['cache_paths'])
    assert audit['execution_lock_nonblocking_acquired_and_released'] and audit['available_kib'] > 50 * 1024 ** 2
    ci = obj('ci-686c427/final-run.json'); jobs = obj('ci-686c427/final-jobs.json')['jobs']
    assert ci['id'] == 38741332898 and ci['status'] == 'completed' and ci['conclusion'] == 'cancelled'
    assert collections.Counter(v['conclusion'] for v in jobs) == {'success': 11, 'cancelled': 2}
    pitr = obj('ci-686c427/linux-pitr-artifacts.json')
    ci_binding = obj('ci-686c427/linux-pitr-source-binding.json')
    assert ci_binding['actual_CI_checkout_tree'] == ci_binding['head_tree'] == '222152eb603e960cadbb5001a80294b9de96bc84'
    for name, commit in [('head', '686c42739e9f128db9d90fcb4e9d1aefd36942d8'), ('merge', 'b552ef62bcb9540fc4b0291b847665885201be5b')]:
        b = gzip.decompress((HERE / ('ci-686c427/' + name + '.commit.gz')).read_bytes())
        assert hashlib.sha1(b'commit ' + str(len(b)).encode() + b'\0' + b).hexdigest() == commit
        assert b.splitlines()[0] == b'tree 222152eb603e960cadbb5001a80294b9de96bc84'
    for major in [16, 17]:
        annotations = obj(f'ci-686c427/postgres-{major}-annotations.json'); b = raw(f'ci-686c427/postgres-{major}.log')
        assert any(v['message'] == 'The job has exceeded the maximum execution time of 30m0s' for v in annotations)
        assert len(re.findall(rb'test result:', b)) == 1 and b'264 passed; 0 failed; 0 ignored;' in b and b'running 189 tests' in b and not re.search(rb'\bFAILED\b', b)
        v = next(x for x in pitr['records'] if x['major'] == major); zipped = raw(f'ci-686c427/pitr-{major}.zip')
        assert sha(zipped) == v['archive_sha256']
        with zipfile.ZipFile(io.BytesIO(zipped)) as z:
            assert set(z.namelist()) == {'child-join-pitr.log', 'child-join-pitr-environment.log'}
            for f in v['files']:
                assert sha(z.read(f['path'])) == f['sha256'] and len(z.read(f['path'])) == f['bytes']
            log = z.read('child-join-pitr.log').decode()
            assert SUM.findall(log) == [('ok', '1', '0', '0', '0', '188')]
            marker = json.loads(re.search(r'^STATEKNOT_CHILD_JOIN_PITR_EVIDENCE=(\{[^\n]+\})$', log, re.M)[1])
            assert marker == v['marker'] and marker['os'] == 'linux' and marker['arch'] == 'x86_64' and marker['invariants'] == 'passed'
            assert all(marker[k] for k in ['base_backup_verified', 'cross_tenant_refusal_verified', 'exact_durable_snapshot_verified', 'noninitial_replay_verified', 'post_target_write_excluded', 'resource_cleanup_verified'])
    adoption = load(HERE / 'expected-adoption.json')
    proposal = obj('ci-profile-candidate/v2-receipt.json')
    patch = raw('ci-profile-candidate/v2-candidate.patch').decode().splitlines()
    additions = [v[1:] for v in patch if v.startswith('+') and not v.startswith('+++')]
    assert len(additions) == 9
    workflow = files['.github/workflows/ci.yml']
    assert sha(workflow) == proposal['baseline_sha256'] == adoption['base_workflow_sha256']
    lines = workflow.decode().splitlines(); start = lines.index('  postgres:')
    stop = next((i for i in range(start + 1, len(lines)) if lines[i].startswith('  ') and not lines[i].startswith('    ') and lines[i].strip()), len(lines))
    job = lines[start:stop]; insert = start + job.index('    timeout-minutes: 30') + 1
    assert sum(v.startswith('      - ') for v in job) == 53 and sum(v.startswith('        run:') for v in job) == 31
    lines[insert:insert] = additions
    assert sha(('\n'.join(lines) + '\n').encode()) == proposal['v2_candidate_sha256'] == adoption['workflow_sha256']
    for v in adoption['documents']:
        b = (HERE.parents[3] / v['path']).read_bytes()
        assert len(b) == v['bytes'] and sha(b) == v['sha256'], v['path']
    record = load(HERE.parents[1] / 'experimental-postgres-test-profile-2026-10-15.json')
    assert record['original_runner_exit_code'] == 1 and record['runner_overall_passed'] is False and record['all_six_static_and_ten_Native_commands_exit_zero']
    assert not record['claim_limits']['full_CI_30m_qualified'] and not record['claim_limits']['local_feature_union_transferred_to_CI_default']
    if args.workspace:
        paths = sorted({str(p.relative_to(repo)) for root in ['crates', '.cargo'] for p in (repo / root).rglob('*') if p.is_file()} | {'Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml'})
        assert paths == sorted(v['path'] for v in supplied), 'actual complete compilation input path set changed'
        for v in supplied + adoption['documents']:
            b = (repo / v['path']).read_bytes(); assert len(b) == v['bytes'] and sha(b) == v['sha256'], v['path']
        assert sha((repo / '.github/workflows/ci.yml').read_bytes()) == adoption['workflow_sha256'], 'reviewed CI candidate not adopted'
    print(json.dumps({'state': 'offline evidence verified', 'runner_exit_code': 1, 'product_commands_exit_zero': 16, 'whole_CI_30m_qualified': False, 'workspace_adoption_checked': args.workspace}))

if __name__ == '__main__':
    main()
