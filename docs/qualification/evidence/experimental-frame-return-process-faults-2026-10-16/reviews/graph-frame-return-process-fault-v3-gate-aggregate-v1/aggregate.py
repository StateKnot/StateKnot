#!/usr/bin/env python3
"""Read only completed existing Return v3 gate receipts; never execute them."""
import hashlib
import json
from pathlib import Path
import re

OUT = Path(__file__).resolve().parent
BASE = OUT.parents[1]
Q = BASE / 'graph-frame-return-process-fault-qualification-v3'
N = BASE / 'graph-frame-return-process-fault-qualification-v3-network-enabled'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def binding(path):
    return {'path': str(path.relative_to(BASE)), 'bytes': path.stat().st_size,
            'sha256': sha(path)}


def read(path):
    return json.loads(path.read_bytes())


def completed_gate(directory, name, expected):
    rp = directory / (name + '.receipt.json')
    receipt = read(rp)
    lp = directory / (name + '.log')
    assert receipt['gate'] == name and receipt['exit_code'] == expected
    assert sha(lp) == receipt['log_sha256']
    assert receipt['source_before_after_exact'] is True
    assert receipt['source_tree'] == read(Q / 'scope.json')['source_tree_sha256']
    assert receipt['native_database_execution'] is False
    assert not any(receipt['override_presence'].values())
    env = receipt['environment_allowlist']
    assert env['CARGO_INCREMENTAL'] == '0'
    for profile in ('DEV', 'TEST'):
        assert env['CARGO_PROFILE_' + profile + '_DEBUG'] == '0'
        assert env['CARGO_PROFILE_' + profile + '_OPT_LEVEL'] == '0'
        assert env['CARGO_PROFILE_' + profile + '_DEBUG_ASSERTIONS'] == 'true'
        assert env['CARGO_PROFILE_' + profile + '_OVERFLOW_CHECKS'] == 'true'
    assert env['RUSTFLAGS'] == env['RUSTDOCFLAGS'] == '-D warnings'
    assert sha(directory / 'run-gate.py') == receipt['runner_sha256']
    return {'gate': name, 'actual_exit_code': expected, 'receipt': binding(rp),
            'raw_log': binding(lp), 'runner': binding(directory / 'run-gate.py'),
            'argv': receipt['argv']}


def summaries(directory, name):
    raw = (directory / (name + '.log')).read_bytes()
    pattern = rb'^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out; finished in ([^\r\n]+)$'
    rows = re.findall(pattern, raw, re.M)
    assert rows, 'missing actual terminal test summaries: ' + name
    counts = [sum(int(row[i]) for row in rows) for i in range(1, 6)]
    return {'summary_count': len(rows), 'passed': counts[0], 'failed': counts[1],
            'ignored': counts[2], 'measured': counts[3], 'filtered_out': counts[4],
            'all_summary_status_ok': all(row[0] == b'ok' for row in rows)}


def main():
    terminal = N / 'remaining-gates.receipt.json'
    assert terminal.exists(), 'WAIT: root continuation has no terminal driver receipt'
    driver = read(terminal)
    assert driver['actual_results'] == [{'gate': name, 'exit_code': 0} for name in ('workspace', 'doctests', 'rustdoc', 'nightly')]
    assert driver['four_remaining_gates_passed'] is True
    for name in ('frozen-source-manifest.json', 'compiled-inputs.json'):
        assert (Q / name).read_bytes() == (N / name).read_bytes()
    static = read(OUT / 'static-increment.receipt.json')
    source = BASE / read(Q / 'scope.json')['source_directory']
    manifest = read(Q / 'frozen-source-manifest.json')
    for row in manifest['files']:
        p = source / row['path']
        assert p.stat().st_size == row['bytes'] and sha(p) == row['sha256']
    gates = [completed_gate(Q, name, 0) for name in ('fmt', 'clippy')]
    gates += [completed_gate(N, name, 0) for name in ('workspace', 'doctests', 'rustdoc', 'nightly')]
    failure = completed_gate(Q, 'workspace', 101)
    failed_raw = (Q / 'workspace.log').read_bytes()
    failure_counts = summaries(Q, 'workspace')
    assert failure_counts == {'summary_count': 1, 'passed': 29, 'failed': 5,
                             'ignored': 0, 'measured': 0, 'filtered_out': 0,
                             'all_summary_status_ok': False}
    assert failed_raw.count(b'kind: PermissionDenied, message: "Operation not permitted"') == 5
    assert b'TcpListener::bind' in (source / 'crates/stateknot/src/agent_http/introspection/tests.rs').read_bytes()
    assert b'TcpListener::bind' in (source / 'crates/stateknot/src/agent_host/operations/tests.rs').read_bytes()
    ws = summaries(N, 'workspace')
    docs = summaries(N, 'doctests')
    assert ws['all_summary_status_ok'] and ws['failed'] == ws['measured'] == ws['filtered_out'] == 0
    assert docs['all_summary_status_ok'] and docs['failed'] == docs['ignored'] == docs['measured'] == docs['filtered_out'] == 0
    ignored = re.findall(rb'^test ([^\r\n]+?) \.\.\. ignored(?:[^\r\n]*)$', (N / 'workspace.log').read_bytes(), re.M)
    assert len(ignored) == ws['ignored']
    original_driver = read(Q / 'six-gates-driver.receipt.json')
    assert original_driver['six_gates_all_passed'] is False
    assert original_driver['actual_results'] == [{'gate': 'fmt', 'exit_code': 0}, {'gate': 'clippy', 'exit_code': 0}, {'gate': 'workspace', 'exit_code': 101}]
    result = {'kind': 'offline aggregate of two original successful gates and four same-source successful continuation gates',
              'aggregate_all_six_gate_receipts_zero': True,
              'a_new_single_command_six_gate_success': False,
              'original_six_driver_passed': False,
              'continuation_four_driver_passed': True,
              'source_tree': static['source_tree'], 'source_count': static['source_count'],
              'supplied_compiled_count': static['supplied_compiled_count'],
              'supplied_compiled_sha256': static['supplied_compiled_sha256'],
              'current_selected_source_paths_bytes_sha_equal': True,
              'same_frozen_and_compiled_manifest_bytes_between_attempts': True,
              'gates': gates, 'workspace': ws, 'doctests': docs,
              'workspace_ignored_names': [x.decode() for x in ignored],
              'retained_original_workspace_failure': {'gate': failure,
                'counts': failure_counts, 'permission_denied_bind_failures': 5,
                'classification': 'default sandbox loopback TcpListener bind EPERM; not a production-change assertion red'},
              'original_driver': binding(Q / 'six-gates-driver.receipt.json'),
              'continuation_driver': binding(terminal),
              'continuation_driver_tool': binding(N / 'continue-gates.py'),
              'original_driver_tool': binding(Q / 'run-six-gates.py'),
              'static_increment': binding(OUT / 'static-increment.receipt.json'),
              'aggregate_tool_sha256': sha(Path(__file__)),
              'Native_database_CI_performance_or_R1_qualification': False,
              'Rust_DB_CI_or_archived_scripts_executed_by_aggregator': False}
    (OUT / 'aggregate.receipt.json').write_text(json.dumps(result, indent=2) + '\n')
    report = ('# Return v3 gate aggregate (offline)\n\n'
              'The original fmt and strict full-workspace Clippy returned 0. The same frozen source continuation returned 0 for workspace, doctests, strict rustdoc and fixed-nightly checks. This is a six-receipt aggregate, not a new single-command six-gate run.\n\n'
              f'Workspace: {ws["summary_count"]} summaries, {ws["passed"]} passed / {ws["failed"]} failed / {ws["ignored"]} retained ignored / {ws["filtered_out"]} filtered. Doctests: {docs["summary_count"]} groups, {docs["passed"]} passed, no failures/ignored/filtered.\n\n'
              'Both qualification directories contain byte-identical frozen and supplied-compiled manifests (932 / 541). Actual selected source bytes remain exact. Original workspace exit101 is retained: first suite 29 passed / 5 failed, each loopback bind EPERM. The first driver remains six_gates_all_passed=false. Continuation evidence does not erase that attempt.\n\n'
              'Independent v3 static increment: two await-position-preserving Box::pin insertions; middle corruption test equals the original-production single-walk baseline fixture; both single-walk production files remain a544 bytes. No production/API/SQL change. No Native, performance, CI or R1 qualification is granted by these conditional workspace tests.\n')
    (OUT / 'report.md').write_text(report)
    print(json.dumps({'status': 'offline aggregate verified', 'workspace': ws, 'doctests': docs,
                      'receipt_sha256': sha(OUT / 'aggregate.receipt.json')}))


if __name__ == '__main__':
    main()
