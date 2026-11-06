#!/usr/bin/env python3
"""Read-only qualification of real Native outputs, preserving v3 wrapper failure."""
import ast
import hashlib
import json
from pathlib import Path
import re

HERE = Path(__file__).resolve().parent
BASE = HERE.parent
RUNNER = BASE / 'graph-frame-return-process-fault-native-runner-v4/run-case.py'
PIN = '5971de42998a1505ae81abc13d2cd7523f77ecd3b9fa08e2fe9da2b74cd52bab'
PREFIX = 'STATEKNOT_FRAME_RETURN_COMMIT_LOSS_EVIDENCE='


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def obj(path):
    return json.loads(Path(path).read_text())


def main():
    assert sha(RUNNER) == PIN
    tree = ast.parse(RUNNER.read_text())
    main_node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'main')
    parser_nodes = [n for n in main_node.body if isinstance(n, ast.Assign)
                    and any(isinstance(t, ast.Name) and t.id in {'starts', 'completions', 'selected'}
                            for t in n.targets)]
    assert len(parser_nodes) == 3
    parser = compile(ast.Module(body=parser_nodes, type_ignores=[]), str(RUNNER), 'exec')
    out = HERE / 'four-case-offline-qualification-v1'
    out.mkdir(exist_ok=False)
    rows = []
    for major in [16, 17]:
        for package, count in [('store', 264), ('runtime', 191)]:
            run = (BASE / 'graph-frame-return-process-fault-native-v3' / ('pg16-' + package + '-exact')
                   if major == 16 else HERE / ('pg17-' + package + '-exact-network-enabled'))
            receipt = obj(run / 'test.receipt.json')
            observations = obj(run / 'observations.json')
            case = receipt['argv'][1]
            assert receipt['exit_code'] == 0 and receipt['timed_out'] is False
            assert receipt['argv'][2:] == ['--exact', '--test-threads=1', '--nocapture']
            assert receipt['full_inventory_count'] == count and receipt['expected_filtered'] == count - 1
            assert observations['test_receipt_sha256'] == sha(run / 'test.receipt.json')
            assert observations['source_and_binary_before_after_exact'] is True
            assert observations['container_and_durability_settings_unchanged'] is True
            assert receipt['source_tree'] == '560a04129499fa5fb7a435e556a1ce3f19f2e95a75064af412338a000196e95a'
            assert receipt['compiled_inputs_sha256'] == '4a59f6fbb2d89ae8a71a13e8806b819974ec985c07346c4e7cbe59ec1fd5821e'
            assert sha(receipt['argv'][0]) == receipt['binary_sha256']
            for stream in ['test.stdout', 'test.stderr']:
                meta = receipt['output_streams'][stream]
                assert meta['saved_is_raw'] is True and meta['redactions'] == 0
                assert sha(run / stream) == meta['saved_sha256'] == meta['raw_sha256']
            inventory = re.findall(rb'^([^\n]+): test$', (run / 'full-inventory.stdout').read_bytes(), re.M)
            assert len(inventory) == len(set(inventory)) == count and case.encode() in inventory
            output = (run / 'test.stdout').read_bytes()
            summary = re.findall(rb'^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;', output, re.M)
            assert summary == [(b'1', b'0', b'0', b'0', str(count - 1).encode())]
            env = {'case': case, 'output': output, 're': re}
            exec(parser, env)
            assert env['selected'] is True
            for scope in ['server', 'database']:
                before = obj(run / (scope + '-settings-before.stdout'))
                after = obj(run / (scope + '-settings-after.stdout'))
                assert before == after
                assert int(before['server_version_num']) // 10000 == major
                assert all(before[k] == 'on' for k in ['fsync', 'synchronous_commit', 'full_page_writes'])
            assert obj(run / 'container-before.stdout') == obj(run / 'container-after.stdout')
            markers = []
            if package == 'runtime':
                markers = [json.loads(line[len(PREFIX):]) for line in output.decode().splitlines() if line.startswith(PREFIX)]
                assert len(markers) == 2
                assert {m['cut'] for m in markers} == {'commit_not_forwarded', 'commit_response_withheld'}
                for marker in markers:
                    committed = marker['cut'] == 'commit_response_withheld'
                    for key in ['writer_killed_and_reaped', 'cold_higher_fence_driver_verified']:
                        assert marker[key] is True
                    assert marker['commit_forwarded'] is committed
                    assert marker['original_return_recovered'] is committed
                    assert marker['commit_response_forwarded'] is False
                    assert marker['profile'] == 'frame-return-commit-loss-v1'
                    assert marker['invariants'] == 'passed' and marker['termination'] == 'SIGKILL'
                    assert type(marker['leaf_application_redispatches']) is int and marker['leaf_application_redispatches'] == 0
                    assert type(marker['parent_application_calls']) is int and marker['parent_application_calls'] == 1
                    assert marker['os'] == 'macos' and marker['arch'] == 'aarch64'
                    assert marker['postgres'].startswith(str(major) + '.')
            original_false = major == 16 and package == 'runtime'
            assert observations['exact_case_passed'] is (not original_false)
            rows.append({'major': major, 'package': package, 'native_exit_code': 0,
                         'seconds': receipt['seconds'], 'original_observations_passed': not original_false,
                         'supplementary_exact_case_and_markers_passed': True,
                         'original_runtime_pg16_wrapper_failure_preserved': original_false,
                         'run': str(run), 'test_receipt_sha256': sha(run / 'test.receipt.json'),
                         'original_observations_sha256': sha(run / 'observations.json'),
                         'stdout_sha256': sha(run / 'test.stdout'), 'return_markers': markers})
    result = {'all_four_actual_native_cases_qualified': True, 'rows': rows,
              'parser_runner_sha256': PIN, 'verifier_sha256': sha(__file__),
              'pg16_runtime_original_wrapper_exit_code': 1,
              'pg16_runtime_failure_classification': 'nocapture evidence interrupts contiguous case-start/ok substring',
              'original_evidence_modified': False, 'Rust_DB_or_original_scripts_executed_by_verifier': False,
              'whole_suite_Native_full_CI_capacity_or_R1_qualification': False}
    (out / 'qualification.receipt.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'qualified_actual_cases': len(rows), 'receipt': str(out / 'qualification.receipt.json'),
                      'sha256': sha(out / 'qualification.receipt.json')}))


if __name__ == '__main__':
    main()
