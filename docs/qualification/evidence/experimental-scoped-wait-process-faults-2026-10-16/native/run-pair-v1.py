#!/usr/bin/env python3
"""Execute both major-version scoped Wait exact profiles, fail closed on evidence."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
RUNNER = HERE.parent / 'graph-frame-wait-process-fault-native-runner-v1/run-case.py'
PIN = '66a53bff1bff2590e2a45dd9cc6e8507f6c0976b8935997af6f535c4f5527316'
PREFIX = 'STATEKNOT_SCOPED_WAIT_COMMIT_LOSS_EVIDENCE='
KEYS = {'profile', 'cut', 'commit_forwarded', 'commit_response_forwarded',
        'writer_killed_and_reaped', 'cold_driver_after_authorized_resolution',
        'condition_count', 'snapshot_tables', 'wait_application_redispatches',
        'scoped_successor_calls', 'parent_application_calls', 'fresh_old_fence_wait_refused',
        'original_wait_recovered', 'original_request_ack_after_return_verified',
        'invariants', 'postgres', 'termination', 'os', 'arch'}


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        assert key not in result, 'duplicate JSON marker field'
        result[key] = value
    return result


def reject_constant(value):
    raise ValueError('nonstandard JSON constant: ' + value)


def main():
    assert hashlib.sha256(RUNNER.read_bytes()).hexdigest() == PIN
    output = HERE / 'pair-execution-v1'
    output.mkdir(exist_ok=False)
    summary = {'started_unix': time.time(), 'runner_sha256': PIN, 'actual_results': [],
               'both_actual_cases_and_all_markers_passed': False}
    try:
        for major in [16, 17]:
            destination = HERE / ('pg' + str(major) + '-runtime-exact')
            argv = [sys.executable, '-B', str(RUNNER), '--build-dir',
                    str(HERE / 'build-candidate-runtime'), '--major', str(major),
                    '--leg', 'B', '--case', 'runtime_exact', '--out', str(destination)]
            result = subprocess.run(argv)
            row = {'major': major, 'actual_runner_exit_code': result.returncode}
            summary['actual_results'].append(row)
            assert result.returncode == 0, 'case failed; remaining major not launched'
            lines = (destination / 'test.stdout').read_text().splitlines()
            markers = [json.loads(line[len(PREFIX):], object_pairs_hook=unique_object,
                                  parse_constant=reject_constant)
                       for line in lines if line.startswith(PREFIX)]
            assert len(markers) == 2
            assert {m['cut'] for m in markers} == {'commit_not_forwarded', 'commit_response_withheld'}
            for marker in markers:
                assert type(marker) is dict and set(marker) == KEYS
                committed = marker['cut'] == 'commit_response_withheld'
                expected = {'profile': 'scoped-wait-commit-loss-v1', 'commit_forwarded': committed,
                            'commit_response_forwarded': False, 'writer_killed_and_reaped': True,
                            'cold_driver_after_authorized_resolution': True, 'condition_count': 2,
                            'snapshot_tables': 22, 'wait_application_redispatches': 0,
                            'scoped_successor_calls': 1, 'parent_application_calls': 1,
                            'fresh_old_fence_wait_refused': None if committed else True,
                            'original_wait_recovered': committed,
                            'original_request_ack_after_return_verified': committed,
                            'invariants': 'passed', 'termination': 'SIGKILL', 'os': 'macos', 'arch': 'aarch64'}
                for key, value in expected.items():
                    assert type(marker[key]) is type(value) and marker[key] == value, (key, marker[key])
                assert type(marker['postgres']) is str and marker['postgres'].startswith(str(major) + '.')
            assert markers[0]['postgres'] == markers[1]['postgres']
            row['two_markers_all_fields_verified'] = True
            (destination / 'verified-wait-markers.json').write_text(json.dumps(markers, indent=2) + '\n')
            print(json.dumps(row), flush=True)
        summary['both_actual_cases_and_all_markers_passed'] = True
    except Exception as error:
        summary['failure'] = {'type': type(error).__name__, 'message': str(error)}
    finally:
        summary['seconds'] = time.time() - summary['started_unix']
        summary['orchestrator_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        summary['full_Native_CI_capacity_or_R1_qualification'] = False
        (output / 'execution.receipt.json').write_text(json.dumps(summary, indent=2) + '\n')
        print(json.dumps(summary), flush=True)
    return 0 if summary['both_actual_cases_and_all_markers_passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
