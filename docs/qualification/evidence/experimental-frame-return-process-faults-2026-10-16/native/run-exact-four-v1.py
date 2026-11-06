#!/usr/bin/env python3
"""Run four actual saved-binary cases serially; retain the first failure."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
RUNNER = HERE.parent / 'graph-frame-return-process-fault-native-runner-v3/run-case.py'
PIN = '10d37cb3583def862427473c4bbdfd673bb48fe9dec208f6f5e260c8b08469fc'


def main():
    assert hashlib.sha256(RUNNER.read_bytes()).hexdigest() == PIN
    output = HERE / 'exact-four-execution-v1'
    output.mkdir(exist_ok=False)
    summary = {'started_unix': time.time(), 'case_runner_sha256': PIN,
               'actual_results': [], 'all_four_cases_and_return_markers_passed': False}
    try:
        for major in [16, 17]:
            for package in ['store', 'runtime']:
                destination = HERE / ('pg' + str(major) + '-' + package + '-exact')
                argv = [sys.executable, '-B', str(RUNNER), '--build-dir',
                        str(HERE / ('build-candidate-' + package)), '--major', str(major),
                        '--leg', 'B', '--case', package + '_exact', '--out', str(destination)]
                result = subprocess.run(argv)
                row = {'major': major, 'package': package, 'runner_exit_code': result.returncode}
                summary['actual_results'].append(row)
                assert result.returncode == 0, 'actual exact case failed; remaining cases not launched'
                if package == 'runtime':
                    prefix = 'STATEKNOT_FRAME_RETURN_COMMIT_LOSS_EVIDENCE='
                    lines = (destination / 'test.stdout').read_text().splitlines()
                    markers = [json.loads(line[len(prefix):]) for line in lines if line.startswith(prefix)]
                    assert len(markers) == 2
                    cuts = {m['cut'] for m in markers}
                    assert cuts == {'commit_not_forwarded', 'commit_response_withheld'}
                    for marker in markers:
                        assert marker['profile'] == 'frame-return-commit-loss-v1'
                        assert marker['writer_killed_and_reaped'] is True
                        assert marker['cold_higher_fence_driver_verified'] is True
                        assert marker['leaf_application_redispatches'] == 0
                        assert marker['parent_application_calls'] == 1
                        assert marker['invariants'] == 'passed'
                        assert marker['termination'] == 'SIGKILL'
                        assert marker['commit_response_forwarded'] is False
                        committed = marker['cut'] == 'commit_response_withheld'
                        assert marker['commit_forwarded'] is committed
                        assert marker['original_return_recovered'] is committed
                    row['two_return_markers_verified'] = True
                    (destination / 'verified-return-markers.json').write_text(json.dumps(markers, indent=2) + '\n')
        summary['all_four_cases_and_return_markers_passed'] = True
    except Exception as error:
        summary['failure'] = {'type': type(error).__name__, 'message': str(error)}
    finally:
        summary['seconds'] = time.time() - summary['started_unix']
        (output / 'execution.receipt.json').write_text(json.dumps(summary, indent=2) + '\n')
        print(json.dumps(summary), flush=True)
    return 0 if summary['all_four_cases_and_return_markers_passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
