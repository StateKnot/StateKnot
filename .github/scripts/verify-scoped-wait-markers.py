#!/usr/bin/env python3
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0
"""Validate both real scoped-Wait COMMIT-loss cuts on the PostgreSQL CI matrix."""
import hashlib
import json
import os
from pathlib import Path
import platform
import re

CASE = 'nested_driver::frame_wait_process_faults::scoped_wait_commit_loss_survives_real_kill_authorized_resolution_and_cold_driver'
PREFIX = 'STATEKNOT_SCOPED_WAIT_COMMIT_LOSS_EVIDENCE='
KEYS = {'profile', 'cut', 'commit_forwarded', 'commit_response_forwarded',
        'writer_killed_and_reaped', 'cold_driver_after_authorized_resolution',
        'condition_count', 'snapshot_tables', 'wait_application_redispatches',
        'scoped_successor_calls', 'parent_application_calls', 'fresh_old_fence_wait_refused',
        'original_wait_recovered', 'original_request_ack_after_return_verified',
        'invariants', 'postgres', 'termination', 'os', 'arch'}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate JSON key: ' + key)
        result[key] = value
    return result


def reject_constant(value):
    raise ValueError('nonstandard JSON constant: ' + value)


def validate_bytes(raw, major, expected_os, expected_arch):
    require(major in ('16', '17'), 'unexpected PostgreSQL matrix major')
    text = raw.decode('utf-8', errors='strict')
    lines = text.splitlines()
    require(lines.count('running 1 test') == 1, 'exact Wait controller did not run once')
    require(sum(line.startswith('test ' + CASE + ' ...') for line in lines) == 1, 'exact Wait start missing/repeated')
    summaries = re.findall(r'^test result: ok[.] (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out; finished in [0-9.]+s$', text, re.MULTILINE)
    require(summaries == [('1', '0', '0', '0', '193')], 'complete 1/193 Wait harness summary missing/repeated')
    marker_lines = [line[len(PREFIX):] for line in lines if line.startswith(PREFIX)]
    require(len(marker_lines) == 2, 'exactly two Wait cut markers required')
    seen = {}
    for encoded in marker_lines:
        row = json.loads(encoded, object_pairs_hook=unique_object, parse_constant=reject_constant)
        require(type(row) is dict and set(row) == KEYS, '19-key Wait marker schema differs')
        require(type(row['cut']) is str and row['cut'] in ('commit_not_forwarded', 'commit_response_withheld'), 'unknown Wait cut')
        cut = row['cut']
        require(cut not in seen, 'repeated Wait cut')
        committed = cut == 'commit_response_withheld'
        expected = {'profile': 'scoped-wait-commit-loss-v1', 'commit_forwarded': committed,
                    'commit_response_forwarded': False, 'writer_killed_and_reaped': True,
                    'cold_driver_after_authorized_resolution': True, 'condition_count': 2, 'snapshot_tables': 22,
                    'wait_application_redispatches': 0, 'scoped_successor_calls': 1, 'parent_application_calls': 1,
                    'fresh_old_fence_wait_refused': None if committed else True,
                    'original_wait_recovered': committed, 'original_request_ack_after_return_verified': committed,
                    'invariants': 'passed', 'termination': 'SIGKILL', 'os': expected_os, 'arch': expected_arch}
        for key, value in expected.items():
            require(type(row[key]) is type(value) and row[key] == value, 'invalid ' + cut + ': ' + key)
        require(type(row['postgres']) is str and re.fullmatch(re.escape(major) + r'[.][0-9]+(?:[ .(].*)?', row['postgres']),
                'Wait marker PostgreSQL version differs from matrix major')
        seen[cut] = row
    require(set(seen) == {'commit_not_forwarded', 'commit_response_withheld'}, 'missing Wait cut')
    require(len({row['postgres'] for row in seen.values()}) == 1, 'Wait cuts reported different PostgreSQL versions')
    return [seen[cut] for cut in ('commit_not_forwarded', 'commit_response_withheld')]


def main():
    require(platform.system() == 'Linux', 'Wait CI profile requires Linux')
    raw = Path('scoped-wait-commit-loss.log').read_bytes()
    major = os.environ['STATEKNOT_WAIT_EXPECTED_POSTGRES_MAJOR']
    rows = validate_bytes(raw, major, 'linux', platform.machine())
    result = {'profile': 'scoped-wait-commit-loss-v1', 'status': 'passed', 'exact_case': CASE,
              'full_runtime_inventory': 194, 'selected_passed': 1, 'filtered_out': 193, 'marker_count': 2,
              'log_sha256': hashlib.sha256(raw).hexdigest(), 'postgres_major': major,
              'runner_os': 'linux', 'runner_arch': platform.machine(), 'cuts': rows}
    Path('scoped-wait-commit-loss-validation.json').write_text(json.dumps(result, indent=2) + '\n')
    print('Both scoped-Wait COMMIT-loss cuts passed all harness, schema, type and field guards.')


if __name__ == '__main__':
    main()
