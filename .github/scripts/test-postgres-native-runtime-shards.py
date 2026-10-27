#!/usr/bin/env python3
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0
"""Positive-first Native shard artifact controls; no binaries, Cargo, DB or API.

Only the producer binding/environment and final live process-scan boundaries
are substituted. Persisted owner identities and ABI receipts remain real inputs.
The real sharded plan, harness, owner, routing and stream validators run over
complete artifact-shaped files and original 276/229 producer inventories.
Every rejection first proves that its untouched fixture is accepted.
"""
import copy
from contextlib import ExitStack, contextmanager
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
DIRECTORY = Path(__file__).resolve().parent
FIXTURES = DIRECTORY / 'tests/fixtures/postgres-native-runtime-shards'
# These protocol expectations are independent of the implementation constants.
LANES = ('store', 'runtime-0', 'runtime-1')
DATABASES = {'store': 'stateknot_store_native', 'runtime-0': 'stateknot_runtime_native_0',
             'runtime-1': 'stateknot_runtime_native_1'}

def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, DIRECTORY / filename)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded

binaries = module('native_shard_binary_controls', 'postgres-native-binaries.py')
artifacts = module('native_shard_artifact_controls', 'postgres-native-artifacts.py')

def digest(raw):
    return hashlib.sha256(raw).hexdigest()

def put(root, name, value):
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(value if isinstance(value, bytes) else value.encode())

def put_json(root, name, value):
    put(root, name, json.dumps(value, sort_keys=True, indent=2) + '\n')

def inventory(names):
    return (''.join(name + ': test\n' for name in names)
            + '\n' + str(len(names)) + ' tests, 0 benchmarks\n').encode()

def harness(names, total):
    return ('\nrunning ' + str(len(names)) + ' tests\n'
            + ''.join('test ' + name + ' ... ok\n' for name in names)
            + '\ntest result: ok. ' + str(len(names))
            + ' passed; 0 failed; 0 ignored; 0 measured; ' + str(total - len(names))
            + ' filtered out; finished in 0.75s\n\n').encode()

class Evidence:
    def __init__(self, root):
        self.root = root
        self.out = root / 'postgres-native-parallel'
        self.out.mkdir()
        provenance = json.loads((FIXTURES / 'provenance.json').read_bytes())
        if tuple(binaries.LANES) != LANES or binaries.DATABASES != DATABASES:
            raise ValueError('implementation lane or database constants drifted from the independent protocol')
        self.names, raw_lists = {}, {}
        for role, row in provenance['roles'].items():
            raw = (FIXTURES / row['file']).read_bytes()
            if (len(raw), digest(raw)) != (row['bytes'], row['sha256']):
                raise ValueError('controlled real producer fixture changed')
            if row['inventory'] != binaries.ROLES[role][1]:
                raise ValueError('fixture update requires an explicit actual inventory update')
            lines = raw.decode('utf-8', errors='strict').splitlines()
            names = [line.removesuffix(': test') for line in lines[:-2]]
            if (len(names) != row['inventory'] or len(set(names)) != len(names)
                    or lines[-2:] != ['', str(row['inventory']) + ' tests, 0 benchmarks']
                    or any(line != name + ': test' for line, name in zip(lines[:-2], names))):
                raise ValueError('independent original fixture inventory grammar/count')
            self.names[role] = sorted(names)
            raw_lists[role] = raw
            put(self.out, 'import/producer/' + role + '-inventory.stdout', raw)
        source = {'head': 'c9c3398015ea3183f915c7ec1dd4d7aa81d497ac',
                  'files': [], 'manifest_sha256': '1' * 64}
        self.receipts, self.environments = {}, {}
        for role in ('store', 'runtime'):
            binary = root / 'target/debug/deps' / ('postgres-' + role)
            put(root, str(binary.relative_to(root)), ('unit fixture ' + role).encode())
            manifest = root / 'crates' / binaries.ROLES[role][0] / 'Cargo.toml'
            receipt = {'role': role, 'binary': str(binary), 'binary_sha256': digest(binary.read_bytes()),
                       'source': source, 'inventory_count': binaries.ROLES[role][1],
                       'expected_inventory': binaries.ROLES[role][1],
                       'selected_artifact': {'features': [], 'profile': {'opt_level': '1', 'debuginfo': 0,
                           'debug_assertions': True, 'overflow_checks': True, 'test': True}},
                       'inventory_stdout_sha256': digest(raw_lists[role]),
                       'inventory_stderr_sha256': digest(b''),
                       'package': {'manifest_path': str(manifest)}}
            self.receipts[role] = receipt
            put_json(self.out, role + '-binary.json', receipt)
            self.environments[role] = {'CARGO': '/unit/pinned/cargo',
                'CARGO_PKG_NAME': binaries.ROLES[role][0],
                'CARGO_MANIFEST_DIR': str(manifest.parent), 'CARGO_MANIFEST_PATH': str(manifest),
                'LD_LIBRARY_PATH': '/unit/pinned/lib'}
        runtime = self.receipts['runtime']
        put(self.out, 'runtime-shards-inventory.stdout', raw_lists['runtime'])
        put(self.out, 'runtime-shards-inventory.stderr', b'')
        lanes = []
        for number in (0, 1):
            label, selected = 'runtime-' + str(number), self.names['runtime'][number::2]
            listed = inventory(selected)
            put(self.out, label + '-selection.stdout', listed)
            put(self.out, label + '-selection.stderr', b'')
            lanes.append({'label': label, 'index': number, 'role': 'runtime',
                'database': DATABASES[label], 'passed': len(selected),
                'filtered': binaries.ROLES['runtime'][1] - len(selected), 'names': selected,
                'argv': [runtime['binary'], *selected, '--exact', '--test-threads=1'],
                'selection_stdout_sha256': digest(listed), 'selection_stderr_sha256': digest(b'')})
        closure = {role: [{'name': '<binary>', 'sha256': self.receipts[role]['binary_sha256']}]
                   for role in ('store', 'runtime')}
        manifest_sha = '2' * 64
        put_json(self.out, 'import/import.json', {
            'schema': 'stateknot-postgres-native-artifact-v1-import', 'source': source,
            'consumer_local_Cargo_build': False, 'producer_receipts_preserved': True,
            'manifest_sha256': manifest_sha, 'consumer_admission_runtime': closure,
            'role_receipt_sha256': {role: digest((self.out / (role + '-binary.json')).read_bytes())
                                   for role in ('store', 'runtime')}})
        for phase in ('before', 'after'):
            put_json(self.out, 'import/' + phase + '.json', {
                'schema': 'stateknot-postgres-native-artifact-v1-verification', 'phase': phase,
                'actual_runtime_closure': closure, 'all_source_binary_DSO_unchanged': True,
                'producer_manifest_sha256': manifest_sha, 'consumer_local_Cargo_build': False})
        self.plan = {'schema': binaries.PLAN_SCHEMA, 'execution_schema': binaries.EXEC_SCHEMA,
            'assignment': 'sorted-full-inventory-alternating-v1',
            'runtime_binary_sha256': runtime['binary_sha256'],
            'runtime_receipt_sha256': digest((self.out / 'runtime-binary.json').read_bytes()),
            'source': source, 'profile': binaries.PROFILE,
            'producer_inventory_sha256': digest(raw_lists['runtime']),
            'actual_inventory_sha256': digest(raw_lists['runtime']),
            'inventory_count': binaries.ROLES['runtime'][1], 'all_names': self.names['runtime'], 'lanes': lanes}
        self.write_plan()
        self.executions = {}
        started, observed, completed = 1700000000000000000, 1700000001000000000, 1700000004000000000
        for number, label in enumerate(LANES):
            role = 'store' if label == 'store' else 'runtime'
            receipt = self.receipts[role]
            selected = self.names['store'] if role == 'store' else lanes[number - 1]['names']
            argv = [receipt['binary'], '--test-threads=1'] if role == 'store' else lanes[number - 1]['argv']
            execution = {'pid': 200000000 + number, 'start_ticks': str(4000 + number),
                'binary': receipt['binary'], 'binary_sha256': receipt['binary_sha256'],
                'cwd': str(Path(receipt['package']['manifest_path']).parent), 'argv': argv,
                'started_unix_ns': started + number * 10000000,
                'runtime_environment': self.environments[role]}
            if role == 'runtime':
                execution.update({'execution_schema': binaries.EXEC_SCHEMA, 'lane': label,
                    'logical_role': role, 'database': DATABASES[label],
                    'plan_sha256': digest((self.out / 'runtime-shards-plan.json').read_bytes()),
                    'selected_names_sha256': digest(json.dumps(selected, separators=(',', ':')).encode())})
            self.executions[label] = execution
            put_json(self.out, label + '-execution.json', execution)
            put(self.out, label + '.stdout', harness(selected, binaries.ROLES[role][1]))
            put(self.out, label + '.stderr', b'')
            identity = str(100000000 + number) + ' ' + str(3000 + number) + '\n'
            for filename, value in {'leader-identity': identity, 'helper-pid': str(300000000 + number) + '\n',
                'cleanup-complete': identity, 'cleanup-reason': 'parent-cleanup\n',
                'helper-wait-status': '0\n', 'suite-status': '0\n'}.items():
                put(self.out, label + '-owner/' + filename, value)
            command = 'python3 .github/scripts/postgres-native-binaries.py '
            command += 'run store' if role == 'store' else 'run-runtime-shard ' + label[-1]
            put(self.out, label + '-command.txt', command + '\n')
        identities = {}
        for number, label in enumerate(LANES):
            leader, helper, binary = 100000000 + number, 300000000 + number, 200000000 + number
            def identity(pid, start, parent, exe):
                return {'pid': pid, 'start_ticks': str(start), 'ppid': parent,
                        'pgrp': leader, 'session': leader, 'exe': exe}
            identities[label] = {
                'leader': identity(leader, 3000 + number, 400000000, '/usr/bin/bash'),
                'helper': identity(helper, 3500 + number, leader, '/usr/bin/bash'),
                'binary': identity(binary, 4000 + number, helper, self.executions[label]['binary'])}
        self.overlap = {'schema': binaries.EXEC_SCHEMA, 'observed_unix_ns': observed,
            'all_actual_binary_processes_alive': True, 'processes': copy.deepcopy(self.executions),
            'databases': dict(DATABASES), 'identities': identities}
        self.write_overlap()
        put(self.out, 'created-databases.txt', ''.join(DATABASES[lane] + '\n' for lane in LANES))
        put(self.out, 'pids.tsv', ''.join(lane + '\t' + str(100000000 + number) + '\n'
            for number, lane in enumerate(LANES)))
        for filename in ('suite-exits.tsv', 'leader-exits.tsv'):
            put(self.out, filename, ''.join(lane + '\t0\n' for lane in LANES))
        put(self.out, 'retired.tsv', ''.join(lane + '\t' + str(100000000 + number) + '\t0\n'
            for number, lane in enumerate(LANES)))
        put(self.out, 'retire-requests.tsv', ''.join(lane + '\tsuite-complete\n' for lane in LANES))
        put(self.out, 'cleanup.tsv', 'monitor_exit\t0\n')
        put(self.out, 'lane-completions.tsv', ''.join(lane + '\t' + str(completed + number * 10000000) + '\n'
            for number, lane in enumerate(LANES)))
        put(self.out, 'pair-started.txt', '2023-11-14T22:13:19Z\n')
        put(self.out, 'pair-completed.txt', '2023-11-14T22:13:25Z\n')
        self.rehash_streams()

    def write_plan(self):
        put_json(self.out, 'runtime-shards-plan.json', self.plan)
        put_json(self.out, 'execution-schema.json', {'schema': binaries.EXEC_SCHEMA,
            'plan_sha256': digest((self.out / 'runtime-shards-plan.json').read_bytes()),
            'lanes': list(LANES), 'pair_cap_seconds': 1800})

    def write_overlap(self):
        put_json(self.out, 'native-binary-overlap-sharded.json', self.overlap)

    def rewrite_execution(self, lane, key, value):
        self.executions[lane][key] = value
        self.overlap['processes'][lane][key] = value
        put_json(self.out, lane + '-execution.json', self.executions[lane])
        self.write_overlap()

    def rehash_streams(self):
        put(self.out, 'log-sha256.txt', ''.join(digest((self.out / (lane + '.' + stream)).read_bytes())
            + '  postgres-native-parallel/' + lane + '.' + stream + '\n'
            for lane in LANES for stream in ('stdout', 'stderr')))

    def replace_stream(self, lane, value):
        put(self.out, lane + '.stdout', value)
        self.rehash_streams()

@contextmanager
def evidence():
    with tempfile.TemporaryDirectory() as raw, ExitStack() as stack:
        fixture = Evidence(Path(raw))
        stack.enter_context(patch.object(binaries, 'ROOT', fixture.root))
        stack.enter_context(patch.object(binaries, 'OUT', fixture.out))
        stack.enter_context(patch.object(binaries, 'load_bound', lambda role: copy.deepcopy(fixture.receipts[role])))
        stack.enter_context(patch.object(binaries, 'runtime_env', lambda role, receipt: dict(fixture.environments[role])))
        fixture.groups_retired = stack.enter_context(patch.object(binaries, 'assert_groups_retired'))
        yield fixture

class RuntimeShardEvidenceTests(unittest.TestCase):
    def accepted(self, fixture):
        binaries.verify_sharded()
        output = fixture.out / 'native-runtime-shards-verification.json'
        result = json.loads(output.read_bytes())
        self.assertEqual(result['full_runtime_inventory'], binaries.ROLES['runtime'][1])
        self.assertFalse(result['synthetic_full_runtime_summary'])
        self.assertEqual(set(result['results']), {'store', 'runtime-0', 'runtime-1'})
        fixture.groups_retired.assert_called_once_with([100000000 + number for number in range(len(LANES))])
        fixture.groups_retired.reset_mock()
        output.unlink()
        return result

    def reject(self, mutation, error=ValueError):
        with evidence() as fixture:
            self.accepted(fixture)  # Mandatory positive-first; never swallowed by assertRaises.
            mutation(fixture)
            with self.assertRaises(error):
                binaries.verify_sharded()
            self.assertFalse((fixture.out / 'native-runtime-shards-verification.json').exists())

    def test_complete_original_inventory_has_disjoint_once_union_and_three_raw_streams(self):
        with evidence() as fixture:
            result = self.accepted(fixture)
            names = [lane['names'] for lane in fixture.plan['lanes']]
            self.assertTrue(set(names[0]).isdisjoint(names[1]))
            self.assertEqual(sorted(names[0] + names[1]), fixture.names['runtime'])
            self.assertEqual(sum(result['results'][lane]['passed'] for lane in ('runtime-0', 'runtime-1')),
                             binaries.ROLES['runtime'][1])
            for number, lane in enumerate(('runtime-0', 'runtime-1')):
                expected = len(fixture.names['runtime'][number::2])
                self.assertEqual(result['results'][lane]['passed'], expected)
                self.assertEqual(result['results'][lane]['filtered'], len(fixture.names['runtime']) - expected)
            self.assertEqual(result['results']['store']['passed'], len(fixture.names['store']))
            self.assertEqual(result['results']['store']['filtered'], 0)
            self.assertFalse((fixture.out / 'runtime.stdout').exists())

    def test_missing_inventory_case_is_rejected(self):
        self.reject(lambda f: put(f.out, 'runtime-shards-inventory.stdout', inventory(f.names['runtime'][:-1])))

    def test_duplicate_inventory_case_is_rejected(self):
        def mutate(f):
            names = list(f.names['runtime']); names[-1] = names[0]
            put(f.out, 'runtime-shards-inventory.stdout', inventory(names))
        self.reject(mutate)

    def test_lane_union_cannot_omit_original_case(self):
        def mutate(f):
            f.plan['lanes'][1]['names'].pop(); f.write_plan()
        self.reject(mutate)

    def test_lane_union_cannot_duplicate_other_lane_case(self):
        def mutate(f):
            f.plan['lanes'][1]['names'][0] = f.plan['lanes'][0]['names'][0]; f.write_plan()
        self.reject(mutate)

    def test_wrong_filter_argv_is_rejected(self):
        def mutate(f):
            f.plan['lanes'][0]['argv'].remove('--exact'); f.write_plan()
        self.reject(mutate)

    def test_missing_result_is_rejected_even_with_fresh_raw_hash(self):
        def mutate(f):
            names = f.plan['lanes'][0]['names']
            raw = harness(names, binaries.ROLES['runtime'][1])
            f.replace_stream('runtime-0', raw.replace(('test ' + names[0] + ' ... ok\n').encode(), b''))
        self.reject(mutate)

    def test_duplicate_result_is_rejected_even_with_fresh_raw_hash(self):
        def mutate(f):
            names = f.plan['lanes'][0]['names']
            f.replace_stream('runtime-0', harness(names, binaries.ROLES['runtime'][1])
                             + ('test ' + names[0] + ' ... ok\n').encode())
        self.reject(mutate)

    def test_wrong_passed_filtered_total_is_rejected(self):
        def mutate(f):
            names = f.plan['lanes'][0]['names']
            f.replace_stream('runtime-0', harness(names, binaries.ROLES['runtime'][1] + 1))
        self.reject(mutate)

    def test_result_from_other_lane_cannot_replace_selected_case(self):
        def mutate(f):
            names = list(f.plan['lanes'][0]['names'])
            names[0] = f.plan['lanes'][1]['names'][0]
            f.replace_stream('runtime-0', harness(names, binaries.ROLES['runtime'][1]))
        self.reject(mutate)

    def test_lane_cross_database_is_rejected(self):
        self.reject(lambda f: f.rewrite_execution('runtime-0', 'database', DATABASES['runtime-1']))

    def test_same_runtime_binary_hash_cannot_be_substituted(self):
        self.reject(lambda f: f.rewrite_execution('runtime-0', 'binary_sha256', '0' * 64))

    def test_execution_pid_cannot_be_reused_by_other_lane(self):
        self.reject(lambda f: f.rewrite_execution('runtime-1', 'pid', f.executions['runtime-0']['pid']))

    def test_actual_database_observation_cannot_cross_lanes(self):
        def mutate(f):
            f.overlap['databases']['runtime-0'] = DATABASES['runtime-1']; f.write_overlap()
        self.reject(mutate)

    def test_owner_cleanup_must_match_original_identity(self):
        self.reject(lambda f: put(f.out, 'runtime-0-owner/cleanup-complete', '1 2\n'))

    def test_forced_kill_is_never_successful_retirement(self):
        self.reject(lambda f: put(f.out, 'runtime-0-owner/forced-kill', 'unqualified\n'))

    def test_failed_helper_wait_is_rejected(self):
        self.reject(lambda f: put(f.out, 'runtime-0-owner/helper-wait-status', '1\n'))

    def test_active_or_missing_lane_cannot_be_a_complete_retirement(self):
        self.reject(lambda f: put(f.out, 'retired.tsv', 'store\t100000000\t0\n'))

    def test_raw_stream_bytes_must_match_saved_six_hashes(self):
        self.reject(lambda f: put(f.out, 'runtime-0.stderr', b'changed after capture'))

    def test_concatenated_runtime_streams_cannot_masquerade_as_one_lane(self):
        def mutate(f):
            f.replace_stream('runtime-0', (f.out / 'runtime-0.stdout').read_bytes()
                             + (f.out / 'runtime-1.stdout').read_bytes())
        self.reject(mutate)

    def test_legacy_full_runtime_stream_is_rejected_with_complete_lanes(self):
        def mutate(f):
            put(f.out, 'runtime.stdout', harness(f.names['runtime'], binaries.ROLES['runtime'][1]))
        self.reject(mutate, ValueError)

    def test_missing_runtime_lane_stream_is_rejected(self):
        self.reject(lambda f: (f.out / 'runtime-0.stdout').unlink(), FileNotFoundError)

    def test_pair_budget_cannot_be_extended_in_policy(self):
        def mutate(f):
            value = json.loads((f.out / 'execution-schema.json').read_bytes())
            value['pair_cap_seconds'] = 1801; put_json(f.out, 'execution-schema.json', value)
        self.reject(mutate)

    def test_store_full_inventory_cannot_omit_a_case(self):
        def mutate(f):
            f.replace_stream('store', harness(f.names['store'][:-1], len(f.names['store'])))
        self.reject(mutate)

    def test_store_may_not_report_filtered_cases(self):
        def mutate(f):
            f.replace_stream('store', harness(f.names['store'], len(f.names['store']) + 1))
        self.reject(mutate)

    def test_duplicate_success_summary_cannot_qualify(self):
        def mutate(f):
            raw = (f.out / 'runtime-0.stdout').read_bytes()
            summary = next(line for line in raw.splitlines() if line.startswith(b'test result:'))
            f.replace_stream('runtime-0', raw + summary + b'\n')
        self.reject(mutate)

    def test_execution_filter_must_match_the_bound_plan(self):
        def mutate(f):
            argv = list(f.executions['runtime-0']['argv']); argv.remove('--exact')
            f.rewrite_execution('runtime-0', 'argv', argv)
        self.reject(mutate)

    def test_two_runtime_lanes_must_use_the_same_binary_path(self):
        self.reject(lambda f: f.rewrite_execution('runtime-1', 'binary', f.receipts['store']['binary']))

    def test_original_leader_start_identity_cannot_be_replaced(self):
        def mutate(f):
            f.overlap['identities']['runtime-0']['leader']['start_ticks'] = '999'
            f.write_overlap()
        self.reject(mutate)

    def test_binary_start_identity_must_match_the_execution_receipt(self):
        def mutate(f):
            f.overlap['identities']['runtime-0']['binary']['start_ticks'] = '999'
            f.write_overlap()
        self.reject(mutate)

    def test_helper_must_be_a_child_of_its_original_leader(self):
        def mutate(f):
            f.overlap['identities']['runtime-0']['helper']['ppid'] = 1
            f.write_overlap()
        self.reject(mutate)

    def test_binary_must_be_a_child_of_its_original_helper(self):
        def mutate(f):
            f.overlap['identities']['runtime-0']['binary']['ppid'] = 1
            f.write_overlap()
        self.reject(mutate)

    def test_process_group_and_session_cannot_escape_the_lane(self):
        for kind, field in (('binary', 'pgrp'), ('helper', 'session')):
            with self.subTest(kind=kind, field=field):
                def mutate(f):
                    f.overlap['identities']['runtime-0'][kind][field] = 1
                    f.write_overlap()
                self.reject(mutate)

    def test_leaders_must_have_one_original_monitor_parent(self):
        def mutate(f):
            f.overlap['identities']['runtime-0']['leader']['ppid'] += 1
            f.write_overlap()
        self.reject(mutate)

    def test_observed_binary_executable_must_be_the_bound_runtime_binary(self):
        def mutate(f):
            f.overlap['identities']['runtime-0']['binary']['exe'] = f.receipts['store']['binary']
            f.write_overlap()
        self.reject(mutate)

    def test_actual_failed_suite_wait_cannot_be_relabelled_success(self):
        def mutate(f):
            put(f.out, 'suite-exits.tsv', 'store\t0\nruntime-0\t1\nruntime-1\t0\n')
        self.reject(mutate)

    def test_residual_owned_process_group_prevents_success_receipt(self):
        def mutate(f):
            f.groups_retired.side_effect = ValueError('actual owned process group remains')
        self.reject(mutate)

    def test_before_and_after_abi_closures_must_equal_admission(self):
        for phase in ('before', 'after'):
            with self.subTest(phase=phase):
                def mutate(f):
                    rel = 'import/' + phase + '.json'
                    value = json.loads((f.out / rel).read_bytes())
                    value['actual_runtime_closure']['runtime'][0]['sha256'] = '0' * 64
                    put_json(f.out, rel, value)
                self.reject(mutate)

    def test_abi_verification_cannot_claim_a_different_producer_manifest(self):
        def mutate(f):
            rel = 'import/after.json'; value = json.loads((f.out / rel).read_bytes())
            value['producer_manifest_sha256'] = '0' * 64
            put_json(f.out, rel, value)
        self.reject(mutate)

    def test_local_consumer_compilation_cannot_qualify(self):
        def mutate(f):
            rel = 'import/import.json'; value = json.loads((f.out / rel).read_bytes())
            value['consumer_local_Cargo_build'] = True
            put_json(f.out, rel, value)
        self.reject(mutate)

    def test_imported_source_cannot_differ_from_bound_binaries(self):
        def mutate(f):
            rel = 'import/import.json'; value = json.loads((f.out / rel).read_bytes())
            value['source']['head'] = '0' * 40
            put_json(f.out, rel, value)
        self.reject(mutate)

    def test_missing_after_abi_receipt_cannot_fall_back_to_legacy_evidence(self):
        self.reject(lambda f: (f.out / 'import/after.json').unlink(), FileNotFoundError)

    def test_native_runtime_features_must_remain_default(self):
        self.reject(lambda f: f.receipts['runtime']['selected_artifact'].update(features=['other']))

    def test_native_runtime_overflow_and_debug_assertions_remain_enabled(self):
        for field in ('overflow_checks', 'debug_assertions'):
            with self.subTest(field=field):
                self.reject(lambda f: f.receipts['runtime']['selected_artifact']['profile'].update({field: False}))

    def test_pair_observer_budget_remains_below_thirty_minutes(self):
        self.reject(lambda f: put(f.out, 'pair-completed.txt', '2023-11-14T22:43:20Z\n'))

    def test_pair_timestamp_must_include_all_actual_lane_exits(self):
        self.reject(lambda f: put(f.out, 'pair-completed.txt', '2023-11-14T22:13:21Z\n'))

    def test_imported_producer_inventory_cannot_fall_back_to_unbound_local_list(self):
        def mutate(f):
            raw = (f.out / 'import/producer/runtime-inventory.stdout').read_bytes()
            put(f.out, 'runtime-inventory.stdout', raw)
            (f.out / 'import/producer/runtime-inventory.stdout').unlink()
        self.reject(mutate, FileNotFoundError)

class CurrentContractTests(unittest.TestCase):
    def reject(self, mutate):
        contract = json.loads((DIRECTORY.parent / 'postgres-native-artifact-contract.json').read_bytes())
        workflow = digest((DIRECTORY.parent / 'workflows/ci.yml').read_bytes())
        artifacts.validate_ci_contract(contract, workflow)  # Positive-first again.
        mutate(contract)
        with self.assertRaises(ValueError):
            artifacts.validate_ci_contract(contract, workflow)

    def test_current_contract_is_the_explicit_successor(self):
        contract = json.loads((DIRECTORY.parent / 'postgres-native-artifact-contract.json').read_bytes())
        artifacts.validate_ci_contract(contract, digest((DIRECTORY.parent / 'workflows/ci.yml').read_bytes()))
        self.assertEqual(contract['native_execution']['runtime_assignment']['shard_count'], 2)
        self.assertEqual(artifacts.SCHEMA, 'stateknot-postgres-native-artifact-v1')

    def test_sealed_v1_is_not_a_current_sharded_schema(self):
        self.reject(lambda c: c.update(schema='stateknot-postgres-native-shared-build-ci-contract-v1'))

    def test_old_store275_literal_is_rejected(self):
        self.reject(lambda c: c['full_inventory'].update(store=275))

    def test_unknown_execution_type_is_rejected(self):
        self.reject(lambda c: c['native_execution'].update(schema='unrecognized-execution'))

    def test_synthetic_runtime_stream_policy_is_rejected(self):
        self.reject(lambda c: c['native_execution']['artifact_layout'].update(synthetic_runtime_streams_allowed=True))

    def test_other_lane_database_in_contract_is_rejected(self):
        self.reject(lambda c: c['native_execution']['lanes'][1].update(database='stateknot_runtime_native_1'))

    def test_workflow_drift_is_rejected(self):
        self.reject(lambda c: c.update(current_candidate_workflow_sha256='0' * 64))

    def test_old_pair_owner_assertion_is_rejected(self):
        self.reject(lambda c: c.update(native_pair_owner_unchanged=True))

    def test_three_lane_owner_cannot_reuse_the_two_lane_arrangement(self):
        self.reject(lambda c: c['native_execution']['owner_protocol'].update(original_two_lane_arrangement_unchanged=True))

    def test_one_live_owner_cannot_satisfy_all_three(self):
        self.reject(lambda c: c['native_execution']['owner_protocol'].update(successful_cleanup_requires_all_three_retired=False))

    def test_runtime_lanes_cannot_use_independent_binaries(self):
        self.reject(lambda c: c['native_execution']['runtime_assignment'].update(same_runtime_binary=False))

    def test_counts_cannot_use_a_fixed_subset_literal(self):
        self.reject(lambda c: c['native_execution']['runtime_assignment'].update(passed_count=115))

    def test_current_upload_mapping_must_select_the_three_lane_layout(self):
        def mutate(c):
            next(row for row in c['current_60_step_mapping'] if row['source_index'] == 5)['artifact_layout_schema'] = 'legacy'
        self.reject(mutate)

if __name__ == '__main__':
    unittest.main()
