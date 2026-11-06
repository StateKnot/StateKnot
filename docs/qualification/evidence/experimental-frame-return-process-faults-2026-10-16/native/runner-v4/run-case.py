#!/usr/bin/env python3
"""Run the saved Return fault exact case in a fresh fixed-plan durable DB."""
import argparse
import hashlib
import json
import os
import re
import signal
import subprocess
import threading
import time
from pathlib import Path
import build as shared

MAJORS = {
    16: ('stateknot-r1-oct15-pg16', 'cd3def02c3d0888a0d3cf2d73ab6400ce02e80c65d961f7a5186a2017b0fa7ea',
         'sha256:a3b7f434b2dc57ce85a67e171163eb8ab1a1ebcb39d27484661f26b1dfbe30d6', 55416),
    17: ('stateknot-r1-oct15-pg17', '700ced678f1efb67e230785eb4d42d77c0327e64d032f6fd12ed386e46f43127',
         'sha256:f4c66b820c6f974249089d3d16d86a3698eae11e8746eb6644b2271031e91232', 55417),
}
FMT = '{"id":{{json .Id}},"image":{{json .Image}},"labels":{{json .Config.Labels}},"ports":{{json .NetworkSettings.Ports}},"running":{{json .State.Running}}}'
SETTINGS = "SELECT json_build_object('version',version(),'server_version_num',current_setting('server_version_num'),'fsync',current_setting('fsync'),'synchronous_commit',current_setting('synchronous_commit'),'full_page_writes',current_setting('full_page_writes'),'shared_buffers',current_setting('shared_buffers'),'max_connections',current_setting('max_connections'));"
# Existing public CI fixture value. Never print it or the complete URL.
FIXTURE_PASSWORD = 'stateknot_test_password'


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--build-dir', type=Path, required=True)
    p.add_argument('--major', type=int, choices=MAJORS, required=True)
    p.add_argument('--leg', choices=('B',), required=True)
    p.add_argument('--case', choices=('store_exact', 'runtime_exact'), required=True)
    p.add_argument('--out', type=Path, required=True)
    a = p.parse_args()
    out = a.out.resolve(); out.mkdir(parents=True, exist_ok=False)
    build_dir = a.build_dir.resolve(); summary = shared.obj(build_dir / 'build-summary.json')
    receipt = shared.obj(build_dir / 'build.receipt.json')
    assert shared.sha(build_dir / 'build.receipt.json') == summary['build_receipt_sha256']
    assert receipt['exit_code'] == 0 and receipt['source_before_after_exact'] is True
    package = a.case.split('_')[0]
    assert summary['package'] == package and summary['role'] == ('candidate' if a.leg == 'B' else 'baseline')
    assert package == 'runtime' or a.leg == 'B', 'Return fault execution is candidate-only'
    source = Path(summary['source'])
    m, ci = shared.check_source(source, Path(summary['source_manifest']), Path(summary['compiled_inputs']))
    assert shared.sha(summary['source_manifest']) == receipt['source_manifest_sha256']
    assert shared.sha(summary['compiled_inputs']) == receipt['compiled_manifest_sha256']
    assert m['source_tree_sha256'] == summary['source_tree'] and ci['sha256'] == summary['compiled_inputs_sha256']
    binary = Path(summary['saved_executable'])
    assert binary.resolve().is_relative_to(build_dir / 'binaries') and not binary.is_symlink()
    assert shared.sha(binary) == summary['binary_sha256']
    env = shared.common_env()
    assert shared.env_allowlist(env) == receipt['environment_allowlist']
    shared.disk(out, 'before')

    def command(label, argv, db_env=None):
        started = time.time()
        r = subprocess.run(argv, cwd=source, env=db_env or env, capture_output=True, timeout=30)
        # These inventory/inspect/local-psql commands contain no URL/password.
        assert FIXTURE_PASSWORD.encode() not in r.stdout + r.stderr
        (out / (label + '.stdout')).write_bytes(r.stdout)
        (out / (label + '.stderr')).write_bytes(r.stderr)
        shared.write(out, label + '.receipt.json', {'argv': argv, 'start_unix': started,
            'seconds': time.time() - started, 'exit_code': r.returncode,
            'stdout_sha256': shared.sha(out / (label + '.stdout')),
            'stderr_sha256': shared.sha(out / (label + '.stderr'))})
        assert r.returncode == 0, (label, r.returncode)
        return r.stdout

    name, identity, image, port = MAJORS[a.major]
    observed = json.loads(command('container-before', ['docker', 'inspect', '--format', FMT, name]))
    assert observed['id'] == identity and observed['image'] == image and observed['running'] is True
    assert observed['labels']['stateknot.task'] == 'r1-oct15'
    assert observed['ports']['5432/tcp'] == [{'HostIp': '127.0.0.1', 'HostPort': str(port)}]
    database = 'stateknot_r1_returnfaultv4_20261016_pg' + str(a.major) + '_' + package + '_' + a.leg.lower()
    assert re.fullmatch('[a-z0-9_]{1,63}', database)

    def sql(label, text, db='postgres'):
        return command(label, ['docker', 'exec', name, 'psql', '-X', '-v', 'ON_ERROR_STOP=1',
                               '-U', 'postgres', '-d', db, '-At', '-c', text])

    server_settings = json.loads(sql('server-settings-before', SETTINGS))
    assert int(server_settings['server_version_num']) // 10000 == a.major
    assert all(server_settings[k] == 'on' for k in ('fsync', 'synchronous_commit', 'full_page_writes'))
    assert sql('database-absent', "SELECT count(*) FROM pg_database WHERE datname='" + database + "';").strip() == b'0'
    command('create-database', ['docker', 'exec', name, 'createdb', '-U', 'postgres',
                               '--template=template0', '--encoding=UTF8', database])
    assert sql('database-created', 'SELECT current_database();', database).strip() == database.encode()
    db_settings = json.loads(sql('database-settings-before', SETTINGS, database))
    assert all(db_settings[k] == 'on' for k in ('fsync', 'synchronous_commit', 'full_page_writes'))
    _, expected_count, case = shared.CASES[package]
    inventory = command('full-inventory', [str(binary), '--list'])
    names = re.findall(rb'^([^\n]+): test$', inventory, re.M)
    assert len(names) == len(set(names)) == expected_count and case.encode() in names
    assert expected_count == summary['inventory_count'] and case == summary['exact_case']
    env.update(STATEKNOT_REQUIRE_POSTGRES_TESTS='1',
               STATEKNOT_TEST_DATABASE_URL='postgres://postgres:' + FIXTURE_PASSWORD + '@127.0.0.1:' + str(port) + '/' + database)
    argv = [str(binary), case, '--exact', '--test-threads=1', '--nocapture']
    started = time.time(); timed_out = False; streams = {}

    def capture(pipe, label):
        raw_hash = hashlib.sha256(); redactions = 0
        with (out / label).open('wb') as saved:
            for line in iter(pipe.readline, b''):
                raw_hash.update(line); redactions += line.count(FIXTURE_PASSWORD.encode())
                saved.write(line.replace(FIXTURE_PASSWORD.encode(), b'[REDACTED_PUBLIC_FIXTURE_PASSWORD]'))
        pipe.close()
        streams[label] = {'raw_sha256': raw_hash.hexdigest(), 'redactions': redactions,
                          'saved_sha256': shared.sha(out / label), 'saved_is_raw': redactions == 0}

    process = subprocess.Popen(argv, cwd=source, env=env, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, start_new_session=True)
    running = {'argv': argv, 'cwd': str(source), 'pid': process.pid, 'runner_pid': os.getpid(),
               'start_unix': started, 'runner_sha256': shared.sha(__file__),
               'shared_build_module_sha256': shared.sha(Path(shared.__file__)),
               'role': summary['role'], 'leg': a.leg, 'case': a.case,
               'source_tree': summary['source_tree'], 'compiled_inputs_sha256': summary['compiled_inputs_sha256'],
               'binary_sha256': shared.sha(binary), 'profile_build_receipt_sha256': shared.sha(build_dir / 'build.receipt.json'),
               'environment_allowlist': shared.env_allowlist(env), 'mandatory_database_env_present': True,
               'overrides_present': {k: k in env for k in shared.OVERRIDES},
               'database': {'container_id': identity, 'image_id': image, 'port': port, 'name': database,
                            'URL_credentials_not_logged': True},
               'full_inventory_count': expected_count, 'expected_filtered': expected_count - 1,
               'external_observation_watchdog_seconds': 600, 'original_business_budgets_unchanged': True,
               'diagnostic_markers_required': False}
    shared.write(out, 'running.json', running); print(json.dumps(running), flush=True)
    threads = [threading.Thread(target=capture, args=(process.stdout, 'test.stdout')),
               threading.Thread(target=capture, args=(process.stderr, 'test.stderr'))]
    for t in threads:
        t.start()
    try:
        code = process.wait(timeout=600)
    except subprocess.TimeoutExpired:
        timed_out = True; os.killpg(process.pid, signal.SIGTERM)
        try:
            code = process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL); code = process.wait()
    seconds = time.time() - started
    for t in threads:
        t.join()
    assert set(streams) == {'test.stdout', 'test.stderr'}, 'output capture failed'
    test_receipt = {**running, 'exit_code': code, 'seconds': seconds, 'timed_out': timed_out,
                    'output_streams': streams, 'database_preserved_no_cleanup': True}
    shared.write(out, 'test.receipt.json', test_receipt)
    print(json.dumps(test_receipt), flush=True)
    # Save actual process outcome first; a later observation failure never
    # overwrites it or becomes a fabricated product/test failure.
    after_m, after_ci = shared.check_source(source, Path(summary['source_manifest']), Path(summary['compiled_inputs']))
    assert after_m == m and after_ci == ci and shared.sha(binary) == summary['binary_sha256']
    observed_after = json.loads(command('container-after', ['docker', 'inspect', '--format', FMT, name]))
    settings_after = json.loads(sql('database-settings-after', SETTINGS, database))
    server_after = json.loads(sql('server-settings-after', SETTINGS))
    assert observed_after == observed and settings_after == db_settings and server_after == server_settings
    shared.disk(out, 'after')
    output = (out / 'test.stdout').read_bytes()
    matches = re.findall(rb'^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;', output, re.M)
    expected = [(b'1', b'0', b'0', b'0', str(expected_count - 1).encode())]
    # --nocapture may print evidence between libtest's exact case start and ok.
    # Bind exactly one selected start to its inline or standalone completion.
    starts = list(re.finditer(rb'^test ' + re.escape(case.encode()) + rb' \.\.\. (?:(ok))?$', output, re.M))
    completions = re.findall(rb'^ok$', output, re.M)
    selected = len(starts) == 1 and (
        (starts[0].group(1) == b'ok' and not completions)
        or (starts[0].group(1) is None and completions == [b'ok']
            and output.find(b'\nok\n', starts[0].end()) >= starts[0].end()))
    ok = code == 0 and not timed_out and matches == expected and selected
    shared.write(out, 'observations.json', {'exact_case_passed': ok, 'source_and_binary_before_after_exact': True,
        'container_and_durability_settings_unchanged': True, 'libtest_summary': [list(map(int, x)) for x in matches],
        'test_receipt_sha256': shared.sha(out / 'test.receipt.json'), 'diagnostic_markers_required': False,
        'whole_suite_Native_cold_CI_capacity_or_R1_qualification': False})
    assert ok, 'exact-case outcome/summary was not qualified; original receipt preserved'


if __name__ == '__main__':
    main()
