from pathlib import Path
import hashlib
import json
import os
import re
import subprocess
import sys
import time

root = Path('/Users/jiawy/Documents/RustAgentFramework')
base = root / '.local-verification/2026-10-15'
integration = base / 'elapsed36-attempt-proof-integration'
output = integration / 'full-native-v1'
plan = json.loads((output / 'plan.json').read_text())
source = Path(plan['source'])
manifest = json.loads((integration / 'source-manifest.json').read_text())
major = sys.argv[1]
assert major in ['16', '17'] and 'RUST_MIN_STACK' not in os.environ
out = output / ('pg' + major)
out.mkdir(exist_ok=False)
(out / 'runner.py').write_bytes(Path(__file__).read_bytes())
database = plan['database']
assert re.fullmatch('stateknot_r1_[a-z0-9_]+', database)
container = 'stateknot-r1-oct15-pg' + major


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n')


def source_check(label):
    files = [dict(path=str(p.relative_to(source)), bytes=p.stat().st_size, sha256=sha(p))
             for p in sorted(source.rglob('*')) if p.is_file()]
    assert files == manifest['files'] and len(files) == 919
    write(out / (label + '-source-proof.json'),
          dict(source_tree_sha256=manifest['source_tree_sha256'], file_count=len(files), all_bytes_match=True))


def psql(db, sql):
    return subprocess.run(['docker', 'exec', '-i', container, 'psql', '-X', '-v',
                           'ON_ERROR_STOP=1', '-U', 'postgres', '-d', db, '-At', '-f', '-'],
                          input=sql, text=True, capture_output=True)


source_check('before')
(out / 'before.df').write_bytes(subprocess.check_output(['df', '-k', str(root)]))
info = json.loads(subprocess.check_output(['docker', 'inspect', container], text=True))[0]
assert info['Config']['Labels']['stateknot.task'] == 'r1-oct15' and info['State']['Running']
assert info['NetworkSettings']['Ports']['5432/tcp'] == [{'HostIp': '127.0.0.1', 'HostPort': '554' + major}]
write(out / 'container-identity.json', dict(name=info['Name'], id=info['Id'], image=info['Image'],
      labels=info['Config']['Labels'], ports=info['NetworkSettings']['Ports']))
before = psql('postgres', "SELECT current_setting('server_version_num'),current_setting('server_version'),current_database(),current_user; SELECT count(*) FROM pg_database WHERE datname='" + database + "';")
(out / 'server-before.log').write_text(before.stdout + before.stderr)
assert before.returncode == 0
lines = before.stdout.splitlines()
version = int(lines[0].split('|')[0])
assert int(major) * 10000 <= version < (int(major) + 1) * 10000 and lines[1] == '0'
created = psql('postgres', 'CREATE DATABASE ' + database + ' TEMPLATE template0;')
(out / 'create-own-database.log').write_text(created.stdout + created.stderr)
assert created.returncode == 0
initial = psql(database, "SELECT current_database(),current_user,current_setting('server_version'); SELECT count(*) FROM pg_tables WHERE schemaname='stateknot';")
(out / 'database-initial-identity.log').write_text(initial.stdout + initial.stderr)
assert initial.returncode == 0 and initial.stdout.splitlines()[-1] == '0'
assert initial.stdout.splitlines()[0].startswith(database + '|postgres|')

env = os.environ.copy()
removed = []
for key in list(env):
    if key.startswith('STATEKNOT_TEST_') or key in [
            'STATEKNOT_REQUIRE_POSTGRES_TESTS', 'STATEKNOT_PROOF_BASE_PROBE', 'STATEKNOT_SQL37_TRACE_DIR']:
        removed.append(key)
        env.pop(key)
env['STATEKNOT_TEST_DATABASE_URL'] = 'postgres://postgres:stateknot_test_password@127.0.0.1:554' + major + '/' + database
env['STATEKNOT_REQUIRE_POSTGRES_TESTS'] = '1'
write(out / 'environment.json', dict(database=database, postgres_major=major, server_version_num=version,
      default_stack=True, original_resources_and_assertions=True, old_test_environment_keys_removed=removed,
      only_set_test_keys=['STATEKNOT_TEST_DATABASE_URL', 'STATEKNOT_REQUIRE_POSTGRES_TESTS'],
      backup_and_PITR_conditional_not_qualified_by_libtest_count=True))
rows = []
chain_started = time.time()
for package in plan['suite_order']:
    case_out = out / package
    case_out.mkdir(exist_ok=False)
    if package in plan['artifacts']:
        artifact = plan['artifacts'][package]
    else:
        build_path = output / 'fresh-additional-build' / (package + '-receipt.json')
        write(case_out / 'waiting-for-build.json', dict(path=str(build_path), started_unix=time.time()))
        while not build_path.exists():
            failed = output / 'fresh-additional-build' / 'build-failed.json'
            if failed.exists():
                raise RuntimeError('Additional exact-source build failed: ' + failed.read_text())
            time.sleep(1)
        build = json.loads(build_path.read_text())
        assert build['exit_code'] == 0 and build['source_tree_sha256'] == plan['source_tree_sha256']
        assert len(build['artifacts']) == 1
        artifact = build['artifacts'][0]
        write(case_out / 'fresh-build-receipt.json', build)
    binary = Path(artifact['path'])
    assert sha(binary) == artifact['sha256']
    source_check('before-' + package)
    listing = subprocess.check_output([str(binary), '--list', '--format', 'terse'], text=True)
    (case_out / 'test-list.txt').write_text(listing)
    names = [line[:-6] for line in listing.splitlines() if line.endswith(': test')]
    assert names and len(names) == len(set(names)) and len(names) == artifact['exact_test_count']
    command = [str(binary), '--nocapture', '--test-threads=1']
    started = time.time()
    with (case_out / 'native.log').open('w') as log:
        proc = subprocess.Popen(command, cwd=source, env=env, stdout=log, stderr=subprocess.STDOUT)
        write(case_out / 'running.json', dict(pid=proc.pid, started_unix=started, command=command,
              binary_sha256=artifact['sha256'], source_tree_sha256=plan['source_tree_sha256'],
              exact_test_count=len(names), database=database))
        write(out / 'current-suite.json', dict(package=package, pid=proc.pid, started_unix=started,
              completed_suites=len(rows), log_path=str(case_out / 'native.log')))
        proc.wait()
    body = (case_out / 'native.log').read_text(errors='replace')
    summaries = re.findall(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;', body)
    valid = proc.returncode == 0 and summaries == [('ok', str(len(names)), '0', '0')]
    source_check('after-' + package)
    receipt = dict(package=package, source=str(source), source_tree_sha256=plan['source_tree_sha256'],
                   source_files=919, source_unchanged=True, binary=str(binary), binary_sha256=artifact['sha256'],
                   postgres_major=major, database=database, command=command, expected_tests=len(names),
                   exit_code=proc.returncode, seconds=time.time() - started, summaries=summaries,
                   exact_all_tests_passed=valid, default_stack=True, original_resources_and_assertions=True,
                   log_sha256=sha(case_out / 'native.log'),
                   backup_and_PITR_conditional_requires_separate_evidence=True)
    write(case_out / 'receipt.json', receipt)
    rows.append(receipt)
    write(out / 'receipt.json', dict(postgres_major=major, complete=False, rows=rows))
    print(json.dumps(dict(package=package, postgres_major=major, count=len(names),
                          exit_code=proc.returncode, exact_all_tests_passed=valid, seconds=receipt['seconds'])), flush=True)

source_check('after')
final = psql(database, "SELECT current_database(),current_user,current_setting('server_version'); SELECT count(*),min(version),max(version),bool_and(success) FROM public._sqlx_migrations; SELECT count(*) FROM pg_tables WHERE schemaname='stateknot';")
(out / 'database-final-identity.log').write_text(final.stdout + final.stderr)
(out / 'after.df').write_bytes(subprocess.check_output(['df', '-k', str(root)]))
write(out / 'receipt.json', dict(postgres_major=major, complete=True, rows=rows, seconds=time.time() - chain_started,
      all_four_native_suites_passed=all(row['exact_all_tests_passed'] for row in rows),
      source_tree_sha256=plan['source_tree_sha256'], source_files=919, source_unchanged=True,
      default_stack=True, database=database, final_catalog_query_exit_code=final.returncode,
      backup_and_PITR_conditional_requires_separate_evidence=True))
sys.exit(0 if all(row['exact_all_tests_passed'] for row in rows) else 1)
