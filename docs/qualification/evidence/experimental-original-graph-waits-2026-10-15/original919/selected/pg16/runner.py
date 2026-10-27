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
output = integration / 'selected-native-feedback-v1'
plan = json.loads((output / 'plan.json').read_text())
source = Path(plan['source'])
manifest = json.loads((integration / 'source-manifest.json').read_text())
major = sys.argv[1]
assert major in ['16', '17']
assert plan['case_count'] == 30 and len(plan['per_major'][major]) == 15
out = output / ('pg' + major)
out.mkdir(exist_ok=False)
(out / 'runner.py').write_bytes(Path(__file__).read_bytes())
database = plan['database']
assert re.fullmatch('stateknot_r1_[a-z0-9_]+', database)
container = 'stateknot-r1-oct15-pg' + major
assert 'RUST_MIN_STACK' not in os.environ


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n')


def source_check(label):
    files = [dict(path=str(p.relative_to(source)), bytes=p.stat().st_size, sha256=sha(p))
             for p in sorted(source.rglob('*')) if p.is_file()]
    assert manifest['file_count'] == 919 and files == manifest['files']
    write(out / (label + '-source-manifest.json'),
          dict(tree_sha256=manifest['source_tree_sha256'], files=files))


def psql(db, sql):
    return subprocess.run(['docker', 'exec', '-i', container, 'psql', '-X', '-v',
                           'ON_ERROR_STOP=1', '-U', 'postgres', '-d', db, '-At', '-f', '-'],
                          input=sql, text=True, capture_output=True)


source_check('before')
(out / 'before.df').write_bytes(subprocess.check_output(['df', '-k', str(root)]))
info = json.loads(subprocess.check_output(['docker', 'inspect', container], text=True))[0]
assert info['Config']['Labels']['stateknot.task'] == 'r1-oct15' and info['State']['Running']
identity = dict(container_name=info['Name'], container_id=info['Id'], image=info['Image'],
                labels=info['Config']['Labels'], ports=info['NetworkSettings']['Ports'])
write(out / 'container-identity.json', identity)
check = psql('postgres', "SELECT current_setting('server_version_num'),current_setting('server_version'),current_database(),current_user; SELECT count(*) FROM pg_database WHERE datname='" + database + "';")
(out / 'before-environment.log').write_text(check.stdout + check.stderr)
assert check.returncode == 0
lines = check.stdout.splitlines()
version = int(lines[0].split('|')[0])
assert int(major) * 10000 <= version < (int(major) + 1) * 10000
assert lines[1] == '0', 'Database exists; preserve it and stop this runner.'
created = psql('postgres', 'CREATE DATABASE ' + database + ' TEMPLATE template0;')
(out / 'create-database.log').write_text(created.stdout + created.stderr)
assert created.returncode == 0
initial = psql(database, "SELECT current_database(),current_user,current_setting('server_version'); SELECT count(*) FROM pg_tables WHERE schemaname='stateknot';")
(out / 'database-initial-identity.log').write_text(initial.stdout + initial.stderr)
assert initial.returncode == 0 and initial.stdout.splitlines()[-1] == '0'
assert initial.stdout.splitlines()[0].startswith(database + '|postgres|')

env = os.environ.copy()
removed = []
for key in list(env):
    if key.startswith('STATEKNOT_TEST_') or key in [
            'STATEKNOT_REQUIRE_POSTGRES_TESTS', 'STATEKNOT_PROOF_BASE_PROBE',
            'STATEKNOT_SQL37_TRACE_DIR']:
        removed.append(key)
        env.pop(key)
env['STATEKNOT_TEST_DATABASE_URL'] = 'postgres://postgres:stateknot_test_password@127.0.0.1:554' + major + '/' + database
env['STATEKNOT_REQUIRE_POSTGRES_TESTS'] = '1'
write(out / 'environment.json', dict(postgres_major=major, server_version_num=version,
      database=database, default_stack=True, require_postgres=True,
      old_test_environment_keys_removed=removed, no_source_or_resource_overrides=True,
      original_node_timeout_seconds=30, original_role_concurrency=24,
      original_role_pool_maximum=8, source_tree_sha256=manifest['source_tree_sha256'],
      binaries=plan['artifacts']))

rows = []
series_started = time.time()
for number, case in enumerate(plan['per_major'][major], 1):
    case_out = out / ('case-' + str(number).zfill(2))
    case_out.mkdir(exist_ok=False)
    artifact = plan['artifacts'][case['package']]
    binary = Path(artifact['path'])
    assert sha(binary) == artifact['sha256']
    command = [str(binary), case['exact_test'], '--exact', '--nocapture', '--test-threads=1']
    started = time.time()
    with (case_out / 'native.log').open('w') as log:
        proc = subprocess.Popen(command, env=env, cwd=source, stdout=log, stderr=subprocess.STDOUT)
        write(case_out / 'running.json', dict(pid=proc.pid, started_unix=started,
              exact_test=case['exact_test'], command=command, binary_sha256=artifact['sha256'],
              database=database))
        proc.wait()
    body = (case_out / 'native.log').read_text()
    summaries = re.findall(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;', body)
    valid = proc.returncode == 0 and summaries == [('ok', '1', '0', '0')]
    receipt = dict(case=number, exact_test=case['exact_test'], origin_label=case['origin_label'],
                   postgres_major=major, database=database, package=case['package'],
                   binary_sha256=artifact['sha256'], source_tree_sha256=manifest['source_tree_sha256'],
                   command=command, exit_code=proc.returncode, seconds=time.time() - started,
                   valid_exact_native_success=valid, summaries=summaries,
                   result_lines=[line for line in body.splitlines() if 'test result:' in line
                                 or 'panicked' in line or 'fatal runtime' in line or 'overflow' in line],
                   log_sha256=sha(case_out / 'native.log'), default_stack=True,
                   original_config_and_assertions=True)
    write(case_out / 'receipt.json', receipt)
    rows.append(receipt)
    write(out / 'receipt.json', dict(postgres_major=major, rows=rows, complete=False,
          count=len(rows), source_tree_sha256=manifest['source_tree_sha256']))
    print(json.dumps(dict(postgres_major=major, case=number, exact_test=case['exact_test'],
                          passed=valid, exit_code=proc.returncode, seconds=receipt['seconds'])), flush=True)

source_check('after')
final = psql(database, "SELECT current_database(),current_user,current_setting('server_version'); SELECT count(*),min(version),max(version),bool_and(success) FROM public._sqlx_migrations; SELECT count(*) FROM pg_tables WHERE schemaname='stateknot';")
(out / 'database-final-identity.log').write_text(final.stdout + final.stderr)
assert final.returncode == 0 and final.stdout.splitlines()[-2:] == ['36|1|36|t', '54']
(out / 'after.df').write_bytes(subprocess.check_output(['df', '-k', str(root)]))
write(out / 'receipt.json', dict(postgres_major=major, container_id=info['Id'],
      server_version_num=version, database=database, rows=rows, complete=True,
      count=len(rows), passed=sum(row['valid_exact_native_success'] for row in rows),
      failed=sum(not row['valid_exact_native_success'] for row in rows),
      seconds=time.time() - series_started, source_files=919,
      source_tree_sha256=manifest['source_tree_sha256'], source_unchanged=True,
      final_ledger_exact_1_to_36=True, final_business_table_count=54,
      qualification='Only these exact 15 existing selected Native cases. No full suite, no R1 qualification.'))
sys.exit(0 if all(row['valid_exact_native_success'] for row in rows) else 1)
