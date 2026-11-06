from pathlib import Path
import hashlib
import json
import os
import re
import subprocess
import time
import sys

major = sys.argv[1]
assert major in ["16", "17"]

root = Path('/Users/jiawy/Documents/RustAgentFramework')
base = root / '.local-verification/2026-10-15'
integration = base / 'elapsed36-attempt-proof-integration'
source = base / 'source32-old35-producer-source'
out = integration / ('source32-old35-producer/native-pg' + major + '-v1')
out.mkdir(parents=True, exist_ok=False)
(out / 'runner.py').write_bytes(Path(__file__).read_bytes())
manifest = json.loads((integration / 'source32-old35-producer/source-manifest.json').read_text())
artifact = next(a for a in json.loads((integration / 'source32-old35-producer/strict-build-v1/receipt.json').read_text())['artifacts']
                if a['package'] == 'stateknot-store-postgres')
binary = Path(artifact['path'])
assert hashlib.sha256(binary.read_bytes()).hexdigest() == artifact['sha256']
exact = 'frame_closures::actual_schema35_writer_exports_original_wait_return_source32_fixture'
listing = subprocess.check_output([str(binary), '--list', '--format', 'terse'], text=True)
(out / 'test-list.txt').write_text(listing)
assert listing.splitlines().count(exact + ': test') == 1
assert 'RUST_MIN_STACK' not in os.environ
container = 'stateknot-r1-oct15-pg' + major
database = 'stateknot_r1_old35_source32_export_oct15v1'
info = json.loads(subprocess.check_output(['docker', 'inspect', container], text=True))[0]
assert info['Config']['Labels']['stateknot.task'] == 'r1-oct15'


def psql(db, sql):
    return subprocess.run(['docker', 'exec', '-i', container, 'psql', '-X', '-v', 'ON_ERROR_STOP=1',
                           '-U', 'postgres', '-d', db, '-At', '-f', '-'], input=sql, text=True, capture_output=True)


initial = psql('postgres', "SELECT current_setting('server_version_num'); SELECT count(*) FROM pg_database WHERE datname='" + database + "';")
assert initial.returncode == 0 and initial.stdout.splitlines() == [str(int(major) * 10000 + (15 if major == '16' else 11)), '0']
created = psql('postgres', 'CREATE DATABASE ' + database + ' TEMPLATE template0;')
assert created.returncode == 0
(out / 'database-create.log').write_text(initial.stdout + initial.stderr + created.stdout + created.stderr)
env = os.environ.copy()
for key in list(env):
    if key.startswith('STATEKNOT_TEST_') or key in ['STATEKNOT_REQUIRE_POSTGRES_TESTS', 'STATEKNOT_PROOF_BASE_PROBE', 'STATEKNOT_SQL37_TRACE_DIR']:
        env.pop(key)
env['STATEKNOT_REQUIRE_POSTGRES_TESTS'] = '1'
env['STATEKNOT_TEST_SOURCE32_EXPORT_DIR'] = str(out / 'export')
env['STATEKNOT_TEST_DATABASE_URL'] = 'postgres://postgres:stateknot_test_password@127.0.0.1:554' + major + '/' + database
command = [str(binary), exact, '--exact', '--nocapture', '--test-threads=1']
started = time.time()
with (out / 'native.log').open('w') as log:
    process = subprocess.Popen(command, cwd=source, env=env, stdout=log, stderr=subprocess.STDOUT)
    (out / 'running.json').write_text(json.dumps(dict(pid=process.pid, command=command, database=database, started_unix=started), indent=2) + '\n')
    process.wait()
files = [dict(path=str(p.relative_to(source)), bytes=p.stat().st_size, sha256=hashlib.sha256(p.read_bytes()).hexdigest())
         for p in sorted(source.rglob('*')) if p.is_file()]
assert files == manifest['files']
body = (out / 'native.log').read_text()
summary = re.findall(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;', body)
result = dict(source=str(source), source_files=899, source_tree_sha256=manifest['source_tree_sha256'],
              source_unchanged=True, binary=str(binary), binary_sha256=artifact['sha256'], exact_test=exact,
              command=command, database=database, postgres_major=int(major), server_version_num=int(major) * 10000 + (15 if major == "16" else 11),
              container_id=info['Id'], default_stack=True, original_config_fixture_and_assertions=True,
              exit_code=process.returncode, seconds=time.time() - started, summary=summary,
              exact_candidate_success=process.returncode == 0 and summary == [('ok', '1', '0', '0')],
              concurrent_host_full_native_chains=True, log_sha256=hashlib.sha256((out / 'native.log').read_bytes()).hexdigest())
(out / 'receipt.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2), flush=True)

assert result['exact_candidate_success']
metadata = json.loads((out / 'export/metadata.json').read_text())
fixture_database = metadata['database']
assert re.fullmatch('stateknot_frame_close_upgrade_[a-z0-9]+', fixture_database)
check = psql(fixture_database, "SELECT current_database(),current_setting('server_version'); SELECT count(*),min(version),max(version),bool_and(success) FROM public._sqlx_migrations; SELECT count(*) FROM pg_tables WHERE schemaname='stateknot'; SELECT convert_from(barrier_bytes,'UTF8')::jsonb->>'version',count(*) FROM stateknot.graph_frame_barriers GROUP BY 1 ORDER BY 1;")
(out / 'export/actual-source32-identity.log').write_text(check.stdout + check.stderr)
assert check.returncode == 0 and check.stdout.splitlines()[1] == '32|1|32|t'
dump_command = ['docker','exec',container,'pg_dump','-U','postgres','--dbname',fixture_database,'--data-only','--inserts','--column-inserts','--no-owner','--no-privileges','--disable-triggers']
dump = subprocess.run(dump_command, capture_output=True)
(out / 'export/data.original.sql').write_bytes(dump.stdout)
(out / 'export/pg-dump.stderr.log').write_bytes(dump.stderr)
assert dump.returncode == 0
(out / 'export/dump-receipt.json').write_text(json.dumps(dict(command=dump_command,exit_code=dump.returncode,fixture_database=fixture_database,producer_binary_sha256=artifact['sha256'],source_tree_sha256=manifest['source_tree_sha256'],source_files=899,actual_producer_schema=35,reconstructed_source_schema=32,no_canonical_row_transformation=True,data_sha256=hashlib.sha256(dump.stdout).hexdigest(),qualification='Actual schema35 writer plus source32 reconstruction, not a schema32 or released N-1/N-2 historical binary.'),indent=2)+'\n')
print(json.dumps(dict(export_database=fixture_database,data_sql_bytes=len(dump.stdout),data_sha256=hashlib.sha256(dump.stdout).hexdigest())),flush=True)
