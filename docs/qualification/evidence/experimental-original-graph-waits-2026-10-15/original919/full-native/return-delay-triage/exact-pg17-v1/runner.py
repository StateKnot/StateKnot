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
source = base / 'elapsed36-attempt-proof-source'
out = integration / ('full-native-v1/return-delay-triage/exact-pg' + major + '-v1')
out.mkdir(parents=True, exist_ok=False)
(out / 'runner.py').write_bytes(Path(__file__).read_bytes())
manifest = json.loads((integration / 'source-manifest.json').read_text())
artifact = next(a for a in json.loads((integration / 'strict-native-build-v1/receipt.json').read_text())['artifacts']
                if a['package'] == 'stateknot-store-postgres')
binary = Path(artifact['path'])
assert hashlib.sha256(binary.read_bytes()).hexdigest() == artifact['sha256']
exact = 'frame_returns::deferred_return_delay_cannot_commit_after_live_lease_expires'
listing = subprocess.check_output([str(binary), '--list', '--format', 'terse'], text=True)
(out / 'test-list.txt').write_text(listing)
assert listing.splitlines().count(exact + ': test') == 1
assert 'RUST_MIN_STACK' not in os.environ
container = 'stateknot-r1-oct15-pg' + major
database = 'stateknot_r1_a36_return_delay_exact_oct15v1'
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
env['STATEKNOT_TEST_DATABASE_URL'] = 'postgres://postgres:stateknot_test_password@127.0.0.1:554' + major + '/' + database
command = [str(binary), exact, '--exact', '--nocapture', '--test-threads=1']
started = time.time()
with (out / 'native.log').open('w') as log:
    process = subprocess.Popen(command, cwd=source, env=env, stdout=log, stderr=subprocess.STDOUT)
    (out / 'running.json').write_text(json.dumps(dict(pid=process.pid, command=command, database=database, started_unix=started), indent=2) + '\n')
    observations = []
    while process.poll() is None:
        probe_started = time.time()
        query = "SELECT coalesce(json_agg(json_build_object('pid',pid,'state',state,'query',query,'wait_event_type',wait_event_type,'wait_event',wait_event,'query_start',query_start)),'[]'::json) FROM pg_stat_activity WHERE datname='" + database + "' AND pid<>pg_backend_pid() AND backend_type='client backend';"
        observation = psql('postgres', query)
        if observation.returncode == 0:
            observations.append(dict(observed_unix=probe_started, observer_seconds=time.time()-probe_started, backends=json.loads(observation.stdout)))
        time.sleep(0.5)
    process.wait()
    (out / 'bounded-readonly-backend-observations.json').write_text(json.dumps(observations, indent=2) + '\n')
    trigger_observed = any(x.get('wait_event') == 'PgSleep' for item in observations for x in item['backends'])
    (out / 'target-observation.json').write_text(json.dumps(dict(pg_sleep_observed=trigger_observed, original_lease_seconds=2, original_deferred_trigger_sleep_seconds=2.1, no_source_or_test_changes=True, active_host_full_native_chains=True), indent=2) + '\n')
files = [dict(path=str(p.relative_to(source)), bytes=p.stat().st_size, sha256=hashlib.sha256(p.read_bytes()).hexdigest())
         for p in sorted(source.rglob('*')) if p.is_file()]
assert files == manifest['files']
body = (out / 'native.log').read_text()
summary = re.findall(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;', body)
result = dict(source=str(source), source_files=919, source_tree_sha256=manifest['source_tree_sha256'],
              source_unchanged=True, binary=str(binary), binary_sha256=artifact['sha256'], exact_test=exact,
              command=command, database=database, postgres_major=int(major), server_version_num=int(major) * 10000 + (15 if major == "16" else 11),
              container_id=info['Id'], default_stack=True, original_config_fixture_and_assertions=True,
              exit_code=process.returncode, seconds=time.time() - started, summary=summary,
              exact_candidate_success=process.returncode == 0 and summary == [('ok', '1', '0', '0')],
              concurrent_host_full_native_chains=True, log_sha256=hashlib.sha256((out / 'native.log').read_bytes()).hexdigest())
(out / 'receipt.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result, indent=2), flush=True)
