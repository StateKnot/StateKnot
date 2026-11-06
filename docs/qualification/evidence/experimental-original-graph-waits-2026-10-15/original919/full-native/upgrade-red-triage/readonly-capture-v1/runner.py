from pathlib import Path
import hashlib
import json
import re
import subprocess

root = Path('/Users/jiawy/Documents/RustAgentFramework')
base = root / '.local-verification/2026-10-15'
out = base / 'elapsed36-attempt-proof-integration/full-native-v1/upgrade-red-triage/readonly-capture-v1'
out.mkdir(parents=True, exist_ok=False)
(out / 'runner.py').write_bytes(Path(__file__).read_bytes())
for major in ['16', '17']:
    container = 'stateknot-r1-oct15-pg' + major
    info = json.loads(subprocess.check_output(['docker', 'inspect', container], text=True))[0]
    assert info['Config']['Labels']['stateknot.task'] == 'r1-oct15'
    command = ['docker', 'exec', '-i', container, 'psql', '-X', '-v', 'ON_ERROR_STOP=1',
               '-U', 'postgres', '-d', 'postgres', '-At', '-f', '-']
    result = subprocess.run(command, input="SELECT datname FROM pg_database WHERE datname LIKE 'stateknot_frame_close_upgrade_%' ORDER BY datname;", text=True, capture_output=True, check=True)
    names = result.stdout.splitlines()
    assert names
    (out / ('pg' + major + '-fixture-databases.txt')).write_text(result.stdout)
    database = names[-1]
    assert re.fullmatch('stateknot_frame_close_upgrade_[a-z0-9]+', database)
    command[command.index('postgres', command.index('-d'))] = database
    catalog = subprocess.run(command, input="SELECT table_name FROM information_schema.columns WHERE table_schema='stateknot' AND column_name='tenant_id' ORDER BY table_name;", text=True, capture_output=True, check=True)
    tables = catalog.stdout.splitlines()
    assert all(re.fullmatch('[a-z_]+', table) for table in tables)
    sql = "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;\nSELECT jsonb_build_object('database',current_database(),'server',current_setting('server_version'),'readonly',current_setting('transaction_read_only'),'ledger',(SELECT jsonb_agg(to_jsonb(m) ORDER BY version) FROM public._sqlx_migrations m));\n"
    for table in tables:
        sql += "SELECT jsonb_build_object('table','" + table + "','rows',coalesce(jsonb_agg(to_jsonb(t)),'[]'::jsonb)) FROM stateknot." + table + " t WHERE tenant_id LIKE 'frame-close-upgrade-%';\n"
    sql += "COMMIT;\n"
    (out / ('pg' + major + '-readonly.sql')).write_text(sql)
    result = subprocess.run(command, input=sql, text=True, capture_output=True)
    (out / ('pg' + major + '-raw.jsonl')).write_text(result.stdout)
    (out / ('pg' + major + '-stderr.log')).write_text(result.stderr)
    assert result.returncode == 0
    records = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
    identity = records[0]
    assert identity['database'] == database and identity['readonly'] == 'on'
    data = {item['table']: item['rows'] for item in records[1:]}
    artifact = out / ('pg' + major + '-full-fixture.json')
    artifact.write_text(json.dumps(dict(identity=identity, container_id=info['Id'], tables=data), indent=2) + '\n')
    barriers = [dict(tenant_id=row['tenant_id'], run_id=row['run_id'], journal_sequence=row['journal_sequence'], wire=json.loads(bytes.fromhex(row['barrier_bytes'][2:]))) for row in data['graph_frame_barriers']]
    summary = dict(postgres_major=major, database=database, ledger_max=max(x['version'] for x in identity['ledger']),
                   conditions=[{key: row.get(key) for key in ['profile', 'wait_kind', 'due_at', 'journal_recorded_at', 'tenant_id', 'run_id']} for row in data['graph_wait_conditions']],
                   scoped_barrier_versions=[dict(tenant_id=x['tenant_id'], run_id=x['run_id'], sequence=x['journal_sequence'], version=x['wire']['version']) for x in barriers],
                   first_revert_guard_nonlegacy_condition_present=any(row['profile'] != 'legacy_registration' for row in data['graph_wait_conditions']),
                   first_revert_guard_scoped_v3_present=any(x['wire']['version'] == 3 for x in barriers),
                   actual_firings=len(data['timer_firings']), full_fixture_sha256=hashlib.sha256(artifact.read_bytes()).hexdigest())
    (out / ('pg' + major + '-summary.json')).write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary), flush=True)
