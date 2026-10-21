from pathlib import Path
import hashlib
import json
import re
import subprocess

root = Path('/Users/jiawy/Documents/RustAgentFramework')
base = root / '.local-verification/2026-10-15'
out = base / 'elapsed36-attempt-proof-integration/wait-oracle-v2/readonly-candidate-capture-v1'
out.mkdir(parents=True, exist_ok=False)
(out / 'runner.py').write_bytes(Path(__file__).read_bytes())
cases = [('candidate-pg16','16','stateknot_r1_a36_wait_oracle_oct15v1'),('candidate-pg17','17','stateknot_r1_a36_wait_oracle_oct15v1')]
for label, major, database in cases:
    container = 'stateknot-r1-oct15-pg' + major
    info = json.loads(subprocess.check_output(['docker', 'inspect', container], text=True))[0]
    assert info['Config']['Labels']['stateknot.task'] == 'r1-oct15'
    assert re.fullmatch('stateknot_r1_[a-z0-9_]+', database)
    command = ['docker', 'exec', '-i', container, 'psql', '-X', '-v', 'ON_ERROR_STOP=1',
               '-U', 'postgres', '-d', database, '-At', '-f', '-']
    catalog_sql = "SELECT table_name FROM information_schema.columns WHERE table_schema='stateknot' AND column_name='tenant_id' ORDER BY table_name;"
    catalog = subprocess.run(command, input=catalog_sql, text=True, capture_output=True, check=True)
    tables = catalog.stdout.splitlines()
    assert tables and all(re.fullmatch('[a-z_]+', table) for table in tables)
    sql = "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;\n"
    sql += "SELECT json_build_object('database',current_database(),'server',current_setting('server_version'),'readonly',current_setting('transaction_read_only'),'isolation',current_setting('transaction_isolation'));\n"
    for table in tables:
        sql += "SELECT jsonb_build_object('table','" + table + "','rows',coalesce(jsonb_agg(to_jsonb(t)),'[]'::jsonb)) FROM stateknot." + table + " t WHERE tenant_id LIKE 'application-replay-wait-%';\n"
    sql += "COMMIT;\n"
    (out / (label + '-readonly.sql')).write_text(sql)
    result = subprocess.run(command, input=sql, text=True, capture_output=True)
    (out / (label + '-raw.jsonl')).write_text(result.stdout)
    (out / (label + '-stderr.log')).write_text(result.stderr)
    assert result.returncode == 0
    records = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
    assert records[0]['database'] == database and records[0]['readonly'] == 'on'
    data = {item['table']: item['rows'] for item in records[1:]}
    assert len(data['runs']) == 1
    (out / (label + '-full-fixture.json')).write_text(json.dumps(dict(identity=records[0], container_id=info['Id'], tables=data), indent=2) + '\n')
    conditions = data['graph_wait_conditions']
    registration = data['run_wait_registrations']
    summary = dict(label=label, database=database, major=major,
                   run={key: data['runs'][0].get(key) for key in ['tenant_id', 'run_id', 'lifecycle_status', 'unresolved_wait_count', 'journal_sequence', 'next_timer_due_at']},
                   conditions=[{key: row.get(key) for key in ['wait_id', 'profile', 'wait_kind', 'due_at', 'journal_sequence', 'journal_recorded_at', 'source_sequence', 'source_recorded_at']} for row in conditions],
                   registration=[{key: row.get(key) for key in ['wait_id', 'wait_kind', 'due_at', 'registered_at', 'status', 'registration_sequence', 'terminal_sequence', 'terminal_recorded_at']} for row in registration],
                   events=[{key: row.get(key) for key in ['sequence', 'event_kind', 'recorded_at', 'event_id']} for row in data['run_events']],
                   full_fixture_sha256=hashlib.sha256((out / (label + '-full-fixture.json')).read_bytes()).hexdigest())
    (out / (label + '-summary.json')).write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary), flush=True)
