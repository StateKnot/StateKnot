from pathlib import Path
import hashlib, json, os, re, subprocess, sys, time

base = Path(__file__).resolve().parent
build_label, label, major, database, expected = sys.argv[1:]
assert major in ('16', '17') and re.fullmatch(r'stateknot_r1_[a-z0-9_]+', database)
build = json.loads((base / 'evidence' / build_label / 'receipt.json').read_text())
assert build['exit_code'] == 0 and len(build['artifacts']) == 1
artifact = build['artifacts'][0]
binary = Path(artifact['path'])
assert hashlib.sha256(binary.read_bytes()).hexdigest() == artifact['sha256']
out = base / 'evidence' / label
out.mkdir(exist_ok=False)
(out / 'runner.py').write_bytes(Path(__file__).read_bytes())
container = 'stateknot-r1-oct15-pg' + major
context = json.loads(subprocess.check_output(['docker', 'inspect', container]))[0]
assert context['Config']['Labels']['stateknot.task'] == 'r1-oct15'
assert context['NetworkSettings']['Ports']['5432/tcp'] == [{'HostIp':'127.0.0.1','HostPort':'554'+major}]
environment = {'container_id': context['Id'], 'image': context['Image'], 'task_label':'r1-oct15', 'database': database, 'postgres_major': major, 'host':os.uname().sysname}
sql = "SELECT json_build_object('server_version',current_setting('server_version'),'database',current_database());"
environment['server'] = json.loads(subprocess.check_output(['docker','exec',container,'psql','-X','-U','postgres','-d',database,'-At','-c',sql],text=True))
assert environment['server']['server_version'].startswith(major+'.')
(out / 'environment.json').write_text(json.dumps(environment,indent=2)+'\n')
(out / 'before.df').write_bytes(subprocess.check_output(['df','-k',str(base)]))
env = os.environ.copy()
assert 'RUST_MIN_STACK' not in env
env.update(STATEKNOT_REQUIRE_POSTGRES_TESTS='1', STATEKNOT_TEST_DATABASE_URL='postgres://postgres:stateknot_test_password@127.0.0.1:554'+major+'/'+database)
command = [str(binary), '--nocapture', '--test-threads=1']
listing = subprocess.check_output([str(binary),'--list'],text=True)
(out / 'test-list.txt').write_text(listing)
assert len([x for x in listing.splitlines() if x.endswith(': test')]) == int(expected)
start = time.time()
with (out / 'native.log').open('w') as log:
    process = subprocess.Popen(command,env=env,stdout=log,stderr=subprocess.STDOUT)
    (out / 'running.json').write_text(json.dumps({'pid':process.pid,'started_unix':start,'command':command,'binary_sha256':artifact['sha256']},indent=2)+'\n')
    process.wait()
log = (out / 'native.log').read_bytes()
summaries = re.findall(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;', log.decode(errors='replace'))
ok = summaries == [('ok',expected,'0','0')] and process.returncode == 0
receipt = {'source':build['source'],'binary_sha256':artifact['sha256'],'command':command,'postgres_major':major,'database':database,'exit_code':process.returncode,'expected_tests':int(expected),'summary':summaries,'all_expected_tests_passed':ok,'seconds':time.time()-start,'default_stack':True,'log_sha256':hashlib.sha256(log).hexdigest(),'environment':environment}
(out / 'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(receipt,indent=2),flush=True)
sys.exit(0 if ok else 1)
