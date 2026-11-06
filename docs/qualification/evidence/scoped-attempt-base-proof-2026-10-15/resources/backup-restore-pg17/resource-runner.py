from pathlib import Path
import hashlib, json, os, subprocess, sys

base=Path(__file__).resolve().parent
major=sys.argv[1]
assert major in ('16','17')
container='stateknot-r1-oct15-pg'+major
context=json.loads(subprocess.check_output(['docker','inspect',container]))[0]
assert context['Config']['Labels']['stateknot.task']=='r1-oct15'
assert context['NetworkSettings']['Ports']['5432/tcp']==[{'HostIp':'127.0.0.1','HostPort':'554'+major}]
database='stateknot_r1_a_backup_20261015v1'
assert subprocess.check_output(['docker','exec',container,'psql','-X','-U','postgres','-d','postgres','-At','-c',"SELECT count(*) FROM pg_database WHERE datname='"+database+"'"],text=True).strip()=='0'
subprocess.run(['docker','exec',container,'createdb','-U','postgres','--template=template0',database],check=True)
label='proof-base-erased-backup-restore-pg'+major+'-v1'
test='child_admission::ownership::join::backup_restore::consumed_child_join_survives_isolated_backup_restore'
env=os.environ.copy()
env.update(STATEKNOT_REQUIRE_BACKUP_RESTORE_TESTS='1',STATEKNOT_TEST_POSTGRES_CONTAINER=container)
command=[sys.executable,str(base/'run_native_database.py'),'proof-base-erased-runtime-v1',label,major,test,database]
result=subprocess.run(command,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
out=base/'evidence'/label
out.mkdir(exist_ok=True)
(out/'resource-runner.py').write_bytes(Path(__file__).read_bytes())
(out/'resource-runner.stdout.log').write_text(result.stdout)
log=(out/'native.log').read_text()
markers=[json.loads(line.split('=',1)[1]) for line in log.splitlines() if line.startswith('STATEKNOT_CHILD_JOIN_BACKUP_RESTORE_EVIDENCE=')]
receipt={'runner_exit_code':result.returncode,'mandatory_backup_restore':True,'mandatory_postgres':True,'container':container,'container_id':context['Id'],'image':context['Image'],'management_database':database,'marker_count':len(markers),'markers':markers,'native_log_sha256':hashlib.sha256(log.encode()).hexdigest(),'source_commit':'83559f7651f48d65755c1fc5fd8a9ba82e721193','runtime_native_receipt':str(out/'receipt.json')}
(out/'resource-receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(receipt,indent=2))
sys.exit(0 if result.returncode==0 and len(markers)==1 else 1)
