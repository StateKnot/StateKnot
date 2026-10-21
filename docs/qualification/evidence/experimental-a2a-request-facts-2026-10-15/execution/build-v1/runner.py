from pathlib import Path
import argparse, fcntl, hashlib, json, os, re, shutil, subprocess, sys, time
BASE=Path(__file__).resolve().parent; REPO=BASE.parents[3]
SOURCE=REPO/'.local-verification/2026-10-15/elapsed36-a2a-auth-facts-source'
TARGET=REPO/'target/r1-oct15-proof-base'
TREE='250066800739a33390a316c7fbb40181335de67394cd9de6bb67ccb425226433'
EXPECTED_DURABLE={'mcp_write_is_durable_before_dispatch_and_reconciles_without_redispatch','mcp_approved_write_is_authorized_after_durable_start_and_reconciles_once','mcp_approved_denial_commits_without_remote_dispatch_or_repeat_authorization','a2a_send_is_durable_before_dispatch_and_unknown_is_not_redispatched','a2a_authorized_send_reads_the_durable_start_and_recovery_does_not_send_again','a2a_authorized_denial_commits_once_without_business_send_or_policy_repeat'}
parser=argparse.ArgumentParser(); parser.add_argument('stage',choices=['build','network','native']); parser.add_argument('--start-authorized',action='store_true'); parser.add_argument('--label'); parser.add_argument('--gate-label',default='gates-v1-loopback'); parser.add_argument('--build-label',default='build-v1'); parser.add_argument('--network-label',default='network-v1'); args=parser.parse_args()
if not args.start_authorized: print(json.dumps({'state':'plan only','stage':args.stage,'source_tree':TREE})); sys.exit(0)
auth=json.loads((BASE/'root-start-authorization.json').read_text()); assert auth['authorized'] and auth['source_tree_sha256']==TREE
gate=json.loads((BASE/args.gate_label/'summary.json').read_text()); assert gate['state']=='passed' and gate['source_tree']==TREE and gate['gates']==6 and gate['native_postgres'] is False
out=BASE/(args.label or args.stage+'-v1'); out.mkdir(exist_ok=False); (out/'runner.py').write_bytes(Path(__file__).read_bytes())
lock=(BASE/'execution.lock').open('a+'); fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
env=os.environ.copy(); env.update(CARGO_TARGET_DIR=str(TARGET),CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',RUSTFLAGS='-D warnings',RUSTDOCFLAGS='-D warnings')
for k in ['STATEKNOT_TEST_DATABASE_URL','STATEKNOT_REQUIRE_POSTGRES_TESTS']: env.pop(k,None)
assert 'RUST_MIN_STACK' not in env

def put(path,obj): path.write_text(json.dumps(obj,indent=2)+'\n')
def verify():
 m=json.loads((BASE/'source-manifest.json').read_text()); assert m['source_tree_sha256']==TREE
 actual={str(p.relative_to(SOURCE)):(p.stat().st_size,hashlib.sha256(p.read_bytes()).hexdigest()) for p in SOURCE.rglob('*') if p.is_file()}
 assert actual=={v['path']:(v['bytes'],v['sha256']) for v in m['files']}, 'frozen source drift'
def disk(name):
 raw=subprocess.check_output(['df','-k',str(REPO)],text=True); (out/(name+'.df')).write_text(raw); assert int(raw.splitlines()[-1].split()[3])>=50*1024**2

def run(name,cmd,active=None):
 verify(); disk(name+'-before'); started=time.time()
 with (out/(name+'.log')).open('wb') as log:
  proc=subprocess.Popen(cmd,cwd=SOURCE,env=active or env,stdout=log,stderr=subprocess.STDOUT)
  running={'runner_pid':os.getpid(),'pid':proc.pid,'step':name,'command':cmd,'source_tree':TREE,'started_unix':started}; put(out/'running.json',running); print(json.dumps(running),flush=True); rc=proc.wait()
 raw=(out/(name+'.log')).read_bytes(); verify(); row={**running,'exit_code':rc,'seconds':time.time()-started,'log_sha256':hashlib.sha256(raw).hexdigest()}; put(out/(name+'.receipt.json'),row); print(json.dumps(row),flush=True); assert rc==0,name+' failed; receipt retained'; return raw.decode(errors='replace')

def passed_summary(text,expected):
 found=re.findall(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;',text)
 assert len(found)==1 and found[0]==('ok',str(expected),'0','0','0','0'), found
 return found[0]

def load_binaries():
 build=json.loads((BASE/args.build_label/'summary.json').read_text()); assert build['state']=='passed' and build['source_tree']==TREE
 for v in build['binaries'].values(): assert hashlib.sha256(Path(v['saved_executable']).read_bytes()).hexdigest()==v['sha256']
 return build['binaries']

verify(); put(out/'context.json',{'source_tree':TREE,'source':str(SOURCE),'runner_pid':os.getpid(),'stage':args.stage,'gate_label':args.gate_label,'rust_env':{k:env[k] for k in ['CARGO_TARGET_DIR','CARGO_INCREMENTAL','CARGO_PROFILE_DEV_DEBUG','CARGO_PROFILE_TEST_DEBUG','RUSTFLAGS','RUSTDOCFLAGS']}})
if args.stage=='build':
 assert TARGET.parent.resolve()==(REPO/'target').resolve() and not TARGET.is_symlink()
 assert 'Signature: 8a477f597d28d172789f06886806bc55' in (TARGET/'CACHEDIR.TAG').read_text()
 assert not subprocess.check_output(['git','ls-files','--',str(TARGET.relative_to(REPO))],cwd=REPO).strip()
 opened=subprocess.run(['lsof','+D',str(TARGET)],capture_output=True,text=True); (out/'cache-lsof.txt').write_text(opened.stdout+opened.stderr); assert opened.returncode==1 and not opened.stdout.strip() and not opened.stderr.strip()
 for path in TARGET.rglob('.cargo-lock'):
  with path.open('a+') as h: fcntl.flock(h,fcntl.LOCK_EX|fcntl.LOCK_NB); fcntl.flock(h,fcntl.LOCK_UN)
 size=subprocess.check_output(['du','-sk',str(TARGET)],text=True); (out/'cache-du.txt').write_text(size); assert int(size.split()[0])<=20*1024**2
 run('clean-integrations',['cargo','clean','--manifest-path',str(SOURCE/'Cargo.toml'),'--target-dir',str(TARGET),'-p','stateknot-integrations'])
 body=run('fresh-build',['cargo','test','-p','stateknot-integrations','--test','a2a_client_contract','--test','mcp_durable','--all-features','--locked','--no-run','--message-format=json'])
 binaries={}; artifact_dir=out/'artifacts'; artifact_dir.mkdir()
 for line in body.splitlines():
  try: item=json.loads(line)
  except json.JSONDecodeError: continue
  if item.get('reason')!='compiler-artifact' or not item.get('executable') or item['target']['name'] not in ['a2a_client_contract','mcp_durable']: continue
  name=item['target']['name']; exe=Path(item['executable']).resolve(); expected=SOURCE/'crates/stateknot-integrations/tests'/f'{name}.rs'
  assert Path(item['target']['src_path']).resolve()==expected and item['fresh'] is False and item['profile']['test'] is True
  assert exe.is_relative_to(TARGET.resolve()) and name not in binaries
  saved=artifact_dir/name; shutil.copy2(exe,saved); digest=hashlib.sha256(saved.read_bytes()).hexdigest(); assert digest==hashlib.sha256(exe.read_bytes()).hexdigest()
  listing=run(name+'-list',[str(saved),'--list']); names=re.findall(r'^(.+): test$',listing,re.M); assert names and len(names)==len(set(names))
  if name=='mcp_durable': assert set(names)==EXPECTED_DURABLE
  else: assert len([n for n in names if n.startswith('authorization_facts::')])==7
  dep=exe.with_suffix('.d'); dep_saved=None
  if dep.exists(): dep_saved=str(artifact_dir/dep.name); shutil.copy2(dep,dep_saved)
  binaries[name]={'cargo_artifact':item,'original_executable':str(exe),'saved_executable':str(saved),'sha256':digest,'bytes':saved.stat().st_size,'tests':names,'dep_info_raw':dep_saved}
 assert set(binaries)=={'a2a_client_contract','mcp_durable'}
 put(out/'summary.json',{'state':'passed','source_tree':TREE,'binaries':binaries,'finished_unix':time.time()})
elif args.stage=='network':
 binaries=load_binaries(); item=binaries['a2a_client_contract']; body=run('a2a-client-contract',[item['saved_executable'],'--nocapture','--test-threads=1']); result=passed_summary(body,len(item['tests']))
 put(out/'summary.json',{'state':'passed','source_tree':TREE,'binary_sha256':item['sha256'],'summary':result,'finished_unix':time.time()})
else:
 binaries=load_binaries(); network=json.loads((BASE/args.network_label/'summary.json').read_text()); assert network['state']=='passed' and network['source_tree']==TREE
 item=binaries['mcp_durable']; results=[]
 for major,port in [(16,55416),(17,55417)]:
  container='stateknot-r1-oct15-pg'+str(major); db='stateknot_r1_a2a_auth_facts_20261015v1'
  inspected=json.loads(subprocess.check_output(['docker','inspect',container]))[0]; assert inspected['State']['Running']
  cfg=dict(x.split('=',1) for x in inspected['Config']['Env'] if '=' in x); user=cfg.get('POSTGRES_USER','postgres'); password=cfg.get('POSTGRES_PASSWORD')
  assert password, 'expected existing task-container credential'
  meta={'container':container,'container_id':inspected['Id'],'image_id':inspected['Image'],'major':major,'host':'127.0.0.1','port':port,'database':db,'user':user}
  def psql(sql,database='postgres'):
   return subprocess.check_output(['docker','exec',container,'psql','-X','-v','ON_ERROR_STOP=1','-U',user,'-d',database,'-At','-c',sql],text=True).strip()
  assert psql("SELECT count(*) FROM pg_database WHERE datname='"+db+"'")=='0', 'fresh DB name already exists; do not drop'
  meta['server_version']=psql('SHOW server_version'); assert meta['server_version'].split('.')[0]==str(major)
  bindings=inspected['NetworkSettings']['Ports']['5432/tcp']; assert any(int(v['HostPort'])==port for v in bindings)
  put(out/f'pg{major}-database-before-create.json',{**meta,'existing_database_count':0})
  subprocess.run(['docker','exec',container,'createdb','-U',user,db],check=True)
  meta['database_oid']=psql("SELECT oid FROM pg_database WHERE datname='"+db+"'"); put(out/f'pg{major}-database.json',meta)
  from urllib.parse import quote
  active=env.copy(); active['STATEKNOT_REQUIRE_POSTGRES_TESTS']='1'; active['STATEKNOT_TEST_DATABASE_URL']=f'postgresql://{quote(user,safe="")}:{quote(password,safe="")}@127.0.0.1:{port}/{db}'
  body=run('pg'+str(major)+'-mcp-durable',[item['saved_executable'],'--nocapture','--test-threads=1'],active); result=passed_summary(body,6)
  markers={}
  for prefix in ['STATEKNOT_MCP_AUTHORIZATION_EVIDENCE','STATEKNOT_A2A_AUTHORIZATION_EVIDENCE']:
   values=[json.loads(v) for v in re.findall(r'^'+prefix+r'=(\{[^\n]+\})$',body,re.M)]; assert len(values)==2 and {v['case'] for v in values}=={'denial','lost_response'}
   for v in values:
    assert v['calls']==(0 if v['case']=='denial' else 1) and v['policy_calls']==1
    if prefix.startswith('STATEKNOT_A2A'): assert v['durable_start_verified'] is True and v['payload_from_full_input'] is True and v['duplicate_ledger_unchanged'] is True
    else:
     assert v['durable_before_authorization'] is True
     assert v['duplicate_ledger_unchanged' if v['case']=='denial' else 'reconciliation_idempotent'] is True
   markers[prefix]=values
  row={'major':major,'database':meta,'binary_sha256':item['sha256'],'summary':result,'markers':markers}; results.append(row); put(out/f'pg{major}-qualification.json',row)
 put(out/'summary.json',{'state':'passed','source_tree':TREE,'binary_sha256':item['sha256'],'postgres':results,'finished_unix':time.time()})
print(json.dumps({'state':'passed','stage':args.stage,'output':str(out)}),flush=True)
