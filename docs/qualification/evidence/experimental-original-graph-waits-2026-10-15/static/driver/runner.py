from pathlib import Path
import os,subprocess,json,hashlib,time,sys,fcntl
root=Path('/Users/jiawy/Documents/RustAgentFramework').resolve();base=root/'.local-verification/2026-10-15';source=base/'elapsed36-attempt-proof-final-test-source';integration=base/'elapsed36-attempt-proof-integration';manifest_path=integration/'final-test-fixes-v1/source-manifest.json';manifest=json.loads(manifest_path.read_text());out=integration/'final-test-fixes-v1/six-static-gates-v1-loopback';out.mkdir(exist_ok=False)
label='elapsed36-final-test-fixes-gates-v1-loopback';target=root/'target/r1-oct15-elapsed36-lint';nightly=Path(str(target)+'-nightly');gate_output=base/'evidence'/label
(out/'runner.py').write_bytes(Path(__file__).read_bytes());(out/'expected-source-manifest.json').write_bytes(manifest_path.read_bytes())
def check_source(label):
 rows=[{'path':str(p.relative_to(source)),'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}for p in sorted(source.rglob('*'))if p.is_file()]
 assert len(rows)==922 and rows==manifest['files'],'Frozen922 source changed'
 sha=hashlib.sha256(json.dumps(rows,sort_keys=True,separators=(',',':')).encode()).hexdigest();assert sha==manifest['source_tree_sha256']
 (out/(label+'-manifest.json')).write_text(json.dumps({'source':str(source),'file_count':922,'source_tree_sha256':sha,'files':rows},indent=2)+'\n')
check_source('before')
for cache in [target,nightly]:
 assert cache.resolve().parent==root/'target' and cache.name.startswith('r1-oct15-elapsed36-lint')
 if cache.exists():
  assert 'Signature: 8a477f597d28d172789f06886806bc55' in (cache/'CACHEDIR.TAG').read_text()
  assert not subprocess.check_output(['git','ls-files','--',str(cache.relative_to(root))],cwd=root).strip()
  opened=subprocess.run(['lsof','+D',str(cache)],capture_output=True,text=True)
  (out/(cache.name+'-open-files.txt')).write_text(opened.stdout+opened.stderr)
  assert opened.returncode==1 and not opened.stdout.strip() and not opened.stderr.strip(),'Cache active or inactivity unproven; skip'
  (out/(cache.name+'-size-before.txt')).write_bytes(subprocess.check_output(['du','-sk',str(cache)]))
  lock_path=cache/'debug/.cargo-lock'
  if lock_path.exists():
   with lock_path.open('a+')as lock:
    fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
    fcntl.flock(lock.fileno(),fcntl.LOCK_UN)
   (out/(cache.name+'-lock-status.json')).write_text(json.dumps({'path':str(lock_path),'nonblocking_exclusive_lock_acquired_and_released':True},indent=2)+'\n')
(out/'before.df').write_bytes(subprocess.check_output(['df','-k',str(root)]));available=int((out/'before.df').read_text().splitlines()[-1].split()[3]);assert available>50*1024*1024
for cache in [target,nightly]:
 p=out/(cache.name+'-size-before.txt')
 if p.exists():assert int(p.read_text().split()[0])<=20*1024*1024,'Task cache exceeds20GiB; assess before continuing'
cmd=['python3',str(base/'run_gates.py'),str(source),label,target.name,'--clean-workspace'];env=os.environ.copy()
for key in list(env):
 if key.startswith('STATEKNOT_TEST_')or key in ['STATEKNOT_REQUIRE_POSTGRES_TESTS','STATEKNOT_PROOF_BASE_PROBE','STATEKNOT_SQL37_TRACE_DIR']:env.pop(key,None)
assert 'RUST_MIN_STACK'not in env
start=time.time()
with(out/'orchestration.log').open('w')as log:
 proc=subprocess.Popen(cmd,cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT)
 (out/'running.json').write_text(json.dumps({'pid':proc.pid,'command':cmd,'started_unix':start,'source_tree_sha256':manifest['source_tree_sha256'],'native_database_tests':False},indent=2)+'\n')
 status=proc.wait()
check_source('after');(out/'after.df').write_bytes(subprocess.check_output(['df','-k',str(root)]));deltas=[]
for name in ['stable','nightly']:
 a=gate_output/('cache-'+name+'-before.df');z=gate_output/('cache-'+name+'-after.df')
 if a.exists()and z.exists():
  before=int(a.read_text().splitlines()[-1].split()[3]);after=int(z.read_text().splitlines()[-1].split()[3]);deltas.append({'cache':name,'df_before_available_KiB':before,'df_after_clean_available_KiB':after,'observed_available_delta_KiB':after-before,'observed_available_delta_GiB':(after-before)/(1024**2)})
for cache in [target,nightly]:
 if cache.exists():(out/(cache.name+'-size-after.txt')).write_bytes(subprocess.check_output(['du','-sk',str(cache)]))
receipt_path=gate_output/'receipts.json';receipts=json.loads(receipt_path.read_text())if receipt_path.exists()else None
value={'source':str(source),'source_files':922,'source_tree_sha256':manifest['source_tree_sha256'],'source_unchanged_before_after':True,'command':cmd,'exit_code':status,'seconds':time.time()-start,'static_gate_receipts':str(receipt_path),'gates':receipts,'cache_clean_df_deltas':deltas,'df_delta_note':'Observed whole filesystem values may include concurrent tasks;Cargo/du deleted sizes are not actual released space.','native_database_tests':False,'loopback_http_execution':'originaltests/permissions; no removals or assertion changes','retained_caches':[str(target),str(nightly)]};(out/'receipt.json').write_text(json.dumps(value,indent=2)+'\n');print(json.dumps({k:v for k,v in value.items()if k!='gates'},indent=2));sys.exit(status)
