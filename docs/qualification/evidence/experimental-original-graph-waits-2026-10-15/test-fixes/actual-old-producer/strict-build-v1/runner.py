from pathlib import Path
import os,json,subprocess,sys,time,hashlib,shutil,fcntl
root=Path('/Users/jiawy/Documents/RustAgentFramework').resolve();base=root/'.local-verification/2026-10-15';evidence=base/'elapsed36-attempt-proof-integration';source=base/'source32-old35-producer-source';target=root/'target/r1-oct15-elapsed36-lint';out=evidence/'source32-old35-producer/strict-build-v1';out.mkdir(exist_ok=False)
(out/'runner.py').write_bytes(Path(__file__).read_bytes()); expected=json.loads((evidence/'source32-old35-producer/source-manifest.json').read_text());assert expected['file_count']==899

def check_source():
 rows=[{'path':str(p.relative_to(source)),'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}for p in sorted(source.rglob('*'))if p.is_file()]
 assert rows==expected['files'],'Frozen composed source changed'
check_source();assert target.resolve().parent==root/'target' and target.resolve().name=='r1-oct15-elapsed36-lint'
assert (target/'CACHEDIR.TAG').read_text().startswith('Signature: 8a477f597d28d172789f06886806bc55')
tracked=subprocess.check_output(['git','ls-files','--',str(target.relative_to(root))],cwd=root);assert not tracked.strip()
opened=subprocess.run(['lsof','+D',str(target)],capture_output=True,text=True);(out/'cache-open-files.txt').write_text(opened.stdout+opened.stderr);assert opened.returncode==1 and not opened.stdout.strip() and not opened.stderr.strip(),'Cannot confirm cache inactive; skip clean'
lock_path=target/'debug/.cargo-lock';lock_status={'path':str(lock_path),'exists':lock_path.exists(),'open_files_test':'lsof +D entire target returned 1 with empty stdout/stderr'}
(out/'cache-lock-status.json').write_text(json.dumps(lock_status,indent=2)+'\n')
(out/'cache-before.df').write_bytes(subprocess.check_output(['df','-k',str(root)]));(out/'cache-size-before.txt').write_bytes(subprocess.check_output(['du','-sk',str(target)]))
env=os.environ.copy();env.update(CARGO_TARGET_DIR=str(target),CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',RUSTFLAGS='-D warnings',RUSTDOCFLAGS='-D warnings')
for key in list(env):
 if key.startswith('STATEKNOT_TEST_') or key in ['STATEKNOT_REQUIRE_POSTGRES_TESTS','STATEKNOT_PROOF_BASE_PROBE','STATEKNOT_SQL37_TRACE_DIR']: env.pop(key,None)
assert 'RUST_MIN_STACK' not in env
for tool in ['cargo','rustc']:(out/(tool+'-version.txt')).write_bytes(subprocess.check_output([tool,'--version'],env=env))
packages=['stateknot-store-postgres']
clean=['cargo','clean','--manifest-path',str(source/'Cargo.toml'),'--target-dir',str(target)]
for package in packages:clean+=['-p',package]
with (out/'workspace-clean.log').open('w') as f:r=subprocess.run(clean,cwd=source,env=env,stdout=f,stderr=subprocess.STDOUT)
assert r.returncode==0,'Explicit workspace-only clean failed'
(out/'cache-after-clean.df').write_bytes(subprocess.check_output(['df','-k',str(root)]))
command=['cargo','test','--manifest-path',str(source/'Cargo.toml'),'-p','stateknot-store-postgres','--test','postgres','--all-features','--locked','--no-run','--message-format=json'];start=time.time()
with (out/'build.stdout.jsonl').open('w') as stdout,(out/'build.stderr.log').open('w') as stderr:
 proc=subprocess.Popen(command,cwd=source,env=env,stdout=stdout,stderr=stderr)
 (out/'running.json').write_text(json.dumps({'pid':proc.pid,'started_at_unix':start,'source_tree_sha256':expected['source_tree_sha256'],'command':command},indent=2)+'\n');status=proc.wait()
artifacts=[]
if status==0:
 for line in (out/'build.stdout.jsonl').read_text().splitlines():
  try:event=json.loads(line)
  except json.JSONDecodeError:continue
  if event.get('reason')=='compiler-artifact' and event.get('executable') and event.get('target',{}).get('name')=='postgres':
   assert event.get('fresh') is False,'Requested package retained a fresh artifact after explicit workspace clean'
   identity=event['package_id'];package='stateknot-store-postgres' if '/stateknot-store-postgres#' in identity else 'stateknot-runtime' if '/stateknot-runtime#' in identity else None;assert package,identity
   saved=out/(package+'-postgres');shutil.copy2(event['executable'],saved);artifacts.append({'package':package,'package_id':identity,'path':str(saved),'sha256':hashlib.sha256(saved.read_bytes()).hexdigest(),'fresh':False})
 assert {x['package'] for x in artifacts}=={'stateknot-store-postgres'}
check_source();(out/'cache-after.df').write_bytes(subprocess.check_output(['df','-k',str(root)]));(out/'cache-size-after.txt').write_bytes(subprocess.check_output(['du','-sk',str(target)]))
receipt={'source':str(source),'source_tree_sha256':expected['source_tree_sha256'],'source_files':899,'command':command,'exit_code':status,'seconds':time.time()-start,'target':str(target),'artifacts':artifacts,'workspace_only_clean':clean,'cache_inactive_before_clean':True,'strict_environment':{k:env[k]for k in ['CARGO_INCREMENTAL','CARGO_PROFILE_DEV_DEBUG','CARGO_PROFILE_TEST_DEBUG','RUSTFLAGS','RUSTDOCFLAGS']},'database_tests_executed':False,'qualification':'Strict Store Native binary built only. No Native execution qualification.'};(out/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt,indent=2));sys.exit(status)
