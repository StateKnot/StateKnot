from pathlib import Path
import os,json,subprocess,sys,time,hashlib,shutil,fcntl
root=Path('/Users/jiawy/Documents/RustAgentFramework').resolve();base=root/'.local-verification/2026-10-15';evidence=base/'elapsed36-attempt-proof-integration/final-test-fixes-v1';source=base/'elapsed36-attempt-proof-final-test-source';target=root/'target/r1-oct15-elapsed36-lint';out=evidence/'strict-remaining-build-v1';out.mkdir(exist_ok=False);(out/'runner.py').write_bytes(Path(__file__).read_bytes());m=json.loads((evidence/'source-manifest.json').read_text());assert m['file_count']==922

def check(label):
 rows=[dict(path=str(p.relative_to(source)),bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest())for p in sorted(source.rglob('*'))if p.is_file()];assert rows==m['files'];(out/(label+'-source-proof.json')).write_text(json.dumps(dict(source_tree_sha256=m['source_tree_sha256'],source_files=922,all_bytes_match=True),indent=2)+'\n')
check('before');assert target.resolve().parent==root/'target';assert(target/'CACHEDIR.TAG').read_text().startswith('Signature: 8a477f597d28d172789f06886806bc55');assert not subprocess.check_output(['git','ls-files','--',str(target.relative_to(root))],cwd=root).strip();opened=subprocess.run(['lsof','+D',str(target)],capture_output=True,text=True);(out/'cache-open-files.txt').write_text(opened.stdout+opened.stderr);assert opened.returncode==1 and not opened.stdout.strip()and not opened.stderr.strip()
with(target/'debug/.cargo-lock').open('a+')as lock:fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB);fcntl.flock(lock.fileno(),fcntl.LOCK_UN)
(out/'cache-before.df').write_bytes(subprocess.check_output(['df','-k',str(root)]));assert int((out/'cache-before.df').read_text().splitlines()[-1].split()[3])>50*1024*1024
size=subprocess.check_output(['du','-sk',str(target)]);(out/'cache-size-before.txt').write_bytes(size);assert int(size.split()[0])<20*1024*1024
env=os.environ.copy();env.update(CARGO_TARGET_DIR=str(target),CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',RUSTFLAGS='-D warnings',RUSTDOCFLAGS='-D warnings')
for key in list(env):
 if key.startswith('STATEKNOT_TEST_')or key in ['STATEKNOT_REQUIRE_POSTGRES_TESTS','STATEKNOT_REQUIRE_BACKUP_RESTORE_TESTS','STATEKNOT_REQUIRE_PITR_TESTS','STATEKNOT_PROOF_BASE_PROBE','STATEKNOT_SQL37_TRACE_DIR']:env.pop(key,None)
assert 'RUST_MIN_STACK'not in env
for tool in ['cargo','rustc']:(out/(tool+'-version.txt')).write_bytes(subprocess.check_output([tool,'--version'],env=env))
rows=[];start=time.time()
for package,targetkind,targetname,savedname in [('stateknot-runtime','--test','postgres','stateknot-runtime-postgres'),('stateknot-integrations','--test','provider_native_postgres','stateknot-integrations'),('stateknot','--example','deepseek_agent','stateknot-deepseek_agent')]:
 check('before-'+package);clean=['cargo','clean','--manifest-path',str(source/'Cargo.toml'),'--target-dir',str(target),'-p',package]
 with(out/(package+'-clean.log')).open('w')as f:result=subprocess.run(clean,cwd=source,env=env,stdout=f,stderr=subprocess.STDOUT)
 assert result.returncode==0
 command=['cargo','test','--manifest-path',str(source/'Cargo.toml'),'-p',package,targetkind,targetname,'--all-features','--locked','--no-run','--message-format=json'];t=time.time()
 with(out/(package+'-build.stdout.jsonl')).open('w')as stdout,(out/(package+'-build.stderr.log')).open('w')as stderr:
  proc=subprocess.Popen(command,cwd=source,env=env,stdout=stdout,stderr=stderr);(out/'running.json').write_text(json.dumps(dict(package=package,pid=proc.pid,command=command,started_unix=t),indent=2)+'\n');code=proc.wait()
 artifacts=[];library=[]
 if code==0:
  for line in(out/(package+'-build.stdout.jsonl')).read_text().splitlines():
   try:event=json.loads(line)
   except json.JSONDecodeError:continue
   if event.get('reason')!='compiler-artifact'or f'/{package}#'not in event.get('package_id',''):continue
   if 'lib'in event.get('target',{}).get('kind',[]):library.append(dict(package_id=event['package_id'],target=event['target'],fresh=event['fresh']));assert event['fresh']is False
   if event.get('executable')and event.get('target',{}).get('name')==targetname:
    assert event['fresh']is False;saved=out/savedname;shutil.copy2(event['executable'],saved);artifacts.append(dict(package=package,package_id=event['package_id'],name=targetname,path=str(saved),sha256=hashlib.sha256(saved.read_bytes()).hexdigest(),fresh=False))
  assert len(artifacts)==1
 check('after-'+package);receipt=dict(source=str(source),source_files=922,source_tree_sha256=m['source_tree_sha256'],source_unchanged=True,package=package,command=command,workspace_only_clean=clean,exit_code=code,seconds=time.time()-t,artifacts=artifacts,requested_library_compile_events=library,strict_environment={k:env[k]for k in ['CARGO_INCREMENTAL','CARGO_PROFILE_DEV_DEBUG','CARGO_PROFILE_TEST_DEBUG','RUSTFLAGS','RUSTDOCFLAGS']},database_tests_executed=False);(out/(package+'-receipt.json')).write_text(json.dumps(receipt,indent=2)+'\n');rows.append(receipt);print(json.dumps(dict(package=package,exit_code=code,seconds=receipt['seconds'],saved_sha256=[a['sha256']for a in artifacts])),flush=True)
 if code!=0:sys.exit(code)
check('after');(out/'cache-after.df').write_bytes(subprocess.check_output(['df','-k',str(root)]));(out/'cache-size-after.txt').write_bytes(subprocess.check_output(['du','-sk',str(target)]));(out/'receipt.json').write_text(json.dumps(dict(source=str(source),source_files=922,source_tree_sha256=m['source_tree_sha256'],source_unchanged=True,exit_code=0,seconds=time.time()-start,serial_builds=True,rows=rows,database_tests_executed=False,target=str(target)),indent=2)+'\n')
