from pathlib import Path
import fcntl,hashlib,json,os,re,subprocess,time
REPO=Path(__file__).resolve().parents[3]
OUT=Path(__file__).resolve().parent
TARGET=REPO/'target/r1-oct15-proof-base'
COMMANDS=[('fmt',['cargo','fmt','--all','--check']),('clippy',['cargo','clippy','--workspace','--all-targets','--all-features','--locked','--','-D','warnings']),('workspace',['cargo','test','--workspace','--all-targets','--all-features','--locked']),('doctests',['cargo','test','--workspace','--doc','--all-features','--locked']),('rustdoc',['cargo','doc','--workspace','--all-features','--no-deps','--locked']),('nightly',['cargo','+nightly-2026-10-07','check','--workspace','--all-targets','--all-features','--locked'])]
def sha(b):return hashlib.sha256(b).hexdigest()
def put(n,obj):(OUT/n).write_text(json.dumps(obj,indent=2)+'\n')
def files():
 tracked=subprocess.check_output(['git','ls-files','-z'],cwd=REPO).split(b'\0');extra=subprocess.check_output(['git','ls-files','--others','--exclude-standard','-z'],cwd=REPO).split(b'\0')
 return [{'path':p,'bytes':len(data),'sha256':sha(data)} for p in sorted({v.decode() for v in tracked+extra if v}) if (data:=(REPO/p).read_bytes()) is not None]
def disk(label):
 data=subprocess.check_output(['df','-k',str(REPO)]);(OUT/(label+'.df')).write_bytes(data);free=int(data.decode().splitlines()[-1].split()[3])*1024;assert free>=50*1024**3;return free
def cachecheck(cache):
 assert cache.resolve().parent==(REPO/'target').resolve() and not cache.is_symlink()
 assert 'Signature: 8a477f597d28d172789f06886806bc55' in (cache/'CACHEDIR.TAG').read_text()
 assert not subprocess.check_output(['git','ls-files','--',str(cache.relative_to(REPO))],cwd=REPO).strip()
 p=subprocess.run(['lsof','+D',str(cache)],capture_output=True);(OUT/(cache.name+'-lsof.log')).write_bytes(p.stdout+p.stderr);assert p.returncode==1 and not p.stdout and not p.stderr
 for lock in cache.rglob('.cargo-lock'):
  with lock.open('r+') as h:fcntl.flock(h,fcntl.LOCK_EX|fcntl.LOCK_NB);fcntl.flock(h,fcntl.LOCK_UN)
 (OUT/(cache.name+'-du.txt')).write_bytes(subprocess.check_output(['du','-sk',str(cache)]))
assert not (OUT/'summary.json').exists()
original=json.loads((OUT/'expected-compiled-inputs.json').read_text()); assert len(original)==531
before=files();put('all-inputs-before.json',before)
actual=[r for r in before if r['path'].startswith('crates/') or r['path'] in ['Cargo.toml','Cargo.lock','rust-toolchain.toml'] or r['path'].startswith('.cargo/')]
assert {r['path']:(r['bytes'],r['sha256']) for r in actual}=={r['path']:(r['bytes'],r['sha256']) for r in original},'compiled input drift before start'
put('git-context.json',{'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip(),'status':subprocess.check_output(['git','status','--porcelain'],cwd=REPO,text=True),'original_opt_level':0,'whole_main_input_files':len(before),'compiled_inputs':len(original)})
disk('before');cachecheck(TARGET);cachecheck(Path(str(TARGET)+'-nightly'))
assert not any(k in os.environ for k in ['RUST_MIN_STACK','CARGO_ENCODED_RUSTFLAGS','CARGO_ENCODED_RUSTDOCFLAGS'])
assert not any(k.startswith('CARGO_PROFILE_') and k not in ['CARGO_PROFILE_DEV_DEBUG','CARGO_PROFILE_TEST_DEBUG'] for k in os.environ)
env={k:v for k,v in os.environ.items() if not k.startswith('STATEKNOT_')}
env.update(CARGO_TARGET_DIR=str(TARGET),CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',RUSTFLAGS='-D warnings',RUSTDOCFLAGS='-D warnings')
put('environment.json',{k:v for k,v in env.items() if k.startswith(('CARGO_','RUST'))}); receipts=[]; overall=time.time()
for name,cmd in COMMANDS:
 assert files()==before,'source drift';disk(name+'-before');active=env.copy()
 if name=='nightly':active['CARGO_TARGET_DIR']=str(TARGET)+'-nightly'
 start=time.time()
 with (OUT/(name+'.log')).open('wb') as log:
  p=subprocess.Popen(cmd,cwd=REPO,env=active,stdout=log,stderr=subprocess.STDOUT)
  running={'runner_pid':os.getpid(),'pid':p.pid,'gate':name,'command':cmd,'started_unix':start,'target':active['CARGO_TARGET_DIR']};put('running.json',running);print(json.dumps(running),flush=True);rc=p.wait()
 raw=(OUT/(name+'.log')).read_bytes();row={**running,'exit_code':rc,'seconds':time.time()-start,'log_sha256':sha(raw),'summaries':re.findall(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;',raw.decode(errors='replace'))};put(name+'.receipt.json',row);receipts.append(row);put('receipts.json',receipts);print(json.dumps({k:v for k,v in row.items() if k!='summaries'}),flush=True)
 assert files()==before,'source drift';assert rc==0,'gate failure: '+name
put('all-inputs-after.json',files());disk('after')
workspace=next(r for r in receipts if r['gate']=='workspace'); docs=next(r for r in receipts if r['gate']=='doctests'); counts=[sum(int(s[i]) for s in workspace['summaries']) for i in [1,2,3]]
put('summary.json',{'state':'passed','gates':6,'original_opt_level':0,'compiled_input_files':531,'whole_main_input_files':len(before),'source_before_after_exact':True,'workspace_suites':len(workspace['summaries']),'workspace_counts':counts,'doctest_counts':[sum(int(s[i]) for s in docs['summaries']) for i in [1,2,3]],'seconds':time.time()-overall,'native_database_execution':False,'full_CI_qualification':False});print((OUT/'summary.json').read_text(),flush=True)
