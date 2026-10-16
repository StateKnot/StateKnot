from pathlib import Path
import hashlib,json,os,subprocess,sys,time,fcntl
R=Path('/Users/jiawy/Documents/RustAgentFramework'); B=R/'.local-verification/2026-10-16'; S=B/'mcp-http-debug-source-v2'; O=Path(__file__).resolve().parent
GATES={
 'fmt':['cargo','+1.88.0','fmt','--all','--check'],
 'clippy':['cargo','+1.88.0','clippy','--workspace','--all-targets','--all-features','--locked','--','-D','warnings'],
 'workspace':['cargo','+1.88.0','test','--workspace','--all-targets','--all-features','--locked'],
 'doctests':['cargo','+1.88.0','test','--workspace','--doc','--all-features','--locked'],
 'rustdoc':['cargo','+1.88.0','doc','--workspace','--all-features','--no-deps','--locked'],
 'nightly':['cargo','+nightly-2026-10-07','check','--workspace','--all-targets','--all-features','--locked'],
}
name=sys.argv[1]; assert name in GATES; out=O/name; assert not out.with_suffix('.receipt.json').exists()
lock=(O/'gate.lock').open('a+'); fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
manifest=json.loads((O/'frozen-source-manifest.json').read_text())
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def snapshot():
 rows=[]
 for p in sorted(S.rglob('*')):
  assert not p.is_symlink()
  if p.is_file(): rows.append({'path':str(p.relative_to(S)),'bytes':p.stat().st_size,'sha256':sha(p)})
 assert rows==manifest['files'],'frozen source drift'
 return rows
def disk(label):
 raw=subprocess.check_output(['df','-k',str(R)],text=True); (O/(name+'-'+label+'.df')).write_text(raw); assert int(raw.splitlines()[-1].split()[3])>=50*1024**2
 sizes=[]
 for p in sorted((R/'target').glob('r1-oct15*')):
  if p.is_dir(): sizes.append({'path':str(p.resolve()),'logical_kib':int(subprocess.check_output(['du','-sk',str(p)],text=True).split()[0])})
 total=sum(x['logical_kib'] for x in sizes); (O/(name+'-'+label+'-cache-budget.json')).write_text(json.dumps({'total_logical_kib':total,'inventory':sizes,'no_cleanup':True,'logical_sizes_not_APFS_reclaimable_space':True},indent=2)+'\n'); assert total<=20*1024**2
snapshot(); disk('before')
env={k:v for k,v in os.environ.items() if not k.startswith('STATEKNOT_') and not k.startswith('CARGO_PROFILE_') and k not in ['CARGO_ENCODED_RUSTFLAGS','RUST_MIN_STACK','RUSTC_BOOTSTRAP']}
env.update(CARGO_TARGET_DIR=str(R/('target/r1-oct15-proof-base'+('-nightly' if name=='nightly' else ''))),CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',RUSTFLAGS='-D warnings',RUSTDOCFLAGS='-D warnings')
command=GATES[name]; start=time.time()
with out.with_suffix('.log').open('wb') as log:
 p=subprocess.Popen(command,cwd=S,env=env,stdout=log,stderr=subprocess.STDOUT)
 running={'gate':name,'command':command,'runner_pid':os.getpid(),'pid':p.pid,'start_unix':start,'source_tree':manifest['source_tree_sha256'],'profile':'original opt0; debug/overflow assertions unchanged; no stack override'}; (O/'running.json').write_text(json.dumps(running,indent=2)+'\n'); print(json.dumps(running),flush=True); code=p.wait()
exact=False
try: snapshot(); exact=True
finally:
 receipt={**running,'exit_code':code,'seconds':time.time()-start,'log_sha256':sha(out.with_suffix('.log')),'source_before_after_exact':exact,'native_database_execution':False}; out.with_suffix('.receipt.json').write_text(json.dumps(receipt,indent=2)+'\n'); print(json.dumps(receipt),flush=True)
disk('after'); assert exact; sys.exit(code)
