from pathlib import Path
import argparse, fcntl, hashlib, json, os, re, shutil, subprocess, sys, time
BASE = Path(__file__).resolve().parent
REPO = BASE.parents[3]
SOURCE = REPO / '.local-verification/2026-10-15/elapsed36-a2a-auth-facts-source'
TARGET = REPO / 'target/r1-oct15-proof-base'
TREE = '250066800739a33390a316c7fbb40181335de67394cd9de6bb67ccb425226433'
PACKAGES = ['stateknot','stateknot-artifact-store','stateknot-core','stateknot-integrations','stateknot-runtime','stateknot-store-postgres','stateknot-testkit']
COMMANDS = [('fmt',['cargo','fmt','--all','--check']),('clippy',['cargo','clippy','--workspace','--all-targets','--all-features','--locked','--','-D','warnings']),('workspace',['cargo','test','--workspace','--all-targets','--all-features','--locked']),('doctests',['cargo','test','--workspace','--doc','--all-features','--locked']),('rustdoc',['cargo','doc','--workspace','--all-features','--no-deps','--locked']),('nightly',['cargo','+nightly-2026-10-07','check','--workspace','--all-targets','--all-features','--locked'])]
parser = argparse.ArgumentParser(); parser.add_argument('--start-authorized',action='store_true'); parser.add_argument('--label',default='gates-v1'); args=parser.parse_args()
if not args.start_authorized:
 print(json.dumps({'state':'plan only; explicit start flag required','source_tree':TREE,'commands':COMMANDS},indent=2)); sys.exit(0)
auth=json.loads((BASE/'root-start-authorization.json').read_text()); assert auth['authorized'] is True and auth['source_tree_sha256']==TREE
out=BASE/args.label; out.mkdir(exist_ok=False)
(out/'runner.py').write_bytes(Path(__file__).read_bytes())
lock=(BASE/'execution.lock').open('a+'); fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
env=os.environ.copy(); env.update(CARGO_TARGET_DIR=str(TARGET),CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',RUSTFLAGS='-D warnings',RUSTDOCFLAGS='-D warnings')
for key in ['STATEKNOT_TEST_DATABASE_URL','STATEKNOT_REQUIRE_POSTGRES_TESTS']: env.pop(key,None)
assert 'RUST_MIN_STACK' not in env

def put(path,obj): path.write_text(json.dumps(obj,indent=2)+'\n')
def verify_source():
 manifest=json.loads((BASE/'source-manifest.json').read_text()); assert manifest['source_tree_sha256']==TREE
 expected={v['path']:(v['bytes'],v['sha256']) for v in manifest['files']}
 actual={str(p.relative_to(SOURCE)):(p.stat().st_size,hashlib.sha256(p.read_bytes()).hexdigest()) for p in SOURCE.rglob('*') if p.is_file()}
 assert actual==expected, 'frozen source drift'
 assert hashlib.sha256(json.dumps(manifest['files'],sort_keys=True,separators=(',',':')).encode()).hexdigest()==TREE

def disk(label):
 raw=subprocess.check_output(['df','-k',str(REPO)],text=True); (out/(label+'.df')).write_text(raw)
 available=int(raw.splitlines()[-1].split()[3])*1024; assert available>=50*1024**3, 'less than 50GiB free'
 return available

def execute(name,cmd,active_env):
 verify_source(); disk(name+'-before'); started=time.time()
 with (out/(name+'.log')).open('wb') as log:
  proc=subprocess.Popen(cmd,cwd=SOURCE,env=active_env,stdout=log,stderr=subprocess.STDOUT)
  running={'runner_pid':os.getpid(),'pid':proc.pid,'step':name,'command':cmd,'started_unix':started,'source_tree':TREE,'target':active_env['CARGO_TARGET_DIR']}
  put(out/'running.json',running); print(json.dumps(running),flush=True); rc=proc.wait()
 data=(out/(name+'.log')).read_bytes(); verify_source()
 row={**running,'exit_code':rc,'seconds':time.time()-started,'log_sha256':hashlib.sha256(data).hexdigest(),'summaries':re.findall(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;',data.decode(errors='replace'))}
 put(out/(name+'.receipt.json'),row); print(json.dumps(row),flush=True)
 assert rc==0, name+' failed; original receipt retained'
 return row

def clean(cache,nightly=False):
 assert cache.parent.resolve()==(REPO/'target').resolve() and not cache.is_symlink() and cache.name.startswith('r1-oct15-proof-base')
 if not cache.exists():
  cache.mkdir(); (cache/'CACHEDIR.TAG').write_text('Signature: 8a477f597d28d172789f06886806bc55\n# Cargo task cache\n')
 assert 'Signature: 8a477f597d28d172789f06886806bc55' in (cache/'CACHEDIR.TAG').read_text()
 assert not subprocess.check_output(['git','ls-files','--',str(cache.relative_to(REPO))],cwd=REPO).strip()
 opened=subprocess.run(['lsof','+D',str(cache)],capture_output=True,text=True)
 (out/('cache-'+cache.name+'-lsof.txt')).write_text(opened.stdout+opened.stderr)
 assert opened.returncode==1 and not opened.stdout.strip() and not opened.stderr.strip(), 'cache active or lsof inconclusive'
 for p in cache.rglob('.cargo-lock'):
  with p.open('a+') as held: fcntl.flock(held,fcntl.LOCK_EX|fcntl.LOCK_NB); fcntl.flock(held,fcntl.LOCK_UN)
 size=subprocess.check_output(['du','-sk',str(cache)],text=True); (out/('cache-'+cache.name+'-du.txt')).write_text(size)
 assert int(size.split()[0])<=20*1024**2, 'cache exceeds 20GiB; assess safe cleanup first'
 cmd=['cargo']+(['+nightly-2026-10-07'] if nightly else [])+['clean','--manifest-path',str(SOURCE/'Cargo.toml'),'--target-dir',str(cache)]
 for package in PACKAGES: cmd += ['-p',package]
 active=env.copy(); active['CARGO_TARGET_DIR']=str(cache); execute('clean-'+('nightly' if nightly else 'stable'),cmd,active)
 disk('clean-'+('nightly' if nightly else 'stable')+'-after')

verify_source(); put(out/'context.json',{'source':str(SOURCE),'source_tree':TREE,'file_count':923,'runner_pid':os.getpid(),'conditional_database_entries_are_native_qualification':False,'rust_env':{k:env[k] for k in ['CARGO_TARGET_DIR','CARGO_INCREMENTAL','CARGO_PROFILE_DEV_DEBUG','CARGO_PROFILE_TEST_DEBUG','RUSTFLAGS','RUSTDOCFLAGS']}})
clean(TARGET)
receipts=[]
for name,cmd in COMMANDS:
 active=env.copy()
 if name=='nightly':
  active['CARGO_TARGET_DIR']=str(TARGET)+'-nightly'; clean(Path(active['CARGO_TARGET_DIR']),True)
 receipts.append(execute(name,cmd,active)); put(out/'receipts.json',receipts)
put(out/'summary.json',{'state':'passed','source_tree':TREE,'gates':len(receipts),'native_postgres':False,'completed_unix':time.time()})
