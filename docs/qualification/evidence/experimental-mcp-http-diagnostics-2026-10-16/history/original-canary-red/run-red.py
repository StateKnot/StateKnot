from pathlib import Path
import os,json,hashlib,subprocess,time,fcntl
R=Path('/Users/jiawy/Documents/RustAgentFramework');B=R/'.local-verification/2026-10-16';S=B/'mcp-http-debug-red-source-v1';O=Path(__file__).resolve().parent/'red-probe-v1';O.mkdir(exist_ok=False)
Q=O.parent;lock=(Q/'gate.lock').open('a+');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();m=json.loads((Q/'red-source-manifest.json').read_text())
def snapshot():
 rows=[{'path':str(p.relative_to(S)),'bytes':p.stat().st_size,'sha256':sha(p)} for p in sorted(S.rglob('*')) if p.is_file()]
 assert rows==m['files'];return rows
def disk(label):
 raw=subprocess.check_output(['df','-k',str(R)],text=True);(O/(label+'.df')).write_text(raw);assert int(raw.splitlines()[-1].split()[3])>=50*1024**2
 total=sum(int(subprocess.check_output(['du','-sk',str(p)],text=True).split()[0]) for p in (R/'target').glob('r1-oct15*') if p.is_dir());assert total<=20*1024**2
 return total
snapshot();before=disk('before');env={k:v for k,v in os.environ.items() if not k.startswith(('STATEKNOT_','CARGO_PROFILE_')) and k not in ['CARGO_ENCODED_RUSTFLAGS','RUST_MIN_STACK','RUSTC_BOOTSTRAP']};env.update(CARGO_TARGET_DIR=str(R/'target/r1-oct15-proof-base'),CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',RUSTFLAGS='-D warnings',RUSTDOCFLAGS='-D warnings')
command=['cargo','+1.88.0','test','-p','stateknot-integrations','--test','mcp_client_contract','--locked','--','--exact','http_response_debug_hides_payloads_without_changing_raw_values_or_resume','--nocapture','--test-threads=1'];started=time.time()
with (O/'test.log').open('wb') as log:
 p=subprocess.Popen(command,cwd=S,env=env,stdout=log,stderr=subprocess.STDOUT);print(json.dumps({'command':command,'pid':p.pid,'expected_red_only_after_actual_canary_assertion':True}),flush=True);code=p.wait()
snapshot();after=disk('after');result={'command':command,'exit_code':code,'seconds':time.time()-started,'source_tree':m['source_tree_sha256'],'source_928_before_after_exact':True,'log_sha256':sha(O/'test.log'),'task_cache_before_kib':before,'task_cache_after_kib':after,'DB_environment_cleared':True,'main_source_modified':False};(O/'receipt.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result),flush=True)
