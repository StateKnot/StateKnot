from pathlib import Path
import hashlib, json, os, shutil, subprocess, time

base = Path(__file__).resolve().parent
source = base / 'proof-base-erased-source'
manifest_path = base / 'evidence/proof-base-erased/source-manifest.json'
files = json.loads(manifest_path.read_text())
def verify():
    assert all(hashlib.sha256((source/f['path']).read_bytes()).hexdigest()==f['sha256'] for f in files)
verify()
target = base.parents[1] / 'target/r1-oct15-root-layout'
env = os.environ.copy()
env.update(CARGO_TARGET_DIR=str(target), CARGO_INCREMENTAL='0', CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0', RUSTFLAGS='-D warnings', RUSTDOCFLAGS='-D warnings')
for package, kind, name, label in [
    ('stateknot-integrations','--test','provider_native_postgres','proof-base-erased-integrations-v1'),
    ('stateknot','--example','deepseek_agent','proof-base-erased-example-v1'),
]:
    out = base / 'evidence' / label
    out.mkdir(exist_ok=False)
    (out/'runner.py').write_bytes(Path(__file__).read_bytes())
    (out/'before.df').write_bytes(subprocess.check_output(['df','-k',str(base)]))
    cmd=['cargo','test','--manifest-path',str(source/'Cargo.toml'),'-p',package,kind,name,'--all-features','--locked','--no-run','--message-format=json']
    start=time.time()
    with (out/'build.stdout.jsonl').open('w') as stdout, (out/'build.stderr.log').open('w') as stderr:
        result=subprocess.run(cmd,cwd=source,env=env,stdout=stdout,stderr=stderr)
    artifacts=[]
    if result.returncode==0:
        for line in (out/'build.stdout.jsonl').read_text().splitlines():
            event=json.loads(line)
            if event.get('reason')=='compiler-artifact' and event.get('executable') and event.get('target',{}).get('name')==name:
                saved=out/(package+'-'+name)
                shutil.copy2(event['executable'],saved)
                artifacts.append({'path':str(saved),'sha256':hashlib.sha256(saved.read_bytes()).hexdigest(),'fresh':event['fresh']})
    verify()
    receipt={'source':str(source),'source_manifest_sha256':hashlib.sha256(manifest_path.read_bytes()).hexdigest(),'all_frozen_files_match':True,'command':cmd,'exit_code':result.returncode,'seconds':time.time()-start,'target':str(target),'artifacts':artifacts,'same_source_cache':True,'env_overrides':{k:env[k] for k in ('CARGO_INCREMENTAL','CARGO_PROFILE_DEV_DEBUG','CARGO_PROFILE_TEST_DEBUG','RUSTFLAGS','RUSTDOCFLAGS')}}
    (out/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
    print(json.dumps(receipt,indent=2),flush=True)
    assert result.returncode==0 and len(artifacts)==1
