#!/usr/bin/env python3
"""Pure offline receipt/inventory checks; optional actual531 and published-input reconstruction."""
from pathlib import Path
import argparse, difflib, gzip, hashlib, io, json, re, tarfile
H=Path(__file__).resolve().parent
sha=lambda b:hashlib.sha256(b).hexdigest()
def read(p):return json.loads(p.read_text())
def maps(rows):
 assert len(rows)==len({x['path']for x in rows})
 return {x['path']:(x['bytes'],x['sha256'])for x in rows}
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--workspace',action='store_true');p.add_argument('--workspace-root',type=Path);args=p.parse_args()
 manifest=read(H/'manifest.json')
 for x in manifest['files']:
  data=(H/x['path']).read_bytes();assert len(data)==x['bytes']and sha(data)==x['sha256'],x['path']
 idx=read(H/'artifact-index.json')['artifacts'];by={x['path']:x for x in idx};assert len(by)==len(idx)
 def raw(n):
  x=by[n if n in by else n+'.gz'];b=(H/x['path']).read_bytes();assert len(b)==x['stored_bytes']and sha(b)==x['stored_sha256']
  if x['encoding']=='gzip':assert b[4:8]==b'\0'*4 and b[3]&8==0;b=gzip.decompress(b)
  else:assert x['encoding']=='identity'
  assert len(b)==x['raw_bytes']and sha(b)==x['raw_sha256'];return b
 def obj(n):return json.loads(raw(n))
 for n in by:raw(n)
 before=obj('all-inputs-before.json');after=obj('all-inputs-after.json');expected=obj('expected-compiled-inputs.json')
 assert len(before)==len(after)==2182 and before==after
 selection=lambda n:n.startswith(('crates/','.cargo/'))or n in ['Cargo.toml','Cargo.lock','rust-toolchain.toml']
 actual=[x for x in before if selection(x['path'])];assert len(actual)==len(expected)==531 and maps(actual)==maps(expected)
 summary=obj('summary.json');assert summary['state']=='passed'and summary['gates']==6 and summary['original_opt_level']==0 and summary['source_before_after_exact']
 assert summary['compiled_input_files']==531 and summary['whole_main_input_files']==2182 and summary['native_database_execution']is False and summary['full_CI_qualification']is False
 env=obj('environment.json');assert env['CARGO_INCREMENTAL']==env['CARGO_PROFILE_DEV_DEBUG']==env['CARGO_PROFILE_TEST_DEBUG']=='0'and env['RUSTFLAGS']==env['RUSTDOCFLAGS']=='-D warnings'
 assert not any(k.endswith('_OPT_LEVEL')or k in ['RUST_MIN_STACK','CARGO_ENCODED_RUSTFLAGS']for k in env)
 gate_names=['fmt','clippy','workspace','doctests','rustdoc','nightly'];receipts=obj('receipts.json');assert [r['gate']for r in receipts]==gate_names
 rx=r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;'
 for r in receipts:
  assert r==obj(r['gate']+'.receipt.json')and r['exit_code']==0
  b=raw(r['gate']+'.log');assert sha(b)==r['log_sha256']and [list(t)for t in re.findall(rx,b.decode())]==r['summaries']
  if r['gate']=='nightly':assert '+nightly-2026-10-07'in r['command']
  if r['gate']in ['clippy','workspace','nightly']:assert '--workspace'in r['command']and '--all-targets'in r['command']and '--all-features'in r['command']and '--locked'in r['command']
 counts=re.findall(rx,raw('workspace.log').decode());assert len(counts)==98 and [sum(int(x[i])for x in counts)for i in [1,2,3]]==[2073,0,6]
 docs=re.findall(rx,raw('doctests.log').decode());assert [sum(int(x[i])for x in docs)for i in [1,2,3]]==[45,0,0]
 err=obj('preflight-order-error.json');assert err['product_commands_started']is False and err['path_bytes_SHA_maps_equal']and err['expected_inputs']==err['actual_inputs']==531
 assert sha(raw('runner-preflight-v1.py.txt'))==err['original_runner_sha256']
 runner=raw('runner.py').decode();assert "if not k.startswith('STATEKNOT_')"in runner and "'RUST_MIN_STACK'"in runner
 context=obj('git-context.json');assert context['head']=='04608c6b20a7c18d6b042eeccc83dd38d096b1d1'and context['original_opt_level']==0 and context['compiled_inputs']==531
 seal=read(H/'seal.json');assert sha((H/'manifest.json').read_bytes())==seal['manifest_sha256']
 for n,digest in seal['reports'].items():assert sha((H.parents[1]/n).read_bytes())==digest
 profile=read(H.parents[1]/'experimental-composed-request-facts-pitr-2026-10-15.json');assert profile['gates']['Native_database_execution']is False and profile['claim_limits']['R1_complete']is False and profile['claim_limits']['full_CI_passed']is False
 assert profile['inputs']['compiled_input_list_identity_sha256']==sha(json.dumps(sorted(expected,key=lambda x:x['path']),sort_keys=True,separators=(',',':')).encode())
 workspace=None
 if args.workspace:
  workspace=args.workspace_root or next((p for p in H.parents if(p/'.git').exists()),None);assert workspace is not None
  paths=set()
  for directory in ['crates','.cargo']:
   q=workspace/directory
   if q.exists():paths.update(p.relative_to(workspace).as_posix()for p in q.rglob('*')if p.is_file())
  paths.update(n for n in ['Cargo.toml','Cargo.lock','rust-toolchain.toml']if(workspace/n).is_file());assert paths==set(maps(expected))
  assert all(len((workspace/x['path']).read_bytes())==x['bytes']and sha((workspace/x['path']).read_bytes())==x['sha256']for x in expected)
  ref=read(H/'references.json')
  for key in ['A2A_source_archive','PITR_artifact_manifest','PITR_seal']:assert sha((workspace/ref[key]['path']).read_bytes())==ref[key]['sha256']
  with tarfile.open(fileobj=io.BytesIO((workspace/ref['A2A_source_archive']['path']).read_bytes()),mode='r:gz')as t:
   members=t.getmembers();assert len(members)==923 and all(m.isfile()and m.name.startswith('source/')for m in members)
   source={m.name[len('source/'):]:t.extractfile(m).read()for m in members}
  pf=workspace/ref['PITR_artifact_manifest']['path'];pi={x['key']:x for x in read(pf)['artifacts']}
  def prior(key):
   x=pi[key];b=(pf.parent/x['path']).read_bytes();assert sha(b)==x['stored_sha256'];b=gzip.decompress(b)if x['encoding']=='gzip'else b;assert sha(b)==x['raw_sha256'];return b
  original=source[ref['changed_path']];candidate=prior(ref['PITR_candidate_key']);patch=prior(ref['PITR_patch_key'])
  recreated=''.join(difflib.unified_diff(original.decode().splitlines(True),candidate.decode().splitlines(True),fromfile='a/'+ref['changed_path'],tofile='b/'+ref['changed_path'])).encode();assert recreated==patch
  source[ref['changed_path']]=candidate
  assert all(len(source[x['path']])==x['bytes']and sha(source[x['path']])==x['sha256']for x in expected)
 print(json.dumps({'evidence_integrity':'passed','raw_artifacts':len(idx),'all_nonignored_before_after_exact':2182,'compiled_input_files':531,'original_opt_level':0,'six_local_gates_passed':True,'workspace_suites':98,'workspace_passed':2073,'original_ignored':6,'doctests_passed':45,'workspace_checked':workspace is not None,'published_source_plus_exact_PITR_patch_reconstructed':workspace is not None,'Native_database_execution':False,'full_CI_or_R1_qualified':False},indent=2))
if __name__=='__main__':main()
