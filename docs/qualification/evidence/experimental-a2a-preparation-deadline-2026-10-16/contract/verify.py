#!/usr/bin/env python3
"""Static, read-only input and frozen-contract verification. No Rust/network/DB."""
from pathlib import Path
import hashlib,json,re,subprocess,sys
H=Path(__file__).resolve().parent;R=H.parents[2]
load=lambda p:json.loads(p.read_text());sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
def main():
 draft=sys.argv[1:]==['--draft'];assert not sys.argv[1:] or draft
 if not draft:
  m=load(H/'manifest.json');f=load(H/'seal.json');assert sha(H/'manifest.json')==f['manifest_sha256']
  assert f['contract_bytes_frozen'] is True and f['actual_product_execution'] is False
  assert {str(p.relative_to(H)) for p in H.rglob('*') if p.is_file()}=={row['path'] for row in m['files']}|{'manifest.json','seal.json'}
  for row in m['files']:
   p=H/row['path'];assert p.stat().st_size==row['bytes'] and sha(p)==row['sha256'],row['path']
 s=load(H/'sources.json');assert s['HEAD']=='1638f5adb597f5cafdcb2923608ca63ed75eccfe'
 # Repository inputs bind immutable adopted Git head, not a later workingtree.
 for row in s['files']:
  b=subprocess.check_output(['git','show',s['HEAD']+':'+row['path']],cwd=R)
  assert len(b)==row['bytes'] and hashlib.sha256(b).hexdigest()==row['sha256'],row['path']
 for row in s['locked_primary_SDK_source']+s['locked_primary_Axum_body_sources']:
  p=Path(row['path']);assert p.stat().st_size==row['bytes'] and sha(p)==row['sha256'],row['path']
 original=subprocess.check_output(['git','show',s['HEAD']+':crates/stateknot-integrations/src/a2a_server.rs'],cwd=R).decode()
 assert original.count('.prepare(')==11
 lock=subprocess.check_output(['git','show',s['HEAD']+':Cargo.lock'],cwd=R).decode()
 for name,version in s['locked_versions'].items():
  assert re.search(r'name = "'+re.escape(name)+r'"\nversion = "'+re.escape(version)+r'"',lock),(name,version)
 mat=load(H/'entry-matrix.json');body=load(H/'body-path-matrix.json');a=load(H/'acceptance.json')
 assert len(mat['operations'])==11 and len(mat['bindings'])==2 and len(body['classes'])==6
 assert len({x['id'] for x in a['cases']})==len(a['cases'])==a['case_count']==21
 assert all(x['executed'] is False for x in a['cases'])
 prior=load(H/'prior-contract-binding.json');v2=R/prior['v2_directory']
 assert sha(v2/'manifest.json')==prior['v2_manifest_sha256'] and sha(v2/'seal.json')==prior['v2_seal_sha256']
 for row in load(v2/'manifest.json')['files']:
  assert sha(v2/row['path'])==row['sha256']
 ax={Path(x['path']).as_posix():Path(x['path']).read_text() for x in s['locked_primary_Axum_body_sources']}
 def source(suffix):return next(v for k,v in ax.items() if k.endswith(suffix))
 assert 'pub fn apply<B>' in source('extract/default_body_limit.rs')
 assert 'req.extensions_mut().insert(self.kind);' in source('extract/default_body_limit.rs')
 assert 'const DEFAULT_LIMIT: usize = 2_097_152;' in source('ext_traits/request.rs')
 assert 'pub use bytes::Bytes;' in source('body/mod.rs')
 assert 'pub enum JsonRejection' in source('axum-0.8.9/src/extract/rejection.rs')
 assert 'BytesRejection,' in source('axum-0.8.9/src/extract/rejection.rs')
 assert '#[derive(Clone)]\npub struct Parts' in source('http-1.5.0/src/request.rs')
 assert all('timeout' not in Path(x['path']).read_text().lower() for x in s['locked_primary_SDK_source'])
 print(json.dumps({'state':'static input and acceptance bindings verified','mode':'draft' if draft else 'frozen','operations':11,'bindings':2,'body_classes':6,'unexecuted_acceptance_cases':21,'prior_v2_preserved':True,'actual_product_red_green_Native_CI':False}))
if __name__=='__main__':main()
