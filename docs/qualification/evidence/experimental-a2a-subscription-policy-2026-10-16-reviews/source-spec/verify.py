#!/usr/bin/env python3
"""Read-only Spec receipt/source bindings; no Rust/network/DB/cache or main changes."""
from pathlib import Path
import hashlib,json
H=Path(__file__).resolve().parent;R=H.parents[3]
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();load=lambda p:json.loads(p.read_text())
def main():
 m=load(H/'manifest.json');r=load(H/'receipt.json');assert sha(H/'manifest.json')==r['manifest_sha256']
 assert {str(p.relative_to(H)) for p in H.rglob('*') if p.is_file()}=={i['path'] for i in m['files']}|{'manifest.json','receipt.json'}
 for row in m['files']:
  p=H/row['path'];assert p.stat().st_size==row['bytes'] and sha(p)==row['sha256']
 for row in load(H/'input-bindings.json')['files']:
  p=R/row['path'];assert p.stat().st_size==row['bytes'] and sha(p)==row['sha256'],row['path']
 assert r['actionable_Spec_findings']==0 and r['reviewer_authored_contract'] is True and r['reviewer_authored_implementation_or_tests'] is False
 c=load(H/'checks.json');assert c['source930_exact'] and c['compiled538_exact'] and c['complete_production_normalization_to_4e726']
 assert c['workspace_actual_suites_pass_fail_ignore']==[100,2108,0,6] and c['doctest_actual_groups_pass_fail']==[10,48,0]
 assert len(c['contract_matrix_ids'])==17 and len(c['six_actual_gate_receipts_and_raw_hashes_verified'])==6
 print(json.dumps({'state':'verified','actionable_Spec_findings':0,'source930':c['source_tree'],'source538':c['compiled_canonical_sha256'],'actual_tests_rerun_by_reviewer':False,'new_Native_or_fullCI_or_R1_qualification':False}))
if __name__=='__main__':main()
