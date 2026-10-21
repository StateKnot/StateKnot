#!/usr/bin/env python3
"""Read-only independent publication review bindings; pure offline replay only."""
from pathlib import Path
import hashlib,json,subprocess,sys
H=Path(__file__).resolve().parent;R=H.parents[3]
load=lambda p:json.loads(p.read_text());sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
def main():
 m=load(H/'manifest.json');r=load(H/'receipt.json');assert sha(H/'manifest.json')==r['manifest_sha256']
 assert {str(p.relative_to(H)) for p in H.rglob('*') if p.is_file()}=={row['path'] for row in m['files']}|{'manifest.json','receipt.json'}
 for row in m['files']:
  p=H/row['path'];assert p.stat().st_size==row['bytes'] and sha(p)==row['sha256']
 for row in load(H/'input-bindings.json')['files']:
  p=R/row['path'];assert p.stat().st_size==row['bytes'] and sha(p)==row['sha256']
 for name,exit_code in [('outer-offline',0),('direct-offline',0),('fixed-source-workspace',0),('main-workspace-before-adoption',1)]:
  rec=load(H/(name+'.receipt.json'));assert rec['actual_exit_code']==rec['expected_exit_code']==exit_code
  assert sha(H/(name+'.stdout'))==rec['stdout_sha256'] and sha(H/(name+'.stderr'))==rec['stderr_sha256']
 c=load(H/'checks.json');assert c['candidate178_exact'] and c['actual_gate_receipt_count']==16 and len(c['all_original_helpers_preserved'])==13
 assert r['actionable_Spec_findings']==0 and r['reviewer_authored_publication'] is False
 outer=R/'.local-verification/2026-10-16/a2a-subscription-publication-preparation-v1/verify-candidate.py'
 subprocess.run([sys.executable,'-B',str(outer),'--repository-root',str(R)],check=True,stdout=subprocess.DEVNULL)
 print(json.dumps({'state':'verified','actionable_Spec_findings':0,'candidate_files':178,'actual_gate_receipts_preserved':16,'actual_helpers':13,'historical_main_before_adoption_rejection_preserved':True,'new_execution_or_Native_CI_R1_qualification':False}))
if __name__=='__main__':main()
