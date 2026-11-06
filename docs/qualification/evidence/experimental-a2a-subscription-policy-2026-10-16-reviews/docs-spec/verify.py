#!/usr/bin/env python3
"""Read-only sealed docs Spec bindings, no build/network/main modification."""
from pathlib import Path
import json,hashlib
H=Path(__file__).resolve().parent;R=H.parents[3]
load=lambda p:json.loads(p.read_text());sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
def main():
 m=load(H/'manifest.json');r=load(H/'receipt.json');assert sha(H/'manifest.json')==r['manifest_sha256']
 assert {str(p.relative_to(H)) for p in H.rglob('*') if p.is_file()}=={f['path'] for f in m['files']}|{'manifest.json','receipt.json'}
 for row in m['files']:
  p=H/row['path'];assert p.stat().st_size==row['bytes'] and sha(p)==row['sha256']
 for row in load(H/'input-bindings.json')['files']:
  p=R/row['path'];assert p.stat().st_size==row['bytes'] and sha(p)==row['sha256']
 assert r['actionable_Spec_findings']==0 and r['reviewer_authored_docs'] is False
 assert len(load(H/'checks.json')['candidate_files'])==5
 print(json.dumps({'state':'verified','actionable_Spec_findings':0,'candidate_manifest':r['candidate_manifest_sha256'],'adoption_requires_actual_reserved_reports':True,'new_execution_or_Native_CI_qualification':False}))
if __name__=='__main__':main()
