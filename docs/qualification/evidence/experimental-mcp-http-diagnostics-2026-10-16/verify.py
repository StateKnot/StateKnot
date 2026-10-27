#!/usr/bin/env python3
"""Offline integrity, source reconstruction and raw evidence verifier."""
from pathlib import Path
import argparse,hashlib,json,subprocess,sys
H=Path(__file__).resolve().parent
sha=lambda b:hashlib.sha256(b).hexdigest()
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repository-root',type=Path);p.add_argument('--workspace',action='store_true');a=p.parse_args()
repo=a.repository_root.resolve() if a.repository_root else H.parents[3]
m=json.loads((H/'manifest.json').read_text());seal=json.loads((H/'seal.json').read_text())
assert sha((H/'manifest.json').read_bytes())==seal['manifest_sha256']
expected={row['path'] for row in m['files']}|{'manifest.json','seal.json'}
assert {str(f.relative_to(H)) for f in H.rglob('*') if f.is_file()}==expected
for row in m['files']:
 f=H/row['path'];assert f.is_file() and not f.is_symlink() and '..' not in Path(row['path']).parts
 assert (f.stat().st_size,sha(f.read_bytes()))==(row['bytes'],row['sha256']),row['path']
command=[sys.executable,str(H/'verify-component.py'),'--repository-root',str(repo)]
if a.workspace:command.append('--workspace')
subprocess.run(command,check=True)
subprocess.run([sys.executable,str(H/'verify-native.py')],check=True)
report=H.parents[1]/'experimental-mcp-http-diagnostics-2026-10-16.json'
if report.exists():
 r=json.loads(report.read_text());assert r['evidence']['manifest_sha256']==seal['manifest_sha256']
 assert r['source']['frozen_tree_sha256']==seal['source_tree'] and r['source']['compiled_canonical_sha256']==seal['compiled_canonical_sha256']
 assert r['scope']['full_CI_qualified'] is False and r['scope']['R1_complete'] is False
print(json.dumps({'state':'bounded evidence verified','source':seal['source_tree'],'workspace_bytecheck_requested':a.workspace,'independent_review_performed':False,'executed_build_network_database_commands':False}))
