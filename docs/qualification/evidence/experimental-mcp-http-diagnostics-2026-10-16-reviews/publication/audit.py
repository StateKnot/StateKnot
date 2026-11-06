from pathlib import Path
from datetime import datetime,timezone
import subprocess,json,hashlib,gzip,re,sys,os
R=Path('/Users/jiawy/Documents/RustAgentFramework');B=R/'.local-verification/2026-10-16';P=B/'mcp-http-debug-publication-preparation-v1';C=P/'candidate';E=C/'docs/qualification/evidence/experimental-mcp-http-diagnostics-2026-10-16';O=B/'evidence/mcp-http-debug-publication-independent-review-v1';Q=B/'mcp-http-debug-qualification-v3';N=B/'mcp-http-debug-native-v1/execution-v1';sha=lambda x:hashlib.sha256(x).hexdigest();put=lambda n,v:(O/n).write_text(json.dumps(v,indent=2,ensure_ascii=False)+'\n')
frozen=sha((P/'candidate-manifest.json').read_bytes());assert frozen=='29815d58e4d758bd2e65e99966f5811e2785d5de73bc772edf859ebc19ecc679'
f=json.loads((P/'freeze.json').read_bytes());assert sha((P/'freeze.json').read_bytes())=='b51dadcb17feadce1aba0c2eb26db3e3f62bbc5b8ea33d93194b4e526cf31e29';rows=json.loads((P/'candidate-manifest.json').read_bytes())['files'];assert len(rows)==98 and sum(r['bytes'] for r in rows)==595170
assert {p.relative_to(C).as_posix() for p in C.rglob('*') if p.is_file()}=={r['path'] for r in rows}
for row in rows:
 data=(C/row['path']).read_bytes();assert not (C/row['path']).is_symlink() and (len(data),sha(data))==(row['bytes'],row['sha256'])
assert sha((E/'manifest.json').read_bytes())=='fa851d1a2e82d25dfbca99749a31fea315961109260370f1e5f8973c05d9349f'
commands=[('outer-offline',[sys.executable,str(P/'verify.py')]),('published-offline',[sys.executable,str(E/'verify.py'),'--repository-root','.']),('published-workspace-negative',[sys.executable,str(E/'verify.py'),'--repository-root','.','--workspace'])]
observations=[]
for name,cmd in commands:
 p=subprocess.run(cmd,cwd=R,stdout=subprocess.PIPE,stderr=subprocess.PIPE);(O/(name+'.stdout')).write_bytes(p.stdout);(O/(name+'.stderr')).write_bytes(p.stderr)
 row={'command':cmd,'exit_code':p.returncode,'stdout_sha256':sha(p.stdout),'stderr_sha256':sha(p.stderr),'read_only_saved_evidence_verification':True};put(name+'.receipt.json',row);observations.append(row)
 if name.endswith('negative'):assert p.returncode!=0 and b'crates/stateknot-integrations/src/mcp_client.rs' in p.stderr
 else:assert p.returncode==0
index={r['path']:r for r in json.loads((E/'artifact-index.json').read_bytes())['artifacts']}
def raw(name):
 key=name if name in index else name+'.gz'; d=(E/key).read_bytes();return gzip.decompress(d) if index[key]['storage_encoding']=='gzip' else d
original_comparisons=[]
for gate in ['fmt','clippy','workspace','doctests','rustdoc','nightly']:
 for suffix in ['.log','.receipt.json']:
  path=gate+suffix; assert raw('execution/'+path)==(Q/path).read_bytes(); original_comparisons.append('execution/'+path)
 r=json.loads(raw('execution/'+gate+'.receipt.json'));assert 'runner_sha256' not in r and 'rust_environment_allowlist' not in r
for filename in ['run-gate.py','execute-six.py','six-gates-summary.json']:
 assert raw('execution/'+filename)==(Q/filename).read_bytes();original_comparisons.append('execution/'+filename)
for name in index:
 if name.startswith('native/'):
  rel=name[len('native/'):];rel=rel[:-3] if index[name]['storage_encoding']=='gzip' else rel
  if (N/rel).exists():assert raw(name)==(N/rel).read_bytes();original_comparisons.append(name)
refs=json.loads(raw('source/base-references.json'))['reused_source_only_refs'];assert len(refs)==18
payloads=list((E/'source/payloads').glob('*'));assert len(payloads)==2
record=json.loads((C/'docs/qualification/experimental-mcp-http-diagnostics-2026-10-16.json').read_bytes());assert record['source']['frozen_tree_sha256']=='31a5ff5596ead9f859a20e7b483d6b660e0577fcf47327ec0a572eb3493e6fca';assert record['source']['compiled_canonical_sha256']=='1ec97f720269934853dcb3fd66241b38a5b381fda48f4164c11f3dcc091f945c';assert record['source']['final_payloads']==2 and record['source']['source_only_external_refs']==18
assert record['six_local_gates']['workspace']=={'suites':99,'passed':2099,'failed':0,'ignored':6,'filtered':0};assert record['six_local_gates']['doctests']=={'suites':10,'passed':48,'failed':0,'ignored':0,'filtered':0}
assert record['real_TCP_regression']['family_count']==1 and record['real_TCP_regression']['branches']==8 and record['real_TCP_regression']['branches_are_not_separate_harness_tests']
assert record['history']['original_Debug_canary']['raw_sha256']=='fde49495aaf4bd84acc7f673816dbf90250c2b4a5a2a7368f7b0308205902858';assert record['history']['first_fixed_candidate_Clippy']['exit_code']==101
assert record['targeted_Native']['postgres_versions']==['16.15','17.11'] and record['targeted_Native']['markers_each_major']==7 and record['targeted_Native']['32_harness_executions_are_32_database_only_cases'] is False
assert record['scope']['full_CI_qualified'] is record['scope']['R1_complete'] is record['scope']['full_Store_Runtime_Native_qualified'] is False
links=[]
for name in ['experimental-mcp-http-diagnostics-2026-10-16.md','experimental-mcp-http-diagnostics-2026-10-16.zh-CN.md']:
 text=(C/'docs/qualification'/name).read_text()
 for token in ['31a5ff5596ead9f859a20e7b483d6b660e0577fcf47327ec0a572eb3493e6fca','1ec97f720269934853dcb3fd66241b38a5b381fda48f4164c11f3dcc091f945c','McpNotification','McpCompleteToolResult','McpRemoteError','McpToolCallResponse','McpInputRequired','McpToolCall::InputRequired','Applied + Never','16.15','17.11','schema:24','234a01c8','d0927746','fde49495','96e4fed5']:assert token in text,(name,token)
 for target in re.findall(r'\[[^\]]*\]\(([^)]+)\)',text):
  if '://' in target:continue
  resolved=Path(os.path.normpath(str(Path('docs/qualification')/target.split('#',1)[0]))).as_posix();assert (C/resolved).is_file() or (R/resolved).is_file();links.append({'document':name,'target':target,'resolved':resolved})
# Scan only for credential-bearing URL/key forms; never persist a match value.
findings=[]
patterns=[rb'(?i)(?:https?|postgres(?:ql)?)://[^/\s:@]+:[^@\s/]+@',rb'github_pat_[A-Za-z0-9_]{15,}',rb'ghp_[A-Za-z0-9]{20,}',rb'-----BEGIN (?:OPENSSH|RSA|EC|DSA)? ?PRIVATE KEY-----']
for row in rows:
 path=row['path'];d=(C/path).read_bytes();d=gzip.decompress(d) if path.endswith('.gz') else d
 if any(re.search(pattern,d) for pattern in patterns):findings.append(path)
 assert not path.endswith(('.tar','.tar.gz','.tgz','.zip','.exe','.dylib','.so')) and '/target/' not in path and '/node_modules/' not in path
assert not findings,findings
put('audit.json',{'candidate_manifest_sha256':frozen,'freeze_sha256':sha((P/'freeze.json').read_bytes()),'candidate_paths':98,'candidate_bytes':595170,'Emanifest_sha256':sha((E/'manifest.json').read_bytes()),'source_reconstruction_and_historical_derivation_verifier_exits':[r['exit_code'] for r in observations],'original_artifacts_compared_byte_exact':original_comparisons,'two_payloads_and18_source_refs':True,'source31a_compiled536_1ec9':True,'six_gate_runner_sources_retained_current_bytes_not_contemporaneous_runner_or_env_attestation':True,'Native_runner_hash_env_profile_lists_and7_complete_JSON_objects_each_major_verified':True,'schema24_legacy_profile_not_schema36_database':True,'original_history_red101_and_fixture_lint101_preserved':True,'EN_CN_JSON_scope_and_count_consistent':True,'local_report_links':links,'binary_cache_full_archive_credential_URL_or_private_key_payloads_found':False,'workspace_negative_rejects_unadopted_mcp_client_source':True,'all_saved_runners_examined_not_executed':True,'no_product_build_network_DB_Docker_cache_main_or_original_candidate_write':True,'limitations':['Direct published verifier validates the evidence closure and selected JSON source/scope fields; the outer candidate manifest independently binds all98 current files including EN/CN/JSON reports. Its workspace mode checks536 product inputs, not all documentation/CI/Git-tree files.','This review validates the fixed supplied artifact set and original observable facts; it is not new execution or a source Spec review.']})
print(json.dumps({'candidate98_verified':True,'offline':0,'direct_published_offline':0,'workspace_expected_rejection':observations[-1]['exit_code'],'credential_URL_or_private_key_matches':len(findings),'current_original_comparisons':len(original_comparisons),'remaining_findings':0}))
