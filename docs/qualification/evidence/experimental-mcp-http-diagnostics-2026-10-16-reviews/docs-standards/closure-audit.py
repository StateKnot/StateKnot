from pathlib import Path
from datetime import datetime,timezone
import json,hashlib,re,subprocess,difflib,os
R=Path('/Users/jiawy/Documents/RustAgentFramework');B=R/'.local-verification/2026-10-16';O=B/'evidence/mcp-http-debug-docs-standards-review-v1';D=B/'mcp-http-debug-adoption-docs-v3';V2=B/'mcp-http-debug-adoption-docs-v2';V1=B/'mcp-http-debug-adoption-docs-v1';N=B/'mcp-http-debug-native-v1/execution-v1';sha=lambda d:hashlib.sha256(d).hexdigest(); put=lambda n,v:(O/n).write_text(json.dumps(v,indent=2,ensure_ascii=False)+'\n')
m=json.loads((D/'manifest.json').read_text()); assert sha((D/'manifest.json').read_bytes())=='aef6ecd8f947bf410d4e97f721ea3d0fb7c01a1a5b23dce1bf4d2d07b54a1044'
for row in m['files']:
 d=(D/row['path']).read_bytes();assert (len(d),sha(d))==(row['bytes'],row['sha256'])
assert sha((D/'receipt.json').read_bytes())=='add2713ee28d08b2942e4b4ad5e2fed4b5cd7c76e6c6e06d3d12448688137932'
docs=json.loads((D/'candidate-documents.json').read_bytes())['files']; proofs=[]
for row in docs:
 p=row['path'];before=(D/'baseline'/p).read_bytes();after=(D/'candidate'/p).read_bytes()
 assert before==subprocess.check_output(['git','show','8016b74e8047fda51f60c735d477d735a897cb2f:'+p],cwd=R)
 assert after==(V2/'candidate'/p).read_bytes(); assert sha(after)==row['sha256']; assert sha(before)==row['baseline_sha256']
 ops=difflib.SequenceMatcher(a=before.splitlines(keepends=True),b=after.splitlines(keepends=True),autojunk=False).get_opcodes(); assert all(x[0] in ['equal','insert'] for x in ops)
 fences=lambda raw:re.findall(rb'^```[^\n]*\n.*?^```[ \t]*$',raw,re.M|re.S);assert fences(before)==fences(after)
 proofs.append({'path':p,'sha256':sha(after),'baseline8016_byte_equal':True,'insertions_only':True,'old_examples_byte_equal':True,'v2_v3_document_bytes_equal':True})
text=(D/'candidate/docs/mcp-client.md').read_text();cn=(D/'candidate/docs/mcp-client.zh-CN.md').read_text();assert 'PostgreSQL15-case' not in text and 'PostgreSQL 16/17 full 15-test' in text;assert 'PostgreSQL 16/17上每版完整15项' in cn
summary=json.loads((N/'summary.json').read_bytes());assert summary['state']=='passed' and summary['source_tree']=='31a5ff5596ead9f859a20e7b483d6b660e0577fcf47327ec0a572eb3493e6fca';assert summary['compiled_canonical_sha256']=='1ec97f720269934853dcb3fd66241b38a5b381fda48f4164c11f3dcc091f945c';assert summary['source_928_before_after_exact'] and not summary['full_Store_Runtime_Native_CI_capacity_or_R1_qualified']
pattern=re.compile(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;'); cases=[]
for major,version in [(16,'16.15'),(17,'17.11')]:
 identity=json.loads((N/f'pg{major}-identity.json').read_bytes());assert identity['major']==major and identity['actual_version'].split()[0]==version;marker_count=0
 for target,count in [('mcp_durable',6),('mcp_reconciliation',10)]:
  prefix=f'pg{major}-{target}';raw=(N/(prefix+'.log')).read_bytes();r=json.loads((N/(prefix+'.receipt.json')).read_bytes());q=json.loads((N/(prefix+'-qualification.json')).read_bytes());binary=json.loads((N/(target+'-binary.json')).read_bytes());build=json.loads((N/(target+'-fresh-build.receipt.json')).read_bytes())
  assert r['exit_code']==0 and sha(raw)==r['log_sha256'];assert r['source_tree']==summary['source_tree'] and r['default_stack_no_runner_override'];assert '--test-threads=1' in r['argv']
  assert pattern.findall(raw.decode())==[('ok',str(count),'0','0','0','0')];assert q['passed']==count and q['failed']==q['ignored']==q['filtered']==0
  parsed={}
  for line in raw.decode().splitlines():
   if line.startswith('STATEKNOT_') and '_EVIDENCE' in line:
    name,sep,payload=line.partition('=')
    if not sep:name,sep,payload=line.partition(':')
    assert sep;obj=json.loads(payload);parsed.setdefault(name,[]).append(obj)
  assert parsed==q['markers'];marker_count+=sum(len(v) for v in parsed.values())
  event=binary['cargo_artifact'];assert event['fresh'] is False and event['features']==[] and event['profile']=={'debug_assertions':True,'debuginfo':0,'opt_level':'0','overflow_checks':True,'test':True};assert q['binary_sha256']==binary['sha256'];assert build['exit_code']==0 and '--all-features' not in build['argv']
  events=[json.loads(l) for l in (N/(target+'-fresh-build.log')).read_text().splitlines() if l.startswith('{')];assert event in events
  cases.append({'major':major,'server_version':version,'suite':target,'passed':count,'failed':0,'ignored':0,'filtered':0,'source':summary['source_tree'],'binary_sha256':binary['sha256'],'raw_log_sha256':sha(raw),'fresh_default_feature_Cargo_artifact_confirmed_in_raw_build_log':True,'profile':event['profile'],'original_receipt_default_stack_no_override':True,'all_JSON_markers_equal_saved_qualification':True})
 assert marker_count==7
initial=json.loads((N/'initial-inputs.json').read_bytes()); final=json.loads((N/'final-inputs.json').read_bytes());assert initial['source_tree']==final['source_tree']==summary['source_tree'] and initial['source_files']==final['source_files']==928 and initial['exact'] is final['exact'] is True
receipt=json.loads((D/'receipt.json').read_bytes());assert receipt['new_targeted_Native_qualification'] is True and receipt['new_full_Native_or_fullCI_conformance_or_stdio_qualification'] is False and 'new_Native_or_fullCI_conformance_or_stdio_qualification' not in receipt
bindings=json.loads((D/'fact-bindings.json').read_bytes())
for row in bindings['refs']:
 data=(R/row['path']).read_bytes();assert (len(data),sha(data))==(row['bytes'],row['sha256'])
put('closure-v3.json',{'observed_utc':datetime.now(timezone.utc).isoformat(),'final_docs_manifest_sha256':sha((D/'manifest.json').read_bytes()),'final_docs_freeze_sha256':sha((D/'freeze.json').read_bytes()),'final_docs_receipt_sha256':sha((D/'receipt.json').read_bytes()),'prior_v1_manifest_sha256':sha((V1/'manifest.json').read_bytes()),'v1_finding_preserved_unchanged':'findings-v1.json','v1_P2_PostgreSQL15_case_wording_closed':True,'final_document_bytes_identical_to_v2':True,'v3_only_receipt_classification_refinement_targeted_vs_full_qualification':True,'baseline8016_and_original_examples_preserved':proofs,'new_targeted_Native_facts':cases,'per_major_JSON_marker_count':7,'Native_summary_sha256':sha((N/'summary.json').read_bytes()),'source_before_after_identity_fields_exact':True,'complete_resource_observation_bodies_equal':initial==final,'cache_logical_size_observations_are_distinct_not_source_changes':True,'targeted_scope_not_all_database_cases_or_full_Store_Runtime':True,'remaining_findings':0,'new_compilation_network_DB_Docker_cache_or_product_runner_executed_by_reviewer':False,'reserved_report_targets_not_yet_qualified_as_present':True})
put('receipt.json',{'classification':'independent three-document Standards review, final immutable docs-v3 with preserved v1 finding','source_tree':'31a5ff5596ead9f859a20e7b483d6b660e0577fcf47327ec0a572eb3493e6fca','conservative_inputs':536,'compiled_sha256':'1ec97f720269934853dcb3fd66241b38a5b381fda48f4164c11f3dcc091f945c','reviewer_authored_docs':False,'reviewer_authored_this_Debug_code_or_TCP_test_family':False,'v1_original_wording_findings':1,'v1_findings_preserved':True,'v3_closed_wording_findings':1,'remaining_documented_standard_findings':0,'remaining_heuristic_smell_findings':0,'fixed_doc_manifest_sha256':sha((D/'manifest.json').read_bytes()),'fixed_doc_receipt_sha256':sha((D/'receipt.json').read_bytes()),'checks':['actual diff against fixed Git8016','insertions only; all original code fences and paragraphs retained','exact four Debug types, fixed fields and bare InputRequired exclusions','2099/6ignored48doc independently recounted in v1 audit','four new targeted Native raw logs/receipts and fresh default Cargo artifacts independently read','per-major seven complete JSON marker equality','v2/v3 docs bytes identical, metadata targeted-vs-full clarified','no older Native/fullCI/stdio/R1 transfer','three reserved report targets require final collector publication'],'qualification':'document Standards only; no independent source Spec or new execution gate grant','no_Cargo_network_DB_Docker_cache_main_or_candidate_edits':True})
print(json.dumps({'final_docs':'v3','closed':1,'remaining':0,'new_targeted_Native_harness_total':sum(x['passed'] for x in cases),'markers_per_major':7}))
