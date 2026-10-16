from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,re,difflib
R=Path('/Users/jiawy/Documents/RustAgentFramework'); B=R/'.local-verification/2026-10-16'; O=B/'evidence/mcp-http-debug-standards-review-v1'; S=B/'mcp-http-debug-source-v3'; P=B/'mcp-lossless-bound-state-source-v3'; Q=B/'mcp-http-debug-qualification-v3'; A=B/'mcp-http-debug-authoring-v1'
sha=lambda d:hashlib.sha256(d).hexdigest(); canon=lambda v:json.dumps(v,sort_keys=True,separators=(',',':')).encode()
put=lambda name,v:(O/name).write_text(json.dumps(v,indent=2,ensure_ascii=False)+'\n')
manifest=json.loads((Q/'frozen-source-manifest.json').read_text()); rows=manifest['files']; tree=sha(canon(rows)); assert tree=='31a5ff5596ead9f859a20e7b483d6b660e0577fcf47327ec0a572eb3493e6fca' and len(rows)==928
assert all((len((S/r['path']).read_bytes()),sha((S/r['path']).read_bytes()))==(r['bytes'],r['sha256']) for r in rows)
parent=json.loads((B/'mcp-lossless-qualification-v3/network-enabled-retry-v1/frozen-source-manifest.json').read_text()); before={r['path']:r for r in parent['files']}; changed=[r['path'] for r in rows if r!=before.get(r['path'])]
assert changed==['crates/stateknot-integrations/src/mcp_client.rs','crates/stateknot-integrations/tests/mcp_client_contract.rs']
paths=sorted({p.relative_to(S).as_posix() for prefix in ['crates','.cargo'] for p in (S/prefix).rglob('*') if p.is_file()}|{'Cargo.toml','Cargo.lock','rust-toolchain.toml'})
compiled=[{'path':p,'bytes':len((S/p).read_bytes()),'sha256':sha((S/p).read_bytes())} for p in paths]; cd=sha(canon(compiled)); assert len(compiled)==536 and cd=='1ec97f720269934853dcb3fd66241b38a5b381fda48f4164c11f3dcc091f945c'
prod=changed[0]; a=(P/prod).read_text(); z=(S/prod).read_text(); restored=z
names=['McpNotification','McpCompleteToolResult','McpToolCallResponse','McpRemoteError']
for name in names:
 marker='impl fmt::Debug for '+name+' {'; start=restored.index(marker); depth=0
 for pos in range(start+len(marker)-1,len(restored)):
  if restored[pos]=='{':depth+=1
  elif restored[pos]=='}':
   depth-=1
   if depth==0:break
 assert restored[pos+1:pos+3]=='\n\n'; restored=restored[:start]+restored[pos+3:]
for docs in ['///\n/// `Debug` reports parameter presence without the untrusted method or payload.\n','///\n/// `Debug` reports only content count, structured-output presence and `is_error`;\n/// no content or raw result values are formatted.\n','///\n/// `Debug` reports only the outcome kind and notification count, without\n/// formatting the nested result, continuation, Tool metadata or notifications.\n/// This does not change the separate `Debug` contract of `McpInputRequired`.\n','///\n/// `Debug` retains only its code and data presence; message and data remain\n/// available through explicit untrusted accessors.\n']:
 assert docs in restored; restored=restored.replace(docs,'',1)
for name in ['McpNotification','McpCompleteToolResult','McpRemoteError']:
 restored=restored.replace('#[derive(Clone, PartialEq)]\npub struct '+name,'#[derive(Clone, Debug, PartialEq)]\npub struct '+name,1)
restored=restored.replace('pub struct McpToolCallResponse {','#[derive(Debug)]\npub struct McpToolCallResponse {',1); assert restored==a
suffix='impl fmt::Display for McpRemoteError {'; assert a[a.index(suffix):]==z[z.index(suffix):]
test=changed[1]; prefix=(P/test).read_bytes(); actual=(S/test).read_bytes(); assert len(prefix)==13917 and len(prefix.splitlines())==357 and actual.startswith(prefix)
added=actual[len(prefix):].decode(); assert len(re.findall(r'#\[tokio::test',added))==1
assert 'async fn http_response_debug_hides_payloads_without_changing_raw_values_or_resume()' in added
files=[S/'CONTRIBUTING.md',S/'Cargo.toml',S/'docs/rfcs/README.md',S/'SECURITY.md',S/prod,S/test,P/prod,P/test,Q/'frozen-source-manifest.json',Q/'compiled-inputs.json',Q/'six-gates-summary.json',A/'contract-v1/CONTRACT.zh-CN.md',A/'contract-v1/oracle.json',A/'contract-v1/freeze.json']
put('input-identities.json',{'files':[{'path':str(p.relative_to(R)),'bytes':p.stat().st_size,'sha256':sha(p.read_bytes())} for p in files]})
(O/'reviewed.patch').write_text(''.join(''.join(difflib.unified_diff((P/p).read_text().splitlines(True),(S/p).read_text().splitlines(True),fromfile='69af/'+p,tofile='31a5/'+p)) for p in changed))
put('source-audit.json',{'source_tree_sha256':tree,'all_928_frozen_rows_match_actual_source_bytes':True,'parent_source_tree_sha256':parent['source_tree_sha256'],'changed_paths':changed,'canonical_relative_path_string_sorted_compiled_inputs':len(compiled),'compiled_inputs_sha256':cd,'exact_production_byte_restoration_after_removing_only_four_manual_Debug_impls_and_associated_docs_and_restoring_original_derives':restored==a,'original_Display_Error_network_parser_unit_suffix_byte_equal':True,'original_test_prefix_bytes':len(prefix),'original_test_prefix_lines':len(prefix.splitlines()),'original_test_prefix_sha256':sha(prefix),'original_test_prefix_byte_equal':True,'new_integration_test_families':1,'old_Clone_PartialEq_derives_retained':True,'McpToolCallResponse_has_no_new_Clone_PartialEq':True,'new_API_dependencies_persistence_or_resource_budget_changes':False,'reviewer_executed_build_tests_network_DB_Docker_cache_or_saved_runner':False})
put('receipt.json',{'kind':'independent Standards source review only','observed_utc':datetime.now(timezone.utc).isoformat(),'source_tree_sha256':tree,'conservative_inputs':536,'canonical_compiled_input_sha256':cd,'reviewer_authored_this_production_or_test_slice':False,'documented_standard_findings':0,'judgement_only_smell_findings':0,'remaining_findings':0,'Spec_review':'separate root-assigned reviewer; not asserted by this Standards review','documented_standards':['CONTRIBUTING.md: focused independently reviewable change; behavior changes require tests; user-visible changes require documentation and Unreleased CHANGELOG before final publication','docs/rfcs/README.md: small bug fixes/tests normally do not require a new RFC; supported contract requires lifecycle acceptance','SECURITY.md: pre-alpha best-effort; no production-support promise','Cargo.toml: workspace lints are tooling-enforced and not repeated as manual findings'],'smell_baseline_reviewed':['Mysterious Name','Duplicated Code','Feature Envy','Data Clumps','Primitive Obsession','Repeated Switches','Shotgun Surgery','Divergent Change','Speculative Generality','Message Chains','Middle Man','Refused Bequest'],'qualification_scope':'read-only fixed source Standards, not compiler, runtime, whole Git, CI, RFC, R1 or privacy qualification','source_only_scope_note':'Broader adoption documentation and CHANGELOG publication requirements remain the publication owner task; this review does not authorize omitting them.','excluded_privacy_types':['McpInputRequired','bare McpToolCall::InputRequired'],'existing_author_reds_not_rerun_or_reclassified':['v2 old-production first JSON Complete canary exit101; no claim that all eight old cases ran red','v2 fixture Clippy needless-pass-by-value101; v3 id:&Value and borrowed call repair only'],'read_only_before_after_source_exact':all((len((S/r['path']).read_bytes()),sha((S/r['path']).read_bytes()))==(r['bytes'],r['sha256']) for r in rows)})
print(json.dumps({'Standards':0,'heuristic_smells':0,'source':tree,'compiled_inputs':536,'canonical_sha256':cd,'production_reversal_byte_equal':restored==a,'old_test_prefix_exact':True}))
