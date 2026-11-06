from pathlib import Path
from datetime import datetime,timezone
import subprocess,json,hashlib,difflib,re,os
R=Path('/Users/jiawy/Documents/RustAgentFramework'); B=R/'.local-verification/2026-10-16'; D=B/'mcp-http-debug-adoption-docs-v1'; Q=B/'mcp-http-debug-qualification-v3'; O=B/'evidence/mcp-http-debug-docs-standards-review-v1'; HEAD='8016b74e8047fda51f60c735d477d735a897cb2f'
sha=lambda d:hashlib.sha256(d).hexdigest(); put=lambda n,v:(O/n).write_text(json.dumps(v,indent=2,ensure_ascii=False)+'\n')
m=json.loads((D/'manifest.json').read_text()); assert sha((D/'manifest.json').read_bytes())==json.loads((D/'freeze.json').read_text())['manifest_sha256']
for row in m['files']:
 p=D/row['path'];assert p.is_file() and (len(p.read_bytes()),sha(p.read_bytes()))==(row['bytes'],row['sha256'])
source=json.loads((Q/'frozen-source-manifest.json').read_text()); assert source['source_tree_sha256']=='31a5ff5596ead9f859a20e7b483d6b660e0577fcf47327ec0a572eb3493e6fca'
rows=json.loads((D/'candidate-documents.json').read_text())['files']; evidence=[]
for row in rows:
 before=(D/'baseline'/row['path']).read_bytes(); after=(D/'candidate'/row['path']).read_bytes(); history=subprocess.check_output(['git','show',HEAD+':'+row['path']],cwd=R)
 assert before==history and sha(before)==row['baseline_sha256'] and sha(after)==row['sha256']
 diff=difflib.SequenceMatcher(a=before.splitlines(keepends=True),b=after.splitlines(keepends=True),autojunk=False); ops=diff.get_opcodes();assert all(tag in ['equal','insert'] for tag,*_ in ops)
 fences=lambda raw:re.findall(rb'^```[^\n]*\n.*?^```[ \t]*$',raw,re.M|re.S)
 assert fences(before)==fences(after)
 evidence.append({'path':row['path'],'baseline_git_object_byte_equal':True,'baseline_sha256':sha(before),'candidate_sha256':sha(after),'insertions_only':True,'original_code_fences_preserved':True,'original_code_fence_count':len(fences(before)),'added_lines':sum(j2-j1 for tag,i1,i2,j1,j2 in ops if tag=='insert')})
pattern=re.compile(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;')
counts={}
for name,expected in [('workspace',(99,2099,0,6,0)),('doctests',(10,48,0,0,0))]:
 data=(Q/(name+'.log')).read_bytes();values=pattern.findall(data.decode()); assert all(v[0]=='ok' and v[4]=='0' for v in values)
 observed=(len(values),sum(int(v[1]) for v in values),sum(int(v[2]) for v in values),sum(int(v[3]) for v in values),sum(int(v[5]) for v in values)); assert observed==expected
 counts[name]={'raw_sha256':sha(data),'counts':observed}
assert b'http_response_debug_hides_payloads_without_changing_raw_values_or_resume ... ok' in (Q/'workspace.log').read_bytes()
links=[];reserved=set(json.loads((D/'link-check.json').read_text())['reserved_reports'])
for row in rows:
 text=(D/'candidate'/row['path']).read_text()
 for target in re.findall(r'\[[^\]]*\]\(([^)]+)\)',text):
  if '://' in target or target.startswith('#'):continue
  resolved=Path(os.path.normpath(str(Path(row['path']).parent/target.split('#')[0]))).as_posix();kind='reserved-new-qualification-report' if resolved in reserved else 'existing-main'
  assert resolved in reserved or (R/resolved).is_file(),resolved;links.append({'document':row['path'],'target':target,'resolved':resolved,'kind':kind})
assert len(links)==17 and len(reserved)==3
issue='Earlier bound-state PostgreSQL15-case results';text=(D/'candidate/docs/mcp-client.md').read_text(); assert issue in text; line=text[:text.index(issue)].count('\n')+1
put('baseline-and-facts-audit.json',{'fixed_baseline':HEAD,'candidate_manifest_sha256':sha((D/'manifest.json').read_bytes()),'candidate_freeze_sha256':sha((D/'freeze.json').read_bytes()),'documents':evidence,'source_tree':source['source_tree_sha256'],'compiled_inputs':536,'canonical_compiled_sha256':'1ec97f720269934853dcb3fd66241b38a5b381fda48f4164c11f3dcc091f945c','independent_raw_summary_counts':counts,'new_TCP_family_actual_ok_line':True,'old_original_boundstate_Changelog_paragraph_preserved_by_insertions_only':True,'links':links,'reserved_reports_exist_claimed':False,'reserved_reports_must_be_supplied_before_publication':True,'reviewer_authored_documents':False,'executed_Cargo_network_DB_Docker_cache_or_saved_runner':False})
put('findings-v1.json',{'findings':[{'priority':'P2','classification':'wording/technical identity ambiguity; no production-code defect','file':'docs/mcp-client.md','line':line,'quoted_text':issue,'problem':'PostgreSQL15-case reads as PostgreSQL major15. Actual earlier bound-state evidence is PostgreSQL16/17 with15 tests per major.','minimal_fix':'Earlier bound-state PostgreSQL 16/17 full 15-test serial-suite results','paired_CN_suggestion':'此前bound-state PostgreSQL16/17每版15项','basis':'CONTRIBUTING.md Chinese terminology rule: preserve technical meaning and do not broaden implementation/recovery/production qualification; review expectation of accurate compatibility and scope.','status':'open pending separate immutable docs revision'}],'other_documented_standard_findings':0,'heuristic_smell_findings':0})
print(json.dumps({'docs':3,'baseline_exact':True,'insertions_only':True,'old_code_fences_exact':True,'counts':counts,'links':17,'reserved_unique':3,'remaining_wording_findings':1,'line':line}))
