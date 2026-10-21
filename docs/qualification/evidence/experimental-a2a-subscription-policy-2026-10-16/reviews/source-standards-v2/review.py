import pathlib,json,hashlib,difflib,gzip,re,datetime
B=pathlib.Path('/Users/jiawy/Documents/RustAgentFramework/.local-verification/2026-10-16');P=B/'evidence/a2a-subscription-source-standards-review-v2';S=B/'a2a-subscription-source-v5';V3=B/'a2a-subscription-source-v3';Q=B/'a2a-subscription-qualification-v5';prior=B/'evidence/a2a-subscription-source-standards-review-v1'
h=lambda q:hashlib.sha256(q.read_bytes()).hexdigest()
def rows(s):return [{'path':q.relative_to(s).as_posix(),'bytes':q.stat().st_size,'sha256':h(q)}for q in sorted(s.rglob('*'))if q.is_file()]
def canonical(r):return hashlib.sha256(json.dumps(r,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def ident(q):return {'path':str(q),'bytes':q.stat().st_size,'sha256':h(q)}
f=json.loads((Q/'frozen-source-manifest.json').read_text());c=json.loads((Q/'compiled-inputs.json').read_text());before=rows(S);old=rows(V3)
assert before==f['files'] and len(before)==930 and canonical(before)==f['source_tree_sha256']=='0e91c287e33ccf7f33c0f11e3ed0c204c2826f3373c4a6f3e701a56cd66c7bad'
assert len(c['files'])==538 and canonical(c['files'])==c['sha256']=='43e9cb48b9fe45bb8631b99fe9548db84f8e652dba2d147c8198bd04dff3a6f4'
for r in c['files']:assert (S/r['path']).stat().st_size==r['bytes'] and h(S/r['path'])==r['sha256']
a={r['path']:r for r in old};b={r['path']:r for r in before};assert set(a)==set(b)
changed=sorted(n for n in a if a[n]!=b[n]);expected=sorted(['crates/stateknot-integrations/src/a2a_server/subscription_tests.rs','crates/stateknot-integrations/tests/a2a_server_subscription.rs']);assert changed==expected
prod='crates/stateknot-integrations/src/a2a_server.rs';assert (S/prod).read_bytes()==(V3/prod).read_bytes();assert h(S/prod)=='a5997b0857ca7dfd0979faf1fda6f8394d18f89a8e941cbe0bb23eb96529b9c5'
patch='';inventory=[]
for n in changed:
 x=(V3/n).read_text();y=(S/n).read_text();patch+=''.join(difflib.unified_diff(x.splitlines(keepends=True),y.splitlines(keepends=True),fromfile='v3/'+n,tofile='v5/'+n))
 assertions=lambda z:len(re.findall(r'\bassert(?:_eq|_ne)?!\s*\(',z))
 assert assertions(x)==assertions(y)
 tests=lambda z:re.findall(r'#\[(?:tokio::test[^\]]*|test)\]\s*(?:async )?fn ([a-z_0-9]+)',z)
 assert tests(x)==tests(y)
 inventory.append({'path':n,'before':ident(V3/n),'after':ident(S/n),'assertion_macro_count':assertions(y),'test_function_names_unchanged':tests(y)})
(P/'v3-to-v5-fixture-only.patch.gz').write_bytes(gzip.compress(patch.encode(),mtime=0))
known=[]
for version,gate,expected_exit in [('v3','clippy',101),('v4','fmt',1)]:
 q=B/('a2a-subscription-qualification-'+version);r=json.loads((q/(gate+'.receipt.json')).read_text());assert r['exit_code']==expected_exit and h(q/(gate+'.log'))==r['log_sha256'];known.append({'version':version,'gate':gate,'exit_code':expected_exit,'receipt':ident(q/(gate+'.receipt.json')),'log':ident(q/(gate+'.log'))})
d={'role':'independent Standards incremental review, not fixture author;no executed product commands','observed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'parent_review_receipt':ident(prior/'receipt.json'),'parent_review_manifest':ident(prior/'artifact-manifest.json'),'source_tree_sha256':canonical(before),'source_count':930,'compiled_sha256':canonical(c['files']),'compiled_count':538,'frozen_manifest':ident(Q/'frozen-source-manifest.json'),'compiled_manifest':ident(Q/'compiled-inputs.json'),'v3_to_v5_changed_paths':changed,'other928_paths_byte_identical':True,'production_byte_identical':ident(S/prod),'test_inventory':inventory,'semantic_review':{'private_fixture_alias':'type alias names the same existing four-element tuple;no new state or runtime work','setup_capabilities':'reuse existing capability type:normal fixture streaming=true and other fields default;unsupported fixture all defaults exactly reproduces old streaming=false','borrow_fixes':'open borrows context but still clones original principal into evidence;assert_error borrows same error and keeps full normalized envelope equality','syntax_fixes':'single match to equivalent if-let,semicolon and ordinary documentation wording;no policy/assertion/timing/limit/matrix reduction','formatting':'repository2024 formatting applied after preserved v4 format mismatch'},'known_original_tooling_reds_preserved':known,'non_tooling_standards_findings':[],'actionable_smells':[],'source_before_after_exact':rows(S)==before,'qualification':'incremental static review only;final v5 gate results are independently executed and not inferred from these edits','delivery_docs':'five-doc ignored draft prepared separately;final qualification numbers and report links still require actual results/publication'}
(P/'review-basis.json').write_text(json.dumps(d,ensure_ascii=False,indent=2)+'\n');assert rows(S)==before
print('READONLY:v5source930/compiled538 exact;only2fixtures differ from reviewedv3;production+other928 byte-equal;assertions/test names retained;old Clippy101/fmt1 preserved;0 new Standards findings.')
