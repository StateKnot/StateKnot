import datetime,difflib,gzip,hashlib,json,pathlib,re
R=pathlib.Path('/Users/jiawy/Documents/RustAgentFramework'); B=R/'.local-verification/2026-10-16'; P=B/'evidence/a2a-subscription-source-standards-review-v1'; S=B/'a2a-subscription-source-v3'; OLD=B/'mcp-http-debug-source-v3'; Q=B/'a2a-subscription-qualification-v3'
h=lambda b:hashlib.sha256(b).hexdigest()
def file(q):return {'path':str(q),'bytes':q.stat().st_size,'sha256':h(q.read_bytes())}
def rows(root):return [{'path':q.relative_to(root).as_posix(),'bytes':q.stat().st_size,'sha256':h(q.read_bytes())}for q in sorted(root.rglob('*')) if q.is_file()]
def canonical(r):return h(json.dumps(r,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode())
def save(n,x):(P/n).write_text(json.dumps(x,ensure_ascii=False,indent=2)+'\n')
f=json.loads((Q/'frozen-source-manifest.json').read_text());c=json.loads((Q/'compiled-inputs.json').read_text()); before=rows(S);old=rows(OLD)
assert len(before)==930 and before==f['files'] and canonical(before)==f['source_tree_sha256']=='d994771b87e580ef6b15e28dc101e35e52c2a97f9b9984ded83c060bc7968b56'
assert len(old)==928 and canonical(old)=='31a5ff5596ead9f859a20e7b483d6b660e0577fcf47327ec0a572eb3493e6fca'
assert len(c['files'])==538 and canonical(c['files'])==c['sha256']=='c04f7bf97b13395753f4a704da1e0c8f992403d00ceb8cf5c93ba2aa59ae9cd5'
for r in c['files']:assert file(S/r['path'])['sha256']==r['sha256'] and (S/r['path']).stat().st_size==r['bytes']
a={r['path']:r for r in old};b={r['path']:r for r in before}; added=sorted(b.keys()-a.keys());changed=sorted(n for n in a.keys()&b.keys() if a[n]!=b[n]);removed=sorted(a.keys()-b.keys())
prod='crates/stateknot-integrations/src/a2a_server.rs';unit='crates/stateknot-integrations/src/a2a_server/subscription_tests.rs';tcp='crates/stateknot-integrations/tests/a2a_server_subscription.rs'
assert changed==[prod] and added==sorted([unit,tcp]) and not removed
base_compiled=[{'path':r['path'],'bytes':(OLD/r['path']).stat().st_size,'sha256':h((OLD/r['path']).read_bytes())}for r in c['files'] if (OLD/r['path']).exists()]
assert len(base_compiled)==536 and canonical(base_compiled)=='1ec97f720269934853dcb3fd66241b38a5b381fda48f4164c11f3dcc091f945c'
patch=''
for n in changed+added:
 x=(OLD/n).read_text().splitlines(keepends=True) if n in a else [];y=(S/n).read_text().splitlines(keepends=True)
 patch+=''.join(difflib.unified_diff(x,y,fromfile='baseline/'+n if n in a else '/dev/null',tofile='candidate/'+n))
(P/'reviewed-source.patch.gz').write_bytes(gzip.compress(patch.encode(),mtime=0))
def method(text,impl,name):
 text=text[text.index(impl):];start=re.search(r'(?m)^    (?:async )?fn '+name+r'(?:<|\()',text).start(); tail=text[start:];ends=[x.start() for x in re.finditer(r'(?m)^    (?:async )?fn |^}',tail) if x.start()>0];return tail[:min(ends)]
orig=(OLD/prod).read_text();new=(S/prod).read_text();equal=[]
for name in ['prepare','unary','stream','bounded_stream']:
 x=method(orig,'impl A2aSdkRequestHandler {',name);y=method(new,'impl A2aSdkRequestHandler {',name);assert x==y,name;equal.append({'method':name,'sha256':h(x.encode()),'bytes':len(x.encode())})
for name in ['send_message','send_streaming_message','get_task','list_tasks','cancel_task','create_push_config','get_push_config','list_push_configs','delete_push_config','get_extended_agent_card']:
 x=method(orig,'impl RequestHandler for A2aSdkRequestHandler {',name);y=method(new,'impl RequestHandler for A2aSdkRequestHandler {',name);assert x==y,name;equal.append({'method':name,'sha256':h(x.encode()),'bytes':len(x.encode())})
assert (B/'a2a-subscription-source-v2'/prod).read_bytes()==(S/prod).read_bytes()
assert (B/'a2a-subscription-source-v2'/unit).read_bytes()==(S/unit).read_bytes()
clippy=json.loads((Q/'clippy.receipt.json').read_text());log=(Q/'clippy.log').read_bytes();assert clippy['exit_code']==101 and h(log)==clippy['log_sha256']=='9152d15ed966317e7173b8e82a8048eaff4f5f8bcc3eb3fec24ccebab6d77e1a'
errors=re.findall(r'(?m)^error: (?!could not compile)(.*)',log.decode());assert len(errors)==7
standards=[R/'CONTRIBUTING.md',R/'docs/rfcs/README.md',R/'docs/rfcs/0025-protocol-identity-and-authority.md',R/'docs/rfcs/0025-protocol-identity-and-authority.zh-CN.md',pathlib.Path('/Users/jiawy/.codex/plugins/cache/mattpocock/mattpocock-skills/1.3.1/skills/engineering/code-review/SKILL.md'),pathlib.Path('/Users/jiawy/.codex/plugins/cache/mattpocock/mattpocock-skills/1.3.1/skills/engineering/codebase-design/SKILL.md')]
contract=B/'mcp-adjacent-a2a-subscription-contract-v1'
save('review-basis.json',{'observed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'role':'independent non-author Standards source review; Spec separately owned','standards':[file(q)for q in standards],'contract':[file(contract/n)for n in ['CONTRACT.zh-CN.md','acceptance-matrix.json','public-interface.rs.txt']],'candidate':{'source_tree_sha256':canonical(before),'source_count':930,'compiled_count':538,'compiled_sha256':canonical(c['files']),'manifest':file(Q/'frozen-source-manifest.json'),'compiled_manifest':file(Q/'compiled-inputs.json')},'baseline':{'source_count':928,'source_tree_sha256':canonical(old),'compiled_count':536,'compiled_sha256':canonical(base_compiled)},'exact_source_delta':{'changed':changed,'added':added,'removed':removed,'all_other927_paths_unchanged':True},'original_methods_byte_equal':equal,'v2_to_v3_production_and_private_test_byte_equal':True,'reviewed_changed_files':[file(S/n) for n in changed+added],'known_tooling_failure':{'receipt':file(Q/'clippy.receipt.json'),'log':file(Q/'clippy.log'),'exit_code':101,'diagnostic_count':7,'diagnostics':errors,'diagnostics_only_test_paths':True,'not_a_full_strict_gate_pass':True},'source_before_after_exact':rows(S)==before,'new_code_standard_violations_excluding_tooling_findings':[],'actionable_non_tooling_smells':[],'known_delivery_obligations':['CONTRIBUTING requires user-visible guide documentation and Unreleased CHANGELOG before adoption.','RFC25 remains Draft; append experimental Interface and precise scope without declaring stable supported authority/backend.','Current-host-policy and bounded nonblocking callbacks; no bearer reauthentication, after-yield retraction, periodic guarantee or durable backend claim.'],'qualification_limit':'source review only; known v3 strict Clippy101 retained; v4 incremental review and final gates remain separate'})
assert rows(S)==before
print('READONLY Standards: frozen930/compiled538 exact;delta1production+2tests;14 original methods byte-equal;known fixtureClippy101 retained;0 additional non-tooling findings.')
