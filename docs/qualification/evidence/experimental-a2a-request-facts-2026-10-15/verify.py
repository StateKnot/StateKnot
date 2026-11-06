#!/usr/bin/env python3
"""Offline evidence verification; --workspace additionally checks actual adopted inputs/docs."""
from pathlib import Path
import argparse, collections, gzip, hashlib, io, json, posixpath, re, tarfile
HERE=Path(__file__).resolve().parent
TREE='250066800739a33390a316c7fbb40181335de67394cd9de6bb67ccb425226433'
PRODUCTION='46022f375430c2387bb73e7914229a55de62144067417053327e0c06e3397216'
SUM=re.compile(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;')
sha=lambda b:hashlib.sha256(b).hexdigest()
def read(p):return json.loads(p.read_text())
def tree_check(manifest,files):
    rows=manifest['files']; expected=manifest.get('source_tree_sha256',manifest.get('tree_sha256'))
    assert len(rows)==len(files)==manifest.get('file_count',len(rows))
    assert sha(json.dumps(rows,sort_keys=True,separators=(',',':')).encode())==expected
    assert {x['path'] for x in rows}==set(files)
    for x in rows:assert len(files[x['path']])==x['bytes'] and sha(files[x['path']])==x['sha256'],x['path']
# Apply exact unified text hunks, including reverse application and added files.
def apply(files,patch,reverse=False):
    result=dict(files);lines=patch.decode().splitlines(True);i=0
    while i<len(lines):
        assert lines[i].startswith('--- ');old=lines[i][4:].strip();i+=1
        assert lines[i].startswith('+++ ');new=lines[i][4:].strip();i+=1
        norm=lambda n:None if n=='/dev/null' else n.split('/',1)[1]
        old,new=norm(old),norm(new);source,dest=(new,old)if reverse else(old,new)
        original=result[source].decode().splitlines(True)if source else[];output=[];cursor=0
        while i<len(lines)and lines[i].startswith('@@ '):
            m=re.match(r'@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@',lines[i]);assert m
            a,ac,b,bc=int(m[1]),int(m[2]if m[2]is not None else 1),int(m[3]),int(m[4]if m[4]is not None else 1);i+=1
            position=max(0,(b if reverse else a)-1);assert position>=cursor;output.extend(original[cursor:position]);cursor=position
            consumed_old=consumed_new=0
            while consumed_old<ac or consumed_new<bc:
                line=lines[i];i+=1;kind=line[0];text=line[1:];assert kind in ' +-'
                if kind in ' -':consumed_old+=1
                if kind in ' +':consumed_new+=1
                take=kind==' 'or kind==('+'if reverse else'-');put=kind==' 'or kind==('-'if reverse else'+')
                if take:assert original[cursor]==text,(source,cursor);cursor+=1
                if put:output.append(text)
            assert consumed_old==ac and consumed_new==bc
        output.extend(original[cursor:])
        if source:del result[source]
        if dest:result[dest]=''.join(output).encode()
        else:assert output==[]
    return result

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--require-execution-record',action='store_true');p.add_argument('--workspace',action='store_true');p.add_argument('--workspace-root',type=Path);args=p.parse_args()
    manifest=read(HERE/'manifest.json')
    for x in manifest['files']:
        b=(HERE/x['path']).read_bytes();assert len(b)==x['bytes'] and sha(b)==x['sha256'],x['path']
    index=read(HERE/'artifact-index.json');items=index['artifacts'];by={x['path']:x for x in items};assert len(by)==len(items)
    def raw(n):
        x=by[n];b=(HERE/n).read_bytes();assert len(b)==x['stored_bytes']and sha(b)==x['stored_sha256']
        if x['storage_encoding']=='gzip':assert b[4:8]==b'\0'*4 and b[3]&8==0;b=gzip.decompress(b)
        else:assert x['storage_encoding']=='identity'
        assert len(b)==x['raw_bytes']and sha(b)==x['raw_sha256'];return b
    def asset(n):return raw(n)if n in by else raw(n+'.gz')if n+'.gz'in by else(HERE/n).read_bytes()
    def obj(n):return json.loads(asset(n))
    for n,x in by.items():
        raw(n);assert 'target'not in Path(x['origin']).parts
        assert Path(n).name not in ['a2a_client_contract','mcp_durable']
    source=obj('source/manifest.json');assert source['source_tree_sha256']==TREE and source['file_count']==923
    archive=asset('source/source.tar.gz');assert sha(archive)=='5a882322c32b5ec165d1a37bf84f087f0c5afc8811b805c07cc4ac7a590e75e3'
    with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz')as t:
        members=t.getmembers();assert len(members)==len({m.name for m in members})==923 and all(m.isfile()for m in members)
        assert all(m.name.startswith('source/')for m in members)
        files={m.name[len('source/'):]:t.extractfile(m).read()for m in members}
    tree_check(source,files);assert sha(files['crates/stateknot-integrations/src/a2a_client.rs'])==PRODUCTION
    parent=obj('source/delta-from922.json');assert len(parent['changes'])==4
    baseline=json.loads(gzip.decompress(asset('parent922/source-manifest.json.gz')))
    restored=apply(files,asset('source/delta-from922.patch'),reverse=True);tree_check(baseline,restored)
    assert baseline['source_tree_sha256']=='88dd9d363194af6c52809fabcd82b9c7e6cba2ee136c95ae161e40a67aebb0eb'
    prior=apply(files,asset('execution/tenant-fixture-only.patch'),reverse=True)
    tree_check(obj('history/prior59fd/source-manifest.json'),prior)
    assert obj('history/prior59fd/source-manifest.json')['source_tree_sha256']=='59fd524f0e21460a9ffc8edf7de8cc6ad4d0f95d69384c6515200675364bbea6'
    unexecuted=apply(prior,asset('history/prior59fd/import-only.patch'),reverse=True)
    tree_check(obj('history/unexecuted755ffa/after-source-manifest.json'),unexecuted)
    assert obj('history/unexecuted755ffa/status.json')['actual_checks']['compilation']=='NOT RUN'
    for n in ['history/prior59fd/gates-v1/workspace','history/prior59fd/gates-v2-loopback/workspace']:
        receipt=obj(n+'.receipt.json');log=asset(n+'.log');assert receipt['exit_code']==101 and sha(log)==receipt['log_sha256']
    assert b'PermissionDenied'in asset('history/prior59fd/gates-v1/workspace.log')
    assert b'25 passed; 1 failed;'in asset('history/prior59fd/gates-v2-loopback/workspace.log')
    assert b'unexpected A2A REST matrix request'in asset('history/prior59fd/gates-v2-loopback/workspace.log')
    inputs=read(HERE/'compiled-inputs.json');expected=[x for x in source['files']if x['path'].startswith(('crates/','.cargo/'))or x['path']in ['Cargo.toml','Cargo.lock','rust-toolchain.toml']]
    assert inputs['files']==expected and inputs['count']==len(expected)==531
    gates=obj('execution/gates-v1-loopback/receipts.json');assert [x['step']for x in gates]==['fmt','clippy','workspace','doctests','rustdoc','nightly']
    for x in gates:
        log=asset('execution/gates-v1-loopback/'+x['step']+'.log');assert x['exit_code']==0 and x['source_tree']==TREE and sha(log)==x['log_sha256']
        assert [list(v)for v in SUM.findall(log.decode())]==x['summaries']
    summaries=SUM.findall(asset('execution/gates-v1-loopback/workspace.log').decode());assert len(summaries)==98 and sum(int(x[1])for x in summaries)==2073 and sum(int(x[2])for x in summaries)==0 and sum(int(x[3])for x in summaries)==6
    assert sum(int(x[1])for x in SUM.findall(asset('execution/gates-v1-loopback/doctests.log').decode()))==45
    assert '+nightly-2026-10-07'in gates[-1]['command']
    builds=obj('execution/build-v1/summary.json');assert builds['state']=='passed'and builds['source_tree']==TREE
    events=[json.loads(line)for line in asset('execution/build-v1/fresh-build.log').decode().splitlines()if line.startswith('{')]
    for name,b in builds['binaries'].items():
        event=b['cargo_artifact'];assert event in events and event['fresh']is False
        assert event['profile']['debug_assertions']and event['profile']['overflow_checks']and event['profile']['debuginfo']==0
        assert event['target']['src_path'].endswith('/elapsed36-a2a-auth-facts-source/crates/stateknot-integrations/tests/'+name+'.rs')
        names=re.findall(r'^(\S+): test$',asset('execution/build-v1/'+name+'-list.log').decode(),re.M);assert names==b['tests']and len(names)==(26 if name=='a2a_client_contract'else 6)
        local=read(HERE/'local-binary-identities.json')[name];assert local['sha256']==local['expected_sha256']==b['sha256']
    network=obj('execution/network-v1/summary.json');nr=obj('execution/network-v1/a2a-client-contract.receipt.json');log=asset('execution/network-v1/a2a-client-contract.log')
    assert network['state']=='passed'and network['summary']==['ok','26','0','0','0','0']and nr['exit_code']==0 and sha(log)==nr['log_sha256']
    assert network['binary_sha256']==builds['binaries']['a2a_client_contract']['sha256']
    native=obj('execution/native-v1/summary.json');assert native['state']=='passed'and native['source_tree']==TREE
    for entry in native['postgres']:
        major=entry['major'];nr=obj(f'execution/native-v1/pg{major}-mcp-durable.receipt.json');log=asset(f'execution/native-v1/pg{major}-mcp-durable.log')
        assert nr['exit_code']==0 and nr['source_tree']==TREE and sha(log)==nr['log_sha256']
        assert SUM.findall(log.decode())==[('ok','6','0','0')]and entry['summary']==['ok','6','0','0','0','0']
        assert entry['binary_sha256']==builds['binaries']['mcp_durable']['sha256']
        markers=collections.defaultdict(list)
        for line in log.decode().splitlines():
            if line.startswith(('STATEKNOT_MCP_AUTHORIZATION_EVIDENCE=','STATEKNOT_A2A_AUTHORIZATION_EVIDENCE=')):
                k,v=line.split('=',1);markers[k].append(json.loads(v))
        assert dict(markers)==entry['markers']and all(len(v)==2 for v in markers.values())
        for key,rows in markers.items():
            assert {r['case']for r in rows}=={'denial','lost_response'}
            for r in rows:
                assert r['policy_calls']==1 and r['calls']==(0 if r['case']=='denial'else 1)
                if 'A2A'in key:assert r['durable_start_verified']and r['payload_from_full_input']and r['duplicate_ledger_unchanged']
                else:assert r['durable_before_authorization']
        assert entry['database']['server_version'].startswith('16.15'if major==16 else'17.11')
    assert {x['major']for x in native['postgres']}=={16,17}
    terminal=obj('execution/terminal-resource-source-check.json');assert terminal['source_tree']==TREE and terminal['file_count']==923 and terminal['production_sha256']==PRODUCTION and terminal['active_matching_processes']==[]
    for x in terminal['stage_summaries']:assert sha(asset('execution/'+x['path']))==x['sha256']
    observed=obj('execution/compiler-observed-workspace-inputs.json');assert observed['observed_workspace_file_count']==len(observed['files'])==215 and len(observed['artifacts'])==6
    for x in observed['files']:assert len(files[x['path']])==x['bytes']and sha(files[x['path']])==x['sha256']
    for x in observed['artifacts']:
        dep=asset('execution/compiler-observed-dep-info/'+Path(x['raw_dep_info']).name);assert sha(dep)==x['raw_sha256']
        tokens=dep.decode().splitlines()[0].split(': ',1)[1].split()
        normalized={posixpath.normpath(n)for n in tokens}
        normalized={n[n.index('crates/'):]for n in normalized if'crates/'in n}
        assert normalized==set(x['observed_source_inputs'])
    for review in ['a2a-auth-facts-production-review-v2','a2a-auth-facts-docs-independent-review-v2']:
        r=obj('reviews/'+review+'/review.json');assert r['standards']['count']==r['spec']['count']==0
        assert r.get('candidate_sha256',r.get('production_candidate_sha256'))==PRODUCTION
    docs=obj('adoption-docs/manifest.json');assert len(docs['files'])==5
    for x in docs['files']:assert len(asset('adoption-docs/'+x['path']))==x['bytes']and sha(asset('adoption-docs/'+x['path']))==x['sha256']
    adoption=obj('adoption/actual-main.json');assert adoption['actual_main_compiled_inputs_exact']==531 and len(adoption['code_files_adopted'])==4 and len(adoption['docs_adopted'])==5 and not adoption['new_PITR_cleanup_candidate_included']
    if args.require_execution_record:assert (HERE/'seal.json').is_file()
    sealed=(HERE/'seal.json').is_file()
    if sealed:
        seal=read(HERE/'seal.json');assert sha((HERE/'manifest.json').read_bytes())==seal['manifest_sha256']
        for n,h in seal['reports'].items():assert sha((HERE.parents[1]/n).read_bytes())==h
        profile=read(HERE.parents[1]/'experimental-a2a-request-facts-2026-10-15.json')
        assert profile['exact_execution_record_passed']is True and profile['overall_production_qualification']is False
        assert profile['static']['workspace_passed']==2073 and profile['network']['passed']==26 and profile['durable']['passed_per_major']==6
        assert profile['claim_limits']['full_CI_passed']is False and profile['claim_limits']['R1_complete']is False
    workspace=None
    if args.workspace:
        workspace=args.workspace_root or next((p for p in HERE.parents if(p/'.git').exists()),None);assert workspace is not None
        assert all((workspace/x['path']).is_file()and sha((workspace/x['path']).read_bytes())==x['sha256']for x in expected),'actual workspace compiled bytes differ'
        actual=set()
        for directory in ['crates','.cargo']:
            q=workspace/directory
            if q.exists():actual.update(p.relative_to(workspace).as_posix()for p in q.rglob('*')if p.is_file())
        actual.update(n for n in ['Cargo.toml','Cargo.lock','rust-toolchain.toml']if(workspace/n).is_file());assert actual=={x['path']for x in expected}
        assert all(sha((workspace/x['path']).read_bytes())==x['sha256']for x in docs['files']),'actual adopted docs differ'
    print(json.dumps({'evidence_integrity':'passed','sealed_exact_execution_record':sealed,'artifact_count':len(items),'source_files':923,'compiled_input_files':531,'compiler_emitted_workspace_source_entries':215,'static_workspace_suites':98,'static_workspace_passed':2073,'original_ignored':6,'doctests_passed':45,'network_passed':26,'durable_passed_per_major':6,'workspace_checked':workspace is not None,'overall_production_qualification':False},indent=2))
if __name__=='__main__':main()
