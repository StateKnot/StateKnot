#!/usr/bin/env python3
"""Verify source closure and actual component raw logs; no Native/CI transfer."""
from pathlib import Path
import argparse,gzip,hashlib,io,json,re,tarfile

HERE=Path(__file__).resolve().parent
sha=lambda raw:hashlib.sha256(raw).hexdigest()
canon=lambda value:json.dumps(value,sort_keys=True,separators=(',',':')).encode()
load=lambda path:json.loads(path.read_text())
TREE='31a5ff5596ead9f859a20e7b483d6b660e0577fcf47327ec0a572eb3493e6fca'
BASE='69af3a0f28d333f4e67d2b2cfcc2afc071938a6ce398527cc098d31ee8db60d8'
COMPILED='1ec97f720269934853dcb3fd66241b38a5b381fda48f4164c11f3dcc091f945c'

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository-root',type=Path)
    parser.add_argument('--workspace',action='store_true')
    args=parser.parse_args()
    repo=args.repository_root.resolve() if args.repository_root else HERE.parents[3]
    index={row['path']:row for row in load(HERE/'artifact-index.json')['artifacts']}
    def raw(name):
        name=name if name in index else name+'.gz'
        row=index[name]; stored=(HERE/name).read_bytes()
        assert (len(stored),sha(stored))==(row['stored_bytes'],row['stored_sha256']),name
        if row['storage_encoding']=='gzip':
            assert stored[4:8]==b'\0'*4 and not stored[3]&8
            data=gzip.decompress(stored)
        else:
            assert row['storage_encoding']=='identity'
            data=stored
        assert (len(data),sha(data))==(row['raw_bytes'],row['raw_sha256']),name
        return data
    obj=lambda name:json.loads(raw(name))
    for name in index: raw(name)
    refs=obj('source/base-references.json')['reused_source_only_refs']
    for row in refs:
        data=(repo/row['path']).read_bytes()
        assert (len(data),sha(data))==(row['bytes'],row['sha256'])
    old=repo/'docs/qualification/evidence/experimental-mcp-lossless-bound-state-2026-10-16'
    original_refs=load(old/'source/base-references.json')['files']
    with tarfile.open(fileobj=io.BytesIO((repo/original_refs[0]['path']).read_bytes()),mode='r:gz') as archive:
        members=archive.getmembers()
        assert len(members)==len({m.name for m in members})==923
        assert all(m.isfile() and m.name.startswith('source/') and '..' not in Path(m.name).parts for m in members)
        base={m.name[7:]:archive.extractfile(m).read() for m in members}
    base['crates/stateknot-runtime/tests/postgres/child_join_pitr.rs']=gzip.decompress((repo/original_refs[2]['path']).read_bytes())
    for change in load(old/'source/final-reconstruction.json')['changes']:
        name=change['path']
        assert (sha(base[name]) if name in base else None)==change['before_sha256']
        data=gzip.decompress((old/change['payload']).read_bytes())
        assert (len(data),sha(data))==(change['bytes'],change['after_sha256'])
        base[name]=data
    rows=lambda files,order:[{'path':v['path'],'bytes':len(files[v['path']]),'sha256':sha(files[v['path']])} for v in order]
    basem=json.loads(gzip.decompress((old/'source/final-source-manifest.json.gz').read_bytes()))
    assert len(base)==928 and rows(base,basem['files'])==basem['files'] and sha(canon(basem['files']))==BASE
    final=dict(base)
    changes=obj('source/reconstruction.json')['changes']
    assert len(changes)==2
    for change in changes:
        assert sha(base[change['path']])==change['before_sha256']
        data=raw(change['payload'])
        assert (len(data),sha(data))==(change['bytes'],change['after_sha256'])
        final[change['path']]=data
    fm=obj('source/final-source-manifest.json')
    assert len(final)==928 and rows(final,fm['files'])==fm['files'] and sha(canon(fm['files']))==TREE
    inputs=obj('source/compiled-inputs.json')
    assert len(inputs['files'])==536 and inputs['files']==sorted(inputs['files'],key=lambda row:row['path'])
    assert sha(canon(inputs['files']))==inputs['canonical_sha256']==COMPILED
    assert rows(final,inputs['files'])==inputs['files']
    # Reconstruct both historical source sets with no third payload/archive.
    test='crates/stateknot-integrations/tests/mcp_client_contract.rs'
    oldtest=final[test].decode()
    assert oldtest.count('    id: &Value,\n')==1
    oldtest=oldtest.replace('    id: &Value,\n','    id: Value,\n',1)
    needle='                let response = diagnostic_rpc_response(method, &message["id"], outcome, index);\n'
    replacement='                let response =\n                    diagnostic_rpc_response(method, message["id"].clone(), outcome, index);\n'
    assert oldtest.count(needle)==1
    oldtest=oldtest.replace(needle,replacement,1).encode()
    v2=dict(final);v2[test]=oldtest
    m2=obj('history/initial-fixed-clippy/frozen-source-manifest.json')
    assert rows(v2,m2['files'])==m2['files'] and sha(canon(m2['files']))=='99861ea4d92e931a28608411f3611144d468c0c3d514ec880589a4e0752c8774'
    red=dict(base);red[test]=oldtest
    mr=obj('history/original-canary-red/red-source-manifest.json')
    assert rows(red,mr['files'])==mr['files'] and sha(canon(mr['files']))=='262754645fd0265a31fd1da53dd3150079b1a2bf31f6fd27768cda574e263300'
    assert final[test].startswith(base[test]) and base[test].count(b'\n')==357
    prod='crates/stateknot-integrations/src/mcp_client.rs'
    restored=final[prod].decode()
    for name in ['McpNotification','McpCompleteToolResult','McpRemoteError','McpToolCallResponse']:
        start=restored.index('impl fmt::Debug for '+name+' {');end=restored.index('\n}\n',start)+3
        restored=restored[:start]+restored[end:]
    for name in ['McpNotification','McpCompleteToolResult','McpRemoteError']:
        restored=restored.replace('#[derive(Clone, PartialEq)]\npub struct '+name,'#[derive(Clone, Debug, PartialEq)]\npub struct '+name)
    restored=restored.replace('pub struct McpToolCallResponse {','#[derive(Debug)]\npub struct McpToolCallResponse {')
    normalize=lambda t:'\n'.join(line.rstrip() for line in t.splitlines() if line.strip() and not line.lstrip().startswith('///'))
    assert normalize(restored)==normalize(base[prod].decode())
    suffix='/// Closed failure from the modern stateless MCP client.'
    assert final[prod].decode().split(suffix,1)[1]==base[prod].decode().split(suffix,1)[1]
    gates=obj('execution/six-gates-summary.json')
    assert gates['state']=='passed' and gates['gate_count']==6 and gates['source_tree']==TREE
    for gate in gates['receipts']:
        receipt=obj('execution/'+gate['gate']+'.receipt.json')
        assert receipt==gate and gate['exit_code']==0 and gate['source_before_after_exact']
        assert sha(raw('execution/'+gate['gate']+'.log'))==gate['log_sha256']
    pattern=r'^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;'
    for name,expected in [('workspace',[99,2099,0,6,0]),('doctests',[10,48,0,0,0])]:
        values=[tuple(map(int,v)) for v in re.findall(pattern,raw('execution/'+name+'.log').decode(),re.M)]
        assert values and all(v[1]==0 and v[3]==0 for v in values)
        assert [len(values),sum(v[0] for v in values),sum(v[1] for v in values),sum(v[2] for v in values),sum(v[4] for v in values)]==expected
    assert b'test http_response_debug_hides_payloads_without_changing_raw_values_or_resume ... ok' in raw('execution/workspace.log')
    rr=obj('history/original-canary-red/receipt.json')
    assert rr['exit_code']==101 and rr['source_928_before_after_exact']
    assert sha(raw('history/original-canary-red/test.log'))==rr['log_sha256']=='fde49495aaf4bd84acc7f673816dbf90250c2b4a5a2a7368f7b0308205902858'
    assert b'0 passed; 1 failed; 0 ignored; 0 measured; 2 filtered out' in raw('history/original-canary-red/test.log')
    lint=obj('history/initial-fixed-clippy/clippy.receipt.json')
    assert lint['exit_code']==101 and sha(raw('history/initial-fixed-clippy/clippy.log'))==lint['log_sha256']=='96e4fed554374155e12a7496a90e635dd30e05b1bf56a29d3fd593cae26dabc0'
    # Full actual runner source is preserved, never executed by this verifier.
    assert b"'nightly':" in raw('execution/run-gate.py') and b'subprocess.Popen' in raw('execution/run-gate.py')
    assert b'2099' in raw('execution/execute-six.py') and b'run-gate.py' in raw('execution/execute-six.py')
    if args.workspace:
        for row in inputs['files']:
            data=(repo/row['path']).read_bytes()
            assert (len(data),sha(data))==(row['bytes'],row['sha256']),row['path']
    print(json.dumps({'state':'component source/raw receipt checks verified','source':TREE,'inputs':536,'source_payloads':2,'workspace_bytecheck_requested':args.workspace,'workspace_suites':99,'passed':2099,'ignored':6,'doctests':48,'Native_CI_deployment_R1_claimed':False}))

if __name__=='__main__':
    main()
