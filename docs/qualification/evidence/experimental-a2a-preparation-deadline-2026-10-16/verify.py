#!/usr/bin/env python3
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0
"""Pure offline evidence/source verifier; never executes preserved gate runners."""
import argparse
import gzip
import hashlib
import io
import json
import tarfile
from pathlib import Path
import re
from reconstruct import apply_patch, reconstruct_base

HERE=Path(__file__).resolve().parent
TREE='9a98cd929c10698f8cb866b99b3fd00a9b0f69dd840a68da1d01f5b30817f49d'
COMPILED='c3c5ed5bb8f102d9b1b31bf1afbb21e75e1e05d4e2b37d523f4ba4ca2bc80edc'
GATES=('fmt','clippy','workspace','doctests','rustdoc','nightly')
sha=lambda value:hashlib.sha256(value).hexdigest()
canon=lambda value:json.dumps(value,sort_keys=True,separators=(',',':')).encode()

def safe(value):
    path=Path(value)
    assert value and not path.is_absolute() and '..' not in path.parts,value
    return path

def summaries(raw):
    return [tuple(map(int,row)) for row in re.findall(rb'^test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;',raw,re.M)]

def test_names(data):
    return [m.decode() for m in re.findall(rb'#\[(?:tokio::test[^\n]*|test)\]\n(?:async )?fn (\w+)',data)]

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository-root',type=Path,help='repository containing reused published source references')
    parser.add_argument('--workspace',action='store_true',help='bytecheck only the540 supplied compilation inputs, not wholeGit or compiler read closure')
    parser.add_argument('--workspace-root',type=Path)
    args=parser.parse_args()
    repo=args.repository_root.resolve() if args.repository_root else HERE.parents[3]
    workspace=args.workspace_root.resolve() if args.workspace_root else repo
    seal=json.loads((HERE/'seal.json').read_bytes())
    manifest_bytes=(HERE/'manifest.json').read_bytes()
    assert sha(manifest_bytes)==seal['manifest_sha256']
    manifest=json.loads(manifest_bytes)
    rows=manifest['artifacts']
    assert len({row['path'] for row in rows})==len(rows)
    for row in rows:
        path=HERE/safe(row['path']);assert not path.is_symlink()
        data=path.read_bytes();assert (len(data),sha(data))==(row['bytes'],row['sha256']),row['path']
    actual={p.relative_to(HERE).as_posix() for p in HERE.rglob('*') if p.is_file()}
    assert actual=={row['path'] for row in rows}|{'manifest.json','seal.json'}
    index=json.loads((HERE/'artifact-index.json').read_bytes())['artifacts']
    index={row['path']:row for row in index}
    assert len(index)==len(json.loads((HERE/'artifact-index.json').read_bytes())['artifacts'])
    def raw(name):
        if name not in index:name+='.gz'
        row=index[name];data=(HERE/safe(name)).read_bytes()
        assert (len(data),sha(data))==(row['stored_bytes'],row['stored_sha256'])
        if row['storage_encoding']=='gzip':
            assert data[4:8]==b'\0'*4 and not data[3]&8
            data=gzip.decompress(data)
        else:assert row['storage_encoding']=='identity'
        assert (len(data),sha(data))==(row['raw_bytes'],row['raw_sha256'])
        return data
    obj=lambda name:json.loads(raw(name) if name in index or name+'.gz' in index else (HERE/safe(name)).read_bytes())
    for name in index:raw(name)
    for row in obj('source/references.json')['files']:
        data=(repo/safe(row['path'])).read_bytes()
        assert (len(data),sha(data))==(row['bytes'],row['sha256']),row['path']
    # Reuse source bytes only. No prior execution/Native qualification transfers.
    base=reconstruct_base(repo)
    prior=repo/'docs/qualification/evidence/experimental-a2a-subscription-policy-2026-10-16'
    for change in json.loads((prior/'source/reconstruction.json').read_bytes())['changes']:
        assert (sha(base[change['path']]) if change['path'] in base else None)==change['before_sha256']
        data=gzip.decompress((prior/safe(change['payload'])).read_bytes())
        assert (len(data),sha(data))==(change['bytes'],change['after_sha256'])
        base[change['path']]=data
    old_inputs=json.loads(gzip.decompress((prior/'source/compiled-inputs.json.gz').read_bytes()))
    assert old_inputs['count']==538 and sha(canon(old_inputs['files']))==old_inputs['sha256']=='43e9cb48b9fe45bb8631b99fe9548db84f8e652dba2d147c8198bd04dff3a6f4'
    supplied={row['path']:base[row['path']] for row in old_inputs['files']}
    for row in old_inputs['files']:
        assert (len(supplied[row['path']]),sha(supplied[row['path']]))==(row['bytes'],row['sha256'])
    final=dict(supplied)
    changes=obj('source/reconstruction.json')['changes'];assert len(changes)==4
    for change in changes:
        assert (sha(supplied[change['path']]) if change['path'] in supplied else None)==change['before_sha256']
        data=raw(change['payload']);assert (len(data),sha(data))==(change['bytes'],change['after_sha256'])
        final[change['path']]=data
    assert apply_patch(supplied,raw('source/candidate.patch'))==final
    def inputs(files,document):
        rows=document['files'];assert document['count']==len(rows)==len(files)==540
        assert rows==sorted(rows,key=lambda row:row['path'])
        assert sha(canon(rows))==document['sha256']
        assert [{'path':row['path'],'bytes':len(files[row['path']]),'sha256':sha(files[row['path']])} for row in rows]==rows
    final_inputs=obj('source/compiled-inputs.json');inputs(final,final_inputs)
    assert final_inputs['sha256']==COMPILED
    v4=apply_patch(final,raw('original/q5/fixture-clippy-and-target-owner-fix.patch.gz'),reverse=True)
    v3=apply_patch(v4,raw('original/q4/clippy-helper-and-owned-callback-oracle.patch.gz'),reverse=True)
    v2=apply_patch(v3,raw('original/q3/fixture-phase-and-sdk-methods-fix.patch.gz'),reverse=True)
    v1=apply_patch(v2,raw('original/q2/fixture-oracle-fix.patch.gz'),reverse=True)
    red=apply_patch(v1,raw('source/old-product-red.patch'))
    historical={'red':red,'q1':v1,'q2':v2,'q3':v3,'q4':v4,'q5':final}
    for label,files in historical.items():
        inputs(files,obj('original/'+label+'/compiled-inputs.json'))
        fm=obj('original/'+label+'/frozen-source-manifest.json')
        assert fm['count']==len(fm['files'])==931
        assert len({row['path'] for row in fm['files']})==931
        assert sha(canon(fm['files']))==fm['source_tree_sha256']
        by_path={row['path']:row for row in fm['files']}
        for name,data in files.items():assert by_path[name]=={'path':name,'bytes':len(data),'sha256':sha(data)}
    assert obj('original/q5/frozen-source-manifest.json')['source_tree_sha256']==TREE
    prod='crates/stateknot-integrations/src/a2a_server.rs'
    # Old-product red changes only a cfg(test) hook relative to the prior production file.
    hook=b'#[cfg(test)]\n#[path = "a2a_server/preparation_tests.rs"]\nmod preparation_tests;\n\n'
    assert red[prod].count(hook)==1 and red[prod].replace(hook,b'',1)==base[prod]
    assert v1[prod]==v2[prod]==v3[prod] and v4[prod]==final[prod]
    old_parent='crates/stateknot-integrations/tests/a2a_server_subscription.rs'
    marker=b'async fn real_server_subscription_authorization_matrix_covers_both_bindings()'
    old_body=base[old_parent][base[old_parent].index(marker):].strip()
    new_body=final[old_parent][final[old_parent].index(marker):final[old_parent].index(b'#[path = "a2a_server_subscription/initial_authority.rs"]')].strip()
    assert old_body==new_body
    # Original source review manifests are checked in memory only.
    delivery=obj('reviews/delivery.json')
    with tarfile.open(fileobj=io.BytesIO(raw(delivery['archive'])),mode='r:') as archive:
        members=archive.getmembers();assert len(members)==len(delivery['files'])
        assert len({m.name for m in members})==len(members)
        assert all(m.isfile() and not m.issym() and not m.islnk() for m in members)
        original={safe(m.name).as_posix():archive.extractfile(m).read() for m in members}
    for row in delivery['files']:assert (len(original[row['path']]),sha(original[row['path']]))==(row['bytes'],row['sha256'])
    for group in {name.split('/')[0] for name in original}:
        document=json.loads(original[group+'/manifest.json'])
        for row in (document if isinstance(document,list) else document.get('files',document.get('artifacts',[]))):
            data=original[group+'/'+row['path']];assert (len(data),sha(data))==(row['bytes'],row['sha256'])
    assert raw('contract/input-corrections.md')==original['source-spec-v1/input-corrections.md']
    review=json.loads(original['source-spec-v1/receipt.json'])
    assert review['actionable_Spec_findings']==0 and review['source931']==TREE and review['compiled540']==COMPILED
    review=json.loads(original['source-standards-v5/receipt.json'])
    assert review['new_or_remaining_findings']==0 and review['source_tree_sha256']==TREE and review['compiled_canonical_sha256']==COMPILED
    commands=obj('execution/commands.json')
    receipt_count=0
    for name in index:
        if not name.startswith('original/') or not name.endswith('.receipt.json'):continue
        receipt=obj(name)
        if 'gate' not in receipt:continue
        label=name.split('/')[1];prefix='original/'+label+'/'
        assert receipt['argv']==commands[receipt['gate']]
        assert receipt['runner_sha256']==sha(raw(prefix+'run-gate.py'))
        assert receipt['source_tree']==obj(prefix+'frozen-source-manifest.json')['source_tree_sha256']
        assert receipt['source_before_after_exact'] is True and receipt['native_database_execution'] is False
        assert sha(raw(prefix+receipt['gate']+'.log'))==receipt['log_sha256']
        env=receipt['environment_allowlist']
        assert env['CARGO_INCREMENTAL']=='0' and env['RUSTFLAGS']==env['RUSTDOCFLAGS']=='-D warnings'
        for profile in ('DEV','TEST'):
            assert env['CARGO_PROFILE_'+profile+'_OPT_LEVEL']==env['CARGO_PROFILE_'+profile+'_DEBUG']=='0'
            assert env['CARGO_PROFILE_'+profile+'_DEBUG_ASSERTIONS']==env['CARGO_PROFILE_'+profile+'_OVERFLOW_CHECKS']=='true'
        assert not any(receipt['override_presence'].values())
        receipt_count+=1
    driver=obj('original/q5/six-gates-driver.receipt.json')
    assert driver['six_gates_all_passed'] is True
    assert driver['actual_results']==[{'gate':name,'exit_code':0} for name in GATES]
    assert driver['driver_sha256']==sha(raw('original/q5/run-six-gates.py'))
    assert driver['runner_sha256']==sha(raw('original/q5/run-gate.py'))
    for name in GATES:assert obj('original/q5/'+name+'.receipt.json')['exit_code']==0
    red_log=raw('original/red/red-policy.log')
    assert obj('original/red/red-policy.receipt.json')['exit_code']==101
    assert summaries(red_log)==[(0,1,0)] and b'left: 200' in red_log and b'right: 500' in red_log
    assert obj('original/q1/tcp.receipt.json')['exit_code']==101
    assert obj('original/q2/tcp.receipt.json')['exit_code']==101
    assert obj('original/q3/clippy.receipt.json')['exit_code']==101
    assert obj('original/q4/clippy.receipt.json')['exit_code']==101
    workspace_raw=raw('original/q5/workspace.log');doc_raw=raw('original/q5/doctests.log')
    ws=summaries(workspace_raw);docs=summaries(doc_raw)
    assert ws and docs and all(row[1]==0 for row in ws+docs)
    profile=obj('execution-profile.json')
    assert profile['workspace']=={'suites':len(ws),'passed':sum(r[0] for r in ws),'failed':sum(r[1] for r in ws),'ignored':sum(r[2] for r in ws)}
    assert profile['doctests']=={'groups':len(docs),'passed':sum(r[0] for r in docs),'failed':sum(r[1] for r in docs),'ignored':sum(r[2] for r in docs)}
    for path,prefix in [('crates/stateknot-integrations/src/a2a_server/preparation_tests.rs','a2a_server::preparation_tests::'),('crates/stateknot-integrations/tests/a2a_server_subscription/initial_authority.rs','initial_authority::'),('crates/stateknot-integrations/src/a2a_server/subscription_tests.rs','a2a_server::subscription_tests::')]:
        names=test_names(final[path]);assert names
        for name in names:assert ('test '+prefix+name+' ... ok').encode() in workspace_raw,(prefix,name)
    assert b'test real_server_subscription_authorization_matrix_covers_both_bindings ... ok' in workspace_raw
    final_summary=obj('original/q5/summary.json')
    assert final_summary['source_tree']==TREE and final_summary['compiled_sha256']==COMPILED and final_summary['six_gates_passed'] is True
    assert final_summary['workspace']=={'suite_summaries':len(ws),'passed':sum(r[0] for r in ws),'failed':0,'ignored':sum(r[2] for r in ws)}
    assert final_summary['doctests']=={'suite_summaries':len(docs),'passed':sum(r[0] for r in docs),'failed':0,'ignored':sum(r[2] for r in docs)}
    for gate in GATES:
        assert final_summary['gates'][gate]['receipt_sha256']==sha(raw('original/q5/'+gate+'.receipt.json'))
        assert final_summary['gates'][gate]['raw_log_sha256']==sha(raw('original/q5/'+gate+'.log'))
    assert profile['new_private_unit_tests']==3 and profile['new_integration_families']==8
    assert profile['integration_family_total_including_retained']==9
    assert all(profile[key] is False for key in ('Native_database_qualified','new_IdP_qualified','durable_task_backend_qualified','fullCI_qualified','cold30m_qualified','stdio_qualified','R1_complete','independent_publication_review_in_this_seal'))
    for binding in obj('report-bindings.json')['files']:
        data=(HERE.parents[3]/safe(binding['path'])).read_bytes()
        assert (len(data),sha(data))==(binding['bytes'],binding['sha256'])
    if args.workspace:
        for row in final_inputs['files']:
            path=workspace/safe(row['path']);assert not path.is_symlink()
            data=path.read_bytes();assert (len(data),sha(data))==(row['bytes'],row['sha256']),row['path']
    print(json.dumps({'status':'passed','offline':True,'artifacts':len(rows),'source_row_binding_count':931,'reconstructed_supplied_compilation_inputs':540,'source_tree':TREE,'compiled_inputs_sha256':COMPILED,'gate_receipts_verified':receipt_count,'six_final_gates_exit0':True,'workspace':profile['workspace'],'doctests':profile['doctests'],'workspace_bytecheck':bool(args.workspace),'Native_database_qualification':False,'whole_Git_or_compiler_read_closure':False},sort_keys=True))

if __name__=='__main__':main()
