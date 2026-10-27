#!/usr/bin/env python3
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0
"""Offline hash/member verifier. Never runs archived reviewers or gate runners."""
import argparse,gzip,hashlib,io,json,tarfile
from pathlib import Path
HERE=Path(__file__).resolve().parent
sha=lambda b:hashlib.sha256(b).hexdigest()
def safe(name):
    path=Path(name);assert name and not path.is_absolute() and '..' not in path.parts,name
    return path

def members(data,mode):
    with tarfile.open(fileobj=io.BytesIO(data),mode=mode) as archive:
        entries=archive.getmembers();assert len({m.name for m in entries})==len(entries)
        assert all(m.isfile() and not m.issym() and not m.islnk() for m in entries)
        return {safe(m.name).as_posix():archive.extractfile(m).read() for m in entries}

def original_manifests(original):
    count=0
    for name,data in original.items():
        if Path(name).name not in ('manifest.json','artifact-manifest.json'):continue
        obj=json.loads(data)
        rows=obj if isinstance(obj,list) else obj.get('files',obj.get('artifacts',[]))
        assert isinstance(rows,list)
        for row in rows:
            child=(Path(name).parent/safe(row['path'])).as_posix()
            raw=original[child];assert (len(raw),sha(raw))==(row['bytes'],row['sha256']),child
        count+=1
    return count

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repository-root',type=Path);args=p.parse_args()
    repo=args.repository_root.resolve() if args.repository_root else HERE.parents[3]
    manifest_data=(HERE/'manifest.json').read_bytes();seal=json.loads((HERE/'seal.json').read_bytes())
    assert sha(manifest_data)==seal['manifest_sha256']
    manifest=json.loads(manifest_data);expected=set()
    for row in manifest['files']:
        path=HERE/safe(row['path']);assert not path.is_symlink();data=path.read_bytes()
        assert (len(data),sha(data))==(row['bytes'],row['sha256']),row['path'];expected.add(row['path'])
    assert {f.relative_to(HERE).as_posix() for f in HERE.rglob('*') if f.is_file()}==expected|{'manifest.json','seal.json'}
    d=json.loads((HERE/'delivery.json').read_bytes())
    raw=(HERE/d['review_archive']['path']).read_bytes()
    assert raw[4:8]==b'\0'*4 and not raw[3]&8
    assert sha(raw)==d['review_archive']['stored_sha256']
    archive=gzip.decompress(raw);assert sha(archive)==d['review_archive']['raw_tar_sha256']
    original=members(archive,'r:')
    assert len(original)==len(d['archived_originals'])
    for row in d['archived_originals']:
        data=original[row['path']];assert (len(data),sha(data))==(row['bytes'],row['sha256'])
    checked=original_manifests(original)
    assert {key.split('/')[0] for key in original}=={group['group'] for group in d['groups']+d['nonreview_archive_groups']}
    receipts={group['group']:json.loads(original[group['group']+'/receipt.json']) for group in d['groups']}
    assert receipts['docs-preflight-v1']['findings']==1
    assert receipts['docs-preflight-v2']['prior_finding_closed'] is True and receipts['docs-preflight-v2']['remaining_findings']==0
    assert receipts['final-docs-spec-v3']['remaining_findings']==0
    assert receipts['publication-standards-v1']['remaining_findings']==0
    assert receipts['publication-standards-v1']['candidate_manifest_sha256']==d['base_publication']['prior_v3_candidate_manifest_sha256']
    # Final publication Spec receipt shape is retained, never rewritten.
    prior=receipts['publication-spec-v1'];spec=receipts['publication-spec-v2']
    assert prior==d['prior_full_publication_spec_receipt'] and prior['candidate_manifest_sha256']==d['base_publication']['prior_v3_candidate_manifest_sha256']
    assert spec==d['final_publication_spec_receipt'] and spec['candidate_manifest_sha256']==d['base_publication']['candidate_manifest_sha256']
    assert d['final_publication_spec_findings']==spec['remaining_findings']==0
    assert spec['prior_full_v3_Spec_receipt_sha256']==sha(original['publication-spec-v1/receipt.json'])
    assert receipts['publication-standards-v2']['candidate_manifest_sha256']==d['base_publication']['candidate_manifest_sha256']
    assert receipts['publication-standards-v2']['remaining_findings']==0
    assert any('attempt1' in key for key in original if key.startswith('publication-standards-v1/'))
    git=json.loads(original['packaging-git-check-original/receipt.json']);assert git['exit_code']==2
    assert git['stdout_sha256']==sha(original['packaging-git-check-original/stdout'])
    assert git['stderr_sha256']==sha(original['packaging-git-check-original/stderr'])
    assert b'new blank line at EOF' in original['packaging-git-check-original/stdout']
    attempt=json.loads(original['packaging-git-check-original/commit-attempt1.posthoc.json'])
    assert attempt['commit_command_reached'] is False and attempt['commit_created'] is False and attempt['cached_diff_check_exit']==2
    assert spec['original_Git_check_exit2_receipt_sha256']==sha(original['packaging-git-check-original/receipt.json'])
    # Reuse the exact existing source-review archive; no copied source reviews.
    base=repo/safe(d['base_publication']['evidence_directory'])
    for key in ('manifest','seal','profile'):
        path=base/d['base_publication'][key+'_path'];assert sha(path.read_bytes())==d['base_publication'][key+'_sha256']
    source=d['existing_source_reviews']
    archive_raw=(repo/safe(source['archive_path'])).read_bytes()
    assert sha(archive_raw)==source['stored_sha256']
    source_tar=gzip.decompress(archive_raw);assert sha(source_tar)==source['raw_tar_sha256']
    old=members(source_tar,'r:');assert len(old)==len(source['original_members'])
    for row in source['original_members']:
        data=old[row['path']];assert (len(data),sha(data))==(row['bytes'],row['sha256'])
    checked+=original_manifests(old)
    for row in d['adoption_originals']:
        data=(HERE/safe(row['path'])).read_bytes();assert (len(data),sha(data))==(row['bytes'],row['sha256'])
    plan=json.loads((HERE/'adoption/plan.json').read_bytes());codecopy=json.loads((HERE/'adoption/code-docs-copy.receipt.json').read_bytes())
    assert sha((HERE/'adoption/plan.json').read_bytes())==codecopy['plan_sha256']
    assert len(plan['files'])==codecopy['paths_copied']==9 and codecopy['main_compiled540_exact'] is True
    assert codecopy['new_Rust_execution_performed'] is False
    pubplan=json.loads((HERE/'adoption/publication-plan.json').read_bytes())
    pubcopy=json.loads((HERE/'adoption/publication-copy.receipt.json').read_bytes())
    assert sha((HERE/'adoption/publication-plan.json').read_bytes())==pubcopy['publication_plan_sha256']
    assert pubcopy['paths']==len(pubplan['files'])==186 and pubcopy['main_compiled540_exact'] is True
    assert pubcopy['public_evidence_paths_exact'] is True
    assert pubcopy['new_Rust_DB_CI_execution'] is False and pubcopy['whole_Git_or_compiler_read_closure'] is False
    assert pubcopy['candidate_manifest_sha256']==pubplan['candidate_manifest_sha256']==d['base_publication']['prior_v3_candidate_manifest_sha256']
    assert pubcopy['publication_Spec_receipt_sha256']==sha(original['publication-spec-v1/receipt.json'])
    assert pubcopy['publication_Standards_receipt_sha256']==sha(original['publication-standards-v1/receipt.json'])
    assert pubcopy['docs_v3_Spec_receipt_sha256']==sha(original['final-docs-spec-v3/receipt.json'])
    v4plan=json.loads((HERE/'adoption/publication-v4-plan.json').read_bytes())
    v4copy=json.loads((HERE/'adoption/publication-v4-copy.receipt.json').read_bytes())
    assert sha((HERE/'adoption/publication-v4-plan.json').read_bytes())==v4copy['publication_plan_sha256']
    assert v4copy['candidate_manifest_sha256']==v4plan['candidate_manifest_sha256']==d['base_publication']['candidate_manifest_sha256']
    assert v4copy['paths_copied_and_verified']==len(v4plan['files'])==186 and v4copy['changed_from_prior_view']==5
    assert v4copy['main_compiled540_exact'] is True and v4copy['new_Rust_DB_CI_qualification'] is False
    assert v4copy['publication_Spec_receipt_sha256']==sha(original['publication-spec-v2/receipt.json'])
    assert v4copy['publication_Standards_receipt_sha256']==sha(original['publication-standards-v2/receipt.json'])
    assert v4copy['prior_v3_copy_receipt_preserved']==sha((HERE/'adoption/publication-copy.receipt.json').read_bytes())
    assert v4copy['original_Git_whitespace_failure_preserved']==sha(original['packaging-git-check-original/receipt.json'])
    assert d['adoption_records_are_new_gates_or_whole_Git_qualification'] is False
    print(json.dumps({'status':'passed','new_original_review_groups':len(d['groups']),'archived_original_files_including_nonreview_Git_failure':len(original),'reused_source_review_groups':6,'reused_source_original_files':len(old),'original_manifests_checked_in_memory':checked,'adoption_originals':len(d['adoption_originals']),'runs_archived_scripts':False,'new_Rust_Native_fullCI_qualification':False},sort_keys=True))
if __name__=='__main__':main()
