from pathlib import Path
from datetime import datetime,timezone
import json,hashlib,gzip,subprocess,sys
R=Path('/Users/jiawy/Documents/RustAgentFramework');B=R/'.local-verification/2026-10-16';O=B/'evidence/mcp-http-debug-publication-independent-review-v2';P=B/'mcp-http-debug-main-publication-v2';V1=B/'mcp-http-debug-publication-preparation-v1';C=P/'candidate';OLD=V1/'candidate';E='docs/qualification/evidence/experimental-mcp-http-diagnostics-2026-10-16/';sha=lambda d:hashlib.sha256(d).hexdigest();load=lambda p:json.loads(p.read_bytes());put=lambda n,v:(O/n).write_text(json.dumps(v,indent=2,ensure_ascii=False)+'\n')
assert sha((P/'candidate-manifest.json').read_bytes())=='cc98e0d5e10949ff07ae412a7c4b33bcdcf4e4ceedb73d05be5f08a845878770';assert sha((P/'freeze.json').read_bytes())=='2d756f7b0b438d91ad288c37bbcf10ab63ad6014e4ba9d5e3831c1bf294d1ba0';assert sha((V1/'candidate-manifest.json').read_bytes())=='29815d58e4d758bd2e65e99966f5811e2785d5de73bc772edf859ebc19ecc679'
f=load(P/'freeze.json');inc=load(P/'packaging-increment.json');assert sha((P/'packaging-increment.json').read_bytes())==f['packaging_increment_sha256'];assert sha((P/'candidate-paths.json').read_bytes())==f['candidate_paths_sha256']
old={p.relative_to(OLD).as_posix():p.read_bytes() for p in OLD.rglob('*') if p.is_file()};new={p.relative_to(C).as_posix():p.read_bytes() for p in C.rglob('*') if p.is_file()};assert len(old)==len(new)==98 and sum(map(len,new.values()))==594517
removed=set(old)-set(new);added=set(new)-set(old);assert removed==set(inc['removed_paths']) and added==set(inc['added_paths']);renames=inc['renamed_lossless_artifacts'];assert len(renames)==2
for row in renames:
 before=old[row['before_path']];after=new[row['after_path']];assert gzip.decompress(after)==before
 assert after[4:8]==b'\0'*4 and not after[3]&8 and gzip.compress(before,compresslevel=9,mtime=0)==after
 assert len(before)==row['raw_bytes']==918 and sha(before)==row['raw_sha256']=='27104f8f6a9609b043eedbaed7035304004675a2746ef95a64da7b5814eb39d7'
 assert (len(after),sha(after))==(row['gzip_bytes'],row['gzip_sha256'])
metadata={row['path'] for row in inc['modified_metadata_files']};changed={p for p in set(old)&set(new) if old[p]!=new[p]};assert changed==metadata=={E+'artifact-index.json',E+'manifest.json',E+'seal.json','docs/qualification/experimental-mcp-http-diagnostics-2026-10-16.json'}
for row in inc['modified_metadata_files']:assert sha(old[row['path']])==row['before_sha256'] and sha(new[row['path']])==row['after_sha256']
oldidx=json.loads(old[E+'artifact-index.json']);newidx=json.loads(new[E+'artifact-index.json']);a={r['path']:r for r in oldidx['artifacts']};b={r['path']:r for r in newidx['artifacts']};mapping={r['before_path'][len(E):]:r['after_path'][len(E):] for r in renames}
for path,row in a.items():
 other=b[mapping.get(path,path)]
 if path in mapping:
  assert row['raw_bytes']==other['raw_bytes'] and row['raw_sha256']==other['raw_sha256'];assert row['storage_encoding']=='identity' and other['storage_encoding']=='gzip'
  keep=lambda r:{k:v for k,v in r.items() if k not in ['path','storage_encoding','stored_bytes','stored_sha256']};assert keep(row)==keep(other)
 else:assert row==other
assert {k:v for k,v in oldidx.items() if k!='artifacts'}=={k:v for k,v in newidx.items() if k!='artifacts'}
a=json.loads(old[E+'seal.json']);b=json.loads(new[E+'seal.json']);a['manifest_sha256']=b['manifest_sha256'];assert a==b
a=json.loads(old['docs/qualification/experimental-mcp-http-diagnostics-2026-10-16.json']);b=json.loads(new['docs/qualification/experimental-mcp-http-diagnostics-2026-10-16.json']);a['evidence']['manifest_sha256']=b['evidence']['manifest_sha256'];a['evidence']['seal_sha256']=b['evidence']['seal_sha256'];note=b['evidence']['publication_packaging'];assert note['version']==2 and note['raw_patch_bytes_retained_by_gzip_mtime0'] and note['old_v1_seals_immutable'];a['evidence']['publication_packaging']=note;assert a==b
for path,data in new.items():
 if path.endswith('.gz'):continue
 text=data.decode();assert not any(line.endswith((' ','\t')) for line in text.splitlines()),path
m=load(P/'candidate-manifest.json');assert set(new)=={r['path'] for r in m['files']}
for row in m['files']:assert (len(new[row['path']]),sha(new[row['path']]))==(row['bytes'],row['sha256'])
assert load(P/'candidate-paths.json')['paths']==sorted(new)
cmd=[sys.executable,str(P/'verify.py')];r=subprocess.run(cmd,cwd=R,stdout=subprocess.PIPE,stderr=subprocess.PIPE);(O/'offline.stdout').write_bytes(r.stdout);(O/'offline.stderr').write_bytes(r.stderr);assert r.returncode==0
put('offline.receipt.json',{'command':cmd,'exit_code':r.returncode,'stdout_sha256':sha(r.stdout),'stderr_sha256':sha(r.stderr),'scope':'offline integrity/reconstruction/raw evidence only; no product execution'})
put('audit.json',{'observed_utc':datetime.now(timezone.utc).isoformat(),'prior_v1_manifest_sha256':'29815d58e4d758bd2e65e99966f5811e2785d5de73bc772edf859ebc19ecc679','final_v2_manifest_sha256':sha((P/'candidate-manifest.json').read_bytes()),'final_v2_freeze_sha256':sha((P/'freeze.json').read_bytes()),'candidate_files':98,'candidate_bytes':594517,'renamed_artifacts':renames,'both_raw_patch_bytes_equal_v1_and_saved_original_SHA':True,'gzip_mtime0_no_filename_and_deterministic_roundtrip':True,'metadata_modified_exactly':sorted(metadata),'unchanged_candidate_files':len(set(new)&set(old))-len(changed),'artifact_index_non_storage_fields_unchanged':True,'seal_only_manifest_hash_changes':True,'machine_JSON_only_evidence_hashes_and_accurate_packaging_note_changes':True,'EN_CN_reports_payloads_runners_logs_receipts_contracts_source_refs_all_bytes_unchanged':True,'all_non_gzip_text_trailing_whitespace_absent':True,'original_v1_review_and_root_staged_diffcheck_failure_retained':True,'new_compile_Native_network_DB_Docker_cache_main_or_original_package_mutation':False,'workspace_mode_not_repeated_or_old_negative_reinterpreted':True})
print(json.dumps({'increment_findings':0,'unchanged_files':92,'raw_roundtrip_patches':2,'changed_metadata':4,'offline_exit':r.returncode,'candidate_files':98,'bytes':594517}))
