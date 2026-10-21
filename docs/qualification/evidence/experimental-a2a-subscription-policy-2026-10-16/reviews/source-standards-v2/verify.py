import pathlib,json,hashlib,gzip
p=pathlib.Path(__file__).resolve().parent;m=json.loads((p/'artifact-manifest.json').read_text())
for r in m['files']:
 q=p/r['path'];assert q.stat().st_size==r['bytes'] and hashlib.sha256(q.read_bytes()).hexdigest()==r['sha256'],q
b=json.loads((p/'review-basis.json').read_text());assert b['source_count']==930 and b['compiled_count']==538 and b['source_before_after_exact']
assert b['other928_paths_byte_identical'] and len(b['v3_to_v5_changed_paths'])==2
assert not b['non_tooling_standards_findings'] and not b['actionable_smells']
assert [r['exit_code']for r in b['known_original_tooling_reds_preserved']]==[101,1]
assert gzip.decompress((p/'v3-to-v5-fixture-only.patch.gz').read_bytes()).startswith(b'--- v3/')
print(f'PASS:{len(m["files"])} review artifact hashes;v5 fixed930/538;production unchanged;0 incremental Standards findings;old tooling reds preserved;no execution transfer.')
