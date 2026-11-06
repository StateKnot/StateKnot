import pathlib,json,hashlib,gzip
p=pathlib.Path(__file__).resolve().parent;m=json.loads((p/'artifact-manifest.json').read_text())
for r in m['files']:
 q=p/r['path'];assert q.stat().st_size==r['bytes'] and hashlib.sha256(q.read_bytes()).hexdigest()==r['sha256'],q
b=json.loads((p/'review-basis.json').read_text());assert b['candidate']['source_count']==930 and b['candidate']['compiled_count']==538
assert b['source_before_after_exact'] and len(b['original_methods_byte_equal'])==14
assert len(b['exact_source_delta']['changed'])==1 and len(b['exact_source_delta']['added'])==2 and not b['exact_source_delta']['removed']
assert not b['new_code_standard_violations_excluding_tooling_findings'] and not b['actionable_non_tooling_smells']
assert b['known_tooling_failure']['exit_code']==101 and b['known_tooling_failure']['diagnostic_count']==7
assert gzip.decompress((p/'reviewed-source.patch.gz').read_bytes()).startswith(b'--- baseline/')
print(f'PASS: {len(m["files"])} artifact hashes;source930/compiled538;3path delta;14 old methods equal;known Clippy101 preserved;0 additional non-tooling Standards findings.')
