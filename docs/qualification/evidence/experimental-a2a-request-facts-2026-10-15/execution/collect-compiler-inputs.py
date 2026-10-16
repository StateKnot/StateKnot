from pathlib import Path
import hashlib,json,shutil
b=Path(__file__).resolve().parent; source=Path(json.loads((b/'source-manifest.json').read_text())['source_root']); target=b.parents[3]/'target/r1-oct15-proof-base'; manifest=json.loads((b/'source-manifest.json').read_text()); exact={v['path']:v for v in manifest['files']}
rawdir=b/'compiler-observed-dep-info'; rawdir.mkdir(exist_ok=True); rows=[]; inputs={}
for line in (b/'build-v1/fresh-build.log').read_text().splitlines():
 try: artifact=json.loads(line)
 except json.JSONDecodeError: continue
 if artifact.get('reason')!='compiler-artifact' or not Path(artifact['target']['src_path']).is_relative_to(source): continue
 paths=[]
 for filename in artifact['filenames']:
  p=Path(filename)
  if p.suffix in ['.rlib','.rmeta'] and p.name.startswith('lib'): paths.append(p.with_name(p.stem[3:]+'.d'))
  elif p.suffix=='': paths.append(p.with_suffix('.d'))
 for dep in sorted(set(p for p in paths if p.exists())):
  assert dep.is_relative_to(target); body=dep.read_text(); deps=body.splitlines()[0].split(': ',1)[1].split(); names=[]
  for name in deps:
   p=(source/name).resolve(); assert p.is_relative_to(source) and p.is_file(),name
   rel=str(p.relative_to(source)); assert rel in exact
   row={'path':rel,'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}; assert row==exact[rel]; inputs[rel]=row; names.append(rel)
  saved=rawdir/dep.name
  if saved.exists(): assert saved.read_bytes()==dep.read_bytes()
  else: shutil.copy2(dep,saved)
  rows.append({'target':artifact['target']['name'],'src_path':artifact['target']['src_path'],'fresh':artifact['fresh'],'profile':artifact['profile'],'raw_dep_info':str(saved),'raw_sha256':hashlib.sha256(saved.read_bytes()).hexdigest(),'observed_source_inputs':sorted(set(names))})
assert {'a2a_client_contract','mcp_durable','stateknot_integrations','stateknot_runtime','stateknot_store_postgres','stateknot_core'}==set(v['target'] for v in rows)
obj={'source_tree':manifest['source_tree_sha256'],'scope':'compiler-emitted source dependency entries of the two final binaries and their four workspace library artifacts; not a claim of complete compiler/filesystem/dependency-registry read closure','artifacts':rows,'observed_workspace_file_count':len(inputs),'files':list(sorted(inputs.values(),key=lambda v:v['path']))}
(b/'compiler-observed-workspace-inputs.json').write_text(json.dumps(obj,indent=2)+'\n'); print(json.dumps({'artifacts':len(rows),'observed_workspace_file_count':len(inputs)}))
