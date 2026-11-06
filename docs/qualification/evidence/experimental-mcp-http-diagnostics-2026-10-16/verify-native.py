#!/usr/bin/env python3
"""Read-only proof of the two targeted Native suites; never executes their runner."""
from pathlib import Path
import gzip,hashlib,json,re
H=Path(__file__).resolve().parent
sha=lambda data:hashlib.sha256(data).hexdigest()
canonical=lambda v:json.dumps(v,sort_keys=True,separators=(',',':'))
idx={row['path']:row for row in json.loads((H/'artifact-index.json').read_text())['artifacts']}
def raw(name):
 name='native/'+name
 if name not in idx:name+='.gz'
 row=idx[name];data=(H/name).read_bytes()
 assert (len(data),sha(data))==(row['stored_bytes'],row['stored_sha256'])
 if row['storage_encoding']=='gzip':data=gzip.decompress(data)
 assert (len(data),sha(data))==(row['raw_bytes'],row['raw_sha256'])
 return data
obj=lambda name:json.loads(raw(name))
summary=obj('summary.json');inventory=obj('inventory.json')
assert sha(raw('summary.json'))=='d09277466fedc0fe80a2b6f42d801481d00831694118858eba18c11868b7eb05'
assert sha(raw('runner.py'))=='234a01c80b914084d707e37b52ae04e7e58601ee264cae45f075fff0108813a9'
assert summary['state']=='passed' and summary['source_928_before_after_exact']
assert summary['source_tree']==inventory['source_tree']=='31a5ff5596ead9f859a20e7b483d6b660e0577fcf47327ec0a572eb3493e6fca'
assert summary['compiled_canonical_sha256']==inventory['compiled_canonical_sha256']=='1ec97f720269934853dcb3fd66241b38a5b381fda48f4164c11f3dcc091f945c'
assert summary['full_Store_Runtime_Native_CI_capacity_or_R1_qualified'] is False
assert inventory['marker_schema_24_is_legacy_profile_constant_not_current_database_schema']
runner_sha=sha(raw('runner.py'));inventory_sha=sha(raw('inventory.json'))
def receipt(name):
 r=obj(name+'.receipt.json')
 assert r['exit_code']==0 and r['source_tree']==summary['source_tree']
 assert r['runner_sha256']==runner_sha and r['inventory_sha256']==inventory_sha
 assert r['log_sha256']==sha(raw(name+'.log'))
 assert r['default_stack_no_runner_override'] and not any(r['override_presence'].values())
 env=r['rust_environment_allowlist']
 assert env['CARGO_INCREMENTAL']=='0' and env['RUSTFLAGS']==env['RUSTDOCFLAGS']=='-D warnings'
 for profile in ['DEV','TEST']:
  for key,value in [('OPT_LEVEL','0'),('DEBUG','0'),('DEBUG_ASSERTIONS','true'),('OVERFLOW_CHECKS','true')]:assert env['CARGO_PROFILE_'+profile+'_'+key]==value
 assert 'STATEKNOT_TEST_DATABASE_URL' not in env
 return r
for suite,count in [('mcp_durable',6),('mcp_reconciliation',10)]:
 binary=obj(suite+'-binary.json');assert binary==summary['binaries'][suite]
 art=binary['cargo_artifact'];assert art['fresh'] is False and art['features']==[]
 assert art['profile']=={'debug_assertions':True,'debuginfo':0,'opt_level':'0','overflow_checks':True,'test':True}
 assert art['target']['kind']==['test'] and art['target']['name']==suite
 receipt(suite+'-fresh-build');build=[]
 for line in raw(suite+'-fresh-build.log').decode().splitlines():
  try:item=json.loads(line)
  except json.JSONDecodeError:continue
  if item.get('reason')=='compiler-artifact' and item.get('executable') and item['target']['name']==suite:build.append(item)
 assert build==[art]
 receipt(suite+'-list');names=re.findall(r'^(\S+): test$',raw(suite+'-list.log').decode(),re.M)
 assert len(names)==len(set(names))==count and sorted(names)==inventory['tests'][suite] and names==binary['tests']
records=[]
for major in [16,17]:
 identity=obj('pg%d-identity.json'%major)
 assert identity['major']==major and identity['actual_version'].startswith(str(major)+'.')
 assert identity['new_absent_database_created']==summary['new_database_retained_each_major'] and identity['database_not_dropped']
 marker_count=0
 for suite,count in [('mcp_durable',6),('mcp_reconciliation',10)]:
  name='pg%d-%s'%(major,suite);r=receipt(name);text=raw(name+'.log').decode()
  assert r['argv'][-2:]==['--nocapture','--test-threads=1'] and r['rust_environment_allowlist']['STATEKNOT_REQUIRE_POSTGRES_TESTS']=='1'
  summaries=re.findall(r'^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;.*$',text,re.M)
  assert summaries==[(str(count),'0','0','0','0')]
  actual_names=re.findall(r'^test (\S+) \.\.\. ',text,re.M)
  assert sorted(actual_names)==inventory['tests'][suite]
  assert not re.search(r'STATEKNOT_.*SKIPPED',text)
  markers={}
  for prefix,wanted in inventory['expected_markers'][suite].items():
   found=[json.loads(line[len(prefix)+1:]) for line in text.splitlines() if line.startswith(prefix+'=')]
   assert sorted(map(canonical,found))==sorted(map(canonical,wanted)),prefix
   markers[prefix]=found;marker_count+=len(found)
  q=obj(name+'-qualification.json');assert q['markers']==markers
  assert [q[k] for k in ['passed','failed','ignored','measured','filtered']]==[count,0,0,0,0]
  assert q['major']==major and q['binary_sha256']==summary['binaries'][suite]['sha256'] and q['database']==summary['new_database_retained_each_major']
  records.append(q)
  for phase in ['before','after']:
   s=obj(name+'-'+phase+'-inputs.json');assert s['exact'] and s['source_files']==928 and s['source_tree']==summary['source_tree']
 assert marker_count==7
assert records==summary['postgres']
final=obj('final-inputs.json');assert final['exact'] and final['total_logical_kib']==10554584
assert final['logical_size_is_not_APFS_reclaimable_space'] and len(final['caches'])==8
assert int(raw('final.df').decode().splitlines()[-1].split()[3])==315708296
for phase in ['initial','final']:
 guard=obj(phase+'-cache-guard.json')
 assert guard['tag_verified'] and guard['git_untracked'] and guard['lsof_empty'] and guard['locks_probeable']
 assert raw(phase+'-cache-lsof.stdout')==raw(phase+'-cache-lsof.stderr')==b''
print(json.dumps({'state':'targeted Native raw/profile/list/complete markers verified','majors':[16,17],'each_major':{'mcp_durable':[6,0,0,0],'mcp_reconciliation':[10,0,0,0]},'markers_per_major':7,'two_fresh_default_feature_binaries':True,'full_Store_Runtime_Native_CI_R1':False}))
