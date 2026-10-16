from pathlib import Path
import hashlib,json,os,re,subprocess,sys,time
root=Path('/Users/jiawy/Documents/RustAgentFramework');base=root/'.local-verification/2026-10-15';integration=base/'elapsed36-attempt-proof-integration';final=integration/'final-test-fixes-v1';source=base/'elapsed36-attempt-proof-final-test-source';out=integration/'final-test-fixes-pitr-v1';out.mkdir(exist_ok=False);(out/'runner.py').write_bytes(Path(__file__).read_bytes())
manifest=json.loads((final/'source-manifest.json').read_text());assert manifest['file_count']==922
(out/'wait-scope.json').write_text(json.dumps(dict(required_parent_chains=[str(final/f'full-native-v1/pg{major}/receipt.json')for major in(16,17)],gate='Both fixed922 four-suite chains AND their separately mandatory backup-restore are terminal before any PITR resource operation.',no_DB_environment=True,no_source_or_run_chain_report_change=True),indent=2)+'\n')
while True:
 done=[]
 for major in(16,17):
  p=final/f'full-native-v1/pg{major}/receipt.json'
  try:v=json.loads(p.read_text());done.append(v.get('complete')is True and 'mandatory_backup_restore'in v)
  except(FileNotFoundError,json.JSONDecodeError):done.append(False)
 if all(done):break
 time.sleep(5)

def source_check(o,label):
 rows=[dict(path=str(p.relative_to(source)),bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest())for p in sorted(source.rglob('*'))if p.is_file()];assert rows==manifest['files'];(o/(label+'-source-proof.json')).write_text(json.dumps(dict(source_files=922,source_tree_sha256=manifest['source_tree_sha256'],all_bytes_match=True),indent=2)+'\n')

def inventory(o,label):
 commands={'containers':['docker','ps','-a','--format','{{.ID}}\t{{.Names}}\t{{.Status}}\t{{.Image}}'],'volumes':['docker','volume','ls','--format','{{.Name}}\t{{.Driver}}']}
 for name,command in commands.items():
  v=subprocess.run(command,capture_output=True,text=True);assert v.returncode==0;(o/(label+'-'+name+'.txt')).write_text(v.stdout)

source_check(out,'before');binary=json.loads((final/'full-native-v1/plan.json').read_text())['artifacts']['stateknot-runtime'];path=Path(binary['path']);assert hashlib.sha256(path.read_bytes()).hexdigest()==binary['sha256'];exact='child_admission::ownership::join::pitr::consumed_child_join_survives_named_point_in_time_recovery';listing=subprocess.check_output([str(path),'--list','--format','terse'],text=True);assert listing.splitlines().count(exact+': test')==1;(out/'runtime--list.txt').write_text(listing)
images={'16':'postgres@sha256:fe03a7605299a34ddf5e4f285dff78c3d7190a576b3c6b46f2fcff69f4bffd54','17':'postgres@sha256:67f41722b7a8cbdb868a44a4995c846eddfdc2973bccb291ce937dce88ad5675'};receipts=[]
for major,image in images.items():
 o=out/('pg'+major);o.mkdir(exist_ok=False);source_check(o,'before');(o/'before.df').write_bytes(subprocess.check_output(['df','-k',str(root)]));assert int((o/'before.df').read_text().splitlines()[-1].split()[3])>50*1024*1024
 info=json.loads(subprocess.check_output(['docker','image','inspect',image],text=True))[0];assert info['Os']=='linux'and info['Architecture']=='arm64'and image in info['RepoDigests'];(o/'image-identity.json').write_text(json.dumps(dict(requested=image,id=info['Id'],repo_digests=info['RepoDigests'],os=info['Os'],architecture=info['Architecture']),indent=2)+'\n');inventory(o,'before')
 env=os.environ.copy()
 for key in list(env):
  if key.startswith('STATEKNOT_TEST_')or key in ['STATEKNOT_REQUIRE_POSTGRES_TESTS','STATEKNOT_REQUIRE_BACKUP_RESTORE_TESTS','STATEKNOT_REQUIRE_PITR_TESTS','STATEKNOT_PITR_POSTGRES_IMAGE','STATEKNOT_PROOF_BASE_PROBE','STATEKNOT_SQL37_TRACE_DIR']:env.pop(key,None)
 assert 'RUST_MIN_STACK'not in env and 'STATEKNOT_TEST_DATABASE_URL'not in env
 env['STATEKNOT_REQUIRE_PITR_TESTS']='1';env['STATEKNOT_PITR_POSTGRES_IMAGE']=image
 command=[str(path),exact,'--exact','--nocapture','--test-threads=1'];started=time.time()
 with(o/'native.log').open('w')as log:
  proc=subprocess.Popen(command,cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT);(o/'running.json').write_text(json.dumps(dict(pid=proc.pid,command=command,started_unix=started,source_tree_sha256=manifest['source_tree_sha256'],binary_sha256=binary['sha256'],image=image),indent=2)+'\n');proc.wait()
 body=(o/'native.log').read_text(errors='replace');summaries=re.findall(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;',body);markers=[json.loads(line.split('=',1)[1])for line in body.splitlines()if line.startswith('STATEKNOT_CHILD_JOIN_PITR_EVIDENCE=')];valid=proc.returncode==0 and summaries==[('ok','1','0','0')]and len(markers)==1
 if valid:
  m=markers[0];valid=m.get('profile')=='child-join-named-pitr-v1'and m.get('invariants')=='passed'and m.get('postgres','').startswith(major+'.')and m.get('named_restore_point')=='stateknot_child_join_before_late_write'and bool(re.fullmatch('[0-9a-fA-F]{64}',m.get('manifest_sha256','')))and bool(re.fullmatch('[0-9a-fA-F]{24}',m.get('wal_segment_archived','')))and all(m.get(key)is True for key in ['base_backup_verified','post_target_write_excluded','exact_durable_snapshot_verified','noninitial_replay_verified','cross_tenant_refusal_verified'])
 inventory(o,'after');source_check(o,'after');assert hashlib.sha256(path.read_bytes()).hexdigest()==binary['sha256'];(o/'after.df').write_bytes(subprocess.check_output(['df','-k',str(root)]))
 receipt=dict(source=str(source),source_files=922,source_tree_sha256=manifest['source_tree_sha256'],source_unchanged=True,binary=str(path),binary_sha256=binary['sha256'],image=image,image_id=info['Id'],actual_server_version=markers[0].get('postgres')if len(markers)==1 else None,postgres_major=int(major),host_harness_platform=markers[0].get('os')if len(markers)==1 else None,actual_server_platform='linux/arm64 container',command=command,exit_code=proc.returncode,seconds=time.time()-started,summaries=summaries,markers=markers,exact_mandatory_PITR_passed=valid,no_DB_environment=True,only_profile_environment_set=['STATEKNOT_REQUIRE_PITR_TESTS','STATEKNOT_PITR_POSTGRES_IMAGE'],default_stack=True,original_profile_resources_and_assertions=True,loopback_only_as_original=True,original_cleanup_scope='Only UUID-named test-owned source/target --rm containers and three own named volumes; original DockerResources Drop. No extra cleanup and no existing task/user DB or volume operation.',parent_four_suites_and_backup_terminal_before_start=True,log_sha256=hashlib.sha256((o/'native.log').read_bytes()).hexdigest());(o/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n');receipts.append(receipt);print(json.dumps(dict(postgres_major=major,exit_code=proc.returncode,qualified=valid,seconds=receipt['seconds'],actual_server_version=receipt['actual_server_version'])),flush=True)
source_check(out,'after');summary=dict(source=str(source),source_files=922,source_tree_sha256=manifest['source_tree_sha256'],source_unchanged=True,binary_sha256=binary['sha256'],both_mandatory_PITR_passed=all(x['exact_mandatory_PITR_passed']for x in receipts),serial_PITR_profiles=True,original_four_suite_and_backup_report_not_modified=True,rows=receipts);(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');files=[dict(path=str(p.relative_to(out)),bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest())for p in sorted(out.rglob('*'))if p.is_file()and p.name!='artifact-manifest.json'];(out/'artifact-manifest.json').write_text(json.dumps(dict(file_count=len(files),files=files),indent=2)+'\n');sys.exit(0 if summary['both_mandatory_PITR_passed'] else 1)
