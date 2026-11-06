# -*- coding: utf-8 -*-
from pathlib import Path
import hashlib,json,time,subprocess
root=Path('/Users/jiawy/Documents/RustAgentFramework');b=root/'.local-verification/2026-10-15';i=b/'elapsed36-attempt-proof-integration';o=i/'source32-real-old-producer-v1';s=b/'elapsed36-attempt-proof-test-fixes-source';prod=i/'source32-old35-producer'
def read(p):return json.loads(p.read_text())
m=read(o/'source-manifest.json');rows=[dict(path=str(p.relative_to(s)),bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest())for p in sorted(s.rglob('*'))if p.is_file()];assert rows==m['files']
command=['cargo','fmt','--manifest-path',str(s/'Cargo.toml'),'--all','--','--check'];r=subprocess.run(command,capture_output=True,text=True);(o/'fmt-check.log').write_text(r.stdout+r.stderr);assert r.returncode==0;(o/'fmt-check-receipt.json').write_text(json.dumps(dict(command=command,exit_code=r.returncode,source_tree_sha256=m['source_tree_sha256']),indent=2)+'\n')
ledger=[
 ('独立新fixture库/原options', '原CREATE DATABASE及migrate36/options', '保留原行为；新库runs=0后才revert'),
 ('schema32账本', '原revert后max(version)==32', '保留；空库重建后及导入真实旧账本后各断言32'),
 ('旧schema拒绝新Store直连', 'connect→IncompatibleSchema', '保留同一原断言'),
 ('1..32 checksum逐字', '原writer即时查询32行', '真实旧writer原checksums，升级后原完整32行相等'),
 ('Wait Run完整14表', 'facts(fence)==before', '原完整表达式保留；before来自真实旧writer导出'),
 ('Return Run完整14表', 'facts(prior_fence)==prior', '原完整表达式保留；prior来自真实旧writer导出'),
 ('Wait registrations全部原行', 'jsonb_agg(to_jsonb(w))==waits_before', '原完整表达式保留，全部bytea字段亦在原SQL逐字保留'),
 ('真实Return完整owning read', 'load_graph_frame_return(...).digest()==returned.digest()', '同一个公开完整reader，期望digest来自旧writer实际return'),
 ('实际active checkpoint', 'load_active_graph_frame.checkpoint()==saved.checkpoint()', '原完整Checkpoint比较，期望Core从旧metadata完整decode'),
 ('真实Wait完整owning read', '此前仅active CP+原facts', '新增load_graph_frame_barrier：原完整CP/event/directusage逐项相等'),
 ('真实Cancellation/abandon', '原append_control_plane_abandon_waits', '保留原original journal expectation、revision、33_030、cancellation_request(original event time)'),
 ('真实whole closure', 'close_graph_frames→confirm', '保留原事件、当前head/revision、原usage；actual closure和confirm均必须成功'),
 ('whole closure后rollback拒绝', 'combined原reverters execute.is_err，rollback', '原表达式保留；新增V3/nonlegacy计数0，证明前置elapsed guard不掩盖closure拒绝'),
 ('rollback后closure身份', 'public load_graph_frame_closure digest相等', '原完整表达式及verify_schema保留'),
 ('restore guard/trigger', '没有snapshot导入', '新增全部schema32 trigger(name/table/state)前后全等，dump连接关闭以丢弃session settings')
]
(o/'assert-ledger.json').write_text(json.dumps(dict(original_test='frame_closures::populated_schema_32_upgrade_keeps_waits_returns_and_checksums_before_whole_closure',production_changes=[],original_guard_changes=[],runtime_option_changes=[],removed_business_assertions=[],assertions=[dict(invariant=a,original=c,candidate=d)for a,c,d in ledger]),indent=2,ensure_ascii=False)+'\n')
red={str(major):read(i/f'full-native-v1/upgrade-red-triage/exact-pg{major}-v1/receipt.json')for major in (16,17)};green={str(major):read(o/f'native-pg{major}-v1/receipt.json')for major in (16,17)};export={str(major):read(prod/f'native-pg{major}-v1/export/dump-receipt.json')for major in (16,17)}
for major in ('16','17'):assert red[major]['exact_original_red_observed']and green[major]['exact_candidate_success']
summary=dict(source=str(s),source_files=921,source_tree_sha256=m['source_tree_sha256'],production_unchanged=True,source_unchanged_after_build_and_native=True,guard_unchanged=True,original_default_stack_and_runtime_resources=True,strict_store_build=read(o/'strict-store-build-v1/receipt.json'),original_exact_red=red,candidate_exact_green=green,producer_export=export,producer_scope='Actual immutable schema35+A writer, source32 reconstructed by existing source reverter; not actual historical schema32 or released N-1/N-2 qualification.',fixture_transformations='Only psql client restrict/unrestrict lines omitted; all INSERT bytes and all original canonical/data/ledger values unchanged.',qualifications_not_granted=['full Store suite','full Runtime suite','integrations','example','six static gates','CI 30-minute job budget','backup/restore conditional drill','PITR conditional drill','released N-1/N-2 upgrade'],original_v1_failure_records_preserved=True)
(o/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
text='''# 实际旧 writer source32 升级回归\n\n第二红来自测试构造错误：当前36 writer已写入V3原条件事实，原回退guard正确拒绝把这些事实重建成32。原exact测试在PG16/17均以Pg22012失败，完整日志保留；生产guard未改。\n\n修复只改隔离副本的该测试及Wait oracle测试，增加真实旧fixture两个固定文件。独立源921文件/tree `'''+m['source_tree_sha256']+'''`，相对原919的生产Rust/Core/Store、全部1..36 migrations、配置、其余测试逐字相同。严格Store构建0/fresh，binary `de221bb45fcb9bd2efe50ebd9355bca18b302bbea6113604c06b09ef4f9c2bc7`，原配置/default stack。\n\n旧producer来自899文件schema35+A冻结源，仅追加测试exporter；实际Wait及Return业务调用、原revert至32、原assert32保留。PG16/17各1/1通过，实际账本1..32全success、50业务表、scoped barrier V1×1/V2×1、无V3。两个原dump与metadata均保留。本回归双PG统一使用PG16实际snapshot，非分别生成新期望。原dump361875字节，SHA `2921a7cd08e9458a7bcbed9bf6225a7e2d0f48d5230d6ada35a28258b2dbc990`。仅剥两条psql客户端restrict行，所有其他字节不变；原bytea逐项定长打包348项/151642字节并有offset/length/SHA清单。未解析再编码任何canonical row，也未做V3→V2转换。\n\n新测试先在独立空36库按原guards重建32，在单事务用原pg_dump SQL恢复真实旧数据及原32账本，逐一核对所有trigger状态与空32一致，关闭dump连接清其session设置；再使用真实MIGRATOR升36。原checksum/双Run14表facts/Wait行/Return reader/active CP/取消与wholeClosure/拒绝回退与rollback后closure等断言保留，另新增完整wholeWait reader和前置V3 guard假值认证。逐项表见assert-ledger.json。\n\n| PostgreSQL | 原919 exact | 新921 exact |\n|---|---|---|\n| 16.15 | 0 passed /1 failed，5.626s，Pg22012 | 1 passed /0 failed，6.329s |\n| 17.11 | 0 passed /1 failed，3.190s，Pg22012 | 1 passed /0 failed，6.569s |\n\n以上是实际schema35 writer生成的数据加source32 schema重建，不能声称是实际历史schema32二进制或released N−1/N−2资格。当前只授予严格Store构建、fmt和这一个exact回归的双PG资格；完整Store/Runtime/静态六项均需重新验收，不继承旧绿。原919完整双链仍运行，源码、binary和红均未覆盖。\n'''
(o/'README.zh-CN.md').write_text(text)
# External producer evidence is immutable and separate from compilation cache.
for e in (prod,o):
 artifact=[dict(path=str(p.relative_to(e)),bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest())for p in sorted(e.rglob('*'))if p.is_file()and p.name!='artifact-manifest.json'];(e/'artifact-manifest.json').write_text(json.dumps(dict(file_count=len(artifact),files=artifact),indent=2)+'\n')
print(json.dumps(dict(source_tree_sha256=m['source_tree_sha256'],exact_red_majors=list(red),exact_green_majors=list(green),report=str(o/'README.zh-CN.md'),fmt_exit=0),indent=2))
