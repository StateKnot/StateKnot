from pathlib import Path
import subprocess,json,sys,re,time,hashlib
O=Path(__file__).resolve().parent
receipts=[]; started=time.time()
for name in ['fmt','clippy','workspace','doctests','rustdoc','nightly']:
 p=subprocess.run([sys.executable,str(O/'run-gate.py'),name]); receipts.append(json.loads((O/(name+'.receipt.json')).read_text()))
 if p.returncode: sys.exit(p.returncode)
totals={}
for name in ['workspace','doctests']:
 matches=re.findall(r'test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;',(O/(name+'.log')).read_text())
 assert matches and all(v[0]=='ok' for v in matches)
 totals[name]={'suites':len(matches),'passed':sum(int(v[1]) for v in matches),'failed':sum(int(v[2]) for v in matches),'ignored':sum(int(v[3]) for v in matches),'filtered':sum(int(v[5]) for v in matches)}
result={'state':'passed','gate_count':6,'source_tree':json.loads((O/'frozen-source-manifest.json').read_text())['source_tree_sha256'],'receipts':receipts,'totals':totals,'seconds':time.time()-started,'original_profile_opt0_default_stack':True,'database_conditionals_not_native_qualification':True}
(O/'six-gates-summary.json').write_text(json.dumps(result,indent=2)+'\n'); print(json.dumps(result),flush=True)
assert (totals['workspace']['passed'],totals['workspace']['failed'],totals['workspace']['ignored'])==(2099,0,6)
assert (totals['doctests']['passed'],totals['doctests']['failed'])==(48,0)
