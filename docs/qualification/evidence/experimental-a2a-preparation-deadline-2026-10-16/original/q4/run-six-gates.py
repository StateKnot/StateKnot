#!/usr/bin/env python3
"""Run the six existing qualification gates sequentially; preserve first failure."""
import hashlib,json,subprocess,sys,time
from pathlib import Path
out=Path(__file__).resolve().parent
started=time.time()
results=[]
assert not (out/'six-gates-driver.receipt.json').exists()
for name in ['fmt','clippy','workspace','doctests','rustdoc','nightly']:
    code=subprocess.run([sys.executable,'-B',str(out/'run-gate.py'),name]).returncode
    results.append({'gate':name,'exit_code':code})
    if code:break
receipt={'started_unix':started,'seconds':time.time()-started,'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'runner_sha256':hashlib.sha256((out/'run-gate.py').read_bytes()).hexdigest(),'actual_results':results,'six_gates_all_passed':len(results)==6 and all(r['exit_code']==0 for r in results)}
(out/'six-gates-driver.receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(receipt),flush=True)
sys.exit(results[-1]['exit_code'])
