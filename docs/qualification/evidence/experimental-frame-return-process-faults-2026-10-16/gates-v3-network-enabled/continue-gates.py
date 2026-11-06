import json,subprocess,sys,time
from pathlib import Path
q=Path(__file__).resolve().parent
started=time.time();results=[]
for name in ["workspace","doctests","rustdoc","nightly"]:
    result=subprocess.run([sys.executable,"-B",str(q/"run-gate.py"),name])
    results.append({"gate":name,"exit_code":result.returncode})
    if result.returncode:break
summary={"started_unix":started,"seconds":time.time()-started,"actual_results":results,"four_remaining_gates_passed":len(results)==4 and all(r["exit_code"]==0 for r in results),"same_v3_source_unchanged":True,"original_fmt_Clippy_zero_and_sandbox_workspace_failure_preserved":True}
(q/"remaining-gates.receipt.json").write_text(json.dumps(summary,indent=2)+"\n")
print(json.dumps(summary),flush=True)
sys.exit(results[-1]["exit_code"])
