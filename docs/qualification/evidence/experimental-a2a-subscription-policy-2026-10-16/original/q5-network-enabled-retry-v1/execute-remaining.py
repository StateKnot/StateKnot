#!/usr/bin/env python3
import json,subprocess,sys
from pathlib import Path
root=Path(__file__).resolve().parent
first=root.parent/"a2a-subscription-qualification-v5"
for gate in ("fmt","clippy"):
 r=json.loads((first/(gate+".receipt.json")).read_bytes());assert r["exit_code"]==0 and r["source_before_after_exact"]
for gate in ("workspace","doctests","rustdoc","nightly"):
 result=subprocess.run([sys.executable,str(root/"run-gate.py"),gate])
 if result.returncode:sys.exit(result.returncode)
paths=[first/(g+".receipt.json") for g in ("fmt","clippy")]+[root/(g+".receipt.json") for g in ("workspace","doctests","rustdoc","nightly")]
receipts=[json.loads(p.read_bytes()) for p in paths]
assert all(r["exit_code"]==0 and r["source_before_after_exact"] and r["source_tree"]==receipts[0]["source_tree"] for r in receipts)
(root/"six-gates-summary.json").write_text(json.dumps({"state":"passed","gate_count":6,"source_tree":receipts[0]["source_tree"],"receipt_paths":[str(p) for p in paths],"receipts":receipts,"earlier_workspace_sandbox_EP ERM_retained":True},indent=2)+"\n")
print("All six gates passed on the same frozen source",flush=True)
