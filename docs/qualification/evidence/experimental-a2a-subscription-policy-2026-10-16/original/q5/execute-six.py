#!/usr/bin/env python3
import json,subprocess,sys
from pathlib import Path
root=Path(__file__).resolve().parent
for gate in ("fmt", "clippy", "workspace", "doctests", "rustdoc", "nightly"):
    result=subprocess.run([sys.executable,str(root/"run-gate.py"),gate])
    if result.returncode:
        sys.exit(result.returncode)
receipts=[json.loads((root/(gate+".receipt.json")).read_bytes()) for gate in ("fmt","clippy","workspace","doctests","rustdoc","nightly")]
assert all(row["exit_code"]==0 and row["source_before_after_exact"] for row in receipts)
(root/"six-gates-summary.json").write_text(json.dumps({"state":"passed","gate_count":6,"source_tree":receipts[0]["source_tree"],"receipts":receipts},indent=2)+"\n")
print("All six gates passed on the same frozen source",flush=True)
