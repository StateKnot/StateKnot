#!/usr/bin/env python3
import subprocess,sys
from pathlib import Path
root=Path(__file__).resolve().parent
for gate in ("fmt", "tcp"):
    result=subprocess.run([sys.executable,str(root/"run-gate.py"),gate])
    if result.returncode:
        sys.exit(result.returncode)
