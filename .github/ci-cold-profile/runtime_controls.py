#!/usr/bin/env python3
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0
"""Mandatory future Linux-only harmless controls, before any Cargo command."""
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve().parent
KEYS = ("LANG", "LC_ALL", "LC_CTYPE")
MODULES = ("locale_guard.sh", "runtime_controls.py", "rustc_wrapper.sh",
           "rustc_wrapper.py", "cold_probe.py")


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def snapshot(raw):
    fields = raw.split(b"\0")
    if len(fields) != 10 or fields[-1] != b"":
        raise RuntimeError("invalid closed Bash locale snapshot")
    result = {}
    for index, key in enumerate(KEYS):
        name, present, value = fields[index * 3:index * 3 + 3]
        if name != key.encode() or present not in (b"0", b"1") or (present == b"0" and value):
            raise RuntimeError("invalid Bash locale snapshot field")
        result[key] = {"present": present == b"1", "value": value.decode("utf-8", "surrogateescape")}
    return result


def current_locale():
    return {key: {"present": key in os.environ, "value": os.environ.get(key, "")} for key in KEYS}


def execute(argv, env, cwd, pass_fds=()):
    return subprocess.run(argv, env=env, cwd=cwd, pass_fds=pass_fds,
                          capture_output=True, timeout=5)


def main():
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        raise RuntimeError("runtime controls require actual Linux x86_64; no host substitution")
    proof = Path(os.environ["STATEKNOT_COLD_LOCALE_PROOF"]).resolve()
    baseline_raw = (proof / "baseline.nul").read_bytes()
    baseline = snapshot(baseline_raw)
    if current_locale() != baseline:
        raise RuntimeError("Python startup changed the original Bash locale snapshot")
    hashes = {name: sha((HERE / name).read_bytes()) for name in MODULES}
    report = proof / "runtime-controls.json"
    if report.exists():
        raise RuntimeError("runtime controls already recorded; no replay")
    control = Path(tempfile.mkdtemp(prefix="runtime-controls-", dir=proof))
    evidence = control / "evidence" / "rustc-argv"
    evidence.mkdir(parents=True)
    env = {key: value["value"] for key, value in baseline.items() if value["present"]}
    env.update(PATH=os.environ["PATH"], STATEKNOT_COLD_LOCALE_PROOF=str(proof),
               STATEKNOT_COLD_ROOT=str(control), STATEKNOT_COLD_STEP="harmless-runtime-control",
               PYTHONDONTWRITEBYTECODE="1")
    direct = execute(["/usr/bin/env"], env, control)
    wrapped = execute(["bash", str(HERE / "rustc_wrapper.sh"), "/usr/bin/env"], env, control)
    def only_locale(raw):
        values = dict(line.split(b"=", 1) for line in raw.split(b"\n") if b"=" in line)
        return {key: {"present": key.encode() in values,
                      "value": values.get(key.encode(), b"").decode("utf-8", "surrogateescape")}
                for key in KEYS}
    before, after = only_locale(direct.stdout), only_locale(wrapped.stdout)
    if direct.returncode != 0 or wrapped.returncode != 0 or before != baseline or after != baseline:
        raise RuntimeError("direct/wrapped original allowlist environment mismatch")
    (control / "environment.json").write_text(json.dumps({"before": before, "after": after}, indent=2) + "\n")
    stub = control / "compiler_stub.py"
    stub.write_text('import json,os,sys\n'
                    'a,b=map(int,os.environ["FD_CONTROL"].split(","))\n'
                    'print(json.dumps({"argv":sys.argv[1:],"cwd":os.getcwd(),'
                    '"jobserver":os.environ["CARGO_MAKEFLAGS"],'
                    '"fd_a":os.fstat(a).st_ino,"fd_b":os.fstat(b).st_ino,'
                    '"token":os.read(a,1).decode()}))\n'
                    'sys.exit(37)\n')
    left, right = os.pipe()
    arguments = ["space value", "--cfg", 'feature="control"', "--literal=$()`", "中文"]
    flags = "--jobserver-auth=%d,%d -j" % (left, right)
    env.update(FD_CONTROL="%d,%d" % (left, right), CARGO_MAKEFLAGS=flags)
    try:
        os.write(right, b"J")
        result = execute(["bash", str(HERE / "rustc_wrapper.sh"), sys.executable, str(stub), *arguments],
                         env, control, (left, right))
    finally:
        os.close(left)
        os.close(right)
    value = json.loads(result.stdout)
    if (result.returncode != 37 or value["argv"] != arguments or value["cwd"] != str(control)
            or value["jobserver"] != flags or value["fd_a"] != value["fd_b"] or value["token"] != "J"):
        raise RuntimeError("literal argv/cwd/inherited jobserver FD/token/exit37 control failed")
    (control / "forwarding.json").write_text(json.dumps(value, indent=2) + "\n")
    rejected = {}
    for name, locale in (("C_locale_rejected", "C"), ("invalid_locale_rejected", "stateknot_invalid.UTF-8")):
        negative = dict(env)
        negative.pop("STATEKNOT_COLD_LOCALE_PROOF")
        negative.update(LANG=locale, LC_ALL=locale, LC_CTYPE=locale,
                        RUNNER_TEMP=str(control), GITHUB_ENV=str(control / (name + ".github-env")))
        marker = control / (name + ".command-ran")
        result = execute(["bash", str(HERE / "locale_guard.sh"), "init", "/usr/bin/touch", str(marker)],
                         negative, control)
        if result.returncode == 0 or marker.exists():
            raise RuntimeError("unacceptable locale was executed: " + name)
        rejected[name] = True
        (control / (name + ".stderr")).write_bytes(result.stderr)
    drift = dict(env)
    drift["LC_ALL"] = "C.utf8" if env.get("LC_ALL") != "C.utf8" else "C.UTF-8"
    marker = control / "drift.command-ran"
    result = execute(["bash", str(HERE / "locale_guard.sh"), "check", "/usr/bin/touch", str(marker)],
                     drift, control)
    if result.returncode == 0 or marker.exists():
        raise RuntimeError("changed original locale bypassed entry guard")
    records = [json.loads(path.read_bytes()) for path in evidence.glob("*.json")]
    if len(records) != 2 or not any(record["argv"] == [sys.executable, str(stub), *arguments] for record in records):
        raise RuntimeError("actual wrapper argv observation mismatch")
    if current_locale() != baseline or (proof / "baseline.nul").read_bytes() != baseline_raw:
        raise RuntimeError("runtime controls changed original locale or baseline")
    value = {
        "schema": "stateknot.cold-locale-runtime-controls.v1", "state": "passed",
        "runtime_controls_executed": True,
        "platform": {"system": platform.system(), "machine": platform.machine()},
        "modules": hashes,
        "baseline": {"snapshot_sha256": sha(baseline_raw), "charmap": (proof / "charmap.txt").read_text().rstrip("\n"),
                     "variables": baseline},
        "controls": {"environment_preserved": True, "literal_argv_preserved": True,
                     "cwd_preserved": True, "jobserver_fds_and_token_preserved": True,
                     "exit37_preserved": True, "entry_locale_drift_rejected": True, **rejected},
        "scope": "harmless runtime shim controls only; no Cargo/rustc/DB/CI30m qualification",
        "cargo_or_real_rustc_executed": False,
        "control_directory": str(control),
    }
    with report.open("x") as output:
        json.dump(value, output, indent=2)
        output.write("\n")
    print(json.dumps({"schema": value["schema"], "state": "passed", "report": str(report)}, sort_keys=True))


if __name__ == "__main__":
    main()
