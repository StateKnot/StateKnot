#!/usr/bin/env python3
"""Current55-step measurement hooks. Prepared only. Never cleans resources."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import sys
import time
import uuid

HERE = Path(__file__).resolve().parent
GIB = 1024 ** 3
SIX = {"CARGO_INCREMENTAL": "0", "CARGO_PROFILE_DEV_DEBUG": "0",
       "CARGO_PROFILE_TEST_DEBUG": "0", "CARGO_PROFILE_TEST_DEBUG_ASSERTIONS": "true",
       "CARGO_PROFILE_TEST_OVERFLOW_CHECKS": "true"}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text())


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()


def config():
    value = read_json(HERE / "frozen-contract.json")
    if value.get("frozen") is not True or value.get("full_cold_contract_frozen") is not True:
        raise RuntimeError("Draft only: source/MCP acceptance contract has not been frozen")
    if value["mcp_expected_tests"] < 1 or not value["mcp_marker"]:
        raise RuntimeError("missing final mandatory MCP inventory/marker")
    return value


def source_check(contract):
    # Capture checkout facts only. The post-analyzer must compare independent
    # pre-dispatch anchor values, never treating these self-recorded facts as a freeze.
    expected_tree = git("rev-parse", "HEAD^{tree}")
    manifest_raw = (HERE / "source-manifest.json").read_bytes()
    if sha(manifest_raw) != contract["compiled_input_manifest_sha256"]:
        raise RuntimeError("compiled input manifest hash mismatch")
    entries = json.loads(manifest_raw)["files"]
    workspace = Path.cwd().resolve()
    seen = set()
    for item in entries:
        relative = Path(item["path"])
        if relative.is_absolute() or ".." in relative.parts or str(relative) in seen:
            raise RuntimeError("unsafe/duplicate compiled input path")
        seen.add(str(relative))
        path = workspace / relative
        if path.is_symlink() or workspace not in path.resolve().parents:
            raise RuntimeError("compiled input escapes checkout")
        raw = path.read_bytes()
        if len(raw) != item["bytes"] or sha(raw) != item["sha256"]:
            raise RuntimeError("compiled input mismatch: " + str(relative))
    if len(entries) != contract["compiled_input_count"]:
        raise RuntimeError("compiled input path count mismatch")
    subprocess.run(["git", "diff", "--exit-code", "--quiet", "HEAD", "--"], check=True)
    unexpected = git("ls-files", "--others", "--exclude-standard").splitlines()
    if any(Path(p).suffix in {".rs", ".sql", ".toml"} or p.startswith("crates/") for p in unexpected):
        raise RuntimeError("untracked possible compiled input appeared")
    workflow_path = Path(contract["measurement_workflow_path"])
    if workflow_path.is_absolute() or ".." in workflow_path.parts:
        raise RuntimeError("unsafe measurement workflow path")
    workflow_hash = sha((workspace / workflow_path).read_bytes())
    if workflow_hash != contract["measurement_workflow_sha256"]:
        raise RuntimeError("actual adopted measurement workflow bytes differ")
    return {"workflow_path": str(workflow_path), "workflow_sha256": workflow_hash,
            "commit": git("rev-parse", "HEAD"), "tree": expected_tree,
            "compiled_input_count": len(entries), "manifest_sha256": sha(manifest_raw),
            "cargo_lock_sha256": sha(Path("Cargo.lock").read_bytes())}


def disk_guard(target=None):
    free = shutil.disk_usage(Path.cwd()).free
    if free < 50 * GIB:
        raise RuntimeError("less than 50GiB available; retain evidence and stop, no automatic deletion")
    size = 0
    if target and target.exists():
        size = sum(p.stat().st_size for p in target.rglob("*") if p.is_file() and not p.is_symlink())
        if size > 20 * GIB:
            raise RuntimeError("task target exceeds 20GiB; retain evidence and stop, no automatic deletion")
        tag = target / "CACHEDIR.TAG"
        if tag.exists() and not tag.read_bytes().startswith(b"Signature: 8a477f597d28d172789f06886806bc55"):
            raise RuntimeError("invalid actual Cargo CACHEDIR.TAG")
    return {"free_bytes": free, "target_logical_bytes": size,
            "size_is_not_APFS_reclaimable_space": True}



def runtime_control_check():
    proof = Path(os.environ["STATEKNOT_COLD_LOCALE_PROOF"])
    temporary = Path(os.environ["RUNNER_TEMP"]).resolve()
    if proof.is_symlink() or temporary not in proof.resolve().parents:
        raise RuntimeError("locale controls proof must be the actual runner temporary directory")
    report_raw = (proof / "runtime-controls.json").read_bytes()
    report = json.loads(report_raw)
    required = {"environment_preserved", "literal_argv_preserved", "cwd_preserved",
                "jobserver_fds_and_token_preserved", "exit37_preserved",
                "C_locale_rejected", "invalid_locale_rejected", "entry_locale_drift_rejected"}
    modules = {name: sha((HERE / name).read_bytes()) for name in
               ("locale_guard.sh", "runtime_controls.py", "rustc_wrapper.sh", "rustc_wrapper.py", "cold_probe.py")}
    baseline = (proof / "baseline.nul").read_bytes()
    fields = baseline.split(b"\0")
    expected = []
    variables = {}
    for key in ("LANG", "LC_ALL", "LC_CTYPE"):
        present = key in os.environ
        value = os.environ.get(key, "")
        expected.extend([key.encode(), b"1" if present else b"0", value.encode("utf-8", "surrogateescape")])
        variables[key] = {"present": present, "value": value}
    expected.append(b"")
    if fields != expected or report.get("baseline") != {"snapshot_sha256": sha(baseline),
            "charmap": "UTF-8", "variables": variables} or (proof / "charmap.txt").read_bytes() != b"UTF-8\n":
        raise RuntimeError("runtime controls do not bind the original Bash locale allowlist")
    if (report.get("schema") != "stateknot.cold-locale-runtime-controls.v1"
            or report.get("state") != "passed" or report.get("runtime_controls_executed") is not True
            or report.get("platform") != {"system": "Linux", "machine": "x86_64"}
            or report.get("modules") != modules or set(report.get("controls", {})) != required
            or any(v is not True for v in report["controls"].values())
            or report.get("cargo_or_real_rustc_executed") is not False):
        raise RuntimeError("actual Linux environment/FD/argv/exit37 controls not complete or assets differ")
    return proof, {"sha256": sha(report_raw), "state": "passed", "runtime_controls_executed": True,
                   "baseline_snapshot_sha256": sha(baseline), "locale_allowlist": variables,
                   "controls": report["controls"], "modules": modules}


def setup():
    contract = config()
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        raise RuntimeError("this draft measures the original Linux x86_64 CI profile only")
    for key in ("RUSTC", "RUSTC_BOOTSTRAP", "CARGO_BUILD_RUSTC", "CARGO_BUILD_RUSTC_WRAPPER",
                "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER", "CARGO_BUILD_INCREMENTAL",
                "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTDOCFLAGS", "RUSTC_WRAPPER",
                "RUSTC_WORKSPACE_WRAPPER", "RUST_MIN_STACK", "RUST_TEST_THREADS", "CARGO_BUILD_JOBS",
                "CARGO_BUILD_TARGET", "CARGO_BUILD_RUSTFLAGS", "CARGO_BUILD_RUSTDOCFLAGS"):
        if os.environ.get(key):
            raise RuntimeError("unexpected graph/resource override: " + key)
    allowed_profile = set(SIX) | {"CARGO_PROFILE_TEST_OPT_LEVEL"}
    if any(k.startswith("CARGO_PROFILE_") and k not in allowed_profile for k in os.environ):
        raise RuntimeError("unexpected profile override")
    if any(k.startswith("CARGO_TARGET_") and k != "CARGO_TARGET_DIR" for k in os.environ):
        raise RuntimeError("unexpected target-specific compiler override")
    opt = os.environ["CARGO_PROFILE_TEST_OPT_LEVEL"]
    if opt not in {"1", "2"} or opt != contract["expected_profile_opt"] or any(os.environ.get(k) != v for k, v in SIX.items()):
        raise RuntimeError("six required profile settings differ")
    proof, runtime_controls = runtime_control_check()
    # An existing target is never inspected as reusable or copied. mkdir fails if this path exists.
    root = Path(os.environ["RUNNER_TEMP"]) / ("stateknot-cold-" + uuid.uuid4().hex)
    root.mkdir(mode=0o700)
    target = root / "target"
    target.mkdir()
    evidence = root / "evidence"
    evidence.mkdir()
    (evidence / "rustc-argv").mkdir()
    if any(p.is_symlink() for p in proof.rglob("*")):
        raise RuntimeError("linked runtime control artifact")
    shutil.copytree(proof, evidence / "locale-controls")
    with open(os.environ["GITHUB_OUTPUT"], "a") as out:
        out.write(f"evidence={evidence}\n")
    before = disk_guard(target)
    source = source_check(contract)
    cargo_home = Path(os.environ.get("CARGO_HOME", str(Path.home() / ".cargo")))
    configs = [cargo_home / "config", cargo_home / "config.toml"]
    configs += [parent / ".cargo" / name for parent in [Path.cwd(), *Path.cwd().parents]
                for name in ("config", "config.toml")]
    present = {str(p): sha(p.read_bytes()) for p in configs if p.is_file()}
    if present != contract["allowed_cargo_config_sha256"]:
        raise RuntimeError("Cargo configuration differs from frozen contract")
    container = os.environ["STATEKNOT_COLD_PG_CONTAINER"]
    expected_image = os.environ["STATEKNOT_COLD_PG_IMAGE"]
    actual_image = subprocess.check_output(["docker", "inspect", "--format", "{{.Config.Image}}", container], text=True).strip()
    state = subprocess.check_output(["docker", "inspect", "--format", "{{.State.Running}} {{.State.Health.Status}}", container], text=True).strip()
    version = subprocess.check_output(["docker", "exec", container, "psql", "--no-psqlrc", "-U", "postgres", "-d", "stateknot_test", "-Atqc", "SHOW server_version;"], text=True).strip()
    if actual_image != expected_image or state != "true healthy" or version.split(".")[0] != os.environ["STATEKNOT_COLD_PG_MAJOR"]:
        raise RuntimeError("actual existing CI service image/health/major mismatch")
    (HERE / "rustc_wrapper.sh").chmod(0o755)
    write_json(evidence / "preflight.json", {"source": source, "disk": before, "runtime_controls": runtime_controls,
        "target_was_new_and_empty": not any(target.iterdir()), "target": str(target),
        "artifact_cache_restore": False, "incremental": False,
        "download_cache": str(cargo_home), "download_cache_may_be_warm": True,
        "opt_level": opt, "assertions": True, "overflow_checks": True,
        "profile_environment": {key: os.environ.get(key) for key in sorted(set(SIX) | {"CARGO_PROFILE_TEST_OPT_LEVEL"})},
        "wrapper_sha256": sha((HERE / "rustc_wrapper.py").read_bytes()),
        "wrapper_entry_sha256": sha((HERE / "rustc_wrapper.sh").read_bytes()),
        "locale_guard_sha256": sha((HERE / "locale_guard.sh").read_bytes()),
        "runtime_controls_sha256": sha((HERE / "runtime_controls.py").read_bytes()),
        "probe_sha256": sha(Path(__file__).read_bytes()), "configs": present,
        "contract_sha256": sha((HERE / "frozen-contract.json").read_bytes()),
        "run_bodies_sha256": sha((HERE / "measured-run-bodies.json").read_bytes()),
        "actual_postgres": {"container": container, "image": actual_image, "health": state, "version": version}})
    with (evidence / "df-before.txt").open("w") as out:
        subprocess.run(["df", "-Pk", str(Path.cwd()), str(root)], stdout=out, check=True)
    with open(os.environ["GITHUB_ENV"], "a") as out:
        out.write(f"STATEKNOT_COLD_ROOT={root}\nCARGO_TARGET_DIR={target}\n")
        out.write(f"RUSTC_WRAPPER={HERE / 'rustc_wrapper.sh'}\nPYTHONDONTWRITEBYTECODE=1\n")
    print("STATEKNOT_COLD_PREFLIGHT=" + json.dumps({"source": source, "opt_level": opt,
          "cold_artifacts": True, "network_downloads_cold": False}, sort_keys=True), flush=True)


def shell(script):
    root = Path(os.environ["STATEKNOT_COLD_ROOT"])
    evidence = root / "evidence"
    raw = Path(script).read_bytes()
    # GitHub adds a terminal newline to run bodies; no other normalization is accepted.
    key = sha(raw.rstrip(b"\n"))
    steps = read_json(HERE / "measured-run-bodies.json")
    match = [s for s in steps if sha(s["run"].encode().rstrip(b"\n")) == key]
    if len(match) != 1:
        raise RuntimeError("unknown or ambiguous measured command body")
    step = match[0]
    disk = disk_guard(root / "target")
    start = time.monotonic_ns()
    event = {"step": step["label"], "body_sha256": sha(raw), "start_monotonic_ns": start,
             "disk_before": disk, "exit_code": None, "complete": False}
    event_path = evidence / (step["label"] + ".json")
    if event_path.exists():
        raise RuntimeError("step already has an execution record; no replay")
    write_json(event_path, event)
    if ("cargo test" in step["run"] or "conformance/agent-identity/run.sh" in step["run"]) and not (evidence / "toolchain.txt").exists():
        rust = subprocess.check_output(["rustc", "-Vv"], text=True)
        cargo = subprocess.check_output(["cargo", "-V"], text=True)
        if not rust.startswith("rustc 1.88.0 ") or not cargo.startswith("cargo 1.88.0 "):
            raise RuntimeError("actual compiler/Cargo differs from pinned 1.88.0")
        (evidence / "toolchain.txt").write_text(rust + cargo)
    child_env = os.environ.copy()
    child_env["STATEKNOT_COLD_STEP"] = step["label"]
    print("\nSTATEKNOT_COLD_STEP_BEGIN=" + json.dumps({"step": step["label"]}), flush=True)
    # Original Linux unspecified-shell semantics: bash -e, preserving each body's own pipefail.
    child = subprocess.Popen(["bash", "-e", script], env=child_env)
    def forward(signum, _frame):
        event["interrupted_signal"] = signum
        write_json(event_path, event)
        if child.poll() is None:
            child.send_signal(signum)
    signal.signal(signal.SIGTERM, forward)
    signal.signal(signal.SIGINT, forward)
    result = child.wait()
    event.update(exit_code=result, complete="interrupted_signal" not in event,
                 end_monotonic_ns=time.monotonic_ns())
    event["elapsed_seconds"] = (event["end_monotonic_ns"] - start) / 1e9
    event["disk_after"] = disk_guard(root / "target")
    write_json(event_path, event)
    print("\nSTATEKNOT_COLD_STEP_END=" + json.dumps(event, sort_keys=True), flush=True)
    return result if result >= 0 else 128 - result


def rustc():
    root = Path(os.environ["STATEKNOT_COLD_ROOT"])
    event = {"step": os.environ.get("STATEKNOT_COLD_STEP"), "monotonic_ns": time.monotonic_ns(),
             "pid": os.getpid(), "argv": sys.argv[1:], "cwd": os.getcwd(),
             "argv_is_actual_unit_evidence_not_unit_duration": True}
    path = root / "evidence" / "rustc-argv" / (str(time.time_ns()) + "-" + str(os.getpid()) + ".json")
    with path.open("x") as out:
        json.dump(event, out, sort_keys=True)
        out.write("\n")
    # Literal argv forwarded without feature/profile flags, shell expansion, retry, or a new rustc child.
    os.execvp(sys.argv[1], sys.argv[1:])


def audit():
    contract = config()
    root = Path(os.environ["STATEKNOT_COLD_ROOT"])
    with (root / "evidence" / "df-after.txt").open("w") as out:
        subprocess.run(["df", "-Pk", str(Path.cwd()), str(root)], stdout=out, check=True)
    tag = root / "target" / "CACHEDIR.TAG"
    if not tag.is_file():
        raise RuntimeError("no actual Cargo CACHEDIR.TAG after required compilation")
    records = [read_json(root / "evidence" / (s["label"] + ".json"))
               for s in read_json(HERE / "measured-run-bodies.json")
               if (root / "evidence" / (s["label"] + ".json")).exists()]
    expected = len(read_json(HERE / "measured-run-bodies.json"))
    summary = {"source_after": source_check(contract), "disk_after": disk_guard(root / "target"),
               "runtime_controls_after": runtime_control_check()[1],
               "expected_measured_run_bodies": expected, "observed": len(records),
               "all_run_bodies_terminal_zero": len(records) == expected and all(
                   r["complete"] and r["exit_code"] == 0 for r in records),
               "full_job_qualified": False,
               "remaining_verification": "raw libtest/markers, all action steps and final GitHub job/30m conclusion required"}
    write_json(root / "evidence" / "audit.json", summary)
    print("STATEKNOT_COLD_AUDIT=" + json.dumps(summary, sort_keys=True), flush=True)
    # An always audit cannot convert an interrupted/missing original step into a successful profile.
    return 0 if summary["all_run_bodies_terminal_zero"] else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["setup", "shell", "audit"])
    parser.add_argument("script", nargs="?")
    args = parser.parse_args()
    result = {"setup": setup, "audit": audit}.get(args.mode)
    sys.exit(shell(args.script) if args.mode == "shell" else (result() or 0))
