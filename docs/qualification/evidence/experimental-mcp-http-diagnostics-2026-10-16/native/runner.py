#!/usr/bin/env python3
"""Root-reviewed opt0/default-feature affected Native regression runner. Not run by author."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
from urllib.parse import quote

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[2]
TARGET = REPO / "target/r1-oct15-proof-base"
load = lambda p: json.loads(p.read_bytes())
sha = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
canonical = lambda v: json.dumps(v, sort_keys=True, separators=(",", ":"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="Root approval and exclusive resource window required")
    args = parser.parse_args()
    contract = load(ROOT / "inventory.json")
    if not args.execute:
        print(json.dumps({"state": "plan_only", "tests_per_major": {k: len(v) for k, v in contract["tests"].items()}, "execution": False}))
        return
    source = Path(contract["source_root"])
    expected = load(Path(contract["source_manifest"]))
    compiled = load(Path(contract["compiled_manifest"]))
    assert expected["source_tree_sha256"] == contract["source_tree"] and len(expected["files"]) == 928
    assert len(compiled["files"]) == 536
    assert hashlib.sha256(canonical(sorted(compiled["files"], key=lambda r: r["path"])).encode()).hexdigest() == contract["compiled_canonical_sha256"]
    for binding in contract["bindings"]:
        p = Path(binding["path"])
        assert p.stat().st_size == binding["bytes"] and sha(p) == binding["sha256"], str(p)
    gates = load(source.parent / "mcp-http-debug-qualification-v3/six-gates-summary.json")
    assert gates["state"] == "passed" and gates["gate_count"] == 6 and gates["source_tree"] == contract["source_tree"]
    assert len(gates["receipts"]) == 6 and all(r["exit_code"] == 0 for r in gates["receipts"])
    # Read-only lock acquisition; no edits to root's existing gate file.
    gate_lock = (source.parent / "mcp-http-debug-qualification-v3/gate.lock").open("rb")
    fcntl.flock(gate_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    out = ROOT / "execution-v1"
    out.mkdir(exist_ok=False)
    shutil.copy2(Path(__file__), out / "runner.py")
    shutil.copy2(ROOT / "inventory.json", out / "inventory.json")
    runner_sha = sha(Path(__file__))
    inventory_sha = sha(ROOT / "inventory.json")
    def put(name, value):
        (out / name).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    def check_source():
        files = {}
        for p in source.rglob("*"):
            assert not p.is_symlink(), str(p)
            if p.is_file():
                files[str(p.relative_to(source))] = (p.stat().st_size, sha(p))
        assert files == {r["path"]: (r["bytes"], r["sha256"]) for r in expected["files"]}, "source drift"
    def budget(label):
        assert sha(Path(__file__)) == sha(out / "runner.py") == runner_sha
        assert sha(ROOT / "inventory.json") == sha(out / "inventory.json") == inventory_sha
        check_source()
        raw = subprocess.check_output(["df", "-k", str(REPO)], text=True)
        (out / (label + ".df")).write_text(raw)
        assert int(raw.splitlines()[-1].split()[3]) >= 50 * 1024**2, "free space below50GiB; no automatic cleanup"
        rows = []
        for p in sorted((REPO / "target").glob("r1-oct*")):
            if p.is_dir():
                rows.append({"path": str(p), "logical_kib": int(subprocess.check_output(["du", "-sk", str(p)], text=True).split()[0])})
        total = sum(r["logical_kib"] for r in rows)
        put(label + "-inputs.json", {"source_tree": contract["source_tree"], "source_files": 928, "exact": True, "caches": rows, "total_logical_kib": total, "logical_size_is_not_APFS_reclaimable_space": True, "cache_cleanup": False})
        assert total <= 20 * 1024**2, "aggregate task caches exceed20GiB; stop for assessment"
    def assert_idle(label):
        assert TARGET.resolve() == TARGET and not TARGET.is_symlink()
        assert (TARGET / "CACHEDIR.TAG").read_bytes().startswith(b"Signature: 8a477f597d28d172789f06886806bc55")
        assert not subprocess.check_output(["git", "ls-files", "--", str(TARGET.relative_to(REPO))], cwd=REPO, env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"}).strip()
        p = subprocess.run(["lsof", "-nP", "+D", str(TARGET)], capture_output=True)
        (out / (label + "-cache-lsof.stdout")).write_bytes(p.stdout)
        (out / (label + "-cache-lsof.stderr")).write_bytes(p.stderr)
        assert p.returncode == 1 and not p.stdout.strip() and not p.stderr.strip(), "active or uninspectable cache"
        locks = []
        for path in TARGET.rglob(".cargo-lock"):
            with path.open("rb") as handle:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                fcntl.flock(handle, fcntl.LOCK_UN)
            locks.append(str(path))
        assert locks, "expected existing Cargo cache locks"
        put(label + "-cache-guard.json", {"target": str(TARGET), "tag_verified": True, "git_untracked": True, "lsof_empty": True, "locks_probeable": locks, "root_exclusive_window_still_required": True})
    # Same original opt0/debug0 strict environment as the completed source gates.
    override_names = ("RUST_MIN_STACK", "CARGO_ENCODED_RUSTFLAGS", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "RUSTC_BOOTSTRAP", "RUSTC")
    for name in override_names:
        assert not os.environ.get(name), "unexpected Rust override: " + name
    env = {k: v for k, v in os.environ.items() if not k.startswith(("STATEKNOT_", "CARGO_PROFILE_"))}
    env.update(CARGO_TARGET_DIR=str(TARGET), CARGO_INCREMENTAL="0", CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0", RUSTFLAGS="-D warnings", RUSTDOCFLAGS="-D warnings")
    for profile_name in ("DEV", "TEST"):
        env["CARGO_PROFILE_" + profile_name + "_OPT_LEVEL"] = "0"
        env["CARGO_PROFILE_" + profile_name + "_DEBUG_ASSERTIONS"] = "true"
        env["CARGO_PROFILE_" + profile_name + "_OVERFLOW_CHECKS"] = "true"
    allowed_env = ("CARGO_TARGET_DIR", "CARGO_INCREMENTAL", "RUSTFLAGS", "RUSTDOCFLAGS", "STATEKNOT_REQUIRE_POSTGRES_TESTS") + tuple("CARGO_PROFILE_" + p + "_" + key for p in ("DEV", "TEST") for key in ("DEBUG", "OPT_LEVEL", "DEBUG_ASSERTIONS", "OVERFLOW_CHECKS"))
    def run(label, command, active=None):
        budget(label + "-before")
        start = time.time()
        selected_env = active or env
        with (out / (label + ".log")).open("wb") as log:
            process = subprocess.Popen(command, cwd=source, env=selected_env, stdout=log, stderr=subprocess.STDOUT)
            print(json.dumps({"step": label, "pid": process.pid, "command": command}), flush=True)
            code = process.wait()
        receipt = {"command": command, "argv": command, "cwd": str(source), "runner_sha256": runner_sha, "inventory_sha256": inventory_sha, "pid": process.pid, "exit_code": code, "seconds": time.time() - start, "log_sha256": sha(out / (label + ".log")), "source_tree": contract["source_tree"], "rust_environment_allowlist": {k: selected_env.get(k) for k in allowed_env}, "override_presence": {k: bool(selected_env.get(k)) for k in override_names}, "default_stack_no_runner_override": not any(selected_env.get(k) for k in override_names)}
        put(label + ".receipt.json", receipt)
        budget(label + "-after")
        assert code == 0, label + " failed; raw log and receipt retained"
        return (out / (label + ".log")).read_bytes().decode()
    budget("initial")
    assert_idle("initial")
    run("rustc-version", ["rustc", "+1.88.0", "-Vv"])
    # Only these two packages; preserve dependencies, no target copy or broad clean.
    run("clean-affected-packages", ["cargo", "+1.88.0", "clean", "--manifest-path", str(source / "Cargo.toml"), "--target-dir", str(TARGET), "-p", "stateknot-integrations", "-p", "stateknot"])
    artifacts = out / "artifacts"
    artifacts.mkdir()
    binaries = {}
    for name in ("mcp_durable", "mcp_reconciliation"):
        package = contract["packages"][name]
        raw = run(name + "-fresh-build", ["cargo", "+1.88.0", "test", "-p", package, "--test", name, "--locked", "--no-run", "--message-format=json"])
        selected = []
        for line in raw.splitlines():
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if item.get("reason") == "compiler-artifact" and item.get("executable") and item["target"]["name"] == name:
                selected.append(item)
        assert len(selected) == 1
        item = selected[0]
        profile = item["profile"]
        assert item["fresh"] is False and item["target"]["kind"] == ["test"]
        assert profile["test"] is True and profile["opt_level"] == "0" and profile["debuginfo"] == 0 and profile["debug_assertions"] is True and profile["overflow_checks"] is True
        assert item["target"]["src_path"] == str(source / "crates" / package / "tests" / (name + ".rs"))
        executable = Path(item["executable"])
        assert executable.resolve().is_relative_to(TARGET)
        saved = artifacts / name
        shutil.copy2(executable, saved)
        saved.chmod(0o755)
        assert sha(saved) == sha(executable)
        listing = run(name + "-list", [str(saved), "--list"])
        names = re.findall(r"^(\S+): test$", listing, re.M)
        assert sorted(names) == contract["tests"][name] and len(names) == len(set(names))
        dep = executable.with_suffix(".d")
        if dep.is_file():
            shutil.copy2(dep, artifacts / (name + ".d"))
        binaries[name] = {"path": str(saved), "sha256": sha(saved), "bytes": saved.stat().st_size, "tests": names, "cargo_artifact": item, "features": "per-package default command, no workspace/all-features union"}
        put(name + "-binary.json", binaries[name])
    def markers_and_summary(raw, name):
        lines = raw.split("\n")
        count = len(contract["tests"][name])
        assert lines.count("running " + str(count) + " tests") == 1
        summaries = [line for line in lines if line.startswith("test result:")]
        assert len(summaries) == 1 and re.fullmatch(r"test result: ok\. " + str(count) + r" passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in [0-9.]+s", summaries[0])
        assert not any("STATEKNOT_" in line and "SKIPPED" in line for line in lines)
        actual = {}
        for prefix, wanted in contract["expected_markers"][name].items():
            found = [json.loads(line[len(prefix) + 1:]) for line in lines if line.startswith(prefix + "=")]
            assert sorted(map(canonical, found)) == sorted(map(canonical, wanted)), prefix
            actual[prefix] = found
        return {"passed": count, "failed": 0, "ignored": 0, "measured": 0, "filtered": 0, "markers": actual}
    config = load(Path(contract["task_container_config"]))
    database = contract["database"]
    assert re.fullmatch("[a-z0-9_]{1,63}", database)
    def inspect(pg):
        info = json.loads(subprocess.check_output(["docker", "inspect", pg["container"]]))[0]
        assert info["State"]["Running"] and info["Id"] == pg["expected_container_id"] and info["Image"] == pg["expected_image_id"]
        assert info["Config"]["Labels"] == {"stateknot.task": "r1-oct15"}
        assert info["NetworkSettings"]["Ports"]["5432/tcp"] == [{"HostIp": "127.0.0.1", "HostPort": str(pg["port"])}]
        return info
    def sql(pg, command):
        return subprocess.check_output(["docker", "exec", pg["container"], "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "postgres", "-At", "-c", command], text=True).strip()
    baseline_databases = {}
    for pg in config["postgres"]:
        inspect(pg)
        names = sql(pg, "SELECT datname FROM pg_database ORDER BY datname").splitlines()
        assert database not in names, "new database already exists; never drop/reuse"
        baseline_databases[pg["major"]] = names
        put("pg" + str(pg["major"]) + "-databases-before.json", names)
    records = []
    for pg in config["postgres"]:
        major = pg["major"]
        info = inspect(pg)
        version = sql(pg, "SHOW server_version")
        assert int(sql(pg, "SHOW server_version_num")) // 10000 == major
        cfg = dict(value.split("=", 1) for value in info["Config"]["Env"] if "=" in value)
        assert cfg.get("POSTGRES_USER", "postgres") == "postgres" and cfg.get("POSTGRES_PASSWORD")
        assert sql(pg, "SELECT count(*) FROM pg_database WHERE datname='" + database + "'") == "0"
        sql(pg, "CREATE DATABASE " + database)
        identity = {"major": major, "actual_version": version, "container": pg["container"], "container_id": info["Id"], "image_id": info["Image"], "configured_image": info["Config"]["Image"], "labels": info["Config"]["Labels"], "port": pg["port"], "new_absent_database_created": database, "database_oid": sql(pg, "SELECT oid FROM pg_database WHERE datname='" + database + "'"), "database_not_dropped": True}
        put("pg" + str(major) + "-identity.json", identity)
        active = dict(env)
        active.update(STATEKNOT_REQUIRE_POSTGRES_TESTS="1", STATEKNOT_TEST_DATABASE_URL="postgresql://postgres:" + quote(cfg["POSTGRES_PASSWORD"], safe="") + "@127.0.0.1:" + str(pg["port"]) + "/" + database)
        for name in ("mcp_durable", "mcp_reconciliation"):
            binary = binaries[name]
            assert sha(Path(binary["path"])) == binary["sha256"]
            raw = run("pg" + str(major) + "-" + name, [binary["path"], "--nocapture", "--test-threads=1"], active)
            verified = markers_and_summary(raw, name)
            assert sha(Path(binary["path"])) == binary["sha256"]
            row = {"major": major, "database": database, "target": name, "binary_sha256": binary["sha256"], **verified}
            put("pg" + str(major) + "-" + name + "-qualification.json", row)
            records.append(row)
        inspect(pg)
        names = sql(pg, "SELECT datname FROM pg_database ORDER BY datname").splitlines()
        put("pg" + str(major) + "-databases-after.json", names)
        assert set(names) == set(baseline_databases[major]) | {database}, "database inventory drift; preserve all data"
    budget("final")
    assert_idle("final")
    put("summary.json", {"state": "passed", "source_tree": contract["source_tree"], "compiled_canonical_sha256": contract["compiled_canonical_sha256"], "source_928_before_after_exact": True, "binaries": binaries, "postgres": records, "scope": "two affected default-feature suites only;32 harness tests across two majors, existing authorization/MRTR/effect/reconciliation regressions", "new_database_retained_each_major": database, "full_Store_Runtime_Native_CI_capacity_or_R1_qualified": False, "profile": "opt0/debug0, assertions/overflow true, default stack; original fixture budgets unchanged"})
    print(json.dumps({"state": "passed", "output": str(out), "postgres_suites": 4}), flush=True)


if __name__ == "__main__":
    main()
