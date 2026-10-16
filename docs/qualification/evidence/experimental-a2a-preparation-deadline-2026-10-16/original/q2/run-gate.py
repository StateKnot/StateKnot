#!/usr/bin/env python3
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0
"""Execute one frozen-source gate; preserve every failed attempt separately."""

import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

OUT = Path(__file__).resolve().parent
REPO = OUT.parents[2]
SOURCE = OUT.parent / json.loads((OUT / "scope.json").read_bytes())["source_directory"]
GATES = {
    "red-policy": ["cargo", "+1.88.0", "test", "-p", "stateknot-integrations", "--test", "a2a_server_subscription", "initial_authority::initial_policy_deadline_shutdown_and_capacity_are_owned", "--locked", "--", "--exact", "--nocapture"],
    "initial-unit": ["cargo", "+1.88.0", "test", "-p", "stateknot-integrations", "--lib", "a2a_server::preparation_tests", "--locked", "--", "--nocapture"],
    "initial-tcp": ["cargo", "+1.88.0", "test", "-p", "stateknot-integrations", "--test", "a2a_server_subscription", "initial_authority::", "--locked", "--", "--nocapture"],
    "fmt": ["cargo", "+1.88.0", "fmt", "--all", "--check"],
    "unit": ["cargo", "+1.88.0", "test", "-p", "stateknot-integrations", "--lib", "a2a_server::subscription_tests", "--locked", "--", "--nocapture"],
    "tcp": ["cargo", "+1.88.0", "test", "-p", "stateknot-integrations", "--test", "a2a_server_subscription", "--locked", "--", "--nocapture"],
    "clippy": ["cargo", "+1.88.0", "clippy", "--workspace", "--all-targets", "--all-features", "--locked", "--", "-D", "warnings"],
    "workspace": ["cargo", "+1.88.0", "test", "--workspace", "--all-targets", "--all-features", "--locked"],
    "doctests": ["cargo", "+1.88.0", "test", "--workspace", "--doc", "--all-features", "--locked"],
    "rustdoc": ["cargo", "+1.88.0", "doc", "--workspace", "--all-features", "--no-deps", "--locked"],
    "nightly": ["cargo", "+nightly-2026-10-07", "check", "--workspace", "--all-targets", "--all-features", "--locked"],
}


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    name = sys.argv[1]
    assert name in GATES
    assert not (OUT / (name + ".receipt.json")).exists()
    lock = (OUT / "gate.lock").open("a+")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    manifest = json.loads((OUT / "frozen-source-manifest.json").read_bytes())
    runner_sha = sha(Path(__file__))

    def snapshot():
        rows = []
        for path in sorted(SOURCE.rglob("*")):
            assert not path.is_symlink()
            if path.is_file():
                rows.append({"path": str(path.relative_to(SOURCE)), "bytes": path.stat().st_size, "sha256": sha(path)})
        assert rows == manifest["files"], "frozen source drift"

    def disk(label):
        raw = subprocess.check_output(["df", "-k", str(REPO)], text=True)
        (OUT / (name + "-" + label + ".df")).write_text(raw)
        assert int(raw.splitlines()[-1].split()[3]) >= 50 * 1024**2
        caches = [{"path": str(path), "logical_kib": int(subprocess.check_output(["du", "-sk", str(path)], text=True).split()[0])} for path in sorted((REPO / "target").glob("r1-oct15*")) if path.is_dir()]
        total = sum(row["logical_kib"] for row in caches)
        (OUT / (name + "-" + label + "-caches.json")).write_text(json.dumps({"caches": caches, "total_logical_kib": total, "logical_sizes_are_not_APFS_reclaimable_space": True, "cleanup_performed": False}, indent=2) + "\n")
        assert total <= 20 * 1024**2

    overrides = ("RUST_MIN_STACK", "CARGO_ENCODED_RUSTFLAGS", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "RUSTC_BOOTSTRAP", "RUSTC")
    assert not any(os.environ.get(key) for key in overrides), "unexpected compiler or stack override"
    target = REPO / ("target/r1-oct15-proof-base" + ("-nightly" if name == "nightly" else ""))
    assert target.resolve() == target and not target.is_symlink()
    assert (target / "CACHEDIR.TAG").read_bytes().startswith(b"Signature: 8a477f597d28d172789f06886806bc55")
    env = {key: value for key, value in os.environ.items() if not key.startswith(("STATEKNOT_", "CARGO_PROFILE_"))}
    for key in overrides:
        env.pop(key, None)
    env.update(CARGO_TARGET_DIR=str(target), CARGO_INCREMENTAL="0", CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0", RUSTFLAGS="-D warnings", RUSTDOCFLAGS="-D warnings")
    for profile in ("DEV", "TEST"):
        env["CARGO_PROFILE_" + profile + "_OPT_LEVEL"] = "0"
        env["CARGO_PROFILE_" + profile + "_DEBUG_ASSERTIONS"] = "true"
        env["CARGO_PROFILE_" + profile + "_OVERFLOW_CHECKS"] = "true"
    snapshot()
    disk("before")
    command = GATES[name]
    started = time.time()
    with (OUT / (name + ".log")).open("wb") as log:
        process = subprocess.Popen(command, cwd=SOURCE, env=env, stdout=log, stderr=subprocess.STDOUT)
        running = {"gate": name, "argv": command, "cwd": str(SOURCE), "pid": process.pid, "runner_pid": os.getpid(), "runner_sha256": runner_sha, "start_unix": started, "source_tree": manifest["source_tree_sha256"], "environment_allowlist": {key: env[key] for key in sorted(env) if key.startswith("CARGO_PROFILE_") or key in {"CARGO_TARGET_DIR", "CARGO_INCREMENTAL", "RUSTFLAGS", "RUSTDOCFLAGS"}}, "override_presence": {key: key in env for key in overrides}, "native_database_execution": False}
        (OUT / "running.json").write_text(json.dumps(running, indent=2) + "\n")
        print(json.dumps(running), flush=True)
        code = process.wait()
    snapshot()
    assert sha(Path(__file__)) == runner_sha
    receipt = {**running, "exit_code": code, "seconds": time.time() - started, "log_sha256": sha(OUT / (name + ".log")), "source_before_after_exact": True}
    (OUT / (name + ".receipt.json")).write_text(json.dumps(receipt, indent=2) + "\n")
    disk("after")
    print(json.dumps(receipt), flush=True)
    sys.exit(code)


if __name__ == "__main__":
    main()
