#!/usr/bin/env python3
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0

"""Reproduce fixed seed regressions and bounded ASan/libFuzzer qualification."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
import tempfile

ROOT = Path(__file__).resolve().parent.parent
FUZZ = ROOT / "fuzz"
NIGHTLY = "nightly-2026-10-07"
TARGETS = ("bounded_json", "core_readers", "schema_registry")
MAX_BYTES = 128 * 1024
RESULTS = FUZZ / "results"
ENV = {**os.environ, "CARGO_NET_OFFLINE": "true", "CARGO_BUILD_JOBS": "2",
       "ASAN_OPTIONS": "detect_leaks=0:abort_on_error=1"}
for key in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "RUSTC", "RUSTC_WRAPPER",
            "RUSTC_WORKSPACE_WRAPPER", "RUSTUP_TOOLCHAIN"):
    ENV.pop(key, None)
REPORT = {
    "schema": "https://stateknot.github.io/schema/qualification/core-fuzz/1.0.0",
    "targets": {},
    "limits": {
        "max_input_bytes": MAX_BYTES,
        "max_mutation_runs_per_target": 10000,
        "max_seconds_per_target": 60,
        "max_seconds_per_input": 10,
        "rss_limit_mib": 2048,
        "build_jobs": 2,
        "sanitizer": "address",
        "cfg_fuzzing": False,
    },
    "complete": False,
}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def capture(args):
    return subprocess.check_output(args, cwd=ROOT, env=ENV, text=True, timeout=60).strip()


def execute(args, log, timeout):
    """Own the process group, including compiler/fuzzer children, on every exit."""
    with log.open("ab") as output:
        output.write(("\nCOMMAND " + json.dumps(args) + "\n").encode())
        output.flush()
        process = subprocess.Popen(
            args, cwd=ROOT, stdout=output, stderr=subprocess.STDOUT,
            env=ENV,
            start_new_session=True,
        )
        try:
            status = process.wait(timeout=timeout)
            if status:
                raise RuntimeError(f"exit {status}: {args[0]}; see {log}")
        finally:
            # A descendant may outlive a failed parent. The owned process group
            # also receives TERM/KILL when the qualification is interrupted.
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()


def source_snapshot():
    paths = subprocess.check_output(["git", "ls-files", "-co", "--exclude-standard", "-z"], cwd=ROOT)
    state = hashlib.sha256()
    for relative in sorted(set(paths.split(b"\0")) - {b""}):
        path = ROOT / os.fsdecode(relative)
        data = os.fsencode(os.readlink(path)) if path.is_symlink() else path.read_bytes()
        state.update(relative + b"\0" + hashlib.sha256(data).digest())
    return state.hexdigest()


def seed_corpus():
    fixtures = ROOT / "crates/stateknot-core/tests/fixtures"
    inventory = json.loads((fixtures / "core-public-type-inventory-v1.json").read_bytes())
    manifest = []

    def add(target, name, data, source):
        assert len(data) <= MAX_BYTES, (target, name, len(data))
        path = FUZZ / "corpus" / target / (name + "-" + digest(data)[:16])
        path.write_bytes(data)
        manifest.append({"target": target, "file": path.name,
                         "bytes": len(data), "sha256": digest(data), "source": source})

    for target in TARGETS:
        path = FUZZ / "corpus" / target
        if path.exists():
            # Only generated seeds are recreated. Discovered coverage cases
            # stay in results/corpus, and reviewed crashes belong in seeds/.
            shutil.rmtree(path)
        path.mkdir(parents=True)
        for retained in sorted((FUZZ / "seeds" / target).glob("*")):
            if retained.is_file():
                add(target, retained.name, retained.read_bytes(), str(retained.relative_to(ROOT)))

    def compact(value):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()

    readers = 0
    for name, entry in inventory["types"].items():
        if entry["mode"] != "read_write":
            continue
        readers += 1
        for index, vector in enumerate(entry["vectors"]):
            document = json.loads((fixtures / vector["fixture"]).read_bytes())
            wire = document
            if vector["pointer"]:
                for part in vector["pointer"].split("/")[1:]:
                    part = part.replace("~1", "/").replace("~0", "~")
                    wire = wire[int(part)] if isinstance(wire, list) else wire[part]
            prefix = (name + "\n").encode()
            source = vector["fixture"] + vector["pointer"]
            add("core_readers", f"{name}-{index}-valid", prefix + compact(wire), source)
            if isinstance(wire, dict) and name not in ("BoundedJson", "Extensions"):
                extra = {**wire, "untrusted_authority": True}
                add("core_readers", f"{name}-{index}-unknown", prefix + compact(extra), source)
                if wire:
                    key, value = next(iter(wire.items()))
                    duplicate = b"{" + compact(key) + b":" + compact(value) + b"," + compact(wire)[1:]
                    add("core_readers", f"{name}-{index}-duplicate", prefix + duplicate, source)
            for label, bad in (("null", None), ("array", []), ("object", {}), ("unicode", "\ud800")):
                # Invalid Unicode stays as raw JSON escape bytes, not a Python
                # surrogate encoded into invalid UTF-8 by the seed generator.
                payload = b'"\\ud800"' if label == "unicode" else compact(bad)
                add("core_readers", f"{name}-{index}-{label}", prefix + payload, source)
    REPORT["reader_count"] = readers
    assert readers == 310, "review and qualify an expanded public reader set"
    variants = json.loads((fixtures / "core-public-enum-variants-v1.json").read_bytes())
    variant_count = 0
    for name, vectors in variants["types"].items():
        assert inventory["types"][name]["kind"] == "enum"
        assert inventory["types"][name]["mode"] == "read_write"
        for vector in vectors:
            case = compact(vector["case"])
            add("core_readers", f"{name}-variant-{digest(case)[:16]}",
                (name + "\n").encode() + compact(vector["wire"]),
                "core-public-enum-variants-v1.json " + case.decode())
            variant_count += 1
    assert len(variants["types"]) == 71 and variant_count == 298
    REPORT["enum_type_count"] = len(variants["types"])
    REPORT["enum_variant_count"] = variant_count
    for name, data in (
        ("depth-65", b"[" * 65 + b"0" + b"]" * 65),
        ("entries-8193", b"[" + b"0," * 8192 + b"0]"),
        ("string-65537", b'"' + b"a" * 65537 + b'"'),
        ("key-1025", b'{"' + b"k" * 1025 + b'":0}'),
        ("raw-limit-whitespace", b" " * (MAX_BYTES - 4) + b"null"),
    ):
        add("bounded_json", name, data, "fixed malicious/resource-boundary generator")
    for name, size in (("aggregate-limit", 25 * 1024), ("single-limit", 33 * 1024)):
        add("schema_registry", name, compact({"schema": {"description": "a" * size}, "instance": None}),
            "fixed schema byte-limit generator")
    (RESULTS / "seeds.json").write_text(json.dumps(manifest, indent=2) + "\n")
    REPORT["seeds_manifest_sha256"] = digest((RESULTS / "seeds.json").read_bytes())


def main():
    global RESULTS
    if os.name != "posix":
        raise RuntimeError("this ASan qualification profile requires Linux or macOS")
    if len(sys.argv) != 1:
        raise RuntimeError("usage: python3 fuzz/qualify.py (fixed qualification limits)")
    (FUZZ / "results").mkdir(exist_ok=True)
    RESULTS = Path(tempfile.mkdtemp(prefix="qualification-", dir=FUZZ / "results"))
    REPORT["run_directory"] = str(RESULTS.relative_to(ROOT))
    source = source_snapshot()
    REPORT["source_files_sha256"] = source
    locks = {path: path.read_bytes() for path in (ROOT / "Cargo.lock", FUZZ / "Cargo.lock")}
    started = time.monotonic()
    try:
        REPORT["source_commit"] = capture(["git", "rev-parse", "HEAD"])
        REPORT["source_dirty"] = bool(capture(["git", "status", "--porcelain"]))
        if os.environ.get("CI") == "true" and REPORT["source_dirty"]:
            raise RuntimeError("CI qualification requires a clean immutable source")
        REPORT["rustc"] = capture(["rustup", "run", NIGHTLY, "rustc", "--version"])
        REPORT["cargo_fuzz"] = capture(["cargo", "+" + NIGHTLY, "fuzz", "--version"])
        assert REPORT["cargo_fuzz"] == "cargo-fuzz 0.13.2", "install the pinned engine"
        assert f'channel = "{NIGHTLY}"' in (FUZZ / "rust-toolchain.toml").read_text()
        REPORT["cargo_locks"] = {str(path.relative_to(ROOT)): digest(data) for path, data in locks.items()}
        package_sets = []
        for path in (ROOT / "Cargo.toml", FUZZ / "Cargo.toml"):
            metadata = json.loads(capture(["cargo", "+" + NIGHTLY, "metadata", "--manifest-path", str(path),
                                           "--locked", "--offline", "--format-version", "1"]))
            package_sets.append({(p["name"], p["version"], p["source"]) for p in metadata["packages"]})
        additions = {(name, version) for name, version, source in package_sets[1] - package_sets[0] if source}
        assert additions == {("arbitrary", "1.5.0"), ("libfuzzer-sys", "0.4.13")}, additions
        assert not any(name == "libfuzzer-sys" for name, _, _ in package_sets[0])
        REPORT["product_dependency_versions_preserved"] = True
        seed_corpus()
        execute(["cargo", "+" + NIGHTLY, "test", "--manifest-path", str(FUZZ / "Cargo.toml"),
                 "--lib", "--locked", "--offline"], RESULTS / "seed-regressions.log", 600)
        execute(["cargo", "+" + NIGHTLY, "fuzz", "build", "--fuzz-dir", str(FUZZ),
                 "--target-dir", str(FUZZ / "target"), "--no-cfg-fuzzing", "--codegen-units", "16"],
                RESULTS / "build.log", 1200)
        host = capture(["rustup", "run", NIGHTLY, "rustc", "-vV"]).split("host: ")[1].splitlines()[0]
        for target in TARGETS:
            binary = FUZZ / "target" / host / "release" / target
            seeds = sorted((FUZZ / "corpus" / target).glob("*"))
            artifacts = FUZZ / "artifacts" / target
            artifacts.mkdir(parents=True, exist_ok=True)
            output = RESULTS / "corpus" / target
            output.mkdir(parents=True, exist_ok=True)
            log = RESULTS / (target + ".log")
            flags = ["-max_len=" + str(MAX_BYTES), "-timeout=10", "-rss_limit_mb=2048",
                     "-artifact_prefix=" + str(artifacts) + "/", "-seed=20261008", "-print_final_stats=1"]
            record = {"seed_files": len(seeds), "seed_replay_passed": False, "mutation_passed": False,
                      "binary_sha256": digest(binary.read_bytes()), "flags": flags}
            REPORT["targets"][target] = record
            # Explicit file mode replays every seed without corpus minimization
            # or coverage selection. Small batches stay below platform ARG_MAX.
            for offset in range(0, len(seeds), 64):
                execute([str(binary)] + [str(path) for path in seeds[offset:offset + 64]] + flags, log, 120)
            record["seed_replay_passed"] = True
            execute([str(binary), str(output), str(FUZZ / "corpus" / target)] + flags
                    + ["-max_total_time=60", "-runs=10000"], log, 90)
            record["mutation_passed"] = True
            record["retained_coverage_files"] = len(list(output.glob("*")))
            record["retained_coverage_bytes"] = sum(path.stat().st_size for path in output.glob("*"))
            record["log_sha256"] = digest(log.read_bytes())
            print(f"{target}: {len(seeds)} seeds replayed; bounded mutations passed", flush=True)
        REPORT["complete"] = True
    finally:
        unchanged = all(path.read_bytes() == data for path, data in locks.items())
        REPORT["lockfiles_unchanged"] = unchanged
        REPORT["elapsed_seconds"] = round(time.monotonic() - started, 3)
        source_unchanged = source_snapshot() == source
        REPORT["source_files_unchanged"] = source_unchanged
        if not unchanged or not source_unchanged:
            REPORT["complete"] = False
        evidence = json.dumps(REPORT, indent=2) + "\n"
        (RESULTS / "qualification.json").write_text(evidence)
        (FUZZ / "results/qualification.json").write_text(evidence)
        print(f"qualification evidence: {RESULTS}", flush=True)
        if not unchanged or not source_unchanged:
            raise RuntimeError("qualification changed a frozen lockfile or source snapshot")


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda *_: sys.exit("qualification terminated"))
    main()
