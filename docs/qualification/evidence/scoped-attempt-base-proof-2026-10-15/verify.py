# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0

"""Verify frozen evidence; --workspace also checks the adopted build inputs."""

from pathlib import Path
import argparse
import gzip
import hashlib
import json
import re


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
PROFILE = ROOT / "docs/qualification/experimental-scoped-attempt-base-proof-2026-10-15.json"


def sha(data):
    return hashlib.sha256(data).hexdigest()


def read_json(path):
    return json.loads(path.read_text())


def raw_evidence(path):
    data = path.read_bytes()
    return gzip.decompress(data) if path.suffix == ".gz" else data


def function(text, marker):
    start = text.index(marker)
    return text[start:text.index("\n}", start) + 2]


def sql_literals(text):
    """Read literals outside comments, preserving source order and raw bytes."""
    raw = re.compile(r'(?:br|rb|r)(#*)"')
    char = re.compile(r"b?'(?:\\.|[^'\\\n])'")
    keyword = re.compile(r"\s*(SELECT|INSERT|UPDATE|DELETE|WITH|CREATE|ALTER|DROP|SET|BEGIN|COMMIT|ROLLBACK|LOCK|TRUNCATE|SAVEPOINT|RELEASE)\b")
    literals = []
    pos = 0
    while pos < len(text):
        if text.startswith("//", pos):
            end = text.find("\n", pos + 2)
            pos = end if end >= 0 else len(text)
            continue
        if text.startswith("/*", pos):
            depth = 1
            pos += 2
            while depth:
                assert pos < len(text), "unclosed comment"
                if text.startswith("/*", pos):
                    depth += 1
                    pos += 2
                elif text.startswith("*/", pos):
                    depth -= 1
                    pos += 2
                else:
                    pos += 1
            continue
        boundary = pos == 0 or not (text[pos - 1].isalnum() or text[pos - 1] == "_")
        matched = raw.match(text, pos) if boundary else None
        if matched:
            terminator = '"' + matched.group(1)
            start = matched.end()
            end = text.index(terminator, start)
            value = text[start:end]
            pos = end + len(terminator)
        else:
            character = char.match(text, pos)
            if character:
                pos = character.end()
                continue
            if text[pos] != '"':
                pos += 1
                continue
            start = pos + 1
            pos = start
            while text[pos] != '"':
                pos += 2 if text[pos] == "\\" else 1
            value = text[start:pos]
            pos += 1
        if keyword.match(value):
            literals.append(value)
    return literals


def main():
    if not __debug__:
        raise SystemExit("Run without -O or PYTHONOPTIMIZE so evidence checks remain active.")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", action="store_true")
    args = parser.parse_args()
    profile = read_json(PROFILE)
    inventory_path = HERE / "evidence-manifest.json"
    assert sha(inventory_path.read_bytes()) == profile["evidence_manifest_sha256"]
    inventory = read_json(inventory_path)
    for row in inventory:
        data = (HERE / row["path"]).read_bytes()
        assert sha(data) == row["sha256"], row["path"]
        if "uncompressed_sha256" in row:
            assert sha(gzip.decompress(data)) == row["uncompressed_sha256"], row["path"]

    manifest_path = HERE / "source-manifest.json"
    assert sha(manifest_path.read_bytes()) == profile["source"]["tested_manifest_sha256"]
    manifest = read_json(manifest_path)
    assert len(manifest) == 899
    source_hashes = {row["path"]: row["sha256"] for row in manifest}
    original = gzip.decompress((HERE / "original-a-store.rs.gz").read_bytes()).decode()
    baseline = gzip.decompress((HERE / "layout-baseline-store.rs.gz").read_bytes()).decode()
    before = (HERE / "start-helper-before.txt").read_text()
    after = (HERE / "start-helper-after.txt").read_text()
    assert original.count(before) == 1
    restored = original.replace(before, after, 1)
    store_path = "crates/stateknot-store-postgres/src/store.rs"
    assert sha(restored.encode()) == source_hashes[store_path]
    body_before = before.split("> {\n", 1)[1].rsplit("\n}", 1)[0]
    body_after = after.split("    Box::pin(async move {\n", 1)[1].rsplit("\n    })", 1)[0]
    pattern = r'"(?:\\.|[^"\\])*"|\x27[^\x27\n]*\x27|[A-Za-z_][A-Za-z_0-9]*|[^\s]'
    tokens = re.findall(pattern, body_before)
    assert len(tokens) == 123 and tokens == re.findall(pattern, body_after)
    assert "'start: 'a" in after and "&'a mut Transaction" in after
    patch = raw_evidence(HERE / "erase-only.patch.gz")
    assert sha(patch) == read_json(HERE / "source-proof.json")["patch_sha256"]
    assert sha(patch) == read_json(HERE / "independent-review.json")["patch_sha256"]
    assert sha(patch) == profile["compiler_repair"]["erasure_patch_uncompressed_sha256"]
    sql = sql_literals(baseline)
    assert len(sql) == 139 and sql == sql_literals(original) == sql_literals(restored)
    for name in ["verify_node_attempt_base_checkpoint", "verify_pending_node_result_base_checkpoint", "verify_pending_node_result_bindings", "verify_node_attempt_anchor", "load_node_attempt_record"]:
        marker = "async fn " + name + "("
        assert function(baseline, marker) == function(original, marker) == function(restored, marker), name

    summaries = re.compile(r"test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;")
    counts = {}
    for row in read_json(HERE / "gates/receipts.json")["receipts"]:
        path = HERE / "gates" / (row["gate"] + ".log")
        body = raw_evidence(path if path.exists() else path.with_suffix(".log.gz"))
        assert row["exit_code"] == 0 and sha(body) == row["log_sha256"]
        found = summaries.findall(body.decode())
        if found:
            assert all(state == "ok" and failed == "0" for state, _, failed, _ in found)
            counts[row["gate"]] = [len(found), sum(int(x[1]) for x in found), sum(int(x[3]) for x in found)]
    assert counts == {"workspace": [98, 2048, 6], "doctests": [10, 45, 0]}
    for major in [16, 17]:
        directory = HERE / ("native-pg" + str(major))
        receipt = read_json(directory / "receipt.json")
        assert receipt["all_passed"] and receipt["default_stack"] and len(receipt["results"]) == 6
        assert receipt["binary_sha256"] == profile["build"]["binary_sha256"]
        for index, row in enumerate(receipt["results"]):
            data = raw_evidence(directory / (str(index) + ".native.log.gz"))
            assert row["exit_code"] == 0 and row["valid_one_test_success"] and sha(data) == row["log_sha256"]
            assert summaries.findall(data.decode()) == [("ok", "1", "0", "0")]
        for candidate, expected_count in [("baseline", 93), ("a", 53)]:
            diagnostic = HERE / "diagnostic" / (candidate + "-pg" + str(major))
            observation = read_json(diagnostic / "receipt.json")
            assert sha(raw_evidence(diagnostic / "native.log.gz")) == observation["log_sha256"]
            assert sha(raw_evidence(diagnostic / "postgres-session-complete.log.gz")) == observation["session_complete_log_sha256"]
            assert len(observation["trials"]) == 5
            assert all(trial["sql_statements"] == expected_count for trial in observation["trials"])

    matched = 0
    if args.workspace:
        for row in manifest:
            name = row["path"]
            if name.startswith(("crates/", ".cargo/")) or name in ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "rustfmt.toml", "clippy.toml"]:
                assert sha((ROOT / name).read_bytes()) == row["sha256"], name
                matched += 1
        assert matched == 515
    print(json.dumps({"evidence_files": len(inventory), "body_tokens_equal": 123, "sql_literals_equal": 139, "workspace": counts["workspace"], "doctests": counts["doctests"], "selected_native_passed_per_postgres": 6, "workspace_inputs_checked": matched}))


if __name__ == "__main__":
    main()
