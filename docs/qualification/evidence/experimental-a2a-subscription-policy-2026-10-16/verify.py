#!/usr/bin/env python3
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0
"""Pure offline source/receipt verifier. Never executes preserved gate runners."""
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import tarfile

HERE = Path(__file__).resolve().parent
TREE = "0e91c287e33ccf7f33c0f11e3ed0c204c2826f3373c4a6f3e701a56cd66c7bad"
COMPILED = "43e9cb48b9fe45bb8631b99fe9548db84f8e652dba2d147c8198bd04dff3a6f4"
BASE = "31a5ff5596ead9f859a20e7b483d6b660e0577fcf47327ec0a572eb3493e6fca"
GATES = ("fmt", "clippy", "workspace", "doctests", "rustdoc", "nightly")
COMMANDS = {
    "fmt": ["cargo", "+1.88.0", "fmt", "--all", "--check"],
    "clippy": ["cargo", "+1.88.0", "clippy", "--workspace", "--all-targets", "--all-features", "--locked", "--", "-D", "warnings"],
    "workspace": ["cargo", "+1.88.0", "test", "--workspace", "--all-targets", "--all-features", "--locked"],
    "doctests": ["cargo", "+1.88.0", "test", "--workspace", "--doc", "--all-features", "--locked"],
    "rustdoc": ["cargo", "+1.88.0", "doc", "--workspace", "--all-features", "--no-deps", "--locked"],
    "nightly": ["cargo", "+nightly-2026-10-07", "check", "--workspace", "--all-targets", "--all-features", "--locked"],
    "unit": ["cargo", "+1.88.0", "test", "-p", "stateknot-integrations", "--lib", "a2a_server::subscription_tests", "--locked", "--", "--nocapture"],
    "tcp": ["cargo", "+1.88.0", "test", "-p", "stateknot-integrations", "--test", "a2a_server_subscription", "--locked", "--", "--nocapture"],
}
sha = lambda data: hashlib.sha256(data).hexdigest()
canon = lambda value: json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def safe_path(value):
    path = Path(value)
    assert value and not path.is_absolute() and ".." not in path.parts, value
    return path


def apply_patch(files, raw, reverse=False):
    """Exact byte/context application of the archived fixture unified diffs."""
    lines = raw.splitlines(keepends=True)
    result = dict(files)
    index = 0
    while index < len(lines):
        assert lines[index].startswith(b"--- a/"), lines[index]
        name = lines[index][6:].rstrip(b"\n").decode()
        safe_path(name)
        index += 1
        assert lines[index] == ("+++ b/" + name + "\n").encode()
        index += 1
        source = files.get(name, b"").splitlines(keepends=True)
        output, cursor = [], 0
        while index < len(lines) and lines[index].startswith(b"@@ "):
            match = re.fullmatch(rb"@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@[^\n]*\n", lines[index])
            assert match, lines[index]
            a, ac, b, bc = match.groups()
            start, old_count, new_count = (int(b), int(bc or b"1"), int(ac or b"1")) if reverse else (int(a), int(ac or b"1"), int(bc or b"1"))
            start = max(start - 1, 0)
            assert start >= cursor
            output.extend(source[cursor:start])
            cursor = start
            index += 1
            old_seen = new_seen = 0
            while old_seen < old_count or new_seen < new_count:
                line = lines[index]
                index += 1
                kind, content = line[:1], line[1:]
                if reverse:
                    kind = {b"+": b"-", b"-": b"+", b" ": b" "}[kind]
                if kind in (b" ", b"-"):
                    assert source[cursor] == content, (name, cursor, line)
                    cursor += 1
                    old_seen += 1
                if kind in (b" ", b"+"):
                    output.append(content)
                    new_seen += 1
                assert kind in (b" ", b"-", b"+")
            assert (old_seen, new_seen) == (old_count, new_count)
        output.extend(source[cursor:])
        result[name] = b"".join(output)
    return result


def check_rows(files, manifest):
    rows = manifest["files"]
    assert len(files) == manifest["count"] == len(rows)
    assert len({row["path"] for row in rows}) == len(rows)
    actual = [{"path": row["path"], "bytes": len(files[row["path"]]), "sha256": sha(files[row["path"]])} for row in rows]
    assert actual == rows
    assert sha(canon(rows)) == manifest["source_tree_sha256"]


def reconstruct_base(repo):
    """Source-only A2A-facts -> PITR fixture -> lossless MCP -> Debug chain."""
    old = repo / "docs/qualification/evidence/experimental-mcp-lossless-bound-state-2026-10-16"
    refs = json.loads((old / "source/base-references.json").read_bytes())["files"]
    with tarfile.open(fileobj=io.BytesIO((repo / refs[0]["path"]).read_bytes()), mode="r:gz") as archive:
        members = archive.getmembers()
        assert len(members) == len({member.name for member in members}) == 923
        assert all(member.isfile() and member.name.startswith("source/") for member in members)
        for member in members:
            safe_path(member.name)
        files = {member.name[7:]: archive.extractfile(member).read() for member in members}
    files["crates/stateknot-runtime/tests/postgres/child_join_pitr.rs"] = gzip.decompress((repo / refs[2]["path"]).read_bytes())
    for change in json.loads((old / "source/final-reconstruction.json").read_bytes())["changes"]:
        assert (sha(files[change["path"]]) if change["path"] in files else None) == change["before_sha256"]
        data = gzip.decompress((old / change["payload"]).read_bytes())
        assert (len(data), sha(data)) == (change["bytes"], change["after_sha256"])
        files[change["path"]] = data
    manifest = json.loads(gzip.decompress((old / "source/final-source-manifest.json.gz").read_bytes()))
    check_rows(files, manifest)
    assert manifest["source_tree_sha256"] == "69af3a0f28d333f4e67d2b2cfcc2afc071938a6ce398527cc098d31ee8db60d8"
    debug = repo / "docs/qualification/evidence/experimental-mcp-http-diagnostics-2026-10-16"
    for change in json.loads((debug / "source/reconstruction.json").read_bytes())["changes"]:
        assert sha(files[change["path"]]) == change["before_sha256"]
        data = gzip.decompress((debug / change["payload"]).read_bytes())
        assert (len(data), sha(data)) == (change["bytes"], change["after_sha256"])
        files[change["path"]] = data
    manifest = json.loads(gzip.decompress((debug / "source/final-source-manifest.json.gz").read_bytes()))
    check_rows(files, manifest)
    assert manifest["source_tree_sha256"] == BASE and len(files) == 928
    return files


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", type=Path, help="repository containing the existing published source references")
    parser.add_argument("--workspace", action="store_true", help="check the exact 538 supplied compilation inputs; not a whole Git tree/read closure")
    parser.add_argument("--workspace-root", type=Path, help="checkout/candidate to bytecheck; defaults to repository root")
    args = parser.parse_args()
    repo = args.repository_root.resolve() if args.repository_root else HERE.parents[3]
    workspace = args.workspace_root.resolve() if args.workspace_root else repo
    manifest_raw = (HERE / "manifest.json").read_bytes()
    seal = json.loads((HERE / "seal.json").read_bytes())
    assert sha(manifest_raw) == seal["manifest_sha256"]
    manifest = json.loads(manifest_raw)
    for row in manifest["artifacts"]:
        path = HERE / safe_path(row["path"])
        assert not path.is_symlink()
        data = path.read_bytes()
        assert (len(data), sha(data)) == (row["bytes"], row["sha256"]), row["path"]
    actual_names = {str(p.relative_to(HERE)) for p in HERE.rglob("*") if p.is_file() and p != HERE / "seal.json"}
    assert actual_names == {row["path"] for row in manifest["artifacts"]} | {"manifest.json"}
    index = {row["path"]: row for row in json.loads((HERE / "artifact-index.json").read_bytes())["artifacts"]}

    def raw(name):
        name = name if name in index else name + ".gz"
        row = index[name]
        data = (HERE / safe_path(name)).read_bytes()
        assert (len(data), sha(data)) == (row["stored_bytes"], row["stored_sha256"]), name
        if row["storage_encoding"] == "gzip":
            assert data[4:8] == b"\0" * 4 and not data[3] & 8, name
            data = gzip.decompress(data)
        else:
            assert row["storage_encoding"] == "identity"
        assert (len(data), sha(data)) == (row["raw_bytes"], row["raw_sha256"]), name
        return data

    obj = lambda name: json.loads(raw(name))
    for name in index:
        raw(name)
    for row in obj("source/base-references.json")["reused_source_only_refs"]:
        data = (repo / safe_path(row["path"])).read_bytes()
        assert (len(data), sha(data)) == (row["bytes"], row["sha256"]), row["path"]
    base = reconstruct_base(repo)
    final = dict(base)
    changes = obj("source/reconstruction.json")["changes"]
    assert len(changes) == 3
    for change in changes:
        assert (sha(base[change["path"]]) if change["path"] in base else None) == change["before_sha256"]
        data = raw(change["payload"])
        assert (len(data), sha(data)) == (change["bytes"], change["after_sha256"])
        final[change["path"]] = data
    assert apply_patch(base, raw("source/candidate.patch")) == final
    fm = obj("source/final-source-manifest.json")
    check_rows(final, fm)
    assert fm["count"] == 930 and fm["source_tree_sha256"] == TREE
    inputs = obj("source/compiled-inputs.json")
    assert inputs["count"] == len(inputs["files"]) == 538
    assert inputs["files"] == sorted(inputs["files"], key=lambda row: row["path"])
    assert sha(canon(inputs["files"])) == inputs["sha256"] == COMPILED
    for row in inputs["files"]:
        assert (len(final[row["path"]]), sha(final[row["path"]])) == (row["bytes"], row["sha256"])
    # Historical sources reconstructed from final payloads and exact original patches.
    v3 = apply_patch(final, raw("original/q5/fixture-clippy-fix.patch.gz"), reverse=True)
    v4 = apply_patch(v3, raw("original/q4/fixture-clippy-fix.patch.gz"))
    v2 = apply_patch(v3, raw("original/q3/dynamic-error-timestamp-oracle-fix.patch.gz"), reverse=True)
    v1 = apply_patch(v2, raw("original/q2/fixture-eof-repoll-fix.patch.gz"), reverse=True)
    historical = {"q1": v1, "q2": v2, "q3": v3, "q4": v4, "q5": final, "q5-network-enabled-retry-v1": final}
    prod = "crates/stateknot-integrations/src/a2a_server.rs"
    assert all(files[prod] == final[prod] for files in historical.values())
    def method(data, marker):
        start = data.index(marker)
        return data[start:data.index(b"\n    }\n", start) + 7]
    for marker in (b"    async fn prepare(", b"    async fn unary<T>(", b"    async fn stream(", b"    fn bounded_stream("):
        assert method(base[prod], marker) == method(final[prod], marker), marker
    for label, files in historical.items():
        check_rows(files, obj("original/" + label + "/frozen-source-manifest.json"))
    # Every preserved gate receipt binds its original runner, original source and raw log.
    gate_receipts = 0
    for name in index:
        if not name.startswith("original/") or not name.endswith(".receipt.json"):
            continue
        receipt = obj(name)
        if "gate" not in receipt:
            continue
        assert receipt["argv"] == COMMANDS[receipt["gate"]]
        parent = str(Path(name).parent)
        source = obj(parent + "/frozen-source-manifest.json")
        assert sha(raw(parent + "/run-gate.py")) == receipt["runner_sha256"]
        assert sha(raw(parent + "/" + receipt["gate"] + ".log")) == receipt["log_sha256"]
        assert receipt["source_tree"] == source["source_tree_sha256"]
        assert receipt["source_before_after_exact"] is True and receipt["native_database_execution"] is False
        assert not any(receipt["override_presence"].values())
        env = receipt["environment_allowlist"]
        for key, value in {"CARGO_INCREMENTAL": "0", "CARGO_PROFILE_DEV_DEBUG": "0", "CARGO_PROFILE_TEST_DEBUG": "0", "CARGO_PROFILE_DEV_OPT_LEVEL": "0", "CARGO_PROFILE_TEST_OPT_LEVEL": "0", "CARGO_PROFILE_DEV_DEBUG_ASSERTIONS": "true", "CARGO_PROFILE_TEST_DEBUG_ASSERTIONS": "true", "CARGO_PROFILE_DEV_OVERFLOW_CHECKS": "true", "CARGO_PROFILE_TEST_OVERFLOW_CHECKS": "true", "RUSTFLAGS": "-D warnings", "RUSTDOCFLAGS": "-D warnings"}.items():
            assert env[key] == value, (name, key)
        gate_receipts += 1
    summary = obj("original/q5-network-enabled-retry-v1/six-gates-summary.json")
    assert summary["state"] == "passed" and summary["gate_count"] == 6 and summary["source_tree"] == TREE
    for gate, receipt, original_path in zip(GATES, summary["receipts"], summary["receipt_paths"]):
        label = "q5" if gate in ("fmt", "clippy") else "q5-network-enabled-retry-v1"
        name = "original/" + label + "/" + gate + ".receipt.json"
        assert receipt == obj(name) and receipt["gate"] == gate and receipt["exit_code"] == 0
        assert original_path.endswith("a2a-subscription-qualification-v5" + ("" if label == "q5" else "-network-enabled-retry-v1") + "/" + gate + ".receipt.json")
        assert receipt["runner_sha256"] == "19f70e13be916748ecca9f519b82360a742a1e17fb652e47d134737c47459cf6"
    pattern = rb"^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out; finished in [0-9.]+s$"
    for gate, expected in (("workspace", [100, 2108, 0, 6, 0, 0]), ("doctests", [10, 48, 0, 0, 0, 0])):
        rows = [tuple(map(int, match)) for match in re.findall(pattern, raw("original/q5-network-enabled-retry-v1/" + gate + ".log"), re.M)]
        assert [len(rows)] + [sum(row[column] for row in rows) for column in range(5)] == expected
    work = raw("original/q5-network-enabled-retry-v1/workspace.log")
    private = final["crates/stateknot-integrations/src/a2a_server/subscription_tests.rs"].decode()
    tests = re.findall(r"#\[(?:tokio::)?test[^\n]*\]\s*(?:async )?fn (\w+)", private)
    assert len(tests) == 8
    for test in tests:
        assert work.count(("test a2a_server::subscription_tests::" + test + " ... ok").encode()) == 1
    assert work.count(b"test real_server_subscription_authorization_matrix_covers_both_bindings ... ok") == 1
    assert b"Unfold must not be polled after it returned `Poll::Ready(None)`" in raw("original/q1/unit.log")
    assert obj("original/q1/unit.receipt.json")["exit_code"] == 101
    assert b"7 passed; 1 failed; 0 ignored; 0 measured; 86 filtered out" in raw("original/q1/unit.log")
    assert obj("original/q2/unit.receipt.json")["exit_code"] == 0
    assert b"8 passed; 0 failed; 0 ignored; 0 measured; 86 filtered out" in raw("original/q2/unit.log")
    assert obj("original/q2/tcp.receipt.json")["exit_code"] == 101 and b'"timestamp"' in raw("original/q2/tcp.log")
    assert obj("original/q3/tcp.receipt.json")["exit_code"] == 0
    assert obj("original/q3/clippy.receipt.json")["exit_code"] == 101
    assert len(re.findall(rb"^error: (?!could not compile)", raw("original/q3/clippy.log"), re.M)) == 7
    assert obj("original/q4/fmt.receipt.json")["exit_code"] == 1
    assert obj("original/q5/workspace.receipt.json")["exit_code"] == 101
    assert raw("original/q5/workspace.log").count(b'kind: PermissionDenied, message: "Operation not permitted"') == 5
    for label in ("q2", "q3", "q4", "q5"):
        receipt = obj("original/" + label + "/fixture-fix.receipt.json")
        patch = next(name for name in index if name.startswith("original/" + label + "/") and name.endswith(".patch.gz"))
        assert sha(raw(patch)) == receipt["patch_raw_sha256"]
    profile = json.loads((HERE / "execution-profile.json").read_bytes())
    assert sha((HERE / "execution-profile.json").read_bytes()) == seal["qualification_profile_sha256"]
    assert profile["source_tree"] == TREE and profile["compiled_canonical_sha256"] == COMPILED
    assert profile["workspace"] == {"suites": 100, "passed": 2108, "failed": 0, "ignored": 6, "measured": 0, "filtered": 0, "database_environment_cleared": True, "conditional_database_tests_not_Native": True}
    assert profile["qualification"]["local_component_execution"] is True
    assert all(value is False for key, value in profile["qualification"].items() if key != "local_component_execution")
    assert profile["independent_reviews"]["final_Spec_review_included"] is False
    report = json.loads((HERE.parents[1] / "experimental-a2a-subscription-policy-2026-10-16.json").read_bytes())
    assert all(report[key] == value for key, value in profile.items())
    assert report["default_enabled"] is False and report["new_source_payload_count"] == 3
    assert report["evidence"]["manifest_sha256"] == sha(manifest_raw)
    assert report["evidence"]["seal_sha256"] == sha((HERE / "seal.json").read_bytes())
    if args.workspace:
        for row in inputs["files"]:
            path = workspace / safe_path(row["path"])
            assert not path.is_symlink()
            data = path.read_bytes()
            assert (len(data), sha(data)) == (row["bytes"], row["sha256"]), "workspace compiled input mismatch: " + row["path"]
    print(json.dumps({"state": "verified", "scope": "fixed A2A subscription candidate component evidence only", "source_tree": TREE, "source_files": 930, "compiled_inputs": 538, "source_payloads": 3, "preserved_gate_receipts": gate_receipts, "six_terminal_gates": 6, "workspace_suites": 100, "workspace_passed": 2108, "original_ignored": 6, "doctests": 48, "private_tests": 8, "TCP_families": 1, "matrix_ids_per_binding": 17, "bindings": 2, "workspace_checked": args.workspace, "Native_DB_fullCI_IdP_durableTask_stdio_R1_qualified": False}, sort_keys=True))


if __name__ == "__main__":
    main()
