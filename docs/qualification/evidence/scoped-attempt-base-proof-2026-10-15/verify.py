# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0

"""Verify frozen evidence; --workspace also checks the adopted build inputs."""

from pathlib import Path
import argparse
import collections
import gzip
import hashlib
import json
import re
import subprocess


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


def evidence_markers(data):
    found = collections.defaultdict(list)
    for line in data.decode().splitlines():
        if re.match(r"^STATEKNOT_[A-Z_]+=", line):
            key, value = line.split("=", 1)
            found[key].append(json.loads(value))
    return dict(found)


def verify_current_ci(profile):
    directory = HERE / "ci-83559"
    observed = read_json(directory / "observed.json")
    run = read_json(directory / "run.json")
    jobs = read_json(directory / "jobs.json")["jobs"]
    assert run["id"] == observed["run_id"] == profile["current_ci"]["run_id"] == 38709873198
    assert run["head_sha"] == observed["head_sha"] == "83559f7651f48d65755c1fc5fd8a9ba82e721193"
    assert run["status"] == "completed" and run["conclusion"] == "cancelled"
    assert len(jobs) == 13 and dict(collections.Counter(row["conclusion"] for row in jobs)) == {"success": 11, "cancelled": 2}
    assert observed["ordinary_postgres_job_limit_minutes"] == 30
    expected_annotation = "The job has exceeded the maximum execution time of 30m0s"
    assert observed["timeout_annotation"] == expected_annotation
    assert len(observed["ordinary_postgres"]) == 2
    for major, expected_seconds, row in zip([16, 17], ["1231.22", "1243.15"], observed["ordinary_postgres"]):
        assert row["postgres_major"] == major
        job = read_json(directory / row["job"])
        annotations = read_json(directory / row["annotations"])
        assert len([item for item in annotations if item["message"] == expected_annotation]) == 1
        assert job["id"] == row["job_id"] and job["name"] == "PostgreSQL (" + str(major) + ")"
        assert job["status"] == "completed" and job["conclusion"] == "cancelled"
        snapshot = [item for item in jobs if item["id"] == job["id"]]
        assert len(snapshot) == 1 and snapshot[0]["conclusion"] == "cancelled"
        data = raw_evidence(directory / row["log"])
        assert sha(data) == row["log_sha256"]
        text = data.decode()
        summaries = re.findall(r"test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;[^\n]*finished in ([0-9.]+)s", text)
        assert summaries == [("ok", "252", "0", "0", expected_seconds)]
        assert row["store_passed"] == 252 and row["store_failed"] == row["store_ignored"] == 0
        assert row["store_harness_seconds"] == float(expected_seconds)
        assert text.count("running 185 tests") == 1 and row["runtime_harness_entries_started"] == 185
        assert "test result:" not in text.split("running 185 tests", 1)[1]
        assert not re.search(r"test [^\n]+ \.\.\. FAILED", text)
        assert row["runtime_terminal_summary_observed"] is False and row["explicit_failed_test_lines_observed"] == 0
        steps = {item["name"]: item["conclusion"] for item in job["steps"]}
        assert steps["Run mandatory PostgreSQL integration tests"] == "success"
        assert steps["Run durable Graph Driver integration tests"] == "cancelled"
        assert steps["Qualify real model adapter, pricing, and exact-key authorization"] == "skipped"
        assert steps["Execute recoverable DeepSeek Agent example against PostgreSQL"] == "skipped"
    assert observed["runtime_and_following_ci_qualification_complete"] is False
    assert observed["runtime_assertion_failure_proven"] is False and observed["full_ci_passed"] is False
    return {"ci_83559_jobs": {"success": 11, "cancelled": 2}, "ci_store_passed_per_postgres": 252, "ci_runtime_complete": False}


def verify_full_native(profile, summaries, workspace):
    directory = HERE / "full-native"
    full = read_json(directory / "receipt.json")
    assert full["source_manifest_sha256"] == profile["source"]["tested_manifest_sha256"]
    adoption = "83559f7651f48d65755c1fc5fd8a9ba82e721193"
    assert full["adoption_commit"] == adoption
    source = read_json(directory / "source-verification.json")
    assert source["all_frozen_files_matched_after_all_native"]
    assert source["frozen_files"] == 899 and source["adopted_crate_and_build_inputs_matched"] == 515
    assert source["adoption_tree"] == full["adoption_tree"]
    expected_counts = {"store": 252, "runtime": 185, "integrations": 2, "example": 1}
    assert profile["full_native"]["passed_per_major"] == expected_counts
    binaries = {"store": profile["build"]["binary_sha256"]}
    for suite in ["runtime", "integrations", "example"]:
        build = read_json(directory / ("build-" + suite) / "receipt.json")
        assert build["exit_code"] == 0 and len(build["artifacts"]) == 1
        binaries[suite] = build["artifacts"][0]["sha256"]
        messages = raw_evidence(directory / ("build-" + suite) / "build.stdout.jsonl.gz").decode().splitlines()
        artifacts = [json.loads(line) for line in messages if line.startswith("{")]
        assert any(row.get("reason") == "compiler-artifact" and row.get("executable") and row.get("fresh") is False for row in artifacts)
    coverage = read_json(directory / "coverage.json")
    assert coverage["runtime_binary_sha256"] == binaries["runtime"]
    assert coverage["runtime_inventory_count"] == 185
    roles = dict(collections.Counter(row["role"] for row in coverage["runtime_tests"]))
    assert roles == coverage["role_counts"] == {
        "ordinary_database_scenario": 164, "auxiliary_subprocess_worker": 15,
        "independent_logical_restore_resource_gate": 1,
        "independent_pitr_resource_gate": 1, "harness_or_protocol_self_test": 4,
    }
    expected_markers = {}
    for step in coverage["ci_qualification_steps"]:
        if step["inside_runtime185"]:
            expected_markers.update(step["required_markers"])
    expected_markers.pop("STATEKNOT_CHILD_JOIN_BACKUP_RESTORE_EVIDENCE")
    expected_markers.pop("STATEKNOT_CHILD_JOIN_PITR_EVIDENCE")
    recorded_markers = read_json(directory / "runtime-markers.json")
    assert len(full["suites"]) == 8
    observed = set()
    for row in full["suites"]:
        suite, major = row["suite"], row["postgres_major"]
        assert (suite, major) not in observed and major in [16, 17]
        observed.add((suite, major))
        count = expected_counts[suite]
        receipt = read_json(directory / row["receipt"])
        data = raw_evidence(directory / row["log"])
        assert receipt["exit_code"] == 0 and receipt["all_expected_tests_passed"]
        assert receipt["expected_tests"] == row["passed"] == count
        assert receipt["default_stack"] and row["default_stack"]
        assert receipt["binary_sha256"] == row["binary_sha256"] == binaries[suite]
        assert sha(data) == receipt["log_sha256"] == row["log_sha256"]
        assert summaries.findall(data.decode()) == [("ok", str(count), "0", "0")]
        listing = (directory / row["receipt"]).parent / "test-list.txt"
        tests = [line.removesuffix(": test") for line in listing.read_text().splitlines() if line.endswith(": test")]
        assert len(tests) == count and len(set(tests)) == count
        assert row["server_version"].startswith(str(major) + ".")
        if suite == "runtime":
            assert set(tests) == {item["name"] for item in coverage["runtime_tests"]}
            for case in full["original_runtime_cases"]:
                assert "test " + case + " ... ok" in data.decode(), case
            markers = evidence_markers(data)
            assert markers == recorded_markers[str(major)]
            assert {key: len(values) for key, values in markers.items()} == expected_markers
            assert all(value.get("invariants", "passed") == "passed" for values in markers.values() for value in values)
    assert len(observed) == 8 and full["harness_passed_per_postgres"] == 440

    resources = HERE / "resources"
    resource_receipt = read_json(resources / "receipt.json")
    assert len(resource_receipt["logical_restore"]) == len(resource_receipt["pitr"]) == 2
    for major in [16, 17]:
        backup = resources / ("backup-restore-pg" + str(major))
        receipt = read_json(backup / "receipt.json")
        resource = read_json(backup / "resource-receipt.json")
        data = raw_evidence(backup / "native.log.gz")
        assert receipt["exit_code"] == 0 and resource["runner_exit_code"] == 0
        assert receipt["binary_sha256"] == binaries["runtime"]
        assert resource["source_commit"] == adoption
        assert resource["mandatory_backup_restore"] and resource["mandatory_postgres"]
        assert sha(data) == receipt["log_sha256"] == resource["native_log_sha256"]
        assert summaries.findall(data.decode()) == [("ok", "1", "0", "0")]
        markers = evidence_markers(data)["STATEKNOT_CHILD_JOIN_BACKUP_RESTORE_EVIDENCE"]
        assert resource["marker_count"] == len(markers) == 1 and markers == resource["markers"]
        assert markers[0]["invariants"] == "passed"
        for key in ["cross_tenant_refusal_verified", "exact_durable_snapshot_verified", "noninitial_replay_verified", "restore_isolated", "schema_verified", "source_isolated"]:
            assert markers[0][key] is True

    run = read_json(resources / "pitr-ci-run.json")
    jobs = read_json(resources / "pitr-ci-jobs.json")["jobs"]
    assert run["head_sha"] == adoption and run["id"] == 38709873198
    pitr = read_json(resources / "pitr-verification.json")
    assert len(pitr) == 2
    for major, row in zip([16, 17], pitr):
        assert "-" + str(major) + "-" in row["artifact"]
        job = [item for item in jobs if item["name"] == "PostgreSQL PITR (" + str(major) + ")"]
        assert len(job) == 1 and job[0]["conclusion"] == "success"
        data = raw_evidence(resources / ("pitr-pg" + str(major)) / "native.log.gz")
        environment = raw_evidence(resources / ("pitr-pg" + str(major)) / "environment.log.gz")
        assert sha(data) == row["log_sha256"] and sha(environment) == row["environment_sha256"]
        assert summaries.findall(data.decode()) == [("ok", "1", "0", "0")]
        markers = evidence_markers(data)["STATEKNOT_CHILD_JOIN_PITR_EVIDENCE"]
        assert row["marker_count"] == len(markers) == 1 and markers[0] == row["marker"]
        assert markers[0]["invariants"] == "passed" and markers[0]["os"] == "linux" and markers[0]["arch"] == "x86_64"
        for key in ["base_backup_verified", "cross_tenant_refusal_verified", "exact_durable_snapshot_verified", "noninitial_replay_verified", "post_target_write_excluded"]:
            assert markers[0][key] is True
        lines = environment.decode().splitlines()
        assert lines[0] == row["actual_checkout_commit"]
        assert lines[1] == row["actual_checkout_tree"] == source["adoption_tree"]
        assert row["workflow_head_sha"] == adoption and row["actual_checkout_tree_equals_head_tree"]
        assert lines[2].split()[0] == row["cargo_lock_sha256"] == source["cargo_lock_sha256"]
    if workspace:
        actual_tree = subprocess.check_output(["git", "rev-parse", adoption + "^{tree}"], cwd=ROOT, text=True).strip()
        assert actual_tree == source["adoption_tree"]
        assert sha(subprocess.check_output(["git", "show", adoption + ":Cargo.lock"], cwd=ROOT)) == source["cargo_lock_sha256"]
        assert sha((ROOT / coverage["workflow_path"]).read_bytes()) == coverage["workflow_sha256"]
        for name, expected in coverage["runtime_test_files"].items():
            assert sha((ROOT / name).read_bytes()) == expected, name
    return {"four_local_native_suites_passed_per_postgres": expected_counts, "runtime_roles": roles, "forced_restore_passed_per_postgres": 1, "linux_pitr_passed_per_postgres": 1}


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
    full_native = verify_full_native(profile, summaries, args.workspace)
    current_ci = verify_current_ci(profile)
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
    print(json.dumps({"evidence_files": len(inventory), "body_tokens_equal": 123, "sql_literals_equal": 139, "workspace": counts["workspace"], "doctests": counts["doctests"], "selected_native_passed_per_postgres": 6, "workspace_inputs_checked": matched, **full_native, **current_ci}))


if __name__ == "__main__":
    main()
