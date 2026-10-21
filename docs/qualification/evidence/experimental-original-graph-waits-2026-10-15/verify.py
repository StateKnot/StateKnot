#!/usr/bin/env python3
"""Offline evidence checks; --workspace checks actual adopted compiled inputs."""
from pathlib import Path, PurePosixPath
import argparse
import collections
import gzip
import hashlib
import io
import json
import re
import subprocess
import sys
import tarfile
import tempfile


HERE = Path(__file__).resolve().parent
TREE = "88dd9d363194af6c52809fabcd82b9c7e6cba2ee136c95ae161e40a67aebb0eb"
SUMMARIES = re.compile(r"test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;")


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def read(path):
    return json.loads(path.read_text())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--require-execution-record", action="store_true")
    parser.add_argument("--workspace", action="store_true")
    parser.add_argument("--workspace-root", type=Path)
    args = parser.parse_args()
    manifest = read(HERE / "manifest.json")
    for item in manifest["files"]:
        data = (HERE / item["path"]).read_bytes()
        assert len(data) == item["bytes"] and sha(data) == item["sha256"], item["path"]
    index = read(HERE / "artifact-index.json")
    items = index["artifacts"]
    assert len({x["path"] for x in items}) == len(items)
    by_path = {x["path"]: x for x in items}

    def raw(path):
        item = by_path[path]
        data = (HERE / path).read_bytes()
        assert len(data) == item["stored_bytes"] and sha(data) == item["stored_sha256"], path
        if item["storage_encoding"] == "gzip":
            assert data[:3] == b"\x1f\x8b\x08" and data[3] & 8 == 0
            assert data[4:8] == b"\0\0\0\0"
            data = gzip.decompress(data)
        else:
            assert item["storage_encoding"] == "identity"
        assert len(data) == item["raw_bytes"] and sha(data) == item["raw_sha256"], path
        return data

    def asset(path):
        actual = path if path in by_path else path + ".gz"
        assert actual in by_path, path
        return raw(actual)

    def obj(path):
        return json.loads(asset(path))

    for item in items:
        raw(item["path"])
        assert "target" not in PurePosixPath(item["origin"]).parts
        assert "elapsed36-a2a-auth-facts-source" not in item["origin"]
    receipt_log_bindings = 0
    for path in by_path:
        if not (path.endswith("receipt.json") or path.endswith("receipt.json.gz")):
            continue
        receipt = json.loads(raw(path))
        expected_log = receipt.get("log_sha256")
        if expected_log:
            prefix = path.rsplit("/", 1)[0] + "/"
            siblings = [p for p in by_path if p.startswith(prefix) and "/" not in p[len(prefix):]]
            assert any(sha(raw(p)) == expected_log for p in siblings), "receipt raw log missing: " + path
            receipt_log_bindings += 1

    source = obj("source/source-manifest.json")
    assert source["source_tree_sha256"] == TREE and len(source["files"]) == 922
    assert sha(json.dumps(source["files"], sort_keys=True, separators=(",", ":")).encode()) == TREE
    source_bytes = {}
    with tarfile.open(fileobj=io.BytesIO(asset("source/source.tar"))) as tar:
        members = {m.name: m for m in tar.getmembers()}
        assert len(members) == len(source["files"])
        for item in source["files"]:
            member = members[item["path"]]
            assert member.isfile() and member.size == item["bytes"]
            data = tar.extractfile(member).read()
            assert sha(data) == item["sha256"]
            source_bytes[item["path"]] = data
    compiled = read(HERE / "compiled-inputs.json")
    expected_inputs = [x for x in source["files"] if x["path"].startswith(("crates/", ".cargo/")) or x["path"] in ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml"]]
    assert compiled["all_compiled_input_paths"] == expected_inputs
    assert compiled["input_count"] == len(expected_inputs) == 530
    assert not any(p.startswith("crates/stateknot-store-postgres/migrations/0037_") for p in source_bytes)

    # Full source binding includes all old 1..35 migration bytes; 36 is additive.
    original = obj("original919/source-manifest.json")
    assert original["source_tree_sha256"] == "bca1889badb0d3318fcca66490d153a2fec08ab28ab205a474db44f9b6510d45"
    old_by_path = {x["path"]: x for x in original["files"]}
    new_by_path = {x["path"]: x for x in source["files"]}
    changed = {p for p in old_by_path if p in new_by_path and old_by_path[p]["sha256"] != new_by_path[p]["sha256"]}
    assert changed == {
        "crates/stateknot-store-postgres/tests/postgres/frame_application_replay.rs",
        "crates/stateknot-store-postgres/tests/postgres/frame_closures.rs",
    }
    assert set(old_by_path) <= set(new_by_path)
    assert set(new_by_path) - set(old_by_path) == {
        "crates/stateknot-store-postgres/tests/fixtures/actual_schema35_writer_source32.README.md",
        "crates/stateknot-store-postgres/tests/fixtures/actual_schema35_writer_source32.json",
        "crates/stateknot-store-postgres/tests/fixtures/actual_schema35_writer_source32.sql",
    }
    lease_delay = source_bytes["crates/stateknot-store-postgres/tests/postgres/frame_returns.rs"].decode()
    lease_delay = lease_delay[lease_delay.index("async fn deferred_return_delay_cannot_commit_after_live_lease_expires"):]
    lease_delay = lease_delay[:lease_delay.index("\n}") + 2]
    assert "Duration::from_secs(2)" in lease_delay and "pg_sleep(2.1)" in lease_delay
    assert "Err(StoreError::LeaseExpired)" in lease_delay

    gates = obj("static/gates/receipts.json")
    assert gates["native_database_tests"] is False
    assert [x["gate"] for x in gates["receipts"]] == ["fmt", "clippy", "workspace", "doctests", "rustdoc", "nightly"]
    for gate in gates["receipts"]:
        assert gate["exit_code"] == 0
        data = asset("static/gates/" + gate["gate"] + ".log")
        assert sha(data) == gate["log_sha256"]
        assert [list(x) for x in SUMMARIES.findall(data.decode())] == gate["summaries"]
        if gate["gate"] == "workspace":
            summaries = SUMMARIES.findall(data.decode())
            assert sum(int(x[1]) for x in summaries) == 2064
            assert sum(int(x[2]) for x in summaries) == 0
            assert sum(int(x[3]) for x in summaries) == 6
        if gate["gate"] == "doctests":
            assert sum(int(x[1]) for x in SUMMARIES.findall(data.decode())) == 45
        if gate["gate"] == "nightly":
            assert "+nightly-2026-10-07" in gate["command"]

    builds = obj("source/four-binary-identities.json")["artifacts"]
    build_profiles = obj("source/strict-build-profiles.json")
    assert len(builds) == len(build_profiles["events"]) == 4
    for event in build_profiles["events"]:
        assert event["fresh"] is False and event["profile"]["debug_assertions"] is True
        assert event["profile"]["overflow_checks"] is True and event["profile"]["debuginfo"] == 0
    for package, value in builds.items():
        assert value["fresh"] is False
        build_package = "stateknot" if package == "stateknot-deepseek_agent" else package
        prefix = "build/store/build.stdout.jsonl" if package == "stateknot-store-postgres" else "build/remaining/" + build_package + "-build.stdout.jsonl"
        events = [json.loads(line) for line in asset(prefix).decode().splitlines() if line.startswith("{")]
        candidates = [x for x in events if x.get("reason") == "compiler-artifact" and x.get("executable") and x.get("package_id") == value["package_id"]]
        assert len(candidates) == 1 and candidates[0]["fresh"] is False

    coverage = read(HERE / "coverage/runtime189.json")
    assert coverage["runtime_inventory_count"] == 189
    assert coverage["runtime_binary_sha256"] == builds["stateknot-runtime"]["sha256"]
    assert {x["name"] for x in coverage["runtime_tests"]} == set(builds["stateknot-runtime"]["exact_names"])
    assert dict(collections.Counter(x["role"] for x in coverage["runtime_tests"])) == coverage["role_counts"] == {"ordinary_database_scenario":168,"auxiliary_subprocess_worker":15,"independent_logical_restore_resource_gate":1,"independent_pitr_resource_gate":1,"harness_or_protocol_self_test":4}
    parent_coverage = obj("coverage/parent185-map.json")
    for case in coverage["runtime_tests"]:
        assert sha(source_bytes[case["path"]]) == case["source_file_sha256"]
        if case["role"] != "ordinary_database_scenario":
            assert case["conditional_or_auxiliary_whole_file_equals_reviewed_parent"]
            assert case["source_file_sha256"] == parent_coverage["runtime_test_files"][case["path"]]
    assert sha(source_bytes[coverage["workflow_path"]]) == coverage["workflow_sha256"]
    assert coverage["parent185_qualification_transferred"] is False
    assert coverage["ordinary_harness_pass_is_independent_backup_or_PITR_execution"] is False

    ci_run = obj("ci/previous-c7d8768/final-observed-run.json")
    ci_jobs = obj("ci/previous-c7d8768/final-observed-jobs.json")["jobs"]
    assert ci_run["id"] == 38717156638
    assert ci_run["head_sha"] == "c7d8768356b30207cd637d81e406056b745d35ac"
    assert ci_run["status"] == "completed" and ci_run["conclusion"] == "cancelled"
    assert len(ci_jobs) == 13 and dict(collections.Counter(x["conclusion"] for x in ci_jobs)) == {"success":11,"cancelled":2}
    for major, job_id in [(16,116362535821),(17,116362535878)]:
        job = next(x for x in ci_jobs if x["id"] == job_id)
        assert job["name"] == "PostgreSQL (" + str(major) + ")" and job["conclusion"] == "cancelled"
        annotations = obj("ci/previous-c7d8768/" + str(job_id) + "-annotations.json")
        assert sum(x["message"] == "The job has exceeded the maximum execution time of 30m0s" for x in annotations) == 1

    state = read(HERE / "collection-state.json")
    assert state["source_tree_sha256"] == TREE
    observed = {}
    for gate in state["mandatory_gates"]:
        if gate["state"] == "pending":
            continue
        receipt = obj(gate["receipt"])
        data = asset(gate["receipt"].rsplit("/", 1)[0] + "/native.log")
        assert sha(data) == receipt["log_sha256"]
        assert receipt["source_tree_sha256"] == TREE and receipt["source_files"] == 922
        package = "stateknot-runtime" if gate["id"].startswith("mandatory-pitr") else receipt["package"]
        assert receipt["binary_sha256"] == builds[package]["sha256"]
        summaries = SUMMARIES.findall(data.decode())
        assert [list(x) for x in summaries] == receipt["summaries"]
        passed = receipt["exit_code"] == 0 and summaries == [("ok", str(gate["expected_tests"]), "0", "0")]
        if gate["id"].startswith("mandatory-pitr"):
            markers = [json.loads(line.split("=",1)[1]) for line in data.decode().splitlines() if line.startswith("STATEKNOT_CHILD_JOIN_PITR_EVIDENCE=")]
            assert len(markers) == 1 and markers == receipt["markers"]
            assert markers[0]["profile"] == "child-join-named-pitr-v1" and markers[0]["invariants"] == "passed"
            assert markers[0]["os"] == receipt["host_harness_platform"] == "macos"
            assert markers[0]["arch"] == "aarch64" and receipt["actual_server_platform"] == "linux/arm64 container"
            assert markers[0]["postgres"] == receipt["actual_server_version"]
            assert all(markers[0][key] is True for key in ["base_backup_verified","cross_tenant_refusal_verified","exact_durable_snapshot_verified","noninitial_replay_verified","post_target_write_excluded"])
            assert receipt["no_DB_environment"] and receipt["parent_four_suites_and_backup_terminal_before_start"]
            assert receipt["only_profile_environment_set"] == ["STATEKNOT_REQUIRE_PITR_TESTS","STATEKNOT_PITR_POSTGRES_IMAGE"]
            passed = passed and receipt["exact_mandatory_PITR_passed"]
        elif gate["id"].startswith("mandatory-backup"):
            markers = [json.loads(line.split("=", 1)[1]) for line in data.decode().splitlines() if line.startswith("STATEKNOT_CHILD_JOIN_BACKUP_RESTORE_EVIDENCE=")]
            assert len(markers) == 1 and markers == receipt["markers"]
            assert receipt["required_profile"] is True
            assert all(markers[0][key] is True for key in ["source_isolated", "restore_isolated", "schema_verified", "exact_durable_snapshot_verified", "cross_tenant_refusal_verified", "noninitial_replay_verified"])
            assert markers[0]["invariants"] == "passed"
            passed = passed and receipt["exact_mandatory_backup_restore_passed"]
        else:
            assert receipt["expected_tests"] == gate["expected_tests"]
            passed = passed and receipt["exact_all_tests_passed"]
        assert (gate["state"] == "passed") == passed
        observed[gate["id"]] = summaries

    # Original failed runs are preserved as red, never counted as final green.
    for major, numbers in [(16, ["FAILED", "261", "3", "0"]), (17, ["FAILED", "262", "2", "0"])]:
        prefix = "original919/full-native/pg" + str(major) + "/stateknot-store-postgres/"
        receipt = obj(prefix + "receipt.json")
        data = asset(prefix + "native.log")
        assert receipt["exit_code"] == 101 and receipt["summaries"] == [numbers]
        assert sha(data) == receipt["log_sha256"]
        assert [list(x) for x in SUMMARIES.findall(data.decode())] == [numbers]
        for package, count in [("stateknot-runtime",189),("stateknot-integrations",2),("stateknot-deepseek_agent",1)]:
            prefix = "original919/full-native/pg" + str(major) + "/" + package + "/"
            receipt = obj(prefix + "receipt.json")
            data = asset(prefix + "native.log")
            assert receipt["source_files"] == 919 and receipt["source_tree_sha256"] == original["source_tree_sha256"]
            assert receipt["exit_code"] == 0 and sha(data) == receipt["log_sha256"]
            assert SUMMARIES.findall(data.decode()) == [("ok",str(count),"0","0")]
    branches = obj("test-fixes/wait-oracle/exact-branch-coverage.json")
    assert branches["both_oracle_branches_observed"]
    assert {x["actual_profile"] for x in branches["cases"]} == {"graph_registered", "graph_elapsed"}
    for case in branches["cases"]:
        from datetime import datetime
        # Python 3.9 accepts only 3/6 fractional digits; zero-padding is exact.
        def timestamp(value):
            value = re.sub(r"(\.\d{1,5})(?=[+-]\d\d:\d\d$)", lambda match: match.group(1).ljust(7, "0"), value)
            return datetime.fromisoformat(value)
        early = timestamp(case["event"]) < timestamp(case["due"])
        assert early == (case["actual_profile"] == "graph_registered")

    producer_proof = obj("source/old-producer-compiled-inputs-commit83559-proof.json")
    assert producer_proof["compiled_input_file_count"] == 514
    assert producer_proof["all_crate_sources_tests_fixtures_configs_exact"] is True
    assert producer_proof["compiled_input_differences"] == [] and producer_proof["entire_tree_exact"] is False
    producer = obj("test-fixes/original-writer-commit83559/binding514.json")
    assert producer["actual_git_archive_file_count"] == 980
    assert producer["frozen_parent_source_file_count"] == 899 and producer["entire_tree_equal"] is False
    original_git = asset("test-fixes/original-writer-commit83559/commit83559-source.tar")
    assert sha(original_git) == producer["original_git_archive_sha256"]
    with tarfile.open(fileobj=io.BytesIO(original_git)) as tar:
        git_files = {m.name:tar.extractfile(m).read() for m in tar.getmembers() if m.isfile()}
    assert len(git_files) == 980
    with tarfile.open(fileobj=io.BytesIO(asset("test-fixes/actual-old-producer/source.tar"))) as tar:
        exporter_files = {m.name:tar.extractfile(m).read() for m in tar.getmembers() if m.isfile()}
    assert producer["compiled_input_file_count"] == len(producer["inputs"]) == 514
    for item in producer["inputs"]:
        expected = git_files[item["path"]]
        assert len(expected) == item["bytes"] and sha(expected) == item["sha256"]
        original = exporter_files[item["path"]] if item["equals_exporter_source_archive"] else asset("test-fixes/original-writer-commit83559/" + item["original_override_file"])
        assert original == expected
    adoption = obj("adoption-preparation/source-only-copy-apply/receipt.json")
    assert adoption["base_commit"] == "a9b265d5b3a96e0e5bbed604b7558f4ae9edc136"
    assert adoption["copied_current_main_compiled_inputs"] == 514 and adoption["after_patch_compiled_inputs_exact"] == 530
    assert adoption["after_patch_source_tree_origin"] == TREE and adoption["compiled_input_superset_exact"] is True
    assert adoption["whole_Git_tree_equality_claimed"] is False and adoption["main_mutated"] is False
    assert adoption["compile_or_tests_executed"] is False
    assert sha(asset("adoption-preparation/final-main-integration-preflight-v2/code.patch")) == adoption["patch_sha256"]

    binding = obj("released-alpha1/binding160/binding.json")
    assert binding["production_input_files_compared"] == 160
    assert binding["target_candidate_tree_sha256"] == TREE
    assert binding["new_test_execution_on_candidate922"] is False
    assert binding["whole_source_qualification_transfer"] is False
    alpha_inputs = obj("released-alpha1/binding160/candidate922-production-inputs.json")
    assert alpha_inputs["file_count"] == len(alpha_inputs["files"]) == 160
    for item in alpha_inputs["files"]:
        assert sha(source_bytes[item["path"]]) == item["sha256"]
    seal = obj("released-alpha1/bundle-seal.json")
    archive = asset("released-alpha1/released-alpha1-to-experimental-schema36-2026-10-15.tar.gz")
    assert sha(archive) == seal["bundle_sha256"] == binding["original_case_bundle_sha256"]
    assert len(archive) == seal["bundle_bytes"]
    with tempfile.TemporaryDirectory(prefix="stateknot-schema36-alpha-evidence-") as temp:
        dest = Path(temp)
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
            for member in tar.getmembers():
                p = PurePosixPath(member.name)
                assert not p.is_absolute() and ".." not in p.parts and (member.isfile() or member.isdir())
            tar.extractall(dest)
        result = subprocess.run([sys.executable, str(dest / "verify.py")], cwd=dest, capture_output=True, text=True)
        assert result.returncode == 0, result.stdout + result.stderr
        assert sha((dest / "manifest.json").read_bytes()) == seal["manifest_sha256"]
        assert sha((dest / "report.json").read_bytes()) == seal["profile_sha256"]

    complete = all(x["state"] == "passed" for x in state["mandatory_gates"])
    assert state["all_exact_execution_gates_passed"] == complete
    if args.require_execution_record:
        assert complete, "mandatory final Native/resource gates are pending or failed"
        assert state["reports_finalized"] and state["sealed"], "final reports/seal not completed"
    if state["sealed"]:
        seal = read(HERE / "seal.json")
        assert sha((HERE / "manifest.json").read_bytes()) == seal["manifest_sha256"]
        docs = HERE.parents[1]
        for name, expected in seal["reports"].items():
            assert sha((docs / name).read_bytes()) == expected, name
        profile = read(docs / "experimental-original-graph-waits-2026-10-15.json")
        assert profile["status"] == "exact_execution_passed_resource_cleanup_open"
        assert profile["native_execution_gates_passed"] is True
        for field in ["resource_cleanup_qualified","full_resource_lifecycle_qualified","overall_qualification"]:
            assert profile[field] is False and seal[field] is False
        assert profile["native"]["total_libtest_executions"] == 916
        assert profile["native"]["four_suite_libtest_executions"] == 912
        assert profile["native"]["failed"] == profile["native"]["ignored"] == 0
        assert profile["native"]["runtime_roles"] == coverage["role_counts"]
        assert profile["CI"]["full_CI_passed"] is False and profile["claim_limits"]["R1_complete"] is False
        assert profile["whitespace"]["full_staged_check_all_green_claimed"] is False
        assert profile["whitespace"]["original_pg_dump_EOF_blank_line_preserved"] is True
        assert profile["resource_cleanup"]["complete"] is False
        cleanup = obj("resources/pitr/resource-readonly-final/receipt.json")
        volume = "stateknot-pitr-wal-01a13f6d5ad477909c16dcdd725a26b1"
        assert cleanup["retained_volume"] == profile["resource_cleanup"]["PITR16_wal_volume"] == volume
        assert cleanup["read_only"] and cleanup["no_stop_rm_prune_or_DB_operation"]
        assert all(x["exit_code"] == 0 for x in cleanup["commands"])
        inspected = json.loads(asset("resources/pitr/resource-readonly-final/wal-volume-inspect.stdout"))
        assert len(inspected) == 1 and inspected[0]["Name"] == volume and inspected[0]["Driver"] == "local"
        for name in ["wal-container-references.stdout","own-uuid-containers.stdout"]:
            assert asset("resources/pitr/resource-readonly-final/" + name).strip() == b""
        assert volume.encode() not in asset("resources/pitr/pg16/before-volumes.txt")
        assert volume.encode() in asset("resources/pitr/pg16/after-volumes.txt")
        before17 = set(asset("resources/pitr/pg17/before-volumes.txt").splitlines())
        after17 = set(asset("resources/pitr/pg17/after-volumes.txt").splitlines())
        assert after17 - before17 == set()
        schema = read(HERE / "report-schema.json")
        assert set(profile) == set(schema["required"]) == set(schema["properties"])
        for name, rule in schema["properties"].items():
            value = profile[name]
            if "const" in rule:
                assert value == rule["const"]
            else:
                expected = {"object":dict,"integer":int,"string":str,"boolean":bool}[rule["type"]]
                assert type(value) is expected, name
        actual_markers = read(HERE / "coverage/runtime-markers.json")
        for major in [16,17]:
            found = collections.defaultdict(list)
            for line in asset("full-native/pg" + str(major) + "/stateknot-runtime/native.log").decode().splitlines():
                if re.match(r"^STATEKNOT_[A-Z_]+=", line):
                    key,value = line.split("=",1)
                    found[key].append(json.loads(value))
            assert dict(found) == actual_markers["markers"][str(major)]
            assert {key:len(values)for key,values in found.items()} == actual_markers["counts_per_major"]
    workspace = None
    if args.workspace:
        workspace = args.workspace_root
        if workspace is None:
            workspace = next((p for p in HERE.parents if (p / ".git").exists()), None)
        assert workspace is not None, "--workspace-root required outside repository"
        mismatches = [x["path"] for x in expected_inputs if not (workspace / x["path"]).is_file() or sha((workspace / x["path"]).read_bytes()) != x["sha256"]]
        assert not mismatches, "actual workspace compiled inputs differ: " + ", ".join(mismatches)
        expected_paths = {x["path"] for x in expected_inputs}
        actual_paths = {p.relative_to(workspace).as_posix() for p in (workspace / "crates").rglob("*") if p.is_file() and "target" not in p.parts}
        assert actual_paths <= expected_paths, "additional crate input paths need review: " + ", ".join(sorted(actual_paths - expected_paths))
    print(json.dumps({
        "evidence_integrity": "passed", "qualification_status": "sealed-exact-execution-with-open-cleanup" if state["sealed"] else "unsealed-preparation",
        "artifact_count": len(items), "source_files": 922, "compiled_input_files": 530,
        "direct_receipt_raw_log_bindings": receipt_log_bindings,
        "workspace_checked": workspace is not None, "mandatory_gate_counts": dict(collections.Counter(x["state"] for x in state["mandatory_gates"])),
        "static_workspace_passed": 2064, "static_original_ignored": 6, "doctests_passed": 45,
        "nested_alpha_bundle_offline_verifier_exit": result.returncode,
        "exact_execution_evidence_complete": state["sealed"] and complete,
        "complete_production_qualification_claimed": False,
        "resource_cleanup_complete": False if state["sealed"] else None,
        "overall_qualification": False,
    }, indent=2))


if __name__ == "__main__":
    main()
