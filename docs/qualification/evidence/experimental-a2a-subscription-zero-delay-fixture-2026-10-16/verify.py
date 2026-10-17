#!/usr/bin/env python3
"""Read-only, standard-library verification of the local fixture qualification."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re
import sys


def digest(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace-root", type=Path)
    args = parser.parse_args()
    here = Path(__file__).resolve().parent
    title = "experimental-a2a-subscription-zero-delay-fixture-2026-10-16"
    top = json.loads((here.parent.parent / (title + ".json")).read_bytes())
    index_bytes = (here / "artifact-index.json").read_bytes()
    assert digest(index_bytes) == top["artifact_index_sha256"]
    index = json.loads(index_bytes)
    raw = {}
    for row in index["artifacts"]:
        relative = Path(row["path"])
        assert not relative.is_absolute() and ".." not in relative.parts
        path = here / relative
        assert not path.is_symlink()
        stored = path.read_bytes()
        assert (len(stored), digest(stored)) == (row["stored_bytes"], row["stored_sha256"]), relative
        data = gzip.decompress(stored) if row["encoding"] == "gzip" else stored
        assert (len(data), digest(data)) == (row["raw_bytes"], row["raw_sha256"]), relative
        assert row["id"] not in raw
        raw[row["id"]] = data
    load = lambda key: json.loads(raw[key])
    text = lambda key: raw[key].decode()
    scope = load("scope")
    assert scope["status"] == "qualified_local_fixture_and_combined_wait_exact"
    assert scope["native_execution_included"] is True
    assert scope["cross_platform_ci_qualified"] is False
    assert scope["original_ci_root_cause_proved"] is False
    assert top["scope_sha256"] == digest(raw["scope"])
    for language, expected in top["documents"].items():
        suffix = ".zh-CN.md" if language == "zh-CN" else ".md"
        assert digest((here.parent.parent / (title + suffix)).read_bytes()) == expected

    def frozen(prefix):
        source = load(prefix + "/source-manifest")
        compiled = load(prefix + "/compiled-inputs")
        assert source["count"] == len(source["files"]) == 933
        assert compiled["count"] == len(compiled["files"]) == 542
        assert digest(canonical(source["files"])) == source["source_tree_sha256"]
        assert digest(canonical(compiled["files"])) == compiled["sha256"]
        mapped = {row["path"]: row for row in source["files"]}
        assert len(mapped) == 933
        assert len({row["path"] for row in compiled["files"]}) == 542
        assert all(mapped[row["path"]] == row for row in compiled["files"])
        file_row = mapped[scope["changed_compiled_path"]]
        if prefix != "parent-wait":
            payload = raw[prefix + "/test-source"]
            assert (len(payload), digest(payload)) == (file_row["bytes"], file_row["sha256"])
        return source, compiled

    parent, _ = frozen("parent-wait")
    versions = {name: frozen(name) for name in ("compile-v1", "red-v2", "green-v2")}
    pmap = {row["path"]: row for row in parent["files"]}
    for name, (source, _) in versions.items():
        current = {row["path"]: row for row in source["files"]}
        assert set(current) == set(pmap)
        changed = [path for path in current if current[path] != pmap[path]]
        assert changed == [scope["changed_compiled_path"]], (name, changed)
    green_source, green_compiled = versions["green-v2"]
    assert green_source["source_tree_sha256"] == scope["source_tree_sha256"]
    assert green_compiled["sha256"] == scope["compiled_inputs_ordered_sha256"]
    red = text("red-v2/test-source")
    green = text("green-v2/test-source")
    old = "            } else {\n                tokio::time::sleep(delay).await;"
    new = "            } else if !delay.is_zero() {\n                tokio::time::sleep(delay).await;"
    assert red.count(old) == 1 and red.replace(old, new) == green
    matrix_start = '#[tokio::test(flavor = "multi_thread", worker_threads = 2)]'
    assert red[red.index(matrix_start):] == green[green.index(matrix_start):]

    summary_pattern = re.compile(
        r"^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; "
        r"(\d+) measured; (\d+) filtered out;", re.M
    )

    def summary(key):
        rows = summary_pattern.findall(text(key))
        assert rows, key
        return {"groups": len(rows), "passed": sum(int(row[1]) for row in rows),
                "failed": sum(int(row[2]) for row in rows),
                "ignored": sum(int(row[3]) for row in rows),
                "filtered": sum(int(row[5]) for row in rows)}

    def recorded(prefix, gate, expected_exit):
        receipt = load(prefix + "/" + gate + "-receipt")
        assert receipt["exit_code"] == expected_exit
        assert receipt["log_sha256"] == digest(raw[prefix + "/" + gate + "-log"])
        assert receipt["runner_sha256"] == digest(raw["runner"])
        assert receipt["source_before_after_exact"] is True
        assert receipt["source_tree"] == versions[prefix][0]["source_tree_sha256"]
        assert receipt["native_database_execution"] is False
        assert not any(receipt["override_presence"].values())
        environment = receipt["environment_allowlist"]
        for profile in ("DEV", "TEST"):
            assert environment["CARGO_PROFILE_" + profile + "_OPT_LEVEL"] == "0"
            assert environment["CARGO_PROFILE_" + profile + "_DEBUG_ASSERTIONS"] == "true"
            assert environment["CARGO_PROFILE_" + profile + "_OVERFLOW_CHECKS"] == "true"
        assert environment["CARGO_INCREMENTAL"] == "0"
        return receipt

    recorded("compile-v1", "zero-delay", 101)
    assert "unused import: `std::future::Future`" in text("compile-v1/zero-delay-log")
    assert not summary_pattern.search(text("compile-v1/zero-delay-log"))
    recorded("red-v2", "zero-delay", 101)
    assert '[("task-b", Pending), ("task-a", Pending)]' in text("red-v2/zero-delay-log")
    assert summary("red-v2/zero-delay-log") == dict(groups=1, passed=0, failed=1, ignored=0, filtered=9)
    recorded("green-v2", "zero-delay", 0)
    assert summary("green-v2/zero-delay-log") == dict(groups=1, passed=1, failed=0, ignored=0, filtered=9)
    recorded("green-v2", "tcp", 0)
    assert summary("green-v2/tcp-log") == dict(groups=1, passed=10, failed=0, ignored=0, filtered=0)
    name = "real_server_subscription_authorization_matrix_covers_both_bindings"
    assert "test " + name + " ... ok" in text("green-v2/tcp-log")
    gates = ["fmt", "clippy", "workspace", "doctests", "rustdoc", "nightly"]
    for gate in gates:
        recorded("green-v2", gate, 0)
    driver = load("green-v2/six-gates-receipt")
    assert driver["actual_results"] == [{"gate": gate, "exit_code": 0} for gate in gates]
    assert driver["six_gates_all_passed"] is True
    assert driver["driver_sha256"] == digest(raw["six-gate-driver"])
    assert driver["runner_sha256"] == digest(raw["runner"])
    assert summary("green-v2/workspace-log") == scope["workspace"]
    assert summary("green-v2/doctests-log") == scope["doctests"]
    review = load("independent-review-receipt")
    assert review["reviewer"] == "/root/a2a_fixture_review_oct16c"
    assert review["standards_findings"] == review["spec_findings"] == []
    assert review["cross_platform_ci_qualified"] is False
    assert digest(raw["independent-review-receipt"]) == scope["independent_review_receipt_sha256"]
    normalization = load("native-input-normalization")
    sorted_inputs = load("native-inputs-sorted")
    assert normalization["old_ordered_rows_sha256"] == green_compiled["sha256"]
    assert normalization["source_tree_unchanged"] == green_source["source_tree_sha256"]
    assert sorted_inputs["files"] == sorted(green_compiled["files"], key=lambda row: row["path"])
    assert digest(canonical(sorted_inputs["files"])) == normalization["new_ordered_rows_sha256"]
    assert normalization["only_row_order_changed"] is True
    assert normalization["first_build_preflight_exit_code"] == 1
    assert normalization["first_build_no_cargo_command_started"] is True
    build = load("native/build-receipt")
    build_summary = load("native/build-summary")
    assert build["exit_code"] == 0 and build["source_before_after_exact"] is True
    assert build["source_tree"] == green_source["source_tree_sha256"]
    assert build["compiled_inputs_sha256"] == normalization["new_ordered_rows_sha256"]
    assert build["source_manifest_sha256"] == digest(raw["green-v2/source-manifest"])
    assert build["compiled_manifest_sha256"] == digest(raw["native-inputs-sorted"])
    assert build["stdout_sha256"] == digest(raw["native/build-stdout"])
    assert build["stderr_sha256"] == digest(raw["native/build-stderr"])
    assert build["runner_sha256"] == digest(raw["native/build-runner"])
    assert build["freshness_sha256"] == digest(raw["native/freshness"])
    assert build_summary["build_receipt_sha256"] == digest(raw["native/build-receipt"])
    assert build_summary["inventory_count"] == 193
    assert build_summary["inventory_sha256"] == digest(raw["native/build-inventory"])
    assert sum(line.endswith(": test") for line in text("native/build-inventory").splitlines()) == 193
    artifacts = build_summary["selected_artifacts_all_fresh_false"]
    assert len(artifacts) == 4
    for artifact in artifacts:
        assert artifact["fresh"] is False and artifact["features"] == []
        assert artifact["profile"]["opt_level"] == "1"
        assert artifact["profile"]["debug_assertions"] is True
        assert artifact["profile"]["overflow_checks"] is True
    freshness = load("native/freshness")
    assert freshness["all542_bytes_unchanged"] is True
    assert freshness["all542_mtimes_after_previous_compile"] is True
    assert len(freshness["files"]) == 542
    pair = load("native/pair-receipt")
    assert pair["both_actual_cases_and_all_markers_passed"] is True
    assert pair["runner_sha256"] == digest(raw["native/case-runner"])
    assert pair["orchestrator_sha256"] == digest(raw["native/pair-runner"])
    assert pair["actual_results"] == [
        {"major": major, "actual_runner_exit_code": 0, "two_markers_all_fields_verified": True}
        for major in (16, 17)
    ]
    marker_prefix = "STATEKNOT_SCOPED_WAIT_COMMIT_LOSS_EVIDENCE="
    marker_fields = {
        "profile", "cut", "commit_forwarded", "commit_response_forwarded",
        "writer_killed_and_reaped", "cold_driver_after_authorized_resolution",
        "condition_count", "snapshot_tables", "wait_application_redispatches",
        "scoped_successor_calls", "parent_application_calls", "fresh_old_fence_wait_refused",
        "original_wait_recovered", "original_request_ack_after_return_verified",
        "invariants", "postgres", "termination", "os", "arch",
    }
    for major in (16, 17):
        prefix = "native/pg" + str(major)
        receipt = load(prefix + "/test-receipt")
        assert receipt["exit_code"] == 0 and receipt["timed_out"] is False
        assert receipt["source_tree"] == green_source["source_tree_sha256"]
        assert receipt["compiled_inputs_sha256"] == normalization["new_ordered_rows_sha256"]
        assert receipt["binary_sha256"] == build_summary["binary_sha256"]
        assert receipt["profile_build_receipt_sha256"] == digest(raw["native/build-receipt"])
        assert receipt["runner_sha256"] == digest(raw["native/case-runner"])
        assert receipt["full_inventory_count"] == 193 and receipt["expected_filtered"] == 192
        assert receipt["mandatory_database_env_present"] is True
        assert not any(receipt["overrides_present"].values())
        assert receipt["environment_allowlist"]["CARGO_PROFILE_TEST_OPT_LEVEL"] == "1"
        assert summary(prefix + "/test-stdout") == dict(groups=1, passed=1, failed=0, ignored=0, filtered=192)
        for filename, identifier in (("test.stdout", "test-stdout"), ("test.stderr", "test-stderr")):
            stream = receipt["output_streams"][filename]
            assert stream["redactions"] == 0 and stream["saved_is_raw"] is True
            assert stream["raw_sha256"] == stream["saved_sha256"] == digest(raw[prefix + "/" + identifier])
        assert raw[prefix + "/test-stderr"] == b""
        observations = load(prefix + "/observations")
        assert observations["exact_case_passed"] is True
        assert observations["source_and_binary_before_after_exact"] is True
        assert observations["container_and_durability_settings_unchanged"] is True
        assert observations["test_receipt_sha256"] == digest(raw[prefix + "/test-receipt"])
        markers = [json.loads(line[len(marker_prefix):]) for line in text(prefix + "/test-stdout").splitlines()
                   if line.startswith(marker_prefix)]
        assert len(markers) == 2 and markers == load(prefix + "/markers")
        assert {marker["cut"] for marker in markers} == {"commit_not_forwarded", "commit_response_withheld"}
        for marker in markers:
            committed = marker["cut"] == "commit_response_withheld"
            assert set(marker) == marker_fields
            expected = dict(profile="scoped-wait-commit-loss-v1", commit_forwarded=committed,
                            commit_response_forwarded=False, writer_killed_and_reaped=True,
                            cold_driver_after_authorized_resolution=True, condition_count=2,
                            snapshot_tables=22, wait_application_redispatches=0,
                            scoped_successor_calls=1, parent_application_calls=1,
                            fresh_old_fence_wait_refused=None if committed else True,
                            original_wait_recovered=committed,
                            original_request_ack_after_return_verified=committed,
                            invariants="passed", termination="SIGKILL", os="macos", arch="aarch64")
            for key, value in expected.items():
                assert type(marker[key]) is type(value) and marker[key] == value, key
            assert marker["postgres"].startswith(str(major) + ".")
        settings = []
        for label in ("server-settings-before", "server-settings-after", "database-settings-before", "database-settings-after"):
            value = load(prefix + "/" + label + "-stdout")
            metadata = load(prefix + "/" + label + "-receipt")
            assert metadata["exit_code"] == 0
            assert metadata["stdout_sha256"] == digest(raw[prefix + "/" + label + "-stdout"])
            assert value["server_version_num"].startswith(str(major))
            assert all(value[key] == "on" for key in ("fsync", "synchronous_commit", "full_page_writes"))
            settings.append(value)
        assert all(value == settings[0] for value in settings)
        assert load(prefix + "/container-before") == load(prefix + "/container-after")
        assert load(prefix + "/container-before")["id"] == receipt["database"]["container_id"]
    assert ":1010:5:" in text("ci-old-macos")
    assert ":734:9:" in text("ci-old-windows")
    assert ":736:9:" in text("ci-new-windows")
    assert "left: Null" in text("ci-new-windows")
    if args.workspace_root:
        root = args.workspace_root.resolve()
        for row in green_compiled["files"]:
            path = root / row["path"]
            assert not path.is_symlink()
            content = path.read_bytes()
            assert (len(content), digest(content)) == (row["bytes"], row["sha256"]), row["path"]
    print(json.dumps({"status": "verified_local_fixture_component",
                      "artifacts": len(index["artifacts"]),
                      "source_files": 933, "compiled_inputs": 542,
                      "workspace": scope["workspace"], "doctests": scope["doctests"],
                      "native_execution_included": "PG16/17 Wait exact only", "cross_platform_ci_qualified": False,
                      "original_ci_root_cause_proved": False}, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (AssertionError, KeyError, ValueError, OSError) as error:
        print("verification failed: " + str(error), file=sys.stderr)
        sys.exit(1)
