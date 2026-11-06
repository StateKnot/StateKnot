#!/usr/bin/env python3
"""Read archived evidence only; never execute an archived runner, Rust, SQL or CI."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path, PurePosixPath
import re

ROOT = Path(__file__).resolve().parent
NAME = "experimental-scoped-wait-process-faults-2026-10-16"
TREE = "b286803afacf84278ffa579e8f898610e9474378bd982897e90f1c3c0f0c718c"
COMPILED = "43877dc8d16584c5a957356a1d0310ac4faea1525b743c05c4a159178780c305"
INDEX = "3834a4ca9a6f457bd9df83e298b72054623bd3b9743ebd5d815ccbfd6bf897e7"
PREVIOUS_INDEX = "db328636614104c5f408e5c5e7163287e75ffe70fd811d9190116ef7ab702f7c"
MANIFEST = "a503bcbbdd88b94b9ba71fc3b67d6626b6b6734b1a68fa6f4bca71a750f727c5"
BINARY = "ade87f1b40b8a1b28da07d1908e73114e40d77f0595a9d6e919ae6dc8306a1f8"
CASE = "nested_driver::frame_wait_process_faults::scoped_wait_commit_loss_survives_real_kill_authorized_resolution_and_cold_driver"
PREFIX = "STATEKNOT_SCOPED_WAIT_COMMIT_LOSS_EVIDENCE="
KEYS = {"profile", "cut", "commit_forwarded", "commit_response_forwarded",
        "writer_killed_and_reaped", "cold_driver_after_authorized_resolution",
        "condition_count", "snapshot_tables", "wait_application_redispatches",
        "scoped_successor_calls", "parent_application_calls", "fresh_old_fence_wait_refused",
        "original_wait_recovered", "original_request_ack_after_return_verified",
        "invariants", "postgres", "termination", "os", "arch"}
ROWS = {}
sha = lambda value: hashlib.sha256(value).hexdigest()
canon = lambda value: json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate JSON key: " + key)
        result[key] = value
    return result


def reject_constant(value):
    raise ValueError("nonstandard JSON constant: " + value)


def json_bytes(raw):
    return json.loads(raw.decode("utf-8", errors="strict"),
                      object_pairs_hook=unique_object, parse_constant=reject_constant)


def safe_path(name):
    path = PurePosixPath(name)
    require(type(name) is str and path.parts and not path.is_absolute() and
            ".." not in path.parts and path.as_posix() == name, "unsafe artifact path")
    target = ROOT.joinpath(*path.parts)
    require(not target.is_symlink() and target.resolve().is_relative_to(ROOT), "artifact escapes root")
    return target


def raw(name):
    if name not in ROWS and name + ".gz" in ROWS:
        name += ".gz"
    require(name in ROWS, "unindexed artifact requested: " + name)
    stored = safe_path(name).read_bytes()
    return gzip.decompress(stored) if ROWS[name]["gzip"] else stored


def obj(name):
    return json_bytes(raw(name))


def summaries(value):
    return [tuple(map(int, row)) for row in re.findall(
        rb"^test result: ok[.] (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out; finished in [0-9.]+s$",
        value, re.MULTILINE)]


def totals(value):
    rows = summaries(value)
    return [len(rows)] + [sum(row[index] for row in rows) for index in range(5)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace-root", type=Path,
                        help="Additionally compare the 542 supplied inputs; permits unrelated extra files.")
    args = parser.parse_args()
    old_raw = (ROOT / "artifact-index.json").read_bytes()
    index_raw = (ROOT / "artifact-index-v2.json").read_bytes()
    require(sha(old_raw) == PREVIOUS_INDEX and sha(index_raw) == INDEX, "index bytes differ")
    old, index = json_bytes(old_raw), json_bytes(index_raw)
    require(index["version"] == 2 and len(old["files"]) == 181 and len(index["files"]) == 184,
            "index version/count differs")
    require(index["files"][:181] == old["files"], "old indexed entries changed")
    require(index["predecessor_index"]["sha256"] == PREVIOUS_INDEX, "index predecessor differs")
    for row in index["files"]:
        require(type(row["gzip"]) is bool and row["path"] not in ROWS, "invalid or duplicate index row")
        stored = safe_path(row["path"]).read_bytes()
        value = gzip.decompress(stored) if row["gzip"] else stored
        require((len(stored), sha(stored), len(value), sha(value)) ==
                (row["stored_bytes"], row["stored_sha256"], row["raw_bytes"], row["raw_sha256"]),
                "stored/raw artifact mismatch: " + row["path"])
        ROWS[row["path"]] = row
    expected_files = set(ROWS) | {"artifact-index.json", "artifact-index-v2.json", "manifest.json", "verify.py"}
    require({p.relative_to(ROOT).as_posix() for p in ROOT.rglob("*") if p.is_file()} == expected_files,
            "unindexed or missing evidence file")
    manifest_raw = (ROOT / "manifest.json").read_bytes()
    require(sha(manifest_raw) == MANIFEST, "original manifest bytes differ")
    manifest = json_bytes(manifest_raw)
    metadata = json_bytes((ROOT.parent.parent / (NAME + ".json")).read_bytes())
    require(metadata["manifest_sha256"] == MANIFEST and metadata["artifact_index"]["sha256"] == INDEX,
            "machine scope is not bound to these artifacts")
    require(metadata["verification"]["verifier_sha256"] == sha(Path(__file__).read_bytes()),
            "verifier bytes differ from machine scope")
    require(metadata["source_tree_sha256"] == TREE and metadata["supplied_compilation_sha256"] == COMPILED,
            "machine source scope differs")
    require(metadata["production_Rust_SQL_API_migrations_or_business_budgets_changed"] is False,
            "unexpected production scope")
    source, supplied = obj("gates-v2/frozen-source-manifest.json"), obj("gates-v2/compiled-inputs.json")
    require(source["count"] == len(source["files"]) == 933 and
            sha(canon(source["files"])) == source["source_tree_sha256"] == TREE, "source933 manifest differs")
    require(supplied["count"] == len(supplied["files"]) == 542 and
            sha(canon(supplied["files"])) == supplied["sha256"] == COMPILED and
            supplied["files"] == sorted(supplied["files"], key=lambda row: row["path"]), "compiled542 manifest differs")
    source_rows = {row["path"]: row for row in source["files"]}
    require(len(source_rows) == 933 and all(source_rows[row["path"]] == row for row in supplied["files"]),
            "compilation rows do not belong to selected source")
    require(manifest["source_tree_sha256"] == TREE and manifest["supplied_compilation_sha256"] == COMPILED,
            "original scope manifest differs")
    require(len(manifest["adopt_only_test_paths"]) == 3 and
            manifest["adopt_only_test_paths"] == metadata["adopt_only_test_paths"], "test adoption scope differs")
    for name in manifest["adopt_only_test_paths"]:
        value = raw("source/" + name + ".gz")
        require((len(value), sha(value)) == (source_rows[name]["bytes"], source_rows[name]["sha256"]),
                "adopted test payload differs: " + name)
    module = raw("source/crates/stateknot-runtime/tests/postgres/frame_wait_process_faults.rs.gz").decode()
    first_module = raw("history/first-formatted-wait.rs.gz").decode()
    original_header = "async fn resolve_all(store: &PostgresStore, db: &PgPool, scenario: &Scenario) {\n"
    helper_header = ("async fn assert_resolution_authority_required(\n"
                     "    store: &PostgresStore,\n    db: &PgPool,\n    scenario: &Scenario,\n) {\n")
    join = "}\n\n" + original_header + "    assert_resolution_authority_required(store, db, scenario).await;\n"
    require(module.count(helper_header) == module.count(join) == 1 and
            module.replace(helper_header, original_header, 1).replace(join, "", 1) == first_module,
            "private helper extraction no longer restores the exact first formatted source")
    authoring = obj("authoring-v2/manifest.json")
    require(sha(raw("authoring-v2/manifest.json")) == "fced554cbb3b3566f571d4fb4679e77c2c114e06dbda8a093db11db1070349ba" and
            sha(raw("authoring-v2/candidate.patch")) == authoring["patch_sha256"] and
            gzip.decompress(raw("authoring-v2/candidate.patch.gz")) == raw("authoring-v2/candidate.patch"),
            "original raw authoring subject differs")
    gates = ("fmt", "clippy", "workspace", "doctests", "rustdoc", "nightly")
    driver = obj("gates-v2/six-gates-driver.receipt.json")
    require(driver["six_gates_all_passed"] is True and driver["actual_results"] ==
            [{"gate": name, "exit_code": 0} for name in gates], "six actual outcomes differ")
    require(driver["driver_sha256"] == sha(raw("gates-v2/run-six-gates.py")) and
            driver["runner_sha256"] == sha(raw("gates-v2/run-gate.py")), "gate runner binding differs")
    expected_argv = {
        "fmt": ["cargo", "+1.88.0", "fmt", "--all", "--check"],
        "clippy": ["cargo", "+1.88.0", "clippy", "--workspace", "--all-targets", "--all-features", "--locked", "--", "-D", "warnings"],
        "workspace": ["cargo", "+1.88.0", "test", "--workspace", "--all-targets", "--all-features", "--locked"],
        "doctests": ["cargo", "+1.88.0", "test", "--workspace", "--doc", "--all-features", "--locked"],
        "rustdoc": ["cargo", "+1.88.0", "doc", "--workspace", "--all-features", "--no-deps", "--locked"],
        "nightly": ["cargo", "+nightly-2026-10-07", "check", "--workspace", "--all-targets", "--all-features", "--locked"],
    }
    for gate in gates:
        receipt = obj("gates-v2/" + gate + ".receipt.json")
        require(receipt["argv"] == expected_argv[gate], "gate command was narrowed: " + gate)
        require(receipt["exit_code"] == 0 and receipt["source_before_after_exact"] is True and
                receipt["source_tree"] == TREE and receipt["log_sha256"] == sha(raw("gates-v2/" + gate + ".log")),
                "gate result/raw hash differs: " + gate)
        env = receipt["environment_allowlist"]
        require(all(env[key] == value for key, value in {
            "CARGO_INCREMENTAL": "0", "CARGO_PROFILE_DEV_DEBUG": "0", "CARGO_PROFILE_TEST_DEBUG": "0",
            "CARGO_PROFILE_DEV_OPT_LEVEL": "0", "CARGO_PROFILE_TEST_OPT_LEVEL": "0",
            "CARGO_PROFILE_DEV_DEBUG_ASSERTIONS": "true", "CARGO_PROFILE_TEST_DEBUG_ASSERTIONS": "true",
            "CARGO_PROFILE_DEV_OVERFLOW_CHECKS": "true", "CARGO_PROFILE_TEST_OVERFLOW_CHECKS": "true",
            "RUSTFLAGS": "-D warnings", "RUSTDOCFLAGS": "-D warnings"}.items()), "local opt0 profile differs")
    require("+nightly-2026-10-07" in obj("gates-v2/nightly.receipt.json")["argv"], "nightly pin differs")
    require(totals(raw("gates-v2/workspace.log")) == [100, 2123, 0, 6, 0, 0], "workspace totals differ")
    require(totals(raw("gates-v2/doctests.log")) == [10, 48, 0, 0, 0, 0], "doctest totals differ")
    first = obj("history/qualification-v1/clippy.receipt.json")
    require(first["exit_code"] == 101 and first["log_sha256"] == sha(raw("history/qualification-v1/clippy.log")) and
            b"too many lines (103/100)" in raw("history/qualification-v1/clippy.log"), "original Clippy failure missing")
    build = obj("native/build-runtime/build.receipt.json")
    built = obj("native/build-runtime/build-summary.json")
    require(build["exit_code"] == 0 and build["source_before_after_exact"] is True and
            build["source_tree"] == TREE and build["compiled_inputs_sha256"] == COMPILED,
            "Native build source/outcome differs")
    require(build["stdout_sha256"] == sha(raw("native/build-runtime/build.jsonl")) and
            build["stderr_sha256"] == sha(raw("native/build-runtime/build.stderr")) and
            built["build_receipt_sha256"] == sha(raw("native/build-runtime/build.receipt.json")), "Native build hashes differ")
    artifacts = [json_bytes(line) for line in raw("native/build-runtime/build.jsonl").splitlines()
                 if json_bytes(line).get("reason") == "compiler-artifact"]
    require(artifacts == obj("native/build-runtime/compiler-artifacts.json") and
            built["all_compiler_artifacts_sha256"] == sha(raw("native/build-runtime/compiler-artifacts.json")),
            "actual Cargo artifact inventory differs")
    selected = built["selected_artifacts_all_fresh_false"]
    require(len(selected) == 4 and {(x["target"]["name"], tuple(x["target"]["kind"])) for x in selected} ==
            {("stateknot_core", ("lib",)), ("stateknot_store_postgres", ("lib",)),
             ("stateknot_runtime", ("lib",)), ("postgres", ("test",))}, "selected Native targets differ")
    for item in selected:
        profile = item["profile"]
        require(item in artifacts and item["fresh"] is False and item["features"] == [] and
                profile["opt_level"] == "1" and profile["debuginfo"] == 0 and
                profile["debug_assertions"] is True and profile["overflow_checks"] is True and
                profile["test"] is (item["target"]["kind"] == ["test"]), "fresh/default opt1 Native artifact differs")
    require(build["argv"] == ["cargo", "+1.88.0", "test", "-p", "stateknot-runtime", "--test", "postgres", "--locked",
                              "--no-run", "--message-format=json"], "Native build command differs")
    require(all(build["environment_allowlist"][key] == value for key, value in {
        "CARGO_INCREMENTAL": "0", "CARGO_PROFILE_DEV_DEBUG": "0", "CARGO_PROFILE_TEST_DEBUG": "0",
        "CARGO_PROFILE_TEST_OPT_LEVEL": "1", "CARGO_PROFILE_TEST_DEBUG_ASSERTIONS": "true",
        "CARGO_PROFILE_TEST_OVERFLOW_CHECKS": "true", "RUSTFLAGS": "-D warnings", "RUSTDOCFLAGS": "-D warnings"}.items()),
        "Native six-profile/strict warnings differ")
    require(build["DEV_OPT_LEVEL_override_present"] is False and
            build["runner_sha256"] == sha(raw("native/runner/build.py")), "Native helper/profile changed")
    fresh = obj("native/build-runtime/freshness.json")
    previous_raw = raw("native/build-runtime/previous-compile.receipt.json")
    previous = json_bytes(previous_raw)
    cutoff = int((previous["start_unix"] + previous["seconds"]) * 1_000_000_000)
    require(previous["exit_code"] == 0 and fresh["previous_compile_receipt_sha256"] == sha(previous_raw) and
            fresh["previous_compile_end_unix_ns"] == cutoff and len(fresh["files"]) == 542,
            "historical compile freshness cutoff differs")
    require([(x["path"], x["sha256"]) for x in fresh["files"]] ==
            [(x["path"], x["sha256"]) for x in supplied["files"]] and
            all(x["after_mtime_ns"] == fresh["refresh_unix_ns"] and x["after_mtime_ns"] > cutoff
                for x in fresh["files"]) and build["freshness_sha256"] == sha(raw("native/build-runtime/freshness.json")),
            "all542 historical mtime/byte witnesses differ")
    require(built["binary_sha256"] == BINARY and built["binary_bytes"] == 50408400 and
            built["inventory_count"] == 193 and built["exact_case"] == CASE and
            built["inventory_sha256"] == sha(raw("native/build-runtime/inventory.stdout")), "binary/inventory binding differs")
    names = re.findall(rb"^([^\n]+): test$", raw("native/build-runtime/inventory.stdout"), re.MULTILINE)
    require(len(names) == len(set(names)) == 193 and CASE.encode() in names, "actual193 inventory differs")
    pair = obj("native/pair-execution-v1/execution.receipt.json")
    require(pair["both_actual_cases_and_all_markers_passed"] is True and pair["actual_results"] == [
        {"major": major, "actual_runner_exit_code": 0, "two_markers_all_fields_verified": True} for major in (16, 17)],
        "actual Runtime wrappers differ")
    require(pair["orchestrator_sha256"] == sha(raw("native/run-pair-v1.py")) and
            pair["runner_sha256"] == sha(raw("native/runner/run-case.py")), "Native pair runner hash differs")
    actual_cases = []
    for major in (16, 17):
        folder = "native/pg" + str(major) + "/"
        receipt, observed = obj(folder + "test.receipt.json"), obj(folder + "observations.json")
        require(receipt["exit_code"] == 0 and receipt["timed_out"] is False and
                receipt["source_tree"] == TREE and receipt["compiled_inputs_sha256"] == COMPILED and
                receipt["binary_sha256"] == BINARY and receipt["full_inventory_count"] == 193 and
                receipt["expected_filtered"] == 192 and receipt["argv"][1:] ==
                [CASE, "--exact", "--test-threads=1", "--nocapture"], "Runtime exact result/source differs")
        require(receipt["runner_sha256"] == sha(raw("native/runner/run-case.py")) and
                receipt["shared_build_module_sha256"] == sha(raw("native/runner/build.py")) and
                receipt["profile_build_receipt_sha256"] == sha(raw("native/build-runtime/build.receipt.json")),
                "actual executed runner/build bindings differ")
        require(receipt["environment_allowlist"] == build["environment_allowlist"] and
                not any(receipt["overrides_present"].values()) and receipt["mandatory_database_env_present"] is True,
                "Runtime environment/default stack differs")
        require(observed["exact_case_passed"] is True and observed["source_and_binary_before_after_exact"] is True and
                observed["container_and_durability_settings_unchanged"] is True and
                observed["test_receipt_sha256"] == sha(raw(folder + "test.receipt.json")), "Runtime post-observation differs")
        for stream in ("test.stdout", "test.stderr"):
            recorded = receipt["output_streams"][stream]
            require(recorded["saved_is_raw"] is True and recorded["redactions"] == 0 and
                    recorded["raw_sha256"] == recorded["saved_sha256"] == sha(raw(folder + stream)), "raw stream differs")
        output = raw(folder + "test.stdout")
        require(summaries(output) == [(1, 0, 0, 0, 192)] and output.splitlines().count(b"running 1 test") == 1,
                "actual1/192 harness differs")
        starts = re.findall(rb"^test " + re.escape(CASE.encode()) + rb" \.\.\. (?:ok)?$", output, re.MULTILINE)
        require(len(starts) == 1 and len(re.findall(rb"^ok$", output, re.MULTILINE)) == 1,
                "nocapture selected start/completion differs")
        auxiliary_names = {"container-before", "container-after", "server-settings-before", "server-settings-after",
                           "database-absent", "create-database", "database-created", "database-settings-before",
                           "database-settings-after", "full-inventory"}
        require({key[len(folder):-len(".receipt.json")] for key in ROWS if key.startswith(folder) and
                 key.endswith(".receipt.json") and key != folder + "test.receipt.json"} == auxiliary_names,
                "complete auxiliary command receipt set differs")
        for key in ROWS:
            if key.startswith(folder) and key.endswith(".receipt.json") and key != folder + "test.receipt.json":
                auxiliary = obj(key)
                stem = key[:-len(".receipt.json")]
                require(auxiliary["exit_code"] == 0 and auxiliary["stdout_sha256"] == sha(raw(stem + ".stdout")) and
                        auxiliary["stderr_sha256"] == sha(raw(stem + ".stderr")), "auxiliary command/hash differs")
        container = obj(folder + "container-before.stdout")
        require(container == obj(folder + "container-after.stdout") and container["id"] == receipt["database"]["container_id"] and
                container["image"] == receipt["database"]["image_id"] and container["running"] is True and
                container["labels"]["stateknot.task"] == "r1-oct15", "owned container identity changed")
        identities = {
            16: ("cd3def02c3d0888a0d3cf2d73ab6400ce02e80c65d961f7a5186a2017b0fa7ea",
                 "sha256:a3b7f434b2dc57ce85a67e171163eb8ab1a1ebcb39d27484661f26b1dfbe30d6", 55416),
            17: ("700ced678f1efb67e230785eb4d42d77c0327e64d032f6fd12ed386e46f43127",
                 "sha256:f4c66b820c6f974249089d3d16d86a3698eae11e8746eb6644b2271031e91232", 55417),
        }
        identity, image, port = identities[major]
        require(container["id"] == identity and container["image"] == image and
                container["ports"]["5432/tcp"] == [{"HostIp": "127.0.0.1", "HostPort": str(port)}],
                "fixed owned container/loopback profile differs")
        require(raw(folder + "database-absent.stdout").strip() == b"0" and
                raw(folder + "database-created.stdout").decode().strip() == receipt["database"]["name"], "fresh database differs")
        for scope in ("server", "database"):
            before = obj(folder + scope + "-settings-before.stdout")
            require(before == obj(folder + scope + "-settings-after.stdout") and
                    int(before["server_version_num"]) // 10000 == major and
                    all(before[key] == "on" for key in ("fsync", "synchronous_commit", "full_page_writes")),
                    "actual durability flags changed")
        markers = [json_bytes(line[len(PREFIX):].encode()) for line in output.decode().splitlines() if line.startswith(PREFIX)]
        require(len(markers) == 2 and {x["cut"] for x in markers} == {"commit_not_forwarded", "commit_response_withheld"},
                "two Wait cuts missing")
        for marker in markers:
            require(type(marker) is dict and set(marker) == KEYS, "19-key Wait marker schema differs")
            committed = marker["cut"] == "commit_response_withheld"
            expected = {"profile": "scoped-wait-commit-loss-v1", "commit_forwarded": committed,
                        "commit_response_forwarded": False, "writer_killed_and_reaped": True,
                        "cold_driver_after_authorized_resolution": True, "condition_count": 2, "snapshot_tables": 22,
                        "wait_application_redispatches": 0, "scoped_successor_calls": 1, "parent_application_calls": 1,
                        "fresh_old_fence_wait_refused": None if committed else True,
                        "original_wait_recovered": committed, "original_request_ack_after_return_verified": committed,
                        "invariants": "passed", "termination": "SIGKILL", "os": "macos", "arch": "aarch64"}
            require(all(type(marker[key]) is type(value) and marker[key] == value for key, value in expected.items()),
                    "Wait marker field/type differs")
            minor = "16.15" if major == 16 else "17.11"
            require(type(marker["postgres"]) is str and re.fullmatch(re.escape(minor) + r"(?: .*)?", marker["postgres"]),
                    "actual PostgreSQL profile differs")
        require(markers[0]["postgres"] == markers[1]["postgres"] and markers == obj(folder + "verified-wait-markers.json"),
                "Wait marker records differ")
        actual_cases.append({"major": major, "actual_test_exit_code": 0, "actual_wrapper_exit_code": 0,
                             "seconds": receipt["seconds"], "selected_passed": 1, "filtered_out": 192})
    require(obj("reviews/graph-frame-wait-process-fault-execution-review-v1/receipt.json")["result"]["blocking_findings"] == 0,
            "independent execution review has remaining findings")
    if args.workspace_root:
        workspace = args.workspace_root.resolve()
        for row in supplied["files"]:
            path = workspace / row["path"]
            require(not path.is_symlink() and path.is_file() and
                    (path.stat().st_size, sha(path.read_bytes())) == (row["bytes"], row["sha256"]),
                    "supplied workspace input differs: " + row["path"])
    print(json.dumps({"status": "passed", "artifact_index_version": 2, "indexed_artifacts": len(ROWS),
                      "source_manifest_files": 933, "supplied_inputs": 542,
                      "workspace_inputs_compared": args.workspace_root is not None,
                      "six_actual_gates_zero": True, "workspace": [100, 2123, 0, 6, 0, 0],
                      "doctests": [10, 48, 0, 0, 0, 0], "Native_exact_controllers": actual_cases,
                      "omitted_binary_bytes_recomputed": False, "archived_scripts_Rust_DB_or_CI_executed": False,
                      "full_Native_CI_R1_or_release_qualification": False}, indent=2))


if __name__ == "__main__":
    main()
