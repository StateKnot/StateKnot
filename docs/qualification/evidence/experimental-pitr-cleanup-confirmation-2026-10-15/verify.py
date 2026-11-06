#!/usr/bin/env python3
"""Offline byte, execution and exact resource-delta checks; no subprocesses."""
from pathlib import Path
import argparse
import difflib
import gzip
import hashlib
import io
import json
import re
import tarfile
import uuid

TREE = "82fc93f1e66fd13703bd9ae8bff268460829324cebe19ee94eaca497d87aed7d"
PARENT = "88dd9d363194af6c52809fabcd82b9c7e6cba2ee136c95ae161e40a67aebb0eb"
CHANGED = "crates/stateknot-runtime/tests/postgres/child_join_pitr.rs"
EXACT = "child_admission::ownership::join::pitr::consumed_child_join_survives_named_point_in_time_recovery"
BINARY = "994be9d00fe567660a847627c82b96a5e51c45e9ded56b0ba54e6760b75f0a32"
LEGACY = "stateknot-pitr-wal-01a13f6d5ad477909c16dcdd725a26b1"

def sha(raw):
    return hashlib.sha256(raw).hexdigest()

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, help="Optional: exact frozen candidate source directory (not composed main)")
    parser.add_argument("--binary", type=Path, help="Optional: rehash saved Native binary; not included in compact bundle")
    args = parser.parse_args()
    folder = Path(__file__).resolve().parent
    seal = json.loads((folder / "seal.json").read_text())
    assert sha((folder / "artifact-manifest.json").read_bytes()) == seal["artifact_manifest_sha256"]
    manifest = json.loads((folder / "artifact-manifest.json").read_text())
    entries = manifest["artifacts"]
    assert len(entries) == len({row["key"] for row in entries}) == len({row["path"] for row in entries})
    raw_by_key = {}
    for row in entries:
        assert not Path(row["path"]).is_absolute() and ".." not in Path(row["path"]).parts
        stored = (folder / row["path"]).read_bytes()
        assert len(stored) == row["stored_bytes"] and sha(stored) == row["stored_sha256"], row["path"]
        raw = gzip.decompress(stored) if row["encoding"] == "gzip" else stored
        assert row["encoding"] in ["gzip", "identity"]
        if row["encoding"] == "gzip":
            assert stored[4:8] == b"\0\0\0\0", "gzip timestamp must be deterministic"
        assert len(raw) == row["raw_bytes"] and sha(raw) == row["raw_sha256"], row["key"]
        raw_by_key[row["key"]] = raw
    declared = {r["path"] for r in entries} | {"artifact-manifest.json", "seal.json"}
    assert {p.relative_to(folder).as_posix() for p in folder.rglob("*") if p.is_file()} == declared
    def raw(key):
        return raw_by_key[key]
    def js(key):
        return json.loads(raw(key))
    def text(key):
        return raw(key).decode()

    frozen = js("source-manifest.json")
    parent = js("context/final922-source-manifest.json")
    assert frozen["source_tree_sha256"] == TREE and parent["source_tree_sha256"] == PARENT
    for value, tree in [(frozen,TREE),(parent,PARENT)]:
        assert len(value["files"]) == 922
        assert sha(json.dumps(value["files"],sort_keys=True,separators=(",", ":")).encode()) == tree
    candidate_rows = {r["path"]:r for r in frozen["files"]}
    parent_rows = {r["path"]:r for r in parent["files"]}
    assert candidate_rows.keys() == parent_rows.keys()
    assert [p for p in candidate_rows if candidate_rows[p] != parent_rows[p]] == [CHANGED]
    members = {}
    with tarfile.open(fileobj=io.BytesIO(raw("source.tar")),mode="r:") as archive:
        for member in archive.getmembers():
            assert member.isfile() and member.name not in members
            members[member.name] = archive.extractfile(member).read()
    assert set(members) == set(candidate_rows)
    for path, data in members.items():
        assert len(data) == candidate_rows[path]["bytes"] and sha(data) == candidate_rows[path]["sha256"]
    assert raw("candidate/child_join_pitr.rs") == members[CHANGED]
    assert sha(raw("baseline.rs")) == parent_rows[CHANGED]["sha256"]
    preparation = js("preparation.json")
    assert sha(raw("candidate.patch")) == preparation["patch_sha256"]
    assert sha(raw("candidate/child_join_pitr.rs")) == preparation["candidate_sha256"]
    recreated = "".join(difflib.unified_diff(text("baseline.rs").splitlines(True),text("candidate/child_join_pitr.rs").splitlines(True),fromfile="a/"+CHANGED,tofile="b/"+CHANGED))
    assert recreated.encode() == raw("candidate.patch")
    freeze = js("freeze.json")
    assert sha(raw("source-manifest.json")) == freeze["source_manifest_sha256"]
    assert sha(raw("source.tar")) == freeze["source_tar_sha256"]
    review = js("owner-readonly-review-v1/review.json")
    for key in ["baseline_sha256","candidate_sha256","patch_sha256"]:
        assert review[key] == preparation[key]
    assert review["original_Drop_and_docker_command_helpers_byte_identical"] is True
    assert review["original_PITR_business_body_assertions_and_resources_byte_identical"] is True

    gate = js("verification-v1/resource-gate.json")
    assert gate["authorized"] and gate["counterpart_all_requested_phases_terminal"] and gate["counterpart_no_active_processes_confirmed"]
    for index, row in enumerate(gate["terminal_receipts"]):
        assert sha(raw("context/counterpart-" + str(index) + ".json")) == row["sha256"]
    assert js("context/counterpart-4.json")["active_matching_processes"] == []
    prefix = "verification-v1/strict-runtime-build-v1/"
    build = js(prefix + "receipt.json")
    assert build["source_tree_sha256"] == TREE and build["targeted_Runtime_Clippy_exit"] == build["fresh_Runtime_build_exit"] == 0
    assert build["full_workspace_six_gates_qualified"] is False and build["Native_tests_executed"] is False
    names = [line[:-6] for line in text(prefix+"runtime--list.txt").splitlines() if line.endswith(": test")]
    assert len(names) == len(set(names)) == 189 and names == build["exact_test_names"] and names.count(EXACT) == 1
    original = js("context/final922-four-binary-identities.json")["artifacts"]["stateknot-runtime"]["exact_names"]
    assert set(names) == set(original)
    artifact = build["artifacts"][0]
    assert artifact["sha256"] == BINARY and artifact["fresh"] is False
    for stage in ["clippy", "runtime-only-clean", "build"]:
        receipt = js(prefix + stage + "-receipt.json")
        assert receipt["exit_code"] == 0 and receipt["source_tree_sha256"] == TREE
        for suffix in ["stdout", "stderr"]:
            assert sha(raw(prefix+stage+"."+suffix)) == receipt[suffix+"_sha256"]
        running = js(prefix + stage + "-running.json")
        assert running["started_unix"] >= gate["gate_released_unix"]
    environment = js(prefix+"environment.json")
    assert environment["strict"] == {"CARGO_INCREMENTAL":"0","CARGO_PROFILE_DEV_DEBUG":"0","CARGO_PROFILE_TEST_DEBUG":"0","RUSTFLAGS":"-D warnings","RUSTDOCFLAGS":"-D warnings"}
    assert environment["RUST_MIN_STACK_absent"] and environment["DB_test_environment_removed"]
    assert text(prefix+"rustc-version.txt").startswith("rustc 1.88.0")
    cargo_events = [json.loads(line) for line in text(prefix+"build.stdout").splitlines() if line.startswith("{")]
    matches = [e for e in cargo_events if e.get("reason") == "compiler-artifact" and e.get("executable") and e["target"]["name"] == "postgres" and e["package_id"] == artifact["package_id"]]
    assert len(matches) == 1 and matches[0]["fresh"] is False and matches[0]["profile"] == artifact["profile"]
    assert artifact["profile"]["debug_assertions"] and artifact["profile"]["overflow_checks"] and artifact["profile"]["debuginfo"] == 0
    summary = js("verification-v1/mandatory-pitr-v1/summary.json")
    assert summary["both_exact_cases_and_cleanup_confirmations_passed"] and summary["serial_PITR_execution"]
    assert summary["original922_open_cleanup_history_rewritten"] is False and summary["full_workspace_six_gates_or_full_Native_qualified"] is False
    case_rows = []
    for major, version in [(16,"16.14"),(17,"17.11")]:
        stem = "verification-v1/mandatory-pitr-v1/pg" + str(major) + "/"
        receipt = js(stem+"receipt.json")
        case_rows.append(receipt)
        body = text(stem+"native.log")
        assert sha(raw(stem+"native.log")) == receipt["log_sha256"]
        assert re.findall(r"test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;",body) == [("ok","1","0","0")]
        markers = [json.loads(line.split("=",1)[1]) for line in body.splitlines() if line.startswith("STATEKNOT_CHILD_JOIN_PITR_EVIDENCE=")]
        assert len(markers) == 1 and markers == receipt["markers"]
        marker = markers[0]
        assert receipt["exit_code"] == 0 and receipt["binary_sha256"] == BINARY and receipt["source_tree_sha256"] == TREE
        assert marker["postgres"].startswith(version + " ") and marker["profile"] == "child-join-named-pitr-v1" and marker["invariants"] == "passed"
        assert marker["os"] == "macos" and marker["arch"] == "aarch64"
        for key in ["base_backup_verified","post_target_write_excluded","exact_durable_snapshot_verified","noninitial_replay_verified","cross_tenant_refusal_verified","resource_cleanup_verified"]:
            assert marker[key] is True
        ident = marker["resource_identity"]
        assert uuid.UUID(hex=ident).version == 7 and receipt["resource_identity"] == ident
        assert marker["named_restore_point"] == "stateknot_child_join_before_late_write"
        assert re.fullmatch(r"[0-9a-f]{64}",marker["manifest_sha256"]) and re.fullmatch(r"[0-9A-Fa-f]{24}",marker["wal_segment_archived"])
        owned_volumes = {"stateknot-pitr-"+kind+"-"+ident for kind in ["wal","base","target-data"]}
        owned_containers = {"stateknot-pitr-"+kind+"-"+ident for kind in ["source","target"]}
        assert set(receipt["expected_owned_volumes"]) == owned_volumes and set(receipt["expected_owned_containers"]) == owned_containers
        snapshots = []
        for when in ["before","after"]:
            containers = [line.split("\t") for line in text(stem+when+"-containers.txt").splitlines()]
            volumes = {line.split("\t")[0] for line in text(stem+when+"-volumes.txt").splitlines()}
            assert not ({r[1] for r in containers} & owned_containers) and not (volumes & owned_volumes)
            assert LEGACY in volumes
            assert all(not raw(stem+when+"-"+kind+".stderr").strip() for kind in ["containers","volumes","legacy-wal-inspect"])
            snapshots.append(({r[0] for r in containers},volumes))
        assert snapshots[0] == snapshots[1]
        assert raw(stem+"before-legacy-wal-inspect.stdout") == raw(stem+"after-legacy-wal-inspect.stdout")
        assert js(stem+"before-legacy-wal-inspect.stdout")[0]["Name"] == LEGACY
        assert receipt["extra_container_IDs"] == receipt["extra_volume_names"] == []
        assert receipt["exact_case_and_cleanup_confirmation_passed"] and receipt["owned_named_resources_absent_before_and_after"] and receipt["all_prior_container_IDs_and_volume_names_preserved"]
        assert receipt["manual_stop_rm_prune_or_historical_volume_cleanup"] is False
        image = js(stem+"image-inspect.json")[0]
        assert receipt["image"] in image["RepoDigests"] and image["Os"] == "linux" and image["Architecture"] == "arm64"
        assert receipt["command"][1:] == [EXACT,"--exact","--nocapture","--test-threads=1"]
        assert js(stem+"running.json")["started_unix"] >= gate["gate_released_unix"]
    assert summary["rows"] == case_rows
    terminal = js("verification-v1/terminal-resources.json")
    assert terminal["recorded_task_PIDs_absent"] and terminal["source_all_bytes_unchanged"] and terminal["saved_binary_rehashed"]
    assert terminal["cache_no_open_files"] and terminal["cargo_lock_nonblocking_acquired_released"] and terminal["cache_cleaned"] is False
    assert terminal["free_KiB"] >= 50*1024*1024 and terminal["retained_task_cache_KiB"] <= 20*1024*1024
    profile = js("qualification.json")
    assert profile["targeted_Runtime_Clippy_passed"] and profile["two_exact_PITR_execution_and_owned_cleanup_passed"]
    assert profile["full_workspace_six_gates_qualified"] is False and profile["full_Runtime189_executed"] is False and profile["R1_closed"] is False
    assert profile["historical_WAL_volume_removed"] is False and profile["all_failure_path_cleanup_qualified"] is False
    if args.source:
        paths = {p.relative_to(args.source).as_posix():p for p in args.source.rglob("*") if p.is_file()}
        assert paths.keys() == members.keys()
        assert all(p.read_bytes() == members[name] for name,p in paths.items())
    if args.binary:
        assert sha(args.binary.read_bytes()) == BINARY and args.binary.stat().st_size == artifact["bytes"]
    print(json.dumps({"verified":True,"artifacts":len(entries),"frozen_source_files":922,"source_tree_sha256":TREE,"exact_tests_executed":2,"each_pass_fail_ignore":[1,0,0],"new_owned_resource_cleanup_confirmed_each":True,"legacy_WAL_preserved":True,"source_directory_checked":bool(args.source),"saved_binary_checked":bool(args.binary),"full_workspace_six_gates_or_R1_qualified":False},indent=2))

if __name__ == "__main__":
    main()
