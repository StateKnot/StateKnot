#!/usr/bin/env python3
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0
"""Single-fixture confirmation. Build/native phases require the A2A resource gate."""
from pathlib import Path
import argparse
import fcntl
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import traceback
import uuid


OUT = Path(__file__).resolve().parent
FROZEN = OUT.parent
BASE = FROZEN.parent
ROOT = BASE.parent.parent
SOURCE = BASE / "elapsed36-pitr-cleanup-source"
TARGET = ROOT / "target/r1-oct15-elapsed36-lint"
TREE = "82fc93f1e66fd13703bd9ae8bff268460829324cebe19ee94eaca497d87aed7d"
EXACT = "child_admission::ownership::join::pitr::consumed_child_join_survives_named_point_in_time_recovery"
LEGACY_WAL = "stateknot-pitr-wal-01a13f6d5ad477909c16dcdd725a26b1"
IMAGES = {
    16:"postgres@sha256:fe03a7605299a34ddf5e4f285dff78c3d7190a576b3c6b46f2fcff69f4bffd54",
    17:"postgres@sha256:67f41722b7a8cbdb868a44a4995c846eddfdc2973bccb291ce937dce88ad5675",
}
COUNTS = re.compile(r"test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;")


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def read(path):
    return json.loads(path.read_text())


def source_check(directory, label):
    manifest = read(FROZEN / "source-manifest.json")
    assert manifest["source_tree_sha256"] == TREE and len(manifest["files"]) == 922
    actual = {p.relative_to(SOURCE).as_posix():p for p in SOURCE.rglob("*") if p.is_file()}
    assert set(actual) == {x["path"] for x in manifest["files"]}
    for item in manifest["files"]:
        data = actual[item["path"]].read_bytes()
        assert len(data) == item["bytes"] and sha(data) == item["sha256"], item["path"]
    assert sha(json.dumps(manifest["files"],sort_keys=True,separators=(",",":")).encode()) == TREE
    write(directory / (label + "-source-proof.json"), {"source_files":922,"source_tree_sha256":TREE,"all_bytes_match":True})


def disk(directory, label):
    raw = subprocess.check_output(["df","-k",str(ROOT)])
    (directory / (label + ".df")).write_bytes(raw)
    available = int(raw.decode().splitlines()[-1].split()[3])
    assert available >= 50 * 1024 * 1024, "disk below 50 GiB; evaluate this task's rebuildable artifacts first"
    return available


def gate():
    value = read(OUT / "resource-gate.json")
    assert value.get("authorized") is True, "A2A chain still owns the resource window"
    assert value["counterpart_all_requested_phases_terminal"] is True
    assert value["counterpart_no_active_processes_confirmed"] is True
    assert value["terminal_receipts"], "actual terminal receipt references required"
    for item in value["terminal_receipts"]:
        path = ROOT / item["path"]
        assert path.is_file() and sha(path.read_bytes()) == item["sha256"]
    return value


def environment():
    env = os.environ.copy()
    assert "RUST_MIN_STACK" not in env and "CARGO_ENCODED_RUSTFLAGS" not in env
    for key in list(env):
        if key.startswith("STATEKNOT_TEST_") or key.startswith("STATEKNOT_REQUIRE_") or key in ["STATEKNOT_PITR_POSTGRES_IMAGE","STATEKNOT_PROOF_BASE_PROBE","STATEKNOT_SQL37_TRACE_DIR"]:
            env.pop(key,None)
    env.update(CARGO_TARGET_DIR=str(TARGET),CARGO_INCREMENTAL="0",CARGO_PROFILE_DEV_DEBUG="0",CARGO_PROFILE_TEST_DEBUG="0")
    for key in ["RUSTFLAGS","RUSTDOCFLAGS"]:
        env[key] = (env.get(key,"") + " -D warnings").strip()
    return env


def cache_preflight(directory, label):
    disk(directory,label)
    assert TARGET.resolve() == ROOT.resolve() / "target/r1-oct15-elapsed36-lint"
    assert (TARGET / "CACHEDIR.TAG").read_text().startswith("Signature: 8a477f597d28d172789f06886806bc55")
    tracked = subprocess.check_output(["git","ls-files","--",str(TARGET.relative_to(ROOT))],cwd=ROOT)
    assert not tracked.strip()
    opened = subprocess.run(["lsof","+D",str(TARGET)],capture_output=True,text=True)
    (directory / (label + "-cache-lsof.txt")).write_text(opened.stdout + opened.stderr)
    assert opened.returncode == 1 and not opened.stdout.strip() and not opened.stderr.strip(), "cache has open files or lsof could not prove inactivity"
    with (TARGET / "debug/.cargo-lock").open("a+") as lock:
        fcntl.flock(lock.fileno(),fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(lock.fileno(),fcntl.LOCK_UN)
    size = subprocess.check_output(["du","-sk",str(TARGET)])
    (directory / (label + "-cache-size.txt")).write_bytes(size)
    assert int(size.split()[0]) <= 20 * 1024 * 1024, "cache above 20 GiB; evaluate before building"
    write(directory / (label + "-cache-guard.json"), {"real_path":str(TARGET.resolve()),"Cargo_CACHEDIR_TAG_verified":True,"git_untracked_cache":True,"lsof_no_open_files":True,"cargo_lock_nonblocking_acquired_and_released":True,"no_target_copy":True})


def run_logged(command, directory, stem, env):
    started = time.time()
    with (directory / (stem + ".stdout")).open("wb") as stdout, (directory / (stem + ".stderr")).open("wb") as stderr:
        process = subprocess.Popen(command,cwd=SOURCE,env=env,stdout=stdout,stderr=stderr)
        write(directory / (stem + "-running.json"), {"pid":process.pid,"command":command,"started_unix":started})
        code = process.wait()
    receipt = {"command":command,"exit_code":code,"seconds":time.time()-started,"stdout_sha256":sha((directory/(stem+".stdout")).read_bytes()),"stderr_sha256":sha((directory/(stem+".stderr")).read_bytes()),"source_tree_sha256":TREE}
    write(directory / (stem + "-receipt.json"),receipt)
    return receipt


def build():
    authorization = gate()
    directory = OUT / "strict-runtime-build-v1"
    directory.mkdir(exist_ok=False)
    write(directory / "resource-gate-snapshot.json",authorization)
    source_check(directory,"before")
    cache_preflight(directory,"before")
    env = environment()
    for tool in ["cargo","rustc"]:
        (directory/(tool+"-version.txt")).write_bytes(subprocess.check_output([tool,"--version"],env=env,cwd=SOURCE))
    assert (directory/"rustc-version.txt").read_text().startswith("rustc 1.88.0")
    strict = {key:env[key] for key in ["CARGO_INCREMENTAL","CARGO_PROFILE_DEV_DEBUG","CARGO_PROFILE_TEST_DEBUG","RUSTFLAGS","RUSTDOCFLAGS"]}
    write(directory / "environment.json",{"strict":strict,"DB_test_environment_removed":True,"RUST_MIN_STACK_absent":True,"target":str(TARGET)})
    command = ["cargo","clippy","--manifest-path",str(SOURCE/"Cargo.toml"),"-p","stateknot-runtime","--all-targets","--all-features","--locked","--","-D","warnings"]
    clippy = run_logged(command,directory,"clippy",env)
    source_check(directory,"after-clippy")
    assert clippy["exit_code"] == 0, "targeted strict Runtime Clippy failed; original red retained"
    cache_preflight(directory,"before-runtime-only-clean")
    clean = ["cargo","clean","--manifest-path",str(SOURCE/"Cargo.toml"),"--target-dir",str(TARGET),"-p","stateknot-runtime"]
    cleaned = run_logged(clean,directory,"runtime-only-clean",env)
    assert cleaned["exit_code"] == 0
    command = ["cargo","test","--manifest-path",str(SOURCE/"Cargo.toml"),"-p","stateknot-runtime","--test","postgres","--all-features","--locked","--no-run","--message-format=json"]
    compiled = run_logged(command,directory,"build",env)
    source_check(directory,"after-build")
    assert compiled["exit_code"] == 0, "fresh Runtime build failed; original red retained"
    artifacts=[]; libraries=[]
    for line in (directory/"build.stdout").read_text().splitlines():
        try:event=json.loads(line)
        except json.JSONDecodeError:continue
        if event.get("reason") != "compiler-artifact" or str(SOURCE/"crates/stateknot-runtime") not in event.get("package_id",""):
            continue
        if "lib" in event.get("target",{}).get("kind",[]):
            libraries.append(event)
            assert event["fresh"] is False
        if event.get("executable") and event["target"]["name"] == "postgres":
            assert event["fresh"] is False
            profile=event["profile"]
            assert profile["debug_assertions"] and profile["overflow_checks"] and profile["debuginfo"] == 0 and profile["opt_level"] == "0"
            saved=directory/"stateknot-runtime-postgres"
            shutil.copy2(event["executable"],saved)
            artifacts.append({"package":"stateknot-runtime","package_id":event["package_id"],"path":str(saved),"sha256":sha(saved.read_bytes()),"bytes":saved.stat().st_size,"fresh":False,"profile":profile})
    assert len(artifacts) == 1 and libraries
    listed = subprocess.run([artifacts[0]["path"],"--list","--format","terse"],cwd=SOURCE,env=env,capture_output=True,text=True)
    (directory/"runtime--list.txt").write_text(listed.stdout)
    (directory/"runtime--list.stderr").write_text(listed.stderr)
    assert listed.returncode == 0
    names=[line.removesuffix(": test") for line in listed.stdout.splitlines() if line.endswith(": test")]
    original=read(BASE/"elapsed36-attempt-proof-integration/final-test-fixes-v1/four-binary-identities.json")["artifacts"]["stateknot-runtime"]["exact_names"]
    assert len(names) == len(set(names)) == 189 and set(names) == set(original)
    assert names.count(EXACT) == 1
    disk(directory,"after")
    write(directory/"receipt.json",{"source":str(SOURCE),"source_tree_sha256":TREE,"source_files":922,"source_unchanged":True,"targeted_Runtime_Clippy_exit":0,"fresh_Runtime_build_exit":0,"artifacts":artifacts,"library_fresh_false":True,"exact_test_names":names,"exact_test_count":189,"strict_environment":strict,"default_stack":True,"Native_tests_executed":False,"full_workspace_six_gates_qualified":False})
    print(json.dumps({"phase":"build","exit_code":0,"binary_sha256":artifacts[0]["sha256"],"exact_test_count":189}),flush=True)


def inventory(directory,label):
    commands={"containers":["docker","ps","-a","--format","{{.ID}}\t{{.Names}}\t{{.Status}}\t{{.Image}}"],"volumes":["docker","volume","ls","--format","{{.Name}}\t{{.Driver}}"]}
    result={}
    for name,command in commands.items():
        value=subprocess.run(command,capture_output=True,text=True)
        (directory/(label+"-"+name+".txt")).write_text(value.stdout)
        (directory/(label+"-"+name+".stderr")).write_text(value.stderr)
        assert value.returncode == 0
        result[name]=value.stdout.splitlines()
    old=subprocess.run(["docker","volume","inspect",LEGACY_WAL],capture_output=True,text=True)
    (directory/(label+"-legacy-wal-inspect.stdout")).write_text(old.stdout)
    (directory/(label+"-legacy-wal-inspect.stderr")).write_text(old.stderr)
    assert old.returncode == 0
    result["legacy_volume_inspect"]=json.loads(old.stdout)
    assert len(result["legacy_volume_inspect"]) == 1 and result["legacy_volume_inspect"][0]["Name"] == LEGACY_WAL
    return result


def native():
    authorization=gate()
    build_receipt=read(OUT/"strict-runtime-build-v1/receipt.json")
    assert build_receipt["source_tree_sha256"] == TREE and build_receipt["exact_test_count"] == 189
    artifact=build_receipt["artifacts"][0]
    binary=Path(artifact["path"])
    assert sha(binary.read_bytes()) == artifact["sha256"] and "target" not in binary.parts
    directory=OUT/"mandatory-pitr-v1"
    directory.mkdir(exist_ok=False)
    write(directory/"resource-gate-snapshot.json",authorization)
    source_check(directory,"before")
    rows=[]
    for major,image in IMAGES.items():
        case=directory/("pg"+str(major));case.mkdir()
        disk(case,"before");source_check(case,"before")
        image_raw=subprocess.check_output(["docker","image","inspect",image])
        (case/"image-inspect.json").write_bytes(image_raw)
        info=json.loads(image_raw)[0]
        assert info["Os"] == "linux" and info["Architecture"] == "arm64" and image in info["RepoDigests"]
        before=inventory(case,"before")
        env=environment()
        env["STATEKNOT_REQUIRE_PITR_TESTS"]="1";env["STATEKNOT_PITR_POSTGRES_IMAGE"]=image
        assert "STATEKNOT_TEST_DATABASE_URL" not in env and "RUST_MIN_STACK" not in env
        command=[str(binary),EXACT,"--exact","--nocapture","--test-threads=1"]
        started=time.time()
        with (case/"native.log").open("wb") as log:
            process=subprocess.Popen(command,cwd=SOURCE,env=env,stdout=log,stderr=subprocess.STDOUT)
            write(case/"running.json",{"pid":process.pid,"command":command,"started_unix":started,"source_tree_sha256":TREE,"binary_sha256":artifact["sha256"]})
            code=process.wait()
        raw=(case/"native.log").read_bytes();body=raw.decode(errors="replace")
        summaries=COUNTS.findall(body)
        markers=[json.loads(line.split("=",1)[1]) for line in body.splitlines() if line.startswith("STATEKNOT_CHILD_JOIN_PITR_EVIDENCE=")]
        after=inventory(case,"after")
        before_ids={line.split("\t",2)[0] for line in before["containers"]}
        after_ids={line.split("\t",2)[0] for line in after["containers"]}
        before_volumes={line.split("\t",1)[0] for line in before["volumes"]}
        after_volumes={line.split("\t",1)[0] for line in after["volumes"]}
        retained=before_ids <= after_ids and before_volumes <= after_volumes and before["legacy_volume_inspect"] == after["legacy_volume_inspect"]
        absence=False;identity=None;owned_volumes=[];owned_containers=[]
        valid=code == 0 and summaries == [("ok","1","0","0")] and len(markers) == 1
        if len(markers) == 1:
            marker=markers[0];identity=marker.get("resource_identity")
            if isinstance(identity,str) and re.fullmatch(r"[0-9a-f]{32}",identity) and uuid.UUID(hex=identity).version == 7:
                owned_volumes=["stateknot-pitr-"+kind+"-"+identity for kind in ["wal","base","target-data"]]
                owned_containers=["stateknot-pitr-"+kind+"-"+identity for kind in ["source","target"]]
                names={line.split("\t")[1] for line in after["containers"]}
                original_names={line.split("\t")[1] for line in before["containers"]}
                absence=not(set(owned_volumes)&after_volumes or set(owned_containers)&names or set(owned_volumes)&before_volumes or set(owned_containers)&original_names)
            valid=valid and absence and retained and not(after_ids-before_ids) and not(after_volumes-before_volumes)
            valid=valid and marker.get("profile") == "child-join-named-pitr-v1" and marker.get("invariants") == "passed"
            valid=valid and marker.get("resource_cleanup_verified") is True and marker.get("postgres","").startswith(str(major)+".")
            valid=valid and all(marker.get(key) is True for key in ["base_backup_verified","post_target_write_excluded","exact_durable_snapshot_verified","noninitial_replay_verified","cross_tenant_refusal_verified"])
            valid=valid and marker.get("named_restore_point") == "stateknot_child_join_before_late_write"
            valid=valid and bool(re.fullmatch(r"[0-9a-f]{64}",marker.get("manifest_sha256",""))) and bool(re.fullmatch(r"[0-9A-Fa-f]{24}",marker.get("wal_segment_archived","")))
        source_check(case,"after");disk(case,"after")
        assert sha(binary.read_bytes()) == artifact["sha256"]
        receipt={"source":str(SOURCE),"source_tree_sha256":TREE,"source_files":922,"binary_sha256":artifact["sha256"],"image":image,"image_id":info["Id"],"postgres_major":major,"command":command,"exit_code":code,"seconds":time.time()-started,"summaries":summaries,"markers":markers,"exact_case_and_cleanup_confirmation_passed":bool(valid),"resource_identity":identity,"expected_owned_volumes":owned_volumes,"expected_owned_containers":owned_containers,"owned_named_resources_absent_before_and_after":absence,"all_prior_container_IDs_and_volume_names_preserved":retained,"legacy_wal_volume_inspection_unchanged":before["legacy_volume_inspect"] == after["legacy_volume_inspect"],"extra_container_IDs":sorted(after_ids-before_ids),"extra_volume_names":sorted(after_volumes-before_volumes),"legacy_wal_volume_intentionally_retained":LEGACY_WAL,"only_profile_environment_set":["STATEKNOT_REQUIRE_PITR_TESTS","STATEKNOT_PITR_POSTGRES_IMAGE"],"no_existing_DB_environment_or_access_by_runner":True,"default_stack":True,"original_profile_resources_and_business_assertions":True,"log_sha256":sha(raw),"manual_stop_rm_prune_or_historical_volume_cleanup":False}
        write(case/"receipt.json",receipt);rows.append(receipt)
        print(json.dumps({"phase":"Native","major":major,"exit_code":code,"exact_case_and_cleanup_confirmation_passed":bool(valid),"resource_identity":identity}),flush=True)
    source_check(directory,"after")
    passed=all(row["exact_case_and_cleanup_confirmation_passed"] for row in rows)
    write(directory/"summary.json",{"source_tree_sha256":TREE,"source_files":922,"binary_sha256":artifact["sha256"],"both_exact_cases_and_cleanup_confirmations_passed":passed,"serial_PITR_execution":True,"legacy_volume_preserved":True,"original922_open_cleanup_history_rewritten":False,"full_workspace_six_gates_or_full_Native_qualified":False,"rows":rows})
    return 0 if passed else 1


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase",choices=["plan","build","native"])
    args=parser.parse_args()
    if args.phase == "plan":
        source_check(OUT,"runner-plan")
        print(json.dumps({"source_tree_sha256":TREE,"phase":"plan-only","A2A_gate_authorized":read(OUT/"resource-gate.json")["authorized"],"next_phases":["targeted strict Runtime Clippy","fresh Runtime postgres binary and identical 189 test inventory","serial mandatory pinned PG16.14/17.11 PITR plus UUID cleanup proof"],"compiled_or_Native_started":False},indent=2))
        return 0
    try:
        if args.phase == "build":
            build();return 0
        return native()
    except BaseException as error:
        write(OUT/(args.phase+"-runner-failure.json"),{"phase":args.phase,"error_type":type(error).__name__,"message":str(error),"traceback":traceback.format_exc(),"original_logs_preserved":True,"manual_resource_cleanup_performed":False})
        raise


if __name__ == "__main__":
    sys.exit(main())
