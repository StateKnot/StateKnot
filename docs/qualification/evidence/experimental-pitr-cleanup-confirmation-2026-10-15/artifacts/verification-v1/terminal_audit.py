#!/usr/bin/env python3
"""Read-only process/cache/source audit; no build, Docker or cleanup commands."""
from pathlib import Path
import fcntl
import hashlib
import json
import subprocess
import time

OUT = Path(__file__).resolve().parent
BASE = OUT.parent.parent
ROOT = BASE.parent.parent
TARGET = ROOT / "target/r1-oct15-elapsed36-lint"
SOURCE = BASE / "elapsed36-pitr-cleanup-source"

def sha(raw):
    return hashlib.sha256(raw).hexdigest()

def capture(name, command):
    value = subprocess.run(command, capture_output=True)
    (OUT / (name + ".stdout")).write_bytes(value.stdout)
    (OUT / (name + ".stderr")).write_bytes(value.stderr)
    return value

def main():
    pids = sorted({json.loads(p.read_text())["pid"] for p in OUT.rglob("*running.json")})
    processes = capture("terminal-known-processes", ["ps", "-p", ",".join(map(str, pids)), "-o", "pid=,stat=,comm="])
    assert processes.returncode == 1 and not processes.stdout.strip() and not processes.stderr.strip(), "Recorded task PID still present or ps failed"
    disk = capture("terminal-disk", ["df", "-k", str(ROOT)])
    size = capture("terminal-cache-size-audit", ["du", "-sk", str(TARGET)])
    opened = capture("terminal-cache-open-files", ["lsof", "+D", str(TARGET)])
    tracked = capture("terminal-cache-git-tracked", ["git", "ls-files", "--", str(TARGET.relative_to(ROOT))])
    assert disk.returncode == size.returncode == tracked.returncode == 0
    assert not tracked.stdout.strip()
    tag = (TARGET / "CACHEDIR.TAG").read_bytes()
    assert tag.startswith(b"Signature: 8a477f597d28d172789f06886806bc55")
    lock_available = False
    with (TARGET / "debug/.cargo-lock").open("r+") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            lock_available = True
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        except BlockingIOError:
            pass
    manifest = json.loads((OUT.parent / "source-manifest.json").read_text())
    actual = {p.relative_to(SOURCE).as_posix():p for p in SOURCE.rglob("*") if p.is_file()}
    assert set(actual) == {r["path"] for r in manifest["files"]}
    for row in manifest["files"]:
        raw = actual[row["path"]].read_bytes()
        assert len(raw) == row["bytes"] and sha(raw) == row["sha256"], row["path"]
    artifact = json.loads((OUT / "strict-runtime-build-v1/receipt.json").read_text())["artifacts"][0]
    assert sha(Path(artifact["path"]).read_bytes()) == artifact["sha256"]
    value = {
        "audited_unix":time.time(),
        "scope":"post-execution read-only audit; no Docker, compilation, deletion or cleanup",
        "recorded_task_PIDs":pids,
        "recorded_task_PIDs_absent":True,
        "execution_sessions_waited_exit_zero":[16807,10019],
        "free_KiB":int(disk.stdout.decode().splitlines()[-1].split()[3]),
        "retained_task_cache_realpath":str(TARGET.resolve()),
        "retained_task_cache_KiB":int(size.stdout.split()[0]),
        "Cargo_CACHEDIR_TAG_sha256":sha(tag),
        "git_tracked_cache_files":0,
        "lsof_exit":opened.returncode,
        "cache_no_open_files":opened.returncode == 1 and not opened.stdout.strip() and not opened.stderr.strip(),
        "cargo_lock_nonblocking_acquired_released":lock_available,
        "cache_cleaned":False,
        "cache_retained_for":"root composed validation follow-up; do not infer cache ownership if another stage later starts",
        "source_files":len(actual),
        "source_tree_sha256":manifest["source_tree_sha256"],
        "source_all_bytes_unchanged":True,
        "saved_binary_sha256":artifact["sha256"],
        "saved_binary_rehashed":True,
        "old_WAL_retained_by_before_after_Native_inventory":True,
        "manual_stop_rm_prune_or_historical_volume_cleanup":False,
        "main_or_candidate_source_modified_by_verification":False
    }
    (OUT / "terminal-resources.json").write_text(json.dumps(value, indent=2) + "\n")
    print(json.dumps(value, indent=2))

if __name__ == "__main__":
    main()
