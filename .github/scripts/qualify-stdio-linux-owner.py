#!/usr/bin/python3
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0
"""Independent native witness for the Rust Linux owner and recovery broker."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import select
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time

PYTHON = "/usr/bin/python3"
SYSTEMCTL = "/usr/bin/systemctl"
SYSTEMD_RUN = "/usr/bin/systemd-run"
SETPRIV = "/usr/bin/setpriv"
ENV = {"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C"}
UID = 65534
LEASE_NS = 5_000_000_000
MAX_STREAM = 512 * 1024
MAX_OWNER_BINARY = 32 * 1024 * 1024
LITERAL_ARGUMENT = "literal $HOME; $(false) *"
LITERAL_ENV = "value ${HOME}; no expansion"
PIPE_MARKER = b"stateknot-application-pipe\n"
SUCCESS_FIXTURE_EXIT = 42  # Intentional failed-unit retention, not a suite status.
REJECT_FIXTURE_EXIT = 44
FOUNDATION_CASES = ("P0", "P1", "P2", "P3", "P4", "P5")
RECOVERY_CASES = ("C0", "C1", "C2")
LAUNCH_CASES = ("L0",)
CASES = FOUNDATION_CASES + RECOVERY_CASES + LAUNCH_CASES
RECORD_NAME = "generation.json"
LAUNCH_INTENT_NAME = "launch-intent.json"
MANAGED_RECOVERY_ROOT = Path("/var/lib/stateknot/mcp-stdio")
EVIDENCE_ERROR = b"stateknot-mcp-stdio-owner: owner-evidence-unavailable\n"
PROPERTIES = ("InvocationID", "MainPID", "ExecMainPID", "ExecMainCode",
              "ExecMainStatus", "ExecMainExitTimestampMonotonic", "ControlGroup",
              "PrivatePIDs", "Restart", "WatchdogUSec", "ActiveState", "SubState",
              "Result")
CGROOT = Path("/sys/fs/cgroup")


def require(ok, message):
    if not ok:
        raise ValueError(message)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def save(path, value, private=False):
    raw = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
    save_bytes(path, raw, private)


def save_bytes(path, raw, private=False):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                 0o600 if private else 0o644)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def boot_id():
    return Path("/proc/sys/kernel/random/boot_id").read_text().strip()


def status(raw):
    return dict(line.split(":", 1) for line in raw.splitlines() if ":" in line)


def proc_fact(pid, directory_fd=None):
    def read(name):
        if directory_fd is None:
            return Path("/proc", str(pid), name).read_text()
        fd = os.open(name, os.O_RDONLY | os.O_CLOEXEC, dir_fd=directory_fd)
        with os.fdopen(fd) as stream:
            return stream.read()
    fields = read("stat").rsplit(") ", 1)[1].split()
    row = status(read("status"))
    return {"pid": pid, "state": fields[0], "ppid": int(fields[1]),
            "pgrp": int(fields[2]), "session": int(fields[3]),
            "start_ticks": fields[19],
            "uids": [int(x) for x in row["Uid"].split()],
            "gids": [int(x) for x in row["Gid"].split()],
            "groups": [int(x) for x in row["Groups"].split()],
            "nspid": [int(x) for x in row["NSpid"].split()],
            "caps": {k: int(row[k], 16) for k in
                     ("CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb")},
            "nnp": int(row["NoNewPrivs"])}


def ns_identity(pid, directory_fd=None):
    path = Path("/proc", str(pid), "ns/pid") if directory_fd is None else "ns/pid"
    st = os.stat(path, dir_fd=directory_fd)
    return [st.st_dev, st.st_ino]


class ProcessIdentity:
    """A host observer, not a wait/reaper for a systemd-owned process."""
    def __init__(self, pid, log):
        self.log = log
        self.last_observation = None
        require(pid > 1, "refuse host PID1 or invalid process")
        self.pid = pid
        self.pidfd = os.pidfd_open(pid, 0)
        try:
            self.procfd = os.open("/proc/" + str(pid),
                                  os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
            try:
                self.fact = proc_fact(pid, self.procfd)
                self.namespace = ns_identity(pid, self.procfd)
                require(self.fact["state"] not in ("Z", "X")
                        and not select.select([self.pidfd], [], [], 0)[0],
                        "identity already retired")
                require(proc_fact(pid)["start_ticks"] == self.fact["start_ticks"],
                        "PID changed while identity was captured")
            except BaseException:
                os.close(self.procfd)
                raise
        except BaseException:
            os.close(self.pidfd)
            raise

    def record(self, row):
        body = json.dumps(row, sort_keys=True)
        if body != self.last_observation:
            with self.log.open("a") as stream:
                stream.write(json.dumps({"monotonic_ns": time.monotonic_ns(), **row},
                                        sort_keys=True) + "\n")
            self.last_observation = body

    def exited_and_reaped(self):
        row = {"pid": self.pid, "original_start_ticks": self.fact["start_ticks"],
               "pidfd_exit_ready": bool(select.select([self.pidfd], [], [], 0)[0]),
               "retired": False}
        if not row["pidfd_exit_ready"]:
            self.record(row)
            return False
        try:
            row["pinned_original_proc"] = proc_fact(self.pid, self.procfd)
            self.record(row)  # Includes zombie; exit-ready is not reap.
            return False
        except OSError as exc:
            row["pinned_proc_errno"] = exc.errno
            self.record(row)
            require(exc.errno in (2, 3), "original proc access failed, not absence")
        try:
            row["current_host_proc"] = proc_fact(self.pid)
            row["retired"] = (row["current_host_proc"]["start_ticks"] != self.fact["start_ticks"])
        except OSError as exc:
            row["host_proc_errno"] = exc.errno
            self.record(row)
            require(exc.errno in (2, 3), "host proc access failed, not absence")
            row["retired"] = True
        self.record(row)
        return row["retired"]

    def close(self):
        os.close(self.procfd)
        os.close(self.pidfd)


class Capture:
    """Bounded real pipes; EOF and wait are observed separately."""
    def __init__(self, process, directory, label):
        self.process = process
        self.streams = {}
        self.bytes = {}
        for name in ("stdout", "stderr"):
            pipe = getattr(process, name)
            os.set_blocking(pipe.fileno(), False)
            self.streams[pipe.fileno()] = (pipe, (directory / (label + "." + name)).open("xb"))
            self.bytes[pipe.fileno()] = 0

    def pump(self):
        if not self.streams:
            return
        for fd in select.select(list(self.streams), [], [], 0)[0]:
            data = os.read(fd, 8192)
            pipe, output = self.streams[fd]
            if not data:
                output.close()
                pipe.close()
                del self.streams[fd]
                continue
            self.bytes[fd] += len(data)
            output.write(data)
            output.flush()
            require(self.bytes[fd] <= MAX_STREAM, "bounded fixture stream exceeded")

    def complete(self):
        self.pump()
        return not self.streams and self.process.poll() is not None

    def close(self):
        for pipe, output in self.streams.values():
            output.close()
            pipe.close()
        self.streams.clear()


def command(directory, label, argv, timeout=8, *, owner_binary=None, application_pipes=False):
    owner_modes = ("launch-generation",) if application_pipes else ("record-generation", "recover-generation")
    require(argv[0] in (SYSTEMCTL, SYSTEMD_RUN, SETPRIV, PYTHON)
            or (owner_binary is not None and argv[0] == str(owner_binary)
                and len(argv) == 3 and argv[1] in owner_modes),
            "command outside fixed executable allowlist")
    started = time.monotonic_ns()
    out_path, err_path = directory / (label + ".stdout"), directory / (label + ".stderr")
    error = None
    if application_pipes:
        process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, env=ENV, cwd="/", close_fds=True)
        process.stdin.close()
        capture = Capture(process, directory, label)
        try:
            wait_for(capture.complete, (), timeout, "fixed launcher command timed out")
            code = process.wait(timeout=1)
        except TimeoutError:
            error = "TimeoutExpired"
            code = None
        except (OSError, ValueError) as exc:
            error = type(exc).__name__ + ": " + str(exc)
            code = process.poll()
        finally:
            if process.poll() is None:
                kill_direct_child(process)
            capture.close()
    else:
        with out_path.open("xb") as out, err_path.open("xb") as err:
            process = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=out, stderr=err,
                                       env=ENV, cwd="/", close_fds=True)
            try:
                code = process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                error = "TimeoutExpired"
                kill_direct_child(process)
                code = None
    stdout, stderr = out_path.read_bytes(), err_path.read_bytes()
    save(directory / (label + ".receipt.json"),
         {"argv": argv, "actual_exit_code": code, "error": error,
          "started_monotonic_ns": started, "ended_monotonic_ns": time.monotonic_ns(),
          "stdout_sha256": digest(stdout), "stderr_sha256": digest(stderr)})
    require(len(stdout) <= MAX_STREAM and len(stderr) <= MAX_STREAM,
            "fixed control command output exceeded")
    require(error is None, "fixed control command timed out")
    return subprocess.CompletedProcess(argv, code, stdout, stderr)


def kill_direct_child(process):
    # Popen has not reaped this direct child, so its PID is still reserved.
    pidfd = os.pidfd_open(process.pid)
    try:
        signal.pidfd_send_signal(pidfd, signal.SIGKILL)
    except ProcessLookupError:
        pass
    finally:
        os.close(pidfd)
    process.wait(timeout=2)


def unit_properties(directory, unit, sequence):
    result = command(directory, "show-" + str(sequence),
                     [SYSTEMCTL, "show", unit, "--no-pager",
                      "--property=" + ",".join(PROPERTIES)])
    require(result.returncode == 0, "cannot read exact unit identity")
    return dict(line.split("=", 1) for line in result.stdout.decode().splitlines()
                if "=" in line)


def broker_command(directory, label, owner_binary, mode, path, *, succeeds):
    result = command(directory, label, [str(owner_binary), mode, str(path)], timeout=12,
                     owner_binary=owner_binary, application_pipes=mode == "launch-generation")
    require(result.returncode == (0 if succeeds else 1) and not result.stdout
            and result.stderr == (b"" if succeeds else EVIDENCE_ERROR),
            label + " produced unexpected broker outcome")


def journal_snapshot(path):
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o600
            and info.st_uid == 0 and info.st_nlink == 1 and 0 < info.st_size <= 1024,
            "journal metadata differs from durable-record contract")
    raw = path.read_bytes()
    require(len(raw) == info.st_size, "journal size changed while observing")
    return raw, {"sha256": digest(raw), "bytes": len(raw), "device": info.st_dev,
                 "inode": info.st_ino, "mode": "0600", "uid": info.st_uid,
                 "links": info.st_nlink, "mtime_ns": info.st_mtime_ns,
                 "ctime_ns": info.st_ctime_ns}


def provision_managed_recovery(generation):
    for path in (MANAGED_RECOVERY_ROOT.parent, MANAGED_RECOVERY_ROOT):
        path.mkdir(mode=0o700, exist_ok=True)
        metadata = path.lstat()
        require(stat.S_ISDIR(metadata.st_mode) and metadata.st_uid == 0
                and not metadata.st_mode & 0o022, "untrusted managed recovery root")
    directory = MANAGED_RECOVERY_ROOT / generation
    directory.mkdir(mode=0o700)
    return directory


def require_journal_absent(path):
    try:
        path.lstat()
    except FileNotFoundError:
        return
    raise ValueError("unexpected recovery record")


def cgroup_path(value):
    require(value.startswith("/system.slice/") and ".." not in value.split("/"),
            "unexpected task cgroup location")
    return CGROOT / value.lstrip("/")


def cgroup_members(path):
    members = set()
    for entry in (path, *path.rglob("*")):
        if entry.is_dir():
            members.update(int(x) for x in (entry / "cgroup.procs").read_text().split())
    return sorted(members)


def fixture():
    """Fixed hostile tree, reached only after setpriv's complete drop."""
    me = proc_fact(os.getpid())
    require(me["uids"] == [UID] * 4 and me["gids"] == [UID] * 4
            and me["groups"] == [] and all(v == 0 for v in me["caps"].values())
            and me["nnp"] == 1, "fixture privilege drop failed")
    require("NOTIFY_SOCKET" not in os.environ, "notify authority leaked")
    require(sys.argv[1:] == ["fixture", LITERAL_ARGUMENT], "literal argv was not preserved")
    require(os.environ.get("STATEKNOT_OWNER_FIXTURE") == LITERAL_ENV, "explicit env differs")
    require(sys.stdin.buffer.read(len(PIPE_MARKER) + 1) == PIPE_MARKER,
            "post-drop launch changed application bytes")
    for descriptor in os.listdir("/proc/self/fd"):
        if int(descriptor) > 2:
            try:
                os.fstat(int(descriptor))
            except OSError as exc:
                require(exc.errno == 9, "unexpected FD inspection failure")
            else:
                raise ValueError("control or unexpected FD leaked to fixture")
    for branch in ("setsid", "double-fork"):
        pid = os.fork()
        if pid == 0:
            os.setsid()
            if branch == "double-fork":
                nested = os.fork()
                if nested:
                    os._exit(0)
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            os.write(1, ("leaf:" + branch + "\n").encode())
            os.write(2, ("pipe-holder:" + branch + "\n").encode())
            while True:
                time.sleep(1)
        if branch == "double-fork":
            os.waitpid(pid, 0)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    os.write(1, b"fixture-parent-ready\n")
    while True:
        time.sleep(1)


def launch_fixture(stdin, code):
    """Send an exact launch prelude and independent application bytes."""
    body = json.dumps({
        "executable": list(os.fsencode(PYTHON)),
        "cwd": [ord("/")],
        "arguments": [list(os.fsencode(s)) for s in ("-I", str(code), "fixture", LITERAL_ARGUMENT)],
        "environment": [{"name": list(b"STATEKNOT_OWNER_FIXTURE"),
                         "value": list(os.fsencode(LITERAL_ENV))}],
    }, separators=(",", ":")).encode()
    frame = len(body).to_bytes(4, "big") + body + PIPE_MARKER
    require(len(frame) <= 4096, "fixed launch frame exceeds PIPE_BUF")
    require(stdin.write(frame) == len(frame), "incomplete fixed launch write")
    stdin.flush()


def session_owner(private):
    config = json.loads((private / "config.json").read_bytes())
    with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as connection:
        connection.settimeout(10)
        connection.connect(str(private / "control.sock"))
        line = sys.stdin.buffer.readline()
        require(line in (b"GO\n", b"GO-ONCE\n", b"REJECT\n"), "owner instruction differs")
        generation = "wrong-generation" if line == b"REJECT\n" else config["generation"]
        def send(op):
            connection.sendall(json.dumps({"op": op, "generation": generation}).encode())
        send("go")
        renewing = line == b"GO\n"
        paused = None
        last_renew_sent_ns = None
        if renewing:
            send("renew")
            last_renew_sent_ns = time.monotonic_ns()
        while True:
            ready = select.select([sys.stdin.buffer, connection], [], [], 0.3)[0]
            if connection in ready:
                try:
                    if not connection.recv(2048):
                        if paused is not None:
                            save(private / "owner-paused-peer-eof.json",
                                 {**paused, "peer_eof_monotonic_ns": time.monotonic_ns()}, True)
                        return 0
                except ConnectionResetError:
                    require(paused is None, "paused owner requires clean peer EOF")
                    return 0
            if sys.stdin.buffer in ready:
                instruction = sys.stdin.buffer.readline()
                if instruction == b"STOP\n":
                    send("stop")
                    renewing = False
                elif instruction == b"PAUSE\n" and renewing:
                    renewing = False
                    paused = {"pid": os.getpid(), "socket_fd": connection.fileno(),
                              "socket_inode": os.fstat(connection.fileno()).st_ino,
                              "last_renew_sent_monotonic_ns": last_renew_sent_ns,
                              "pause_monotonic_ns": time.monotonic_ns()}
                    save(private / "owner-renewal-paused.json", paused, True)
                elif not instruction:
                    return 0
                else:
                    raise ValueError("unknown owner instruction")
            if renewing:
                try:
                    send("renew")
                except (BrokenPipeError, ConnectionResetError):
                    # A killed helper can close its peer between select and send.
                    return 0
                last_renew_sent_ns = time.monotonic_ns()


def preflight(output, image_os, image_version):
    require(sys.platform == "linux" and os.geteuid() == 0, "root native Linux required")
    require(image_os == "ubuntu26" and image_version, "explicit Ubuntu26 image metadata required")
    require(Path("/proc/1/comm").read_text().strip() == "systemd", "host PID1 is not systemd")
    require((CGROOT / "cgroup.controllers").is_file(), "cgroup-v2 absent")
    require(hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal"),
            "safe Python pidfd interfaces absent")
    programs = {}
    for executable in (PYTHON, SYSTEMCTL, SYSTEMD_RUN, SETPRIV):
        resolved = Path(executable).resolve(strict=True)
        st = resolved.stat()
        require(stat.S_ISREG(st.st_mode) and st.st_uid == 0
                and not st.st_mode & 0o022, "untrusted fixed executable " + executable)
        programs[executable] = {"resolved": str(resolved), "sha256": digest(resolved.read_bytes())}
    version = command(output, "systemd-version", [SYSTEMCTL, "--version"])
    require(version.returncode == 0, "systemd version command failed")
    major = int(version.stdout.decode().splitlines()[0].split()[1])
    require(major >= 258, "systemd>=258 required")
    space = os.statvfs(output)
    available = space.f_bavail * space.f_frsize
    require(available >= 64 * 1024 * 1024, "raw evidence space below 64 MiB")
    setpriv = command(output, "setpriv-version", [SETPRIV, "--version"])
    require(setpriv.returncode == 0, "setpriv unavailable")
    save(output / "host-profile.json",
         {"boot_id": boot_id(), "uname": list(os.uname()),
          "systemd_version": version.stdout.decode(), "image_os": image_os,
          "image_version": image_version, "python": sys.version,
          "host_namespace": ns_identity(os.getpid()), "programs": programs,
          "evidence_space_available_bytes": available,
          "profile_scope": "this exact native host tuple, not all Ubuntu26 builds"})


def wait_for(predicate, captures, seconds, message):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        for capture in captures:
            capture.pump()
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    raise TimeoutError(message)


def observe_paused_owner(private, owner, identity, captures):
    def read_pause():
        try:
            return json.loads((private / "owner-renewal-paused.json").read_bytes())
        except (FileNotFoundError, json.JSONDecodeError):
            return False

    pause = wait_for(read_pause, captures, 3, "owner did not stop renewing with its socket open")
    require(pause["pid"] == identity.pid and owner.poll() is None, "paused owner not original/live")
    fd, inode = pause["socket_fd"], pause["socket_inode"]
    require(type(fd) is int and fd > 2 and type(inode) is int and inode > 0,
            "invalid paused owner socket identity")
    link = os.readlink("fd/" + str(fd), dir_fd=identity.procfd)
    require(link == f"socket:[{inode}]", "paused owner no longer holds the original socket")
    return {"owner_pause": pause, "actual_proc_socket": link,
            "observed_monotonic_ns": time.monotonic_ns()}


def validate_lease_expiry(observation, peer_eof, terminal):
    pause = observation["owner_pause"]
    deadline = terminal["lease_deadline_monotonic_ns"]
    require(terminal["reason"] == "owner-lease-expired" and terminal["target_spawns"] == 1
            and terminal["accepted_renewals"] > 0
            and terminal["last_renewal_monotonic_ns"] + LEASE_NS == deadline,
            "helper did not expire the absolute owner lease")
    require(peer_eof == {**pause, "peer_eof_monotonic_ns": peer_eof["peer_eof_monotonic_ns"]}
            and 0 < pause["last_renew_sent_monotonic_ns"] <= pause["pause_monotonic_ns"]
            <= observation["observed_monotonic_ns"] < deadline <= terminal["terminal_monotonic_ns"]
            <= peer_eof["peer_eof_monotonic_ns"],
            "same held owner socket did not observe helper EOF after lease expiry")


def validate_manager_exit(final, invocation, pid, case, fault_ns):
    # ExecMainCode uses Linux si_code (CLD_EXITED=1, CLD_KILLED=2).
    # systemd-run propagates exit-code status, returns 1 for watchdog and 255 for signal.
    require(case in CASES, "unknown native owner case")
    if case == "P4":
        result, code, status, launcher = "watchdog", 2, signal.SIGKILL, 1
    elif case in ("P5", "C2"):
        result, code, status, launcher = "signal", 2, signal.SIGKILL, 255
    else:
        if case in ("C0", "C1"):
            status = 1
        else:
            status = REJECT_FIXTURE_EXIT if case == "P0" else SUCCESS_FIXTURE_EXIT
        result, code, launcher = "exit-code", 1, status
    require(final["InvocationID"] == invocation and int(final["ExecMainPID"]) == pid
            and int(final["ExecMainCode"]) == code and int(final["ExecMainStatus"]) == status
            and final["Result"] == result
            and int(final["ExecMainExitTimestampMonotonic"]) >= fault_ns // 1000,
            "same-generation manager exit or retirement cause differs")
    return launcher


def run_case(output, work, recovery_root, code, owner_binary, case, sequence):
    directory = output / case
    directory.mkdir(mode=0o755)
    private = work / case
    private.mkdir(mode=0o700)
    stale = None
    if case == "C1":
        # Reuse the retired P5 unit name with a new systemd invocation. This
        # gives the replacement a real journal from the same boot and unit.
        raw, _ = journal_snapshot(recovery_root / "P5" / RECORD_NAME)
        stale = json.loads(raw)
        generation = stale["generation"]
    else:
        generation = os.urandom(16).hex()
    if case == "L0":
        recovery_dir = provision_managed_recovery(generation)
    else:
        recovery_dir = recovery_root / case
        recovery_dir.mkdir(mode=0o700)
    journal = recovery_dir / RECORD_NAME
    if stale is not None:
        save_bytes(journal, raw, True)
    unit = "stateknot-owner-" + generation + ".service"
    config = {"generation": generation, "host_namespace": ns_identity(os.getpid()),
              "boot_id": boot_id(), "target_uid": UID, "target_gid": UID,
              "recovery_directory": str(recovery_dir)}
    save(private / "config.json", config, True)
    save(directory / "intent.json", {"unit": unit, "boot_id": boot_id(), **config})
    identities = []
    captures = []
    expected_invocation = None
    owner = service = owner_identity = leader = service_pidfd = None
    leader_verified = False
    started = time.monotonic_ns()
    answer = {"case": case, "passed": False, "fallback_used": False, "unit": unit}
    try:
        if stale is not None:
            show_sequence = 0

            def prior_unit_unloaded():
                nonlocal show_sequence
                show_sequence += 1
                result = command(directory, "show-prior-unit-" + str(show_sequence),
                                 [SYSTEMCTL, "show", unit, "--no-pager", "--value",
                                  "--property=LoadState"])
                return result.stdout == b"not-found\n"

            wait_for(prior_unit_unloaded, captures, 3, "retired prior invocation still loaded")
        if case == "L0":
            require_journal_absent(recovery_dir / LAUNCH_INTENT_NAME)
            require_journal_absent(journal)
            argv = [str(owner_binary), "launch-generation", str(private)]
        else:
            argv = [SYSTEMD_RUN, "--quiet", "--wait", "--pipe", "--unit=" + unit]
            for prop in ("Type=notify", "NotifyAccess=main", "PrivatePIDs=yes",
                         "Restart=no", "CollectMode=inactive", "KillMode=control-group",
                         "SendSIGKILL=yes", "WatchdogSec=5s", "WatchdogSignal=SIGKILL",
                         "TimeoutStartSec=15s", "TimeoutStopSec=15s", "RuntimeMaxSec=35s",
                         "TasksMax=16", "MemoryMax=128M", "CPUQuota=100%",
                         "NoNewPrivileges=yes", "RestrictNamespaces=yes",
                         "ProtectControlGroups=yes", "ProtectKernelTunables=yes",
                         "ProtectKernelModules=yes", "ProtectKernelLogs=yes",
                         "CapabilityBoundingSet=CAP_SETUID CAP_SETGID CAP_SETPCAP",
                         "AmbientCapabilities="):
                argv.extend(["--property=" + prop])
            argv.extend(["--", str(owner_binary), "namespace-init", str(private)])
        save(directory / "service-start.json", {"argv": argv})
        service = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, env=ENV, cwd="/", close_fds=True)
        service_pidfd = os.pidfd_open(service.pid)
        service_capture = Capture(service, directory, "service")
        captures.append(service_capture)
        wait_for(lambda: (private / "helper-ready.json").is_file(), captures, 12,
                 "trusted helper did not reach before-GO ready")
        if case == "L0":
            wait_for(lambda: (private / "broker-ready.json").is_file(), captures, 12,
                     "host launcher did not publish before-GO readiness")
        props = unit_properties(directory, unit, sequence)
        if (props["ActiveState"], props["SubState"]) != ("active", "running"):
            ready_sequence = 0

            def manager_ready():
                nonlocal ready_sequence
                ready_sequence += 1
                current = unit_properties(directory, unit, f"{sequence}-ready-{ready_sequence}")
                return current if (current["ActiveState"], current["SubState"]) == ("active", "running") else None

            props = wait_for(manager_ready, captures, 3, "manager did not acknowledge owner readiness")
        expected_invocation = props["InvocationID"]
        require(len(expected_invocation) == 32 and props["PrivatePIDs"] == "yes"
                and props["Restart"] == "no" and props["ActiveState"] == "active"
                and props["SubState"] == "running", "unit profile mismatch")
        leader = ProcessIdentity(int(props["MainPID"]), directory / "proc-observations.jsonl")
        identities.append(leader)
        require(os.path.samestat(os.stat("exe", dir_fd=leader.procfd), owner_binary.stat()),
                "manager MainPID does not own the built Rust executable")
        require(leader.namespace != config["host_namespace"]
                and leader.fact["nspid"][-1] == 1
                and leader.fact["uids"] == [0] * 4 and leader.fact["ppid"] == 1,
                "before-GO PID1/actual manager-parent identity mismatch")
        require(props["ControlGroup"] == "/system.slice/" + unit, "unexpected unit cgroup")
        fd = os.open("cgroup", os.O_RDONLY | os.O_CLOEXEC, dir_fd=leader.procfd)
        with os.fdopen(fd) as stream:
            require(stream.read() == "0::" + props["ControlGroup"] + "\n",
                    "pinned init does not belong to the exact unit cgroup")
        leader_verified = True
        cg = cgroup_path(props["ControlGroup"])
        cg_identity = [cg.stat().st_dev, cg.stat().st_ino]
        wait_for(lambda: cgroup_members(cg) == [leader.pid], captures, 3,
                 "unexpected before-GO process or launcher still present")
        ready = json.loads((private / "helper-ready.json").read_bytes())
        require(ready["generation"] == generation and ready["target_spawns"] == 0
                and ready["namespace"] == leader.namespace, "before-GO helper binding differs")
        save(directory / "identity.json",
             {"unit": unit, "invocation_id": expected_invocation,
              "boot_id": boot_id(), "control_group": props["ControlGroup"],
              "init": leader.fact, "namespace": leader.namespace,
              "profile": props, "cgroup_identity": cg_identity,
              "target_spawns_before_go": 0})
        expected_record = {"version": 1, "boot_id": config["boot_id"],
                           "generation": generation, "unit": unit,
                           "invocation_id": expected_invocation, "main_pid": leader.pid,
                           "main_start_ticks": int(leader.fact["start_ticks"]),
                           "pid_namespace": leader.namespace, "cgroup_identity": cg_identity}
        journal_before = None
        launch_intent = None
        if case == "L0":
            launch_intent = journal_snapshot(recovery_dir / LAUNCH_INTENT_NAME)
            require(json.loads(launch_intent[0]) == {
                "version": 1, "boot_id": config["boot_id"], "generation": generation,
                "host_namespace": config["host_namespace"], "control_directory": str(private),
                "target_uid": UID, "target_gid": UID,
            }, "launcher intent does not bind exact inputs")
            broker_ready = (private / "broker-ready.json").read_bytes()
            require(json.loads(broker_ready) == {"generation": generation,
                                                 "invocation_id": expected_invocation},
                    "broker readiness does not bind the verified invocation")
            journal_before = journal_snapshot(journal)
            require(json.loads(journal_before[0]) == expected_record,
                    "launcher journal does not bind exact native generation")
            save_bytes(directory / LAUNCH_INTENT_NAME, launch_intent[0])
            save(directory / "launch-intent-metadata.json", launch_intent[1])
            save_bytes(directory / "broker-ready.json", broker_ready)
        elif case == "C0":
            require_journal_absent(journal)
        elif stale is not None:
            require(stale["boot_id"] == config["boot_id"] and stale["unit"] == unit
                    and stale["invocation_id"] != expected_invocation,
                    "stale record does not identify a prior invocation of the live unit")
            journal_before = journal_snapshot(journal)
            require(journal_before[0] == raw, "staged prior record differs from P5 journal")
            broker_command(directory, "record-generation-conflict", owner_binary,
                           "record-generation", private, succeeds=False)
        else:
            require_journal_absent(journal)
            broker_command(directory, "record-generation", owner_binary,
                           "record-generation", private, succeeds=True)
            journal_before = journal_snapshot(journal)
            require(json.loads(journal_before[0]) == expected_record,
                    "broker journal does not bind exact native generation")
        if journal_before is not None:
            save_bytes(directory / RECORD_NAME, journal_before[0])
            save(directory / "journal-metadata.json", journal_before[1])
        if case in ("C0", "C1"):
            broker_command(directory, "recover-generation-rejected", owner_binary,
                           "recover-generation", recovery_dir, succeeds=False)
            live = unit_properties(directory, unit, sequence + 3)
            current = proc_fact(leader.pid, leader.procfd)
            require(live == props and not select.select([leader.pidfd], [], [], 0)[0]
                    and current["start_ticks"] == leader.fact["start_ticks"]
                    and current["state"] not in ("Z", "X")
                    and cgroup_members(cg) == [leader.pid],
                    "rejected recovery changed the live generation")
            save(directory / "rejected-recovery-live.json",
                 {"profile": live, "original_init": current, "target_spawns": 0})
        owner = subprocess.Popen([PYTHON, "-I", str(code), "owner", str(private)],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, env=ENV, cwd="/", close_fds=True)
        owner_capture = Capture(owner, directory, "owner")
        captures.append(owner_capture)
        owner_identity = ProcessIdentity(owner.pid, directory / "owner-proc-observations.jsonl")
        save(directory / "session-owner-identity.json",
             {"fact": owner_identity.fact, "namespace": owner_identity.namespace})
        if case in ("C0", "C1"):
            # Supply a valid launch frame before GO. A bypass could spawn
            # the fixture, rather than failing only because input is absent.
            launch_fixture(service.stdin, code)
            service.stdin.close()
        fault_ns = time.monotonic_ns()  # Rejection cases are triggered by attempted GO.
        owner.stdin.write(b"REJECT\n" if case == "P0" else
                          b"GO-ONCE\n" if case in ("C0", "C1") else b"GO\n")
        owner.stdin.flush()
        if case not in ("P0", "C0", "C1"):
            launch_fixture(service.stdin, code)
            service.stdin.close()
            def topology():
                if not (private / "target-started.json").is_file():
                    return False
                members = cgroup_members(cg)
                if len(members) != 4:
                    return False
                facts = [proc_fact(pid) for pid in members]
                target = [row for row in facts if row["pid"] != leader.pid]
                if any(row["state"] in ("Z", "X") for row in facts):
                    return False
                if not all(row["uids"] == [UID] * 4 and row["gids"] == [UID] * 4
                           and row["groups"] == [] and row["nnp"] == 1
                           and all(value == 0 for value in row["caps"].values())
                           and ns_identity(row["pid"]) == leader.namespace for row in target):
                    return False
                if len({row["session"] for row in target}) != 3:
                    return False
                initial = json.loads((private / "target-started.json").read_bytes())
                parent = [row for row in target if row["nspid"][-1] == initial["child_namespace_pid"]]
                if len(parent) != 1:
                    return False
                if sorted(row["ppid"] for row in target) != sorted([leader.pid, leader.pid, parent[0]["pid"]]):
                    return False
                # The observed fork tree can precede its TERM-ignore and
                # pipe writes. Wait for the exact completed readiness lines.
                stdout = (directory / "service.stdout").read_bytes().split(b"\n")[:-1]
                stderr = (directory / "service.stderr").read_bytes().split(b"\n")[:-1]
                if any(stdout.count(marker) != 1 for marker in
                       (b"fixture-parent-ready", b"leaf:setsid", b"leaf:double-fork")):
                    return False
                if any(stderr.count(marker) != 1 for marker in
                       (b"pipe-holder:setsid", b"pipe-holder:double-fork")):
                    return False
                return facts
            facts = wait_for(topology, captures, 10, "live fork tree, privilege drop or pipe readiness absent")
            for row in facts:
                if row["pid"] != leader.pid:
                    identity = ProcessIdentity(row["pid"], directory / "proc-observations.jsonl")
                    require(identity.fact["start_ticks"] == row["start_ticks"],
                            "target identity changed during capture")
                    identities.append(identity)
            save(directory / "actual-hostile-tree.json", {"all_members": facts})
            fault_ns = time.monotonic_ns()
        if case == "L0":
            duplicate = work / "L0-duplicate"
            duplicate.mkdir(mode=0o700)
            save(duplicate / "config.json", config, True)
            broker_command(directory, "launch-generation-conflict", owner_binary,
                           "launch-generation", duplicate, succeeds=False)
            for name in ("helper-ready.json", "broker-ready.json", "control.sock",
                         "target-started.json"):
                require_journal_absent(duplicate / name)
            live = unit_properties(directory, unit, sequence + 3)
            current = [proc_fact(identity.pid, identity.procfd) for identity in identities]
            require(live == props and owner.poll() is None and service.poll() is None
                    and cgroup_members(cg) == sorted(identity.pid for identity in identities)
                    and all(fact["start_ticks"] == identity.fact["start_ticks"]
                            and fact["state"] not in ("Z", "X", "T", "t")
                            and not select.select([identity.pidfd], [], [], 0)[0]
                            for identity, fact in zip(identities, current)),
                    "conflicting launcher disturbed the original invocation or target tree")
            save(directory / "rejected-launch-live.json",
                 {"profile": live, "original_members": current, "owner_pid": owner_identity.pid})
            fault_ns = time.monotonic_ns()
        pause_observation = None
        if case in ("P2", "P4", "P5"):
            target = owner_identity if case == "P2" else leader
            injected_signal = signal.SIGSTOP if case == "P4" else signal.SIGKILL
            signal.pidfd_send_signal(target.pidfd, injected_signal)
            save(directory / "fault-sent.json",
                 {"signal": injected_signal.name, "target": target.fact,
                  "monotonic_ns": fault_ns, "send_success_is_not_completion": True})
            if case == "P4":
                def stopped_init():
                    fact = proc_fact(leader.pid, leader.procfd)
                    return fact if fact["state"] == "T" else False

                stopped = wait_for(stopped_init, captures, 3, "original namespace init did not stop")
                save(directory / "fault-observed.json",
                     {"observed_monotonic_ns": time.monotonic_ns(), "stopped_init": stopped})
        elif case in ("P1", "L0"):
            owner.stdin.write(b"STOP\n")
            owner.stdin.flush()
        elif case == "P3":
            owner.stdin.write(b"PAUSE\n")
            owner.stdin.flush()
            pause_observation = observe_paused_owner(private, owner, owner_identity, captures)
            save(directory / "owner-paused.json", pause_observation)
        elif case == "C2":
            require(owner.poll() is None and service.poll() is None
                    and all(not select.select([identity.pidfd], [], [], 0)[0]
                            for identity in identities),
                    "original owner, init or target tree already exiting before broker recovery")
            fault_ns = time.monotonic_ns()
            broker_command(directory, "recover-generation-live", owner_binary,
                           "recover-generation", recovery_dir, succeeds=True)
            save(directory / "recovery-trigger.json",
                 {"monotonic_ns": fault_ns, "owner_pid": owner_identity.pid,
                  "original_init_and_targets": [identity.fact for identity in identities]})
        if case in ("P0", "P1", "P2"):
            owner.stdin.close()
        wait_for(lambda: owner.poll() is not None, captures, 10, "session-owner did not exit")
        owner_code = owner.wait(timeout=1)
        require(owner_code == (-signal.SIGKILL if case == "P2" else 0),
                "unexpected actual owner exit")
        if not owner.stdin.closed:
            owner.stdin.close()
        owner_identity.close()
        owner_identity = None
        def retired():
            service_capture.pump()
            return all(identity.exited_and_reaped() for identity in identities)
        wait_for(retired, captures, 15, "original init/tree remains live or zombie")
        final = unit_properties(directory, unit, sequence + 1)
        expected_exit = validate_manager_exit(final, expected_invocation, leader.pid, case, fault_ns)
        try:
            require([cg.stat().st_dev, cg.stat().st_ino] == cg_identity,
                    "original cgroup identity replaced")
            empty = (not cgroup_members(cg)
                     and "populated 0" in (cg / "cgroup.events").read_text().splitlines())
            removed = False
        except FileNotFoundError:
            # A member or events file can disappear without the original
            # cgroup being removed. Confirm absence of the cgroup itself.
            try:
                cg.lstat()
            except FileNotFoundError:
                empty, removed = True, True
            else:
                raise
        require(empty, "cgroup still populated")
        wait_for(lambda: all(capture.complete() for capture in captures), captures, 5,
                 "I/O EOF or actual helper process join missing")
        service_code = service.wait(timeout=1)
        require(service_code == expected_exit, "service launcher did not preserve actual exit")
        terminal_path = private / "helper-terminal.json"
        terminal = None
        if case in ("P4", "P5", "C2"):
            require(not terminal_path.exists(), "forced init death claimed a cooperative exit")
        else:
            terminal = json.loads(terminal_path.read_bytes())
            if case == "P3":
                peer_eof = json.loads((private / "owner-paused-peer-eof.json").read_bytes())
                validate_lease_expiry(pause_observation, peer_eof, terminal)
                save(directory / "owner-paused-peer-eof.json", peer_eof)
            else:
                expected_reason = {"P0": "generation-rejected", "P1": "owner-stop", "P2": "owner-eof",
                                   "C0": "owner-evidence-unavailable",
                                   "C1": "owner-evidence-unavailable", "L0": "owner-stop"}[case]
                rejected = case in ("P0", "C0", "C1")
                require(terminal["reason"] == expected_reason
                        and terminal["target_spawns"] == (0 if rejected else 1),
                        "actual control cause differs")
                if case in ("C0", "C1"):
                    require(not (private / "target-started.json").exists()
                            and not (directory / "service.stdout").read_bytes(),
                            "a rejected GO started or produced output from the fixture")
        if case == "C2":
            broker_command(directory, "recover-generation-replay", owner_binary,
                           "recover-generation", recovery_dir, succeeds=True)
            replay = unit_properties(directory, unit, sequence + 4)
            validate_manager_exit(replay, expected_invocation, leader.pid, case, fault_ns)
            require(replay["ExecMainExitTimestampMonotonic"] == final["ExecMainExitTimestampMonotonic"]
                    and int(replay["MainPID"]) == 0
                    and all(identity.exited_and_reaped() for identity in identities),
                    "recovery replay changed the original manager retirement")
        if journal_before is None:
            require_journal_absent(journal)
        else:
            require(journal_snapshot(journal) == journal_before,
                    "generation journal changed after publication, conflict or replay")
        if launch_intent is not None:
            require(journal_snapshot(recovery_dir / LAUNCH_INTENT_NAME) == launch_intent,
                    "launch intent changed after the duplicate launch or STOP")
        require(boot_id() == json.loads((directory / "intent.json").read_bytes())["boot_id"],
                "boot identity changed")
        answer.update({"passed": True, "retired": True, "invocation_id": expected_invocation,
                       "fault_monotonic_ns": fault_ns, "actual_owner_wait_status": owner_code,
                       "manager_exit": final,
                       "helper_terminal": terminal, "all_original_identities_disappeared": True,
                       "cgroup_live_empty": True, "cgroup_removed": removed, "io_eof_and_join": True,
                       "controller_waited_init": False, "init_reaper": "systemd",
                       "witness": "original proc identity + pidfd + manager + namespace + cgroup + I/O",
                       "fixture_exit_is_intentional_failed_unit_retention": True})
        if case == "L0":
            answer.update(actual_launch_generation_wait_status=service_code, duplicate_launch_rejected=True)
        else:
            answer["actual_systemd_run_wait_status"] = service_code
    except Exception as exc:
        answer["error"] = type(exc).__name__ + ": " + str(exc)
    finally:
        if not answer["passed"]:
            answer["fallback_used"] = True
            try:
                require(leader_verified, "original init not verified and pinned: refuse fallback signal")
                if not select.select([leader.pidfd], [], [], 0)[0]:
                    signal.pidfd_send_signal(leader.pidfd, signal.SIGKILL)
                    save(directory / "fallback-init-signal.json",
                         {"signal": "SIGKILL", "target": leader.fact,
                          "monotonic_ns": time.monotonic_ns()})
            except ProcessLookupError:
                pass  # The pinned original already exited.
            except Exception as exc:
                answer["fallback_error"] = type(exc).__name__ + ": " + str(exc)
        if owner is not None and not answer["passed"]:
            owner.stdin.close()
            try:
                if owner.poll() is None and owner_identity is not None:
                    try:
                        signal.pidfd_send_signal(owner_identity.pidfd, signal.SIGKILL)
                        answer["fallback_owner_signal"] = "SIGKILL"
                    except ProcessLookupError:
                        pass
                answer["fallback_owner_wait_status"] = owner.wait(timeout=2)
            except Exception as exc:
                answer["fallback_owner_error"] = type(exc).__name__ + ": " + str(exc)
        if owner_identity is not None:
            owner_identity.close()
        if service is not None and not answer["passed"]:
            try:
                wait_for(lambda: service_capture.complete(), captures, 5,
                         "fallback I/O or systemd-run join incomplete")
                answer["fallback_systemd_run_wait_status"] = service.wait(timeout=1)
            except Exception as exc:
                answer["fallback_join_error"] = type(exc).__name__ + ": " + str(exc)
                if service.poll() is None and service_pidfd is not None:
                    try:
                        signal.pidfd_send_signal(service_pidfd, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    service.wait(timeout=2)
        if answer["passed"]:
            cleanup = command(directory, "reset-failed", [SYSTEMCTL, "reset-failed", unit])
            answer["reset_failed_actual_exit_code"] = cleanup.returncode
            if cleanup.returncode != 0:
                answer["passed"] = False
                answer["cleanup_error"] = "reset-failed failed after strict retirement"
            elif case == "L0":
                try:
                    shutil.rmtree(recovery_dir)
                except OSError as exc:
                    answer["passed"] = False
                    answer["cleanup_error"] = type(exc).__name__ + ": " + str(exc)
        for capture in captures:
            capture.close()
        if service is not None:
            service.stdin.close()
        if service_pidfd is not None:
            os.close(service_pidfd)
        for identity in identities:
            identity.close()
        answer["ended_monotonic_ns"] = time.monotonic_ns()
        answer["started_monotonic_ns"] = started
        save(directory / "case-result.json", answer)
    return answer


def controller(args):
    os.umask(0o022)
    output = Path(args.output).resolve(strict=True)
    require(output.is_dir() and not Path(args.output).is_symlink(), "invalid evidence directory")
    result = {"schema": "stateknot-native-owner-foundation-v3",
              "implemented_cases": list(CASES), "unimplemented_cases": [],
              "full_six_case_qualification": False, "broker_recovery_controls_qualified": False,
              "host_launcher_qualified": False, "stdio_supported": False,
              "R1_closed": False, "actual_cases": []}
    work = recovery_root = None
    try:
        source_binding = json.loads(args.source_binding)
        source_bytes = Path(__file__).read_bytes()
        source_sha256 = digest(source_bytes)
        require(source_binding["files_sha256"][".github/scripts/qualify-stdio-linux-owner.py"]
                == source_sha256, "executed probe differs from verified checkout")
        with Path(args.owner_binary).open("rb") as executable:
            owner_bytes = executable.read(MAX_OWNER_BINARY + 1)
        require(0 < len(owner_bytes) <= MAX_OWNER_BINARY and owner_bytes.startswith(b"\x7fELF")
                and digest(owner_bytes) == source_binding["owner_binary_sha256"],
                "compiled owner differs from the bound Linux executable")
        preflight(output, args.image_os, args.image_version)
        work = Path(tempfile.mkdtemp(prefix="stateknot-owner-", dir="/run"))
        recovery_root = Path(tempfile.mkdtemp(prefix="stateknot-owner-recovery-", dir="/var/lib"))
        os.chmod(work, 0o755)
        public = work / "code"
        public.mkdir(mode=0o755)
        code = public / "probe.py"
        code.write_bytes(source_bytes)
        os.chmod(code, 0o644)
        owner_binary = public / "stateknot-mcp-stdio-owner"
        owner_binary.write_bytes(owner_bytes)
        os.chmod(owner_binary, 0o755)
        require(code.stat().st_uid == owner_binary.stat().st_uid == 0,
                "fixture and owner copies must be root-owned")
        save(output / "probe-source.json",
             {"sha256": digest(code.read_bytes()), "run_id": args.run_id,
              "run_attempt": args.attempt, "source_binding": source_binding,
              "owner_binary_sha256": digest(owner_bytes),
              "public_code_path": str(code), "private_case_dirs_mode": "0700",
              "recovery_root": str(recovery_root), "recovery_case_dirs_mode": "0700",
              "managed_recovery_root": str(MANAGED_RECOVERY_ROOT)})
        for index, case in enumerate(CASES):
            row = run_case(output, work, recovery_root, code, owner_binary, case, 10 * index)
            result["actual_cases"].append(row)
            if not row["passed"]:
                break
    except Exception as exc:
        result["error"] = type(exc).__name__ + ": " + str(exc)
    actual = result["actual_cases"]
    recovery_start = len(FOUNDATION_CASES)
    launch_start = recovery_start + len(RECOVERY_CASES)
    foundation = actual[:recovery_start]
    controls = actual[recovery_start:launch_start]
    launch = actual[launch_start:]
    result["foundation_P0_P2_passed"] = (len(actual) >= 3
                                          and all(row["passed"] for row in actual[:3]))
    result["full_six_case_qualification"] = (tuple(row["case"] for row in foundation) == FOUNDATION_CASES
                                             and all(row["passed"] and not row["fallback_used"] for row in foundation))
    result["broker_recovery_controls_qualified"] = (tuple(row["case"] for row in controls) == RECOVERY_CASES
                                                   and all(row["passed"] and not row["fallback_used"] for row in controls))
    result["host_launcher_qualified"] = (tuple(row["case"] for row in launch) == LAUNCH_CASES
                                        and all(row["passed"] and not row["fallback_used"]
                                                and row["duplicate_launch_rejected"] for row in launch))
    passed = (result["full_six_case_qualification"] and result["broker_recovery_controls_qualified"]
              and result["host_launcher_qualified"])
    result["pending_identity_root"] = str(work) if work is not None and not passed else None
    result["pending_recovery_root"] = str(recovery_root) if recovery_root is not None and not passed else None
    save(output / "qualification.json", result)
    if passed:
        shutil.rmtree(work)
        shutil.rmtree(recovery_root)
    entries = [{"path": str(p.relative_to(output)), "bytes": p.stat().st_size,
                "sha256": digest(p.read_bytes())}
               for p in sorted(output.rglob("*")) if p.is_file()
               and not p.name.startswith("launcher.")]
    save(output / "manifest.json", {"files": entries, "full_profile_qualified": False,
                                    "full_six_case_qualification": result["full_six_case_qualification"],
                                    "broker_recovery_controls_qualified": result["broker_recovery_controls_qualified"],
                                    "host_launcher_qualified": result["host_launcher_qualified"]})
    return 0 if passed else 1


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode == "fixture":
        fixture()
    elif mode == "owner" and len(sys.argv) == 3:
        return session_owner(Path(sys.argv[2]))
    elif mode == "controller":
        parser = argparse.ArgumentParser()
        for name in ("output", "run-id", "attempt", "source-binding", "owner-binary",
                     "image-os", "image-version"):
            parser.add_argument("--" + name, required=True)
        return controller(parser.parse_args(sys.argv[2:]))
    else:
        raise ValueError("fixed probe mode required")


if __name__ == "__main__":
    raise SystemExit(main())
