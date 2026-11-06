// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Host-side authority for an exact systemd generation.

use std::{
    collections::BTreeMap,
    env,
    fs::{self, File},
    io::{ErrorKind, Read},
    os::fd::{AsFd, OwnedFd},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::Duration,
};

use nix::poll::{PollFd, PollFlags, PollTimeout, poll};
use rustix::{
    fs::{AtFlags, Mode, OFlags, fcntl_getfl, fcntl_setfl, open, openat, statat},
    io::Errno,
    process::{self, Pid, PidfdFlags, Signal, pidfd_open, pidfd_send_signal},
};

use super::{
    Fault, Result, check_startup, config_from_directory, current_namespace, monotonic_now,
    poll_input, read_bounded, recovery, trusted_executable,
};

const SYSTEMCTL: &str = "/usr/bin/systemctl";
const CGROUP_ROOT: &str = "/sys/fs/cgroup";
const SNAPSHOT_TIME: Duration = Duration::from_secs(2);
const COMMAND_LIMIT: usize = 8192;
const PROPERTIES: &[&str] = &[
    "Id",
    "LoadState",
    "ActiveState",
    "SubState",
    "Type",
    "NotifyAccess",
    "PrivatePIDs",
    "Restart",
    "KillMode",
    "SendSIGKILL",
    "WatchdogUSec",
    "WatchdogSignal",
    "CollectMode",
    "TimeoutStartUSec",
    "TimeoutStopUSec",
    "RuntimeMaxUSec",
    "ProtectControlGroups",
    "NoNewPrivileges",
    "Delegate",
    "MainPID",
    "ExecMainPID",
    "InvocationID",
    "ControlGroup",
    "ExecMainCode",
    "ExecMainExitTimestampMonotonic",
];

pub(super) fn record_generation(control: &Path) -> Result<()> {
    let end = monotonic_now()? + Duration::from_secs(6);
    let host_namespace = verify_host()?;
    let config = config_from_directory(control)?;
    let boot_id = boot_id()?;
    if boot_id != config.boot_id || host_namespace != config.host_namespace {
        return Err(Fault::Profile);
    }
    let unit = generation_unit(&config.generation);
    let snapshot = Manager::load(&unit, end)?;
    let pid = snapshot.live_pid()?;
    let init = PinnedInit::open(pid, &unit, host_namespace)?;
    init.require_current_executable()?;
    let invocation_id = snapshot.value("InvocationID")?;
    if snapshot != Manager::load(&unit, end)? || init.exited()? {
        return Err(Fault::Evidence);
    }
    check_startup(end)?;
    recovery::persist(
        &config.recovery_directory,
        recovery::Observation {
            boot_id: &boot_id,
            generation: &config.generation,
            unit: &unit,
            invocation_id,
            main_pid: pid,
            main_start_ticks: init.start_ticks,
            pid_namespace: init.namespace,
            cgroup_identity: init.cgroup_identity,
        },
    )
    .map_err(|_| Fault::Evidence)?;
    Ok(())
}

pub(super) fn recover_generation(directory: &Path) -> Result<()> {
    let end = monotonic_now()? + Duration::from_secs(10);
    let host_namespace = verify_host()?;
    let record = recovery::recover(directory).map_err(|_| Fault::Evidence)?;
    let identity = record.identity();
    if boot_id()? != identity.boot_id {
        return Err(Fault::Evidence);
    }
    loop {
        check_startup(end)?;
        let snapshot = Manager::load(identity.unit, end)?;
        snapshot.require_invocation(&identity)?;
        if snapshot.value("ActiveState")? == "active" {
            stop_pinned(&snapshot, &identity, host_namespace, end)?;
        }
        // The manager, rather than this broker, reaps its namespace init.
        if snapshot.retired(&identity)?
            && original_proc_gone(&identity)?
            && cgroup_retired(&identity)?
            && snapshot == Manager::load(identity.unit, end)?
        {
            // Retain the immutable record. A replay must prove the same
            // manager exit again and cannot authorize another generation.
            return Ok(());
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn stop_pinned(
    snapshot: &Manager,
    identity: &recovery::Observation<'_>,
    host_namespace: [u64; 2],
    end: Duration,
) -> Result<()> {
    if snapshot.live_pid()? != identity.main_pid {
        return Err(Fault::Evidence);
    }
    let init = PinnedInit::open(identity.main_pid, identity.unit, host_namespace)?;
    if !init.matches(identity) {
        return Err(Fault::Evidence);
    }
    if *snapshot != Manager::load(identity.unit, end)? {
        return Ok(()); // Reobserve a concurrent manager transition before signalling.
    }
    check_startup(end)?;
    // The pidfd pins the original namespace init across a manager or PID transition.
    match pidfd_send_signal(&init.pidfd, Signal::KILL) {
        Ok(()) | Err(Errno::SRCH) => {}
        Err(_) => return Err(Fault::Evidence),
    }
    while !init.exited()? {
        check_startup(end)?;
        poll_input(&init.pidfd, end)?;
    }
    Ok(())
}

pub(super) fn require_before_go(config: &super::Config, namespace: [u64; 2]) -> Result<()> {
    let record = recovery::recover(&config.recovery_directory).map_err(|_| Fault::Evidence)?;
    let invocation = env::var("INVOCATION_ID").map_err(|_| Fault::Evidence)?;
    let stat = read_bounded(Path::new("/proc/self/stat"), COMMAND_LIMIT as u64)
        .map_err(|_| Fault::Evidence)?;
    let start_ticks = recovery::start_ticks(&stat).map_err(|_| Fault::Evidence)?;
    let unit = generation_unit(&config.generation);
    record
        .require(recovery::LocalIdentity {
            boot_id: &config.boot_id,
            generation: &config.generation,
            invocation_id: &invocation,
            main_start_ticks: start_ticks,
            pid_namespace: namespace,
            cgroup_identity: cgroup_identity(&unit)?,
        })
        .map_err(|_| Fault::Evidence)
}

fn generation_unit(generation: &str) -> String {
    format!("stateknot-owner-{generation}.service")
}

fn cgroup_path(unit: &str) -> PathBuf {
    Path::new(CGROUP_ROOT).join("system.slice").join(unit)
}

fn verify_host() -> Result<[u64; 2]> {
    let namespace = current_namespace()?;
    let init = fs::metadata("/proc/1/ns/pid").map_err(|_| Fault::Profile)?;
    if process::getuid().as_raw() != 0
        || process::geteuid().as_raw() != 0
        || namespace != [init.dev(), init.ino()]
        || read_bounded(Path::new("/proc/1/comm"), 64).map_err(|_| Fault::Profile)? != b"systemd\n"
        || !Path::new(CGROUP_ROOT).join("cgroup.controllers").is_file()
    {
        return Err(Fault::Profile);
    }
    Ok(namespace)
}

fn boot_id() -> Result<String> {
    let raw = read_bounded(Path::new("/proc/sys/kernel/random/boot_id"), 64)
        .map_err(|_| Fault::Profile)?;
    let boot = String::from_utf8(raw).map_err(|_| Fault::Profile)?;
    Ok(boot.trim_end().to_owned())
}

fn cgroup_identity(unit: &str) -> Result<[u64; 2]> {
    let metadata = fs::symlink_metadata(cgroup_path(unit)).map_err(|_| Fault::Evidence)?;
    if !metadata.is_dir() {
        return Err(Fault::Evidence);
    }
    Ok([metadata.dev(), metadata.ino()])
}

struct PinnedInit {
    pidfd: OwnedFd,
    proc_dir: File,
    start_ticks: u64,
    namespace: [u64; 2],
    cgroup_identity: [u64; 2],
}

impl PinnedInit {
    fn open(pid: u32, unit: &str, host_namespace: [u64; 2]) -> Result<Self> {
        if pid <= 1 {
            return Err(Fault::Evidence);
        }
        let kernel_pid = Pid::from_raw(i32::try_from(pid).map_err(|_| Fault::Evidence)?)
            .ok_or(Fault::Evidence)?;
        let pidfd = pidfd_open(kernel_pid, PidfdFlags::empty()).map_err(|_| Fault::Evidence)?;
        let proc_dir = File::open(format!("/proc/{pid}")).map_err(|_| Fault::Evidence)?;
        let ns = statat(&proc_dir, "ns/pid", AtFlags::empty()).map_err(|_| Fault::Evidence)?;
        let status = proc_read(&proc_dir, "status")?;
        let status = std::str::from_utf8(&status).map_err(|_| Fault::Evidence)?;
        let nspid = proc_field(status, "NSpid")?;
        let ids: Vec<_> = nspid.split_ascii_whitespace().collect();
        let namespace = [ns.st_dev, ns.st_ino];
        if namespace == host_namespace
            || ids.len() != 2
            || ids[0].parse::<u32>() != Ok(pid)
            || ids[1] != "1"
            || !proc_field(status, "Uid")?
                .split_ascii_whitespace()
                .eq(["0"; 4])
            || proc_field(status, "PPid")? != "1"
        {
            return Err(Fault::Evidence);
        }
        let cgroup = proc_read(&proc_dir, "cgroup")?;
        if cgroup != format!("0::/system.slice/{unit}\n").as_bytes() {
            return Err(Fault::Evidence);
        }
        let stat = proc_read(&proc_dir, "stat")?;
        let init = Self {
            pidfd,
            proc_dir,
            start_ticks: recovery::start_ticks(&stat).map_err(|_| Fault::Evidence)?,
            namespace,
            cgroup_identity: cgroup_identity(unit)?,
        };
        if init.exited()? {
            return Err(Fault::Evidence);
        }
        Ok(init)
    }

    fn require_current_executable(&self) -> Result<()> {
        let actual =
            statat(&self.proc_dir, "exe", AtFlags::empty()).map_err(|_| Fault::Evidence)?;
        let own_path = trusted_executable(&env::current_exe().map_err(|_| Fault::Profile)?)?;
        let expected = fs::metadata(own_path).map_err(|_| Fault::Profile)?;
        if [actual.st_dev, actual.st_ino] != [expected.dev(), expected.ino()] {
            return Err(Fault::Evidence);
        }
        Ok(())
    }

    fn exited(&self) -> Result<bool> {
        let mut fds = [PollFd::new(self.pidfd.as_fd(), PollFlags::POLLIN)];
        match poll(&mut fds, PollTimeout::ZERO) {
            Ok(ready) => Ok(ready > 0),
            Err(_) => Err(Fault::Evidence),
        }
    }

    fn matches(&self, identity: &recovery::Observation<'_>) -> bool {
        self.start_ticks == identity.main_start_ticks
            && self.namespace == identity.pid_namespace
            && self.cgroup_identity == identity.cgroup_identity
    }
}

fn proc_read(directory: &File, name: &str) -> Result<Vec<u8>> {
    let fd = openat(
        directory,
        name,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
        rustix::fs::Mode::empty(),
    )
    .map_err(|_| Fault::Evidence)?;
    let mut bytes = Vec::new();
    File::from(fd)
        .take(COMMAND_LIMIT as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Fault::Evidence)?;
    if bytes.len() > COMMAND_LIMIT {
        return Err(Fault::Evidence);
    }
    Ok(bytes)
}

fn proc_field<'a>(status: &'a str, field: &str) -> Result<&'a str> {
    status
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find_map(|(key, value)| (key == field).then_some(value.trim()))
        .ok_or(Fault::Evidence)
}

fn original_proc_gone(identity: &recovery::Observation<'_>) -> Result<bool> {
    let path = PathBuf::from(format!("/proc/{}", identity.main_pid));
    let proc_dir = match File::open(&path) {
        Ok(directory) => directory,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(true),
        Err(_) => return Err(Fault::Evidence),
    };
    let Ok(stat) = proc_read(&proc_dir, "stat") else {
        return absent_after_read(&path);
    };
    let start_ticks = recovery::start_ticks(&stat).map_err(|_| Fault::Evidence)?;
    Ok(start_ticks != identity.main_start_ticks)
}

fn cgroup_retired(identity: &recovery::Observation<'_>) -> Result<bool> {
    let path = cgroup_path(identity.unit);
    let directory = match open(
        &path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(directory) => File::from(directory),
        Err(Errno::NOENT) => return Ok(true),
        Err(_) => return Err(Fault::Evidence),
    };
    let metadata = directory.metadata().map_err(|_| Fault::Evidence)?;
    if !metadata.is_dir() || [metadata.dev(), metadata.ino()] != identity.cgroup_identity {
        return Err(Fault::Evidence);
    }
    let Ok(events) = proc_read(&directory, "cgroup.events") else {
        return absent_after_read(&path);
    };
    Ok(events
        .split(|byte| *byte == b'\n')
        .any(|line| line == b"populated 0"))
}

fn absent_after_read(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(true),
        _ => Err(Fault::Evidence),
    }
}

#[derive(PartialEq, Eq)]
struct Manager(BTreeMap<String, String>);

impl Manager {
    fn load(unit: &str, end: Duration) -> Result<Self> {
        let deadline = end.min(monotonic_now()? + SNAPSHOT_TIME);
        let output = manager_output(unit, deadline)?;
        let raw = std::str::from_utf8(&output).map_err(|_| Fault::Evidence)?;
        let mut fields = BTreeMap::new();
        for line in raw.lines() {
            let (key, value) = line.split_once('=').ok_or(Fault::Evidence)?;
            if !PROPERTIES.contains(&key)
                || fields.insert(key.to_owned(), value.to_owned()).is_some()
            {
                return Err(Fault::Evidence);
            }
        }
        let snapshot = Self(fields);
        let cgroup = snapshot.value("ControlGroup")?;
        let terminal = matches!(snapshot.value("ActiveState")?, "inactive" | "failed");
        if snapshot.0.len() != PROPERTIES.len()
            || snapshot.value("Id")? != unit
            || snapshot.value("LoadState")? != "loaded"
            || !(cgroup == format!("/system.slice/{unit}") || terminal && cgroup.is_empty())
        {
            return Err(Fault::Evidence);
        }
        Ok(snapshot)
    }

    fn value(&self, key: &str) -> Result<&str> {
        self.0.get(key).map(String::as_str).ok_or(Fault::Evidence)
    }

    fn number(&self, key: &str) -> Result<u64> {
        self.value(key)?.parse().map_err(|_| Fault::Evidence)
    }

    fn live_pid(&self) -> Result<u32> {
        for (name, expected) in [
            ("ActiveState", "active"),
            ("SubState", "running"),
            ("Type", "notify"),
            ("NotifyAccess", "main"),
            ("PrivatePIDs", "yes"),
            ("Restart", "no"),
            ("KillMode", "control-group"),
            ("SendSIGKILL", "yes"),
            ("WatchdogUSec", "5s"),
            ("CollectMode", "inactive"),
            ("TimeoutStartUSec", "15s"),
            ("TimeoutStopUSec", "15s"),
            ("RuntimeMaxUSec", "35s"),
            ("ProtectControlGroups", "yes"),
            ("NoNewPrivileges", "yes"),
            ("Delegate", "no"),
        ] {
            if self.value(name)? != expected {
                return Err(Fault::Profile);
            }
        }
        if !matches!(self.value("WatchdogSignal")?, "9" | "SIGKILL") {
            return Err(Fault::Profile);
        }
        let pid = self.number("MainPID")?;
        if pid <= 1 || self.number("ExecMainPID")? != pid {
            return Err(Fault::Evidence);
        }
        u32::try_from(pid).map_err(|_| Fault::Evidence)
    }

    fn require_invocation(&self, identity: &recovery::Observation<'_>) -> Result<()> {
        if self.value("InvocationID")? != identity.invocation_id
            || self.number("ExecMainPID")? != u64::from(identity.main_pid)
        {
            return Err(Fault::Evidence);
        }
        Ok(())
    }

    fn retired(&self, identity: &recovery::Observation<'_>) -> Result<bool> {
        self.require_invocation(identity)?;
        Ok(matches!(self.value("ActiveState")?, "inactive" | "failed")
            && self.number("MainPID")? == 0
            && matches!(self.number("ExecMainCode")?, 1..=3)
            && self.number("ExecMainExitTimestampMonotonic")? > 0)
    }
}

fn manager_output(unit: &str, deadline: Duration) -> Result<Vec<u8>> {
    let executable = trusted_executable(Path::new(SYSTEMCTL))?;
    let properties = format!("--property={}", PROPERTIES.join(","));
    check_startup(deadline)?;
    let mut child = Command::new(executable)
        .args(["--no-pager", "--no-ask-password", "show", &properties, unit])
        .env_clear()
        .env("LC_ALL", "C")
        .current_dir("/")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| Fault::Evidence)?;
    let result = (|| {
        let mut stream = child.stdout.take().ok_or(Fault::Evidence)?;
        let flags = fcntl_getfl(&stream).map_err(|_| Fault::Evidence)?;
        fcntl_setfl(&stream, flags | OFlags::NONBLOCK).map_err(|_| Fault::Evidence)?;
        let mut output = Vec::new();
        let mut chunk = [0; 1024];
        loop {
            check_startup(deadline)?;
            match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) if output.len() + n <= COMMAND_LIMIT => output.extend_from_slice(&chunk[..n]),
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    poll_input(&stream, deadline)?;
                }
                _ => return Err(Fault::Evidence),
            }
        }
        loop {
            check_startup(deadline)?;
            if let Some(status) = child.try_wait().map_err(|_| Fault::Evidence)? {
                return if status.success() {
                    Ok(output)
                } else {
                    Err(Fault::Evidence)
                };
            }
            thread::sleep(Duration::from_millis(10));
        }
    })();
    if result.is_err() {
        let _ = child.kill();
        let _ = child.wait();
    }
    result
}
