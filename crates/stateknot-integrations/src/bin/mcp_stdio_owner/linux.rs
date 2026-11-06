// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use std::{
    env,
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::{
        fd::{AsFd, OwnedFd},
        unix::{
            ffi::OsStrExt,
            fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
        },
    },
    path::{Path, PathBuf},
    process::{Child, Command, ExitCode, Stdio},
    time::{Duration, Instant},
};

use nix::poll::{PollFd, PollFlags, PollTimeout, poll};
use rustix::{
    io::Errno,
    net::{self, AddressFamily, RecvFlags, SocketAddrUnix, SocketFlags, SocketType},
    process::{self, Pid, WaitOptions},
    time::{ClockId, clock_gettime},
};
use serde::Deserialize;
use serde_json::{Value, json};

#[path = "linux/launch.rs"]
mod launch;

type Result<T> = std::result::Result<T, Fault>;

const SETPRIV: &str = "/usr/bin/setpriv";
const STARTUP: Duration = Duration::from_secs(12);
const LEASE: Duration = Duration::from_secs(5);
const MAX_GENERATION: Duration = Duration::from_secs(35);
const WATCHDOG_INTERVAL: Duration = Duration::from_secs(1);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone, Copy)]
enum Fault {
    Configuration,
    Profile,
    Evidence,
    Watchdog,
    Control,
    Startup,
    Launch,
}

impl Fault {
    const fn label(self) -> &'static str {
        match self {
            Self::Configuration => "invalid-configuration",
            Self::Profile => "unsupported-owner-profile",
            Self::Evidence => "owner-evidence-unavailable",
            Self::Watchdog => "owner-watchdog-unavailable",
            Self::Control => "invalid-owner-control",
            Self::Startup => "owner-startup-expired",
            Self::Launch => "target-launch-failed",
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    generation: String,
    host_namespace: [u64; 2],
    boot_id: String,
    target_uid: u32,
    target_gid: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Control {
    op: Operation,
    generation: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum Operation {
    Go,
    Renew,
    Stop,
}

enum StopReason {
    GenerationRejected,
    OwnerEof,
    OwnerStop,
    LeaseExpired,
    GenerationExpired,
    TargetExited,
}

impl StopReason {
    const fn label(&self) -> &'static str {
        match self {
            Self::GenerationRejected => "generation-rejected",
            Self::OwnerEof => "owner-eof",
            Self::OwnerStop => "owner-stop",
            Self::LeaseExpired => "owner-lease-expired",
            Self::GenerationExpired => "generation-expired",
            Self::TargetExited => "target-exited",
        }
    }

    const fn exit_code(&self) -> u8 {
        match self {
            Self::GenerationRejected => 44,
            Self::OwnerEof | Self::OwnerStop | Self::LeaseExpired => 42,
            Self::GenerationExpired | Self::TargetExited => 1,
        }
    }
}

pub(super) fn main() -> ExitCode {
    match run() {
        Ok(code) => ExitCode::from(code),
        Err(fault) => {
            eprintln!("stateknot-mcp-stdio-owner: {}", fault.label());
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<u8> {
    let mut args = env::args_os().skip(1);
    match args.next().as_deref() {
        Some(mode) if mode == OsStr::new("namespace-init") => {
            let directory = args.next().ok_or(Fault::Configuration)?;
            if args.next().is_some() {
                return Err(Fault::Configuration);
            }
            namespace_init(Path::new(&directory))
        }
        Some(mode) if mode == OsStr::new("exec-target") => {
            let uid = decimal(args.next().as_deref())?;
            let gid = decimal(args.next().as_deref())?;
            let namespace = [
                decimal(args.next().as_deref())?,
                decimal(args.next().as_deref())?,
            ];
            let deadline = Duration::from_nanos(decimal(args.next().as_deref())?);
            if args.next().is_some() {
                return Err(Fault::Configuration);
            }
            launch::exec_target(uid, gid, namespace, deadline)?;
            Ok(0)
        }
        _ => Err(Fault::Configuration),
    }
}

fn decimal<T: std::str::FromStr>(raw: Option<&OsStr>) -> Result<T> {
    raw.and_then(OsStr::to_str)
        .and_then(|value| value.parse().ok())
        .ok_or(Fault::Configuration)
}

fn namespace_init(directory: &Path) -> Result<u8> {
    let started = monotonic_now()?;
    let (config, namespace) = verify_profile(directory)?;
    let helper = trusted_executable(&env::current_exe().map_err(|_| Fault::Profile)?)?;
    let setpriv = trusted_executable(Path::new(SETPRIV))?;
    let listener = open_control(directory)?;
    let mut watchdog = Watchdog::open()?;
    write_private(
        directory,
        "helper-ready.json",
        &json!({"pid": 1, "namespace": namespace, "target_spawns": 0, "generation": config.generation}),
    )?;
    watchdog.send(b"READY=1\nWATCHDOG=1")?;
    let startup_end = started + STARTUP;
    let connection = accept_owner(&listener, startup_end, &mut watchdog)?;
    drop(listener);

    let mut target = None;
    let mut owner = Owner {
        directory,
        config: &config,
        namespace,
        helper: &helper,
        setpriv: &setpriv,
        target: &mut target,
        startup_end,
        generation_end: started + MAX_GENERATION,
        lease_deadline: startup_end,
        last_renewal: None,
        accepted_renewals: 0,
    };
    let outcome = owner.drive(&connection, &mut watchdog);
    let (reason, decided) = match &outcome {
        Ok((stop, observed)) => (stop.label(), *observed),
        Err(fault) => (fault.label(), monotonic_now()?),
    };
    // Publish the decision before the peer can observe EOF. The outer
    // manager independently confirms retirement and reaping after exit.
    write_private(
        directory,
        "helper-terminal.json",
        &json!({
            "reason": reason,
            "target_spawns": u8::from(owner.target.is_some()),
            "accepted_renewals": owner.accepted_renewals,
            "last_renewal_monotonic_ns": owner.last_renewal.map(monotonic_nanos).transpose()?,
            "lease_deadline_monotonic_ns": monotonic_nanos(owner.lease_deadline)?,
            "terminal_monotonic_ns": monotonic_nanos(decided)?,
        }),
    )?;
    drop(connection);
    outcome.map(|(stop, _)| stop.exit_code())
}

fn verify_profile(directory: &Path) -> Result<(Config, [u64; 2])> {
    if process::getpid() != Pid::INIT
        || process::getuid().as_raw() != 0
        || process::geteuid().as_raw() != 0
    {
        return Err(Fault::Profile);
    }
    verify_directory(directory)?;
    let metadata = fs::symlink_metadata(directory).map_err(|_| Fault::Profile)?;
    if metadata.mode() & 0o777 != 0o700 {
        return Err(Fault::Profile);
    }
    let path = directory.join("config.json");
    let metadata = fs::symlink_metadata(&path).map_err(|_| Fault::Configuration)?;
    if !metadata.is_file() || metadata.uid() != 0 || metadata.mode() & 0o777 != 0o600 {
        return Err(Fault::Configuration);
    }
    let raw = read_bounded(&path, 4096).map_err(|_| Fault::Configuration)?;
    let config: Config = serde_json::from_slice(&raw).map_err(|_| Fault::Configuration)?;
    let valid_generation = config.generation.len() == 32
        && config
            .generation
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase());
    if !valid_generation || !target_id(config.target_uid) || !target_id(config.target_gid) {
        return Err(Fault::Configuration);
    }
    let namespace = current_namespace()?;
    let boot = read_bounded(Path::new("/proc/sys/kernel/random/boot_id"), 64)
        .map_err(|_| Fault::Profile)?;
    let cgroup = read_bounded(Path::new("/proc/self/cgroup"), 4096).map_err(|_| Fault::Profile)?;
    let boot = std::str::from_utf8(&boot).map_err(|_| Fault::Profile)?;
    let cgroup = std::str::from_utf8(&cgroup).map_err(|_| Fault::Profile)?;
    let expected_cgroup = format!(
        "0::/system.slice/stateknot-owner-{}.service",
        config.generation
    );
    if namespace == config.host_namespace
        || boot.trim() != config.boot_id
        || cgroup.trim_end() != expected_cgroup
    {
        return Err(Fault::Profile);
    }
    Ok((config, namespace))
}

const fn target_id(value: u32) -> bool {
    value != 0 && value != u32::MAX
}

fn current_namespace() -> Result<[u64; 2]> {
    let meta = fs::metadata("/proc/self/ns/pid").map_err(|_| Fault::Profile)?;
    Ok([meta.dev(), meta.ino()])
}

fn verify_directory(path: &Path) -> Result<()> {
    if !path.is_absolute() || fs::canonicalize(path).map_err(|_| Fault::Profile)? != path {
        return Err(Fault::Profile);
    }
    for parent in path.ancestors() {
        let meta = fs::symlink_metadata(parent).map_err(|_| Fault::Profile)?;
        if !meta.is_dir() || meta.uid() != 0 || meta.mode() & 0o022 != 0 {
            return Err(Fault::Profile);
        }
    }
    Ok(())
}

fn trusted_executable(path: &Path) -> Result<PathBuf> {
    let path = fs::canonicalize(path).map_err(|_| Fault::Profile)?;
    verify_directory(path.parent().ok_or(Fault::Profile)?)?;
    let meta = fs::symlink_metadata(&path).map_err(|_| Fault::Profile)?;
    if !meta.is_file() || meta.uid() != 0 || meta.mode() & 0o022 != 0 || meta.mode() & 0o111 == 0 {
        return Err(Fault::Profile);
    }
    let mut magic = [0; 4];
    File::open(&path)
        .and_then(|mut file| file.read_exact(&mut magic))
        .map_err(|_| Fault::Profile)?;
    if magic != *b"\x7fELF" {
        return Err(Fault::Profile);
    }
    Ok(path)
}

fn read_bounded(path: &Path, maximum: u64) -> std::io::Result<Vec<u8>> {
    let mut data = Vec::new();
    File::open(path)?.take(maximum + 1).read_to_end(&mut data)?;
    if u64::try_from(data.len()).unwrap_or(u64::MAX) > maximum {
        return Err(std::io::Error::from(std::io::ErrorKind::InvalidData));
    }
    Ok(data)
}

fn write_private(directory: &Path, name: &str, value: &Value) -> Result<()> {
    let pending = directory.join(format!("{name}.pending"));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&pending)
        .map_err(|_| Fault::Evidence)?;
    serde_json::to_writer(&mut file, value).map_err(|_| Fault::Evidence)?;
    file.write_all(b"\n").map_err(|_| Fault::Evidence)?;
    file.sync_all().map_err(|_| Fault::Evidence)?;
    // Publish a complete new record without overwriting old generation data.
    fs::hard_link(&pending, directory.join(name)).map_err(|_| Fault::Evidence)?;
    fs::remove_file(pending).map_err(|_| Fault::Evidence)?;
    File::open(directory)
        .and_then(|dir| dir.sync_all())
        .map_err(|_| Fault::Evidence)
}

fn open_control(directory: &Path) -> Result<OwnedFd> {
    let path = directory.join("control.sock");
    let address = SocketAddrUnix::new(&path).map_err(|_| Fault::Control)?;
    let socket = net::socket_with(
        AddressFamily::UNIX,
        SocketType::SEQPACKET,
        SocketFlags::CLOEXEC | SocketFlags::NONBLOCK,
        None,
    )
    .map_err(|_| Fault::Control)?;
    net::bind(&socket, &address).map_err(|_| Fault::Control)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|_| Fault::Control)?;
    net::listen(&socket, 1).map_err(|_| Fault::Control)?;
    Ok(socket)
}

fn accept_owner(
    listener: &OwnedFd,
    deadline: Duration,
    watchdog: &mut Watchdog,
) -> Result<OwnedFd> {
    loop {
        check_startup(deadline)?;
        watchdog.tick()?;
        match net::accept_with(listener, SocketFlags::CLOEXEC | SocketFlags::NONBLOCK) {
            Ok(owner) => return Ok(owner),
            Err(Errno::INTR) => {}
            Err(Errno::AGAIN) => {
                poll_input(listener, deadline)?;
            }
            Err(_) => return Err(Fault::Control),
        }
    }
}

fn monotonic_now() -> Result<Duration> {
    // A CLOCK_MONOTONIC value can cross exec; std::time::Instant cannot.
    let now = clock_gettime(ClockId::Monotonic);
    let seconds = u64::try_from(now.tv_sec).map_err(|_| Fault::Profile)?;
    let nanos = u32::try_from(now.tv_nsec).map_err(|_| Fault::Profile)?;
    Ok(Duration::new(seconds, nanos))
}

fn monotonic_nanos(value: Duration) -> Result<u64> {
    u64::try_from(value.as_nanos()).map_err(|_| Fault::Profile)
}

fn check_startup(deadline: Duration) -> Result<()> {
    if monotonic_now()? >= deadline {
        return Err(Fault::Startup);
    }
    Ok(())
}

fn poll_input(fd: &impl AsFd, deadline: Duration) -> Result<bool> {
    let timeout =
        PollTimeout::try_from(deadline.saturating_sub(monotonic_now()?).min(POLL_INTERVAL))
            .map_err(|_| Fault::Control)?;
    let mut fds = [PollFd::new(fd.as_fd(), PollFlags::POLLIN)];
    match poll(&mut fds, timeout) {
        Ok(ready) => Ok(ready > 0),
        Err(nix::errno::Errno::EINTR) => Ok(false),
        Err(_) => Err(Fault::Control),
    }
}

struct Watchdog {
    fd: OwnedFd,
    address: SocketAddrUnix,
    next: Instant,
}

impl Watchdog {
    fn open() -> Result<Self> {
        if env::var("WATCHDOG_USEC").ok().as_deref() != Some("5000000") {
            return Err(Fault::Profile);
        }
        let raw = env::var_os("NOTIFY_SOCKET").ok_or(Fault::Profile)?;
        let raw = raw.as_os_str().as_bytes();
        let address = if let Some(name) = raw.strip_prefix(b"@") {
            SocketAddrUnix::new_abstract_name(name)
        } else if raw.starts_with(b"/") {
            SocketAddrUnix::new(OsStr::from_bytes(raw))
        } else {
            return Err(Fault::Profile);
        }
        .map_err(|_| Fault::Watchdog)?;
        let fd = net::socket_with(
            AddressFamily::UNIX,
            SocketType::DGRAM,
            SocketFlags::CLOEXEC | SocketFlags::NONBLOCK,
            None,
        )
        .map_err(|_| Fault::Watchdog)?;
        Ok(Self {
            fd,
            address,
            next: Instant::now(),
        })
    }

    fn send(&mut self, message: &[u8]) -> Result<()> {
        let sent = net::sendto(&self.fd, message, net::SendFlags::empty(), &self.address)
            .map_err(|_| Fault::Watchdog)?;
        if sent != message.len() {
            return Err(Fault::Watchdog);
        }
        self.next = Instant::now() + WATCHDOG_INTERVAL;
        Ok(())
    }

    fn tick(&mut self) -> Result<()> {
        if Instant::now() >= self.next {
            self.send(b"WATCHDOG=1")?;
        }
        Ok(())
    }
}

struct Owner<'a> {
    directory: &'a Path,
    config: &'a Config,
    namespace: [u64; 2],
    helper: &'a Path,
    setpriv: &'a Path,
    target: &'a mut Option<Child>,
    startup_end: Duration,
    generation_end: Duration,
    lease_deadline: Duration,
    last_renewal: Option<Duration>,
    accepted_renewals: u32,
}

impl Owner<'_> {
    fn drive(
        &mut self,
        owner: &OwnedFd,
        watchdog: &mut Watchdog,
    ) -> Result<(StopReason, Duration)> {
        let mut packet = [0; 2048];
        loop {
            let now = monotonic_now()?;
            if let Some(reason) = self.expired(now)? {
                return Ok((reason, now));
            }
            if let Some(child) = self.target.as_mut() {
                if reap_target(child)? {
                    return Ok((StopReason::TargetExited, monotonic_now()?));
                }
            }
            watchdog.tick()?;
            let received = net::recv(owner, &mut packet, RecvFlags::TRUNC);
            let observed = monotonic_now()?;
            // Expiry wins before EOF, an error or a late command can be classified.
            if let Some(reason) = self.expired(observed)? {
                return Ok((reason, observed));
            }
            let length = match received {
                Ok((0, 0)) | Err(Errno::CONNRESET) => return Ok((StopReason::OwnerEof, observed)),
                Ok((_, length)) if length <= packet.len() => length,
                Err(Errno::INTR) => continue,
                Err(Errno::AGAIN) => {
                    poll_input(owner, self.lease_deadline.min(self.generation_end))?;
                    continue;
                }
                _ => return Err(Fault::Control),
            };
            let decoded = serde_json::from_slice::<Control>(&packet[..length]);
            let decision_time = monotonic_now()?;
            if let Some(reason) = self.expired(decision_time)? {
                return Ok((reason, decision_time));
            }
            let command = decoded.map_err(|_| Fault::Control)?;
            if command.generation != self.config.generation {
                return Ok((StopReason::GenerationRejected, decision_time));
            }
            match command.op {
                Operation::Go if self.target.is_none() => {
                    self.start_target()?;
                    self.lease_deadline = observed + LEASE;
                }
                Operation::Renew if self.target.is_some() => self.renew(observed)?,
                Operation::Stop if self.target.is_some() => {
                    return Ok((StopReason::OwnerStop, decision_time));
                }
                _ => return Err(Fault::Control),
            }
        }
    }

    fn renew(&mut self, observed: Duration) -> Result<()> {
        self.accepted_renewals = self
            .accepted_renewals
            .checked_add(1)
            .ok_or(Fault::Control)?;
        self.last_renewal = Some(observed);
        self.lease_deadline = observed + LEASE;
        Ok(())
    }

    fn expired(&self, now: Duration) -> Result<Option<StopReason>> {
        if now >= self.generation_end {
            return Ok(Some(StopReason::GenerationExpired));
        }
        if now >= self.lease_deadline {
            return if self.target.is_some() {
                Ok(Some(StopReason::LeaseExpired))
            } else {
                Err(Fault::Startup)
            };
        }
        Ok(None)
    }

    fn start_target(&mut self) -> Result<()> {
        // Only fixed operator binaries and a cleared environment are used
        // while privileges remain. The dropped child receives the launch
        // prelude directly on stdin and never via the manager or argv.
        let deadline_nanos = monotonic_nanos(self.startup_end)?;
        let mut command = Command::new(self.setpriv);
        command
            .args([
                "--bounding-set=-all",
                "--inh-caps=-all",
                "--ambient-caps=-all",
                "--clear-groups",
                "--no-new-privs",
            ])
            .arg(format!("--reuid={}", self.config.target_uid))
            .arg(format!("--regid={}", self.config.target_gid))
            .arg(self.helper)
            .arg("exec-target")
            .arg(self.config.target_uid.to_string())
            .arg(self.config.target_gid.to_string())
            .arg(self.namespace[0].to_string())
            .arg(self.namespace[1].to_string())
            .arg(deadline_nanos.to_string())
            .env_clear()
            .current_dir("/")
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        check_startup(self.startup_end)?;
        let child = command.spawn().map_err(|_| Fault::Launch)?;
        let pid = child.id();
        *self.target = Some(child);
        // PID1's wait loop owns all reaping, including adopted descendants.
        write_private(
            self.directory,
            "target-started.json",
            &json!({"child_namespace_pid": pid, "target_spawns": 1}),
        )
    }
}

fn reap_target(child: &mut Child) -> Result<bool> {
    if child.try_wait().map_err(|_| Fault::Launch)?.is_some() {
        return Ok(true);
    }
    let target = Pid::from_child(child);
    // Bound one drain turn so a fork-heavy target cannot starve lease checks.
    // If the direct child exits after try_wait, this wait may reap it.
    for _ in 0..16 {
        match process::wait(WaitOptions::NOHANG) {
            Ok(Some((pid, _))) if pid == target => return Ok(true),
            Ok(Some(_)) | Err(Errno::INTR) => {}
            Ok(None) => return Ok(false),
            Err(_) => return Err(Fault::Launch),
        }
    }
    Ok(false)
}
