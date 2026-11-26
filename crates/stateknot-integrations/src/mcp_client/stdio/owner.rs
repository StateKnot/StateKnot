// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Linux host lifecycle for the fixed operator broker.

use std::{
    fs::{self, File},
    io::Read,
    os::{fd::OwnedFd, unix::fs::MetadataExt},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
        mpsc,
    },
    thread::{self, Thread},
    time::{Duration, Instant},
};

use rustix::{
    fs::{self as unix, FlockOperation, Mode, OFlags},
    io::Errno,
    net::{self, AddressFamily, RecvFlags, SendFlags, SocketAddrUnix, SocketFlags, SocketType},
    process,
};
use serde::{Deserialize, de::DeserializeOwned};
use tokio::{
    process::{ChildStderr, ChildStdin, ChildStdout},
    sync::oneshot,
};
use tokio_util::sync::CancellationToken;

const RECOVERY_ROOT: &str = "/var/lib/stateknot/mcp-stdio";
const STARTUP: Duration = Duration::from_secs(12);
const OWNER_LIMIT: Duration = Duration::from_secs(70);
const RENEW: Duration = Duration::from_millis(500);
const GRACE: Duration = Duration::from_secs(2);
const RECOVERY_LIMIT: Duration = Duration::from_secs(12);
const POLL: Duration = Duration::from_millis(20);
const STARTING: u8 = 0;
const READY: u8 = 1;
const GO: u8 = 2;
const RETIRING: u8 = 3;

#[derive(Clone, Copy, Debug, thiserror::Error)]
pub(super) enum OwnerError {
    #[error("Linux stdio owner configuration is unavailable")]
    Configuration,
    #[error("Linux stdio owner could not start")]
    Launch,
    #[error("Linux stdio owner did not become ready")]
    Startup,
    #[error("Linux stdio owner is not accepting commands")]
    Stopped,
    #[error("Linux stdio owner recovery is pending")]
    Recovery,
}

type Result<T> = std::result::Result<T, OwnerError>;

pub(super) struct Owner {
    shared: Arc<Shared>,
    worker: Thread,
    ready: Signal,
    done: Signal,
}

struct Shared {
    stop: CancellationToken,
    state: AtomicU8,
}

struct Signal {
    receiver: oneshot::Receiver<Result<()>>,
    observed: Option<Result<()>>,
}

impl Signal {
    const fn new(receiver: oneshot::Receiver<Result<()>>) -> Self {
        Self {
            receiver,
            observed: None,
        }
    }

    async fn read(&mut self, missing: OwnerError) -> Result<()> {
        if let Some(value) = self.observed {
            return value;
        }
        let value = (&mut self.receiver).await.unwrap_or(Err(missing));
        self.observed = Some(value);
        value
    }
}

impl Owner {
    pub(super) fn spawn(
        helper: &Path,
        control: &Path,
        recovery: &Path,
        generation: &str,
        stop: CancellationToken,
    ) -> Result<(Self, ChildStdin, ChildStdout, ChildStderr)> {
        if stop.is_cancelled() || tokio::runtime::Handle::try_current().is_err() {
            return Err(OwnerError::Configuration);
        }
        let paths = Paths::new(helper, control, recovery, generation)?;
        let mut command = paths.command("launch-generation", &paths.control);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let shared = Arc::new(Shared {
            stop,
            state: AtomicU8::new(STARTING),
        });
        let (ready_tx, ready_rx) = oneshot::channel();
        let (done_tx, done_rx) = oneshot::channel();
        let (transfer, receive) = mpsc::sync_channel::<Child>(1);
        let worker_shared = Arc::clone(&shared);
        let started = Instant::now();
        // Establish the independent reaper before any OS process can start.
        let worker = thread::Builder::new()
            .name("stateknot-mcp-stdio-owner".to_owned())
            .spawn(move || {
                let _cancel = worker_shared.stop.clone().drop_guard();
                if let Ok(child) = receive.recv() {
                    run_owner(&paths, &worker_shared, child, ready_tx, done_tx, started);
                }
            })
            .map_err(|_| OwnerError::Launch)?;
        let worker = worker.thread().clone();

        let mut child = command.spawn().map_err(|_| OwnerError::Launch)?;
        let raw = (child.stdin.take(), child.stdout.take(), child.stderr.take());
        // Tokio pipe conversion can fail. The process is already owned by
        // the independent worker, and this guard stops it on failure.
        let startup_guard = shared.stop.clone().drop_guard();
        if let Err(error) = transfer.send(child) {
            // The worker receives before running any fallible owner work.
            // If it nevertheless vanished, retire only our direct child.
            let mut child = error.0;
            let _ = reap_launcher(&mut child);
            return Err(OwnerError::Launch);
        }
        let (stdin, stdout, stderr) = convert_pipes(raw.0, raw.1, raw.2)?;
        let _ = startup_guard.disarm();
        Ok((
            Self {
                shared,
                worker,
                ready: Signal::new(ready_rx),
                done: Signal::new(done_rx),
            },
            stdin,
            stdout,
            stderr,
        ))
    }

    pub(super) async fn ready(&mut self) -> Result<()> {
        self.ready.read(OwnerError::Startup).await
    }

    pub(super) fn go(&self) -> Result<()> {
        if self.shared.stop.is_cancelled() {
            return Err(OwnerError::Stopped);
        }
        self.shared
            .state
            .compare_exchange(READY, GO, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| OwnerError::Stopped)?;
        self.worker.unpark();
        Ok(())
    }

    pub(super) async fn wait(&mut self) -> Result<()> {
        self.done.read(OwnerError::Recovery).await
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.shared.stop.cancel();
        self.worker.unpark();
    }
}

fn convert_pipes(
    stdin: Option<std::process::ChildStdin>,
    stdout: Option<std::process::ChildStdout>,
    stderr: Option<std::process::ChildStderr>,
) -> Result<(ChildStdin, ChildStdout, ChildStderr)> {
    let stdin =
        ChildStdin::from_std(stdin.ok_or(OwnerError::Launch)?).map_err(|_| OwnerError::Launch)?;
    let stdout =
        ChildStdout::from_std(stdout.ok_or(OwnerError::Launch)?).map_err(|_| OwnerError::Launch)?;
    let stderr =
        ChildStderr::from_std(stderr.ok_or(OwnerError::Launch)?).map_err(|_| OwnerError::Launch)?;
    Ok((stdin, stdout, stderr))
}

struct Paths {
    helper: PathBuf,
    control: PathBuf,
    recovery: PathBuf,
    generation: String,
    boot_id: String,
    control_fd: OwnedFd,
    recovery_fd: OwnedFd,
}

impl Paths {
    fn new(helper: &Path, control: &Path, recovery: &Path, generation: &str) -> Result<Self> {
        if process::getuid().as_raw() != 0
            || process::geteuid().as_raw() != 0
            || !hex_id(generation)
            || recovery.as_os_str() != Path::new(RECOVERY_ROOT).join(generation).as_os_str()
            || control == recovery
        {
            return Err(OwnerError::Configuration);
        }
        verify_helper(helper)?;
        let control_fd = private_directory(control)?;
        let recovery_fd = private_directory(recovery)?;
        // Separate library callers may not compete for the same control path.
        unix::flock(&control_fd, FlockOperation::NonBlockingLockExclusive)
            .map_err(|_| OwnerError::Configuration)?;
        for name in ["broker-ready.json", "helper-ready.json", "control.sock"] {
            require_absent(&control_fd, name)?;
        }
        for name in ["generation.json", "launch-intent.json"] {
            require_absent(&recovery_fd, name)?;
        }
        let boot_id = String::from_utf8(read_small(
            File::open("/proc/sys/kernel/random/boot_id").map_err(|_| OwnerError::Configuration)?,
            64,
        )?)
        .map_err(|_| OwnerError::Configuration)?
        .trim_end()
        .to_owned();
        Ok(Self {
            helper: helper.to_owned(),
            control: control.to_owned(),
            recovery: recovery.to_owned(),
            generation: generation.to_owned(),
            boot_id,
            control_fd,
            recovery_fd,
        })
    }

    fn command(&self, mode: &str, directory: &Path) -> Command {
        let mut command = Command::new(&self.helper);
        command
            .arg(mode)
            .arg(directory)
            .env_clear()
            .env("LC_ALL", "C")
            .current_dir("/");
        command
    }

    fn generation_record(&self) -> Result<Option<Generation>> {
        let Some(record) = read_private::<Generation>(&self.recovery_fd, "generation.json", 1024)?
        else {
            return Ok(None);
        };
        if record.version != 1
            || record.id != self.generation
            || record.boot_id != self.boot_id
            || record.unit != format!("stateknot-owner-{}.service", self.generation)
            || !hex_id(&record.invocation_id)
            || record.main_pid <= 1
            || i32::try_from(record.main_pid).is_err()
            || record.main_start_ticks == 0
            || record.pid_namespace[1] == 0
            || record.cgroup_identity[1] == 0
        {
            return Err(OwnerError::Startup);
        }
        Ok(Some(record))
    }

    fn same_recovery_directory(&self) -> bool {
        let Ok(metadata) = fs::symlink_metadata(&self.recovery) else {
            return false;
        };
        unix::fstat(&self.recovery_fd).is_ok_and(|opened| {
            metadata.is_dir() && metadata.dev() == opened.st_dev && metadata.ino() == opened.st_ino
        })
    }

    fn connect_when_ready(&self) -> Result<Option<Connected>> {
        let Some(ready) = read_private::<BrokerReady>(&self.control_fd, "broker-ready.json", 512)?
        else {
            return Ok(None);
        };
        let record = self.generation_record()?.ok_or(OwnerError::Startup)?;
        if ready.generation != self.generation || ready.invocation_id != record.invocation_id {
            return Err(OwnerError::Startup);
        }
        let metadata = unix::statat(
            &self.control_fd,
            "control.sock",
            unix::AtFlags::SYMLINK_NOFOLLOW,
        )
        .map_err(|_| OwnerError::Startup)?;
        if unix::FileType::from_raw_mode(metadata.st_mode) != unix::FileType::Socket
            || metadata.st_uid != 0
            || metadata.st_mode & 0o7777 != 0o600
        {
            return Err(OwnerError::Startup);
        }
        let socket = net::socket_with(
            AddressFamily::UNIX,
            SocketType::SEQPACKET,
            SocketFlags::CLOEXEC | SocketFlags::NONBLOCK,
            None,
        )
        .map_err(|_| OwnerError::Startup)?;
        let address = SocketAddrUnix::new(self.control.join("control.sock"))
            .map_err(|_| OwnerError::Startup)?;
        match net::connect(&socket, &address) {
            Ok(()) => Ok(Some(Connected { socket, record })),
            Err(Errno::AGAIN | Errno::INTR) => Ok(None),
            Err(_) => Err(OwnerError::Startup),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BrokerReady {
    generation: String,
    invocation_id: String,
}

struct Connected {
    socket: OwnedFd,
    record: Generation,
}

#[derive(Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct Generation {
    version: u8,
    boot_id: String,
    #[serde(rename = "generation")]
    id: String,
    unit: String,
    invocation_id: String,
    main_pid: u32,
    main_start_ticks: u64,
    pid_namespace: [u64; 2],
    cgroup_identity: [u64; 2],
}

fn hex_id(id: &str) -> bool {
    id.len() == 32
        && id
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

fn verify_tree(path: &Path) -> Result<()> {
    if !path.is_absolute()
        || fs::canonicalize(path)
            .map_err(|_| OwnerError::Configuration)?
            .as_os_str()
            != path.as_os_str()
    {
        return Err(OwnerError::Configuration);
    }
    for ancestor in path.ancestors() {
        let metadata = fs::symlink_metadata(ancestor).map_err(|_| OwnerError::Configuration)?;
        if !metadata.is_dir() || metadata.uid() != 0 || metadata.mode() & 0o022 != 0 {
            return Err(OwnerError::Configuration);
        }
    }
    Ok(())
}

fn verify_helper(path: &Path) -> Result<()> {
    verify_tree(path.parent().ok_or(OwnerError::Configuration)?)?;
    let fd = unix::open(
        path,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map_err(|_| OwnerError::Configuration)?;
    let metadata = unix::fstat(&fd).map_err(|_| OwnerError::Configuration)?;
    if unix::FileType::from_raw_mode(metadata.st_mode) != unix::FileType::RegularFile
        || metadata.st_uid != 0
        || metadata.st_mode & 0o022 != 0
        || metadata.st_mode & 0o111 == 0
    {
        return Err(OwnerError::Configuration);
    }
    let mut elf = [0; 4];
    File::from(fd)
        .read_exact(&mut elf)
        .map_err(|_| OwnerError::Configuration)?;
    if elf != *b"\x7fELF" {
        return Err(OwnerError::Configuration);
    }
    Ok(())
}

fn private_directory(path: &Path) -> Result<OwnedFd> {
    verify_tree(path)?;
    let fd = unix::open(
        path,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| OwnerError::Configuration)?;
    let metadata = unix::fstat(&fd).map_err(|_| OwnerError::Configuration)?;
    if metadata.st_uid != 0 || metadata.st_mode & 0o7777 != 0o700 {
        return Err(OwnerError::Configuration);
    }
    Ok(fd)
}

fn require_absent(directory: &OwnedFd, name: &str) -> Result<()> {
    match unix::statat(directory, name, unix::AtFlags::SYMLINK_NOFOLLOW) {
        Err(Errno::NOENT) => Ok(()),
        _ => Err(OwnerError::Configuration),
    }
}

fn read_small(file: File, limit: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    file.take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| OwnerError::Startup)?;
    if u64::try_from(bytes.len()).map_err(|_| OwnerError::Startup)? > limit {
        return Err(OwnerError::Startup);
    }
    Ok(bytes)
}

fn read_private<T: DeserializeOwned>(
    directory: &OwnedFd,
    name: &str,
    limit: u64,
) -> Result<Option<T>> {
    let fd = match unix::openat(
        directory,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(Errno::NOENT) => return Ok(None),
        Err(_) => return Err(OwnerError::Startup),
    };
    let metadata = unix::fstat(&fd).map_err(|_| OwnerError::Startup)?;
    if unix::FileType::from_raw_mode(metadata.st_mode) != unix::FileType::RegularFile
        || metadata.st_uid != 0
        || metadata.st_mode & 0o7777 != 0o600
    {
        return Err(OwnerError::Startup);
    }
    if name == "broker-ready.json" && metadata.st_nlink == 2 {
        // The broker publishes readiness by link + unlink of its temp name.
        return Ok(None);
    }
    if metadata.st_nlink != 1 {
        return Err(OwnerError::Startup);
    }
    let bytes = read_small(File::from(fd), limit)?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| OwnerError::Startup)
}

fn run_owner(
    paths: &Paths,
    shared: &Shared,
    mut launcher: Child,
    ready: oneshot::Sender<Result<()>>,
    done: oneshot::Sender<Result<()>>,
    started: Instant,
) {
    let startup_end = started + STARTUP;
    let connection = start(paths, shared, &mut launcher, startup_end);
    let sent_go = if let Ok(connected) = &connection {
        shared.state.store(READY, Ordering::Release);
        let _ = ready.send(Ok(()));
        own(
            &connected.socket,
            paths,
            shared,
            &mut launcher,
            startup_end,
            started + OWNER_LIMIT,
        )
    } else {
        let _ = ready.send(connection.as_ref().map(|_| ()).map_err(|error| *error));
        false
    };
    shared.state.store(RETIRING, Ordering::Release);
    shared.stop.cancel();
    if sent_go {
        if let Ok(connected) = &connection {
            let _ = packet(&connected.socket, "stop", &paths.generation);
        }
    }
    // Drop the control FD before waiting. EOF is also an owner stop request.
    let expected = connection.ok().map(|connected| connected.record);

    let _ = wait_child(&mut launcher, Instant::now() + GRACE);
    let (verified, pending) = recover(paths, expected.as_ref());
    let reaped = reap_launcher(&mut launcher);
    let _ = done.send(if verified && reaped {
        Ok(())
    } else {
        Err(OwnerError::Recovery)
    });

    // A stuck direct child remains owned even after reporting pending cleanup.
    // Poll both so a stuck launcher cannot delay collecting the verifier.
    reap_pending([(!reaped).then_some(launcher), pending]);
}

fn start(
    paths: &Paths,
    shared: &Shared,
    launcher: &mut Child,
    deadline: Instant,
) -> Result<Connected> {
    loop {
        if shared.stop.is_cancelled() {
            return Err(OwnerError::Stopped);
        }
        if Instant::now() >= deadline
            || launcher
                .try_wait()
                .map_err(|_| OwnerError::Launch)?
                .is_some()
        {
            return Err(OwnerError::Startup);
        }
        if let Some(connection) = paths.connect_when_ready()? {
            if shared.stop.is_cancelled() || Instant::now() >= deadline {
                return Err(OwnerError::Startup);
            }
            return Ok(connection);
        }
        thread::park_timeout(POLL);
    }
}

fn own(
    socket: &OwnedFd,
    paths: &Paths,
    shared: &Shared,
    launcher: &mut Child,
    startup_end: Instant,
    end: Instant,
) -> bool {
    let mut sent_go = false;
    let mut renew = Instant::now();
    while !shared.stop.is_cancelled() && matches!(launcher.try_wait(), Ok(None)) {
        let now = Instant::now();
        if now >= end || (!sent_go && now >= startup_end) {
            break;
        }
        let mut peek = [0; 1];
        if !matches!(
            net::recv(socket, &mut peek, RecvFlags::TRUNC),
            Err(Errno::AGAIN | Errno::INTR)
        ) {
            break; // This control channel has no server messages.
        }
        if shared.stop.is_cancelled() {
            break;
        }
        if !sent_go && shared.state.load(Ordering::Acquire) == GO {
            if Instant::now() >= startup_end || packet(socket, "go", &paths.generation).is_err() {
                break;
            }
            sent_go = true;
            renew = Instant::now() + RENEW;
        } else if sent_go && Instant::now() >= renew {
            if packet(socket, "renew", &paths.generation).is_err() {
                break;
            }
            renew = Instant::now() + RENEW;
        }
        thread::park_timeout(POLL);
    }
    sent_go
}

fn packet(socket: &OwnedFd, operation: &str, generation: &str) -> Result<()> {
    let message = format!("{{\"op\":\"{operation}\",\"generation\":\"{generation}\"}}");
    let bytes = message.as_bytes();
    match net::send(socket, bytes, SendFlags::NOSIGNAL) {
        Ok(sent) if sent == bytes.len() => Ok(()),
        _ => Err(OwnerError::Stopped),
    }
}

fn wait_child(child: &mut Child, deadline: Instant) -> std::io::Result<Option<ExitStatus>> {
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(Some(status));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(None);
        }
        thread::sleep(POLL.min(remaining));
    }
}

fn recover(paths: &Paths, expected: Option<&Generation>) -> (bool, Option<Child>) {
    let Ok(Some(record)) = paths.generation_record() else {
        // An intent alone is never sufficient to report clean retirement.
        return (false, None);
    };
    if expected.is_some_and(|expected| *expected != record)
        || !paths.same_recovery_directory()
        || verify_helper(&paths.helper).is_err()
    {
        return (false, None);
    }
    let mut command = paths.command("recover-generation", &paths.recovery);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let Ok(mut child) = command.spawn() else {
        return (false, None);
    };
    if let Ok(Some(status)) = wait_child(&mut child, Instant::now() + RECOVERY_LIMIT) {
        let verified = status.success()
            && paths.same_recovery_directory()
            && matches!(paths.generation_record(), Ok(Some(actual)) if actual == record);
        return (verified, None);
    }
    if reap_launcher(&mut child) {
        (false, None)
    } else {
        (false, Some(child))
    }
}

fn reap_launcher(child: &mut Child) -> bool {
    if matches!(child.try_wait(), Ok(Some(_))) {
        return true;
    }
    let _ = child.kill();
    matches!(wait_child(child, Instant::now() + GRACE), Ok(Some(_)))
}

fn reap_pending(mut children: [Option<Child>; 2]) {
    while children.iter().any(Option::is_some) {
        for child in &mut children {
            if let Some(process) = child.as_mut() {
                if !matches!(process.try_wait(), Ok(None)) {
                    *child = None;
                }
            }
        }
        if children.iter().any(Option::is_some) {
            thread::sleep(Duration::from_millis(100));
        }
    }
}
