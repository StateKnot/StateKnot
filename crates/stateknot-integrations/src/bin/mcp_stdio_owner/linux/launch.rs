// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::BTreeSet,
    env,
    ffi::{CString, OsStr},
    fs, io,
    os::unix::ffi::OsStrExt,
    path::Path,
    time::Duration,
};

use nix::{errno::Errno, unistd};
use rustix::{
    fs::{FileType, OFlags, fcntl_getfl, fcntl_setfl, fstat},
    process::{self, Pid},
};
use serde::Deserialize;
use zeroize::Zeroizing;

use super::{Fault, Result, check_startup, current_namespace, poll_input, read_bounded, target_id};

const MAX_LAUNCH_BYTES: usize = 32 * 1024;
const MAX_ENTRIES: usize = 128;
const MAX_PATH_BYTES: usize = 4096;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Launch {
    executable: Vec<u8>,
    cwd: Vec<u8>,
    arguments: Vec<Vec<u8>>,
    environment: Vec<Entry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    name: Vec<u8>,
    value: Vec<u8>,
}

pub(super) fn exec_target(
    uid: u32,
    gid: u32,
    namespace: [u64; 2],
    deadline: Duration,
) -> Result<()> {
    verify_privilege_drop(uid, gid, namespace)?;
    check_startup(deadline)?;
    let stdin = io::stdin();
    let info = fstat(&stdin).map_err(|_| Fault::Profile)?;
    if FileType::from_raw_mode(info.st_mode) != FileType::Fifo {
        return Err(Fault::Profile);
    }
    let flags = fcntl_getfl(&stdin).map_err(|_| Fault::Launch)?;
    fcntl_setfl(&stdin, flags | OFlags::NONBLOCK).map_err(|_| Fault::Launch)?;
    let launch = read_prelude(&stdin, deadline);
    // Do not transfer nonblocking mode to the target, even if read failed.
    fcntl_setfl(&stdin, flags).map_err(|_| Fault::Launch)?;
    launch?.exec(deadline)
}

fn read_prelude(stdin: &impl std::os::fd::AsFd, deadline: Duration) -> Result<Launch> {
    let mut length = [0; 4];
    read_exact(stdin, &mut length, deadline)?;
    let length = usize::try_from(u32::from_be_bytes(length)).map_err(|_| Fault::Launch)?;
    if length == 0 || length > MAX_LAUNCH_BYTES {
        return Err(Fault::Launch);
    }
    let mut raw = Zeroizing::new(vec![0; length]);
    read_exact(stdin, &mut raw, deadline)?;
    let decoded = serde_json::from_slice(&raw);
    check_startup(deadline)?;
    decoded.map_err(|_| Fault::Launch)
}

fn verify_privilege_drop(uid: u32, gid: u32, namespace: [u64; 2]) -> Result<()> {
    if !target_id(uid)
        || !target_id(gid)
        || process::getpid() == Pid::INIT
        || process::getppid() != Some(Pid::INIT)
        || current_namespace()? != namespace
    {
        return Err(Fault::Profile);
    }
    let status =
        read_bounded(Path::new("/proc/self/status"), 16 * 1024).map_err(|_| Fault::Profile)?;
    let status = std::str::from_utf8(&status).map_err(|_| Fault::Profile)?;
    for (name, expected) in [("Uid", uid), ("Gid", gid)] {
        let mut ids = status_value(status, name)?.split_ascii_whitespace();
        if (0..4).any(|_| ids.next().and_then(|id| id.parse::<u32>().ok()) != Some(expected))
            || ids.next().is_some()
        {
            return Err(Fault::Profile);
        }
    }
    if !status_value(status, "Groups")?.is_empty()
        || status_value(status, "NoNewPrivs")? != "1"
        || env::var_os("NOTIFY_SOCKET").is_some()
    {
        return Err(Fault::Profile);
    }
    for cap in ["CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"] {
        if u64::from_str_radix(status_value(status, cap)?, 16) != Ok(0) {
            return Err(Fault::Profile);
        }
    }
    verify_inherited_fds()
}

fn status_value<'a>(status: &'a str, name: &str) -> Result<&'a str> {
    status
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find_map(|(field, value)| (field == name).then_some(value.trim()))
        .ok_or(Fault::Profile)
}

fn verify_inherited_fds() -> Result<()> {
    // The directory used to inspect fdinfo is itself opened close-on-exec.
    // Reject any other descriptor that would survive target exec.
    for (index, entry) in fs::read_dir("/proc/self/fd")
        .map_err(|_| Fault::Profile)?
        .enumerate()
    {
        if index > 32 {
            return Err(Fault::Profile);
        }
        let entry = entry.map_err(|_| Fault::Profile)?;
        let name = entry.file_name();
        let fd: u32 = name
            .to_str()
            .and_then(|value| value.parse().ok())
            .ok_or(Fault::Profile)?;
        if fd <= 2 {
            continue;
        }
        let path = Path::new("/proc/self/fdinfo").join(name);
        let info = read_bounded(&path, 4096).map_err(|_| Fault::Profile)?;
        let flags = status_value(
            std::str::from_utf8(&info).map_err(|_| Fault::Profile)?,
            "flags",
        )?;
        let flags = u32::from_str_radix(flags, 8).map_err(|_| Fault::Profile)?;
        if flags & 0o2_000_000 == 0 {
            return Err(Fault::Profile);
        }
    }
    Ok(())
}

fn read_exact(fd: &impl std::os::fd::AsFd, mut bytes: &mut [u8], deadline: Duration) -> Result<()> {
    // Read exactly the prelude bytes. The MCP server sees all following
    // bytes on the original stdin, including an already queued first frame.
    while !bytes.is_empty() {
        check_startup(deadline)?;
        let ready = poll_input(fd, deadline).map_err(|_| Fault::Launch)?;
        check_startup(deadline)?;
        if !ready {
            continue;
        }
        let read = unistd::read(fd, bytes);
        check_startup(deadline)?;
        match read {
            Ok(0) => return Err(Fault::Launch),
            Ok(length) => bytes = &mut bytes[length..],
            Err(Errno::EINTR | Errno::EAGAIN) => {}
            Err(_) => return Err(Fault::Launch),
        }
    }
    Ok(())
}

impl Launch {
    fn exec(self, deadline: Duration) -> Result<()> {
        if self.arguments.len() > MAX_ENTRIES || self.environment.len() > MAX_ENTRIES {
            return Err(Fault::Launch);
        }
        let executable = absolute_cstring(self.executable)?;
        let cwd = absolute_cstring(self.cwd)?;
        let mut args = Vec::with_capacity(self.arguments.len() + 1);
        args.push(executable.clone());
        for argument in self.arguments {
            args.push(CString::new(argument).map_err(|_| Fault::Launch)?);
        }
        let mut seen = BTreeSet::new();
        let mut environment = Vec::with_capacity(self.environment.len());
        for entry in &self.environment {
            if entry.name.is_empty()
                || entry.name.len() > 128
                || entry.name.contains(&b'=')
                || !seen.insert(entry.name.as_slice())
            {
                return Err(Fault::Launch);
            }
            let mut binding = entry.name.clone();
            binding.push(b'=');
            binding.extend_from_slice(&entry.value);
            environment.push(CString::new(binding).map_err(|_| Fault::Launch)?);
        }
        env::set_current_dir(OsStr::from_bytes(cwd.as_bytes())).map_err(|_| Fault::Launch)?;
        check_startup(deadline)?;
        // execve is literal. execvp can interpret ENOEXEC as an implicit shell.
        unistd::execve(&executable, &args, &environment)
            .map(|never| match never {})
            .map_err(|_| Fault::Launch)
    }
}

fn absolute_cstring(path: Vec<u8>) -> Result<CString> {
    if !path.starts_with(b"/") || path.len() > MAX_PATH_BYTES {
        return Err(Fault::Launch);
    }
    CString::new(path).map_err(|_| Fault::Launch)
}
