// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

//! Durable pre-GO identity. The outer broker supplies its verified host observations.
//! A journal record cannot by itself authorize signalling or prove retirement.

use std::{
    fs::File,
    io::{Read, Write},
    os::{fd::OwnedFd, unix::ffi::OsStrExt},
    path::{Component, Path, PathBuf},
};

use rustix::{
    fs::{
        AtFlags, CWD, FileType, Mode, OFlags, RenameFlags, fchmod, fstat, fstatfs, fsync, openat,
        renameat_with, statat, unlinkat,
    },
    io::Errno,
};
use serde::{Deserialize, Serialize};

type Result<T> = std::result::Result<T, JournalError>;

const FILE: &str = "generation.json";
const VERSION: u8 = 1;
const MAX_RECORD: u64 = 1024;
const DIRECTORY_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);

#[derive(Debug)]
pub(super) struct JournalError;

#[derive(Clone, Copy)]
pub(super) struct Observation<'a> {
    pub(super) boot_id: &'a str,
    pub(super) generation: &'a str,
    pub(super) unit: &'a str,
    pub(super) invocation_id: &'a str,
    pub(super) main_pid: u32,
    pub(super) main_start_ticks: u64,
    pub(super) pid_namespace: [u64; 2],
    pub(super) cgroup_identity: [u64; 2],
}

#[derive(Clone, Copy)]
pub(super) struct LocalIdentity<'a> {
    pub(super) boot_id: &'a str,
    pub(super) generation: &'a str,
    pub(super) invocation_id: &'a str,
    pub(super) main_start_ticks: u64,
    pub(super) pid_namespace: [u64; 2],
    pub(super) cgroup_identity: [u64; 2],
}

// Only validated, synced journal reads can construct a Record.
pub(super) struct Record(Wire);

#[derive(Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    version: u8,
    boot_id: String,
    generation: String,
    unit: String,
    invocation_id: String,
    main_pid: u32,
    main_start_ticks: u64,
    pid_namespace: [u64; 2],
    cgroup_identity: [u64; 2],
}

impl Wire {
    fn identity(&self) -> Observation<'_> {
        Observation {
            boot_id: &self.boot_id,
            generation: &self.generation,
            unit: &self.unit,
            invocation_id: &self.invocation_id,
            main_pid: self.main_pid,
            main_start_ticks: self.main_start_ticks,
            pid_namespace: self.pid_namespace,
            cgroup_identity: self.cgroup_identity,
        }
    }

    fn bytes(&self) -> Result<Vec<u8>> {
        let mut bytes = serde_json::to_vec(self).map_err(|_| JournalError)?;
        bytes.push(b'\n');
        Ok(bytes)
    }
}

impl Record {
    pub(super) fn identity(&self) -> Observation<'_> {
        self.0.identity()
    }

    pub(super) fn require(&self, current: LocalIdentity<'_>) -> Result<()> {
        let original = self.identity();
        // The namespace init sees PID 1, not the host-visible MainPID.
        // The same start ticks and kernel namespace identity bind that PID.
        if original.boot_id != current.boot_id
            || original.generation != current.generation
            || original.invocation_id != current.invocation_id
            || original.main_start_ticks != current.main_start_ticks
            || original.pid_namespace != current.pid_namespace
            || original.cgroup_identity != current.cgroup_identity
        {
            return Err(JournalError);
        }
        Ok(())
    }
}

pub(super) fn persist(directory: &Path, observed: Observation<'_>) -> Result<Record> {
    validate(&observed)?;
    let expected = Wire {
        version: VERSION,
        boot_id: observed.boot_id.to_owned(),
        generation: observed.generation.to_owned(),
        unit: observed.unit.to_owned(),
        invocation_id: observed.invocation_id.to_owned(),
        main_pid: observed.main_pid,
        main_start_ticks: observed.main_start_ticks,
        pid_namespace: observed.pid_namespace,
        cgroup_identity: observed.cgroup_identity,
    };
    let journal = Journal::open(directory)?;
    let recorded = if let Some(recorded) = journal.read()? {
        recorded
    } else {
        journal.publish(&expected)?;
        journal.read()?.ok_or(JournalError)?
    };
    if recorded.0 != expected {
        return Err(JournalError);
    }
    Ok(recorded)
}

pub(super) fn recover(directory: &Path) -> Result<Record> {
    Journal::open(directory)?.read()?.ok_or(JournalError)
}

pub(super) fn start_ticks(raw: &[u8]) -> Result<u64> {
    // The process comm field can itself contain parentheses and non-UTF-8.
    let end = raw
        .windows(2)
        .rposition(|bytes| bytes == b") ")
        .ok_or(JournalError)?;
    std::str::from_utf8(&raw[end + 2..])
        .map_err(|_| JournalError)?
        .split_ascii_whitespace()
        .nth(19) // Field 22. The suffix starts at field 3 (state).
        .and_then(|value| value.parse().ok())
        .filter(|ticks| *ticks > 0)
        .ok_or(JournalError)
}

fn validate(identity: &Observation<'_>) -> Result<()> {
    if !boot_id(identity.boot_id)
        || !lower_hex(identity.generation, 32)
        || !lower_hex(identity.invocation_id, 32)
        || identity.unit != format!("stateknot-owner-{}.service", identity.generation)
        || identity.main_pid <= 1
        || i32::try_from(identity.main_pid).is_err()
        || identity.main_start_ticks == 0
        || identity.pid_namespace[1] == 0
        || identity.cgroup_identity[1] == 0
    {
        return Err(JournalError);
    }
    Ok(())
}

fn lower_hex(value: &str, length: usize) -> bool {
    value.len() == length && value.bytes().all(is_lower_hex)
}

fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()
}

fn boot_id(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                is_lower_hex(byte)
            }
        })
}

struct Journal {
    directory: OwnedFd,
    parents: Vec<OwnedFd>,
}

impl Journal {
    fn open(path: &Path) -> Result<Self> {
        if path.as_os_str().as_bytes().len() > 4096 {
            return Err(JournalError);
        }
        let mut components = path.components();
        if components.next() != Some(Component::RootDir) {
            return Err(JournalError);
        }
        let mut actual = PathBuf::from("/");
        let mut directory =
            openat(CWD, "/", DIRECTORY_FLAGS, Mode::empty()).map_err(|_| JournalError)?;
        verify_directory(&directory)?;
        let mut parents = Vec::new();
        for component in components {
            let Component::Normal(part) = component else {
                return Err(JournalError);
            };
            if parents.len() == 64 {
                return Err(JournalError);
            }
            let child = openat(&directory, part, DIRECTORY_FLAGS, Mode::empty())
                .map_err(|_| JournalError)?;
            verify_directory(&child)?;
            actual.push(part);
            parents.push(directory);
            directory = child;
        }
        let stat = fstat(&directory).map_err(|_| JournalError)?;
        let filesystem = fstatfs(&directory).map_err(|_| JournalError)?;
        // Normalize the kernel fs magic across signed/unsigned statfs layouts.
        let magic = i128::from(filesystem.f_type) & 0xffff_ffff;
        if actual.as_os_str() != path.as_os_str()
            || parents.is_empty()
            || stat.st_mode & 0o7777 != 0o700
            || matches!(magic, 0x0102_1994 | 0x8584_58f6)
        // tmpfs, ramfs
        {
            return Err(JournalError);
        }
        Ok(Self { directory, parents })
    }

    fn read(&self) -> Result<Option<Record>> {
        let fd = match openat(
            &self.directory,
            FILE,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK,
            Mode::empty(),
        ) {
            Ok(fd) => fd,
            Err(Errno::NOENT) => return Ok(None),
            Err(_) => return Err(JournalError),
        };
        let before = fstat(&fd).map_err(|_| JournalError)?;
        let size = u64::try_from(before.st_size).map_err(|_| JournalError)?;
        if FileType::from_raw_mode(before.st_mode) != FileType::RegularFile
            || before.st_uid != 0
            || before.st_nlink != 1
            || before.st_mode & 0o7777 != 0o600
            || size == 0
            || size > MAX_RECORD
        {
            return Err(JournalError);
        }
        let file = File::from(fd);
        let mut bytes = Vec::new();
        (&file)
            .take(MAX_RECORD + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| JournalError)?;
        if u64::try_from(bytes.len()).map_err(|_| JournalError)? != size {
            return Err(JournalError);
        }
        let record: Wire = serde_json::from_slice(&bytes).map_err(|_| JournalError)?;
        validate(&record.identity())?;
        if record.version != VERSION || record.bytes()? != bytes {
            return Err(JournalError);
        }
        // Heal an interruption after rename and before the initial directory
        // sync. Sync bottom-up so freshly created ancestors survive as well.
        file.sync_all().map_err(|_| JournalError)?;
        fsync(&self.directory).map_err(|_| JournalError)?;
        for parent in self.parents.iter().rev() {
            fsync(parent).map_err(|_| JournalError)?;
        }
        let after =
            statat(&self.directory, FILE, AtFlags::SYMLINK_NOFOLLOW).map_err(|_| JournalError)?;
        if after.st_dev != before.st_dev
            || after.st_ino != before.st_ino
            || after.st_size != before.st_size
            || after.st_nlink != before.st_nlink
            || after.st_mode != before.st_mode
            || after.st_uid != before.st_uid
        {
            return Err(JournalError);
        }
        Ok(Some(Record(record)))
    }

    fn publish(&self, record: &Wire) -> Result<()> {
        let bytes = record.bytes()?;
        let mut nonce = [0; 16];
        File::open("/dev/urandom")
            .and_then(|mut source| source.read_exact(&mut nonce))
            .map_err(|_| JournalError)?;
        let pending = format!(".generation-{:032x}.pending", u128::from_be_bytes(nonce));
        let mode = Mode::RUSR | Mode::WUSR;
        let mut file = File::from(
            openat(
                &self.directory,
                pending.as_str(),
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                mode,
            )
            .map_err(|_| JournalError)?,
        );
        let result = self.publish_file(&mut file, mode, &pending, &bytes);
        if result.is_err() {
            // This name is unique to this attempted publication. Orphans
            // from a process crash are never read as launch authority.
            let _ = unlinkat(&self.directory, pending.as_str(), AtFlags::empty());
        }
        match result {
            Ok(()) | Err(Errno::EXIST) => Ok(()), // Read and compare the winner.
            Err(_) => Err(JournalError),
        }
    }

    fn publish_file(
        &self,
        file: &mut File,
        mode: Mode,
        pending: &str,
        bytes: &[u8],
    ) -> rustix::io::Result<()> {
        fchmod(&*file, mode)?;
        file.write_all(bytes).map_err(|_| Errno::IO)?;
        file.sync_all().map_err(|_| Errno::IO)?;
        renameat_with(
            &self.directory,
            pending,
            &self.directory,
            FILE,
            RenameFlags::NOREPLACE,
        )
    }
}

fn verify_directory(fd: &OwnedFd) -> Result<()> {
    let stat = fstat(fd).map_err(|_| JournalError)?;
    if stat.st_uid != 0 || stat.st_mode & 0o022 != 0 {
        return Err(JournalError);
    }
    Ok(())
}
