<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Experimental Linux owner for MCP stdio

This experimental operator component provides the namespace init and the
host-side recovery commands for one owned process generation. Use the
[Linux qualification controller](../.github/scripts/qualify-stdio-linux-owner.py)
for the full systemd unit profile.

```console
cargo +1.88.0 build --locked -p stateknot-integrations \
  --bin stateknot-mcp-stdio-owner \
  --features experimental-mcp-stdio-linux-owner
```

## Prepare a generation

Install the owner and the fixed `/usr/bin/setpriv` and `/usr/bin/systemctl`
as root-owned ELF executables under paths that only root can modify. Start
`stateknot-mcp-stdio-owner namespace-init <generation-directory>` as private
PID 1 in the cgroup `/system.slice/stateknot-owner-<generation>.service`.
Use a fresh generation and the qualified systemd profile with a five-second
watchdog that sends `SIGKILL`, `Restart=no`, and `CollectMode=inactive`.

The canonical generation directory and a separate recovery directory must
both be root-owned and mode 0700. Place the recovery directory on persistent
storage such as `/var/lib/stateknot/mcp-stdio/<generation>`. Use a canonical
absolute path with root-owned ancestors that only root can write. The
recovery journal rejects tmpfs and ramfs. The generation directory's root-owned
mode 0600 `config.json` contains `generation` (32 lowercase hexadecimal
characters), `boot_id`, `host_namespace` (device and inode), `target_uid`,
`target_gid`, and `recovery_directory`. The target UID and GID must be non-root.

Once systemd reports the unit active and running, run the host-side command as
root in the system manager's PID namespace. Wait for it to succeed before
sending GO.

```console
stateknot-mcp-stdio-owner record-generation "$generation_directory"
```

The command verifies the live systemd invocation and namespace init, then
atomically publishes and syncs a versioned, root-owned mode 0600
`generation.json` in the recovery directory. The record binds the boot and
generation to the actual unit, invocation ID, host PID and start time, PID
namespace, and cgroup device and inode. A repeat with the same observations
can reuse that record. A conflicting record fails closed.

The broker opens the private `control.sock` once. It sends generation-bound
`go`, `renew`, and `stop` packets. Before accepting its one GO, the owner
verifies the synced record against its own identity and checks its deadline. Missing,
invalid, or mismatched evidence prevents the target from starting. After
dropping privileges, the launcher reads a four-byte big-endian length followed
by at most 32 KiB of JSON describing the executable, working directory,
arguments, and explicit environment as Unix bytes. Subsequent stdin bytes
belong to the application. The launcher uses literal `execve` and keeps the
launch prelude out of the generation record.

## Recover a recorded generation

On the same boot, run as root in the system manager's PID namespace.

```console
stateknot-mcp-stdio-owner recover-generation "$recovery_directory"
```

The command uses the retained record and current systemd evidence. If the
recorded namespace init is still live and its identity matches, it sends
`SIGKILL` through a pidfd for that process. Success requires the same
systemd invocation's exit evidence, reaping of the original init, and an
empty or removed original cgroup. Preserve the failed unit and its record
until recovery is verified. The command retains the immutable record. A
different boot, changed identity, or missing manager evidence yields failure.

Recovery covers generations with a committed record on the current boot.
The interval between unit creation and record publication is protected by
the GO gate and has no durable launch intent in this component. The helper's
terminal record reports its stop reason when available. The live controller
must observe pipe EOF separately. Recovery cannot reconstruct lost pipe or
Tool results. Public MCP Session integration and independent consumer
qualification remain separate delivery work.
