<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Experimental Linux owner for MCP stdio

This experimental operator component launches and recovers one owned MCP
stdio process generation under the host systemd manager. The
[Linux qualification controller](../.github/scripts/qualify-stdio-linux-owner.py)
exercises the launch, control, and recovery paths.

```console
cargo +1.88.0 build --locked -p stateknot-integrations \
  --bin stateknot-mcp-stdio-owner \
  --features experimental-mcp-stdio-linux-owner
```

## Launch a generation

Install the owner and the fixed `/usr/bin/systemd-run`, `/usr/bin/systemctl`,
and `/usr/bin/setpriv` as root-owned ELF executables under paths that only
root can modify. The controller runs as root in the system manager's PID
namespace on a cgroup v2 host.

For each fresh generation, provision a private control directory and the fixed
recovery directory `/var/lib/stateknot/mcp-stdio/<generation>`. Both must be
root-owned and mode 0700. Use separate canonical absolute paths with
root-owned ancestors that only root can write. The control path must be UTF-8
and fit the Unix socket address limit. The recovery directory must be outside
the control directory tree and on persistent storage. The journal rejects
tmpfs and ramfs.

Write root-owned mode 0600 `config.json` in the control directory. It contains
`generation` (32 lowercase hexadecimal characters), `boot_id` (from
`/proc/sys/kernel/random/boot_id`), `host_namespace` (device and inode of
`/proc/1/ns/pid`), the non-root `target_uid` and `target_gid`, and
`recovery_directory` (the fixed generation path above). Use a fresh control
directory without existing socket or readiness files.

The controller starts the following command as a child process with three
distinct FIFO pipes. The child receives the read end on stdin and the write
ends on stdout and stderr. Keep the controller's ends and the child handle,
and drain stdout and stderr separately. Stderr can carry launch diagnostics.

```console
stateknot-mcp-stdio-owner launch-generation "$control_directory"
```

Before requesting the unit, the broker atomically publishes and syncs a
root-owned mode 0600 `launch-intent.json` in the recovery directory. The
intent reserves the generation for one launch. The broker uses the fixed
systemd profile with private PID 1, a five-second `SIGKILL` watchdog,
`Restart=no`, and `CollectMode=inactive`, and passes the three streams through
`systemd-run --wait --pipe`.

The broker verifies the live unit and its `InvocationID`, the namespace
init's exact executable and command line, and the manager's monotonic start
time for this launch. It publishes and syncs a versioned root-owned mode 0600
`generation.json`, binding the boot and generation to the actual unit,
invocation, host PID and start time, PID namespace, and cgroup device and
inode. It then publishes `broker-ready.json` in the control directory and
continues waiting for `systemd-run`.

After `broker-ready.json` appears, connect once to the private Unix
`SOCK_SEQPACKET` socket `control.sock`. Send JSON packets with `op` and
`generation`. Send `go` once, renew with `renew` while the target runs, and
send `stop` to end the generation. The namespace init checks the ready
marker, the synced record, and the deadline before releasing the target.
Missing or mismatched evidence prevents launch.

The child launcher drops privileges before reading a four-byte big-endian
length and up to 32 KiB of JSON on stdin. The JSON describes the executable,
working directory, arguments, and explicit environment as Unix bytes.
Subsequent stdin bytes belong to the application. The launcher calls
`execve` with literal values and keeps the launch prelude out of the
generation record.

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

An intent without `generation.json` reserves the generation and gives no
authority to signal a process or certify retirement. Recovery fails closed,
and another launch with the same generation fails even with a fresh control
directory. Keep the intent. If systemd admitted the unit, the pre-GO startup
deadline and watchdog bound its lifetime. No target can start without the
record and ready marker.

The helper's terminal record reports its stop reason when available. The
live controller observes pipe EOF and joins its launcher separately.
Recovery covers process retirement. Tool results come from the live call.

## Use the experimental Rust Session

Enable `experimental-mcp-stdio-linux-owner` in `stateknot-integrations` on
Linux. The [public API fixture](../crates/stateknot-integrations/tests/mcp_stdio_session.rs)
uses the installed launcher and the pre-provisioned generation described above.
Build `McpStdioConfig` with an explicit command, expected server identity,
reviewed descriptor pins, and a local schema registry. Construct an
`McpStdioSession`, await `start()`, and use `client()` to access the admitted
catalog and call an approved Tool. Await `shutdown()` for retirement and EOF.

This profile uses MCP 2026-07-28, one active call, and complete Tool results.
Use a separate Session per trusted local security scope. A call failure reports
whether a write was attempted. Reconcile uncertain external effects before
dispatching another write. If shutdown reports pending cleanup, retain the
generation and use the recovery procedure above.

## Standalone qualification

`record-generation <control-directory>` is for independently started
qualification units. It requires a recovery directory outside
`/var/lib/stateknot/mcp-stdio`. For a managed launch, the process that claimed
the intent publishes the generation record.
