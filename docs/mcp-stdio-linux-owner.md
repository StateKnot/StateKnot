<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Experimental Linux owner for MCP stdio

This operator component is the namespace init for one owned process generation.
It requires a reviewed systemd deployment and an outer broker. The hosted
qualification workflow defines the unit profile used to test it.

```console
cargo +1.88.0 build --locked -p stateknot-integrations \
  --bin stateknot-mcp-stdio-owner \
  --features experimental-mcp-stdio-linux-owner
```

The broker installs the owner and fixed `/usr/bin/setpriv` as root-owned ELF
executables under paths that only root can modify. It starts
`stateknot-mcp-stdio-owner namespace-init <generation-directory>` as private
PID 1 in the cgroup `/system.slice/stateknot-owner-<generation>.service`.
It configures a five-second systemd watchdog. The canonical generation
directory must be root-owned and mode 0700. Its mode 0600 `config.json`
contains the generation, current boot ID, host PID namespace identity, and
non-root target UID and GID.

The broker opens the private `control.sock` once. It sends generation-bound
`go`, `renew`, and `stop` packets. The owner accepts one `go`. After
dropping privileges, the launcher reads a four-byte big-endian length followed
by at most 32 KiB of JSON describing the executable, working directory,
arguments, and explicit environment as Unix bytes. Subsequent stdin bytes
belong to the application. The launcher uses literal `execve` and keeps the
launch prelude out of the generation record.

Use the outer broker to verify the original process identities have retired,
the cgroup is empty, and the pipes have reached EOF. The helper's terminal
record reports why it stopped. Deployment qualification still requires the
broker's authenticated startup and crash recovery, the public MCP session and
an independent consumer.
