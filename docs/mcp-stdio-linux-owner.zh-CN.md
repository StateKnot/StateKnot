<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 实验性 MCP stdio Linux 所有者

该实验性运维组件在宿主 systemd 下启动和恢复单个 MCP stdio 进程代际。
[Linux 验收控制器](../.github/scripts/qualify-stdio-linux-owner.py)覆盖启动、控制和恢复路径。

```console
cargo +1.88.0 build --locked -p stateknot-integrations \
  --bin stateknot-mcp-stdio-owner \
  --features experimental-mcp-stdio-linux-owner
```

## 启动代际

将所有者和固定的 `/usr/bin/systemd-run`、`/usr/bin/systemctl`、`/usr/bin/setpriv`
安装为由 root 拥有的 ELF 程序，路径只允许 root 修改。控制器在使用 cgroup v2 的宿主上，
以 root 身份运行于 systemd 管理器的 PID 命名空间。

为每个新代际准备独立的私有控制目录和固定恢复目录
`/var/lib/stateknot/mcp-stdio/<generation>`。两个目录都由 root 拥有，权限为 0700。
使用规范化的绝对路径，各级父目录由 root 拥有且只允许 root 写入。控制目录路径使用
UTF-8，并满足 Unix socket 地址长度限制。恢复目录须位于控制目录树之外的持久存储上。
恢复日志拒绝 tmpfs 和 ramfs。

在控制目录写入由 root 拥有且权限为 0600 的 `config.json`。字段包括 `generation`
（32 个小写十六进制字符）、`boot_id`（取自 `/proc/sys/kernel/random/boot_id`）、
`host_namespace`（`/proc/1/ns/pid` 的设备号和 inode）、非 root 的 `target_uid` 和
`target_gid`，以及 `recovery_directory`（上述固定代际路径）。使用未留有 socket 或就绪
标记的新控制目录。

控制器用三个各自独立的 FIFO 管道启动以下子进程。子进程获得 stdin 的读端和 stdout、
stderr 的写端。控制器保留自己的管道端和子进程句柄，并分别读取 stdout 和 stderr。
stderr 可能包含启动诊断。

```console
stateknot-mcp-stdio-owner launch-generation "$control_directory"
```

在请求创建单元前，代理在恢复目录原子发布并同步由 root 拥有、权限为 0600 的
`launch-intent.json`。这份意图为代际保留一次启动资格。代理使用固定的 systemd 配置，
包括私有 PID 1、发送 `SIGKILL` 的 5 秒看门狗、`Restart=no` 和 `CollectMode=inactive`，
并用 `systemd-run --wait --pipe` 传递三个标准流。

代理核验运行中的单元及其 `InvocationID`、命名空间 init 的精确可执行文件和命令行，
以及 systemd 为本次启动记录的单调时钟启动时间。随后发布并同步带版本的
`generation.json`。文件由 root 拥有，权限为 0600，将 boot 和代际绑定到真实单元、
invocation、宿主 PID 和进程启动时间、PID 命名空间及 cgroup 的设备号和 inode。
最后在控制目录发布 `broker-ready.json`，并继续等待 `systemd-run` 退出。

看到 `broker-ready.json` 后，控制器连接一次私有 Unix `SOCK_SEQPACKET` socket
`control.sock`。每个 JSON 数据包包含 `op` 和 `generation`。先发送一次 `go`，目标
运行期间用 `renew` 续约，结束时发送 `stop`。命名空间 init 在放行目标前核验就绪标记
和已同步的记录，并复查截止时间。证据缺失或不匹配时，目标无法启动。

子进程启动器先降权，再从 stdin 读取 4 字节大端长度及最多 32 KiB 的 JSON。其中以
Unix 字节表示可执行文件、工作目录、参数和显式环境。后续 stdin 字节归应用读取。
启动器用 `execve` 原样执行，启动数据不会写入代际记录。

## 恢复已有记录的代际

在同一次宿主启动期间，于系统管理器的 PID 命名空间中以 root 身份执行以下命令。

```console
stateknot-mcp-stdio-owner recover-generation "$recovery_directory"
```

命令结合持久记录和当前 systemd 证据。若记录中的命名空间 init 仍存活且身份匹配，
命令通过该进程的 pidfd 发送 `SIGKILL`。只有取得同一 invocation 的退出证据、确认
原 init 已被回收且原 cgroup 已清空或移除，命令才成功。核实恢复前保留失败的单元和
记录。命令会保留不可变记录。boot 变化、身份变化或管理器证据缺失时，命令失败。

如果只有启动意图而没有 `generation.json`，该代际已被保留，但意图无法授权发送
信号或证明进程已清退。恢复命令失败。同一代际即使用全新的控制目录也无法重新启动，
应保留意图。如果 systemd 已接收启动请求，GO 前的启动截止时间和看门狗会限制单元
的存活时间。缺少记录或就绪标记时，目标无法启动。

辅助程序可通过终止记录报告停止原因。运行中的控制器仍须单独观测管道 EOF 并等待
其启动子进程退出。恢复无法重建丢失的管道或 Tool 结果。公开 MCP Session 集成和独立
消费者验收仍需单独交付。

## 独立验收

`record-generation <控制目录>` 用于独立启动的验收单元，要求恢复目录位于
`/var/lib/stateknot/mcp-stdio` 之外。托管启动由成功写入意图的进程发布代际记录。
