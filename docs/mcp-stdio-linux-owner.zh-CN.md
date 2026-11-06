<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 实验性 MCP stdio Linux 所有者

该实验性运维组件提供单个进程代际的命名空间 init 和宿主恢复命令。
完整的 systemd 单元配置见 [Linux 验收控制器](../.github/scripts/qualify-stdio-linux-owner.py)。

```console
cargo +1.88.0 build --locked -p stateknot-integrations \
  --bin stateknot-mcp-stdio-owner \
  --features experimental-mcp-stdio-linux-owner
```

## 准备代际

将所有者和固定的 `/usr/bin/setpriv`、`/usr/bin/systemctl` 安装为由 root 拥有的
ELF 程序，路径只允许 root 修改。以私有 PID 1 启动
`stateknot-mcp-stdio-owner namespace-init <代际目录>`，将其置于
`/system.slice/stateknot-owner-<generation>.service` cgroup。每次使用新的代际和通过
验收的 systemd 配置，其中包括发送 `SIGKILL` 的 5 秒看门狗、`Restart=no` 和
`CollectMode=inactive`。

规范化后的代际目录和独立的恢复目录都必须由 root 拥有，权限为 0700。恢复目录放在
持久存储上，例如 `/var/lib/stateknot/mcp-stdio/<generation>`。使用规范化的绝对路径，
各级父目录由 root 拥有且只允许 root 写入。恢复记录拒绝 tmpfs 和 ramfs。代际目录中由
root 拥有且权限为 0600 的 `config.json` 包含 `generation`（32 个小写十六进制字符）、
`boot_id`、`host_namespace`（设备号和 inode）、`target_uid`、`target_gid` 和
`recovery_directory`。目标 UID、GID 必须为非 root。

等 systemd 报告单元已激活且处于运行状态后，在系统管理器的 PID 命名空间中以 root
身份执行以下宿主命令。命令成功后才发送 GO。

```console
stateknot-mcp-stdio-owner record-generation "$generation_directory"
```

命令核验正在运行的 systemd invocation 和命名空间 init，再在恢复目录原子发布并同步
带版本的 `generation.json`。文件由 root 拥有，权限为 0600。记录将 boot 和代际绑定到
真实单元、invocation ID、宿主 PID 和进程启动时间、PID 命名空间，以及 cgroup 的
设备号和 inode。重复执行时，身份完全一致才能复用记录。出现冲突时命令失败。

代理只打开一次私有 `control.sock`，发送绑定代际的 `go`、`renew` 和 `stop`。
所有者只接受一次 GO。接受前会核对已同步的记录和自身身份，并复查截止时间。证据
缺失、无效或不匹配时，目标无法启动。启动器降权后读取 4 字节大端长度和最多 32 KiB 的 JSON，
其中以 Unix 字节表示可执行文件、工作目录、参数和显式环境。后续 stdin 字节归应用
读取。启动器用 `execve` 原样执行，启动数据不会写入代际记录。

## 恢复已有记录的代际

在同一次宿主启动期间，于系统管理器的 PID 命名空间中以 root 身份执行以下命令。

```console
stateknot-mcp-stdio-owner recover-generation "$recovery_directory"
```

命令结合持久记录和当前 systemd 证据。若记录中的命名空间 init 仍存活且身份匹配，
命令通过该进程的 pidfd 发送 `SIGKILL`。只有取得同一 invocation 的退出证据、确认
原 init 已被回收且原 cgroup 已清空或移除，命令才成功。核实恢复前保留失败的单元和
记录。命令会保留不可变记录。boot 变化、身份变化或管理器证据缺失时，命令失败。

恢复范围是本次宿主启动期间已有提交记录的代际。从创建单元到发布记录的窗口由 GO
检查阻止目标启动，该组件尚无覆盖此窗口的持久启动意图记录。辅助程序可通过终止记录
报告停止原因。管道 EOF 仍由运行中的控制器单独观测。恢复无法重建丢失的管道或
Tool 结果。公开 MCP Session 集成和独立消费者验收仍需单独交付。
