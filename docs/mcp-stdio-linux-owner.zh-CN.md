<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 实验性 MCP stdio Linux 所有者

该运维组件作为单个进程代际的命名空间 init。它依赖经过审查的 systemd 部署和外层
代理。托管验收工作流定义了测试使用的单元配置。

```console
cargo +1.88.0 build --locked -p stateknot-integrations \
  --bin stateknot-mcp-stdio-owner \
  --features experimental-mcp-stdio-linux-owner
```

代理将所有者和固定的 `/usr/bin/setpriv` 安装为由 root 拥有的 ELF 程序，路径只允许
root 修改。它以私有 PID 1 启动
`stateknot-mcp-stdio-owner namespace-init <代际目录>`，将其置于
`/system.slice/stateknot-owner-<generation>.service` cgroup，并配置 5 秒 systemd
看门狗。规范化后的代际目录由 root 拥有，权限为 0700。权限为 0600 的
`config.json` 保存代际、当前 boot ID、宿主 PID 命名空间身份和目标的非 root UID、GID。

代理只打开一次私有 `control.sock`，发送绑定代际的 `go`、`renew` 和 `stop`。
所有者只接受一次 `go`。启动器降权后读取 4 字节大端长度和最多 32 KiB 的 JSON，
其中以 Unix 字节表示可执行文件、工作目录、参数和显式环境。后续 stdin 字节归应用
读取。启动器用 `execve` 原样执行，启动数据不会写入代际记录。

外层代理负责核验原进程身份已回收、cgroup 已清空及管道已到 EOF。辅助程序的终止
记录说明停止原因。部署验收还需要代理的认证启动和崩溃恢复、公开 MCP Session，
以及独立消费者实证。
