<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# Issue 140：MCP stdio 协议、资源边界与进程所有权研究

研究日期：2026-10-12（Asia/Shanghai）。本文件只记录规范、锁定源码事实及实现选择；未编写生产实现、未执行进程回收资格测试，不据此关闭 R1、Issue 140 或声明任何平台 Supported。

## 1. 需求与证据基线

[Issue 140](https://github.com/StateKnot/StateKnot/issues/140) 要求：宿主以可执行文件、字面参数、显式环境白名单启动；有界发现与调用；启动失败不公布工具；取消、超时和关闭释放完整进程树；工具错误与传输失败分开；不自动重试结果不明的写入；公开版本、平台、取消与孤儿恢复边界。2026-10-12 通过第一方 GitHub connector 读取：issue 为 open，正文 updated_at 为 `2026-10-02T10:25:44Z`，comments 为 0。

仓库基线 HEAD 为 `cb816a9cc8ec2c3820600c8f7afc1fc8b4b18f06`。`docs/production-milestones-2026-10-08.zh-CN.md:221-230` 对上述要求增加了畸形/超大帧、交错通知、握手阻塞、后代、取消和退出的验收，并要求外部消费者精确 pin、只声明实测平台。研究时现有 `McpClient` 仅实现现代 2026-07-28 HTTP；`Cargo.toml:45-54` 锁定 rmcp `=3.5.0`，未启用 `transport-child-process`。已有 `server` feature 会间接启用 `transport-async-rw`；此前将它也记为未启用有误，本句于 2026-10-15 按锁定包的 feature 定义勘正。这不表示宿主已实现有界 stdio 或完整进程所有权。

本机是 macOS `26.6.2 / 25G83`；此信息仅描述读源码的宿主，不是生命周期测试结果。没有创建凭据、安装软件、发送外部消息或修改主仓库其他文件。

## 2. 协议时代与启动规则

现代（2026-07-28 及以后）不使用 initialize 会话握手。每个请求独立携带 `params._meta`：protocolVersion 与 clientCapabilities 必需，clientInfo 建议提供；stdio 没有 HTTP header 层。server/discover 学习版本和能力，不能把一次 discovery 当成以后可省略 metadata 的授权。服务器身份是自报信息。[现代版本规则][modern-version]、[通用 metadata 规则][modern-basic]

现代 stdio 的 probe 规则如下。下表是规范行为，兼容模式须明确声明支持的 legacy 版本；仅支持一个现代 revision 也合法。[现代 stdio][modern-stdio]

| probe 结果 | 客户端规则 |
| --- | --- |
| DiscoverResult | 从 supportedVersions 选择双方实际支持的版本 |
| 可识别的现代错误，例如 `-32022` UnsupportedProtocolVersion | 保持现代时代；检查 advertised supported 并选择交集；不能回落 initialize |
| 其他错误或合理期限内无响应 | dual-era 可以回落 legacy initialize；不能只认 `-32601`，旧实现可回 `-32602` 等或沉默 |
| modern-only 遇到 legacy | 报可操作的版本/时代不支持；先 probe 可以避免 tools/call 被旧服务按不同语义处理 |

era 适用整个 server process 生命周期。换进程后应重新核实。现代 discovery 是能力检查，不是建立对话：规范建议不要把一个 agent task/thread/conversation 当成 stdio process 的自然生命边界；多个独立请求可以共用一个进程。[现代版本规则][modern-version]、[statelessness][modern-basic]

旧版（2025-11-25 及以前）先 initialize：客户端提供支持的版本、身份与能力；服务支持该版本则原样返回，否则返回自身支持的其他版本；客户端不支持返回版本则 SHOULD disconnect。成功后客户端必须发送 notifications/initialized，随后才正常操作。支持旧版还意味着实现该版的服务端请求、能力和错误语义，不能只换方法名。[旧生命周期][legacy-lifecycle]

**实现选择（推论）**：现有框架的首个 stdio 表面可以明确限定 2026-07-28，使用 server/discover 做启动准入，并对旧版给明确错误。若 JiaClaw 需要旧服务器，再实现有测试矩阵的显式 dual-era 模式；不得在现代错误或发现无交集时暗中 initialize，也不得把任意陌生 response 当作版本证明。

## 3. stdio 的真实 wire 与资源边界

每行一个 JSON-RPC request/notification/response，消息中不能有原始内嵌换行；stdout 只能有 MCP 消息，stderr 可有 UTF-8 日志，stderr 内容本身不表示失败。现代客户端不能向 stdin 写 responses，现代服务端不能向 stdout 写 requests。全部 responses 和 notifications 共用一条 stdout，没有 HTTP 的每请求响应流。[现代 stdio][modern-stdio]

responses 按 JSON-RPC id 关联；progress notifications 用客户端提供的 progressToken，它在所有活动请求中必须唯一；subscriptions 用 `_meta.io.modelcontextprotocol/subscriptionId`。收到一条 stdout 消息时，“当前正在等待的调用”不能替代明确关联规则。日志消息没有通用的 RPC id 关联字段，不能凭收到顺序把日志写成某并发调用的已验证证据。[progress][modern-progress]、[schema][modern-schema]、[现代 stdio][modern-stdio]

现有 `mcp_client.rs:1700-1727` 有严格单次 response 分类：id 必须等于预期 JSON value，method+id 被拒绝，result/error 必须二选一。可以复用这个验证思想，但 HTTP 的单响应流天然作用域不能直接移植成 stdio 的全局 channel 作用域。现有 `1335-1427` 的 request-byte 检查、deadline、metadata 和 `1765-1806` 的 bounded discovery/version 检查也有可复用部分。

**必须分别设计的政策（推论）**：

| 资源 | 所有权与限额应覆盖的范围 |
| --- | --- |
| 读帧 | 在寻找 newline 时即限制 bytes，不能先无界 read_until 再检长度；覆盖不换行、CRLF、EOF 半帧、非法 UTF-8、JSON 深度和 batch |
| 写帧 | 序列化及 newline 的最大 bytes；总截止包括等待容量、锁、写、flush；部分写失败不能继续复用 channel |
| stdout | 单帧限额之外，还限制 startup 与每个调用收集的 bytes、通知条数、pending 请求和队列容量；空行、未知消息洪泛也消耗资源 |
| stderr | 独立并发读取，限制保留 bytes、总读取/速率政策和日志输出；保留缓冲满后仍需 drain 或终结，不能阻塞子进程；不把原始结果/日志默认送入 tracing |
| discovery/catalog | 启动总截止、版本数量/长度、页面数、工具数、cursor bytes、重复 cursor/工具检测、schema 预算；全部准入成功后才公布工具 |
| 并发关联 | pending map、通知 map、tombstones 均有界；区分未知、重复、已取消晚响应；禁止 ID 环绕或绑定到另一进程代次 |
| 全生命周期 | spawn/准入/Ready/Stopping/Stopped 由同一 owner 维护；在 startup、等待 permit、写、读、调用返回前各点被 drop/cancel 都有 cleanup owner |

字面参数可直接使用 Command::arg/args；默认 Command 会继承环境，应 env_clear 后只注入明确映射。绝对 executable path 可避免 PATH 搜索差异；Windows 的 cmd.exe/.bat 使用非标准参数解析，不适合作为“无 shell 字面参数”的普通执行目标。白名单是被传入的 key/value，不能暗中读取全部宿主环境再筛选或调用安装器。[Rust Command 官方文档][rust-command]

**取消与结果不明**：stdio 必须向已发且仍认为活动的 request ID 发 notifications/cancelled；服务器可能不能取消，客户端应忽略已取消调用的晚响应；progress 可重置 idle timeout，但必须另有绝对总截止。[取消规则][modern-cancellation] 取消通知不是 rollback、服务器退出或完整树为空的证明。Issue 140 的强回收要求可以通过终结整个 client-owned process 实现；若多个调用共用进程，需同时结束其他 pending 请求并明确它们的结果不明状态。发现请求可以重做，未证明幂等/可查询的 tools/call 不得随进程重启自动重放。这是本仓库比运输规范的 restart/retry 建议更强的应用契约，不能被通用 SDK helper 覆盖。

## 4. rmcp 3.5.0 锁定源码核对

来源为官方 Rust SDK 的本机 registry 包，VCS commit `0cde3c5cf3e6aff0cc852ce6045f107e95991f48`，路径 `crates/rmcp`。以下均描述该版本实际调用路径，不推测其他 SDK 版本。

| 表面 | 已核对行为 | 与本任务的差距 |
| --- | --- | --- |
| `transport/io.rs:1-6` | stdio() 只返回当前进程 stdin/stdout | 不是启动本地服务器的 client process owner |
| `child_process.rs:61-64,123-164` | 默认 stdin/stdout piped，stderr inherit；builder 可返回 captured stderr；spawn 后包装 AsyncRwTransport | 未提供 stderr 上限或自动 drain，也未自动配置 process group/Job/cgroup 或 env 白名单 |
| `async_rw.rs:63-75,125-197` | receive 使用 read_until newline，积累 Vec 并保留 partial-read 内容以跨 select! 取消；忽略空行；解析后的 Vec clear 保留 capacity | 无接收帧上限；攻击帧可无限增长；合法的 EOF 尾部无 newline 仍可进入解析，注释不等于实际 delimiter 强制 |
| `async_rw.rs:220-235,374-435,467-473` | Codec 可 new_with_max_length，默认 usize::MAX；Decoder 检查 line limit；Encoder 序列化后追加 newline | child 的 receive 不走该 Decoder；Encoder 不检查 max_length；仅设置 Codec 的 max_length 不能为默认 child receive 提供限额 |
| `async_rw.rs:164-189,293-348` | syntax/EOF JSON error 被忽略；合法 JSON 但形状错误会向对端发送 error response；一些未知 notifications 被忽略；parse 失败 trace 可记录完整 line | 不是本仓库严格 malformed-frame fail-closed 策略；modern-only stdio 禁 client responses，需外层 wire 验证/明确策略；注意敏感输出进入日志 |
| `service/client.rs:638-843,950-1059` | 默认 serve 仍 Initialize；Discover 显式现代且不回退；Auto probe 总计固定 10 秒，识别 modern rejection/version，再按 legacy 处理其他 JSON-RPC 错误或超时 | Discover/Initialize 不自带 startup 总截止；Auto 的 legacy fallback 也无总截止；外部 ct 可打断启动，但须独立保证 owner cleanup |
| `service/client.rs:168-242` | success/error 检查显式 id；无 id error 也接受为当前启动错误；通知/ping 可忽略/处理后继续等 | 若需严格相关的时代证明，不能接受无 id error 或无限通知作为可归属事实；必须限制 startup 读量/条数 |
| `model.rs:322-335; service.rs:466-487` | 数字 request ID 与可解析的字符串 response ID 有兼容匹配；ID 使用 AtomicU64 fetch_add 后转 i64，没有耗尽拒绝 | 本仓库应决定精确类型相等政策及 ID exhausted 行为；可采用不混淆的 string ID 或独立 allocator，但仍需测试 |
| `service.rs:772-849,870-941` | 默认 send_request 的 options 无 timeout；提供 timeout、progress reset、max_total_timeout；send_request_with_option 的入队发生在 await_response 计时之前 | 内置 timeout 不能覆盖所有排队/写阶段；需外层逻辑 deadline，真实并发 permit 和全局有界 pending |
| `service.rs:555-684,1471-1488` | await_response 超时会等待发送 cancellation；cancel 返回发送结果；取消发送处理会移除 pending | cancellation 本身可能卡在入队/写；没有普通 RequestHandle 的 Drop 自动取消；drop future 不等于远端停止或进程回收 |
| `service.rs:813-818,1360-1386,1722-1752` | outbound channel 1024、proxy 64；pending HashMap/JoinSet 不受这些队列容量直接约束；无关联/duplicate responses 常规路径不匹配则丢弃 | bounded queue 不能证明总 in-flight/memory bounded；严格 unknown/duplicate 策略需宿主/transport 明确实现 |
| `child_process.rs:45-55` | ChildWithCleanup Drop 取出 child 并 tokio::spawn kill future，不 join cleanup | 依赖仍可工作的 runtime；Drop 不是等待退出确认，也不是树回收证明 |
| `child_process.rs:81-106` | take child ownership、关闭 transport、固定 3 秒 wait，否则 await child.kill；没有单独 SIGTERM 阶段 | kill/wait 阶段没有绝对截止；cleanup future 被取消或 close 出错时，child 已不在 ChildWithCleanup 内；不能用 Drop 兜底证明覆盖所有分支 |
| `service.rs:1099-1185,1757-1809` | RunningService.close/cancel 等后台 loop，包括 transport.close；EOF/cancel 可先 drain 5/2 秒；Drop 只触发取消；close_with_timeout 取出 JoinHandle 后 timeout | join 成功仅能证明该 transport 的 close 路径完成；超时后 handle 丢失会 detach task，无法再 join；loop 记录 close error 后仍返回 quit reason，不能仅靠 quit reason 认定 process cleanup 成功 |

锁定 tokio `1.53.2` 的 `process/mod.rs:202-224,1123-1127,1240-1329,1379-1382` 进一步确认：默认 child drop 继续执行；kill_on_drop 仅触发 kill；Unix reap 是 best effort，没有时间保证；完整 await wait/kill 才能获取更强的直接 child 清理证据。`runtime/task/join.rs:18-36` 确认丢弃 JoinHandle 会 detach。

官方 SDK 的协议类型、现代 request metadata、错误结构、工具模型可继续复用；帧准入、整体 deadline、宿主 process owner 和 tree containment 必须是单独受测契约。使用 SDK 不自动满足这些上层要求。

### process-wrap 10.0.0：能力与额外限制

rmcp Cargo.toml `142-145,792-795` 的 child feature 可选依赖 `process-wrap = 10.0`。研究时主项目 Cargo.lock 没有 process-wrap；以下来自本机已存在的 `process-wrap-10.0.0` 包，不能称为主项目已启用或已锁定。VCS commit `d02486b8d1e393a4ac27023d591b6f35c6417ffb`，第一方源码为 [watchexec/process-wrap](https://github.com/watchexec/process-wrap/tree/d02486b8d1e393a4ac27023d591b6f35c6417ffb)。

- `tokio/core.rs:141-180` 默认 kill = start_kill + wait，原生 child passthrough 只针对直接 child。CommandWrap 不会凭空添加 wrapper。
- `tokio/process_group.rs:85-108,113-159,173-203` 显式 ProcessGroup 会建立 PGID，killpg，并 waitpid(-pgid)。后者只能 wait 当前可等待的子进程；ECHILD 不能证明所有组成员都已经消失，更不能覆盖已 setsid/setpgid 逃离的后代。
- `tokio/job_object.rs:71-129` 显式 JobObject 会暂挂 child、创建/assign job 后 resume；仅同时注册 KillOnDrop 时设置 KILL_ON_JOB_CLOSE（`windows.rs:118-171`）。
- `tokio/job_object.rs:177-210` 的 wait/try_wait 不应作为树为空的独立 oracle：`windows.rs:245-270` 的 wait_on_job 在收到 completion 后直接 Break，没有检查 event code 是否 ACTIVE_PROCESS_ZERO；try_wait 最终返回直接 child 的状态。微软还明确普通 job completion 消息不保证交付，宿主应另查实际 active process 状态。[Job Objects][windows-job]、[Job accounting][windows-accounting]

## 5. 原生进程树机制与可实现范围

“完整树”需要一个创建时建立、后代不能通过普通 daemonize 脱离的内核归属边界。PID/PGID 和父子关系快照不是这种边界。更完整的机制也不证明外部副作用回滚，不覆盖服务器要求其他系统服务创建的独立工作负载。

| OS/机制 | 可落地路径 | 必须声明和验证的限制 |
| --- | --- | --- |
| Linux / cgroup v2 | 为每个 owner 建立专用 domain subtree；exec 前将初始 child 纳入；fork 自动继承；关闭 stdin、有限等待后 cgroup.kill；等 events populated=0，并 wait/reap 所有 owned children | 检查真实 v2 和 kill 文件、写权限/合法域；须禁止目标在 kill 前迁出 containment；不能先让可执行文件运行再迁组；zombies 不出现在 cgroup.procs，因此还要 reap；host 死亡不会因为关闭目录 fd 自动 kill，要 supervisor 或恢复 owner |
| Linux / PID namespace | 受控 init/supervisor 从新 namespace 启动服务并 reap；终结 namespace init 会让内核向剩余成员 SIGKILL | 建立 namespace 需要支持与权限/用户 namespace 政策；禁止目标获得逃出权限或外部启动通道；并非所有普通用户运行环境可用；必须真实测试 init 与孤儿回收 |
| Windows / Job Object | 创建 unnamed non-inheritable Job；设 KILL_ON_JOB_CLOSE，禁两种 breakaway；CREATE_SUSPENDED → 成功 assign → resume；强终结 TerminateJobObject；核实 active processes 归零、关闭 handles | 嵌套 jobs/已有 job/权限可导致 assign 失败，失败时 child 不得继续；last-handle-close 才触发 crash cleanup，不能把 job handle 交给 child；CreateProcess 子代默认入 job，WMI Win32_Process.Create 不受该自动归属；job 不替代安全 token/沙箱 |
| macOS / POSIX process group/session | 在 exec 前设置新 group，保留直接 child owner；stdin EOF → 等待 → TERM/KILL group → wait child | 只能覆盖仍在 group 的进程；后代可 setsid/setpgid，double-fork 可 reparent；waitpid(-pgid) 不等于整个组消失；库无法仅凭这些公开 API 证明任意完整树回收 |
| FreeBSD / procctl reaper | 专用 supervisor 在启动前 PROC_REAP_ACQUIRE；PROC_REAP_KILL 对当前后代发信号；PROC_REAP_STATUS + wait 回收 | acquire 不覆盖既有子代；GETPIDS 有 PID reuse 警告，嵌套 reaper 子代不全列出；KILL 返回成功只代表至少一个被 signal，必须检查 rk_fpid/rk_killed、反复状态核实并 reap；此研究未验证该平台 |

Linux 的实际 cgroup 语义来源为 [kernel cgroup v2][linux-cgroup] 的 Organizing Processes、Delegation Containment、cgroup.events、cgroup.kill 章节：fork 子代出生于父 cgroup；kill 覆盖全部后代 cgroups，并处理并发 fork/migration；populated 只表示 live processes。归属禁止提前迁出的权限设计是实现前置条件，不能把一次 cgroup.kill 的并发保护误写成目标永远无权逃离。若采用 `clone3(CLONE_INTO_CGROUP)`，Linux 5.7 起可以直接创建于指定 v2 cgroup，但仍受迁移权限和有效 domain 约束。[clone3][linux-clone]

Linux subreaper 让孤儿重归最近仍存活的 ancestor subreaper，提供 wait 权限；它不提供对任意后代自动 kill 的完整树原语。PDEATHSIG 是直接 child 的 parent-thread-death 信号，会在 fork 子代中清除，也受时序/凭据变化限制；不能用这两个选项替代 containment。共享库不宜悄悄把应用全局设为 subreaper 并 waitpid(-1)，会争抢应用其他 child 的回收权。[subreaper][linux-subreaper]、[PDEATHSIG][linux-pdeath]、[PID namespace][linux-pidns]

Windows 的挂起启动避免 assign 前 child 运行并创建未归属后代；job 在默认禁 breakaway 下保留普通 CreateProcess 子树。Microsoft 对 nested jobs、WMI 例外、completion 可靠性、last handle 与 security limits 有明确说明；因此生产实现应保存独立 job ownership，查询 [ActiveProcesses][windows-accounting]，不能只 wait 初始 process handle。[Job Objects][windows-job]、[AssignProcessToJobObject][windows-assign]、[CREATE_SUSPENDED][windows-flags]、[Job limits][windows-limits]、[non-inheritable handle][windows-create]

macOS group 逃逸是公开系统调用直接允许的行为：[Apple setsid][apple-setsid] 建新 session/group；[Apple setpgid][apple-setpgid] 改 group；[Apple kill][apple-kill] 的负 PID 只 signal 相应 group；[Apple wait][apple-wait] 只等待符合父子约束的进程。本研究没有找到能把普通用户的任意本地 executable 子树强制关入、禁止这些逃逸并自动 kill 的等价通用 API。该结论只限定所审查公开 API，不是对所有 Darwin 内核/特权 helper 能力的不存在证明。把 process group + /proc/libproc 扫描包装成完整树保证是不成立的。

**可落地选择（推论）**：

1. 强回收 API 在 spawn 前核对 containment 能力。不满足时失败，不能先返回 client/公布 tools 再在文档写 best effort。平台仅凭 `cfg(unix)` 不能被列入强回收支持。
2. Linux 可提供真实 cgroup/PID-namespace supervisor 后端；Windows 可提供 Job 后端；macOS 需要另行设计并验证隔离 supervisor（例如明确选择的 VM/容器执行环境），或者将原生 process-group 功能列为较弱契约且保持 R1 强树要求未关闭。VM 改变 executable 的运行环境，不能假装仍是任意宿主本地 executable。
3. 正常 async close/cancel/timeout 须等待 tree empty/reaped 的证据。绝对 cleanup 截止后仍没有证据，应返回 cleanup failure/pending，保留可查询的 owner/recovery 状态；不能把“已发 SIGKILL”报告成“已释放完整树”。Drop 只作为最后手段，无法承担已经同步验证退出的承诺。
4. 进程 unexpected exit、调用 future drop、client final clone drop、runtime shutdown、host crash 必须分别定义 ownership/recovery。Linux cgroup 是持久的内核目录归属，需外部 supervisor 或可认证的遗留归属恢复；Windows last job handle close 需要验证实际 handle 不继承/不泄露。不能只记录 PID 再重启后盲杀同 PID。

## 6. 为 R1 关闭所需的测试证据（建议，未执行）

- 协议：现代 discovery、-32022 无交集、旧 unknown-method 任意错误/沉默、明确 legacy fallback 开关、returned unsupported legacy version；每次调用 metadata；success/error ID mismatch、数字/字符串区别、id-less error、重复/未知/旧进程 response；交错 progress 与无关联日志。
- 边界：无 newline 持续输出、超大帧、EOF 半帧、CRLF、非法 UTF-8、batch、深 JSON、stdout 日志、服务端 request；无限通知/空行/stderr、目录 page/tool/schema/cursor 上限；等待队列、阻塞写、partial write、启动总截止。
- 生命周期：spawn 前后错误、discovery 前后 drop、permit/写/flush/读/返回瞬间 drop；取消通知发送阻塞或失败；stdin EOF 被忽略、TERM 被忽略；初始 child 先退出而后代存活/持管道；double-fork、setsid/setpgid、快速 fork；并发 close/cancel，runtime teardown 和 host abrupt exit。
- 系统 oracle：Linux 核对专用 cgroup/PID namespace 的真实成员与 reap 状态；Windows 核对 Job active processes 及 handle 生命周期；单个 `ps -p initial_pid`、child.wait、kill 成功、pipes EOF 均不能单独证明完整树为空。逃逸测试只能在实际 containment 环境内执行，防止测试自身遗留进程。
- 外部消费者：固定 revision/crate、实际独立项目发现/allowlist/schema/call/关闭；故障下不公布未准入工具；isError 工具结果与运输故障不同；无凭据继承、无安装、无 shell、无 ambiguous-write retry。各平台 Supported 只能在其真实 runner 对最终不可变实现跑过该矩阵后声明。

## 7. 本机来源与 SHA-256 快照

rmcp 本机目录：`/Users/jiawy/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/rmcp-3.5.0`。包 archive SHA-256 已实际读取，等于 Cargo.lock registry checksum：`fae7019994ae0fe4ada40b732f798f3ff26f0f04facb1477f1bf37eb4f18a2d3`。官方 immutable 源：[rmcp commit tree][rmcp-source]。下表为本机实际文件 bytes；行号以该快照为准。

| 文件 | SHA-256 |
| --- | --- |
| rmcp `src/transport/child_process.rs` | `8612b2607643c89d1bfcabf222070b5f9355df788aabd0cb9d331d764263810e` |
| rmcp `src/transport/io.rs` | `86cbe078a7c03964340570d8940ca6a3276cf7c8056dbd4d64a9f654a7fa077f` |
| rmcp `src/transport/async_rw.rs` | `b4234e7d5df9f80cdcc67755f64c72e4fe7909749754ff569e518e6ebcb7d846` |
| rmcp `src/service.rs` | `d4a625e7e49009b2ce25b4498ddef88ef6242915b7aa2e53826da4e9b531d473` |
| rmcp `src/service/client.rs` | `a122773fd2aa273e7707c3b083ce78094acae1c19fd80f319ce4bebc8d4f0814` |
| rmcp `src/model.rs` | `7ea72af619fc02cce7f6699e3274c8a9e4509b9c3d887f7df5bff1854f2debe0` |
| rmcp `.cargo_vcs_info.json` | `b2f51ec38e76d73d7f5c64dab677b72a57639ac3969837a4d0ebd99dafb17184` |
| tokio 1.53.2 `src/process/mod.rs` | `43beb73d468354cab1818b0043a9adafb37764b3b883b34455815ac2b4ec0cd8` |
| tokio 1.53.2 `src/runtime/task/join.rs` | `e1c5c5730743d901ce375f0c46eb3050a9d69b5c28ebe275fb2a585a3e22debb` |
| process-wrap 10.0.0 `src/tokio/core.rs` | `c23981c0da706e921492f4032181ca86fecd3531d12e2282c1180eaff8fcf932` |
| process-wrap 10.0.0 `src/tokio/process_group.rs` | `68396841c599cb6ed4cae198597013a0de668d7ca1e01bcee892d0115e1013be` |
| process-wrap 10.0.0 `src/tokio/job_object.rs` | `5bdc2eca24955c1580ab2966dc25102363852f3f8a78a2ecab24f1799e8dbf6e` |
| process-wrap 10.0.0 `src/windows.rs` | `76214e10c89af6589af8fc9c2ae1d4c319596a07b2a2945afbc4a23b1d52a2b9` |
| 仓库 Cargo.lock（读取时） | `fc36c3966dc2abf990fae6a01b633459d95fedae0fd7f77c429b5f04447b2e94` |
| 仓库 `mcp_client.rs`（读取时） | `d65f932be5b6461e808c06758cf88a130f3a28641e361cd3b03c49846d14deab` |

tokio VCS commit：`ff0c4068a4ba5f3af0fe3c5657ba42879b6ea599`，path `tokio`；[immutable tokio tree][tokio-source]。tokio Cargo.lock checksum 为 `e95f91fcc7a621e8b030f6aa23c71fe9838ae2fb4d8118b75602a328f5144044`。process-wrap 为额外本机源码参考，主项目后续若启用此 feature，必须以最终 lock 重新核对。

## 8. 官方来源索引

所有外部材料均为协议所有者、SDK 所有者、OS 官方文档或 Linux man-pages 项目的第一方资料。Apple 页来自官方 archive，年代较旧；用于解释基础 syscalls，不作为当代 macOS 整体支持资格。本文未以二手文章证明主张。

[modern-stdio]: https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/stdio
[modern-version]: https://modelcontextprotocol.io/specification/2026-07-28/basic/versioning
[modern-basic]: https://modelcontextprotocol.io/specification/2026-07-28/basic/index
[modern-cancellation]: https://modelcontextprotocol.io/specification/2026-07-28/basic/patterns/cancellation
[modern-progress]: https://modelcontextprotocol.io/specification/2026-07-28/basic/patterns/progress
[modern-schema]: https://modelcontextprotocol.io/specification/2026-07-28/schema
[legacy-lifecycle]: https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle
[rust-command]: https://doc.rust-lang.org/std/process/struct.Command.html
[rmcp-source]: https://github.com/modelcontextprotocol/rust-sdk/tree/0cde3c5cf3e6aff0cc852ce6045f107e95991f48/crates/rmcp
[tokio-source]: https://github.com/tokio-rs/tokio/tree/ff0c4068a4ba5f3af0fe3c5657ba42879b6ea599/tokio
[linux-cgroup]: https://docs.kernel.org/admin-guide/cgroup-v2.html
[linux-clone]: https://man7.org/linux/man-pages/man2/clone.2.html
[linux-subreaper]: https://man7.org/linux/man-pages/man2/PR_SET_CHILD_SUBREAPER.2const.html
[linux-pdeath]: https://man7.org/linux/man-pages/man2/PR_SET_PDEATHSIG.2const.html
[linux-pidns]: https://man7.org/linux/man-pages/man7/pid_namespaces.7.html
[windows-job]: https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects
[windows-assign]: https://learn.microsoft.com/en-us/windows/win32/api/jobapi2/nf-jobapi2-assignprocesstojobobject
[windows-flags]: https://learn.microsoft.com/en-us/windows/win32/procthread/process-creation-flags
[windows-limits]: https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-jobobject_basic_limit_information
[windows-create]: https://learn.microsoft.com/en-us/windows/win32/api/jobapi2/nf-jobapi2-createjobobjectw
[windows-accounting]: https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-jobobject_basic_accounting_information
[apple-setsid]: https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/setsid.2.html
[apple-setpgid]: https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/setpgid.2.html
[apple-kill]: https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/kill.2.html
[apple-wait]: https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/wait.2.html

FreeBSD procctl 原生 reaper/KILL/STATUS 资料：<https://man.freebsd.org/cgi/man.cgi?query=procctl&sektion=2&format=html>；Windows 强终结资料：<https://learn.microsoft.com/en-us/windows/win32/api/jobapi2/nf-jobapi2-terminatejobobject>。这些条目仅作为实现机制研究，未转换为平台支持声明。
