<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0024：拥有生命周期且有界的 MCP stdio session

- 状态：Draft
- 作者：StateKnot contributors
- 创建日期：2026-10-15
- 跟踪 Issue：https://github.com/StateKnot/StateKnot/issues/140
- 替代：无
- 被替代：无
- 审阅基准：c7d8768356b30207cd637d81e406056b745d35ac
- 说明：尚无实现；公开名称在 Draft 阶段仍可修订。
- [英文对照](0024-owned-bounded-mcp-stdio.md)

## 摘要

新增一个拥有 MCP stdio 生命周期的 session：启动显式配置的 executable，完成整个有界工具目录的准入，关联并发请求，并负责整个受约束进程树的关闭。可 Clone 的 client 共享由 session 控制的准入状态，不持有独立 OS owner 能力；丢弃请求不会把进程所有权移交给调用者。协议固定为 modern MCP 2026-07-28，HTTP MCP 保留其现有契约。

这是拟议契约，没有实现，也不是 Supported stdio 功能。接受前必须解决下述平台 owner 与依赖选型。parser 测试、SDK child transport 或仅终结初始 child 均不足以满足本 RFC。

## 动机

Issue 140 要求 JiaClaw 能发现并调用本地工具。R1 要求字面参数、显式环境、协议资源上限、完整树清理和精确 pin 的独立外部消费者。现有通用 MCP client 支持 HTTP 和交互结果；持久执行 MCP Adapter 另有更严格授权及不确定效果语义。二者应继续可用，不能要求消费者自行拼出第二套协议实现。

[进程所有权研究](../research/mcp-stdio-process-ownership-2026-10-12.md)记录了固定 SDK 和平台机制的限制。process group 不能让任意后代永久留在组内；owner 因而需要独立验证的 containment 机制。

## 目标与非目标

- 一个拥有生命周期的 session Module 隐藏 discovery、目录准入、framing、关联、有界队列、stderr drain 和进程清理。
- 共享协议 Module 复用现有 discovery、metadata、descriptor、result、notification 和交互结果语义；HTTP 授权头和重试规则留在该 Adapter 内。
- 持久执行 Tool Adapter 保证发送前已提交 durable attempt start，保留逐次授权，以及不确定写入的 Unknown + ReconcileFirst。
- 只有通过真实进程树与 host-crash 测试的平台才能列为支持；不满足 containment 时，在执行配置的 server 前拒绝。

本 profile 不包含 legacy negotiation、自动安装、自动重启/重放、跨租户进程共享或通用 OS 沙箱。RFC-0005 的纯计算限制不变。

## 面向调用者的设计

拟议 Interface 包含拥有生命周期的 McpStdioSession 与可 Clone 的 McpStdioClient。构造仅校验配置，不 spawn。start(&mut self) 负责 discovery 和准入；首次 poll 开始启动后，丢弃尚未完成的 Future 必须同步关闭准入并通知独立 owner 停止，不能仅让清理成为调用者下一次可选操作。session 保留 owner，可继续等待清理。丢弃从未 poll 的 start Future 不启动任何进程，状态仍为 Configured。只有全部准入成功才返回 client，不发布局部目录。

配置包含绝对 executable、字面 OS 参数、显式 cwd 与环境、有界策略、本地 schema registry、审核 descriptor pins、预期 server implementation 身份、一个可信本地安全作用域及运维配置的 containment profile。没有 shell string、PATH 查找、继承后过滤宿主环境、隐式安装器，也不自动换 executable/profile。

调用者可以读取脱敏状态、取得已准入目录、调用已准入工具并显式关闭。begin_shutdown() 同步关闭准入；shutdown(&mut self) 发起清理，wait(&mut self) 仅观察，不另行发起停止。丢弃等待 Future 均保留 owner。shutdown 只有在确认 tree-empty/reaped 时成功返回报告；pending/failed cleanup 必须返回错误，原始原因与恢复状态仍可从保留的 owner 读取。清理错误不替换原调用错误。pending 状态保留 owner 和可恢复的真实 containment identity，不报告成功，也不允许新 generation 复用该 identity。

复用 McpTool、McpToolCallResponse、McpCompleteToolResult、McpNotification、McpInputRequired。通过私有 HTTP/stdio continuation enum 保留现有公开 resume 签名。原 non_exhaustive client error 增加脱敏 stdio 分类，不要求重命名旧类型。配置、启动和清理错误区分无效配置、不支持 containment、准入拒绝、协议错误、上限耗尽、超时和 owner 故障。每项 stdio 调用错误都保留自己的 NotSent 或 MayHaveBeenSent，包括本地参数校验或 MRTR keys 拒绝；该错误的 Display/Debug 不含 OS 原始错误或 server 原文。共享协议解析错误由 stdio Adapter 包装，不能从缺少发送阶段的旧 HTTP variant 直接逸出。

现有 HTTP 诊断边界已为 McpRemoteError、McpCompleteToolResult、McpNotification 与 McpToolCallResponse 提供无 payload 的 Debug。未来 stdio 实现必须保留并回归这一共享边界；显式 raw accessor 仍可取得不可信数据。HTTP wire、授权和重试行为保持不变。

接受前须在独立消费者中编译公开名称、精确配置/错误/报告类型和真实示例。本节没有声称这些 Rust 类型已经存在。

## 详细语义

### 所有权与状态

一个 generation 按 Configured → Starting → Ready → Stopping → Closed 推进。启动/准入失败进入 Stopping；终结原因与 cleanup status 分开保留。Stopping 不回到 Ready，不隐式重启。新 session 必须重新配置与准入。

owner 必须在不可信目标可能执行前存在，拥有 containment identity、初始 child、三条标准管道、dispatcher 和 pending requests。只有完整准入与 Ready 发布成功后才解除 start stop-guard；已完成 start Future 的正常析构不算取消。client clone 没有独立管道，也不能让已退役 generation 重新可用。显式 shutdown、最后 Session drop、runtime teardown、host crash、owner 控制连接丢失都必须有清理路径；单一 Tokio task 无法成为跨这些故障存活的 owner。

第一次可能执行目标前安装 stop guard，跨每个 await 保留。普通请求不能填满或阻塞保留的 stop 通道。OS fallback 在 Tokio runtime 已不存在时仍须工作；Drop 不能依赖新 spawn 一个 async task。client 与已返回的 MRTR continuation 不能在 Session drop 后持有最后一个 Job/containment 控制 handle。

观察到 owner 控制连接丢失后，readiness 变 false，新准入关闭。已经跨入发送阶段的调用仍保守分类；owner 存活与外部写入之间没有原子事务。平台 profile 还必须定义 supervisor 自身退出或被杀时的行为，而非只处理其 client 断开。

调用超时、取消、被 dispatcher 接收后丢弃 Future、partial write、致命 reader/writer/output 违规均使 generation 退役。其他 pending 调用接收同一终结原因，但保留各自 NotSent/MayHaveBeenSent：其他请求引发退役不能把未发排队调用标成不确定写入。Module 接收请求前就取消，不算已发送。可先有界、尽力发送协议取消通知；它不证明外部效果停止或回滚。

### 协议与关联

首先发送 server/discover，带现有三项 inline metadata，并要求支持 MCP 2026-07-28。沉默或 legacy-only server 在启动期限内失败；未知错误不触发 legacy fallback 或隐式重发。目录准入前须确认 Tools capability 和配置的 server name/version 自述；serverInfo 不是 executable 或密码学 attestation。本 profile 不支持 server request。

每个 generation 只有一个 reader 和串行 writer。任何请求字节可能发送前，先分配并注册唯一 pending ID。ID 不回绕，同 generation 不复用；数字 ID 与字符串 ID 不等价。Tool binding ID 使用 HTTP 与所有 stdio generation 共用的 checked、process-wide allocator，不能因两个计数器都从 1 开始而使外来工具匹配另一个 Adapter。unknown、duplicate、id-less 或 stale response 使 generation 失败。

notification 按真实 progress token 关联，无关联 log 不借用“当前调用”的身份。active token 使用固定 SDK 中真实的 request _meta 形状。仅可证明属于本请求的 notification 进入其 McpToolCallResponse；无关联 log/unknown notification 进入独立有界 sink/counter。未知 token 的有界丢弃/拒绝策略必须明确，不能不断扩张 map。本 profile 不启用或广告 subscription。

LF scanner 使用固定 chunk，在读入时限制字节；允许 LF 前一个末尾 CR，拒绝非法 UTF8、batch、重复 JSON key、裸 CR、stdout log、空帧和 EOF 半帧。随后复用 Core 有界 JSON 的全部维度，不能 read_until 后才检查长度。record 上限不计 LF，但计入可选末尾 CR；generation output 计入实际读到的每个字节及分隔符。固定 read chunk 和分隔符的额外 buffer headroom 也必须有界。

启动遍历整个有界目录，包括被拒绝条目和全部页面。cursor 循环、歧义、缺少审核工具、pin 漂移或本地 schema 不符使准入原子失败。发送前校验本地 input schema，结构化成功输出在响应后本地校验。server 不能触发网络 schema resolution 或无界编译。

每项准入固定 remote name、完整已审核 descriptor digest、本地 input schema，并显式选择可选本地 output schema 策略。远端 output schema 是否存在及规范字节须与此策略一致；策略要求的结构化成功输出不能省略。通用 text-only Tool 可在显式无 output-schema 策略下准入；持久执行 Adapter 仍要求已有结构化输出契约。审核名称的任何重复都失败，即使某个重复本来会被排除。首个严格 profile 拒绝审核工具上的 x-mcp-header 与非 forbidden Task 声明；stdio 不作 header promotion，也不把参数改写为 HTTP header。

交互 continuation 保留原 generation、opaque state、原参数与精确 requested keys。resume 消费 continuation，显式发起一轮新请求，不迁移到替换进程。持久执行 write profile 像现有 HTTP Adapter 一样拒绝 interactive/incomplete outcome，可能发送后保留不确定效果。已返回 continuation 中的 MRTR state/input requests/原参数，持续占用 generation 的有界 continuation count 与 byte/node 预算，直到消费或丢弃；它与单轮在途状态分开计数。退役 continuation 不能重启进程或取得新凭据。

InputRequired 是不可信的宿主动作请求，不代表允许自动读取 Roots、执行 Sampling 或展示/回答 Elicitation。宿主必须在同一冻结安全作用域内显式授权响应。resume 重新检查 generation/准入与有界 input-response serialization，保留精确原参数/state，并分配新 correlation ID。

### 期限与上限

所有上限均有限、非零、构造时校验，并有 hard ceiling。startup 包含 preflight、containment、spawn、discovery 与完整准入。单个绝对 call deadline 覆盖 permit、排队、write/flush、响应、本地校验与逐次 policy；cleanup 的独立绝对 deadline 覆盖全部停止步骤，不能每步重置完整 timeout。

同步 host callback 须有界且不阻塞，完成后重新检查 deadline。Rust Future timeout 无法抢占不可中断 OS 操作；缺少清理证明时由独立 owner 保留 Pending，不能虚报 clean。

拟议默认值为 startup 10 秒、call 30 秒、cleanup 5 秒。frame 默认 256 KiB、hard 为 Core 2 MiB；active+queued 默认 16、hard 1024；聚合保留 request bytes 默认 8 MiB、hard 64 MiB；目录默认 16 页/hard64，advertised Tool 默认 1024/hard4096；保留 catalog 默认 8 MiB/hard32 MiB；每调用 notification 默认 1024/hard4096；保留 stderr 默认 64 KiB/hard256 KiB。这些是设计输入，不是容量结果。接受的契约必须冻结并验证其余 hard ceiling、generation lifetime/request/output 总量与 stderr rate policy。

实际 raw buffer capacity、保留的 compact JSON bytes、JSON node count 与对象 count 必须分开定义。compact/wire bytes 不是精确 allocator/RSS bytes。先 reserve 再分配/复制；owned storage 移动时携带 reservation，只有同一个 immutable allocation 才共享同一份 charge。克隆已 materialize JSON 必须重新计费。reservation 跨越结果 handoff queue 和 MRTR continuation，在最后一个计费 storage drop 时恰好释放一次。outbound serialization 使用 bounded sink，不能先建任意巨大 Vec 再检查。等待 permit 的调用不能形成无界参数克隆队列。

Module 预算覆盖 queued objects、active state、writer buffers、尚未交付的 replies 和自身保留的 continuation state，也覆盖被拒绝目录条目。已交给可信调用者的普通完成值、调用者自行克隆的数据，以及调用者拥有/尚未 poll 的请求 Future 中的 input 不在此 session storage 保证内；部署须明确这一限制，提供宿主 session 配额和 OS memory quota。Interface 无法限制任意调用者分配。

unknown message/whitespace 也消耗 output budget。stderr 从 spawn 起独立 drain，不依赖用户消费；retention 满不能阻塞 child。超出总输出/rate 上限使 generation 退役。慢 notification consumer 不能反压 reader 或造成无界队列。

### 发送与关闭

首次可能写入字节前切换为 MayHaveBeenSent。partial write 使共享 generation 失效，不能在未完成帧后追加下一帧；不自动重试。完整 JSON-RPC error、Tool isError 和 transport failure 保持不同 outcome。dispatcher 对完成与退役作线性化选择，不能覆盖已交付的有效结果，也不能把尚未交付的结果当作已提交 durable ledger。任一错误形态本身均不证明写入没有外部效果。

stdio 持久执行写入在发送后丢结果、isError、incomplete、无效/无法验证结果时，均为 Unknown + AmbiguousExternalOutcome + ReconcileFirst；仅本地验证通过的成功结果进入普通 ToolResult completion。这明确严于当前 HTTP McpRemoteTool::invalid_result 对 malformed complete result 使用 write Applied + Never 的映射。本 RFC 不改变该 HTTP 行为，也不能将它作为共享 helper 无意引入 stdio；现有 HTTP incomplete/isError/transport 路径已经使用 Unknown。

关闭顺序：关准入，尽力有界取消，关 stdin，有限 grace，强终结 containment，确认 tree-empty 与 reap，再 join 所有 IO/dispatcher 工作。报告保留原始失败与真实清理结果。Drop 可以请求清理，不能同步宣称已验证完成。

## 持久化与迁移

session 配置、client、owner、grant、credential、pending continuation 都是临时值，不实现 Serialize/Deserialize 或 JsonSchema。不引入数据库迁移、Core checkpoint 编码、Tool ledger wire 变化或 A2A 字段。已有 durable tool origin event 仍是发送权威。

跨 host crash 存活的 Linux owner 需要受限、版本化 recovery record，绑定 boot/generation 与真实 containment identity；PID 本身不能授权恢复。选定 owner Implementation 时必须冻结原子写入、权限、验证和保留契约。恢复只清理旧 generation，不重新执行其不确定 Tool 调用。

目标执行前必须持久记录可恢复所有权，包含 atomic write/fsync、目录/记录权限；只有确认清理后才删除记录。record 创建、spawn、kill、证明和删除前后的 crash cut 都不能丢失 owner，也不能误授权清理被复用的无关 path/PID。记录本身不会杀掉存活进程树。

## 安全与隐私

清空 child 环境，只设置显式、有界键值。校验 OS 编码长度、NUL、无效 key、平台等价重复 key。默认脱敏 executable 参数、环境值、cwd 数据、stderr 和远端错误原文。仅允许显式批准的标准管道与 owner handles 被继承；owner-control capability 不得交给 server。

Windows 字面参数 profile 仅支持经过验证、使用声明的 CRT/Rust 参数约定的 native executable；拒绝 .bat/.cmd 和自动 cmd.exe。Unix 直接将 OS 参数传给 exec。显式解释器仍是运维审核的 executable。

descriptor pin 核对准入的工具描述，不证明 executable 字节或外部效果。containment 部署另行固定 OS 身份、文件/网络访问、CPU/memory/PID 配额。server 不能拥有迁出 containment 或干预 owner 的能力。

持久执行 Adapter 使用显式 local-process authorizer，接收不可替换的 Tool/context/input 事实及冻结 process identity，默认拒绝。不改变 HTTP authorizer 的 endpoint 契约，不把 process-lifetime 环境凭据冒充逐 attempt Bearer。可信宿主在 startup 前绑定 process scope，并从自己的记录解析 Run principal/consent；ToolContext 不是外部 principal 的认证证明。不同 tenant/security principal 不共享一个进程。

环境凭据在进程启动时已释放，早于 Tool 准入与之后逐次检查。宿主拒绝可以阻止 tools/call 发送，却不能撤回已被进程复制的凭据，也不能证明 executable 没做后台副作用。这是对审核 executable 显式授予权力和释放数据的决定。存活 generation 不刷新或重绑凭据。宿主观察到可信撤权/到期后必须调用 begin_shutdown；Module 不能从环境值自动发现凭据到期。此操作关闭准入并退役 generation；重建仅授权新工作，不能重试 Unknown 写入。Debug 脱敏不等于所有 buffer 已清零或 core dump 不含秘密。

没有可信完整结果的可能写入保留 Unknown、AmbiguousExternalOutcome、ReconcileFirst；杀进程不改变该持久事实。

### 平台所有权 profile

Linux 要求运维控制的 cgroup-v2 owner/supervisor：target exec 前进 domain，防迁移，tree-wide kill，观察 populated=0，并 reap owned children。host 控制连接丢失以及 supervisor 自身失败/重启的外层生命周期与恢复路径，必须在 target 执行前建立。kill authority 在被清理的 cgroup 之外；target 不得拥有可破坏该 owner 的迁移、ptrace/signal、控制 IPC 或 namespace/privilege 通道。由独立管理的外层 supervisor 或 kernel-backed lifetime 处理内层 supervisor 的死亡；仅在下次启动扫描旧记录不能承诺及时清理。自身消失会留下 child 的孤立 helper 不合格。最终依赖/部署须证明全部不变量，含并发 fork 与初始 child 提前退出。

Windows 要求非继承 Job Object、kill-on-last-handle-close、禁 breakaway、suspended create → assign 成功 → resume、handle whitelist、Job termination、实际 ActiveProcesses==0 与 initial-child wait。assign 失败不得 resume。handle 生命周期和 crash cleanup 须独立测试。

上述步骤本身不是任意 executable 的完整 containment 配方。选定 token、DACL/handle 和 enclosing-job 策略必须拒绝取得外部进程的 PROCESS_CREATE_PROCESS/PROCESS_DUP_HANDLE 或等效干预能力，含注入、改 DACL、提权；不能直接假定普通同用户无限制 token 已符合此策略。微软文档说明 PROC_THREAD_ATTRIBUTE_PARENT_PROCESS 可继承所选父进程的 Job；仅禁 breakaway 不等于封住 alternate-parent。不能让 target 继承、复制或保留可用 owner Job handle。WMI/其他外部 launch broker 不会自动进入 Job：部署必须禁止这项授权，或明确其工作不在本进程树契约内。接受的 Windows profile 必须精确定义此范围，不能称为任意宿主级隔离。必须测 nested-job compatibility、assign failure、owner kill、last-handle close、alternate-parent 和 handle duplication。

审查过的机制不支持 macOS native 任意 executable containment。选择 native profile 时，在任何 target/helper spawn 或释放环境凭据前失败；不能先执行目标做 probe。sentinel 测试须证明这一点。未来 VM/container profile 必须声明 executable/filesystem/credential 的不同运行域，取得独立证据，不能自动 fallback。

现有 crate 保留 forbid(unsafe_code)。低层 safe owner 依赖或 companion 须显式审阅。任何本地 unsafe 政策例外都必须单独论证并通过 RFC 过程批准，不能靠不继承 workspace lints 偷偷授权。只开 SDK child-process feature 不解决这些要求。

## 可观测性与运维

提供有界、仅本地的 generation/phase/failure-kind counter、queue/output 耗尽、启动/准入拒绝及 cleanup pending/failure。metric label 不含工具参数、远端原文、path 或 credential。readiness 是完整目录已准入且 owner 存活，不是 pipe 打开。pending cleanup 可操作且禁止 generation 复用；运维可检查脱敏 identity 并调用经过认证的 owner recovery 路径。最终原生测量前不承诺 RPO/RTO、吞吐或 SLO。

## 兼容性

保留 Rust1.88、现有 HTTP Interface/wire/MRTR resume 签名及已有无 payload 的共享 Debug 边界。不隐式升级 rmcp 或开启 child-process feature。固定审查基线为 rmcp3.5.0、Tokio1.53.2。target-specific 显式依赖边、feature 和 owner 部署在接受前审阅；传递低层依赖不等于现有直接 Interface。

## 考虑过的替代方案

封装 SDK child transport 更小，但不能建立此处要求的 bounded parser、总预算和完整树/crash oracle。PGID 不能保住改 session/group 的后代；递归 PID 扫描存在 fork/reparent race。parser-only client 会把生命周期正确性分散给每个消费者。任意 transport trait 引入尚无支持的假设 Adapter；真实共享 Seam 是 HTTP 与 stdio 的协议语义。

## 验证与推出

1. 接受 RFC 前审阅完整 Interface、平台/依赖选型和固定资源策略。可先抽取保持 HTTP 不变的内部协议代码，但不授予 stdio 资格。
2. 从相同公开 session/client Interface 测 discovery/version/metadata、全目录上限与末页漂移、schema/pin 拒绝、反序并发响应、精确 ID 类型、交错 progress/log、HTTP/stdio 跨绑定拒绝、持有 MRTR reservation、调用者持有结果的范围、MRTR generation、畸形/超大/超深 JSON、无 LF/半帧 EOF、cursor 循环、未知/重复 reply 与全部聚合预算。
3. 测 permit/write/flush/response deadline、Future drop、partial write、阻塞 writer、stderr 饱和及所有 startup/stop 故障；验证 MayHaveBeenSent 与持久 Unknown，不自动 retry。
4. 每个支持平台独立观察成功、失败、timeout、cancel、最后 owner drop、runtime teardown、host kill、内层 supervisor kill、外层 owner loss、owner restart 后真实 tree-empty/reaped。核对每个存活 OS member/owner handle 与 crash cut，不能只 wait root 或检查 kill syscall 成功。fixture 包含初始 child 早退、持 pipe、忽略 EOF/TERM、改 session 及有界 fork 竞争；逃逸测试只能在已建立的真实 containment 中运行。
5. 运行原 HTTP authorization/MRTR/descriptor pin/durable Tool/conformance 及完整 workspace/release 门禁。不支持目标须证明未执行 sentinel server 就拒绝。
6. 独立外部消费者精确 pin crate/revision，真实执行 discovery/call、区分 isError/运输错误、准入拒绝、取消和可验证清理。双语资格材料保留 binary/source/environment、实际 marker 和默认资源。

Issue140 与 R1 只有在完整 Implementation 和全部适用证据通过后才能关闭。Draft RFC 或含条件资源分支的正向 harness summary 均不代表完成。

stdio 完成不能关闭 R1 其余语义、身份、兼容与资格要求。支持平台矩阵和外部消费者须符合实际约定部署；macOS 拒绝是诚实的能力限制，不证明 macOS 消费者的原生使用场景已可用。

## 未决问题

- 选择并审计精确 Linux 跨 crash owner 部署与 Windows safe Job owner 依赖，证明完整不变量。
- 冻结其余 hard time/rate/generation ceiling、聚合计数表示，按原默认资源验证，不能扩大限制过关。
- 冻结可编译的公开配置/错误/cleanup report、外部消费者 revision 和实际首批支持平台矩阵。
- 冻结 MRTR/已返回值的计数表示、call error phase 包装及 stdio/HTTP invalid-result 显式差别，保留并验证现有共享 Debug 边界，并在最终 Interface 编译。
- 证明 Windows security/handle/alternate-parent profile 与 Linux 外层 owner 生命周期；当前审查依赖尚未提供这些保证。

这些问题影响公开契约，解决前保持 Draft。下一项验证是在一个具名部署上检查 owner 能力、逃逸尝试和 crash 清理，完成后再发布工具。先选精确依赖/companion revision 并读源码，再以固定限制测试完整 oracle；能力不足必须在执行 sentinel target 前拒绝。可并行抽取保持 HTTP 不变的私有协议代码，但不能算 stdio 验收。

## 审阅来源与状态

本稿不改变源码、依赖、公开 Rust 类型或 unsafe 政策，没有编译或运行。源码观察基于 c7d8768；固定 SDK 仍为 rmcp3.5.0、Tokio1.53.2。所链接的研究记录了固定 SDK 来源；本 RFC 的已审阅要求在实现和平台证据完成前仍是提案。

- [现有 HTTP 类型](../../crates/stateknot-integrations/src/mcp_client.rs)
- [HTTP 持久结果映射](../../crates/stateknot-integrations/src/mcp.rs)
- [授权 Interface](../../crates/stateknot-integrations/src/mcp_authorization.rs)
- [Core 有界 JSON](../../crates/stateknot-core/src/json.rs)
- [RFC 流程](README.md)
- [固定 Windows parent-attribute 文档](https://github.com/MicrosoftDocs/sdk-api/blob/d43984d7c322f7d7d35704d6d1ad36dc3539703b/sdk-api-src/content/processthreadsapi/nf-processthreadsapi-updateprocthreadattribute.md)
- [固定 Windows process-rights 文档](https://github.com/MicrosoftDocs/win32/blob/3aee48ac3f48e187d17bdfd56951741737331749/desktop-src/ProcThread/process-security-and-access-rights.md)


## 2026-10-27 私有实现进展

[共享 Tool 结果解码](../qualification/experimental-mcp-tool-outcome-boundary-2026-10-27.zh-CN.md)已由真实 HTTP Adapter 采用，continuation 仍由 HTTP 绑定。本准备片不改公开 Interface，也未实现 owned stdio session。原生 owner 选型、最终限额、外部消费者与平台证据仍是接受条件，状态保持 Draft。
