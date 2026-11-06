<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# 实验性 fresh scoped ChildJoin 采用说明与待验收记录

核对日期：2026-10-19。[English](experimental-scoped-child-join-fresh-2026-10-19.md) · [机器证据索引](experimental-scoped-child-join-fresh-2026-10-19.json) · [Draft RFC-0022](../rfcs/0022-namespaced-graph-frames.md#scoped-isolated-childrun-integration-in-development)。本文的 G2 是开发切片名，不改变缺口账本原 G2 条目的编号。

本文是待采用文档候选，记录已经存在的源码契约和分别绑定的中间实证。最终候选 `scoped-child-join-g2-complete-source-v1` 为 959 份源码、560 份编译输入，摘要分别为 `5f5c726a420de718fbacc329c21753432df31e3907dd40dfb1a086ace40edd68`、`c0a88f666c4704a7636901dbf65a8e72cf8de9e0adfa894b4740cac2b0fbd2e1`。其 CI pin 为 Store272/Runtime216；同源码 Store 的构建与完整 --list 已实际 exit0、库存272，二进制 `7d48b69c924d428830f48e4d0a29d16048240626f430657183630e30e619acf4`。这只证明构建/库存，不是完整Store suite通过；Runtime216的最终实际绑定仍待补。最终六门禁、完整 Native、发布 head/tree、实际 checkout S 与新完整 CI 证据仍待绑定。本次文档 overlay 是额外非编译变化，不把 959 摘要冒称为含本文的未来源码摘要。

## 应用采用边界

fresh entry 要求实际目标及可达嵌套帧的每个 declared child owner 都有 Exclusive、typed ChildJoin executor。callback 通过公开 `load_active_graph_frame` 取得完整实际 frame，核对当前 checkpoint、activation、namespace 与 fence，再使用 `ChildRunAdmissionIntent::for_frame` 和原 Store admission。不要从 GraphNodeContext 的部分字段猜造完整归属，也不需要新增 frame getter 或 coordinator。

每个 logical activation 的 slot 必须稳定。相同 key 的重放必须保留完整 descriptor、request、budget layers、resolved budget、graph、authority、initial state 和初始 ready nodes。`spawn_digest` 包含这些语义以及 parent admission/tenant/key；新候选 child Run/thread/invocation IDs 不参与该重试身份。Store 的幂等结果返回原 child、原事件/checkpoint、ownership 与 reservation，不是再创建一个子级的权限。Digest 是一致性材料，不能替代调用方授权。

Provider-native `initial_state()` 会生成 message/model-plan 身份。遇到进程损失或未知 admission COMMIT 时，先对原 logical key 做 full owning `load_child_run`：已有 child 必须复用原 intent.initial_state，同时仍按稳定应用模板核对 request、authority 和预算；只有精确 ChildRunNotFound 才允许首次创建状态，不能把损坏、隔离或数据库错误当不存在。尚未提交的候选状态没有成为 durable choice；新候选 IDs 可以变化，已提交的逻辑事实不可以变化。

无 Join 的 unfinished activation 可以首次进入 callback，或在 partial admission 后由更高 fence 按原 slot 重入。已注册、尚未发布的 Join 必须等待真实子级终态、结算和完整 publication，普通 claim 不能据此再执行 callback。收到 `context.child_join()` 时，callback 读原发布结果并返回 Completed，不重新 spawn/rejoin。结果和原 publication 的消费仍由既有原子事务提交一次；随后走原 scoped barriers、Return 与 Root continuation。外部 I/O 的不确定结果继续使用 durable invocation 与 reconciliation 协议；此路径不承诺任意 callback 副作用 exactly once。

## 预算与 cold retry 的容量

父 Run 必须为新物理 caller Start、重绑定、后续完成和重试保留真实 headroom。容量同时约束已发生 DIRECT、已结算子树实际用量和所有尚未结算的 child reservation ceiling；新结构 charge 前后的完整账户与数据库时间都要通过原检查。pending child 期间只允许这个精确 structural caller 接管，不因此允许 ordinary barrier、checkpoint advancement、Return 或 provider dispatch。

Reservation 是政策上限，不是已经花掉的 delegated usage。不要把 pending ceiling 写成 spent，不要预加 child usage，也不要为了让测试通过扩大父预算、抹掉 DIRECT 或删除普通 settlement guard。结算后的实际 child cumulative usage 只进入最终累计一次。

旧 admission/registration 双版失败揭示了真实配置问题：child 默认继承父的 25 retries，F2 caller 已真实消耗 1，完整容量为 25+1>25，原拒绝正确。新 v5 仅把该 fixture 的 child retry ceiling 收窄到 4，并加入保留旧 25+1 拒绝的 typed Core 断言；它没有扩大父预算或改变生产计算。v5 在本文证据截点尚未执行。4 是此夹具的限定配置，不是通用生产推荐值；应用须对所有维度按业务并发与恢复需求规划余量。

## 父终态与一次会计

Root terminal handoff 仍是 Active；只有公开 lifecycle coordinator 的实际 terminal transaction 才提交 Succeeded。父零 I/O 图不需要伪造 parent model_turn：父 DIRECT 认证实际完整 journal/framework charges 与真实 terminal output bytes；真实 child 的两次 Model、一次 Tool 调用及其 Succeeded 由原 ProviderNative lifecycle 证明。生产 coordinator 加入一次已结算 child cumulative usage，最终 AgentResult 必须精确等于 DIRECT+child，原 reservation、settlement、child result 和 Join consumption 不重复。

已有独立 parent-terminal v1 在双 PostgreSQL major 实际通过：真正父 Succeeded、完整 output/result/account、UnavailableLifecycleEvidence 下两次原 event/checkpoint 的 Idempotent ACK、活动 F2 下旧 F1 StaleFence、终态 F1/F2 与新 claim RunNotRunnable、全 tenant 无变化，以及重复 settlement/publication 各 empty。该用例是新连接与重建 binding，没有注入 OS kill 或传输 ACK loss；不能据此扩大故障资格，也不转授后续 style 或最终组合。

## 已有实证与下一次完整绑定

`scoped-child-join-g2-current-evidence-root-v1/receipt.json` 的 16 项原始回执已按字节摘要和各自 source/compiled 绑定核对：

| 范围 | 原实际结果 | 源码界限 |
| --- | --- | --- |
| Store capacity 四例 × PG16/17 | 8 次 exit0 | source `b1cd1f58…` / compiled `34e0fd4c…` |
| partial-spawn、consumption 各 × PG16/17 | 4 次 exit0 | 同一 `b1cd1f58…`；partial 含真实 OS kill/reap 与自然租约到期 |
| parent 实际 Succeeded × PG16/17 | 2 次 exit0 | source `76e7143d…` / compiled `4136bc98…` |
| 旧 admission/registration × PG16/17 | 2 次 exit101 | 原预算夹具失败保留；不是 v5 或最终源码通过 |

这 14 次通过来自两组不同源码，不能拼成最终候选的通过。该冻结基准包含 v5、新 deterministic caller/child-terminal overlap、parent style v3 和 CI272/216。随后同一959/560基准的 overlap 在 PG16/17 各实际1 pass/271 filtered，进程耗时1.335/4.217秒；完整回执与原始流绑定该Store二进制。它不补授其余待验场景。v5尚无本文已绑定的实际结果；同基准严格Clippy实际101，仅新test两条style诊断（similar_names、redundant closure），原失败保留，后续style修正组合须另行验收。机器索引明确列出待补的最终 gate/完整 Native/全部定向/final publication/真实 CI 输入；缺失字段意味着未验收，不得填旧 G1 或先前候选结果。后续只追加新版本实际证据，不改写本次截点或原失败。

已有 [G1 343 资格记录](experimental-scoped-child-join-resume-2026-10-19.zh-CN.md)保持原样：它证明已发布 Join 的恢复、零 I/O child failure 前置、同进程重连与 Root Active handoff；run39228803615 不含本 G2 源码，也不认证 fresh child、partial spawn 或父真实 Succeeded。drive 前已隔离 child 的拒绝仍在 owning preflight，最终消费保留原原子 fresh guard；不承诺撤销 gate 后并发在途 I/O。

普通 scoped cancellation、独立 cancellation F2 的完整矩阵、stdio owner/真实平台消费者、历史窗口、R6 容量/部署与 R7 发布仍各自开放。本文不关闭 PR161、RFC0022 Draft、Supported、R1/R6/R7，也不发布新 capability 声明。机器索引只是本地保留证据的导航，不是完整 portable archive。


## 2026-10-20：final-v1 实测增补与未验收修复诊断

本节只追加旧冻结 `scoped-child-join-g2-final-source-v1`（962 个源码、560 个编译输入）的 source-specific 事实；它不是包含本节或后续修复的新组合身份。此前959/560截点、旧失败与待验说明保持历史语境，不回写或反授。该962/560源码的 six-gates-v2 六项实际exit0：workspace2155 passed、6项原ignored，独立doc48 passed。

完整 Native-v2 的真实结果如下，均为macOS harness配保留的PostgreSQL容器，不授Linux宿主资格：Store PG16/17各272 passed/0 failed/0 ignored/0 filtered；Runtime PG17为216/0/0/0，Runtime PG16为215 passed/1 failed/0 ignored/0 filtered、exit101，失败为partial-spawn冷进程原90秒readiness等待。四次wall与monotonic差的绝对值均小于0.054秒，Root记录本轮保持唤醒且无前次sleep/wake干扰；稳定时钟下该失败仍存在，不能算完整双版Native通过。旧受sleep影响的Native-v1失败同样保留。

同一保存Runtime binary `955d35d2a32f175f337925a82ca33296eb575c36810b67cbb75cc6f09c675beb` 的PG16单例诊断实际47.081秒、1 passed/215 filtered、exit0。它是单例重跑，不能覆盖或撤销原全套失败，也不构成全套、CI、性能或容量资格。原失败数据库只读journal已到sequence22的Root节点 `graph-node-attempt-succeeded`，并观察guardian续租；这不能严格区分首次 `cold_driver.drive` 末段，与首次已返回后冻结源码1316行的重复drive。后者也可不提交新journal而启guardian；不得写成已定位首次drive或DirectEvidence唯一根因。

已确定的窄开销是同一次 `usage_at` 对相同child key、physical AttemptId、activation Join重复调用完整reader。拟议test-only修复在该次调用内每身份完整load一次，并对同一已验证journal page的每个spawn/settle事件继续扫描所有keys、保留唯一匹配、settlement、tenant/run/digest等原谓词；每次新调用重新完整读取parent admission/account/journal/head和所需完整records。不得跨call缓存、改Store/Driver的live/quarantine/write守卫、增加原90秒readiness或任何业务预算/fence；这里只减少可证明的重复工作，尚无修复后时延实测，也不冒称唯一耗时根因。

新组合的源码/二进制身份由Root另外计算，本文不嵌入自身或新globalsource摘要。修复候选、包含本节的完整六门禁、双PG完整Native、发布与新CI仍待Root实际绑定。G1准确head/run的资格不转授；原Clippy101、旧admission预算失败、已有中间通过和所有旧限定事实保留，RFC Draft、R1/R6/R7及其余退出条件保持开放。
