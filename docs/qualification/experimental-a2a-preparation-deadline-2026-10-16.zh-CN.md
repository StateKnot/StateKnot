# A2A 准备阶段期限实验资格记录 — 2026-10-16

固定准备阶段期限候选已通过六项本机组件门禁：workspace **100 个测试汇总、2119 通过、0 失败、6 个既有 ignored 夹具**；独立 doctest **10 组、48 通过、0 失败/ignored**。执行时清除了 StateKnot 数据库测试环境；条件数据库测试入口提前返回不构成 Native 数据库资格。见[机器报告](experimental-a2a-preparation-deadline-2026-10-16.json)、[原最终汇总](evidence/experimental-a2a-preparation-deadline-2026-10-16/original/q5/summary.json)和[执行配置](evidence/experimental-a2a-preparation-deadline-2026-10-16/execution-profile.json)。

认证成功后，boundary 仅建立一次服务器持有的 monotonic 绝对期限。实际 JSON body 收集、boundary 规范化、合法 decoded preparation、初始 authorizer 与 admission 共用该期限，覆盖 REST/JSON-RPC 的全部11个 decoded 操作，包括 REST 创建 push 配置。认证和 backend unary/stream 创建保留原独立 request 预算；既有订阅 idle 检查仍独立。本修复作用于原配置和开启订阅复核配置的共享 prepare 路径，不新增公开 API、依赖、数值限额、重试或后台任务。

解码前 body 超时/shutdown 固定返回 HTTP408/plain `request timed out` 和 HTTP503/plain `service unavailable`。初始策略超时/shutdown 使用既有协议 internal 错误 `A2A operation timed out` 与 `A2A service is shutting down`：REST headers 前是完整 SDK AIP envelope，测试的 JSON-RPC 保留原 id42 envelope。期限内原授权/admission 错误映射不变。shutdown 和实际时钟检查会在授予下一阶段前拒绝晚返回的 ready 成功或错误。pending body/callback 由请求直接持有，超时、shutdown 或请求 owner 实际 Drop 会释放对应 future。pending 策略不占 operation slot；最终 handoff 检查可能短暂取得 slot 后立即退役，不能称该分支从未 acquire。

仅缓冲实际 JSON reader。POSTcancel/subscribe、GET/DELETE 与 discovery 保留原忽略 body 行为。将原 `DefaultBodyLimit` 放在 boundary 外层，使 early push extractor 与 SDK 看到同一 configured maximum，包括高于 Axum 默认2MiB的值。RESTpush 保留原 request parts、headers/Content-Length、extensions 和原 payload 字节；原 Bytes/JSON plain413/plain400 与允许 Origin 的头保留；有效 JSON 但无效 push ProtoJSON 仍是 SDK AIP400。RESTsend/RPC 保留原规范化、Content-Length 移除和 early-error 格式。唯一显式接受的兼容例外是无效 UTF8 task path 与慢/无效/超限 body 同时出现：拒绝优先级可变化，但不得进入 policy/backend。原错误严格比较均使用有效 path。

最终 workspace 实际执行 **3 个新增私有单测、8 个新增 integration 测试族**，以及保留的 **一个17-ID订阅测试族×两种 binding** 和其8个既有私有单测。全操作测试族覆盖11×2 decoded 入口、正常 dispatch、四个旧 callback 错误映射及两种初始 pending 阶段。其他测试族覆盖 body/policy 累计期限、认证/backend 预算分离、ready-clock 拒绝、pending shutdown、保留第一份错误 Response 时的 requestcap1 复用、operationcap1 下显式策略 barrier、忽略 body/能力检查顺序及 configured push 限额。部分测试族还包含进程内真实 Router 所有权控制，不能把所有计数称作独立纯 TCP 测试。私有 handoff oracle 用 Clone 可区分的 opaque extension 验证转交原实例，并比对完整 headers/raw bytes。原17×2测试主体逐字不变。

截断 TCP 控制仅在 transport 实际给出完整原 SDK response 时逐值比较；没有 application response 的连接关闭不伪写成普遍400保证。两处内部交错未被强行确定性重现：CPU 时间恰好落在 SDK 同步 decode 内部，以及 operation slot 成功 acquire 与最终 guard 之间的零 await 指令间隙。实际时钟 body/policy 控制覆盖晚 ready 的 Ok/Err 与 shutdown，剩余位置仍需生产源码审阅。不能抢占不合作的同步宿主代码，只能在其返回后拒绝；不承诺端到端响应、断连回收、撤权或 latency SLO。Drop 不能回滚 callback 已提交的外部副作用。

保留的原始历史如下：

| 记录 | 实际结果 | 含义 |
| --- | --- | --- |
| 原产品累计控制 | compile0、test exit101；首个完整响应实际HTTP200、期望500；0通过/1失败/8filtered | 真实准备预算缺口，仅证明首个失败累计场景，不能称全部验收行都已红。 |
| 首次固定源码 TCP | exit101、5通过/4失败；共享 target 复用了旧产物 | 源码前后相等不等于 compiler read closure；原 rlib/fingerprint 观察保留，该次不授固定产品资格。 |
| 实际重编 Q2 TCP | exit101、7通过/2失败 | 夹具认证阶段少一次通知，四个逻辑 push 操作名误作锁定 SDK RPC method。 |
| Q3 TCP / strictClippy | TCP9通过；Clippy exit101 | 生产 boundary 函数超出行数规范；同一 body/deadline/error/Origin 职责抽入私有 helper。 |
| Q4 strictClippy | exit101、3个夹具诊断 | 字段命名、let-and-return、unit 表达式分号；Q5还在释放 barrier 前增加目标请求的 callback Drop 观测，直接检查 callback 仍被持有。 |
| 最终 Q5 | 六份实际回执 exit0、driver alltrue | 该确切最终源码的组件资格。 |

原 source-derived 契约表也包含这四个错误的 push RPC 名。[原件勘正](evidence/experimental-a2a-preparation-deadline-2026-10-16/contract/input-corrections.md)明确以锁定 SDK constants 和已实测最终测试纠正，原表仍按历史输入保留，不再被当作错误 wire 要求。

产品红与夹具/样式/cache 失败分别保留，未重写任何失败记录。[原始/存储 artifact 索引](evidence/experimental-a2a-preparation-deadline-2026-10-16/artifact-index.json)绑定两种 SHA，日志/patch 使用 deterministic gzip 后仍保留原 receipt 的 raw log hash。原 runner、driver、argv/cwd、allowlist profile 和源码 freshness 元数据均保留。本机 Rust1.88 dev/test opt0、debug0、incremental0、debug assertions/overflow checks 保持，warnings 被拒，未设置 stack/compiler wrapper override；固定 nightly 为 `nightly-2026-10-07`。这不是 CI opt1 或 cold-build 性能证据。

冻结快照为 `9a98cd929c10698f8cb866b99b3fd00a9b0f69dd840a68da1d01f5b30817f49d`、931行 manifest；**按 relative-path 字符串排序的540个 supplied compilation inputs** canonical SHA256 为 `c3c5ed5bb8f102d9b1b31bf1afbb21e75e1e05d4e2b37d523f4ba4ca2bc80edc`。[离线 verifier](evidence/experimental-a2a-preparation-deadline-2026-10-16/verify.py)只从已发布 subscription538 加四个新增/变更压缩源码 payload 重构这540项，核验931行清单摘要及其 receipt 绑定，不重构所有非编译文档。`--workspace --workspace-root …` 只逐字核验540项，不证明整个 Git tree、后来采用文档或 compiler read closure。复用源码引用只转授字节，不转授旧执行/Native 资格。包身份见[manifest](evidence/experimental-a2a-preparation-deadline-2026-10-16/manifest.json)及[seal](evidence/experimental-a2a-preparation-deadline-2026-10-16/seal.json)。

包作者编写过测试；独立源码 Standards v1–v5 与实现 Spec 原件以无损归档附入；实现 Spec 审阅者编写过原契约，已披露该身份。它们对最终源码报告0个剩余发现，不授予新执行资格。独立发布包审阅与实际 main 采用须另附固定范围记录，本封签不追认这些资格。Native 数据库、真实 IdP/current-mapping backend、持久任务、完整 CI、原30分钟 CI问题、stdio、部署/pilot 与 R1完成仍**未获资格/保持开放**。
