<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# RFC-0025：MCP 与 A2A 身份、权限及恢复映射

- 状态：Draft
- 作者：StateKnot contributors
- 创建日期：2026-10-15
- 跟踪：[R1 身份与协议安全映射](../r1-contract-gap-ledger.zh-CN.md)、[PR 161](https://github.com/StateKnot/StateKnot/pull/161)
- 替代：无
- 被替代：无
- 审阅基线：`b12ac21c0946787b2f0c3e0f06b64c590219f200`
- 实现状态：记录现有限定 Interface 与拟议宿主要求；没有新增 verifier、持久化 backend 或生产资格
- [English](0025-protocol-identity-and-authority.md)

## 概要

固定本地持久执行与 MCP/A2A peer 之间的信任映射。认证识别调用者，本地策略另行授权具体操作与资源。远端路由租户、descriptor pin、Agent Card、关联 ID 和发现成功都不授予本地权限；入站凭据不自动成为出站凭据。恢复必须具备当前授权，并绑定原先冻结的持久化身份。

本草案独立于 RFC-0004 的子 Run 契约及 RFC-0024 的拟议 stdio owner。现有策略 trait 不因此成为随包交付的身份提供方，A2A TCK 内存夹具也不成为持久化 task backend。接受仍须完成下列具体身份/策略组合及证据。

## 固定源码事实

| 表面 | 固定基线契约 | 宿主责任 |
| --- | --- | --- |
| MCP HTTP 入站 | `McpServerAuthenticator` 返回仅含有界 subject/scopes 的 `McpServerPrincipal`；解码后的 Tool/Resource/Prompt 策略独立 | 验证凭据，保留 issuer 来源，将 subject 绑定到本地主体/租户及具体操作 |
| A2A 入站 | `A2aServerPrincipal` 含 `TenantId`、完整 `PrincipalIdentity` 和 scopes；`A2aServerAuthorizer` 在 backend 查询前运行 | 验证身份和可信租户绑定；提供资源策略、共享准入及持久化 `A2aTaskService` |
| MCP 持久 HTTP 出站 | `connect_authorized` 固定完整原始 descriptor，在持久 attempt start 之后调用 `McpToolAuthorizer` | discovery 凭据绑定已配置 endpoint；每个新 attempt 检查实际输入、可信 Run 身份和目的地 |
| A2A 出站 | 不可变 client 固定发现/interface/security；token provider 收到 operation、card digest、binding、remote tenant/task、required scopes 与可选本地 attempt tuple | provider 绑定精确 endpoint，从宿主持有的来源记录解析当前主体/同意 |
| MCP stdio | RFC-0024 为 Draft，没有 session Implementation | 发布 Tool 前选定并验证 OS owner 与冻结的本地进程安全作用域 |

这些是固定基线的事实，不表示任意自定义 trait 实现满足本 RFC。兼容入口、直接协议 client 与持久 dispatch 继续明确区分。

## 身份与权限模型

外部身份使用精确已验证的 `(issuer, subject)`。显示名、邮箱、Agent Card 名称、Tool metadata、模型输出不能替代它。可信控制面将其映射到一个明确本地租户与有效主体，并设置有限策略寿命。实际权限是已验证凭据权限、本地绑定与资源策略的交集；缺失、过期、歧义或不可用绑定均不授予权限。

在固定审阅基线，MCP principal 没有 issuer/tenant 字段。多 issuer 宿主必须先将已验证 issuer/subject 映射为有界、不透明、宿主持有的 principal key，完整映射保留在协议 subject 之外；在 cursor、request-state 和审计的全部寿命内不得与另一身份产生别名。同一外部 issuer/subject 若改绑本地 tenant、effective principal 或 binding generation，也必须获得不同的 key；或证明原映射在全部关联状态寿命内不可变。现有 private cursor 绑定 subject/scopes、MRTR associated data 绑定 subject/Tool/arguments，本身不含额外本地租户字段；不能复用旧 key 后声称旧状态已失效。不能直接拼接未转义字符串，不能把任意 subject 当作 `TenantId`。具体 key/生命周期表示须在接受前冻结并验证；下文新增候选已有具体类型，数值修复后的资格单独记录。A2A 已携带完整身份与本地租户，但它们仍须来自 authenticator 的可信映射。

本地 Run/invocation/attempt ID、worker fence、journal head 是关联及持久所有权事实，不是远端登录凭据。可信 `ToolContext` 可关联 dispatch，自身不证明终端用户身份、同意或当前 capability；这些必须从宿主持有记录解析。委托需要另行支持的 verifier/策略保留 actor、effective principal、目标资源与权限收窄；未支持的委托或 sender-constrained 凭据须拒绝，不能静默降级。

## 入站映射

开放准入前配置精确 public authority、可选浏览器 Origin、协议挂载点、verifier、允许 issuer/audience/resource、过期及撤销策略。外部边缘使用已验证 TLS 与可信代理配置，客户端 forwarding header 不能选择这些值；verifier endpoint 由运维配置，不由 token URL 决定。Agent HTTP JWT/JWKS/introspection profile 不自动验证 MCP/A2A token：audience、scopes、context 与 authorizer 均不同。

保留各协议现有次序：MCP 先认证/共享准入，再在解码 handler 执行 scope/operation 策略与参数验证；A2A 在解析 body 前认证，在 task/config 查询前授权有界解码操作，随后共享准入并调用 backend。`Mcp-Method`/`Mcp-Name` 只是提示，不能替代解码事实。不能声称两者准入次序或所有拒绝状态完全相同。

MCP 继续按 scope 过滤发现、绑定 private cursor 身份，在披露存在性/schema 诊断前授权。A2A backend 收到 `A2aRequestContext`，每个存储键都必须使用其中 principal 的租户；request tenant override 不能改变存储租户。不透明 task/context/push-config ID 只是未可信查询输入，不是所有权证明。

普通公开 A2A Agent Card 是发现数据，不授予调用或读取 extended card 的权限。MCP anonymous loopback、内存/loopback conformance 夹具仅属开发 profile，不能装上 allow-all 策略后当作生产部署。

## 出站映射与数据释放

dispatch 前冻结部署绑定：精确目的地/transport、已审 descriptor/card 证据、remote name/skill、本地 schema、已批准 extensions/security alternative、远端路由租户及 credential provider。发现漂移必须重新审阅绑定。descriptor pin 认证已审描述，TLS 在其信任根下认证已配置服务；两者都不证明 executable 行为或远端 exactly-once。Agent Card 内嵌签名在没有自有 verifier 时仍是数据。

A2A remote tenant 来自已选 interface，属于远端路由命名空间，与本地存储租户独立。禁止将其当作本地存储权限，或自动由本地租户生成。两个字符串恰好相同不构成映射。

为精确批准操作签发 destination/audience/resource 限定的凭据。MCP approved 逐调用 request 有实际 endpoint/input；其 startup callback 只有本地 descriptor，provider 须预先绑定已审 endpoint/discovery 权限。在该基线，A2A authorization request 有 card digest/binding，没有 endpoint 或实际 payload accessor；provider 须专用于冻结 endpoint，或由可信构造取得绑定，不能仅从 card digest 推断 endpoint。共享 provider 的 endpoint 消歧是待决 Interface，而非已实现检查。

禁止将入站 bearer token、数据库凭据、worker lease、控制面或上游 provider secret 当作 ambient authority 转发。已批准参数/message part 是显式数据释放：Adapter 不能阻止可信应用主动把秘密放进去；本地 schema 验证形状，不授权披露内容。该基线 A2A callback 的 attempt tuple 与 shape schema 都不能替代实际参数授权。接受前必须固定并执行可审计的宿主 pre-send payload policy/wrapper，或另行审阅暴露同一实际目的地/请求载荷的 Interface；其检查必须覆盖首次 send、streaming send 与已 attested MessageIdReplay，保证拒绝后零业务发送。MCP approved request 已有实际输入，两者不能混称为都已完成参数检查。

持久执行必须在策略和可能的业务 send 前提交精确物理 attempt start。MCP approved 调用在剩余 deadline 内授权已验证输入；A2A provider 在持久路径收到本地 attempt tuple。直接 `McpClient`/`A2aClient` 不具备持久 dispatch 权限，不能在可恢复 Graph 内替代这个顺序。MCP `connect` 兼容路径也不因此取得 approved 路径的 input-aware 策略或连接退役保证。

## 撤销、状态与不确定结果

每个新出站操作和恢复请求都重新检查当前策略/凭据，token 与本地策略寿命有界。旧 permit、排队请求、发现 capability、缓存 schema 均不能绕过撤销。撤销无法回滚已发送请求或已缓存字节，接收资源仍须自行验证凭据和资源策略。长流及 continuation 需要明确的过期/复核与寿命契约，不能暗中继承 Agent HTTP SSE 的复核周期。

MRTR state 绑定已认证 principal、operation 与配置好的 codec/keyring。Sampling/Roots/Elicitation 是请求宿主行动的未可信输入，不自动授予文件、模型或 UI 访问。宿主逐次批准响应；continuation 不能切换身份或扩大权限。stdio 的进程代次与凭据作用域另见 RFC-0024。

保留真实 effect mapping，不能声称某个通用协议错误证明没有写入。对 write-risk，MCP HTTP transport 丢失、incomplete/isError 与 A2A 发送后歧义保留现有 Unknown 处理；MCP ReadOnly 对应 `NotApplicable`，其中 protocol loss 为 `SafeAfter(250ms)`、incomplete/isError 为 `Never`，不能将其误称为不确定写入。当前 MCP HTTP 完整但无效结果的 write 路径使用 `Applied + Never`，RFC-0024 对 stdio 拟议 `Unknown + ReconcileFirst`。本草案记录差别，不修改任一路径；接受前必须明确审阅，不能由共享 helper 静默选择。

MCP result/known-error reconciliation 需要单独授权、绑定原 endpoint/descriptor/持久 attempt 的证据，不重发写入。A2A 恢复另行配置：Disabled 无 provider I/O；ContextTaskHistory 仅有界查询原始精确历史；durable artifact 使用已固定精确 task handle；MessageIdReplay 只有在明确持久去重 attestation 和匹配 delivery profile 下才可重发。Agent Card 或重复 message ID 不是该 attestation。恢复权限不来自原 attempt 当时有效的凭据。宿主无法认证完整权威证据时，Unknown 保持 Unknown。

## 持久存储、审计与运维

token、grant、live client、request context、process credential 不成为持久执行 capability。仅保存现有显式身份/来源、descriptor/schema pin、attempt/effect/result 和审计契约。可序列化 `PrincipalIdentity` 是审计值，不证明持有者已认证；不能持久化任意策略对象或把旧决策重放成当前 grant。本 Draft 本身不执行数据库迁移；下文显式状态 profile 候选会使旧 private cursor/continuation 失效，未配置行为保持 legacy。

A2A 生产 server 仍须交付持久幂等、租户隔离投影、稳定 cursor、已提交 stream/cancellation、加密 push secret、具 SSRF 约束的 egress 与事务 outbox；router/TCK 不提供这些。容量与跨副本准入需要真实部署测量。MCP/A2A 服务路由也不建立 Worker-only 数据库隔离，后者属于独立部署契约。

审计保留有界本地 decision/evidence 标识、精确已审绑定和策略 generation、operation、disposition 与恢复结果；身份审计有访问/保留规则。普通 metric/log 省略凭据、参数/结果、request state、原始远端错误。现有 MCP raw response 的 Debug derive 并非已脱敏；RFC-0024 的拟议修改还需实现和回归。Readiness 必须包含实际 verifier、新鲜租户/资源策略、共享额度与持久 backend，发现探针成功不够。

## 选定的 A2A 请求事实 Interface

新增实验性 Interface 在 `A2aClientAuthorizationRequest` 提供 `request_method() -> &str`、`request_url() -> &str`、`request_body() -> Option<&[u8]>`，补齐上述基线的事实可见性缺口。没有新增 provider trait、依赖、verifier 或隐式准入决策，完整身份 profile 的接受仍开放。

Unary/streaming 均在解析凭据前按原上限准备请求；同一个私有不可变请求提供策略事实，随后由 HTTP builder 消费。既有 Bytes 共享序列化 body，无额外完整 payload 拷贝，也不要求公开 Bytes 类型。HTTP+JSON/JSON-RPC 保留各自原 envelope、request ID、URL/query 编码、header、限额、deadline 与 dispatch/effect 分类。getter 不是通用 header policy 对象；部署仍绑定配置好的 header/extensions 与 credential scope。Anonymous 不调用 provider；忽略新事实的 static/旧 provider 保留原行为。

宿主现在可检查实际数据，在发送前拒绝，包括恢复时重新应用当前策略；这不证明任意 provider 都已检查参数或可信 Run consent。MessageIdReplay 保留业务 message 身份，但 JSON-RPC envelope 每轮使用新 correlation ID，应检查实际新请求。deadline/drop 仍在等待的 callback 必须被丢弃且不发送，同步工作继续由宿主保证有界。策略与远端 effect 不因此成为原子事务。

Manual Debug 省略新增 URL/body，原 tenant/task/attempt 字段仍可见。Raw accessor 是对可信宿主策略的显式数据释放；Clone 共享并可能保留 payload 超过 HTTP 调用寿命，宿主保留责任明确。验收须逐字比较 prepared method/URL/query/body 与真实 wire，并覆盖 payload/目的地拒绝、pending callback deadline/drop、legacy/Anonymous、当前授权的 attested 恢复。PostgreSQL 证据须在策略内观察真实原 attempt 已提交，并证明拒绝本身零业务发送。只有同一笔已提交 durable Failed/Unknown attempt receipt 的重复恢复，才要求不新增发送或第二次策略评估。新 probe 或经 attestation 的 MessageIdReplay 仍是新操作，必须重新应用当前策略，并可按原恢复契约发送。

## 选定的实验性 MCP 宿主绑定状态候选

数值修复候选固定928文件source `69af3a0f…`，
536编译输入 `ded1a866…`。原opt0/默认栈六门禁通过：99suite/2098 passed/6个原ignored、
48doc、严格Rust1.88 Clippy/Rustdoc及固定nightly。
fresh default-feature opt0 binary `0751e63128bf…` 双PG16.15/17.11各串行完整15/0/0/0、
完整marker一次；库存13TCP/1constructor/1DB，DB只含3消费者/6commit/12拒绝及两个完整
fixture Run/Journal账本，不是全部表或通用backend。
见[限定记录](../qualification/experimental-mcp-lossless-bound-state-2026-10-16.zh-CN.md)
及[server guide](../mcp-server.zh-CN.md#实验性宿主绑定状态候选)；不表示RFC接受、完整CI、
Supported部署或已发布API。

完整typed binding保留稳定namespace、已验证外部issuer/subject、本地tenant、effective
issuer/subject和不得复用的正u64 generation；canonical scopes参与同一完整指纹。语法不
验证来源；宿主现有decoded策略/admission在排队后核当前mapping，成功检查是授权线性化
点；无新动态backend或在途零时隙撤销。generation在ABA/撤销再启用时也不能复用或回绕。

显式immutable profile保持composite builder与四direct services配置；十owned context
入口覆盖空/未挂载与Skill实际内部dispatch。同步静态metadata、SDK控制方法不新增业务
权限。Private catalog不改应用显式Public Resource内容，也不替代HTTP认证/verifier。

强private cursor仍v2；三种操作AD均v3；内部envelope仍整数1，version/applicationState
两字段必须完整，null合法而missing拒绝。Tool/Prompt使用实际有界parsed Value递归排序
对象后serde_json::to_vec的sorted serde_json value encoding；不是JCS，不保留原wire
数字拼写，保持宽整数/整数浮点/正负零表示。Resource没有参数。实际公开数值反例限定
Tool；Prompt先按原schema验证声明字符串，helper数值测试不扩schema。同key合法旧v2
state在三域拒绝且无fallback；未配置legacy v1/JCS不变。

decoded policy仍见untrusted wire state；原permit/schema及开封成功后业务请求只恢复inner
state，其他数据不变。Some responses含空map也要求token。原完整envelope64KiB/wire128KiB/
正TTL<=24h不变；封装发生在业务之后，失败不证明零effect或授重试。类型不成为Serde
record，限定Debug脱敏不免除getter/Clone敏感数据保留责任。

历史eedd六门禁/14项成功保留，但实际wide-integer TCP反例使旧候选不能按精确参数绑定
采用。原门禁/夹具/marker失败及新E0597/socket失败均保留。v3新门禁不转授旧Native/916
或任意宿主实现；Markdown轮廓不是独立编译的外部consumer。前置85f CI虽双版Store264/
Runtime189有成功终态，整体仍11success/2原30m取消，且没有新MCP候选。新增Ubuntu/
test-opt1 MCP及完整CI、真实mapping/verifier/delegation/长流、A2A持久backend、stdio与R1
继续开放。

## 选定的实验性 A2A 订阅策略 Interface

新增的 `A2aServerHttpOptions::with_subscription_authorization_rechecks()` 显式开关
仅作用于只读 `SubscribeToTask`；默认关闭且幂等，Clone、原限额和其他 Operation
保持原样。它不广告 Streaming 支持，也不改变原 Prepare/Admission/Backend 创建
顺序，因此第一次新增 Pre-check 拒绝不代表未创建订阅。

私有 Owned Stream 在 Backend next 前和取得正常 Event 后，复用原完整 Principal、
Canonical Scopes、原解码 Task ID、`SubscribeToTask` 和 configNone，调用当前宿主
Policy。一个 Idle 绝对期限覆盖两次 Callback 与 Backend 等待；每次 Callback
还受原 Request Timeout 约束。宿主 Callback 必须有界且不阻塞，Timeout 无法抢占
同步宿主工作。步骤观察终结后在终止/交付前释放自有 Stream 与 Operation Permit，
不创建 Detached Policy Task，也没有 Permit Release/Reacquire 窗口。

这是对当前 Decoded Policy 的复核，不是重新认证 Bearer；宿主必须通过当前可信
策略拒绝失效权限。最后 Post-check 不追回已 yield/缓冲字节，也不在背压或未 poll
期间提供周期撤销保证。Rust Drop 不代表持久化 Task Cancellation。默认/非订阅
行为，以及更广的 Backend、Deployment、Stream Lifetime 接受条件仍分别开放。

详见 [Server Guide](../a2a-server.zh-CN.md#实验性订阅当前策略复核)。最终冻结 v5
共 930 文件，六项本地门禁通过：100 个 Workspace Suite，2,108 passed、0 failed、
6 个原有 ignored；10 组 Doctest 共 48 passed。8 个 Direct Ownership Test 与
一个 TCP Family 经 REST/RPC 两条 Binding 覆盖 17 个矩阵 ID，这是可控
Policy/Stream 证据。源码不变的 Loopback 授权重跑与原始红见
[限定记录](../qualification/experimental-a2a-subscription-policy-2026-10-16.zh-CN.md)。
RFC25 保持 Draft，不新增 Native 数据库、独立 Server TCK、完整 CI、部署中的
Identity/Policy/Durable A2A Backend 或 R1 资格。

## 选定的 A2A 初始准备期限

认证成功后，已有 Request Timeout 提供一个私有单调时钟准备期限。全部十一
操作的 Body 收集/规范化、已解码授权与共享准入消耗同一期限；Backend 执行
保留原有独立超时。期限来自可信 Task-local 状态，不来自客户端；缺少 Principal
或期限 Context 时拒绝。真实时钟/Shutdown 检查在下一个策略阶段或 Backend
派发前拒绝迟到的有效结果，等待中的工作始终由请求拥有。

只读取 SDK 实际消费 Body 的路由。REST 创建 Push Config 使用配置上限并
转交原始字节与原 Request Parts。有效路径上的 Body Buffer 拒绝保留固定 SDK
完整 400/413 和允许 Origin 响应头；不保证多缺陷输入的错误优先级。新增解码前
超时/Shutdown 返回纯文本 HTTP 408/503；已解码准备失败使用原 Binding Envelope
与既有 A2A Internal Error。同步宿主工作不能被抢占，不据此承诺外部准入回滚、
断开探测时延或持久化 Backend。详见
[Server 合约](../a2a-server.zh-CN.md#请求准备期限)。此变更收紧原先无界的准备路径，
不代表 RFC25、Deployment 或 R1 已接受。

## 验证与接受

1. 冻结 issuer/subject 到本地主体/租户、MCP key、audience/resource/scopes、策略 lease 与撤销规则。通过公开 Interface 验证同 subject 不同 issuer 不混同，同一外部 pair/相同 scopes 改绑本地 tenant/effective principal 后旧 cursor/MRTR 拒绝，以及恶意 routing/body/metadata 不改变存储租户。
2. 验证每个挂载操作的认证/策略/查询顺序、存在性隐藏、cursor/MRTR 身份替换、权限收窄与不可用/过期策略；适用拒绝路径必须零应用或存储 dispatch，包括排队撤销与启动漂移。
3. 捕获真实出站请求，核对精确目的地、remote routing tenant、逐 attempt context 与限定凭据；wire 不出现 ambient token、durable fence 或数据库凭据。验证相近 card/不同 endpoint 的 provider 复用与实际 payload 数据释放策略，覆盖 unary、SSE 与 attested MessageIdReplay；attempt tuple 本身不授予参数权限。
4. 注入 durable-start-before-send、丢确认、deadline/Future drop、撤销和进程替换。除已明确 attested A2A MessageIdReplay 外不得重发 Unknown 写；所有恢复 probe/evidence 绑定原 attempt 和当前授权。
5. 用已验证 TLS、真实数据库角色、跨副本准入、restore/rotation 验证实际 verifier/secret manager/peer。验证远端 attested retention/dedup/history 窗口；loopback/TCK 不替代部署证据。保留精确源码、binary 和 marker。
6. 审阅 MCP HTTP invalid-result 映射、A2A endpoint-aware provider Interface、长流撤销与持久 A2A task/push backend。完成全工作区/协议/Native/release 门禁及发布阶段要求的独立安全评审。

已有 guide/test 只是起点，不是该矩阵已完成。R1 与本 RFC 在实际限定接受条件满足前保持开放；RFC 接受也不认证任意用户策略实现。

## 备选与待决问题

把协议 tenant、subject、Tool annotation 或 card scope 当作本地权限会丢失独立宿主策略；转发入站 token 会让无关资源共享 audience/consent 假设；通用自动重发会破坏持久 Unknown 契约，均不采用。没有必要为了描述现有真实 Interface 引入统一万能 principal/transport trait，掩盖两者差异。

具体 MCP host principal key 已在选定候选一节冻结、编写，具有上述限定本机组件证据；使用选定请求事实的实际当前参数策略、verifier/delegation profile、长流撤销及持久 A2A backend 仍待决定。由此新增的公开类型、依赖或持久语义，必须先按 RFC 流程审阅再声明支持。

## 源码映射与声明范围

基线事实来自固定本地源码，选定的 A2A 请求事实另行说明为新增实验性 Interface；没有新增外部协议认证或通用身份标准支持声明。未来 stdio 仍只是 RFC-0024 提案。本文本不添加生产策略、测试、凭据或平台能力。

- [MCP ingress](../../crates/stateknot-integrations/src/mcp_server.rs)
- [MCP application policies](../../crates/stateknot-integrations/src/mcp_server_app.rs)
- [MCP approved egress](../../crates/stateknot-integrations/src/mcp_authorization.rs)
- [MCP effect mapping](../../crates/stateknot-integrations/src/mcp.rs)
- [A2A ingress/task contract](../../crates/stateknot-integrations/src/a2a_server.rs)
- [A2A client identity/discovery](../../crates/stateknot-integrations/src/a2a_client.rs)
- [A2A durable delivery/recovery](../../crates/stateknot-integrations/src/a2a_remote_agent.rs)
- [Core identity](../../crates/stateknot-core/src/identity.rs)
- [MCP authorization guide](../mcp-call-authorization.md)
- [A2A client guide](../a2a-client.md)
- [A2A server guide](../a2a-server.md)
- [stdio 草案](0024-owned-bounded-mcp-stdio.zh-CN.md)
- [RFC process](README.md)
