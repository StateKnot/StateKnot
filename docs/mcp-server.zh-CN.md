<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# MCP Server Profile

> 状态：已实现的 pre-alpha Profile；Public API 尚不稳定。<br>
> 协议版本：仅 `2026-07-28`。<br>
> Transport：Stateless Streamable HTTP Complete JSON 与 Request-scoped SSE。<br>
> 明确排除：MCP Tasks Extension 尚未实现，也不做支持声明。

StateKnot 现已提供由自身类型定义的 MCP Server Application Layer，覆盖 Tools、
Resources、Resource Templates、Prompts、Completion，以及 Tool/Resource/Prompt 的
多轮请求（MRTR），并可组合独立的 [MCP Skills Server Profile](mcp-skills-server.zh-CN.md)。
官方 Rust SDK 只作为私有 Wire Adapter，其领域类型不会成为 StateKnot Public API
的组成部分。

该 Server 仍属于 pre-alpha。此实现不等于整个框架已经 Production-ready，也不代表
Rust API 已稳定、已经内置 OAuth Authorization Server，或已经支持 Tasks Extension。

## 生产边界

一个请求按以下顺序穿过边界：

```text
Host/Origin/Body 检查
  -> Bearer Authentication
  -> 跨副本 Admission Policy
  -> 解码 Method 与 Resource Lookup
  -> Scope 与 Operation Authorization
  -> Schema/Argument Validation
  -> 显式配置时的绑定状态开封
  -> Application Handler
  -> 有界且通过 Schema 校验的 Result
```

Transport 不会把 `Mcp-Method` 或 `Mcp-Name` 当作 Authorization Fact。在 JSON-RPC
Request 完成解码与校验前，它们只是 Hint。只要配置中包含非字面 Loopback Host，
匿名服务就会在启动时被拒绝。

`McpServerHttpService` 提供：

- 精确 `2026-07-28` Version Enforcement 与 Stateless Protocol Metadata；
- 不允许 Wildcard 逃逸的显式 Host/Origin Allowlist；
- Streaming Request Body、Concurrency 与 Request Deadline 上限；
- Credential Redaction 与固定 RFC 6750/9728 Challenge 的 Bearer Authentication；
- 由调用方实现的 Admission Interface，用于共享 Quota 与 Rate Limit；
- Cooperative Shutdown 与 Cancellation Propagation；
- Complete JSON 与 Request-scoped SSE；禁用 Legacy Session 和 `initialize`。

生产部署必须在公网边界启用 TLS、Bearer Authentication 与跨副本 Admission 实现。
`anonymous_loopback()` 仅用于本地开发与 Hermetic Conformance Run。

## Application Surface

`McpServerApplicationBuilder` 只组合实际配置的 Surface，并且只声明对应 Capability。
适用的 admission/profile 检查后，调用缺失 Surface 返回 Method-not-found；StateKnot 不会用空实现伪造 Capability。

### Tools

`McpServerToolRegistryBuilder` 在启动时冻结 Executable Tool Registry。它会校验
Portable Name、JSON Schema 2020-12、Catalog/Schema Byte Ceiling、重复项、稳定排序与
Canonical Registry Digest。Validator 只在 Offline 模式编译；未解析或网络 `$ref`
会使启动失败。

每个 Call 先完成资源上限检查，再执行 Policy。Authorization 早于 Input Schema
Diagnostic，因此被拒绝的 Principal 无法探测私有 Tool Schema。Handler 仅在授权与
输入校验成功后运行。Structured Result 离开进程前必须符合注册的 Output Schema。
Text、Image、Audio、Embedded Resource、Resource Link 与未来协议 Content 都有明确
边界并经过校验。

只有请求携带 Progress Token 时才能上报 Progress；Handler Cancellation 采用协作式
传播。Tool-level Failure 保持 Tool Result，Transport、Policy 与 Handler Failure 保持
Protocol Error。

### Resources 与 Templates

不可变 Resource Catalog 校验 Absolute URI、结构化 URI Template、Catalog Ceiling，
并冻结 Stable Digest。Read Authorization 先于 Resource Existence Disclosure。Text 与
Binary Content 会校验 MIME、Base64、Item Count 和 Aggregate Bytes。每个 Result 都
携带显式 TTL 与 Public/Private Cache Scope。

### Skills Extension

可选的不可变 Skill Catalog 会声明 Final SEP-2640
`io.modelcontextprotocol/skills` Extension，并暴露 `skills/list`、`skills/get` 与受
完整 Manifest 约束的精确文件读取。其声明与消费端的
[静态 Client 与 Host Profile](mcp-skills-host.zh-CN.md)保持分离；Server 精确边界见
[MCP Skills Server Profile](mcp-skills-server.zh-CN.md)。

### Prompts 与 Completion

Prompt Catalog 校验 Name、唯一且有界的 Argument、Required Field、Scope、排序与
Stable Digest。Authorization 早于存在性和参数诊断。渲染结果使用有界的 StateKnot
Text、Image、Audio 与 Embedded-resource Content。

Completion 是可选能力，因此只有配置 Provider 后才会声明。Provider 收到有界且已
认证的 Prompt/Resource Template Reference、当前 Argument 与仅 String 的 Context。
Result 最多包含 100 个唯一值，并强制 Pagination Metadata 自洽。Target-specific
Completion Authorization 由 Provider 负责。

## Scope-aware Discovery 与 Cache

下文描述现有未配置状态 profile 的行为；绑定状态实验候选在后文单列。

Tool、Resource、Resource Template 与 Prompt Discovery 会按 Principal 的精确 Scope
过滤。Private Cursor 绑定 Catalog Digest、Principal Subject、Canonical Scope Set、
Surface 与 Offset；不能跨身份或跨 Catalog Revision 重放。只要 Catalog 中存在
Scope-restricted Entry，Public Cache 配置就会在启动时被拒绝。

Annotation、Description、Schema、Server Instruction、Client Capability、Completion
Value 与 Transport Header 都只是 Data，永远不是 Authority。

## 多轮请求与 Request State

Tools、Resources 与 Prompts 可通过 `input_required` 请求 Elicitation、Sampling 或
Roots Input。StateKnot 校验 Request Count、ID、Payload Size、Client Response 与
Opaque Request-state Size。

`McpServerRequestStateCodec` 使用显式 Keyring、Expiry 与 Associated Data 封装
Application JSON。调用方通过 Request Context 将 State 绑定到已认证 Principal 和
精确 Operation。Key 至少 32 Bytes、支持轮换、不会出现在 `Debug`，TTL 最长 24 小时。
Invalid、Expired、Tampered 或 Cross-operation State 会收敛到同一个 Public-safe Error。

不要在 Request State 中保存 Secret。Sealing 提供 Integrity 与 Binding；Payload 仍需
遵守 Application 自身的 Retention 与 Privacy Policy。

## 实验性宿主绑定状态候选

数值修复后的未发布候选绑定928文件
源码 `69af3a0f…`、536编译输入 `ded1a866…`。Rust1.88、原opt0和默认栈的六门禁已通过；
新双版PostgreSQL的限定执行记录见下文。[新限定记录](qualification/experimental-mcp-lossless-bound-state-2026-10-16.zh-CN.md)
和[机器回执](qualification/experimental-mcp-lossless-bound-state-2026-10-16.json)绑定精确源码、二进制和限定执行。[RFC-0025](rfcs/0025-protocol-identity-and-authority.zh-CN.md)仍为 Draft；
完整CI、可信mapping/verifier部署、stdio、A2A持久server与R1继续开放。
这些类型不在已发布alpha.1包中。

`McpServerHostBinding` 保存稳定部署 namespace、已验证外部 `PrincipalIdentity`、本地
`TenantId`、effective `PrincipalIdentity` 与 `NonZeroU64` mapping generation。
`McpServerPrincipal::from_host_binding` 导出不透明 `hb1:` subject 并规范化 scopes，
`host_binding()` 保留完整 tuple。语法校验不验证凭据；现有 authenticator 必须提供当前可信映射。
宿主不得复用或回绕 generation，包括 A→B→A 改绑及撤销后重新激活；代次耗尽须拒绝新的
映射权限。接收同一状态的副本应有意共享 namespace 与当前映射。

通过 consuming `with_bound_state(profile)` 在 `McpServerApplicationBuilder` 或直接
Tool/Resource/Prompt/Skill service 显式安装 `McpServerBoundStateProfile`。Builder 在最终
build 时统一安装同一 profile，不受配置顺序影响；配置后的 Clone 与后续 Completion 配置
保留它，配置前的旧 Clone 仍属 legacy。重复配置报错；Public catalog options 在构造时拒绝，
不静默降级。Private 只覆盖 catalog metadata 与 continuation 身份；应用显式 Public 的
`McpServerResourceResult` 内容保留独立缓存契约，不获得新的内容保密资格。仅有 host-bound
principal 不会自动启用 profile。

候选在 composite 与四种直接 service 均检查十个 owned handler 入口：`discover`、
`list_tools`、`call_tool`、`on_custom_request`、`list_resources`、`list_resource_templates`、
`read_resource`、`list_prompts`、`get_prompt`、`complete`；包括空 Skill templates、未知 custom
方法及未挂载操作。直接 service 未挂载入口在检查后仍取锁定 SDK 原响应；Skill 内部实际
custom/read 调度也检查绑定；Skill 文件读取仍拒绝 MRTR requestState/inputResponses。同步 `get_info`/`get_tool` 只是静态 metadata，不是已认证租户
权限。initialize/ping、tasks、notifications、subscriptions 等 SDK 控制或扩展不因此新增身份
或业务支持。Profile 不替代 HTTP 认证，也不交付 identity provider、A2A 持久 backend 或 stdio。

全部强配置private cursor保持 `v2`，使用完整绑定/canonical scopes同一fingerprint与原
catalog/surface/offset核对；三种Tool/Resource/Prompt操作AD全部改用 `v3`，内部envelope
仍为整数version `1`。同key的合法旧v2操作state也拒绝，无fallback；旧v1 cursor/未封装
state亦拒绝。tenant、effective principal、外部身份、namespace、generation或scope变化
使关联旧状态在业务前失效。迁移后列表从首页重取并协调continuation drain；不能据拒绝
自动重启业务或推导安全重试。未配置服务保留legacy v1 cursor、AD/JCS及应用opaque state。

Tool/Prompt参数采用 **sorted serde_json value encoding**：clone实际有界已解析Map为
Value::Object，递归排序对象键，再serde_json::to_vec。保持宽u64/i64、整数/浮点与正负零
表示、数组及嵌套值；只规范对象键序。它不是JCS，也不保留原始JSON数字拼写。Resource
没有参数字段。已复现数值别名限定实际公开Tool消费；Prompt仍在AD前只接受声明的有界
字符串，内部Prompt helper数值单测不代表公开数值可达，也不扩大schema。

Tool/Resource/Prompt 的解码 authorizer 看到有界、未可信的 **wire** envelope；原 policy 和
argument/schema 检查后，框架按当前 fingerprint 与实际解码 operation/arguments 或 URI 开封，
业务函数收到另一个不可变请求，其中只有 state 恢复为原 **inner** application state，responses
与操作数据不变。每个出站 `input_required` 都封装 state，即使应用没有 inner state。私有
version-1 envelope 必须精确包含 `version` 与 `applicationState` 两字段；显式 null 恢复 None，
缺字段无效。任何 decoded Some(inputResponses)，包括 `{}`，均要求有效绑定状态；wire null
仍按原 SDK Option 解码。坏入站 state 不进入业务函数；出站封装失败发生在函数调用之后，
不能证明零 effect 或授予新的重试权限。

完整 JSON envelope 含字段名、version 与转义后字节，合计不超过 64 KiB；保留原 128 KiB
wire-state 与正 TTL 最长 24 小时。没有截断或提高上限，一些原可接受的大 opaque string 将
无法装入新 envelope。Codec 提供认证，不加密，也不保证单次兑换。新 binding/profile 与
principal 无 Serialize/Deserialize/JsonSchema；其 Debug 省略身份、原 scopes、key、state 与
fingerprint。Raw getter/Clone 仍可保留可信身份或 payload；其他 request/result/context 的
Debug 不因此全部脱敏，state 不应保存秘密。

当前授权仍由宿主承担。现有解码业务策略（及 Skill/Completion provider）在排队后、返回
permit 前核对完整当前 tuple/generation；没有 decoded policy 的 catalog 路径，要求现有
admission 在自身排队后复核。该成功检查是相应授权线性化点，state guard 不增加 lookup
backend，也不跨应用 I/O 持锁。该点之前的映射变化安全拒绝；已授权或在途数据及长流没有
零时隙即时撤销承诺。Allow policy 自身不能证明这一部署条件。

下面函数使用已在本机编译的真实公开 Interface；本 Markdown 轮廓自身不是单独编译的
外部消费者或完整认证部署。第一函数接收宿主已验证事实，第二函数接收已配置的 codec/
builder，不提供 fixture key、万能 verifier 或虚构 backend。
```rust,ignore
use std::{num::NonZeroU64, time::Duration};
use stateknot_core::{PrincipalIdentity, TenantId};
use stateknot_integrations::{
    McpServerApplicationBuilder, McpServerBoundStateProfile, McpServerHostBinding,
    McpServerPrincipal, McpServerRequestStateCodec,
};

// Call from the authenticator with already verified, current host mapping facts.
// Typed inputs alone do not verify credentials, consent, or mapping provenance.
pub fn mapped_principal(
    authenticated_identity: PrincipalIdentity,
    tenant: TenantId,
    effective_principal: PrincipalIdentity,
    generation: NonZeroU64,
    scopes: Vec<String>,
) -> Result<McpServerPrincipal, Box<dyn std::error::Error>> {
    let binding = McpServerHostBinding::new(
        "inventory-mcp-v1", authenticated_identity, tenant,
        effective_principal, generation,
    )?;
    Ok(McpServerPrincipal::from_host_binding(binding, scopes)?)
}

// Call once at startup with Private catalog options and the host's keyring.
pub fn enable_bound_state(
    builder: McpServerApplicationBuilder,
    codec: McpServerRequestStateCodec,
) -> Result<McpServerApplicationBuilder, Box<dyn std::error::Error>> {
    let profile = McpServerBoundStateProfile::new(codec, Duration::from_secs(300))?;
    Ok(builder.with_bound_state(profile)?)
}
```

### 本机限定验收

固定源码最终六门禁通过：Rust1.88 formatting、全workspace/all-target/all-feature严格
Clippy、99个suite合计2098 passed/0 failed/6个原ignored、48个独立文档测试、严格
Rustdoc与固定nightly-2026-10-07 check。原opt0、调试断言/溢出检查及默认栈保持；
workspace条件数据库入口不授Native资格。

另一个fresh **default-feature** opt0 binary `0751e63128bf…` 在实际PostgreSQL16.15/
17.11各以 `--nocapture --test-threads=1` 执行完整15项，均15 passed/0 failed/0 ignored/
0 filtered，唯一完整marker各一次。库存是13个真实TCP、1个constructor和1个真实PG用例；
DB项仍核三个消费者、六次fixture commit、十二次拒绝改绑及两个完整fixture Run/Journal账本，
不是全部业务表或通用持久server backend。

新增TCP family核对八对数字/嵌套Tool参数的业务实际decoded字节。同值及变值的fresh
请求正常续传；原token改值必须HTTP400，business facts/count/completions不变。嵌套对象
键重排有正控。Prompt字符串正常续传、数字仍按原schema拒绝。两个新私有单测覆盖共享
编码及三种域同key合法旧v2拒绝，不宣称另跑三组HTTP旧域场景。

旧 `eeddc9f7…` 的六门禁/2095项/48doc和双版各完整串行14项成功保留；随后真实TCP
反例却使 `9007199254740992` 与 `9007199254740993` 复用token并新增业务调用。旧候选
不能以exact decoded-argument binding采用，历史绿灯不跨授新源码。早前门禁/夹具/串行
marker失败、新修复原Clippy E0597和最初sandbox socket拒绝均保留，不回写；旧916次
Native执行也不转授。

前置main head `85f3fd5` 的 [CI38752172054](https://github.com/StateKnot/StateKnot/actions/runs/38752172054)
终态11项成功、两个普通PG job原30分钟耗尽取消；两版Store264和Runtime189均有真实
成功摘要，但后续完整链未完成。该tree尚无新MCP候选，其未完成CI不认证本组合。
Ubuntu/test-opt1新增mandatory MCP步骤与完整GitHub CI仍未执行；本机loopback组件不授
Linux部署、容量、发布、Supported或R1资格。


## 构建轮廓

完整可执行 Example 目前保留在 Crate Test 中。精确版本 `0.1.0-alpha.1` 公共预览
暴露的 Production-shaped 构建顺序为：

```rust,ignore
let options = McpServerApplicationOptions::new(
    "inventory-mcp",
    "1.0.0",
    100,
    Duration::from_secs(60),
    McpServerCacheScope::Private,
)?;

let app = McpServerApplicationBuilder::new(options)
    .with_tools(tool_registry, tool_authorization)?
    .with_resources(resource_catalog, resource_reader, resource_authorization)?
    .with_skills(skill_catalog, skill_authorization)?
    .with_prompts(prompt_catalog, prompt_renderer, prompt_authorization)?
    .with_completion_provider(completion_provider)?
    .build()?;

let service = McpServerHttpService::with_admission_control(
    app,
    http_options,
    McpServerAuthentication::bearer(authenticator, bearer_challenge),
    admission_control,
)?;
```

把 `service` 挂载到 Axum、Hyper 或其他兼容 Tower Host 的一个精确 Endpoint。不要在
每个请求中重建 Registry；必须在接收流量前构建并校验全部 Definition。

## 验证证据

冻结的官方 Runner 为 `@modelcontextprotocol/conformance@0.2.0-alpha.11`，Source
Revision 为 `c321dd32035556e6769d3724a8ee97d87c3faaac`，Requirement Revision 为
`2026-07-28`。

严格 Transport Fixture 精确通过全部 37 个计分 Server Scenario：114 项 Assertion
Success、5 项显式 Capability Skip、1 项 SSE Info、0 Failure、0 Warning。另有 3 个
Pending 且不计分的 JSON Schema/HTTP Header Gate，共 32 项 Assertion Success、0
Failure、0 Warning。StateKnot Application Surface 另通过真实 HTTP Boundary Test，
覆盖 Capability Discovery、Pagination、Authorization Ordering、Schema Validation、
Tool Dispatch、Resource Read、Prompt Rendering、Completion、MRTR Binding 与 Result
Limit。

```console
cargo test -p stateknot-integrations mcp_server_ --locked
bash conformance/mcp-server/run-2026-07-28.sh
```

官方 Fixture 为匹配 Conformance Inventory 而设计，并使用生产
`McpServerHttpService` Transport。它是 Acceptance Evidence，不是 Application
Template。StateKnot 自有 Registry 与 Policy Layer 由上述独立 HTTP Test 覆盖。完整
Inventory 与 Claim Rule 见 [MCP Conformance 状态](mcp-conformance.zh-CN.md)。

## 不做声明的能力

- MCP Tasks、Task Lifecycle、Task Notification 或 Task/MRTR Composition；
- Deprecated Stateful Session 或 Legacy `initialize` Flow；
- 内置 OAuth Authorization Server 或 Identity Provider；
- Dynamic Catalog Mutation 或 List-changed Notification；
- Dynamic MCP Skills Manifest、MCP Apps 或其他 Extension；
- Stable Rust API、crates.io Release 或 SDK-tier Certification；
- 整个 StateKnot Framework 的 Production Qualification。
