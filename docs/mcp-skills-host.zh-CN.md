<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# MCP Skills Client 与 Host Profile

> 状态：已实现的 pre-alpha **静态 Manifest Client 与 Host** Profile；Public API
> 尚不稳定。<br>
> Extension：Final SEP-2640，`io.modelcontextprotocol/skills`。<br>
> 基础协议：MCP `2026-07-28`。<br>
> 明确边界：Dynamic Manifest、远端 Directory Read、持久化审批、磁盘物化、签名与
> 自动 Discovery-to-Agent 组合尚未实现，也不做支持声明。

StateKnot 可以发现并激活远端静态 Agent Skill，同时不把文件或 Frontmatter 变成权限。
Client 先校验 Final SEP-2640 Wire Contract；Host 再分配本地 Origin，要求应用 Policy
进行全新审批，只从同一个 Client Binding 按需读取 Manifest 中列出的文件，精确校验
Size 与 SHA-256，并在完整 Acting Window 内保留已批准的 Entry。

规范来源为 [MCP Skills Extension](https://modelcontextprotocol.io/extensions/skills/overview)、
[稳定版 Extension 规范](https://github.com/modelcontextprotocol/ext-skills/blob/main/specification/stable/skills.mdx)
与 [Agent Skills 格式](https://agentskills.io/specification)。

## 已实现的生命周期

1. 使用 `McpClientOptions::for_skills()` 连接。只有该显式 Profile 会声明 Skills
   Client Extension。它还会在限制并发为 2 的前提下，提高有界 JSON/SSE Ceiling，
   以容纳最坏情况下转义后的 16 MiB Text Resource。
2. `server/discover` 必须同时声明基础 Resources Capability 与 Skills Extension，
   Client 才会调用 `skills/list`、`skills/get` 或 `resources/read`。
3. 只接受 `resultType: "complete"` 的静态 Manifest。每个 Entry 必须包含
   `SKILL.md`，使用 Root 内的规范 Path 与小写 `sha256:` Digest，并满足 512 文件、
   16 MiB、Frontmatter、Pagination 与 Catalog Ceiling。`"dynamic"` 会失败关闭。
4. Skill 身份是 `(Host 分配的 Origin, 精确 Skill URI)`；Name 与 Server 自报信息
   永远不能充当身份。
5. `McpSkillHostPolicy::approve_activation` 会在读取任何 Skill 文件之前收到精确
   Entry、完整 Manifest Binding Digest、Origin、Source、Description 与不可信的
   `allowed-tools` 请求；只有新的审批成功后才继续。
6. 从同一个 MCP Client 延迟读取 `SKILL.md`，校验 URI、Byte Size 与 Digest，解析
   严格且无重复 Key 的 Frontmatter，并要求它与已审批 Entry 逐字段一致。
7. `McpActivatedSkill` 在 Acting Window 内持续持有 Entry。文件读取只能命中这份
   完整 Manifest；Directory View 在本地推导，因此 Server 不能在审批后添加文件。
8. Nested `SKILL.md` 属于新的 Activation：它必须先列在 Parent Manifest 中，并且
   需要一次独立的新审批。
9. 使用 `McpSkillBoundTool` 把已激活 Skill 绑定到精确的 Tool Owner/Name/Version
   与完整 Descriptor 的规范 Digest；Host 必须显式声明该 Tool 是否可能在本机执行代码。
10. 每次执行与恢复核对 Provider Call 前都重新经过 Policy。请求会区分两种 Operation，
    并携带精确 Tool Identity、Descriptor Digest、Host-code Exposure、Origin、Manifest
    Digest、Activation ID、Tenant/Run/Invocation/Attempt 关联、存在时的已提交 Origin
    Event，以及有界且绑定 Schema 的 Input。`allowed-tools` 只是不可信的对照数据。
11. 向普通不可变 `ToolProviderRegistryBuilder` 注册 Guarded Adapter，而不是原始
    Provider；持久执行的 Attempt-start、Terminal Evidence、Retry 与 Reconciliation
    语义保持不变。

## Host 构建

```rust,ignore
let client = McpClient::connect(
    endpoint,
    McpClientIdentity::new("orders-agent", env!("CARGO_PKG_VERSION"))?,
    authorization,
    McpClientOptions::for_skills(),
)
.await?;

let host = McpSkillHost::new(
    client,
    McpSkillOrigin::new("production/orders-mcp")?,
    Arc::new(application_skill_policy),
    McpSkillHostOptions::default(),
)?;

let catalog = host.list_skills().await?; // 只取 Metadata，不预取文件
let selected = catalog
    .find_uri("skill://incident-review/SKILL.md")
    .ok_or(AppError::SkillUnavailable)?;
let active = Arc::new(host.activate(selected).await?); // 新审批，然后校验内容

// 所有 Model-visible Message 都要把 identity 与 bytes 一起保留。
let file = active.read_file("references/checklist.md").await?;
model_context.push_untrusted_skill_file(file.identity(), file.bytes())?;

// 在构建 Registry 前，把精确 Provider 冻结到该 Activation。
let guarded = Arc::new(McpSkillBoundTool::new(
    Arc::clone(&active),
    ticket_create_tool, // Arc<dyn ErasedTool>
    McpSkillHostCodeExecution::NotPossible,
)?);
tool_registry.register(guarded)?;

// 普通持久执行器现在会对每次调用和恢复核对执行授权。
let tools = tool_registry.build();
```

应用 Policy 是用户/策略交互边界。生产实现应展示 Host 分配的 Origin、精确 URI、
Manifest Digest、文件数与 Byte 数、Description、Activation Source、请求的 Tool、
精确 Registered Tool Identity 与 Descriptor Digest、Operation 和 Host-code Exposure；
同时展示持久执行关联与精确参数；把决定绑定到这些事实，只记录不泄露敏感信息的
Audit Evidence，并在 Policy Authority 不可用时拒绝。禁止只按 Skill Name 或
`allowed-tools` 字符串自动批准，也禁止在没有应用层脱敏的情况下记录
`McpSkillToolInvocation::input()`。

## Cache 与重启行为

已校验文件可以进入私有且不可变的进程内存 Cache；Key 精确绑定 Client Binding、Host
分配的 Origin、Resource URI 与 Digest，并强制 Entry/Byte Ceiling。Cache 满时会跳过
写入，不会通过驱逐或降低校验强度来腾出空间。Cache 中的 `Arc<[u8]>` 不可变，因此
命中时仍沿用首次校验的精确结果。

StateKnot 不会把远端 Skill Byte 写入文件系统 Skill Discovery Path，也不会持久化
审批。重启后 Client Binding、Acting Window、Permit、Approval 与 Memory Cache 全部
失效；后续 Activation 必须重新发现并审批当前完整 Manifest。这是安全的重启合约，
不是持久化审批。Worker 只能在全新 Activation 后重建相同的 Guarded Tool Descriptor；
已经开始的持久 Tool Attempt 仍保留常规 Recovery Ledger，但 Reconciliation 会在
Provider I/O 前再次执行精确的 Skill Policy 检查。

## 安全边界

- Digest 只能证明内容与已发布 Manifest 一致，不能证明作者身份、安全性或可信度。
- `McpVerifiedSkillFile::identity()` 必须保留在 Model Context 与 Audit Trail 中。Library
  返回带 Origin 的类型，但无法强制 Model Adapter 正确携带它。
- 低层 `McpClient::read_skill_resource` 返回值仍不可信；只有通过 Activated Host
  返回的 Byte 才已对照保留且已审批的 Manifest 校验。
- `McpSkillBoundTool` 会在普通 `ErasedTool` Boundary 内消费不可 Clone 的 Permit。
  Authorization Denial 对 Write 生成 `NotStarted` Evidence（Read 为 `NotApplicable`），
  且绝不会调用底层 Provider。
- Executable Registry 必须只注册 Guarded Adapter，不能同时保留 Raw Provider；绕过
  Adapter 直接 Dispatch 属于 Skill Authorization Boundary 之外的 Host 配置错误。
- Entry 绑定到一个 Client Instance，因此跨 Server 复用会失败关闭。应用必须为该
  Binding 分配稳定且唯一的 Origin Label；不能使用 Server 自报信息生成 Origin。
- 校验失败后不会自动重取、替换或执行；调用方必须重新开始 Discovery 与审批流程。

## 验证

```console
cargo test -p stateknot-integrations mcp_skill_host --locked
cargo test -p stateknot-integrations --test mcp_skills_host --locked
```

Loopback Contract Suite 覆盖 Lazy Listing、Capability 声明、有界 Pagination、Host
Origin 保留、先审批后读取、Digest/Size/Frontmatter 精确对账、不可变 Cache Hit、
Manifest-only Read、本地 Directory View、Nested Skill 新审批、Per-call Tool
Authorization、精确 Descriptor/Identity Disclosure、Execution/Reconciliation 分开审批、
Immutable Registry 兼容、Denial 早于 Provider Dispatch，以及内容漂移时的失败关闭。

## 不做声明的能力

- Dynamic Manifest 或 `resources/directory/read`；
- 持久化审批、持久化 Acting Window、Disk Cache 或文件系统 Skill 安装；
- Signature Verification、Provenance、Marketplace Trust、恶意内容检测或 Sandbox；
- Tool 自动发现、`allowed-tools` Pattern 解释或动态 Discovery-to-Agent 组合；
- Stable Rust API、crates.io Release 或官方 Skills Extension Conformance。
