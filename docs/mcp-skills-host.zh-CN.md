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
> Tool Runtime 自动集成尚未实现，也不做支持声明。

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
9. 每个精确 Tool Call 都要重新经过 Policy。`allowed-tools` 只作为不可信请求展示，
   不会自动授权。返回的 Permit 无法由外部构造、不可 Clone，并借用当前 Active Skill；
   集成方 Executor 必须消费它。

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
let active = host.activate(selected).await?; // 新审批，然后校验内容

// 所有 Model-visible Message 都要把 identity 与 bytes 一起保留。
let file = active.read_file("references/checklist.md").await?;
model_context.push_untrusted_skill_file(file.identity(), file.bytes())?;

// Tool Adapter 必须要求并消费这个精确 Permit。
let permit = active.authorize_tool_call("ticket/create", false).await?;
tool_executor.execute_with_skill_permit(permit, arguments).await?;
```

应用 Policy 是用户/策略交互边界。生产实现应展示 Host 分配的 Origin、精确 URI、
Manifest Digest、文件数与 Byte 数、Description、Activation Source 与请求的 Tool；
把决定绑定到这些事实，记录不泄露敏感信息的 Audit Result，并在 Policy Authority
不可用时拒绝。禁止仅按 Skill Name 自动批准。

## Cache 与重启行为

已校验文件可以进入私有且不可变的进程内存 Cache；Key 精确绑定 Client Binding、Host
分配的 Origin、Resource URI 与 Digest，并强制 Entry/Byte Ceiling。Cache 满时会跳过
写入，不会通过驱逐或降低校验强度来腾出空间。Cache 中的 `Arc<[u8]>` 不可变，因此
命中时仍沿用首次校验的精确结果。

StateKnot 不会把远端 Skill Byte 写入文件系统 Skill Discovery Path，也不会持久化
审批。重启后 Client Binding、Acting Window、Permit、Approval 与 Memory Cache 全部
失效；后续 Activation 必须重新发现并审批当前完整 Manifest。这是安全的重启合约，
不是 Durable Approval。

## 安全边界

- Digest 只能证明内容与已发布 Manifest 一致，不能证明作者身份、安全性或可信度。
- `McpVerifiedSkillFile::identity()` 必须保留在 Model Context 与 Audit Trail 中。Library
  返回带 Origin 的类型，但无法强制 Model Adapter 正确携带它。
- 低层 `McpClient::read_skill_resource` 返回值仍不可信；只有通过 Activated Host
  返回的 Byte 才已对照保留且已审批的 Manifest 校验。
- Execution Permit 是强制执行 Primitive，不是透明 Tool Dispatch；Tool Adapter 必须
  通过类型要求它，并为精确调用消费它。现有 Tool Runtime 不会被静默扩大权限。
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
Authorization，以及 Denial 与内容漂移时的失败关闭。

## 不做声明的能力

- Dynamic Manifest 或 `resources/directory/read`；
- 持久化审批、持久化 Acting Window、Disk Cache 或文件系统 Skill 安装；
- Signature Verification、Provenance、Marketplace Trust、恶意内容检测或 Sandbox；
- 与每一种 StateKnot Tool Executor 的自动集成；
- Stable Rust API、crates.io Release 或官方 Skills Extension Conformance。
