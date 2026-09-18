<!--
Copyright 2026 StateKnot contributors
SPDX-License-Identifier: Apache-2.0
-->

# MCP Skills Server Profile

> 状态：已实现的 pre-alpha **服务端** Profile；Public API 尚不稳定。<br>
> Extension：Final SEP-2640，`io.modelcontextprotocol/skills`。<br>
> 基础协议：MCP `2026-07-28`。<br>
> 配套能力：独立的[静态 Client 与 Host Profile](mcp-skills-host.zh-CN.md)负责校验和
> 激活 Skill，不扩大本 Server 声明。

StateKnot 现在可以通过 Final MCP Skills Extension 发布静态 Agent Skill。这不是把
`SKILL.md` 当普通文件暴露的简化方案。Server 会在启动时冻结完整 Skill，从实际返回
的精确字节生成 Manifest，并让 `skills/list`、`skills/get` 与 `resources/read` 共享
同一份不可变快照。

规范来源为 [MCP Skills Extension](https://modelcontextprotocol.io/extensions/skills/overview)、
[稳定版 Extension 规范](https://github.com/modelcontextprotocol/ext-skills/blob/main/specification/stable/skills.mdx)
与 [Agent Skills 格式](https://agentskills.io/specification)。

## 已实现的 Wire Contract

配置 Skill Catalog 后，`server/discover` 会同时声明基础 `resources` Capability 与空的
`io.modelcontextprotocol/skills` Extension Object。空 Object 明确表示不支持可选的
Directory Read。

Server 已实现：

- 有界、可缓存、带分页的 `skills/list`；
- 不依赖列表可见性的精确 `skills/get`；
- 每个文件恰好出现一次的完整静态 Manifest；
- 基于原始字节的 `sha256:` Digest 与精确 Byte Size；
- 通过 `resources/read` 返回 Text 或 Base64 Resource；
- 确定性的 Skill/File 排序；
- Extension Result 的 `resultType: "complete"`、`ttlMs` 与 `cacheScope`；
- Unknown、Scope-hidden、Policy-denied 或参数错误的 Skill 查询统一返回
  JSON-RPC `-32602`；Policy Backend 不可用仍属于 Internal Error。

本 Profile 明确不提供 `resources/directory/read`、`directoryRead: true`、Dynamic
Manifest、Catalog Mutation 与 List-changed Notification。

## 启动校验

以下任一条件不满足时，`McpServerSkillDefinition` 会在接收流量前拒绝启动：

- 缺少 `SKILL.md`，或文件不是 UTF-8 Text，或 YAML Frontmatter 非法；
- 任意层级 YAML Mapping 存在重复 Key，或 Alias 展开后超过物化 Value 预算；
- `name` 或其他已定义 Agent Skills Field 不符合格式；
- URI Parent 与 Frontmatter `name` 不一致；
- Path 为 Absolute、Empty，包含 Traversal、反斜线、Control Byte 或 URI-unsafe Segment；
- Path、Resource URI 或 Skill URI 冲突；
- 单个 Skill 超过 512 个文件或 16 MiB；
- Registry 超出配置的 Skill、File 或 Aggregate Byte Ceiling。

当前 Agent Skills Revision 未定义的 Frontmatter Field 会作为 JSON-compatible Value
完整保留，不会被静默丢弃；但它们仍然只是数据，不会成为权限依据。

## Authorization 与 Cache Isolation

Authentication 仍由 `McpServerHttpService` 完成。随后
`McpServerSkillAuthorization` 接收已认证 Principal、精确 Operation 与不可信 URI。
直接 `skills/get` 与 `resources/read` 会先执行 Policy，再通过 Lookup 暴露存在性；
Discovery 同时按 Required Scope 与动态 Policy 过滤。

Unknown、Scope-hidden 与 Policy-denied Skill 会收敛到同一类 Public Error。存在
Scope-filtered Skill 或 Principal-sensitive/Dynamic Policy 时，启动阶段会拒绝 Public
Cache Metadata。Authorization Policy 默认只能使用 Private Cache；只有显式证明其决策
在完整 TTL 内对所有 Principal 都相同时，才能启用 Public Cache。Private Cursor 会绑定
冻结的 Catalog Digest、Principal Subject、Canonical Scope Set、Surface 与 Offset。

Digest 只能证明返回内容与同一 Server 发布的 Manifest 一致，不能证明作者身份或内容
可信。消费方 Host 仍必须把 Skill 视为不可信输入。StateKnot 独立的
[Client 与 Host Profile](mcp-skills-host.zh-CN.md)为完整静态 Manifest 实现了该校验与
审批边界。

## 构建轮廓

```rust,ignore
let skill = McpServerSkillDefinition::new(
    "skill://code-review/SKILL.md",
    [
        McpServerSkillFile::text(
            "SKILL.md",
            "text/markdown",
            include_str!("skills/code-review/SKILL.md"),
        )?,
        McpServerSkillFile::text(
            "references/checklist.md",
            "text/markdown",
            include_str!("skills/code-review/references/checklist.md"),
        )?,
    ],
)?;

let mut skills = McpServerSkillCatalogBuilder::default();
skills.register(skill)?;

let app = McpServerApplicationBuilder::new(options)
    .with_skills(skills.build()?, skill_authorization)?
    .build()?;
```

Skill Byte 应来自经过审查的 Build Input 或其他由部署方控制的 Source；不要按请求重建
Catalog。

## 验证

```console
cargo test -p stateknot-integrations mcp_server_skill --locked
cargo test -p stateknot-integrations --test mcp_skills_server --locked
```

HTTP Contract Suite 覆盖 Extension Negotiation、Pagination、Direct Lookup、Text/Binary
Digest/Size 精确对账、Scope 隐藏、Authorization Ordering、错误参数、Path Traversal、
YAML 重复键、有界 Alias 展开与 Public Cache 拒绝。

## 不做声明的能力

- 本 Server Surface 的 Dynamic Manifest 或 Directory Read；
- 任何“Server 能交付内容就意味着内容可信或可执行”的声明；
- Signature、Provenance、Marketplace Trust 或 Content Safety；
- Stable Rust API、crates.io Release 或完整框架 Conformance。
