// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

import type { Locale } from "./i18n";
import { localizePath } from "./i18n";

export interface DocumentationPage {
  readonly title: string;
  readonly description: string;
  readonly href: string;
  readonly search: string;
}

export interface DocumentationSection {
  readonly label: string;
  readonly pages: readonly DocumentationPage[];
}

const englishDocumentationSections: readonly DocumentationSection[] = [
  {
    label: "Start",
    pages: [
      {
        title: "Overview",
        description:
          "Choose the right entry point and understand the pre-alpha boundary.",
        href: "/docs/",
        search: "docs documentation overview start pre-alpha",
      },
      {
        title: "Getting started",
        description:
          "Install the pinned toolchain and validate the repository locally.",
        href: "/docs/getting-started/",
        search: "getting started install rust cargo clone tutorial validate",
      },
      {
        title: "Core contract examples",
        description:
          "Compile the first Agent, typed Tool, Model stream, and explicit protocol mapping without runtime dependencies.",
        href: "/docs/core-contracts/",
        search:
          "core contracts examples first agent typed tool model stream protocol adapter dependency boundary msrv",
      },
      {
        title: "Typed Agent",
        description:
          "Generate and pin typed schemas, then bind the first-party OpenAI and Anthropic adapters.",
        href: "/docs/typed-agent/",
        search:
          "typed agent builder schema openai responses anthropic messages adapter tutorial",
      },
      {
        title: "Durable admission",
        description:
          "Atomically commit authenticated intent, database time, the first event, and the initial checkpoint.",
        href: "/docs/admission/",
        search:
          "agent admission atomic idempotency retry database checkpoint policy authentication",
      },
      {
        title: "Durable runs and results",
        description:
          "Submit with a durable ingress key, recover the original run, and read a verified public result.",
        href: "/docs/runs/",
        search:
          "agent run result durable idempotency key submit poll terminal snapshot retry",
      },
    ],
  },
  {
    label: "Understand",
    pages: [
      {
        title: "Compile a graph",
        description:
          "Build a schema-pinned graph, validate its topology, and freeze its canonical digest.",
        href: "/docs/concepts/graphs/",
        search:
          "graph compile compiler node route reducer schema canonical digest tutorial",
      },
      {
        title: "Subgraphs and loops",
        description:
          "Compose shared-state subgraphs, finite loops, and explicit recovery-safe return paths.",
        href: "/docs/graph-composition/",
        search:
          "graph subgraph composition loop bounded iteration return exhaustion shared state recovery",
      },
      {
        title: "Durability model",
        description:
          "Learn how journal, checkpoint, lease, fence, and recovery evidence fit together.",
        href: "/docs/concepts/durability/",
        search: "concept durability journal checkpoint lease fence recovery",
      },
      {
        title: "Durable Graph runtime",
        description:
          "Register exact executable code, replay noninitial state, and drive deterministic bounded sibling batches under one fence.",
        href: "/docs/runtime/",
        search:
          "runtime graph driver executable registry replay lease fence crash recovery handoff parallel sibling deterministic batch",
      },
      {
        title: "Durable Agent Loop",
        description:
          "Commit lifecycle handoffs and execute one tenant-scoped durable scheduling quantum.",
        href: "/docs/agent-loop/",
        search:
          "agent loop lifecycle wait terminal failure evidence tenant scheduler lost acknowledgement",
      },
      {
        title: "Durable invocations",
        description:
          "Execute exact model and tool attempts with durable starts, staged ordered Tool coordination, streaming validation, and no-dispatch terminal recovery.",
        href: "/docs/invocations/",
        search:
          "model tool provider registry invocation executor streaming budget terminal recovery lost acknowledgement",
      },
      {
        title: "Provider-native Agent",
        description:
          "Compile and operate the durable multi-turn graph with bounded output repair, ordered parallel read-only Tools, write barriers, policy, accounting, and cancellation evidence.",
        href: "/docs/provider-native-agent/",
        search:
          "provider native agent graph multi turn structured output repair malformed tool parallel read only write barrier policy accounting cancellation recovery transcript",
      },
    ],
  },
  {
    label: "Integrate",
    pages: [
      {
        title: "AgentService v1",
        description:
          "Authorize and expose exact durable Agent revisions through the versioned embedding boundary.",
        href: "/docs/agent-service/",
        search:
          "agent service api submit read cancel cancellation authorization idempotency ingress",
      },
      {
        title: "Agent HTTP v1",
        description:
          "Submit, recover, read and cancel through authenticated bounded JSON HTTP.",
        href: "/docs/agent-http/",
        search:
          "agent http bearer authentication submission lookup cancel retry recovery limits sse events cursor replay",
      },
      {
        title: "General MCP Tool client",
        description:
          "Discover and call bounded stateless MCP 2026-07-28 Tools with JSON/SSE, OAuth, custom headers, and mediated MRTR.",
        href: "/docs/mcp-client/",
        search:
          "mcp general client tool stateless discover catalog sse oauth custom header mrtr request state conformance",
      },
      {
        title: "MCP OAuth client",
        description:
          "Bind one MCP resource to challenge-driven discovery, PKCE, issuer validation, durable credentials, and bounded replay.",
        href: "/docs/mcp-oauth/",
        search:
          "mcp oauth authorization pkce protected resource metadata cimd dcr issuer scope token callback durable credential",
      },
      {
        title: "MCP Remote Tool",
        description:
          "Bind one strict stateless MCP 2026-07-28 Tool with pinned identity, schemas, durable dispatch, and reconciliation.",
        href: "/docs/mcp-remote-tool/",
        search:
          "mcp remote tool 2026 07 28 stateless discovery schema authorization ambiguous write postgres reconciliation",
      },
      {
        title: "MCP Server profile",
        description:
          "Expose bounded Tools, Resources, Prompts, Completion, and MRTR behind the strict stateless production boundary.",
        href: "/docs/mcp-server/",
        search:
          "mcp server tool resource template prompt completion mrtr authentication authorization conformance",
      },
      {
        title: "MCP Skills",
        description:
          "Publish and activate Final SEP-2640 static Skills with immutable manifests, origin-scoped approval, and verified lazy reads.",
        href: "/docs/mcp-skills/",
        search:
          "mcp skills sep 2640 server client host skill manifest digest origin approval activation resources read",
      },
      {
        title: "MCP conformance status",
        description:
          "See the frozen official runner inventory, implemented evidence, and exact boundary of current MCP claims.",
        href: "/docs/mcp-conformance/",
        search:
          "mcp conformance official runner requirements evidence client server claim 2026 07 28",
      },
      {
        title: "A2A 1.0 Client",
        description:
          "Discover and pin one agent, call all eleven operations, and reconcile durable unknown sends only through attested remote guarantees.",
        href: "/docs/a2a-client/",
        search:
          "a2a 1.0 client remote agent durable outbound discovery card interface pin jsonrpc http json sse unknown reconcile",
      },
      {
        title: "Durable artifact storage",
        description:
          "Materialize terminal A2A parts into immutable PostgreSQL metadata and private integrity-checked object bytes.",
        href: "/docs/artifacts/",
        search:
          "artifact storage a2a task postgres s3 object integrity retrieval multipart authorization",
      },
      {
        title: "A2A 1.0 Server",
        description:
          "Expose bounded HTTP+JSON and JSON-RPC/SSE through identity-first policy and a durable service contract.",
        href: "/docs/a2a-server/",
        search:
          "a2a 1.0 server agent card task artifact streaming subscription push jsonrpc http json authorization",
      },
      {
        title: "A2A conformance status",
        description:
          "Inspect the frozen official TCK, audited harness patch, exact result, and server-only claim boundary.",
        href: "/docs/a2a-conformance/",
        search:
          "a2a conformance official tck evidence 177 265 jsonrpc http json skip claim",
      },
    ],
  },
  {
    label: "Operate",
    pages: [
      {
        title: "Fair scheduling",
        description:
          "Configure replica-safe weighted tenant selection, exact starvation bounds, rollout, and retention.",
        href: "/docs/fair-scheduling/",
        search:
          "fair scheduler weighted tenant starvation bound reservation retention rollout",
      },
      {
        title: "PostgreSQL provider",
        description:
          "Configure, migrate, verify, and test the implemented durability provider.",
        href: "/docs/postgresql/",
        search:
          "postgres postgresql provider migration tls pool recovery operations",
      },
      {
        title: "Host qualification",
        description:
          "Record bounded capacity, recovery and fault evidence without turning CI into a production SLO claim.",
        href: "/docs/qualification/",
        search:
          "host qualification benchmark capacity recovery fault injection hdr histogram report ci release slo evidence",
      },
    ],
  },
  {
    label: "Project",
    pages: [
      {
        title: "Implementation status",
        description:
          "See what is implemented, in progress, and deliberately not claimed.",
        href: "/docs/status/",
        search:
          "status capability matrix roadmap implemented planned mcp a2a agent service agent loop",
      },
    ],
  },
] as const;

const chineseDocumentationSections: readonly DocumentationSection[] = [
  {
    label: "开始",
    pages: [
      {
        title: "文档概览",
        description: "选择正确的阅读入口，并了解当前 pre-alpha 边界。",
        href: "/docs/",
        search: "文档 概览 开始 pre-alpha",
      },
      {
        title: "快速开始",
        description: "安装锁定的工具链，并在本地验证仓库。",
        href: "/docs/getting-started/",
        search: "快速开始 安装 rust cargo 克隆 教程 验证",
      },
      {
        title: "Core 合约示例",
        description:
          "无需 Runtime 依赖，编译首个 Agent、类型化 Tool、Model Stream 与显式协议映射。",
        href: "/docs/core-contracts/",
        search:
          "core 合约 示例 首个 agent 类型化 tool model stream 协议 adapter 依赖 边界 msrv",
      },
      {
        title: "强类型 Agent",
        description:
          "生成并固定类型化 Schema，再绑定第一方 OpenAI 与 Anthropic Adapter。",
        href: "/docs/typed-agent/",
        search:
          "强类型 agent builder schema openai responses anthropic messages adapter 教程",
      },
      {
        title: "持久化准入",
        description:
          "原子提交已认证 Intent、数据库时间、首 Event 与初始 Checkpoint。",
        href: "/docs/admission/",
        search:
          "agent admission 原子 幂等 重试 数据库 checkpoint policy 认证 接纳",
      },
      {
        title: "可恢复 Run 与 Result",
        description:
          "使用持久化 Ingress Key 提交、恢复原 Run，并读取经过验证的公开 Result。",
        href: "/docs/runs/",
        search:
          "agent run result 持久化 幂等 key submit 轮询 terminal snapshot 重试",
      },
    ],
  },
  {
    label: "理解",
    pages: [
      {
        title: "编译确定性 Graph",
        description:
          "构建 Schema-pinned Graph，验证拓扑，并冻结 Canonical Digest。",
        href: "/docs/concepts/graphs/",
        search:
          "graph 编译 compiler 节点 route reducer schema canonical 摘要 教程",
      },
      {
        title: "子图与循环",
        description:
          "组合共享状态子图、有界循环与能够在中断后恢复的显式返回路径。",
        href: "/docs/graph-composition/",
        search:
          "graph subgraph composition loop 子图 循环 有界 轮次 返回 耗尽 共享 状态 恢复",
      },
      {
        title: "持久执行模型",
        description:
          "理解 Journal、Checkpoint、Lease、Fence 与恢复证据如何协同。",
        href: "/docs/concepts/durability/",
        search:
          "概念 持久执行 可恢复 持久化 持久性 durable execution durability persistence journal checkpoint lease fence 恢复",
      },
      {
        title: "可恢复 Graph Runtime",
        description:
          "注册精确可执行代码、重放非初始状态，并在一个 Fence 下驱动确定性有界 Sibling Batch。",
        href: "/docs/runtime/",
        search:
          "runtime graph driver 可执行 注册表 重放 租约 fence 崩溃 恢复 handoff 并行 sibling 确定性 batch",
      },
      {
        title: "可恢复 Agent Loop",
        description:
          "提交 Lifecycle Handoff，并执行一个 Tenant-scoped 可恢复调度 Quantum。",
        href: "/docs/agent-loop/",
        search:
          "agent loop lifecycle wait terminal failure evidence 租户 scheduler lost ack 调度",
      },
      {
        title: "可恢复调用执行",
        description:
          "通过持久化 Start、分阶段有序 Tool Coordination、Streaming 校验与 No-dispatch Terminal Recovery 执行精确 Model/Tool Attempt。",
        href: "/docs/invocations/",
        search:
          "model tool provider registry invocation executor streaming budget terminal recovery lost ack 调用 执行",
      },
      {
        title: "Provider-native Agent",
        description:
          "编译并运维支持有界 Output Repair、有序 Parallel Read-only Tool 与 Write Barrier 的可恢复多轮 Graph，并固定 Policy、Accounting 与 Cancellation Evidence。",
        href: "/docs/provider-native-agent/",
        search:
          "provider native agent graph 多轮 structured output repair 修复 malformed tool parallel read only write barrier policy accounting cancellation recovery transcript 恢复",
      },
    ],
  },
  {
    label: "集成",
    pages: [
      {
        title: "AgentService v1",
        description: "通过带版本的嵌入式边界授权并暴露精确版本的可恢复 Agent。",
        href: "/docs/agent-service/",
        search:
          "agent service api 提交 读取 取消 authorization idempotency ingress",
      },
      {
        title: "Agent HTTP v1",
        description:
          "通过经过认证、有资源上限的 JSON HTTP 接口提交、找回、读取与取消 Agent。",
        href: "/docs/agent-http/",
        search:
          "agent http bearer 认证 提交 查询 取消 重试 恢复 限流 sse 事件 游标 断线续传",
      },
      {
        title: "通用 MCP Tool Client",
        description:
          "通过 JSON/SSE、OAuth、Custom Header 与受控 MRTR 发现和调用有界 Stateless MCP 2026-07-28 Tool。",
        href: "/docs/mcp-client/",
        search:
          "mcp 通用 client tool stateless 发现 catalog sse oauth custom header mrtr request state conformance",
      },
      {
        title: "MCP OAuth Client",
        description:
          "将一个 MCP Resource 绑定到 Challenge-driven Discovery、PKCE、Issuer 校验、持久化 Credential 与有界 Replay。",
        href: "/docs/mcp-oauth/",
        search:
          "mcp oauth authorization pkce protected resource metadata cimd dcr issuer scope token callback 持久化 credential",
      },
      {
        title: "MCP Remote Tool",
        description:
          "固定 Identity、Schema、可恢复 Dispatch 与 Reconciliation，绑定严格 Stateless MCP 2026-07-28 Tool。",
        href: "/docs/mcp-remote-tool/",
        search:
          "mcp remote tool 2026 07 28 stateless discovery schema authorization 不确定 写入 postgres 对账",
      },
      {
        title: "MCP Server Profile",
        description:
          "在严格 Stateless 生产边界后暴露有界 Tools、Resources、Prompts、Completion 与 MRTR。",
        href: "/docs/mcp-server/",
        search:
          "mcp server tool resource template prompt completion mrtr authentication authorization conformance 服务端",
      },
      {
        title: "MCP Skills",
        description:
          "通过不可变 Manifest、Origin-scoped 审批与校验后延迟读取发布并激活 Final SEP-2640 静态 Skill。",
        href: "/docs/mcp-skills/",
        search:
          "mcp skills sep 2640 server client host skill manifest digest origin approval activation resources read 服务端 客户端 技能",
      },
      {
        title: "MCP Conformance 状态",
        description:
          "查看冻结的官方 Runner 清单、已实现证据与当前 MCP 声明的精确边界。",
        href: "/docs/mcp-conformance/",
        search:
          "mcp conformance 官方 runner requirement 证据 client server 声明 2026 07 28",
      },
      {
        title: "A2A 1.0 Client",
        description:
          "发现并固定一个 Agent，调用全部 11 个 Operation，并只依据已背书的远端保证对持久化 Unknown Send 执行 Reconciliation。",
        href: "/docs/a2a-client/",
        search:
          "a2a 1.0 client remote agent 持久化 outbound discovery card interface pin jsonrpc http json sse unknown reconcile 客户端",
      },
      {
        title: "Artifact 持久化存储",
        description:
          "将 Terminal A2A Part 物化为不可变 PostgreSQL Metadata 与私有、经过完整性校验的 Object Bytes。",
        href: "/docs/artifacts/",
        search:
          "artifact storage a2a task postgres s3 object 完整性 读取 multipart authorization 持久化",
      },
      {
        title: "A2A 1.0 Server",
        description:
          "通过 Identity-first Policy 与可恢复 Service 合约暴露有界 HTTP+JSON 和 JSON-RPC/SSE。",
        href: "/docs/a2a-server/",
        search:
          "a2a 1.0 server agent card task artifact streaming subscription push jsonrpc http json authorization 服务端",
      },
      {
        title: "A2A Conformance 状态",
        description:
          "查看冻结的官方 TCK、已审计 Harness Patch、精确结果与 Server-only 声明边界。",
        href: "/docs/a2a-conformance/",
        search:
          "a2a conformance 官方 tck 证据 177 265 jsonrpc http json skip 声明",
      },
    ],
  },
  {
    label: "运维",
    pages: [
      {
        title: "公平调度",
        description:
          "配置 Replica-safe 加权 Tenant Selection、精确 Starvation Bound、Rollout 与 Retention。",
        href: "/docs/fair-scheduling/",
        search:
          "公平 调度 fair scheduler weighted tenant starvation bound reservation retention rollout",
      },
      {
        title: "PostgreSQL Provider",
        description: "配置、迁移、校验并测试已实现的持久化 Provider。",
        href: "/docs/postgresql/",
        search: "postgres postgresql provider 迁移 tls 连接池 恢复 运维",
      },
      {
        title: "宿主验证",
        description:
          "记录有界容量、恢复与故障证据，避免把 CI 结果误写成生产 SLO。",
        href: "/docs/qualification/",
        search:
          "宿主 验证 benchmark 容量 恢复 故障 注入 hdr histogram 报告 ci 发布 slo 证据",
      },
    ],
  },
  {
    label: "项目",
    pages: [
      {
        title: "实现状态",
        description: "区分已经实现、正在开发和明确尚未支持的能力。",
        href: "/docs/status/",
        search:
          "状态 能力矩阵 路线图 已实现 计划 mcp a2a agent service agent loop",
      },
    ],
  },
] as const;

export const getDocumentationSections = (
  locale: Locale,
): readonly DocumentationSection[] => {
  const sections =
    locale === "zh-CN"
      ? chineseDocumentationSections
      : englishDocumentationSections;

  return sections.map((section) => ({
    ...section,
    pages: section.pages.map((page) => ({
      ...page,
      href: localizePath(page.href, locale),
    })),
  }));
};

export const getDocumentationPages = (
  locale: Locale,
): readonly DocumentationPage[] =>
  getDocumentationSections(locale).flatMap((section) => section.pages);

export interface CommandEntry extends DocumentationPage {
  readonly group: string;
}

export const getCommandEntries = (locale: Locale): readonly CommandEntry[] => {
  const isChinese = locale === "zh-CN";
  return [
    ...getDocumentationSections(locale).flatMap((section) =>
      section.pages.map((page) => ({
        ...page,
        group: `${isChinese ? "文档" : "Docs"} · ${section.label}`,
      })),
    ),
    {
      group: isChinese ? "首页" : "Home",
      title: isChinese ? "架构图" : "Architecture map",
      description: isChinese
        ? "查看已经实现、正在开发和规划中的边界。"
        : "What exists, what is active, and what remains planned.",
      href: localizePath("/#architecture", locale),
      search: isChinese
        ? "架构图 合约 运行时 持久化"
        : "architecture map contracts runtime durability",
    },
    {
      group: isChinese ? "首页" : "Home",
      title: isChinese ? "当前实现" : "Current cut",
      description: isChinese
        ? "已经实现的切片与明确缺口。"
        : "Implemented slices and explicit gaps.",
      href: localizePath("/#current-cut", locale),
      search: isChinese
        ? "当前 状态 已实现 pre-alpha 缺口"
        : "current status implemented pre-alpha gaps",
    },
    {
      group: isChinese ? "首页" : "Home",
      title: isChinese ? "路线图" : "Roadmap",
      description: isChinese
        ? "从合约走向可验证 v1 的开发顺序。"
        : "The path from contracts to a qualified v1.",
      href: localizePath("/#roadmap", locale),
      search: isChinese
        ? "路线图 agent loop mcp a2a 验证"
        : "roadmap next provider native mcp a2a qualification",
    },
    {
      group: isChinese ? "仓库" : "Repository",
      title: isChinese ? "源代码" : "Source code",
      description: "StateKnot/StateKnot on GitHub.",
      href: "https://github.com/StateKnot/StateKnot",
      search: isChinese
        ? "github 源代码 仓库"
        : "github source repository code",
    },
    {
      group: isChinese ? "仓库" : "Repository",
      title: "RFCs",
      description: isChinese
        ? "架构与持久执行合约草案。"
        : "Draft architecture and durable execution contracts.",
      href: "https://github.com/StateKnot/StateKnot/tree/main/docs/rfcs",
      search: isChinese
        ? "rfc 架构 graph postgresql 合约"
        : "rfc architecture graph postgresql contracts",
    },
    {
      group: isChinese ? "仓库" : "Repository",
      title: isChinese ? "安全策略" : "Security policy",
      description: isChinese
        ? "私密漏洞报告与支持策略。"
        : "Private vulnerability reporting and support policy.",
      href: "https://github.com/StateKnot/StateKnot/security/policy",
      search: isChinese
        ? "安全 漏洞 报告 策略"
        : "security vulnerability reporting policy",
    },
  ];
};

export const normalizeDocumentationPath = (path: string): string =>
  path.endsWith("/") ? path : `${path}/`;
