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
        title: "Durability model",
        description:
          "Learn how journal, checkpoint, lease, fence, and recovery evidence fit together.",
        href: "/docs/concepts/durability/",
        search: "concept durability journal checkpoint lease fence recovery",
      },
      {
        title: "Durable Graph runtime",
        description:
          "Register exact executable code, replay noninitial state, and drive one fenced run.",
        href: "/docs/runtime/",
        search:
          "runtime graph driver executable registry replay lease fence crash recovery handoff",
      },
      {
        title: "Durable Agent Loop",
        description:
          "Commit lifecycle handoffs and execute one tenant-scoped durable scheduling quantum.",
        href: "/docs/agent-loop/",
        search:
          "agent loop lifecycle wait terminal failure evidence tenant scheduler lost acknowledgement",
      },
    ],
  },
  {
    label: "Operate",
    pages: [
      {
        title: "PostgreSQL provider",
        description:
          "Configure, migrate, verify, and test the implemented durability provider.",
        href: "/docs/postgresql/",
        search:
          "postgres postgresql provider migration tls pool recovery operations",
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
          "status capability matrix roadmap implemented planned mcp a2a agent loop",
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
        title: "耐久执行模型",
        description:
          "理解 Journal、Checkpoint、Lease、Fence 与恢复证据如何协同。",
        href: "/docs/concepts/durability/",
        search: "概念 耐久 journal checkpoint lease fence 恢复",
      },
      {
        title: "耐久 Graph Runtime",
        description:
          "注册精确可执行代码、重放非初始状态，并驱动一个带 Fence 的 Run。",
        href: "/docs/runtime/",
        search:
          "runtime graph driver 可执行 注册表 重放 租约 fence 崩溃 恢复 handoff",
      },
      {
        title: "耐久 Agent Loop",
        description:
          "提交 Lifecycle Handoff，并执行一个 Tenant-scoped 耐久调度 Quantum。",
        href: "/docs/agent-loop/",
        search:
          "agent loop lifecycle wait terminal failure evidence 租户 scheduler lost ack 调度",
      },
    ],
  },
  {
    label: "运维",
    pages: [
      {
        title: "PostgreSQL Provider",
        description: "配置、迁移、校验并测试已实现的耐久化 Provider。",
        href: "/docs/postgresql/",
        search: "postgres postgresql provider 迁移 tls 连接池 恢复 运维",
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
        search: "状态 能力矩阵 路线图 已实现 计划 mcp a2a agent loop",
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
        ? "架构图 合约 运行时 耐久"
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
        : "roadmap next agent loop mcp a2a qualification",
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
        ? "架构与耐久执行合约草案。"
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
