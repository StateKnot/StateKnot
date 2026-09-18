// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";

const responsiveAuditWidths = [
  320, 360, 375, 414, 639, 640, 767, 768, 959, 960, 1279, 1280, 1440, 1920,
] as const;

for (const locale of ["en", "zh"] as const) {
  test(`${locale} documents the MCP Skills server-only boundary`, async ({
    page,
  }) => {
    const prefix = locale === "zh" ? "/zh" : "";
    await page.goto(`${prefix}/docs/mcp-skills/`);
    const boundary = page.locator(
      'aside[aria-labelledby="mcp-skills-boundary-title"]',
    );
    await expect(boundary).toContainText("io.modelcontextprotocol/skills");
    await expect(boundary).toContainText("2026-07-28");
    await expect(boundary).toContainText(
      locale === "zh"
        ? "当前声明仅覆盖 Server"
        : "This claim covers the server only",
    );
    await expect(boundary).toContainText(
      locale === "zh" ? "Client Verification" : "Client verification",
    );
    await expect(page.getByText("skills/list", { exact: true })).toBeVisible();
    await expect(page.getByText("skills/get", { exact: true })).toBeVisible();
    await expect(
      page.getByText("resources/read", { exact: true }),
    ).toBeVisible();
    await expect(
      page.locator(`.docs-nav--desktop a[href="${prefix}/docs/mcp-skills/"]`),
    ).toBeVisible();
    await page.setViewportSize({ width: 320, height: 800 });
    await auditHorizontalLayout(page);
  });

  test(`${locale} separates reduced host evidence from release qualification`, async ({
    page,
  }) => {
    await page.goto(`${locale === "zh" ? "/zh" : ""}/docs/qualification/`);
    const boundary = page.locator(
      `aside[aria-labelledby="${locale === "zh" ? "zh-" : ""}qualification-boundary"]`,
    );
    await expect(boundary).toContainText("CiReduced");
    await expect(boundary).toContainText("release_qualified");
    await expect(boundary).toContainText("false");
    await expect(
      page.getByText("informational_missing", { exact: true }),
    ).toBeVisible();
    await expect(
      page.getByText("QualificationRun", { exact: true }),
    ).toBeVisible();
    await expect(
      page.locator("a[href$='0017-host-qualification-harness.md']"),
    ).toHaveCount(1);
    await page.setViewportSize({ width: 320, height: 800 });
    await auditHorizontalLayout(page);
  });

  test(`${locale} documents separately authorized read-only operations`, async ({
    page,
  }) => {
    await page.goto(`${locale === "zh" ? "/zh" : ""}/docs/agent-http/`);
    const section = page.locator('section[aria-labelledby="host-operations"]');
    for (const contract of [
      "AgentHostOperations::start",
      "InspectHost",
      "AgentHostOperationsPolicy",
      "with_host_inspection_scope",
      "/v1/host/status",
      "/v1/host/live",
      "/v1/host/ready",
      "operations.shutdown().await",
    ])
      await expect(section.getByText(contract, { exact: true })).toBeVisible();
    await expect(section).toContainText(
      locale === "zh"
        ? "业务 Read 权限不能替代运维授权"
        : "Business Read permission is insufficient",
    );
    await expect(section).toContainText(
      locale === "zh" ? "没有匿名探针" : "No anonymous probes",
    );
    await expect(
      section.locator("a[href$='0016-protected-agent-host-operations.md']"),
    ).toHaveCount(1);
    await expect(
      section.locator(
        "a[href$='/docs/agent-operations" +
          (locale === "zh" ? ".zh-CN" : "") +
          ".md']",
      ),
    ).toHaveCount(1);
    await page.setViewportSize({ width: 320, height: 800 });
    await auditHorizontalLayout(page);
  });
  test(`${locale} documents composed host ownership and ordered drain`, async ({
    page,
  }) => {
    await page.goto(`${locale === "zh" ? "/zh" : ""}/docs/agent-http/`);
    const section = page.locator('section[aria-labelledby="agent-host"]');
    for (const contract of [
      "AgentHostBindings::new",
      "AgentHostDependencies",
      "AgentHost::launch",
      "host.wait_ready().await",
      "host.health()",
      "host.shutdown().await",
    ])
      await expect(section.getByText(contract, { exact: true })).toBeVisible();
    await expect(section).toContainText(
      locale === "zh" ? "HTTP → Worker → 维护" : "HTTP → Worker → maintenance",
    );
    await expect(section).toContainText(
      locale === "zh" ? "不是匿名运维接口" : "not an anonymous",
    );
    await expect(
      section.locator("a[href$='0015-owned-agent-host.md']"),
    ).toHaveCount(1);
    await expect(
      section.locator(
        "a[href$='/docs/agent-host" +
          (locale === "zh" ? ".zh-CN" : "") +
          ".md']",
      ),
    ).toHaveCount(1);
    await page.setViewportSize({ width: 320, height: 800 });
    await auditHorizontalLayout(page);
  });

  test(`${locale} documents bounded maintenance ownership and recovery`, async ({
    page,
  }) => {
    await page.goto(`${locale === "zh" ? "/zh" : ""}/docs/agent-loop/`);
    const section = page.locator(
      'section[aria-labelledby="owned-maintenance"]',
    );
    for (const contract of [
      "AgentMaintenanceBinding::new",
      "AgentMaintenance::start",
      "AgentMaintenanceReadiness",
      "role.health()",
      "role.shutdown().await",
    ])
      await expect(section.getByText(contract, { exact: true })).toBeVisible();
    await expect(section).toContainText(
      locale === "zh"
        ? "强制取消不代表事务回滚"
        : "forced cancellation never proves transaction rollback",
    );
    await expect(
      section.locator("a[href$='0014-owned-agent-maintenance.md']"),
    ).toHaveCount(1);
    await expect(
      section.locator(
        "a[href$='/docs/agent-maintenance" +
          (locale === "zh" ? ".zh-CN" : "") +
          ".md']",
      ),
    ).toHaveCount(1);
    await page.setViewportSize({ width: 320, height: 800 });
    await auditHorizontalLayout(page);
  });

  test(`${locale} documents concrete owned Worker boundaries`, async ({
    page,
  }) => {
    await page.goto(`${locale === "zh" ? "/zh" : ""}/docs/agent-loop/`);
    const section = page.locator('section[aria-labelledby="owned-worker"]');
    for (const contract of [
      "AgentWorkerBinding::tenant",
      "AgentWorkerBinding::fair",
      "AgentWorker::start",
      "AgentWorkerReadiness",
      "worker.shutdown().await",
      "worker.health()",
    ])
      await expect(section.getByText(contract, { exact: true })).toBeVisible();
    await expect(section).toContainText(
      locale === "zh" ? "不代表事务回滚" : "never proves rollback",
    );
    await expect(
      section.locator("a[href$='0013-owned-agent-worker.md']"),
    ).toHaveCount(1);
    await page.setViewportSize({ width: 320, height: 800 });
    await auditHorizontalLayout(page);
    await expect(
      page.locator(
        "a[href$='/docs/agent-worker" +
          (locale === "zh" ? ".zh-CN" : "") +
          ".md']",
      ),
    ).toHaveCount(1);
  });

  test(`${locale} explains independently authorized known-error reconciliation`, async ({
    page,
  }) => {
    await page.goto(`${locale === "zh" ? "/zh" : ""}/docs/mcp-remote-tool/`);
    const section = page.locator(
      'section[aria-labelledby="mcp-error-reconciliation-title"]',
    );
    for (const contract of [
      "stateknot_reconcile_tool_error_v1",
      "stateknot:reconcile-error",
      "RetryAdvice::Never",
      "not_applied",
      "applied",
    ]) {
      await expect(section.getByText(contract, { exact: true })).toBeVisible();
    }
    await expect(section).toContainText(
      locale === "zh"
        ? "未知或部分生效仍须保留为未决状态"
        : "Unknown or partial effects remain unresolved",
    );
    await expect(section.locator("a")).toHaveAttribute(
      "href",
      `https://github.com/StateKnot/StateKnot/blob/main/docs/mcp-error-reconciliation${locale === "zh" ? ".zh-CN" : ""}.md`,
    );
    await page.setViewportSize({ width: 320, height: 800 });
    await auditHorizontalLayout(page);
  });
}

const localizedRoutePairs = [
  {
    en: "/",
    zh: "/zh/",
    enHeading: "Durable agent orchestration, written for Rust.",
    zhHeading: "为 Rust 而生的可恢复 Agent 编排。",
  },
  {
    en: "/docs/",
    zh: "/zh/docs/",
    enHeading: "Documentation that follows the code.",
    zhHeading: "文档必须跟得上代码。",
  },
  {
    en: "/docs/getting-started/",
    zh: "/zh/docs/getting-started/",
    enHeading: "Validate StateKnot locally.",
    zhHeading: "在本地验证 StateKnot。",
  },
  {
    en: "/docs/core-contracts/",
    zh: "/zh/docs/core-contracts/",
    enHeading: "Compile the public core contracts.",
    zhHeading: "编译 Core 公共合约。",
  },
  {
    en: "/docs/typed-agent/",
    zh: "/zh/docs/typed-agent/",
    enHeading: "Build a typed Agent contract.",
    zhHeading: "构建一个强类型 Agent 合约。",
  },
  {
    en: "/docs/admission/",
    zh: "/zh/docs/admission/",
    enHeading: "Admit one Agent as an atomic durable fact.",
    zhHeading: "以原子事务提交 Agent 准入记录。",
  },
  {
    en: "/docs/runs/",
    zh: "/zh/docs/runs/",
    enHeading: "Submit once. Recover the same durable Agent run.",
    zhHeading: "重试提交，恢复同一个 Agent Run。",
  },
  {
    en: "/docs/concepts/durability/",
    zh: "/zh/docs/concepts/durability/",
    enHeading: "Durability is evidence, not process memory.",
    zhHeading: "持久执行依赖记录，而非进程内存。",
  },
  {
    en: "/docs/concepts/graphs/",
    zh: "/zh/docs/concepts/graphs/",
    enHeading: "Compile a deterministic graph.",
    zhHeading: "编译一个确定性 Graph。",
  },
  {
    en: "/docs/graph-composition/",
    zh: "/zh/docs/graph-composition/",
    enHeading: "Compose durable subgraphs and bounded loops.",
    zhHeading: "组合可恢复子图与有界循环。",
  },
  {
    en: "/docs/runtime/",
    zh: "/zh/docs/runtime/",
    enHeading: "Drive a Graph from durable evidence.",
    zhHeading: "依据持久化证据驱动 Graph。",
  },
  {
    en: "/docs/agent-loop/",
    zh: "/zh/docs/agent-loop/",
    enHeading: "Run one durable Agent scheduling quantum.",
    zhHeading: "执行一个可恢复 Agent 的调度单元。",
  },
  {
    en: "/docs/invocations/",
    zh: "/zh/docs/invocations/",
    enHeading: "Dispatch each model and tool attempt at most once.",
    zhHeading: "每个 Model 与 Tool Attempt 最多 Dispatch 一次。",
  },
  {
    en: "/docs/provider-native-agent/",
    zh: "/zh/docs/provider-native-agent/",
    enHeading: "Run the provider-native model/tool graph.",
    zhHeading: "运行 Provider-native Model/Tool Graph。",
  },
  {
    en: "/docs/agent-service/",
    zh: "/zh/docs/agent-service/",
    enHeading: "Expose durable Agents through one service boundary.",
    zhHeading: "通过一个服务边界暴露可恢复 Agent。",
  },
  {
    en: "/docs/agent-http/",
    zh: "/zh/docs/agent-http/",
    enHeading: "Submit and recover Agents over authenticated HTTP.",
    zhHeading: "通过认证 HTTP 提交与恢复 Agent。",
  },
  {
    en: "/docs/mcp-client/",
    zh: "/zh/docs/mcp-client/",
    enHeading: "Call a stateless MCP Tool safely.",
    zhHeading: "安全调用一个 Stateless MCP Tool。",
  },
  {
    en: "/docs/mcp-oauth/",
    zh: "/zh/docs/mcp-oauth/",
    enHeading: "Authorize one MCP resource without widening trust.",
    zhHeading: "授权一个 MCP Resource，不扩大信任边界。",
  },
  {
    en: "/docs/mcp-remote-tool/",
    zh: "/zh/docs/mcp-remote-tool/",
    enHeading: "Bind one MCP Tool without weakening durability.",
    zhHeading: "绑定一个 MCP Tool，不削弱持久执行语义。",
  },
  {
    en: "/docs/mcp-server/",
    zh: "/zh/docs/mcp-server/",
    enHeading: "Expose a bounded MCP Server without weakening policy.",
    zhHeading: "在不削弱 Policy 的前提下暴露有界 MCP Server。",
  },
  {
    en: "/docs/mcp-skills/",
    zh: "/zh/docs/mcp-skills/",
    enHeading: "Publish manifest-bound Agent Skills over MCP.",
    zhHeading: "通过 MCP 发布受完整 Manifest 约束的 Agent Skill。",
  },
  {
    en: "/docs/mcp-conformance/",
    zh: "/zh/docs/mcp-conformance/",
    enHeading: "MCP conformance claims stop at the evidence.",
    zhHeading: "MCP Conformance 声明以证据为界。",
  },
  {
    en: "/docs/a2a-client/",
    zh: "/zh/docs/a2a-client/",
    enHeading: "Call an A2A agent without guessing the outcome.",
    zhHeading: "调用 A2A Agent，不猜测执行结果。",
  },
  {
    en: "/docs/artifacts/",
    zh: "/zh/docs/artifacts/",
    enHeading: "Persist A2A artifacts as verifiable facts.",
    zhHeading: "把 A2A Artifact 持久化为可验证事实。",
  },
  {
    en: "/docs/a2a-server/",
    zh: "/zh/docs/a2a-server/",
    enHeading: "Serve A2A 1.0 without leaking wire types.",
    zhHeading: "在不泄露 Wire Type 的前提下提供 A2A 1.0。",
  },
  {
    en: "/docs/a2a-conformance/",
    zh: "/zh/docs/a2a-conformance/",
    enHeading: "A2A conformance claims stop at the evidence.",
    zhHeading: "A2A Conformance 声明以证据为界。",
  },
  {
    en: "/docs/fair-scheduling/",
    zh: "/zh/docs/fair-scheduling/",
    enHeading: "Schedule tenants from one durable order.",
    zhHeading: "依据持久化的全局顺序调度租户。",
  },
  {
    en: "/docs/postgresql/",
    zh: "/zh/docs/postgresql/",
    enHeading: "Operate the PostgreSQL durability provider.",
    zhHeading: "运维 PostgreSQL 持久化 Provider。",
  },
  {
    en: "/docs/qualification/",
    zh: "/zh/docs/qualification/",
    enHeading: "Qualify a host without manufacturing an SLO.",
    zhHeading: "验证宿主，但不伪造 SLO。",
  },
  {
    en: "/docs/status/",
    zh: "/zh/docs/status/",
    enHeading: "Read implementation status before API shape.",
    zhHeading: "判断 API 形态前，先看实现状态。",
  },
] as const;

const contentRoutes = localizedRoutePairs.flatMap(({ en, zh }) => [en, zh]);

const expectIcpFiling = async (page: Page): Promise<void> => {
  const filing = page.locator("footer").getByRole("link", {
    name: "冀ICP备2026036754号-1",
    exact: true,
  });
  await expect(filing).toHaveCount(1);
  await expect(filing).toBeVisible();
  await expect(filing).toHaveAttribute("href", "https://beian.miit.gov.cn/");
  await expect(filing).toHaveAttribute("target", "_blank");
  await expect(filing).toHaveAttribute("rel", "noopener noreferrer");
};

const auditHorizontalLayout = async (page: Page): Promise<void> => {
  const dimensions = await page.evaluate(() => {
    const viewport = document.documentElement.clientWidth;
    const offenders = Array.from(document.querySelectorAll<HTMLElement>("*"))
      .map((element) => {
        const rect = element.getBoundingClientRect();
        const overflowX = getComputedStyle(element).overflowX;
        return {
          selector: `${element.tagName.toLowerCase()}.${element.className}`,
          text: element.textContent?.trim().replace(/\s+/g, " ").slice(0, 120),
          left: Math.round(rect.left),
          right: Math.round(rect.right),
          scrollWidth: element.scrollWidth,
          clientWidth: element.clientWidth,
          overflowX,
        };
      })
      .filter(
        ({ left, right, scrollWidth, clientWidth, overflowX }) =>
          left < 0 ||
          right > viewport ||
          (scrollWidth > clientWidth && overflowX === "visible"),
      )
      .slice(0, 12);

    return {
      body: document.body.scrollWidth,
      root: document.documentElement.scrollWidth,
      viewport,
      offenders,
    };
  });

  const overflowDetails = JSON.stringify(dimensions.offenders, null, 2);
  await expectIcpFiling(page);
  expect(dimensions.body, overflowDetails).toBeLessThanOrEqual(
    dimensions.viewport,
  );
  expect(dimensions.root, overflowDetails).toBeLessThanOrEqual(
    dimensions.viewport,
  );

  const wrappedAffordances = await page
    .locator(".affordance")
    .evaluateAll((nodes) =>
      nodes
        .filter((node) => !node.hasAttribute("hidden"))
        .filter((node) => getComputedStyle(node).whiteSpace !== "nowrap")
        .map((node) => node.textContent?.trim()),
    );
  expect(wrappedAffordances).toEqual([]);
};

test("homepage exposes honest implementation status and semantic structure", async ({
  page,
}) => {
  await page.goto("/");

  await expect(
    page.getByRole("heading", {
      level: 1,
      name: "Durable agent orchestration, written for Rust.",
    }),
  ).toBeVisible();
  await expect(
    page.getByText("Pre-alpha · no stable public API"),
  ).toBeVisible();
  await expect(
    page
      .locator(".spec-list")
      .getByText("A2A 1.0 Client and remote agent", { exact: true }),
  ).toBeVisible();
  await expect(
    page
      .locator(".spec-list")
      .getByText("General MCP Tool client", { exact: true }),
  ).toBeVisible();
  await expect(
    page.locator(".spec-list").getByText("MCP OAuth client", { exact: true }),
  ).toBeVisible();
  await expect(
    page.locator(".spec-list").getByText("MCP Remote Tool", { exact: true }),
  ).toBeVisible();
  await expect(
    page.locator(".spec-list").getByText("MCP Server profile", { exact: true }),
  ).toBeVisible();
  await expect(
    page
      .locator(".spec-list")
      .getByText("MCP Skills server profile", { exact: true }),
  ).toBeVisible();
  await expect(
    page.locator(".spec-list").getByText("AgentService v1", { exact: true }),
  ).toBeVisible();
  await expect(
    page
      .locator(".spec-list")
      .getByText("A2A 1.0 Server profile", { exact: true }),
  ).toBeVisible();
  await expect(
    page
      .locator(".spec-list")
      .getByText("Durable Graph Driver", { exact: true }),
  ).toBeVisible();
  await expect(
    page.locator(".spec-list").getByText("Durable Agent Loop", { exact: true }),
  ).toBeVisible();
  await expect(
    page
      .locator(".spec-list")
      .getByText("Durable model/tool attempts", { exact: true }),
  ).toBeVisible();
  await expect(
    page
      .locator(".spec-list")
      .getByText("Cross-tenant fair scheduler", { exact: true }),
  ).toBeVisible();
  await expect(
    page
      .locator(".spec-list")
      .getByText("Typed Agent and model adapters", { exact: true }),
  ).toBeVisible();
  await expect(
    page
      .locator(".spec-list")
      .getByText("Provider-native Agent graph", { exact: true }),
  ).toBeVisible();
  await expect(
    page.locator(".spec-list").getByText("Atomic Agent admission", {
      exact: true,
    }),
  ).toBeVisible();
  await expect(
    page.locator(".spec-list").getByText("Durable Agent runs and results", {
      exact: true,
    }),
  ).toBeVisible();
  await expect(page.locator(".map-node--planned")).toHaveCount(3);

  const main = page.locator("main");
  await expect(main).toHaveAttribute("id", "main-content");
  await expect(page.locator("footer")).toBeVisible();
});

test("publishes the digest-pinned Graph Driver schema at its stable identity", async ({
  request,
}) => {
  const response = await request.get(
    "/schemas/runtime/graph-driver-event/1.0.0",
  );
  expect(response.status()).toBe(200);
  const schema = await response.json();
  expect(schema.$schema).toBe("https://json-schema.org/draft/2020-12/schema");
  expect(schema.$id).toBe(
    "https://stknot.com/schemas/runtime/graph-driver-event/1.0.0",
  );
  expect(schema.additionalProperties).toBe(false);
  expect(schema.properties.operation.enum).toEqual([
    "node_attempt_started",
    "node_attempt_succeeded",
    "node_attempt_failed",
    "graph_barrier_continued",
  ]);
});

test("publishes the strict Graph lifecycle schema at its stable identity", async ({
  request,
}) => {
  const response = await request.get(
    "/schemas/runtime/graph-lifecycle-event/1.0.0",
  );
  expect(response.status()).toBe(200);
  const schema = await response.json();
  expect(schema.$schema).toBe("https://json-schema.org/draft/2020-12/schema");
  expect(schema.$id).toBe(
    "https://stknot.com/schemas/runtime/graph-lifecycle-event/1.0.0",
  );
  expect(schema.additionalProperties).toBe(false);
  expect(schema.properties.operation.enum).toEqual([
    "graph_barrier_waiting",
    "graph_barrier_succeeded",
    "graph_run_failed",
  ]);
  expect(schema.oneOf).toHaveLength(3);
});

test("publishes the strict invocation execution schema at its stable identity", async ({
  request,
}) => {
  const response = await request.get(
    "/schemas/runtime/invocation-execution-event/1.0.0",
  );
  expect(response.status()).toBe(200);
  const schema = await response.json();
  expect(schema.$schema).toBe("https://json-schema.org/draft/2020-12/schema");
  expect(schema.$id).toBe(
    "https://stknot.com/schemas/runtime/invocation-execution-event/1.0.0",
  );
  expect(schema.additionalProperties).toBe(false);
  expect(schema.properties.operation.enum).toEqual([
    "model_invocation_prepared",
    "model_attempt_started",
    "model_response_committed",
    "model_error_committed",
    "tool_invocation_prepared",
    "tool_attempt_started",
    "tool_result_committed",
    "tool_error_committed",
  ]);
  expect(schema.oneOf).toHaveLength(2);
});

test("publishes the strict Agent admission schema at its stable identity", async ({
  request,
}) => {
  const response = await request.get(
    "/schemas/runtime/agent-admission-event/1.0.0",
  );
  expect(response.status()).toBe(200);
  const schema = await response.json();
  expect(schema.$schema).toBe("https://json-schema.org/draft/2020-12/schema");
  expect(schema.$id).toBe(
    "https://stknot.com/schemas/runtime/agent-admission-event/1.0.0",
  );
  expect(schema.additionalProperties).toBe(false);
  expect(schema.required).toEqual([
    "operation",
    "intent_digest",
    "graph_digest",
    "policy_digest",
    "input_digest",
  ]);
  expect(schema.properties.operation.const).toBe("agent_admitted");
});

test("publishes the strict Agent cancellation schema at its stable identity", async ({
  request,
}) => {
  const response = await request.get(
    "/schemas/runtime/agent-cancellation-event/1.0.0",
  );
  expect(response.status()).toBe(200);
  const schema = await response.json();
  expect(schema.$schema).toBe("https://json-schema.org/draft/2020-12/schema");
  expect(schema.$id).toBe(
    "https://stknot.com/schemas/runtime/agent-cancellation-event/1.0.0",
  );
  expect(schema.additionalProperties).toBe(false);
  expect(schema.required).toEqual([
    "operation",
    "graph_digest",
    "checkpoint_id",
    "superstep",
    "failure_id",
  ]);
  expect(schema.properties.operation.const).toBe(
    "agent_cancellation_confirmed",
  );
});

test("publishes the strict Agent service control schema at its stable identity", async ({
  request,
}) => {
  const response = await request.get(
    "/schemas/runtime/agent-service-control-event/1.0.0",
  );
  expect(response.status()).toBe(200);
  const schema = await response.json();
  expect(schema.$schema).toBe("https://json-schema.org/draft/2020-12/schema");
  expect(schema.$id).toBe(
    "https://stknot.com/schemas/runtime/agent-service-control-event/1.0.0",
  );
  expect(schema.additionalProperties).toBe(false);
  expect(schema.required).toEqual([
    "operation",
    "admission_digest",
    "policy_digest",
    "decision_digest",
    "failure_id",
  ]);
  expect(schema.properties.operation.const).toBe(
    "agent_cancellation_requested",
  );
});

for (const width of responsiveAuditWidths) {
  test(`has no horizontal page overflow at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 900 });
    await page.goto("/");
    await auditHorizontalLayout(page);
  });
}

for (const route of [
  "/docs/",
  "/docs/getting-started/",
  "/docs/core-contracts/",
  "/docs/typed-agent/",
  "/docs/admission/",
  "/docs/runs/",
  "/docs/runtime/",
  "/docs/agent-loop/",
  "/docs/invocations/",
  "/docs/provider-native-agent/",
  "/docs/agent-service/",
  "/docs/agent-http/",
  "/docs/mcp-remote-tool/",
  "/docs/mcp-conformance/",
  "/docs/a2a-client/",
  "/docs/a2a-server/",
  "/docs/a2a-conformance/",
  "/docs/fair-scheduling/",
  "/zh/",
  "/zh/docs/getting-started/",
  "/zh/docs/core-contracts/",
  "/zh/docs/typed-agent/",
  "/zh/docs/admission/",
  "/zh/docs/runs/",
  "/zh/docs/runtime/",
  "/zh/docs/agent-loop/",
  "/zh/docs/invocations/",
  "/zh/docs/provider-native-agent/",
  "/zh/docs/agent-service/",
  "/zh/docs/agent-http/",
  "/zh/docs/mcp-remote-tool/",
  "/zh/docs/mcp-conformance/",
  "/zh/docs/a2a-client/",
  "/zh/docs/a2a-server/",
  "/zh/docs/a2a-conformance/",
  "/zh/docs/fair-scheduling/",
] as const) {
  for (const width of [320, 375, 414, 768] as const) {
    test(`${route} is responsive at ${width}px`, async ({ page }) => {
      await page.setViewportSize({ width, height: 900 });
      await page.goto(route);
      await auditHorizontalLayout(page);
    });
  }
}

test("core contracts expose executable evidence and the draft boundary", async ({
  page,
}) => {
  await page.goto("/docs/core-contracts/");
  await expect(
    page.getByText("Validation gate, not API stability"),
  ).toBeVisible();
  for (const example of [
    "first_agent",
    "typed_tool",
    "model_stream",
    "protocol_adapter",
  ]) {
    await expect(page.getByText(example, { exact: true })).toBeVisible();
  }
  await expect(
    page.getByText("RFC-0001 remains Draft", { exact: false }),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", {
      level: 2,
      name: "Seal the compatibility evidence corpus",
    }),
  ).toBeVisible();
  await expect(
    page.getByText("all 36 committed Core", { exact: false }),
  ).toBeVisible();
  await expect(
    page.getByText("item 2 remains open", { exact: false }),
  ).toBeVisible();
  await expect(page.locator("[data-copy-button]")).toHaveCount(1);

  await page.goto("/zh/docs/core-contracts/");
  await expect(
    page.getByRole("heading", { level: 2, name: "封闭兼容性证据语料库" }),
  ).toBeVisible();
  await expect(
    page.getByText("全部 36 份 Core Fixture", { exact: false }),
  ).toBeVisible();
  await expect(
    page.getByText("第 2 项仍保持开放", { exact: false }),
  ).toBeVisible();
  await expect(page.locator("[data-copy-button]")).toHaveCount(1);
});

test("typed Agent tutorial keeps the durable execution boundary explicit", async ({
  page,
}) => {
  await page.goto("/docs/typed-agent/");
  await expect(page.getByText("Implemented pre-alpha surface")).toBeVisible();
  await expect(
    page.getByText("Current fail-closed restrictions"),
  ).toBeVisible();
  await expect(
    page.getByText("Cross the durable boundary explicitly"),
  ).toBeVisible();
  await expect(
    page.getByText("durable run/result boundary", { exact: false }),
  ).toBeVisible();
  await expect(page.locator("[data-copy-button]")).toHaveCount(3);
});

test("provider-native tutorial exposes recovery and cancellation boundaries", async ({
  page,
}) => {
  await page.goto("/docs/provider-native-agent/");
  await expect(page.getByText("Implemented pre-alpha boundary")).toBeVisible();
  await expect(
    page.getByRole("heading", { level: 2, name: "Follow one durable turn" }),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", {
      level: 2,
      name: "Repair structured output from durable evidence",
    }),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", {
      level: 2,
      name: "Parallelize reads; make every write a barrier",
    }),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", {
      level: 2,
      name: "Treat cancellation as two durable facts",
    }),
  ).toBeVisible();
  await expect(
    page.getByText("StateKnot never substitutes zero usage", { exact: false }),
  ).toBeVisible();
  await expect(page.locator("[data-copy-button]")).toHaveCount(4);
});

test("durable run tutorial documents retry, conflict, and public snapshot semantics", async ({
  page,
}) => {
  await page.goto("/docs/runs/");
  await expect(
    page.getByText("Implemented boundary, pre-alpha API"),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", {
      level: 2,
      name: "Retry logical content, not candidate identities",
    }),
  ).toBeVisible();
  await expect(
    page.getByText("A second key for the same run", { exact: false }),
  ).toBeVisible();
  await expect(page.getByText("outcome: null", { exact: false })).toBeVisible();
  await expect(page.locator("[data-copy-button]")).toHaveCount(2);
});

test("AgentService tutorial keeps authorization, retry, and transport boundaries explicit", async ({
  page,
}) => {
  await page.goto("/docs/agent-service/");
  await expect(page.getByText("Implemented embedding boundary")).toBeVisible();
  await expect(
    page.getByRole("heading", {
      level: 2,
      name: "Submit, inspect, and cancel by durable identity",
    }),
  ).toBeVisible();
  await expect(
    page.getByText("Authorization first", { exact: true }),
  ).toBeVisible();
  await expect(page.locator("[data-copy-button]")).toHaveCount(3);
});

test("Agent HTTP tutorial preserves authentication, recovery and deployment boundaries", async ({
  page,
}) => {
  for (const [path, heading] of [
    ["/docs/agent-http/", "Before exposing a production endpoint"],
    ["/zh/docs/agent-http/", "对外上线前的配置清单"],
  ] as const) {
    await page.goto(path);
    await expect(
      page.getByRole("heading", { name: heading, exact: true }),
    ).toBeVisible();
    await expect(
      page.getByText("POST /v1/agent-runs/lookup", { exact: true }),
    ).toBeVisible();
    await expect(page.locator("[data-copy-button]")).toHaveCount(1);
    await expect(
      page.locator("a[href$='/docs/rfcs/0008-agent-http-v1.md']"),
    ).toHaveCount(1);
  }
});

for (const locale of ["en", "zh"] as const) {
  test(`${locale} documents real online identity without replacing resource policy`, async ({
    page,
  }) => {
    await page.goto(`${locale === "zh" ? "/zh" : ""}/docs/agent-http/`);
    const section = page.locator('section[aria-labelledby="http-identity"]');
    for (const contract of [
      "AgentHttpIntrospection",
      "TenantPolicy",
      "replace_client_secret",
      "AgentServiceAuthorizer",
    ])
      await expect(section.getByText(contract, { exact: true })).toBeVisible();
    await expect(section).toContainText("JWT/JWKS");
    await expect(section).toContainText("503");
    await expect(
      section.locator("a[href$='0011-agent-http-introspection.md']"),
    ).toHaveCount(1);
    await page.setViewportSize({ width: 320, height: 800 });
    await auditHorizontalLayout(page);
  });

  test(`${locale} documents owned ingress readiness and bounded drain`, async ({
    page,
  }) => {
    await page.goto(`${locale === "zh" ? "/zh" : ""}/docs/agent-http/`);
    const section = page.locator('section[aria-labelledby="http-server"]');
    for (const contract of [
      "AgentHttpServer::start",
      "AgentHttpReadiness",
      "server.health()",
      "server.begin_shutdown()",
      "server.shutdown()",
    ])
      await expect(section.getByText(contract, { exact: true })).toBeVisible();
    await expect(section).toContainText(
      locale === "zh"
        ? "连接关闭不代表事务回滚"
        : "closure never proves rollback",
    );
    await expect(
      section.locator("a[href$='0010-owned-agent-http-server.md']"),
    ).toHaveCount(1);
    await page.setViewportSize({ width: 320, height: 800 });
    await auditHorizontalLayout(page);
  });

  test(`${locale} documents resumable activity without claiming historical snapshots`, async ({
    page,
  }) => {
    await page.goto(`${locale === "zh" ? "/zh" : ""}/docs/agent-http/`);
    const section = page.locator('section[aria-labelledby="http-sse"]');
    for (const contract of [
      "activity",
      "snapshot",
      "Last-Event-ID",
      "agent_http.invalid_cursor",
    ])
      await expect(section.getByText(contract, { exact: true })).toBeVisible();
    await expect(section).toContainText(
      locale === "zh"
        ? "它不是游标位置上的历史状态"
        : "It does not describe historical state at a cursor",
    );
    await expect(
      section.locator("a[href$='0009-agent-sse-replay.md']"),
    ).toHaveCount(1);
    await page.setViewportSize({ width: 320, height: 800 });
    await auditHorizontalLayout(page);
  });
}

test("MCP tutorial states the strict profile and ambiguous write contract", async ({
  page,
}) => {
  await page.goto("/docs/mcp-remote-tool/");
  await expect(
    page.getByText("Implemented strict client profile"),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", {
      level: 2,
      name: "Keep a lost write ambiguous",
    }),
  ).toBeVisible();
  await expect(page.getByText("ReconcileFirst", { exact: true })).toBeVisible();
  await expect(
    page.getByRole("heading", {
      level: 2,
      name: "Reconcile without calling the Tool again",
    }),
  ).toBeVisible();
  await expect(
    page.getByText("not a claim of complete MCP conformance", {
      exact: false,
    }),
  ).toBeVisible();
  await expect(page.locator("[data-copy-button]")).toHaveCount(3);
});

test("general MCP Tool client documents MRTR, limits, and official evidence", async ({
  page,
}) => {
  await page.goto("/docs/mcp-client/");
  await expect(
    page.getByText("Implemented Tool client, pre-alpha API"),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", {
      level: 2,
      name: "Mediate every MRTR round",
    }),
  ).toBeVisible();
  await expect(
    page.getByText("373 scored assertions succeed", { exact: false }),
  ).toBeVisible();
  await expect(page.locator("[data-copy-button]")).toHaveCount(2);
});

test("MCP conformance page freezes evidence without overclaiming", async ({
  page,
}) => {
  await page.goto("/docs/mcp-conformance/");
  await expect(page.getByText("Current claim", { exact: true })).toBeVisible();
  await expect(
    page.getByText("all 25 OAuth scenarios", {
      exact: false,
    }),
  ).toBeVisible();
  await expect(
    page.getByText("69 scored scenarios", { exact: false }),
  ).toBeVisible();
  await expect(
    page.getByText("c321dd32035556e6769d3724a8ee97d87c3faaac", {
      exact: true,
    }),
  ).toBeVisible();
  await expect(page.locator("[data-copy-button]")).toHaveCount(1);
});

test("A2A Server page keeps policy and durability boundaries explicit", async ({
  page,
}) => {
  await page.goto("/docs/a2a-server/");
  await expect(
    page.getByText("Implemented server profile, pre-alpha API"),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", {
      level: 2,
      name: "Keep identity ahead of parsing",
    }),
  ).toBeVisible();
  await expect(
    page.getByText("process memory for durability", { exact: false }),
  ).toBeVisible();
  await expect(page.locator("[data-copy-button]")).toHaveCount(1);
});

test("A2A Client page documents all operations and unknown recovery", async ({
  page,
}) => {
  await page.goto("/docs/a2a-client/");
  await expect(
    page.getByText("Implemented Client and durable adapter, pre-alpha API"),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", {
      level: 2,
      name: "Make PostgreSQL the dispatch authority",
    }),
  ).toBeVisible();
  await expect(
    page.getByText("Unknown + ReconcileFirst", { exact: true }),
  ).toBeVisible();
  await expect(
    page
      .locator(".docs-article")
      .getByText("all eleven operations", { exact: false })
      .first(),
  ).toBeVisible();
  await expect(page.locator("[data-copy-button]")).toHaveCount(2);
});

test("A2A conformance page freezes exact evidence without overclaiming", async ({
  page,
}) => {
  await page.goto("/docs/a2a-conformance/");
  await expect(
    page.getByText("Server evidence, not framework certification"),
  ).toBeVisible();
  await expect(page.getByText("177", { exact: true })).toBeVisible();
  await expect(
    page.getByText("263b9cfaf16a554bdfb166a7ba5b67716e946349", {
      exact: true,
    }),
  ).toBeVisible();
  await expect(page.getByText("78.8%", { exact: true })).toBeVisible();
  await expect(page.locator("[data-copy-button]")).toHaveCount(1);
});

test("MCP OAuth page documents durable stores and bounded replay", async ({
  page,
}) => {
  await page.goto("/docs/mcp-oauth/");
  await expect(
    page.getByText("Implemented pre-alpha profile", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("heading", {
      level: 2,
      name: "Treat stores as security infrastructure",
    }),
  ).toBeVisible();
  await expect(
    page.getByText("all 25 scored OAuth", { exact: false }),
  ).toBeVisible();
  await expect(page.locator("[data-copy-button]")).toHaveCount(2);
});

for (const route of localizedRoutePairs) {
  test(`${route.en} and ${route.zh} are equivalent localized routes`, async ({
    page,
  }) => {
    await page.goto(route.en);
    await expectIcpFiling(page);
    await expect(page.locator("html")).toHaveAttribute("lang", "en");
    await expect(page.getByRole("heading", { level: 1 })).toHaveText(
      route.enHeading,
    );
    await expect(page.locator(".language-link")).toHaveAttribute(
      "href",
      route.zh,
    );
    await expect(page.locator('link[rel="canonical"]')).toHaveAttribute(
      "href",
      new URL(route.en, "https://stknot.com").href,
    );
    await expect(
      page.locator('link[rel="alternate"][hreflang="zh-CN"]'),
    ).toHaveAttribute("href", new URL(route.zh, "https://stknot.com").href);

    await page.goto(route.zh);
    await expectIcpFiling(page);
    // Check metadata, accessible labels and hidden search entries as well as body copy.
    expect(await page.content()).not.toContain("耐久");
    await expect(page.locator("html")).toHaveAttribute("lang", "zh-CN");
    await expect(page.getByRole("heading", { level: 1 })).toHaveText(
      route.zhHeading,
    );
    await expect(page.locator(".language-link")).toHaveAttribute(
      "href",
      route.en,
    );
    await expect(
      page.locator('link[rel="alternate"][hreflang="en"]'),
    ).toHaveAttribute("href", new URL(route.en, "https://stknot.com").href);
    await expect(
      page.locator('link[rel="alternate"][hreflang="x-default"]'),
    ).toHaveAttribute("href", new URL(route.en, "https://stknot.com").href);
  });
}

test("command palette supports keyboard navigation and restores focus", async ({
  page,
}) => {
  await page.goto("/");
  const trigger = page.locator("[data-command-open]");
  const dialog = page.locator("[data-command-dialog]");
  const input = page.locator("[data-command-input]");

  await page.keyboard.press("Control+K");
  await expect(dialog).toBeVisible();
  await expect(input).toBeFocused();

  await input.fill("qualified v1");
  await expect(page.locator("[data-command-count]")).toHaveText("1 result");
  await page.keyboard.press("Enter");
  await expect(dialog).not.toBeVisible();
  await expect(page).toHaveURL(/#roadmap$/);

  await trigger.click();
  await expect(dialog).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(dialog).not.toBeVisible();
  await expect(trigger).toBeFocused();
});

test("Chinese command palette searches localized content", async ({ page }) => {
  await page.goto("/zh/");
  const dialog = page.locator("[data-command-dialog]");
  const input = page.locator("[data-command-input]");

  await page.keyboard.press("Control+K");
  await expect(dialog).toBeVisible();
  await input.fill("快速开始");
  await expect(page.locator("[data-command-count]")).toHaveText("1 项结果");
  await page.keyboard.press("Enter");
  await expect(page).toHaveURL(/\/zh\/docs\/getting-started\/$/);
});

test("Chinese durability guide explains terminology without changing API names", async ({
  page,
}) => {
  await page.goto("/zh/docs/concepts/durability/");
  const terminology = page.locator(
    'section[aria-labelledby="terminology-title"]',
  );
  await expect(terminology.locator("dt")).toHaveText([
    "持久执行（Durable Execution）",
    "可恢复（Recoverable）",
    "持久化（Persistence）与持久性（Durability）",
  ]);
  await expect(terminology).toContainText(
    "仅有持久化存储不等于流程能够正确恢复",
  );
  await expect(terminology.locator("code")).toHaveText([
    "DurableGraphDriver",
    "DurableAgentLoop",
  ]);
  await expect(page.locator("blockquote")).toContainText(
    "数据库变更 Exactly-once 并不等于现实世界副作用 Exactly-once",
  );
});

for (const query of ["持久执行", "可恢复", "持久化", "durable execution"]) {
  test(`Chinese terminology search resolves ${query} to the concept guide`, async ({
    page,
  }) => {
    await page.goto("/zh/docs/");
    await page.keyboard.press("Control+K");
    await page.locator("[data-command-input]").fill(query);
    const result = page.locator(
      '[data-command-result][href="/zh/docs/concepts/durability/"]',
    );
    await expect(result).toBeVisible();
    await expect(result).toContainText("持久执行模型");
    await result.click();
    await expect(page).toHaveURL(/\/zh\/docs\/concepts\/durability\/$/);
  });
}

test("command palette becomes a mobile sheet", async ({ page }) => {
  await page.setViewportSize({ width: 375, height: 812 });
  await page.goto("/");
  await page.locator("[data-command-open]").click();

  await expect
    .poll(async () => page.locator("[data-command-dialog]").boundingBox())
    .toEqual({ x: 0, y: 0, width: 375, height: 812 });
});

for (const route of contentRoutes) {
  test(`${route} has no detectable accessibility violations`, async ({
    page,
  }) => {
    await page.goto(route);
    const results = await new AxeBuilder({ page }).analyze();
    expect(results.violations).toEqual([]);
  });
}

test("documentation navigation adapts without losing current-page state", async ({
  page,
}) => {
  await page.setViewportSize({ width: 375, height: 812 });
  await page.goto("/docs/postgresql/");
  await expect(page.locator(".docs-nav-disclosure")).toBeVisible();
  await expect(page.locator(".docs-nav--desktop")).toBeHidden();
  await page.locator(".docs-nav-disclosure summary").click();
  await expect(
    page.locator('.docs-nav--mobile [aria-current="page"]'),
  ).toHaveText("PostgreSQL provider");

  await page.setViewportSize({ width: 1280, height: 900 });
  await expect(page.locator(".docs-nav-disclosure")).toBeHidden();
  await expect(page.locator(".docs-nav--desktop")).toBeVisible();
  await expect(
    page.locator('.docs-nav--desktop [aria-current="page"]'),
  ).toHaveText("PostgreSQL provider");
});

test("every localized internal link resolves", async ({ page, request }) => {
  const checked = new Set<string>();

  for (const route of contentRoutes) {
    await page.goto(route);
    const hrefs = await page
      .locator('a[href^="/"]')
      .evaluateAll((links) =>
        links
          .map((link) => link.getAttribute("href"))
          .filter((href): href is string => Boolean(href)),
      );

    for (const href of hrefs) {
      const path = new URL(href, "http://127.0.0.1:4399").pathname;
      if (checked.has(path)) continue;
      checked.add(path);
      const response = await request.get(path);
      expect(response.status(), `${route} links to ${path}`).toBeLessThan(400);
    }
  }
});

test("visible text and critical interaction colors meet WCAG contrast", async ({
  page,
}) => {
  const auditContrast = async () =>
    page.evaluate(() => {
      type Rgba = { red: number; green: number; blue: number; alpha: number };

      const canvas = document.createElement("canvas");
      canvas.width = 1;
      canvas.height = 1;
      const context = canvas.getContext("2d", { willReadFrequently: true });
      if (!context) throw new Error("2D canvas is unavailable");

      const parseColor = (color: string): Rgba => {
        context.clearRect(0, 0, 1, 1);
        context.fillStyle = "rgba(0, 0, 0, 0)";
        context.fillStyle = color;
        context.fillRect(0, 0, 1, 1);
        const [red = 0, green = 0, blue = 0, alpha = 0] = context.getImageData(
          0,
          0,
          1,
          1,
        ).data;
        return { red, green, blue, alpha: alpha / 255 };
      };

      const composite = (top: Rgba, bottom: Rgba): Rgba => {
        const alpha = top.alpha + bottom.alpha * (1 - top.alpha);
        if (alpha === 0) return { red: 0, green: 0, blue: 0, alpha: 0 };
        return {
          red:
            (top.red * top.alpha +
              bottom.red * bottom.alpha * (1 - top.alpha)) /
            alpha,
          green:
            (top.green * top.alpha +
              bottom.green * bottom.alpha * (1 - top.alpha)) /
            alpha,
          blue:
            (top.blue * top.alpha +
              bottom.blue * bottom.alpha * (1 - top.alpha)) /
            alpha,
          alpha,
        };
      };

      const luminance = ({ red, green, blue }: Rgba): number => {
        const linear = [red, green, blue].map((channel) => {
          const value = channel / 255;
          return value <= 0.04045
            ? value / 12.92
            : ((value + 0.055) / 1.055) ** 2.4;
        });
        return (
          0.2126 * (linear[0] ?? 0) +
          0.7152 * (linear[1] ?? 0) +
          0.0722 * (linear[2] ?? 0)
        );
      };

      const ratio = (foreground: Rgba, background: Rgba): number => {
        const foregroundLuminance = luminance(foreground);
        const backgroundLuminance = luminance(background);
        const lighter = Math.max(foregroundLuminance, backgroundLuminance);
        const darker = Math.min(foregroundLuminance, backgroundLuminance);
        return (lighter + 0.05) / (darker + 0.05);
      };

      const effectiveBackground = (element: Element): Rgba => {
        const layers: Rgba[] = [];
        for (
          let current: Element | null = element;
          current;
          current = current.parentElement
        ) {
          layers.push(parseColor(getComputedStyle(current).backgroundColor));
        }

        let background: Rgba = { red: 255, green: 255, blue: 255, alpha: 1 };
        for (const layer of layers.reverse()) {
          background = composite(layer, background);
        }
        return background;
      };

      const failures: string[] = [];
      const seen = new Set<string>();
      const walker = document.createTreeWalker(
        document.body,
        NodeFilter.SHOW_TEXT,
      );

      for (let node = walker.nextNode(); node; node = walker.nextNode()) {
        if (!node.textContent?.trim()) continue;
        const element = node.parentElement;
        if (!element || element.closest(".sr-only, [aria-hidden='true']"))
          continue;

        const style = getComputedStyle(element);
        const rect = element.getBoundingClientRect();
        if (
          style.display === "none" ||
          style.visibility === "hidden" ||
          Number(style.opacity) === 0 ||
          rect.width === 0 ||
          rect.height === 0
        ) {
          continue;
        }

        const fontSize = Number.parseFloat(style.fontSize);
        const fontWeight = Number.parseInt(style.fontWeight, 10) || 400;
        const minimum =
          fontSize >= 24 || (fontSize >= 18 && fontWeight >= 700) ? 3 : 4.5;
        const measured = ratio(
          parseColor(style.color),
          effectiveBackground(element),
        );
        const key = `${style.color}/${getComputedStyle(element).backgroundColor}/${minimum}`;
        if (measured + 0.01 < minimum && !seen.has(key)) {
          seen.add(key);
          failures.push(
            `${element.tagName.toLowerCase()}.${element.className}: ${measured.toFixed(2)} < ${minimum}`,
          );
        }
      }

      const root = getComputedStyle(document.documentElement);
      const token = (name: string): Rgba =>
        parseColor(root.getPropertyValue(name).trim());
      const criticalPairs = [
        ["accent text", "--color-accent-ink", "--color-accent-strong", 4.5],
        ["focus on paper", "--color-focus", "--color-paper", 3],
        ["focus on paper 2", "--color-focus", "--color-paper-2", 3],
        ["primary focus", "--color-paper", "--color-accent-strong", 3],
        ["control on paper", "--color-control", "--color-paper", 3],
        ["control on paper 2", "--color-control", "--color-paper-2", 3],
        ["success state", "--color-success", "--color-success-soft", 4.5],
        ["error state", "--color-danger", "--color-danger-soft", 4.5],
      ] as const;

      for (const [name, foreground, background, minimum] of criticalPairs) {
        const measured = ratio(token(foreground), token(background));
        if (measured + 0.01 < minimum) {
          failures.push(`${name}: ${measured.toFixed(2)} < ${minimum}`);
        }
      }

      return failures;
    });

  for (const route of ["/", "/docs/", "/zh/", "/zh/docs/status/"] as const) {
    await page.goto(route);
    expect(await auditContrast(), route).toEqual([]);
    await page.locator("[data-command-open]").click();
    expect(await auditContrast(), `${route} command palette`).toEqual([]);
    await page.keyboard.press("Escape");
  }
});

test("hero actions fit within a 13-inch laptop fold", async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 800 });
  await page.goto("/");

  const actions = await page.locator(".hero-actions").boundingBox();
  expect(actions).not.toBeNull();
  expect((actions?.y ?? 801) + (actions?.height ?? 0)).toBeLessThanOrEqual(800);
});

test("copy action reports completion without a redundant toast", async ({
  page,
}) => {
  await page.goto("/");
  const copyButton = page.locator("[data-copy-button]");

  await copyButton.click();
  await expect(copyButton).toHaveText("Copied");
  await expect(page.locator("[role='status']")).toHaveCount(0);
});

test("copy action localizes its state feedback", async ({ page }) => {
  await page.goto("/zh/docs/");
  const copyButton = page.locator("[data-copy-button]").first();

  await copyButton.click();
  await expect(copyButton).toHaveText("已复制");
  await expect(page.locator("[role='status']")).toHaveCount(0);
});

test("copy action exposes clipboard failures and recovers", async ({
  page,
}) => {
  await page.addInitScript(() => {
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: {
        writeText: () => Promise.reject(new Error("clipboard unavailable")),
      },
    });
  });
  await page.goto("/");
  const copyButton = page.locator("[data-copy-button]");

  await copyButton.click();
  await expect(copyButton).toHaveText("Copy failed");
  await expect(copyButton).toBeEnabled();
  await expect(copyButton).not.toHaveAttribute("aria-busy");
});

test("404 page gives a recovery action", async ({ page }) => {
  const response = await page.goto("/route-that-does-not-exist");
  await expectIcpFiling(page);
  expect(response?.status()).toBe(404);
  await expect(
    page.getByRole("heading", {
      level: 1,
      name: "This path has no durable successor.",
    }),
  ).toBeVisible();
  await expect(page.getByRole("link", { name: "Return home" })).toBeVisible();
});

test("Chinese 404 template preserves language and recovery action", async ({
  page,
}) => {
  const response = await page.goto("/zh/404/");
  await expectIcpFiling(page);
  expect(await page.content()).not.toContain("耐久");
  expect(response?.status()).toBe(200);
  await expect(page.locator("html")).toHaveAttribute("lang", "zh-CN");
  await expect(
    page.getByRole("heading", { level: 1, name: "找不到这个页面。" }),
  ).toBeVisible();
  await expect(page.getByRole("link", { name: "返回首页" })).toHaveAttribute(
    "href",
    "/zh/",
  );
});
