// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";

const responsiveAuditWidths = [
  320, 360, 375, 414, 639, 640, 767, 768, 959, 960, 1279, 1280, 1440, 1920,
] as const;

const localizedRoutePairs = [
  {
    en: "/",
    zh: "/zh/",
    enHeading: "Durable agent orchestration, written for Rust.",
    zhHeading: "为 Rust 而生的耐久 Agent 编排。",
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
    en: "/docs/typed-agent/",
    zh: "/zh/docs/typed-agent/",
    enHeading: "Build a typed Agent contract.",
    zhHeading: "构建一个强类型 Agent 合约。",
  },
  {
    en: "/docs/concepts/durability/",
    zh: "/zh/docs/concepts/durability/",
    enHeading: "Durability is evidence, not process memory.",
    zhHeading: "耐久执行依赖证据，而不是进程内存。",
  },
  {
    en: "/docs/concepts/graphs/",
    zh: "/zh/docs/concepts/graphs/",
    enHeading: "Compile a deterministic graph.",
    zhHeading: "编译一个确定性 Graph。",
  },
  {
    en: "/docs/runtime/",
    zh: "/zh/docs/runtime/",
    enHeading: "Drive a Graph from durable evidence.",
    zhHeading: "从耐久证据驱动 Graph。",
  },
  {
    en: "/docs/agent-loop/",
    zh: "/zh/docs/agent-loop/",
    enHeading: "Run one durable Agent scheduling quantum.",
    zhHeading: "执行一个耐久 Agent 调度 Quantum。",
  },
  {
    en: "/docs/invocations/",
    zh: "/zh/docs/invocations/",
    enHeading: "Dispatch each model and tool attempt at most once.",
    zhHeading: "每个 Model 与 Tool Attempt 最多 Dispatch 一次。",
  },
  {
    en: "/docs/fair-scheduling/",
    zh: "/zh/docs/fair-scheduling/",
    enHeading: "Schedule tenants from one durable order.",
    zhHeading: "从一条耐久顺序调度租户。",
  },
  {
    en: "/docs/postgresql/",
    zh: "/zh/docs/postgresql/",
    enHeading: "Operate the PostgreSQL durability provider.",
    zhHeading: "运维 PostgreSQL 耐久化 Provider。",
  },
  {
    en: "/docs/status/",
    zh: "/zh/docs/status/",
    enHeading: "Read implementation status before API shape.",
    zhHeading: "判断 API 形态前，先看实现状态。",
  },
] as const;

const contentRoutes = localizedRoutePairs.flatMap(({ en, zh }) => [en, zh]);

const auditHorizontalLayout = async (page: Page): Promise<void> => {
  const dimensions = await page.evaluate(() => {
    const viewport = document.documentElement.clientWidth;
    const offenders = Array.from(document.querySelectorAll<HTMLElement>("*"))
      .map((element) => {
        const rect = element.getBoundingClientRect();
        const overflowX = getComputedStyle(element).overflowX;
        return {
          selector: `${element.tagName.toLowerCase()}.${element.className}`,
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
  await expect(page.getByText("MCP adapters")).toBeVisible();
  await expect(page.getByText("A2A adapters", { exact: true })).toBeVisible();
  await expect(
    page.getByText("Durable Graph Driver", { exact: true }).first(),
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
  await expect(page.locator(".map-node--planned")).toHaveCount(4);

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
    "model_attempt_started",
    "model_response_committed",
    "model_error_committed",
    "tool_attempt_started",
    "tool_result_committed",
    "tool_error_committed",
  ]);
  expect(schema.oneOf).toHaveLength(2);
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
  "/docs/typed-agent/",
  "/docs/runtime/",
  "/docs/agent-loop/",
  "/docs/invocations/",
  "/docs/fair-scheduling/",
  "/zh/",
  "/zh/docs/getting-started/",
  "/zh/docs/typed-agent/",
  "/zh/docs/runtime/",
  "/zh/docs/agent-loop/",
  "/zh/docs/invocations/",
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
    page.getByText("complete public admit/run/result facade", { exact: false }),
  ).toBeVisible();
  await expect(page.locator("[data-copy-button]")).toHaveCount(3);
});

for (const route of localizedRoutePairs) {
  test(`${route.en} and ${route.zh} are equivalent localized routes`, async ({
    page,
  }) => {
    await page.goto(route.en);
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
  expect(response?.status()).toBe(200);
  await expect(page.locator("html")).toHaveAttribute("lang", "zh-CN");
  await expect(
    page.getByRole("heading", { level: 1, name: "这条路径没有耐久后继。" }),
  ).toBeVisible();
  await expect(page.getByRole("link", { name: "返回首页" })).toHaveAttribute(
    "href",
    "/zh/",
  );
});
