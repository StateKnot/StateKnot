// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

const responsiveAuditWidths = [
  320, 360, 375, 414, 639, 640, 767, 768, 959, 960, 1279, 1280, 1440, 1920,
] as const;

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
  await expect(page.locator(".map-node--planned")).toHaveCount(4);

  const main = page.locator("main");
  await expect(main).toHaveAttribute("id", "main-content");
  await expect(page.locator("footer")).toBeVisible();
});

for (const width of responsiveAuditWidths) {
  test(`has no horizontal page overflow at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 900 });
    await page.goto("/");

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

  await input.fill("road");
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

test("command palette becomes a mobile sheet", async ({ page }) => {
  await page.setViewportSize({ width: 375, height: 812 });
  await page.goto("/");
  await page.locator("[data-command-open]").click();

  await expect
    .poll(async () => page.locator("[data-command-dialog]").boundingBox())
    .toEqual({ x: 0, y: 0, width: 375, height: 812 });
});

test("homepage has no detectable accessibility violations", async ({
  page,
}) => {
  await page.goto("/");
  const results = await new AxeBuilder({ page }).analyze();
  expect(results.violations).toEqual([]);
});

test("visible text and critical interaction colors meet WCAG contrast", async ({
  page,
}) => {
  await page.goto("/");

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

  expect(await auditContrast()).toEqual([]);
  await page.locator("[data-command-open]").click();
  expect(await auditContrast()).toEqual([]);
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
