import { expect, test } from "@playwright/test";

test.describe("Oceanus Folk v2 smoke tests", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await page.waitForLoadState("networkidle");
  });

  test("page title and main headings render", async ({ page }) => {
    await expect(page).toHaveTitle(/Oceanus Folk: Integrated Visual Analysis/i);
    await expect(
      page.getByRole("heading", { name: "Rising Star Analysis Workbench" })
    ).toBeVisible();
  });

  test("navigation links present", async ({ page }) => {
    await expect(
      page.getByRole("link", { name: /Compare Careers/i })
    ).toBeVisible();
    await expect(
      page.getByRole("link", { name: /Characterization/i })
    ).toBeVisible();
  });

  test("timeline chart renders SVG", async ({ page }) => {
    await expect(page.locator("#timeline-chart svg")).toBeVisible();
  });

  test("radar chart renders SVG", async ({ page }) => {
    await expect(page.locator("#radar-chart svg")).toBeVisible();
  });

  test("parallel coordinates chart renders SVG", async ({ page }) => {
    await expect(page.locator("#pcp-chart svg")).toBeVisible();
  });

  test("PCP has 7 dimension axes (6 dims + trend)", async ({ page }) => {
    const lines = page.locator("#pcp-chart svg line");
    expect(await lines.count()).toBeGreaterThanOrEqual(7);
  });

  test("scatter chart renders SVG", async ({ page }) => {
    await expect(page.locator("#scatter-chart svg")).toBeVisible();
  });

  test("network chart renders SVG with nodes", async ({ page }) => {
    const svg = page.locator("#network-chart svg");
    await expect(svg).toBeVisible();
    const circles = svg.locator("circle");
    const paths = svg.locator("path");
    const nodeCount =
      (await circles.count()) + (await paths.count());
    expect(nodeCount).toBeGreaterThan(10);
  });

  test("scatter chart has prediction circles", async ({ page }) => {
    const circles = page.locator("#scatter-chart svg circle");
    expect(await circles.count()).toBeGreaterThanOrEqual(5);
  });
});
