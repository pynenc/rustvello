// Run against the test_workflow_comparison_pages fixture; see monitoring-test-fixtures.md.
const assert = require("node:assert/strict");
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || "playwright");

(async () => {
  const base = process.argv[2];
  assert(base, "Pass the fixture base URL");
  const browser = await chromium.launch();
  try {
    const page = await browser.newPage({
      viewport: { width: 1700, height: 1100 },
    });
    const errors = [];
    page.on("pageerror", (error) => errors.push(error.message));
    const url = `${base}/workflows/rust::test.process_order?limit=10&histogram_workflow=`;
    await page.goto(url);
    async function settled() {
      await page.waitForFunction(
        () =>
          !document.querySelector(
            '[data-workflow-comparison][aria-busy="true"]',
          ),
      );
    }
    const rows = page.locator("[data-workflow-id]");
    assert.equal(await rows.count(), 10);
    await rows.nth(0).locator("td").nth(1).click();
    await settled();
    await rows.nth(1).locator("td").nth(1).click();
    await settled();
    assert.equal(await page.locator(".workflow-comparison-run").count(), 2);
    assert.equal(await page.locator('[aria-selected="true"]').count(), 2);
    assert.equal(await page.locator(".histogram-worker-line").count(), 2);
    await page.locator(".histogram-bucket").first().focus();
    assert.equal(
      await page
        .locator(".histogram-legend-time")
        .filter({ hasText: "+" })
        .count(),
      2,
    );
    await page.getByRole("link", { name: "Next page", exact: true }).click();
    await settled();
    assert.equal(await page.locator(".workflow-comparison-run").count(), 2);
    assert.equal(await rows.count(), 10);
    await page.goBack();
    await settled();
    assert.equal(await page.locator(".workflow-comparison-run").count(), 2);
    await page.goForward();
    await settled();
    await rows.first().focus();
    await page.keyboard.press("Space");
    await settled();
    assert.equal(await page.locator(".workflow-comparison-run").count(), 3);
    await page.locator("[data-workflow-columns='1']").click();
    assert.equal(
      await page
        .locator(".workflow-comparison-grid")
        .getAttribute("data-columns"),
      "1",
    );
    await page.locator("[data-workflow-columns='2']").click();
    await page.screenshot({
      path: "/tmp/rustvello-workflows-desktop.png",
      fullPage: true,
    });
    await page.setViewportSize({ width: 390, height: 844 });
    assert(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
      "page must not overflow on mobile",
    );
    await page.screenshot({
      path: "/tmp/rustvello-workflows-mobile.png",
      fullPage: true,
    });
    await page.locator("[data-workflow-clear]").click();
    await settled();
    assert.equal(await page.locator(".workflow-comparison-run").count(), 0);
    await page.reload();
    assert.equal(await page.locator(".workflow-comparison-run").count(), 0);
    assert.deepEqual(errors, []);
    for (const route of [
      "/",
      "/broker",
      "/orchestrator",
      "/runners",
      "/atomic-service",
      "/state-backend",
      "/client-data-store",
      "/invocations",
      "/invocations/timeline",
      "/tasks",
      "/workflows",
      "/events",
      "/log-explorer",
    ]) {
      const response = await page.goto(base + route);
      assert(response.ok(), `${route}: HTTP ${response.status()}`);
      assert(
        await page.locator("main, .container-fluid").count(),
        `${route}: missing content`,
      );
    }
    assert.deepEqual(errors, []);
    console.log(
      "Workflow selection, pagination, charts, mobile layout, and main navigation passed.",
    );
  } finally {
    await browser.close();
  }
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
