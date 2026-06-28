import { test, expect } from '@playwright/test';

test.describe('Pesticidkort', () => {
  test.beforeEach(async ({ page }) => {
    // Pre-accept the first-visit disclaimer modal so tests can interact with
    // the landing page directly. The modal is gated on this localStorage key.
    await page.addInitScript(() => {
      window.localStorage.setItem(
        'pesticidkort-disclaimer-v1',
        new Date().toISOString()
      );
    });
  });

  test('should load landing page with heading, address input, and explore button', async ({
    page,
  }) => {
    await page.goto('/pesticidkort');

    await expect(page.locator('h1')).toBeVisible();

    await expect(
      page.locator('[data-testid="landing-address-input"]')
    ).toBeVisible();

    await expect(
      page.locator('[data-testid="explore-map-button"]')
    ).toBeVisible();
  });

  test('should navigate to explore map mode and back to landing', async ({
    page,
  }) => {
    await page.goto('/pesticidkort');

    const exploreButton = page.locator('[data-testid="explore-map-button"]');
    await expect(exploreButton).toBeVisible();
    await exploreButton.click();

    // Verify we are in explore mode — back button should appear
    const backButton = page.locator('[data-testid="explore-back-button"]');
    await expect(backButton).toBeVisible({ timeout: 10000 });

    // Navigate back to landing
    await backButton.click();
    await expect(
      page.locator('[data-testid="landing-address-input"]')
    ).toBeVisible({ timeout: 10000 });
  });

  test('should load directly into report mode via URL params', async ({
    page,
  }) => {
    await page.goto(
      '/pesticidkort?lat=55.6761&lng=12.5683&addr=K%C3%B8benhavn&y=2022'
    );

    // Report renders in both mobile bottom sheet and desktop sidebar — only
    // one is visible at a given viewport; pick whichever is visible.
    await expect(
      page.locator('[data-testid="personal-report"]:visible').first()
    ).toBeVisible({ timeout: 15000 });

    await expect(
      page.locator('[data-testid="share-report-button"]:visible').first()
    ).toBeVisible();
  });

  test('should show chemical filter pills in explore mode', async ({
    page,
  }) => {
    await page.goto('/pesticidkort');

    const exploreButton = page.locator('[data-testid="explore-map-button"]');
    await exploreButton.click();

    const pills = page.locator('[data-testid="chemical-filter-pills"]');
    await expect(pills).toBeVisible({ timeout: 10000 });

    await expect(
      page.locator('[data-testid="chemical-filter-none"]')
    ).toBeVisible();
    await expect(
      page.locator('[data-testid="chemical-filter-pfas"]')
    ).toBeVisible();
    await expect(
      page.locator('[data-testid="chemical-filter-glyphosate"]')
    ).toBeVisible();
    await expect(
      page.locator('[data-testid="chemical-filter-diquat"]')
    ).toBeVisible();
  });

  test('should show burden ranges without A-E legend labels in explore mode', async ({
    page,
  }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/pesticidkort');

    const exploreButton = page.locator('[data-testid="explore-map-button"]');
    await exploreButton.click();

    const legend = page.locator('[data-testid="map-legend"]');
    await expect(legend).toBeVisible({ timeout: 10000 });
    await expect(legend).toContainText('Under 0,5 B/ha');
    await expect(legend).toContainText('0,5–2,0 B/ha');
    await expect(legend).not.toContainText('A — Under 0,5 B/ha');
    await expect(legend).not.toContainText('E — Over 8,0 B/ha');

    const legendBox = await legend.boundingBox();
    expect(legendBox).not.toBeNull();
    expect(legendBox!.x + legendBox!.width).toBeLessThanOrEqual(375);

    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    expect(bodyWidth).toBeLessThanOrEqual(376);
  });

  test('should keep map zoom controls visible below the explore overlay on narrow mobile', async ({
    page,
  }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/pesticidkort');

    const exploreButton = page.locator('[data-testid="explore-map-button"]');
    await exploreButton.click();

    const yearTimeline = page.locator('[data-testid="year-timeline"]');
    const chemicalPills = page.locator('[data-testid="chemical-filter-pills"]');
    const navControl = page.locator('.maplibregl-ctrl-top-left');

    await expect(yearTimeline).toBeVisible({ timeout: 10000 });
    await expect(chemicalPills).toBeVisible({ timeout: 10000 });
    await expect(navControl).toBeVisible({ timeout: 10000 });

    const yearBox = await yearTimeline.boundingBox();
    const pillsBox = await chemicalPills.boundingBox();
    const navBox = await navControl.boundingBox();

    expect(yearBox).not.toBeNull();
    expect(pillsBox).not.toBeNull();
    expect(navBox).not.toBeNull();

    expect(yearBox!.height).toBeLessThan(60);
    expect(navBox!.y).toBeGreaterThan(pillsBox!.y + pillsBox!.height + 8);
  });

  test('should render pesticidkort overlays with dark-safe contrast in dark mode', async ({
    page,
  }) => {
    await page.goto('/pesticidkort');

    await page.evaluate(() => {
      window.localStorage.setItem('landbruget-theme', 'dark');
    });

    await page.reload();
    await page.locator('[data-testid="explore-map-button"]').click();

    await expect(page.locator('[data-testid="map-legend"]')).toBeVisible({
      timeout: 10000,
    });
    await expect(page.locator('.maplibregl-ctrl-group').first()).toBeVisible();
    await expect(
      page.locator('[data-testid="chemical-filter-pfas"]')
    ).toBeVisible();

    const styles = await page.evaluate(() => {
      const readLuma = (value: string) => {
        const matches = value.match(/\d+(\.\d+)?/g);
        if (!matches || matches.length < 3) return null;
        const [r, g, b] = matches.slice(0, 3).map(Number);
        return 0.2126 * r + 0.7152 * g + 0.0722 * b;
      };

      const legend = document.querySelector(
        '[data-testid="map-legend"]'
      ) as HTMLElement | null;
      const ctrl = document.querySelector(
        '.maplibregl-ctrl-group'
      ) as HTMLElement | null;
      const pillDot = document.querySelector(
        '[data-testid="chemical-filter-pfas"] span'
      ) as HTMLElement | null;

      if (!legend || !ctrl || !pillDot) {
        return null;
      }

      const legendStyle = getComputedStyle(legend);
      const ctrlStyle = getComputedStyle(ctrl);
      const pillDotStyle = getComputedStyle(pillDot);

      return {
        rootDark: document.documentElement.classList.contains('dark'),
        legendLuma: readLuma(legendStyle.backgroundColor),
        ctrlLuma: readLuma(ctrlStyle.backgroundColor),
        pillDotBg: pillDotStyle.backgroundColor,
      };
    });

    expect(styles).not.toBeNull();
    expect(styles!.rootDark).toBe(true);
    expect(styles!.legendLuma).not.toBeNull();
    expect(styles!.ctrlLuma).not.toBeNull();
    expect(styles!.pillDotBg).not.toBe('rgba(0, 0, 0, 0)');
    expect(styles!.legendLuma!).toBeLessThan(80);
    expect(styles!.ctrlLuma!).toBeLessThan(80);
  });

  test('should keep mobile footer expansion from covering the legend or report sheet', async ({
    page,
  }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/pesticidkort');

    const footer = page.locator(
      '[data-testid="pesticidkort-disclaimer-footer"]'
    );
    const footerToggle = page.locator(
      '[data-testid="pesticidkort-disclaimer-toggle"]'
    );

    await page.locator('[data-testid="explore-map-button"]').click();
    await expect(footerToggle).toBeVisible({ timeout: 10000 });
    await footerToggle.click({ force: true });

    const legend = page.locator('[data-testid="map-legend"]');
    await expect(legend).toBeVisible({ timeout: 10000 });
    await page.waitForTimeout(100);

    const footerTopInExplore = await footer.evaluate((el) =>
      Math.round(el.getBoundingClientRect().top)
    );
    const legendBottom = await legend.evaluate((el) =>
      Math.round(el.getBoundingClientRect().bottom)
    );

    expect(legendBottom).toBeLessThanOrEqual(footerTopInExplore - 4);

    await page.goto(
      '/pesticidkort?lat=55.6761&lng=12.5683&addr=K%C3%B8benhavn&y=2022'
    );

    await expect(
      page.locator('[data-testid="share-report-button"]:visible').first()
    ).toBeVisible({ timeout: 15000 });

    const reportFooterToggle = page.locator(
      '[data-testid="pesticidkort-disclaimer-toggle"]'
    );
    await expect(reportFooterToggle).toBeVisible({ timeout: 10000 });
    await reportFooterToggle.click({ force: true });

    const sheetHandle = page.locator('[data-testid="bottom-sheet-handle"]');
    await expect(sheetHandle).toBeVisible({ timeout: 10000 });
    await page.waitForTimeout(100);

    const footerTopInReport = await footer.evaluate((el) =>
      Math.round(el.getBoundingClientRect().top)
    );
    const sheetBottom = await sheetHandle.evaluate((el) =>
      Math.round(
        (el.parentElement as HTMLElement).getBoundingClientRect().bottom
      )
    );

    expect(sheetBottom).toBeLessThanOrEqual(footerTopInReport);
  });

  test('should adapt landing page to mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/pesticidkort');

    await expect(page.locator('h1')).toBeVisible();

    await expect(
      page.locator('[data-testid="landing-address-input"]')
    ).toBeVisible();

    await expect(
      page.locator('[data-testid="explore-map-button"]')
    ).toBeVisible();

    // Ensure no horizontal overflow on mobile
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 1);
  });

  test('should have address input visible and focusable', async ({ page }) => {
    await page.goto('/pesticidkort');

    const input = page.locator('[data-testid="landing-address-input"]');
    await expect(input).toBeVisible();
    await input.focus();
    await expect(input).toBeFocused();

    // Verify it accepts text input
    await input.fill('Nørrebrogade');
    await expect(input).toHaveValue('Nørrebrogade');
  });

  test('should have correct page title', async ({ page }) => {
    await page.goto('/pesticidkort');

    const title = await page.title();
    expect(title.toLowerCase()).toContain('pesticid');
  });

  test('PDF report renders the new percentile grade label', async ({
    page,
  }) => {
    const response = await page.goto(
      '/api/report-pdf?addr=Test&y=2022&grade=TOP_5&score=3.1&fields=5&pfas=1&dist=42'
    );
    expect(response?.status()).toBe(200);
    await expect(page.getByText('Top 5% mest eksponeret')).toBeVisible();
  });
});
