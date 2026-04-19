import { test, expect } from '@playwright/test';

test.describe('Pesticidkort', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/pesticidkort');
  });

  test('should load landing page with heading, address input, and explore button', async ({
    page,
  }) => {
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

  test('should adapt landing page to mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });

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
    const input = page.locator('[data-testid="landing-address-input"]');
    await expect(input).toBeVisible();
    await input.focus();
    await expect(input).toBeFocused();

    // Verify it accepts text input
    await input.fill('Nørrebrogade');
    await expect(input).toHaveValue('Nørrebrogade');
  });

  test('should have correct page title', async ({ page }) => {
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
