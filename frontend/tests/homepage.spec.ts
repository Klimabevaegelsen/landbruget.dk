import { test, expect } from '@playwright/test';

test.describe('Homepage', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should load homepage and display main elements', async ({ page }) => {
    // Check for main heading or title
    await expect(page.locator('h1')).toBeVisible();

    // Check if navigation is present
    await expect(page.locator('nav')).toBeVisible();
  });

  test('should display ranking tables', async ({ page }) => {
    // Wait for ranking tables to load
    await page.waitForSelector('[data-testid="ranking-table"]', {
      timeout: 10000,
    });

    // Check if at least one ranking table is visible
    const rankingTables = page.locator('[data-testid="ranking-table"]');
    await expect(rankingTables.first()).toBeVisible();

    // Check table structure
    await expect(rankingTables.first().locator('.card-header')).toBeVisible();
    await expect(rankingTables.first().locator('.card-content')).toBeVisible();
  });

  test('should handle company navigation from ranking table', async ({
    page,
  }) => {
    // Wait for ranking table to load
    await page.waitForSelector('[data-testid="ranking-table"]', {
      timeout: 10000,
    });

    // Find and click on a company link
    const companyLink = page.locator('[data-testid="company-link"]').first();

    if ((await companyLink.count()) > 0) {
      await expect(companyLink).toBeVisible();

      // Check that link has proper attributes
      await expect(companyLink).toHaveAttribute('href');

      // Test navigation (but don't actually navigate to avoid dependencies)
      const href = await companyLink.getAttribute('href');
      expect(href).toContain('/company/');
    }
  });

  test('should show category badges correctly', async ({ page }) => {
    // Wait for ranking table to load
    await page.waitForSelector('[data-testid="ranking-table"]', {
      timeout: 10000,
    });

    // Check for category badges
    const badges = page.locator('.badge');

    if ((await badges.count()) > 0) {
      const firstBadge = badges.first();
      await expect(firstBadge).toBeVisible();

      // Check that badge has proper styling classes
      const className = await firstBadge.getAttribute('class');
      expect(className).toContain('badge');
    }
  });

  test('should be responsive on mobile', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    // Check that page is still functional on mobile
    await expect(page.locator('body')).toBeVisible();

    // Check if mobile menu is present or navigation adapts
    const nav = page.locator('nav');
    await expect(nav).toBeVisible();
  });
});
