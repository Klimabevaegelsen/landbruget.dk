/**
 * E2E Tests for Ranking Tables
 *
 * Tests table interactions, sorting, filtering, and data display
 */

import { test, expect } from '@playwright/test';
import { waitForTestId, expectTestIdVisible } from './utils/test-helpers';

test.describe('Ranking Tables', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should display ranking tables on homepage', async ({ page }) => {
    // Wait for tables to load
    await waitForTestId(page, 'ranking-table', { timeout: 15000 });

    // Check that at least one table is visible
    await expectTestIdVisible(page, 'ranking-table');

    // Count tables
    const tables = page.locator('[data-testid="ranking-table"]');
    const count = await tables.count();

    expect(count).toBeGreaterThan(0);
  });

  test('should show table titles', async ({ page }) => {
    await waitForTestId(page, 'ranking-table', { timeout: 15000 });

    // Each table should have a title
    const tables = page.locator('[data-testid="ranking-table"]');
    const firstTable = tables.first();

    const title = firstTable.locator('h2, h3, [data-testid="ranking-title"]');
    await expect(title.first()).toBeVisible();
  });

  test('should display company data in table rows', async ({ page }) => {
    await waitForTestId(page, 'ranking-table', { timeout: 15000 });

    const tables = page.locator('[data-testid="ranking-table"]');
    const firstTable = tables.first();

    // Look for table rows
    const rows = firstTable.locator(
      'tr, [role="row"], [data-testid="ranking-row"]'
    );
    const rowCount = await rows.count();

    expect(rowCount).toBeGreaterThan(0);
  });

  test('should show company names in ranking rows', async ({ page }) => {
    await waitForTestId(page, 'ranking-table', { timeout: 15000 });

    const tables = page.locator('[data-testid="ranking-table"]');
    const firstTable = tables.first();

    // Look for company links/names
    const companyLinks = firstTable.locator('[data-testid="company-link"]');

    if ((await companyLinks.count()) > 0) {
      await expect(companyLinks.first()).toBeVisible();

      const text = await companyLinks.first().textContent();
      expect(text).not.toBe('');
    }
  });

  test('should display ranking numbers', async ({ page }) => {
    await waitForTestId(page, 'ranking-table', { timeout: 15000 });

    const tables = page.locator('[data-testid="ranking-table"]');
    const firstTable = tables.first();

    // Look for rank numbers
    const rankCells = firstTable.locator('text=/^[1-9]\\d*$/');

    if ((await rankCells.count()) > 0) {
      await expect(rankCells.first()).toBeVisible();
    }
  });

  test('should show ranking values with units', async ({ page }) => {
    await waitForTestId(page, 'ranking-table', { timeout: 15000 });

    const tables = page.locator('[data-testid="ranking-table"]');
    const firstTable = tables.first();

    // Look for value cells (numbers with possible units)
    const valueCells = firstTable.locator(
      'text=/\\d+[.,]?\\d*\\s*(kr|kg|ha|ton|%)?/i'
    );

    if ((await valueCells.count()) > 0) {
      await expect(valueCells.first()).toBeVisible();
    }
  });

  test('should handle table sorting if available', async ({ page }) => {
    await waitForTestId(page, 'ranking-table', { timeout: 15000 });

    const tables = page.locator('[data-testid="ranking-table"]');
    const firstTable = tables.first();

    // Look for sortable column headers
    const sortableHeaders = firstTable.locator(
      '[role="columnheader"] button, th button, [data-testid*="sort"]'
    );

    if ((await sortableHeaders.count()) > 0) {
      const firstHeader = sortableHeaders.first();
      await firstHeader.click();

      // Wait for re-render
      await page.waitForTimeout(300);

      // Table should still be visible
      await expect(firstTable).toBeVisible();
    }
  });

  test('should show table descriptions', async ({ page }) => {
    await waitForTestId(page, 'ranking-table', { timeout: 15000 });

    const tables = page.locator('[data-testid="ranking-table"]');
    const firstTable = tables.first();

    // Look for description text
    const description = firstTable.locator(
      '[data-testid="ranking-description"], p, .description'
    );

    if ((await description.count()) > 0) {
      await expect(description.first()).toBeVisible();
    }
  });

  test('should handle pagination if available', async ({ page }) => {
    await waitForTestId(page, 'ranking-table', { timeout: 15000 });

    const tables = page.locator('[data-testid="ranking-table"]');
    const firstTable = tables.first();

    // Look for pagination controls
    const paginationButtons = page.locator(
      '[data-testid*="pagination"], button[aria-label*="next"], button[aria-label*="previous"]'
    );

    if ((await paginationButtons.count()) > 0) {
      const nextButton = paginationButtons
        .filter({ hasText: /next|næste/i })
        .first();

      if (await nextButton.isEnabled()) {
        await nextButton.click();
        await page.waitForTimeout(300);

        // Table should still be visible
        await expect(firstTable).toBeVisible();
      }
    }
  });

  test('should show last updated timestamp', async ({ page }) => {
    await waitForTestId(page, 'ranking-table', { timeout: 15000 });

    const tables = page.locator('[data-testid="ranking-table"]');
    const firstTable = tables.first();

    // Look for timestamp
    const timestamp = firstTable.locator(
      'text=/opdateret|updated|\\d{1,2}\\/\\d{1,2}\\/\\d{4}/i'
    );

    if ((await timestamp.count()) > 0) {
      await expect(timestamp.first()).toBeVisible();
    }
  });

  test('should filter rankings by category tabs', async ({ page }) => {
    await page.waitForLoadState('networkidle');

    // Look for category tabs
    const categoryTabs = page.locator(
      '[data-testid*="category-tab"], [role="tab"]'
    );

    if ((await categoryTabs.count()) > 1) {
      const secondTab = categoryTabs.nth(1);
      await secondTab.click();

      // Wait for content to update
      await page.waitForTimeout(500);

      // Rankings should still be visible
      const tables = page.locator('[data-testid="ranking-table"]');
      const count = await tables.count();

      expect(count).toBeGreaterThan(0);
    }
  });

  test('should show table count indicator', async ({ page }) => {
    await waitForTestId(page, 'ranking-table', { timeout: 15000 });

    // Look for count indicator (e.g., "10 virksomheder")
    const countIndicator = page.locator(
      'text=/\\d+\\s+(virksomheder|companies|entries)/i'
    );

    if ((await countIndicator.count()) > 0) {
      await expect(countIndicator.first()).toBeVisible();
    }
  });

  test('should handle empty state gracefully', async ({ page }) => {
    // Mock empty rankings response
    await page.route('**/api/rankings**', (route) => {
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({
          rankings: [],
          metadata: { generated_at: new Date().toISOString(), total_tables: 0 },
        }),
      });
    });

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Should show empty state or no tables
    const emptyState = page.locator(
      'text=/ingen data|no data|ingen resultater/i'
    );
    const hasTables =
      (await page.locator('[data-testid="ranking-table"]').count()) > 0;

    const hasEmptyState = (await emptyState.count()) > 0;

    expect(hasEmptyState || !hasTables).toBe(true);
  });
});
