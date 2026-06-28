/**
 * E2E Tests for Methodology Sub-Pages
 *
 * Validates that /pesticidanalyse/metode, /pesticidanalyse/pfas,
 * and /pesticidanalyse/grundvand load correctly and render key elements.
 */

import { test, expect } from '@playwright/test';
import { expectResponsiveNavigation } from './helpers/navigation';

const METHODOLOGY_PAGES = [
  {
    path: '/pesticidanalyse/metode',
    testId: 'article-layout',
    heading: 'Metode',
  },
  {
    path: '/pesticidanalyse/pfas',
    testId: 'pfas-page-content',
    heading: 'PFAS',
  },
  {
    path: '/pesticidanalyse/grundvand',
    testId: 'grundvand-page-content',
    heading: 'grundvand',
  },
] as const;

for (const { path: pagePath, testId, heading } of METHODOLOGY_PAGES) {
  test.describe(`Methodology page: ${pagePath}`, () => {
    let consoleErrors: string[];

    test.beforeEach(async ({ page }) => {
      consoleErrors = [];
      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          consoleErrors.push(msg.text());
        }
      });
    });

    test('should load and display content', async ({ page }) => {
      await page.goto(pagePath);
      await page.waitForLoadState('domcontentloaded');

      const content = page.getByTestId(testId);
      await expect(content).toBeVisible({ timeout: 15000 });
    });

    test(`should contain "${heading}" in heading`, async ({ page }) => {
      await page.goto(pagePath);

      const h1 = page.locator('h1');
      await expect(h1).toBeVisible({ timeout: 10000 });
      await expect(h1).toContainText(heading, { ignoreCase: true });
    });

    test('should have navigation visible', async ({ page }) => {
      await page.goto(pagePath);
      await expectResponsiveNavigation(page);
    });

    test('should have no critical console errors', async ({ page }) => {
      await page.goto(pagePath);
      await page.waitForLoadState('networkidle', { timeout: 15000 });

      const criticalErrors = consoleErrors.filter(
        (err) =>
          !err.includes('favicon') &&
          !err.includes('third-party cookie') &&
          !err.includes('DevTools') &&
          !err.includes('Failed to fetch') &&
          !err.includes('ERR_BLOCKED_BY_CLIENT')
      );

      expect(criticalErrors).toEqual([]);
    });
  });
}

test.describe('Methodology scrolly section', () => {
  test('should render scrolly steps on metode page', async ({ page }) => {
    await page.goto('/pesticidanalyse/metode');

    const scrolly = page.getByTestId('scrolly-section');
    await expect(scrolly).toBeVisible({ timeout: 15000 });

    const firstStep = page.getByTestId('scrolly-step-context');
    await expect(firstStep).toBeVisible();
  });

  test('should render SJI record overlay', async ({ page }) => {
    await page.goto('/pesticidanalyse/metode');

    const scrolly = page.getByTestId('scrolly-section');
    await expect(scrolly).toBeVisible({ timeout: 15000 });

    // Scroll to 'record' step to trigger overlay
    const recordStep = page.getByTestId('scrolly-step-record');
    await recordStep.scrollIntoViewIfNeeded();
    await page.waitForTimeout(1000);

    const overlay = page.getByTestId('scrolly-record-overlay');
    await expect(overlay).toBeVisible({ timeout: 5000 });
  });

  test('should render reference anchors', async ({ page }) => {
    await page.goto('/pesticidanalyse/metode');
    await page.waitForLoadState('domcontentloaded');

    // Check that ref-1 exists in the reference list
    const ref1 = page.locator('#ref-1');
    await expect(ref1).toBeAttached();
  });
});
