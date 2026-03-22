/**
 * E2E Smoke Tests for Static Pages
 *
 * Validates that /kilder, /brugsvilkaar, /om-os, /privatlivspolitik
 * load correctly, render an h1, and produce no console errors.
 */

import { test, expect } from '@playwright/test';

const STATIC_PAGES = [
  { path: '/kilder', heading: 'Kilder' },
  { path: '/brugsvilkaar', heading: 'Brugsvilkår' },
  { path: '/om-os', heading: 'Om os' },
  { path: '/privatlivspolitik', heading: 'Privatlivspolitik' },
] as const;

for (const { path: pagePath, heading } of STATIC_PAGES) {
  test.describe(`Static page: ${pagePath}`, () => {
    let consoleErrors: string[];

    test.beforeEach(async ({ page }) => {
      consoleErrors = [];
      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          consoleErrors.push(msg.text());
        }
      });
    });

    test('should load and display h1', async ({ page }) => {
      await page.goto(pagePath);

      const h1 = page.locator('h1');
      await expect(h1).toBeVisible({ timeout: 10000 });
      await expect(h1).toContainText(heading);
    });

    test('should have no console errors', async ({ page }) => {
      await page.goto(pagePath);
      await page.waitForLoadState('networkidle');

      // Filter out known non-critical warnings
      const criticalErrors = consoleErrors.filter(
        (err) =>
          !err.includes('favicon') &&
          !err.includes('third-party cookie') &&
          !err.includes('DevTools')
      );

      expect(criticalErrors).toEqual([]);
    });

    test('should have navigation visible', async ({ page }) => {
      await page.goto(pagePath);
      await expect(page.locator('nav')).toBeVisible();
    });
  });
}
