import { test, expect } from '@playwright/test';

test.describe('Smoke Tests', () => {
  test('homepage loads without errors', async ({ page }) => {
    // Navigate to homepage
    await page.goto('/');

    // Check that page loads (no 404 or 500 errors)
    await expect(page).not.toHaveTitle(/404|500|Error/i);

    // Check that basic HTML structure exists
    await expect(page.locator('body')).toBeVisible();

    // Check for no JavaScript errors in console
    const errors: string[] = [];
    page.on('pageerror', (error) => {
      errors.push(error.message);
    });

    // Wait a moment for any async operations
    await page.waitForTimeout(2000);

    // Verify no critical JavaScript errors
    expect(
      errors.filter(
        (error) => !error.includes('Warning') && !error.includes('DevTools')
      )
    ).toHaveLength(0);
  });

  test('basic navigation works', async ({ page }) => {
    await page.goto('/');

    // Check if navigation elements exist
    const nav = page.locator('nav').first();
    if ((await nav.count()) > 0) {
      await expect(nav).toBeVisible();
    }

    // Test that we can navigate (if links exist)
    const links = page.locator('a[href^="/"]');
    if ((await links.count()) > 0) {
      const firstLink = links.first();
      const href = await firstLink.getAttribute('href');
      if (href && href !== '/') {
        await firstLink.click();
        await page.waitForLoadState('networkidle');
        await expect(page).toHaveURL(new RegExp(href));
      }
    }
  });
});
