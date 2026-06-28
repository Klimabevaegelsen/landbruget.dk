import { test, expect } from '@playwright/test';
import { expectResponsiveNavigation } from './helpers/navigation';

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
    await expectResponsiveNavigation(page);

    const aboutLink = page.locator('a[href="/om-os"]').first();
    await expect(aboutLink).toHaveAttribute('href', '/om-os');

    await page.goto('/om-os', { waitUntil: 'commit' });
    await expect(page).toHaveURL(/\/om-os$/);
    await expect(page.getByRole('heading', { name: 'Om os' })).toBeVisible();
  });
});
