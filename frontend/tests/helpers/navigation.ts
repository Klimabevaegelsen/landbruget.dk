import { expect, type Page } from '@playwright/test';

export async function expectResponsiveNavigation(page: Page) {
  const mobileMenuButton = page.getByRole('button', {
    name: /open main menu/i,
  });

  if (await mobileMenuButton.isVisible().catch(() => false)) {
    await expect(mobileMenuButton).toBeVisible();
    return;
  }

  await expect(page.locator('nav').first()).toBeVisible();
}
