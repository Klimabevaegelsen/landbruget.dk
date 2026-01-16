import { defineConfig, devices } from '@playwright/test';

/**
 * Smoke test configuration for pre-commit hooks
 * Fast, lightweight tests that verify basic functionality
 */
export default defineConfig({
  testDir: './tests',
  /* Run tests in files in parallel */
  fullyParallel: true,
  /* Fail the build on CI if you accidentally left test.only in the source code. */
  forbidOnly: !!process.env.CI,
  /* No retries for smoke tests - fail fast */
  retries: 0,
  /* Single worker for speed */
  workers: 1,
  /* Simple reporter for pre-commit */
  reporter: [['line'], ['html', { open: 'never' }]],
  /* Shared settings for all the projects below. */
  use: {
    /* Base URL to use in actions like `await page.goto('/')`. */
    baseURL: 'http://localhost:3000',
    /* Collect trace only on failure */
    trace: 'retain-on-failure',
    /* Shorter timeouts for smoke tests */
    actionTimeout: 10000,
    navigationTimeout: 30000,
  },

  /* Configure projects - only Chromium for speed */
  projects: [
    {
      name: 'chromium-smoke',
      use: { ...devices['Desktop Chrome'] },
    },
  ],

  /* Run your local dev server before starting the tests */
  webServer: {
    command: 'npm run dev',
    url: 'http://localhost:3000',
    reuseExistingServer: !process.env.CI,
    timeout: 60 * 1000, // 1 minute timeout for smoke tests
  },
});
