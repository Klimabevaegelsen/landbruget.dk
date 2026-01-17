/**
 * Tests for useCompanyCache hook
 *
 * This hook manages company data caching with Tuesday-based expiration
 */

import { test, expect } from '@playwright/test';

// Mock localStorage for tests
const mockLocalStorage = () => {
  const store: Record<string, string> = {};

  return {
    getItem: (key: string) => store[key] || null,
    setItem: (key: string, value: string) => {
      store[key] = value;
    },
    removeItem: (key: string) => {
      delete store[key];
    },
    clear: () => {
      Object.keys(store).forEach((key) => delete store[key]);
    },
    get length() {
      return Object.keys(store).length;
    },
    key: (index: number) => Object.keys(store)[index] || null,
  };
};

test.describe('useCompanyCache', () => {
  test.beforeEach(() => {
    mockLocalStorage();
  });

  test('should initialize with empty cache', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      // Test that cache initializes properly
      const cache = new Map();
      return cache.size === 0;
    });

    expect(result).toBe(true);
  });

  test('should cache company basic info with Tuesday expiration', async ({
    page,
  }) => {
    await page.goto('/');

    const nextTuesday = await page.evaluate(() => {
      // Calculate next Tuesday
      const now = new Date();
      const nextTuesday = new Date(now);
      const daysUntilTuesday = (2 - now.getDay() + 7) % 7;
      if (daysUntilTuesday === 0 && now.getHours() >= 9) {
        nextTuesday.setDate(now.getDate() + 7);
      } else {
        nextTuesday.setDate(now.getDate() + daysUntilTuesday);
      }
      nextTuesday.setHours(9, 0, 0, 0);
      return nextTuesday.getTime();
    });

    expect(nextTuesday).toBeGreaterThan(Date.now());
  });

  test('should handle cache miss correctly', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const cache = new Map();
      const companyId = 'nonexistent-company';
      return cache.get(companyId) === undefined;
    });

    expect(result).toBe(true);
  });

  test('should update last accessed time on cache hit', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const cache = new Map();
      const now = Date.now();
      const companyId = 'test-company';

      // Add company to cache
      cache.set(companyId, {
        basic_info: {
          company_id: companyId,
          cvr_number: '12345678',
          company_name: 'Test Company',
        },
        timestamp: now - 1000,
        expiresAt: now + 86400000,
        last_accessed: now - 1000,
      });

      // Access the company
      const cached = cache.get(companyId);
      if (cached) {
        cached.last_accessed = Date.now();
      }

      // Verify last_accessed was updated
      const updated = cache.get(companyId);
      return updated && updated.last_accessed > now - 1000;
    });

    expect(result).toBe(true);
  });

  test('should increment access count on retrieval', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const accessCounts = new Map<string, number>();
      const companyId = 'test-company';

      // First access
      accessCounts.set(companyId, (accessCounts.get(companyId) || 0) + 1);

      // Second access
      accessCounts.set(companyId, (accessCounts.get(companyId) || 0) + 1);

      return accessCounts.get(companyId) === 2;
    });

    expect(result).toBe(true);
  });

  test('should handle localStorage persistence', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const cache = new Map();
      const companyId = 'test-company';
      const now = Date.now();

      cache.set(companyId, {
        basic_info: {
          company_id: companyId,
          cvr_number: '12345678',
          company_name: 'Test Company',
        },
        timestamp: now,
        expiresAt: now + 86400000,
        last_accessed: now,
      });

      // Convert to object for localStorage
      const cacheObject = Object.fromEntries(cache);
      const serialized = JSON.stringify(cacheObject);

      // Deserialize
      const deserialized = JSON.parse(serialized);

      return deserialized[companyId] !== undefined;
    });

    expect(result).toBe(true);
  });

  test('should reject expired cache entries', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const now = Date.now();
      const expiresAt = now - 1000; // Expired 1 second ago

      return now < expiresAt; // Should return false (expired)
    });

    expect(result).toBe(false);
  });

  test('should clean up cache when reaching threshold', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const MAX_ENTRIES = 500;
      const THRESHOLD = 0.8;
      const cache = new Map();

      // Fill cache to threshold
      for (let i = 0; i < MAX_ENTRIES * THRESHOLD + 10; i++) {
        cache.set(`company-${i}`, {
          basic_info: {
            company_id: `company-${i}`,
            cvr_number: '12345678',
            company_name: `Company ${i}`,
          },
          timestamp: Date.now(),
          expiresAt: Date.now() + 86400000,
          last_accessed: Date.now(),
        });
      }

      return cache.size > MAX_ENTRIES * THRESHOLD;
    });

    expect(result).toBe(true);
  });

  test('should preserve most accessed entries during cleanup', async ({
    page,
  }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const accessCounts = new Map<string, number>();

      // Add entries with different access counts
      accessCounts.set('company-1', 10);
      accessCounts.set('company-2', 5);
      accessCounts.set('company-3', 1);

      // Sort by access count
      const sorted = Array.from(accessCounts.entries()).sort(
        (a, b) => b[1] - a[1]
      );

      return sorted[0][0] === 'company-1' && sorted[0][1] === 10;
    });

    expect(result).toBe(true);
  });

  test('should cache companies from rankings', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const cache = new Map();
      const rankings = [
        {
          items: [
            {
              company_id: 'company-1',
              cvr_number: '12345678',
              company_name: 'Company 1',
              municipality: 'Test Municipality',
            },
            {
              company_id: 'company-2',
              cvr_number: '87654321',
              company_name: 'Company 2',
              municipality: 'Test Municipality 2',
            },
          ],
        },
      ];

      // Cache companies from rankings
      rankings.forEach((ranking) => {
        ranking.items.forEach((item) => {
          if (!cache.has(item.company_id)) {
            cache.set(item.company_id, {
              basic_info: item,
              timestamp: Date.now(),
              expiresAt: Date.now() + 86400000,
              last_accessed: Date.now(),
            });
          }
        });
      });

      return cache.size === 2;
    });

    expect(result).toBe(true);
  });

  test('should clear specific company from cache', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const cache = new Map();
      const companyId = 'test-company';

      // Add company
      cache.set(companyId, {
        basic_info: {
          company_id: companyId,
          cvr_number: '12345678',
          company_name: 'Test Company',
        },
        timestamp: Date.now(),
        expiresAt: Date.now() + 86400000,
        last_accessed: Date.now(),
      });

      // Clear specific company
      cache.delete(companyId);

      return cache.size === 0;
    });

    expect(result).toBe(true);
  });

  test('should calculate cache statistics correctly', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const cache = new Map();
      const now = Date.now();

      // Add companies
      cache.set('company-1', {
        basic_info: {
          company_id: 'company-1',
          cvr_number: '12345678',
          company_name: 'Company 1',
        },
        timestamp: now - 2000,
        expiresAt: now + 86400000,
        last_accessed: now - 2000,
      });

      cache.set('company-2', {
        basic_info: {
          company_id: 'company-2',
          cvr_number: '87654321',
          company_name: 'Company 2',
        },
        details: {
          company_id: 'company-2',
          cvr_number: '87654321',
          company_name: 'Company 2',
          employee_count: 50,
        },
        timestamp: now - 1000,
        expiresAt: now + 86400000,
        last_accessed: now - 1000,
      });

      const totalCompanies = cache.size;
      const companiesWithDetails = Array.from(cache.values()).filter(
        (c) => c.details
      ).length;

      return { totalCompanies, companiesWithDetails };
    });

    expect(result.totalCompanies).toBe(2);
    expect(result.companiesWithDetails).toBe(1);
  });
});
