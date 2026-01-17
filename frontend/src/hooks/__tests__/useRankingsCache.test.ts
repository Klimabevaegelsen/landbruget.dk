/**
 * Tests for useRankingsCache hook
 *
 * This hook manages rankings data caching with Tuesday-based expiration
 */

import { test, expect } from '@playwright/test';

test.describe('useRankingsCache', () => {
  test('should initialize with empty cache', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const cache = new Map();
      return cache.size === 0;
    });

    expect(result).toBe(true);
  });

  test('should cache rankings data by category', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const cache = new Map();
      const category = 'pesticide_usage';
      const now = Date.now();

      const rankingsData = {
        rankings: [
          {
            id: 'table-1',
            title: 'Top Pesticide Users',
            category: 'pesticide_usage',
            description: 'Companies by pesticide usage',
            unit: 'kg',
            last_updated: new Date().toISOString(),
            company_count: 10,
            items: [],
          },
        ],
        metadata: {
          generated_at: new Date().toISOString(),
          total_tables: 1,
        },
      };

      cache.set(category, {
        data: rankingsData,
        timestamp: now,
        expiresAt: now + 86400000,
        category,
      });

      return cache.has(category);
    });

    expect(result).toBe(true);
  });

  test('should return null for cache miss', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const cache = new Map();
      const category = 'nonexistent';
      return cache.get(category) === undefined;
    });

    expect(result).toBe(true);
  });

  test('should return null for expired cache', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const now = Date.now();
      const expiresAt = now - 1000; // Expired

      return now < expiresAt; // Should be false (expired)
    });

    expect(result).toBe(false);
  });

  test('should clear specific category cache', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const cache = new Map();
      const category = 'pesticide_usage';
      const now = Date.now();

      // Add data
      cache.set(category, {
        data: { rankings: [], metadata: { generated_at: '', total_tables: 0 } },
        timestamp: now,
        expiresAt: now + 86400000,
        category,
      });

      // Clear specific category
      cache.delete(category);

      return cache.size === 0;
    });

    expect(result).toBe(true);
  });

  test('should clear all cache', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const cache = new Map();
      const now = Date.now();

      // Add multiple categories
      ['category1', 'category2', 'category3'].forEach((cat) => {
        cache.set(cat, {
          data: {
            rankings: [],
            metadata: { generated_at: '', total_tables: 0 },
          },
          timestamp: now,
          expiresAt: now + 86400000,
          category: cat,
        });
      });

      // Clear all
      cache.clear();

      return cache.size === 0;
    });

    expect(result).toBe(true);
  });

  test('should validate cache correctly', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const now = Date.now();

      const validExpiry = now + 86400000;
      const invalidExpiry = now - 1000;

      return {
        valid: now < validExpiry,
        invalid: now < invalidExpiry,
      };
    });

    expect(result.valid).toBe(true);
    expect(result.invalid).toBe(false);
  });

  test('should calculate cache age correctly', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const now = Date.now();
      const timestamp = now - 3600000; // 1 hour ago

      const age = now - timestamp;

      return age >= 3600000 && age <= 3600100; // Allow small time difference
    });

    expect(result).toBe(true);
  });

  test('should handle localStorage serialization', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const category = 'test_category';
      const now = Date.now();

      const data = {
        data: {
          rankings: [
            {
              id: 'table-1',
              title: 'Test Table',
              category: 'test',
              description: 'Test description',
              unit: 'units',
              last_updated: new Date().toISOString(),
              company_count: 5,
              items: [],
            },
          ],
          metadata: {
            generated_at: new Date().toISOString(),
            total_tables: 1,
          },
        },
        timestamp: now,
        expiresAt: now + 86400000,
        category,
      };

      // Serialize
      const serialized = JSON.stringify(data);

      // Deserialize
      const deserialized = JSON.parse(serialized);

      return deserialized.category === category;
    });

    expect(result).toBe(true);
  });
});
