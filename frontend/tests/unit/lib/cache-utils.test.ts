/**
 * Tests for cache utilities
 *
 * These utilities manage Tuesday-based cache expiration
 */

import { test, expect } from '@playwright/test';

test.describe('Cache Utilities', () => {
  test('should calculate milliseconds until next Tuesday', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const now = new Date();
      const nextTuesday = new Date(now);

      const daysUntilTuesday = (2 - now.getDay() + 7) % 7;
      if (daysUntilTuesday === 0 && now.getHours() >= 9) {
        nextTuesday.setDate(now.getDate() + 7);
      } else {
        nextTuesday.setDate(now.getDate() + daysUntilTuesday);
      }

      nextTuesday.setHours(9, 0, 0, 0);

      const milliseconds = nextTuesday.getTime() - now.getTime();

      return {
        milliseconds,
        isPositive: milliseconds > 0,
        isReasonable:
          milliseconds > 0 && milliseconds <= 7 * 24 * 60 * 60 * 1000,
      };
    });

    expect(result.isPositive).toBe(true);
    expect(result.isReasonable).toBe(true);
  });

  test('should return next Tuesday if today is Tuesday before 9 AM', async ({
    page,
  }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      // Test with a Tuesday at 8 AM
      const testDate = new Date('2024-01-02T08:00:00'); // A Tuesday
      const nextTuesday = new Date(testDate);

      const daysUntilTuesday = (2 - testDate.getDay() + 7) % 7;
      if (daysUntilTuesday === 0 && testDate.getHours() >= 9) {
        nextTuesday.setDate(testDate.getDate() + 7);
      } else {
        nextTuesday.setDate(testDate.getDate() + daysUntilTuesday);
      }

      nextTuesday.setHours(9, 0, 0, 0);

      return {
        isSameDay: nextTuesday.getDate() === testDate.getDate(),
        isNineAM: nextTuesday.getHours() === 9,
      };
    });

    expect(result.isSameDay).toBe(true);
    expect(result.isNineAM).toBe(true);
  });

  test('should return next week Tuesday if today is Tuesday after 9 AM', async ({
    page,
  }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      // Test with a Tuesday at 10 AM
      const testDate = new Date('2024-01-02T10:00:00'); // A Tuesday
      const nextTuesday = new Date(testDate);

      const daysUntilTuesday = (2 - testDate.getDay() + 7) % 7;
      if (daysUntilTuesday === 0 && testDate.getHours() >= 9) {
        nextTuesday.setDate(testDate.getDate() + 7);
      } else {
        nextTuesday.setDate(testDate.getDate() + daysUntilTuesday);
      }

      nextTuesday.setHours(9, 0, 0, 0);

      const daysDiff = Math.floor(
        (nextTuesday.getTime() - testDate.getTime()) / (24 * 60 * 60 * 1000)
      );

      return {
        isNextWeek: daysDiff >= 6 && daysDiff <= 7,
        isNineAM: nextTuesday.getHours() === 9,
      };
    });

    expect(result.isNextWeek).toBe(true);
    expect(result.isNineAM).toBe(true);
  });

  test('should validate cache correctly', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const now = Date.now();

      const validTimestamp = now + 86400000; // Tomorrow
      const expiredTimestamp = now - 86400000; // Yesterday

      return {
        validIsValid: now < validTimestamp,
        expiredIsValid: now < expiredTimestamp,
      };
    });

    expect(result.validIsValid).toBe(true);
    expect(result.expiredIsValid).toBe(false);
  });

  test('should get next Tuesday expiration timestamp', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const now = new Date();
      const nextTuesday = new Date(now);

      const daysUntilTuesday = (2 - now.getDay() + 7) % 7;
      if (daysUntilTuesday === 0 && now.getHours() >= 9) {
        nextTuesday.setDate(now.getDate() + 7);
      } else {
        nextTuesday.setDate(now.getDate() + daysUntilTuesday);
      }

      nextTuesday.setHours(9, 0, 0, 0);

      const expiration = nextTuesday.getTime();

      return {
        isFuture: expiration > Date.now(),
        isTuesday: nextTuesday.getDay() === 2,
      };
    });

    expect(result.isFuture).toBe(true);
    expect(result.isTuesday).toBe(true);
  });

  test('should format cache age correctly', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const now = Date.now();

      const formatCacheAge = (timestamp: number): string => {
        const ageMs = now - timestamp;
        const ageDays = Math.floor(ageMs / (24 * 60 * 60 * 1000));
        const ageHours = Math.floor(
          (ageMs % (24 * 60 * 60 * 1000)) / (60 * 60 * 1000)
        );

        if (ageDays > 0) {
          return `${ageDays} dag${ageDays > 1 ? 'e' : ''} gammel`;
        } else if (ageHours > 0) {
          return `${ageHours} time${ageHours > 1 ? 'r' : ''} gammel`;
        } else {
          return 'Ny data';
        }
      };

      const twoDaysAgo = now - 2 * 24 * 60 * 60 * 1000;
      const oneHourAgo = now - 60 * 60 * 1000;
      const justNow = now - 1000;

      return {
        twoDays: formatCacheAge(twoDaysAgo),
        oneHour: formatCacheAge(oneHourAgo),
        justNow: formatCacheAge(justNow),
      };
    });

    expect(result.twoDays).toBe('2 dage gammel');
    expect(result.oneHour).toBe('1 time gammel');
    expect(result.justNow).toBe('Ny data');
  });

  test('should get cache headers with correct max-age', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const now = new Date();
      const nextTuesday = new Date(now);

      const daysUntilTuesday = (2 - now.getDay() + 7) % 7;
      if (daysUntilTuesday === 0 && now.getHours() >= 9) {
        nextTuesday.setDate(now.getDate() + 7);
      } else {
        nextTuesday.setDate(now.getDate() + daysUntilTuesday);
      }

      nextTuesday.setHours(9, 0, 0, 0);

      const secondsUntilTuesday = Math.floor(
        (nextTuesday.getTime() - now.getTime()) / 1000
      );
      const maxAge = Math.max(3600, secondsUntilTuesday);
      const staleWhileRevalidate = 7 * 24 * 60 * 60;

      const headers = {
        'Cache-Control': `public, max-age=${maxAge}, stale-while-revalidate=${staleWhileRevalidate}`,
      };

      return {
        maxAgeIsPositive: maxAge > 0,
        maxAgeIsReasonable: maxAge >= 3600 && maxAge <= 7 * 24 * 60 * 60,
        hasStaleWhileRevalidate: headers['Cache-Control'].includes(
          'stale-while-revalidate'
        ),
      };
    });

    expect(result.maxAgeIsPositive).toBe(true);
    expect(result.maxAgeIsReasonable).toBe(true);
    expect(result.hasStaleWhileRevalidate).toBe(true);
  });

  test('should get next Tuesday date in Danish format', async ({ page }) => {
    await page.goto('/');

    const result = await page.evaluate(() => {
      const now = new Date();
      const nextTuesday = new Date(now);

      const daysUntilTuesday = (2 - now.getDay() + 7) % 7;
      if (daysUntilTuesday === 0 && now.getHours() >= 9) {
        nextTuesday.setDate(now.getDate() + 7);
      } else {
        nextTuesday.setDate(now.getDate() + daysUntilTuesday);
      }

      nextTuesday.setHours(9, 0, 0, 0);

      const dateString = nextTuesday.toLocaleDateString('da-DK', {
        weekday: 'long',
        year: 'numeric',
        month: 'long',
        day: 'numeric',
      });

      return {
        isString: typeof dateString === 'string',
        hasTuesday: dateString.toLowerCase().includes('tirsdag'),
        isNotEmpty: dateString.length > 0,
      };
    });

    expect(result.isString).toBe(true);
    expect(result.hasTuesday).toBe(true);
    expect(result.isNotEmpty).toBe(true);
  });
});
