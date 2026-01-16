/**
 * Cache utilities for Tuesday-based weekly data updates
 * Since data is updated weekly on Tuesdays, cache should expire on Tuesday morning
 */

/**
 * Calculate milliseconds until next Tuesday at 9:00 AM (when data updates)
 * This ensures cache expires right when fresh data becomes available
 */
export function getMillisecondsUntilNextTuesday(): number {
  const now = new Date();
  const nextTuesday = new Date(now);

  // Find next Tuesday
  const daysUntilTuesday = (2 - now.getDay() + 7) % 7; // Tuesday is day 2
  if (daysUntilTuesday === 0 && now.getHours() >= 9) {
    // If it's Tuesday after 9 AM, go to next Tuesday
    nextTuesday.setDate(now.getDate() + 7);
  } else {
    nextTuesday.setDate(now.getDate() + daysUntilTuesday);
  }

  // Set to 9:00 AM Tuesday
  nextTuesday.setHours(9, 0, 0, 0);

  return nextTuesday.getTime() - now.getTime();
}

/**
 * Get cache expiration timestamp for next Tuesday
 */
export function getNextTuesdayExpiration(): number {
  return Date.now() + getMillisecondsUntilNextTuesday();
}

/**
 * Check if cached data is still valid (hasn't passed Tuesday update time)
 */
export function isCacheValidUntilTuesday(timestamp: number): boolean {
  return Date.now() < timestamp;
}

/**
 * Get HTTP Cache-Control header value for weekly Tuesday updates
 * Uses stale-while-revalidate to serve stale content while fetching fresh data
 */
export function getWeeklyCacheHeaders(): Record<string, string> {
  const secondsUntilTuesday = Math.floor(
    getMillisecondsUntilNextTuesday() / 1000
  );
  const maxAge = Math.max(3600, secondsUntilTuesday); // At least 1 hour, up to next Tuesday
  const staleWhileRevalidate = 7 * 24 * 60 * 60; // 1 week stale-while-revalidate

  return {
    'Cache-Control': `public, max-age=${maxAge}, stale-while-revalidate=${staleWhileRevalidate}`,
    'CDN-Cache-Control': `public, max-age=${maxAge}`,
    'Vercel-CDN-Cache-Control': `public, max-age=${maxAge}`,
  };
}

/**
 * Format cache age for display (e.g., "2 days old")
 */
export function formatCacheAge(timestamp: number): string {
  const ageMs = Date.now() - timestamp;
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
}

/**
 * Get the next Tuesday date for display
 */
export function getNextTuesdayDate(): string {
  const nextTuesday = new Date(Date.now() + getMillisecondsUntilNextTuesday());
  return nextTuesday.toLocaleDateString('da-DK', {
    weekday: 'long',
    year: 'numeric',
    month: 'long',
    day: 'numeric',
  });
}
