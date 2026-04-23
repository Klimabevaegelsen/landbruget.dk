import { beforeEach, describe, expect, it, vi } from 'vitest';

import { GET } from '@/app/api/homepage-statistics/route';
import { getCachedHomepageStatistics } from '@/lib/server-cache';

vi.mock('@/lib/server-cache', () => ({
  getCachedHomepageStatistics: vi.fn(),
}));

const getCachedHomepageStatisticsMock = vi.mocked(getCachedHomepageStatistics);

describe('GET /api/homepage-statistics', () => {
  beforeEach(() => {
    getCachedHomepageStatisticsMock.mockReset();
  });

  it('returns cached homepage statistics with CDN headers', async () => {
    const payload = { totals: { companies: 123 } };
    getCachedHomepageStatisticsMock.mockResolvedValue(payload);

    const response = await GET();

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toEqual(payload);
    expect(response.headers.get('Cache-Control')).toBe(
      'public, max-age=604800, stale-while-revalidate=604800'
    );
    expect(response.headers.get('CDN-Cache-Control')).toBe(
      'public, max-age=604800'
    );
  });

  it('returns a 503 payload when the cache fetch fails', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    getCachedHomepageStatisticsMock.mockRejectedValue(
      new Error('upstream unavailable')
    );

    const response = await GET();

    expect(response.status).toBe(503);
    await expect(response.json()).resolves.toEqual({
      error: 'Failed to fetch homepage statistics',
      message: 'upstream unavailable',
    });
  });
});
