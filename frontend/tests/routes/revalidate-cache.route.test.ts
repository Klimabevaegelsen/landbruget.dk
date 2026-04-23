import { beforeEach, describe, expect, it, vi } from 'vitest';

import { GET, POST } from '@/app/api/revalidate-cache/route';

import { createRouteRequest } from './helpers/request';

const { revalidatePathMock, revalidateTagMock } = vi.hoisted(() => ({
  revalidatePathMock: vi.fn(),
  revalidateTagMock: vi.fn(),
}));

vi.mock('next/cache', () => ({
  revalidatePath: revalidatePathMock,
  revalidateTag: revalidateTagMock,
}));

describe('revalidate-cache route', () => {
  beforeEach(() => {
    revalidatePathMock.mockReset();
    revalidateTagMock.mockReset();
  });

  it('revalidates the requested tags and mapped paths', async () => {
    vi.spyOn(console, 'log').mockImplementation(() => {});
    const request = createRouteRequest(
      'https://landbruget.dk/api/revalidate-cache?tags=homepage-stats, municipality-rankings'
    );

    const response = await POST(request);

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({
      success: true,
      tags: ['homepage-stats', ' municipality-rankings'],
    });
    expect(revalidateTagMock).toHaveBeenCalledWith('homepage-stats', 'max');
    expect(revalidateTagMock).toHaveBeenCalledWith(
      'municipality-rankings',
      'max'
    );
    expect(revalidatePathMock).toHaveBeenCalledWith('/api/homepage-statistics');
    expect(revalidatePathMock).toHaveBeenCalledWith('/api/data/kommuner');
    expect(revalidatePathMock).toHaveBeenCalledWith(
      '/api/data/municipality-details'
    );
  });

  it('returns cache metadata for operators', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-04-22T10:00:00.000Z'));

    const response = await GET();
    const payload = await response.json();

    expect(response.status).toBe(200);
    expect(payload).toMatchObject({
      message: 'Cache revalidation endpoint for Tuesday data updates',
      cache_strategy: '7-day server cache + manual Tuesday invalidation',
    });
    expect(payload.available_tags).toContain('homepage-stats');
    expect(String(payload.next_tuesday_copenhagen)).toContain('tirsdag');

    vi.useRealTimers();
  });
});
