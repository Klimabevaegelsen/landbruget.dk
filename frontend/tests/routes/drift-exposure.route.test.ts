import { beforeEach, describe, expect, it, vi } from 'vitest';

import { GET } from '@/app/api/drift-exposure/route';
import {
  getCachedDriftExposureIndex,
  getCachedDriftExposureTile,
} from '@/lib/server-cache';

import { createRouteRequest } from './helpers/request';

vi.mock('@/lib/server-cache', () => ({
  getCachedDriftExposureIndex: vi.fn(),
  getCachedDriftExposureTile: vi.fn(),
}));

const getCachedDriftExposureIndexMock = vi.mocked(getCachedDriftExposureIndex);
const getCachedDriftExposureTileMock = vi.mocked(getCachedDriftExposureTile);

describe('GET /api/drift-exposure', () => {
  beforeEach(() => {
    getCachedDriftExposureIndexMock.mockReset();
    getCachedDriftExposureTileMock.mockReset();
  });

  it('returns index metadata when no tile is requested', async () => {
    const payload = {
      pesticide_year: 2024,
      national_avg_drift_dose_kg: 1.23,
      building_count: 42,
      tile_zoom: 12,
      tile_count: 3,
    };
    getCachedDriftExposureIndexMock.mockResolvedValue(payload);

    const response = await GET(
      createRouteRequest('https://landbruget.dk/api/drift-exposure')
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toEqual(payload);
  });

  it('returns a tile shard when z/x/y are provided', async () => {
    const payload = [{ uid: 'b1', lat: 55.67, lng: 12.56, pct: 95, dose: 2.4 }];
    getCachedDriftExposureTileMock.mockResolvedValue(payload);

    const response = await GET(
      createRouteRequest(
        'https://landbruget.dk/api/drift-exposure?z=12&x=2204&y=1287'
      )
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toEqual(payload);
    expect(getCachedDriftExposureTileMock).toHaveBeenCalledWith(12, 2204, 1287);
  });

  it('rejects incomplete tile coordinates', async () => {
    const response = await GET(
      createRouteRequest('https://landbruget.dk/api/drift-exposure?z=12&x=2204')
    );

    expect(response.status).toBe(400);
    await expect(response.json()).resolves.toEqual({
      error: 'missing tile coordinates',
    });
  });

  it('rejects invalid tile coordinates', async () => {
    const response = await GET(
      createRouteRequest(
        'https://landbruget.dk/api/drift-exposure?z=12&x=west&y=1287'
      )
    );

    expect(response.status).toBe(400);
    await expect(response.json()).resolves.toEqual({
      error: 'invalid tile coordinates',
    });
  });
});
